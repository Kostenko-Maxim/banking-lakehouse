"""Deterministic, streaming source batches. Amounts are decimal strings."""

import hashlib
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from bank.config import ENTITIES

PROFILES = {"small": 1000, "1m": 1_000_000, "10m": 10_000_000}


def money(cents):
    return f"{cents // 100}.{cents % 100:02d}"


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def timestamp(day, seconds=0):
    return (
        (
            datetime.combine(date.fromisoformat(str(day)), datetime.min.time(), timezone.utc)
            + timedelta(seconds=seconds)
        )
        .isoformat()
        .replace("+00:00", "Z")
    )


def write_batch(root, day, rows, label="generated", schema_version=1):
    """rows is a mapping of entity -> iterable; no input dataset is materialized."""
    day = date.fromisoformat(str(day)).isoformat()
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    import tempfile

    with tempfile.TemporaryDirectory(dir=root) as temporary:
        folder = Path(temporary)
        files = []
        for entity in ENTITIES:
            count, shard, stream, digest, entry = 0, 0, None, None, None
            for record in rows.get(entity, ()):
                if count % 100_000 == 0:
                    if stream:
                        stream.close()
                        files.append(entry)
                    filename = f"{entity}-{shard:05d}.jsonl"
                    stream = (folder / filename).open("wb")
                    digest = hashlib.sha256()
                    entry = {"entity": entity, "name": filename, "records": 0}
                    shard += 1
                payload = dict(record, source_record_id=f"{entity}:{count:012d}")
                raw = (canonical(payload) + "\n").encode("utf-8")
                stream.write(raw)
                digest.update(raw)
                entry["records"] += 1
                entry["sha256"] = digest.hexdigest()
                count += 1
            if stream:
                stream.close()
                files.append(entry)
        identity = {"processing_date": day, "label": label, "schema_version": schema_version, "files": files}
        batch_id = day + "-" + hashlib.sha256(canonical(identity).encode()).hexdigest()[:20]
        manifest = dict(identity, batch_id=batch_id)
        (folder / "manifest.json").write_text(canonical(manifest) + "\n", encoding="utf-8")
        destination = root / batch_id
        if destination.exists():
            if (destination / "manifest.json").read_bytes() != (folder / "manifest.json").read_bytes():
                raise ValueError("Immutable batch collision")
            return destination
        folder.rename(destination)
        # TemporaryDirectory tolerates an already moved directory.
    return destination


def generate(root, day, profile="small", seed=42, scenario="initial", count=None):
    count = PROFILES[profile] if count is None else count
    if count < 1:
        raise ValueError("count must be positive")
    day = date.fromisoformat(str(day))
    customers = min(1000, max(2, count // 10))
    rng = random.Random(f"{seed}:{day}:{scenario}")
    base_day = date(2025, 1, 1)

    def customer_rows():
        for i in range(customers):
            yield {
                "customer_id": f"C{i:06d}",
                "region": ["MOSCOW", "URAL", "SIBERIA"][i % 3],
                "segment": "RETAIL",
                "effective_at": timestamp(base_day),
                "change_version": "1",
            }
        if scenario in ("daily", "customer-late"):
            yield {
                "customer_id": "C000000",
                "region": "MOSCOW",
                "segment": "PREMIUM",
                "effective_at": timestamp(day - timedelta(days=1), 43200),
                "change_version": "2",
            }

    def account_rows():
        for i in range(customers):
            yield {
                "account_id": f"A{i:06d}",
                "customer_id": f"C{i:06d}",
                "currency": "RUB",
                "opened_at": str(base_day),
                "status": "ACTIVE",
                "record_version": "1",
            }

    def transaction_rows():
        for i in range(count):
            row = {
                "transaction_id": f"T{day:%Y%m%d}-{i:010d}",
                "account_id": f"A{i % customers:06d}",
                "event_at": timestamp(day, i % 86400),
                "source_updated_at": timestamp(day, i % 86400),
                "record_version": "1",
                "amount": money(rng.randrange(1, 1000000)),
                "currency": "RUB",
                "status": "POSTED",
                "direction": ["IN", "OUT"][i % 2],
                "channel": ["CARD", "TRANSFER", "ATM"][i % 3],
            }
            yield row
            if scenario == "duplicates" and i % 10 == 0:
                yield row
        if scenario in ("daily", "correction", "cancel"):
            yield {
                "transaction_id": f"T{day - timedelta(days=1):%Y%m%d}-0000000000",
                "account_id": "A000000",
                "event_at": timestamp(day),
                "source_updated_at": timestamp(day, 1),
                "record_version": "2",
                "amount": "123.45",
                "currency": "RUB",
                "direction": "OUT",
                "channel": "TRANSFER",
                "status": "CANCELLED" if scenario == "cancel" else "POSTED",
            }
        if scenario in ("daily", "late"):
            yield {
                "transaction_id": f"LATE-{day}",
                "account_id": "A000000",
                "event_at": timestamp(day - timedelta(days=2), 43200),
                "source_updated_at": timestamp(day),
                "record_version": "1",
                "amount": "10.00",
                "currency": "RUB",
                "direction": "IN",
                "channel": "CARD",
                "status": "POSTED",
            }
        if scenario == "invalid":
            yield {"transaction_id": "BAD", "account_id": "UNKNOWN", "amount": "-1", "record_version": "1"}

    return write_batch(
        root,
        day,
        {"customers": customer_rows(), "accounts": account_rows(), "transactions": transaction_rows()},
        f"{profile}:{seed}:{scenario}:{count}",
    )
