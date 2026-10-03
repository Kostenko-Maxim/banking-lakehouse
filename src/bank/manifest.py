import hashlib
import json
import re
from datetime import date
from pathlib import Path

from bank.config import ENTITIES
from bank.generator import canonical

FIELDS = {
    "customers": {"customer_id", "region", "segment", "effective_at", "change_version"},
    "accounts": {"account_id", "customer_id", "currency", "opened_at", "status", "record_version"},
    "transactions": {
        "transaction_id",
        "account_id",
        "event_at",
        "source_updated_at",
        "record_version",
        "amount",
        "currency",
        "status",
        "direction",
        "channel",
    },
}


def validate(folder):
    folder = Path(folder)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    date.fromisoformat(manifest["processing_date"])
    if manifest["schema_version"] not in (1, 2):
        raise ValueError("Unsupported schema version")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}-[a-f0-9]{20}", manifest["batch_id"]):
        raise ValueError("Invalid batch id")
    names = set()
    if not manifest["files"]:
        raise ValueError("Empty batch manifest")
    for entry in manifest["files"]:
        if entry["name"] in names or not re.fullmatch(
            r"(customers|accounts|transactions)-\d{5}\.jsonl", entry["name"]
        ):
            raise ValueError("Duplicate or unsafe source path")
        names.add(entry["name"])
        if entry["entity"] not in ENTITIES or not entry["name"].startswith(entry["entity"] + "-"):
            raise ValueError("Invalid entity")
        digest, count = hashlib.sha256(), 0
        with (folder / entry["name"]).open("rb") as stream:
            for raw in stream:
                digest.update(raw)
                record = json.loads(raw)
                allowed = FIELDS[entry["entity"]] | {"source_record_id"}
                if manifest["schema_version"] == 2 and entry["entity"] == "transactions":
                    allowed |= {"memo"}
                if set(record) - allowed:
                    raise ValueError("Unapproved schema fields")
                if not isinstance(record.get("source_record_id"), str) or not record["source_record_id"]:
                    raise ValueError("Missing source record id")
                # Ordinal identity makes file streaming validation constant-memory.
                expected = f"{entry['entity']}:{int(entry['name'][-11:-6]) * 100_000 + count:012d}"
                if record["source_record_id"] != expected:
                    raise ValueError("Invalid or duplicated source record id")
                count += 1
        if digest.hexdigest() != entry["sha256"] or count != entry["records"]:
            raise ValueError("Manifest checksum/count mismatch")
    identity = {k: v for k, v in manifest.items() if k != "batch_id"}
    expected_id = (
        manifest["processing_date"] + "-" + hashlib.sha256(canonical(identity).encode()).hexdigest()[:20]
    )
    if expected_id != manifest["batch_id"]:
        raise ValueError("Manifest identity mismatch")
    return manifest
