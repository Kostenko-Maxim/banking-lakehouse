import hashlib
import json
from decimal import Decimal

import pytest

from bank.demo import fixtures
from bank.generator import generate, write_batch
from bank.manifest import validate


def test_seed_and_parameters_are_reproducible(tmp_path):
    left = generate(tmp_path / "left", "2025-01-01", seed=71, count=200)
    right = generate(tmp_path / "right", "2025-01-01", seed=71, count=200)
    assert validate(left) == validate(right)
    for file in left.iterdir():
        assert file.read_bytes() == (right / file.name).read_bytes()
    other = generate(tmp_path / "right", "2025-01-01", seed=72, count=200)
    assert other.name != right.name


def test_manifest_detects_tampering(tmp_path):
    folder = generate(tmp_path, "2025-01-01", count=10)
    file = next(folder.glob("transactions*"))
    file.write_text(file.read_text() + "{}\n")
    with pytest.raises(ValueError):
        validate(folder)


def test_streaming_input_and_shard_boundaries(tmp_path):
    seen = 0

    def rows():
        nonlocal seen
        for i in range(100001):
            seen += 1
            yield {"customer_id": str(i)}

    folder = write_batch(tmp_path, "2025-01-01", {"customers": rows()})
    manifest = validate(folder)
    assert seen == 100001
    assert [entry["records"] for entry in manifest["files"]] == [100000, 1]


def test_manifest_rejects_unapproved_schema(tmp_path):
    folder = write_batch(tmp_path, "2025-01-01", {"transactions": [{"unexpected": "field"}]})
    with pytest.raises(ValueError, match="Unapproved"):
        validate(folder)


def test_manifest_versions_and_duplicate_identity(tmp_path):
    folder = write_batch(tmp_path, "2025-01-01", {"transactions": [{"memo": "allowed"}]}, schema_version=2)
    assert validate(folder)["schema_version"] == 2
    file = next(folder.glob("transactions*"))
    row = json.loads(file.read_text())
    row["source_record_id"] = "transactions:000000000100"
    file.write_text(json.dumps(row) + "\n")
    manifest = json.loads((folder / "manifest.json").read_text())
    manifest["files"][0]["sha256"] = hashlib.sha256(file.read_bytes()).hexdigest()
    (folder / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="record id"):
        validate(folder)


def test_demo_fixture_known_initial_amounts(tmp_path):
    folders = fixtures(tmp_path)
    assert all(validate(folder) for folder in folders)
    tx = [
        json.loads(line)
        for file in folders[0].glob("transactions*")
        for line in file.read_text().splitlines()
    ]
    unique = {r["transaction_id"]: r for r in tx}
    sums = {}
    for row in unique.values():
        if row["status"] == "POSTED":
            sums[row["currency"]] = sums.get(row["currency"], Decimal(0)) + Decimal(row["amount"])
    assert sums == {"RUB": Decimal("150.00"), "USD": Decimal("7.50")}


def test_invalid_business_data_survives_source_validation(tmp_path):
    folder = generate(tmp_path, "2025-01-02", scenario="invalid", count=10)
    manifest = validate(folder)
    assert sum(e["records"] for e in manifest["files"] if e["entity"] == "transactions") == 11
