import csv
import json
from datetime import date
from decimal import Decimal

import pytest

from bank import publisher


def test_export_is_bounded_exact_and_detects_corruption(tmp_path, monkeypatch):
    versions = {"transactions": 123, "customer_history": 456}
    records = [
        ((date(2025, 2, 1), "RUB", f"CHANNEL-{i}", 1, Decimal("9999999999999999.99")) for i in range(5001)),
        iter([("C1", date(2025, 2, 1), "RUB", 1, Decimal("0.00"), Decimal("10.00"), "2025-02-01T12:00:00Z")]),
        iter([("H1", "C1", "MOSCOW", "RETAIL", "2025-02-01T00:00:00Z", None, True, 1)]),
        iter([("RUB", 5001, Decimal("50004999999999999949.99"))]),
    ]
    queries, sizes = [], []

    class Cursor:
        def __init__(self, rows):
            self.rows = rows

        def execute(self, sql):
            queries.append(sql)

        def fetchmany(self, size):
            import itertools

            result = list(itertools.islice(self.rows, size))
            sizes.append(len(result))
            return result

    class Connection:
        def cursor(self):
            return Cursor(records.pop(0))

        def close(self):
            pass

    class Pg:
        def execute(self, *_):
            return self

        def fetchone(self):
            return (versions,)

    monkeypatch.setattr(publisher, "DATA", tmp_path)
    monkeypatch.setattr(publisher, "snapshots", lambda *_: versions)
    monkeypatch.setattr(publisher, "trino_connect", Connection)
    assert publisher.calculate(Pg(), "run-test")["accepted_count"] == 5003
    assert max(sizes) == 2000
    assert all("FOR VERSION AS OF" in sql for sql in queries)
    folder = tmp_path / "exports" / "run-test"
    manifest = json.loads((folder / "manifest.json").read_text())
    assert manifest["files"]["payments"]["records"] == 5001
    with (folder / "payments.csv").open(newline="") as stream:
        assert next(csv.reader(stream))[4] == "9999999999999999.99"
    with (folder / "history.csv").open(newline="") as stream:
        assert next(csv.reader(stream))[5] == ""
    with (folder / "payments.csv").open("a") as stream:
        stream.write("corruption\n")
    with pytest.raises(ValueError, match="checksum"):
        publisher.stage(Pg(), "run-test")
