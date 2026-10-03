"""Real PostgreSQL checks, isolated by rolling back the enclosing transaction."""

import csv
import json
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path

from bank import publisher
from bank.config import pg_connect
from bank.publisher import publish, reconcile


def checks():
    run_id = "check-" + uuid.uuid4().hex
    with pg_connect() as pg:
        pg.execute(
            "INSERT INTO batches(batch_id,processing_date,manifest,state) VALUES(%s,'2025-02-01','{}','test')",
            (run_id,),
        )
        pg.execute("INSERT INTO runs(run_id,batch_id,state) VALUES(%s,%s,'test')", (run_id, run_id))
        versions = {"transactions": 123, "customer_history": 456}
        pg.execute("UPDATE runs SET snapshots=%s::jsonb WHERE run_id=%s", (json.dumps(versions), run_id))
        rows = {
            "payments": ["2025-02-01", "RUB", "CARD", 1, "10.00"],
            "activity": ["C1", "2025-02-01", "RUB", 1, "0.00", "10.00", "2025-02-01T12:00:00Z"],
            "history": ["H1", "C1", "MOSCOW", "RETAIL", "2025-02-01", None, True, 1],
        }
        with tempfile.TemporaryDirectory() as temporary:
            original_data = publisher.DATA
            publisher.DATA = Path(temporary)
            try:
                folder = publisher.DATA / "exports" / run_id
                folder.mkdir(parents=True)
                manifest = {"snapshots": versions, "files": {}, "expected": [["RUB", 1, "10.00"]]}
                for name, row in rows.items():
                    path = folder / (name + ".csv")
                    with path.open("w", encoding="utf-8", newline="") as output:
                        csv.writer(output).writerow(row + [run_id])
                    manifest["files"][name] = {"records": 1, "sha256": publisher.file_hash(path)}
                (folder / "manifest.json").write_text(json.dumps(manifest))
                assert publisher.stage(pg, run_id)["accepted_count"] == 3
                assert (
                    pg.execute("SELECT valid_to FROM stage_history WHERE run_id=%s", (run_id,)).fetchone()[0]
                    is None
                )
            finally:
                publisher.DATA = original_data
        before = pg.execute("SELECT * FROM mart_payments_daily ORDER BY 1,2,3").fetchall()
        pg.execute("SAVEPOINT before_publication")
        try:
            publish(pg, run_id, run_id, fail=True)
        except RuntimeError:
            pg.execute("ROLLBACK TO SAVEPOINT before_publication")
        else:
            raise AssertionError("Expected publication failure")
        assert pg.execute("SELECT * FROM mart_payments_daily ORDER BY 1,2,3").fetchall() == before
        assert (
            pg.execute("SELECT published_at FROM batches WHERE batch_id=%s", (run_id,)).fetchone()[0] is None
        )
        publish(pg, run_id, run_id)
        assert pg.execute("SELECT total_amount FROM mart_payments_daily").fetchone()[0] == Decimal("10.00")
        assert (
            pg.execute("SELECT published_at FROM batches WHERE batch_id=%s", (run_id,)).fetchone()[0]
            is not None
        )
        pg.execute("UPDATE reconciliation SET expected_amount=11 WHERE run_id=%s", (run_id,))
        try:
            reconcile(pg, run_id)
        except ValueError:
            pass
        else:
            raise AssertionError("Amount mismatch should block publication")
        pg.execute("UPDATE reconciliation SET expected_amount=10 WHERE run_id=%s", (run_id,))
        pg.execute("DELETE FROM stage_payments WHERE run_id=%s", (run_id,))
        pg.execute("DELETE FROM stage_activity WHERE run_id=%s", (run_id,))
        try:
            reconcile(pg, run_id)
        except ValueError:
            pass
        else:
            raise AssertionError("Removed staging groups must not reuse old reconciliation values")
        pg.rollback()
    print("PostgreSQL COPY, rollback, atomic marker and reconciliation: passed")


if __name__ == "__main__":
    checks()
