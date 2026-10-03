"""Trino snapshot reads -> bounded COPY -> transactional mart publication."""

import csv
import hashlib
import json
from decimal import Decimal

from bank.config import DATA, SQL, trino_connect

EXPORTS = {
    "payments": ("stage_payments", "mart_payments_daily", "payments.sql"),
    "activity": ("stage_activity", "mart_customer_activity", "activity.sql"),
    "history": ("stage_history", "dim_customer_history", "history.sql"),
}


def snapshots(pg, run_id):
    tc = trino_connect()
    result = {}
    try:
        for table in ("transactions", "customer_history"):
            cursor = tc.cursor()
            cursor.execute(
                f'SELECT snapshot_id FROM iceberg.dds."{table}$snapshots" ORDER BY committed_at DESC LIMIT 1'
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError("Missing Iceberg snapshot")
            result[table] = int(row[0])
    finally:
        tc.close()
    pg.execute("UPDATE runs SET snapshots=%s::jsonb WHERE run_id=%s", (json.dumps(result), run_id))
    return result


def pinned(sql, versions):
    for table, snapshot in versions.items():
        sql = sql.replace(f"iceberg.dds.{table}", f"iceberg.dds.{table} FOR VERSION AS OF {int(snapshot)}")
    return sql


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def calculate(pg, run_id):
    versions = snapshots(pg, run_id)
    folder = DATA / "exports" / run_id
    folder.mkdir(parents=True, exist_ok=True)
    tc = trino_connect()
    manifest = {"snapshots": versions, "files": {}}
    try:
        for name, (_, _, filename) in EXPORTS.items():
            cursor = tc.cursor()
            cursor.execute(pinned((SQL / "trino" / filename).read_text(), versions))
            temporary = folder / (name + ".csv.tmp")
            count = 0
            with temporary.open("w", encoding="utf-8", newline="") as output:
                stream = csv.writer(output, lineterminator="\n")
                while rows := cursor.fetchmany(2000):
                    for row in rows:
                        stream.writerow(tuple(row) + (run_id,))
                    count += len(rows)
            path = temporary.with_suffix("")
            temporary.replace(path)
            manifest["files"][name] = {"records": count, "sha256": file_hash(path)}
        cursor = tc.cursor()
        cursor.execute(pinned((SQL / "trino" / "reconcile.sql").read_text(), versions))
        manifest["expected"] = []
        while rows := cursor.fetchmany(100):
            for currency, count, amount in rows:
                manifest["expected"].append([currency, count, str(amount)])
    finally:
        tc.close()
    temporary = folder / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest), encoding="utf-8")
    temporary.replace(folder / "manifest.json")
    return {"accepted_count": sum(f["records"] for f in manifest["files"].values())}


def stage(pg, run_id):
    versions = pg.execute("SELECT snapshots FROM runs WHERE run_id=%s", (run_id,)).fetchone()[0]
    folder = DATA / "exports" / run_id
    manifest = json.loads((folder / "manifest.json").read_text())
    if not versions or versions != manifest["snapshots"]:
        raise ValueError("Export snapshot identity mismatch")
    copied = 0
    for name, (table, _, _) in EXPORTS.items():
        path = folder / (name + ".csv")
        if file_hash(path) != manifest["files"][name]["sha256"]:
            raise ValueError("Export checksum mismatch")
        pg.execute(f"DELETE FROM {table} WHERE run_id=%s", (run_id,))
        with pg.cursor().copy(f"COPY {table} FROM STDIN WITH (FORMAT CSV, NULL '')") as copy:
            with path.open("rb") as source:
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    copy.write(block)
        count = pg.execute(f"SELECT count(*) FROM {table} WHERE run_id=%s", (run_id,)).fetchone()[0]
        if count != manifest["files"][name]["records"]:
            raise ValueError("Export row count mismatch")
        copied += count
    pg.execute("DELETE FROM reconciliation WHERE run_id=%s", (run_id,))
    for currency, count, amount in manifest["expected"]:
        pg.execute(
            """INSERT INTO reconciliation(run_id,currency,expected_count,expected_amount)
                      VALUES (%s,%s,%s,%s)""",
            (run_id, currency, count, Decimal(amount)),
        )
    return {"accepted_count": copied}


def reconcile(pg, run_id):
    pg.execute("UPDATE reconciliation SET actual_count=NULL,actual_amount=NULL WHERE run_id=%s", (run_id,))
    pg.execute(
        """UPDATE reconciliation r SET actual_count=a.n,actual_amount=a.amount
        FROM (SELECT currency,sum(transaction_count) n,sum(total_amount) amount FROM stage_payments
              WHERE run_id=%s GROUP BY currency) a WHERE r.run_id=%s AND r.currency=a.currency""",
        (run_id, run_id),
    )
    difference = pg.execute(
        """SELECT count(*) FROM reconciliation
        WHERE run_id=%s AND (expected_count IS DISTINCT FROM actual_count OR expected_amount IS DISTINCT FROM actual_amount)""",
        (run_id,),
    ).fetchone()[0]
    unexpected = pg.execute(
        """SELECT count(*) FROM stage_payments p WHERE run_id=%s AND NOT EXISTS
        (SELECT 1 FROM reconciliation r WHERE r.run_id=p.run_id AND r.currency=p.currency)""",
        (run_id,),
    ).fetchone()[0]
    activity = pg.execute(
        """WITH a AS (
        SELECT currency,sum(transaction_count) n,sum(incoming_amount+outgoing_amount) amount
        FROM stage_activity WHERE run_id=%s GROUP BY currency
    ), p AS (
        SELECT currency,sum(transaction_count) n,sum(total_amount) amount
        FROM stage_payments WHERE run_id=%s GROUP BY currency
    ) SELECT count(*) FROM a FULL JOIN p USING(currency)
      WHERE a.n IS DISTINCT FROM p.n OR a.amount IS DISTINCT FROM p.amount""",
        (run_id, run_id),
    ).fetchone()[0]
    for table, keys in (
        ("stage_payments", "payment_date,currency,channel"),
        ("stage_activity", "customer_id,activity_date,currency"),
        ("stage_history", "customer_version_id"),
    ):
        if pg.execute(
            f"SELECT count(*) FROM (SELECT {keys} FROM {table} WHERE run_id=%s GROUP BY {keys} HAVING count(*)>1) q",
            (run_id,),
        ).fetchone()[0]:
            raise ValueError("Critical DQ: staging duplicate keys")
    scd = pg.execute(
        """WITH h AS (
        SELECT *,row_number() OVER (PARTITION BY customer_id ORDER BY valid_from) n,
                 lag(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from) previous_to
        FROM stage_history WHERE run_id=%s
    ) SELECT count(*) FROM h WHERE valid_to<=valid_from OR is_current<>(valid_to IS NULL)
      OR (n>1 AND (previous_to IS NULL OR previous_to>valid_from))""",
        (run_id,),
    ).fetchone()[0]
    missing_customers = pg.execute(
        """SELECT count(*) FROM stage_activity a WHERE a.run_id=%s
        AND NOT EXISTS (SELECT 1 FROM stage_history h WHERE h.run_id=a.run_id AND h.customer_id=a.customer_id)""",
        (run_id,),
    ).fetchone()[0]
    if difference or unexpected or activity or scd or missing_customers:
        raise ValueError(
            f"Critical DQ: reconciliation failed ({difference},{unexpected},{activity},{scd},{missing_customers})"
        )
    return {
        "accepted_count": pg.execute(
            "SELECT count(*) FROM reconciliation WHERE run_id=%s", (run_id,)
        ).fetchone()[0]
    }


def publish(pg, run_id, batch_id, fail=False):
    # Caller holds the global writer lock. All changes and marker share this transaction.
    reconcile(pg, run_id)
    for staging, mart, _ in EXPORTS.values():
        columns = [
            r[0]
            for r in pg.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position""",
                (mart,),
            )
        ]
        projection = ",".join(columns)
        pg.execute(f"DELETE FROM {mart}")
        pg.execute(
            f"INSERT INTO {mart} ({projection}) SELECT {projection} FROM {staging} WHERE run_id=%s", (run_id,)
        )
    if fail:
        raise RuntimeError("Injected failure inside publication transaction")
    pg.execute(
        "UPDATE batches SET state='published',published_at=now(),error=NULL WHERE batch_id=%s", (batch_id,)
    )
    pg.execute("UPDATE runs SET state='published' WHERE run_id=%s", (run_id,))
