"""Known-answer integration demonstration; any mismatch raises an exception."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bank.alerts import check_freshness
from bank.config import DATA, pg_connect, trino_connect
from bank.generator import write_batch
from bank.pipeline import run, spark_job, writer


def tx(key, amount, day="2025-02-01", channel="CARD", version="1", status="POSTED", account="DA1"):
    return {
        "transaction_id": key,
        "account_id": account,
        "event_at": f"{day}T12:00:00Z",
        "source_updated_at": f"2025-02-05T12:00:0{version}Z",
        "record_version": version,
        "amount": amount,
        "currency": "USD" if account == "DA2" else "RUB",
        "status": status,
        "direction": "IN" if key == "D2" else "OUT",
        "channel": channel,
    }


def fixtures(root):
    customer = {
        "customer_id": "DC1",
        "region": "MOSCOW",
        "segment": "RETAIL",
        "effective_at": "2025-02-01T00:00:00Z",
        "change_version": "1",
    }
    account = {
        "account_id": "DA1",
        "customer_id": "DC1",
        "currency": "RUB",
        "opened_at": "2025-02-01",
        "status": "ACTIVE",
        "record_version": "1",
    }
    initial = write_batch(
        root,
        "2025-02-01",
        {
            "customers": [customer],
            "accounts": [account, dict(account, account_id="DA2", currency="USD")],
            "transactions": [
                tx("D1", "100.00"),
                tx("D1", "100.00"),
                tx("D2", "50.00"),
                tx("D3", "20.00", channel="ATM", status="PENDING"),
                tx("DU", "7.50", account="DA2"),
            ],
        },
        "demo-initial",
    )
    correction = write_batch(
        root,
        "2025-02-02",
        {
            "transactions": [
                tx("D1", "120.00", "2025-02-02", "TRANSFER", "2"),
                tx("D2", "50.00", version="2", status="CANCELLED"),
            ]
        },
        "demo-correction",
    )
    late = write_batch(root, "2025-02-03", {"transactions": [tx("DL", "30.00")]}, "demo-late")
    history = write_batch(
        root,
        "2025-02-04",
        {
            "customers": [
                dict(customer, segment="PREMIUM", effective_at="2025-02-01T06:00:00Z", change_version="2")
            ]
        },
        "demo-history",
    )
    recovery = write_batch(
        root, "2025-02-05", {"transactions": [tx("DF", "40.00", "2025-02-03")]}, "demo-recovery"
    )
    bad = write_batch(root, "2025-02-06", {"transactions": [tx("BAD", "-9.00")]}, "demo-critical-dq")
    evolved = write_batch(
        root,
        "2025-02-07",
        {"transactions": [dict(tx("DM", "1.00", "2025-02-04"), memo="schema-v2")]},
        "demo-schema-v2",
        schema_version=2,
    )
    return initial, correction, late, history, recovery, bad, evolved


def mart_state():
    with pg_connect() as pg:
        return {
            table: pg.execute(f"SELECT * FROM {table} ORDER BY 1,2,3").fetchall()
            for table in ("mart_payments_daily", "mart_customer_activity", "dim_customer_history")
        }


def totals(expected):
    with pg_connect() as pg:
        actual = dict(
            pg.execute("SELECT currency,sum(total_amount) FROM mart_payments_daily GROUP BY currency")
        )
    assert actual == {k: Decimal(v) for k, v in expected.items()}, (actual, expected)


def scalar(query):
    connection = trino_connect()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    finally:
        connection.close()


def demo():
    batches = fixtures(DATA / "sources")
    initial, correction, late, history, recovery, bad, evolved = batches
    with pg_connect() as pg:
        completed = pg.execute(
            "SELECT published_at FROM batches WHERE batch_id=%s", (evolved.name,)
        ).fetchone()
        other = pg.execute(
            "SELECT count(*) FROM batches WHERE manifest->>'label' NOT LIKE 'demo-%'"
        ).fetchone()[0]
    if other:
        raise ValueError("Known-answer demo needs a dedicated Compose project; use a clean project name")
    if completed and completed[0]:
        totals({"RUB": "191.00", "USD": "7.50"})
        print("Demo previously completed; final state verified. Use a new Compose project for all steps.")
        return
    run(initial)
    totals({"RUB": "150.00", "USD": "7.50"})
    before = mart_state()
    raw_count = scalar("SELECT count(*) FROM iceberg.raw.records")
    run(initial)
    assert mart_state() == before
    assert scalar("SELECT count(*) FROM iceberg.raw.records") == raw_count
    old_snapshot = scalar(
        'SELECT snapshot_id FROM iceberg.dds."transactions$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )
    run(correction)
    totals({"RUB": "120.00", "USD": "7.50"})
    with pg_connect() as pg:
        assert (
            pg.execute(
                "SELECT count(*) FROM mart_payments_daily WHERE payment_date='2025-02-01' AND currency='RUB'"
            ).fetchone()[0]
            == 0
        )
    assert scalar("SELECT count(*) FROM iceberg.dds.transactions WHERE transaction_id='D1'") == 1
    assert scalar(
        f"SELECT amount FROM iceberg.dds.transactions FOR VERSION AS OF {int(old_snapshot)} WHERE transaction_id='D1'"
    ) == Decimal("100.00")
    assert scalar("SELECT amount FROM iceberg.dds.transactions WHERE transaction_id='D1'") == Decimal(
        "120.00"
    )
    run(late)
    totals({"RUB": "150.00", "USD": "7.50"})
    run(history)
    with pg_connect() as pg:
        rows = pg.execute(
            "SELECT segment,valid_from,valid_to,is_current FROM dim_customer_history ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2 and rows[0][0] == "RETAIL" and rows[1][0] == "PREMIUM"
        assert rows[0][2] == rows[1][1] and not rows[0][3] and rows[1][3]
    assert (
        scalar("""SELECT count(*) FROM iceberg.dds.transactions t JOIN iceberg.dds.customer_history h
        ON t.customer_version_id=h.customer_version_id WHERE h.segment<>'PREMIUM'""")
        == 0
    )
    before = mart_state()
    for failure in ("before-publish", "inside-publish", "reconcile"):
        try:
            run(recovery, failure=failure)
        except (RuntimeError, ValueError) as error:
            assert "Injected" in str(error)
        else:
            raise AssertionError("Expected injected failure")
        assert mart_state() == before
    run(recovery)
    totals({"RUB": "190.00", "USD": "7.50"})
    try:
        run(bad)
    except RuntimeError as error:
        assert "Critical DQ" in str(error), error
        with pg_connect() as pg:
            assert (
                pg.execute("SELECT published_at FROM batches WHERE batch_id=%s", (bad.name,)).fetchone()[0]
                is None
            )
    else:
        raise AssertionError("Critical DQ did not block publication")
    totals({"RUB": "190.00", "USD": "7.50"})
    with writer() as pg:
        assert pg.execute("SELECT count(*) FROM pipeline_lock").fetchone()[0] == 0
        spark_job("schema-demo")
    run(evolved)
    # Reads original v1 source files after schema evolution; cannot resurrect cancelled rows.
    run(initial)
    totals({"RUB": "191.00", "USD": "7.50"})
    assert scalar("SELECT memo FROM iceberg.dds.transactions WHERE transaction_id='DM'") == "schema-v2"
    assert not check_freshness(86400, now=datetime.now(timezone.utc) + timedelta(days=2))
    with pg_connect() as pg:
        assert pg.execute("SELECT count(*) FROM alerts WHERE kind='task_failure'").fetchone()[0] >= 4
        assert pg.execute("SELECT count(*) FROM alerts WHERE kind='freshness'").fetchone()[0] >= 1
    report = {
        "status": "passed",
        "raw_rows": scalar("SELECT count(*) FROM iceberg.raw.records"),
        "dds_transactions": scalar("SELECT count(*) FROM iceberg.dds.transactions"),
        "rub_total": "191.00",
        "usd_total": "7.50",
        "old_snapshot": old_snapshot,
    }
    import json

    (DATA / "demo-result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
