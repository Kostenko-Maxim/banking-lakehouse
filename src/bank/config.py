import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "sql"
DATA = Path(os.getenv("BANK_DATA", "/data"))
ENTITIES = ("customers", "accounts", "transactions")


def pg_connect():
    import psycopg

    return psycopg.connect(
        host=os.getenv("PGHOST", "postgres"),
        user=os.getenv("PGUSER", "bank"),
        dbname=os.getenv("PGDATABASE", "analytics"),
        password=os.environ["POSTGRES_PASSWORD"],
        options="-c timezone=UTC",
        connect_timeout=10,
    )


def trino_connect():
    from trino.dbapi import connect

    return connect(host="trino", port=8080, user="bank", catalog="iceberg", schema="dds", timezone="UTC")
