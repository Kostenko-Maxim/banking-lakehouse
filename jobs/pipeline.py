"""Spark batch stages. Only scalar metrics cross the driver boundary."""

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from bank.config import ENTITIES
from bank.hdfs import read_manifest
from bank.manifest import FIELDS


class QualityError(ValueError):
    def __init__(self, message, metrics):
        super().__init__(message)
        self.metrics = metrics


def initialize(spark):
    for namespace in ("raw", "dds"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    spark.sql("""CREATE TABLE IF NOT EXISTS lake.raw.records (
        entity STRING, payload STRING, batch_id STRING, source_file STRING, source_record_id STRING,
        ingested_at TIMESTAMP, schema_version INT
    ) USING iceberg PARTITIONED BY (batch_id) TBLPROPERTIES ('format-version'='2')""")
    spark.sql("""CREATE TABLE IF NOT EXISTS lake.dds.transactions (
        transaction_id STRING, account_id STRING, customer_id STRING, customer_version_id STRING,
        event_at TIMESTAMP, source_updated_at TIMESTAMP, received_at TIMESTAMP, record_version BIGINT,
        amount DECIMAL(18,2), currency STRING, status STRING, direction STRING, channel STRING,
        record_hash STRING
    ) USING iceberg PARTITIONED BY (days(event_at)) TBLPROPERTIES ('format-version'='2')""")


def raw(spark, manifest, source_root="hdfs://namenode:8020/sources"):
    batch_id = manifest["batch_id"]
    read_count = 0
    for entity in ENTITIES:
        paths = [
            f"{source_root}/{batch_id}/{entry['name']}"
            for entry in manifest["files"]
            if entry["entity"] == entity
        ]
        if not paths:
            continue
        df = spark.read.text(paths).select(
            F.lit(entity).alias("entity"),
            F.col("value").alias("payload"),
            F.lit(batch_id).alias("batch_id"),
            F.input_file_name().alias("source_file"),
            F.get_json_object("value", "$.source_record_id").alias("source_record_id"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(manifest["schema_version"]).alias("schema_version"),
        )
        df.createOrReplaceTempView("incoming_raw")
        spark.sql("""MERGE INTO lake.raw.records t USING incoming_raw s
                     ON t.batch_id=s.batch_id AND t.entity=s.entity AND t.source_record_id=s.source_record_id
                     WHEN NOT MATCHED THEN INSERT *""")
        read_count += sum(e["records"] for e in manifest["files"] if e["entity"] == entity)
    actual = spark.table("lake.raw.records").where(F.col("batch_id") == batch_id).count()
    if actual != read_count:
        raise ValueError(f"Raw reconciliation failed: {actual} != {read_count}")
    return {"read_count": read_count, "accepted_count": actual}


def nonempty(column):
    return f"({column} IS NOT NULL AND length(trim({column}))>0)"


def version(column):
    return f"({column} RLIKE '^[0-9]+$' AND try_cast({column} AS BIGINT)>0)"


def utc_timestamp(column):
    return f"({column} RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T.*Z$' AND try_cast({column} AS TIMESTAMP) IS NOT NULL)"


def prepare(spark):
    rejects = []
    for entity in ENTITIES:
        fields = sorted(FIELDS[entity] | ({"memo"} if entity == "transactions" else set()))
        schema = ", ".join(f"{name} STRING" for name in fields)
        spark.sql(f"""SELECT *, from_json(payload, '{schema}') p FROM lake.raw.records
                      WHERE entity='{entity}'""").createOrReplaceTempView(f"{entity}_parsed")
        checks = []
        for name in sorted(FIELDS[entity]):
            checks.append((nonempty("p." + name), "required:" + name))
        if entity == "customers":
            checks += [
                (version("p.change_version"), "change_version"),
                (utc_timestamp("p.effective_at"), "effective_at"),
                ("p.segment IN ('RETAIL','PREMIUM','BUSINESS')", "segment"),
            ]
        else:
            checks += [
                (version("p.record_version"), "record_version"),
                ("p.currency IN ('RUB','USD','EUR')", "currency"),
            ]
        if entity == "accounts":
            checks += [
                ("p.status IN ('ACTIVE','CLOSED')", "status"),
                (
                    "p.opened_at RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND try_cast(p.opened_at AS DATE) IS NOT NULL",
                    "opened_at",
                ),
            ]
        if entity == "transactions":
            checks += [
                (utc_timestamp("p.event_at"), "event_at"),
                (utc_timestamp("p.source_updated_at"), "source_updated_at"),
                (
                    "p.amount RLIKE '^[0-9]{1,16}([.][0-9]{1,2})?$' AND try_cast(p.amount AS DECIMAL(18,2))>0",
                    "amount",
                ),
                ("p.status IN ('POSTED','PENDING','CANCELLED')", "status"),
                ("p.direction IN ('IN','OUT')", "direction"),
                ("p.channel IN ('CARD','TRANSFER','ATM')", "channel"),
            ]
        reason = (
            "concat_ws(';',"
            + ",".join(
                f"CASE WHEN coalesce(({condition}),false) THEN NULL ELSE '{label}' END"
                for condition, label in checks
            )
            + ")"
        )
        checked = spark.sql(
            f"SELECT *, {reason} reason, sha2(to_json(p),256) record_hash FROM {entity}_parsed"
        )
        checked.createOrReplaceTempView(f"{entity}_checked")
        rejects.append(
            checked.where("reason<>''").select(
                "entity", "payload", "batch_id", "source_record_id", "reason", "ingested_at"
            )
        )
        checked.where("reason=''").createOrReplaceTempView(f"{entity}_valid")

    spark.sql("""WITH ranked AS (
        SELECT p.*, ingested_at received_at, record_hash,
               row_number() OVER (PARTITION BY p.customer_id, p.effective_at
                 ORDER BY cast(p.change_version AS BIGINT) DESC,record_hash DESC) n
        FROM customers_valid
    ), changes AS (
        SELECT *, lag(concat(region,'|',segment)) OVER
          (PARTITION BY customer_id ORDER BY cast(effective_at AS TIMESTAMP)) previous_attributes
        FROM ranked WHERE n=1
    ), intervals AS (
        SELECT customer_id,region,segment,cast(effective_at AS TIMESTAMP) valid_from,
               lead(cast(effective_at AS TIMESTAMP)) OVER
                 (PARTITION BY customer_id ORDER BY cast(effective_at AS TIMESTAMP)) valid_to,
               cast(change_version AS BIGINT) change_version,received_at
        FROM changes WHERE previous_attributes IS NULL OR previous_attributes<>concat(region,'|',segment)
    ) SELECT sha2(concat(customer_id,'|',cast(valid_from AS STRING)),256) customer_version_id,
             *,valid_to IS NULL is_current FROM intervals""").createOrReplaceTempView("history")
    spark.sql("""WITH ranked AS (
        SELECT p.*,ingested_at received_at,record_hash,
          row_number() OVER (PARTITION BY p.account_id
                            ORDER BY cast(p.record_version AS BIGINT) DESC,record_hash DESC) n
        FROM accounts_valid
    ) SELECT account_id,customer_id,currency,cast(opened_at AS DATE) opened_at,status,
             cast(record_version AS BIGINT) record_version,received_at
      FROM ranked WHERE n=1 AND customer_id IN (SELECT customer_id FROM history)""").createOrReplaceTempView(
        "accounts"
    )
    orphans = spark.sql("""SELECT entity,payload,batch_id,source_record_id,'customer_fk' reason,ingested_at
        FROM accounts_valid WHERE p.customer_id NOT IN (SELECT customer_id FROM history)""")
    rejects.append(orphans)
    tx = spark.sql("""SELECT t.*,
        CASE WHEN a.account_id IS NULL THEN 'account_fk'
             WHEN a.currency<>t.p.currency THEN 'account_currency'
             WHEN cast(t.p.event_at AS DATE)<a.opened_at THEN 'before_account_opening'
             WHEN h.customer_version_id IS NULL THEN 'customer_interval'
             ELSE '' END fk_reason,
        a.customer_id, h.customer_version_id
        FROM transactions_valid t LEFT JOIN accounts a ON t.p.account_id=a.account_id
        LEFT JOIN history h ON a.customer_id=h.customer_id
          AND cast(t.p.event_at AS TIMESTAMP)>=h.valid_from
          AND (h.valid_to IS NULL OR cast(t.p.event_at AS TIMESTAMP)<h.valid_to)""")
    rejects.append(
        tx.where("fk_reason<>''").select(
            "entity",
            "payload",
            "batch_id",
            "source_record_id",
            F.col("fk_reason").alias("reason"),
            "ingested_at",
        )
    )
    tx.where("fk_reason=''").createOrReplaceTempView("transactions_with_fk")
    spark.sql("""WITH ranked AS (
        SELECT p.*,customer_id,customer_version_id,ingested_at received_at,record_hash,
               row_number() OVER (PARTITION BY p.transaction_id ORDER BY
                 cast(p.record_version AS BIGINT) DESC,cast(p.source_updated_at AS TIMESTAMP) DESC,
                 record_hash DESC) n
        FROM transactions_with_fk
    ) SELECT transaction_id,account_id,customer_id,customer_version_id,
             cast(event_at AS TIMESTAMP) event_at,cast(source_updated_at AS TIMESTAMP) source_updated_at,
             received_at,cast(record_version AS BIGINT) record_version,cast(amount AS DECIMAL(18,2)) amount,
             currency,status,direction,channel,record_hash,memo FROM ranked WHERE n=1""").createOrReplaceTempView(
        "transactions"
    )
    quarantine = rejects[0]
    for df in rejects[1:]:
        quarantine = quarantine.unionByName(df)
    quarantine.writeTo("lake.dds.quarantine").tableProperty("format-version", "2").createOrReplace()


def validate(spark, manifest):
    prepare(spark)
    batch_id = manifest["batch_id"]
    read_count = sum(e["records"] for e in manifest["files"])
    rejected = spark.table("lake.dds.quarantine").where(F.col("batch_id") == batch_id).count()
    maximum = float(os.getenv("DQ_MAX_REJECT_RATIO", "0.10"))
    if rejected / max(read_count, 1) > maximum:
        raise QualityError(
            f"Critical DQ: rejected {rejected}/{read_count}, threshold {maximum}",
            {"read_count": read_count, "accepted_count": read_count - rejected, "rejected_count": rejected},
        )
    return {"read_count": read_count, "accepted_count": read_count - rejected, "rejected_count": rejected}


def dds(spark, manifest):
    prepare(spark)
    if manifest["schema_version"] == 2 and "memo" not in spark.table("lake.dds.transactions").columns:
        raise ValueError("Schema v2 requires the approved add-memo migration")
    for view, target in (("history", "customer_history"), ("accounts", "accounts")):
        spark.table(view).writeTo(f"lake.dds.{target}").tableProperty("format-version", "2").createOrReplace()
    spark.sql("SELECT * FROM history WHERE is_current").writeTo("lake.dds.customers").createOrReplace()
    columns = spark.table("lake.dds.transactions").columns
    spark.table("transactions").select(*columns).createOrReplaceTempView("transaction_updates")
    changed = spark.sql("""SELECT count(*) FROM transaction_updates s LEFT JOIN lake.dds.transactions t
          ON s.transaction_id=t.transaction_id WHERE t.transaction_id IS NULL
          OR s.record_hash<>t.record_hash OR s.customer_version_id<>t.customer_version_id""").first()[0]
    spark.sql("""MERGE INTO lake.dds.transactions t USING transaction_updates s
        ON t.transaction_id=s.transaction_id
        WHEN MATCHED AND (t.record_hash<>s.record_hash OR t.customer_version_id<>s.customer_version_id)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
    assert_scd(spark)
    if (
        spark.sql(
            "SELECT transaction_id FROM lake.dds.transactions GROUP BY transaction_id HAVING count(*)>1"
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Critical DQ: duplicate transaction")
    if (
        spark.sql("""SELECT t.transaction_id FROM lake.dds.transactions t
        LEFT JOIN lake.dds.accounts a ON t.account_id=a.account_id
        LEFT JOIN lake.dds.customer_history h ON t.customer_version_id=h.customer_version_id
        WHERE a.account_id IS NULL OR a.customer_id<>t.customer_id OR a.currency<>t.currency
          OR h.customer_version_id IS NULL OR t.event_at<h.valid_from
          OR (h.valid_to IS NOT NULL AND t.event_at>=h.valid_to)""")
        .limit(1)
        .count()
    ):
        raise ValueError("Critical DQ: fact references do not match current history/accounts")
    return {"changed_count": changed, "accepted_count": spark.table("lake.dds.transactions").count()}


def assert_scd(spark):
    bad = spark.sql("""WITH ordered AS (
        SELECT *,lag(valid_to) OVER (PARTITION BY customer_id ORDER BY valid_from) previous_to,
                 row_number() OVER (PARTITION BY customer_id ORDER BY valid_from) n
        FROM lake.dds.customer_history
    ) SELECT * FROM ordered WHERE (valid_to IS NOT NULL AND valid_to<=valid_from)
      OR is_current<>(valid_to IS NULL) OR (n>1 AND (previous_to IS NULL OR previous_to>valid_from))""")
    if bad.limit(1).count():
        raise ValueError("Critical DQ: invalid SCD intervals")


def schema_demo(spark):
    before = spark.sql("SELECT count(*) FROM lake.dds.transactions").first()[0]
    if "memo" not in spark.table("lake.dds.transactions").columns:
        spark.sql("ALTER TABLE lake.dds.transactions ADD COLUMN memo STRING")
    try:
        spark.sql("ALTER TABLE lake.dds.transactions ALTER COLUMN amount TYPE STRING")
    except Exception:
        blocked = True
    else:
        raise AssertionError("Incompatible type change unexpectedly succeeded")
    after = spark.sql("SELECT count(*) FROM lake.dds.transactions").first()[0]
    assert before == after
    return {"old_rows_read": after, "incompatible_type_blocked": blocked}


def maintenance(spark):
    spark.sql("""CALL lake.system.rewrite_data_files(table => 'dds.transactions',
                 options => map('min-input-files','2'))""").show(truncate=False)
    spark.sql("CALL lake.system.rewrite_manifests('dds.transactions')").show(truncate=False)
    return {"data_files": spark.sql("SELECT count(*) FROM lake.dds.transactions.files").first()[0]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["raw", "validate", "dds", "schema-demo", "maintenance"])
    parser.add_argument("--batch-id")
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"bank-{args.stage}-{args.batch_id or 'admin'}").getOrCreate()
    try:
        initialize(spark)
        result = (
            globals()[args.stage.replace("-", "_")](spark)
            if not args.batch_id
            else globals()[args.stage](spark, read_manifest(args.batch_id))
        )
        Path(args.result).parent.mkdir(parents=True, exist_ok=True)
        Path(args.result).write_text(json.dumps(result), encoding="utf-8")
    except Exception as error:
        result = {"error": str(error), **getattr(error, "metrics", {})}
        Path(args.result).parent.mkdir(parents=True, exist_ok=True)
        Path(args.result).write_text(json.dumps(result), encoding="utf-8")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
