import argparse
import json
import os
import platform
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from bank.hdfs import read_manifest


CASES = {
    "baseline": {"shuffle": 64, "broadcast": False, "partitioned": False, "target_bytes": 16 * 1024**2},
    "candidate": {"shuffle": 8, "broadcast": True, "partitioned": True, "target_bytes": 128 * 1024**2},
}


def event_metrics(path):
    groups, stages, totals = {}, {}, {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            if event["Event"] == "SparkListenerJobStart":
                group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                if group:
                    groups[event["Job ID"]] = group
                    for stage_id in event["Stage IDs"]:
                        stages[stage_id] = group
            elif event["Event"] == "SparkListenerTaskEnd":
                group = stages.get(event["Stage ID"])
                if not group:
                    continue
                metrics = event.get("Task Metrics") or {}
                total = totals.setdefault(group, {"shuffle_read_bytes": 0, "shuffle_write_bytes": 0})
                read = metrics.get("Shuffle Read Metrics", {})
                total["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get(
                    "Local Bytes Read", 0
                )
                total["shuffle_write_bytes"] += metrics.get("Shuffle Write Metrics", {}).get(
                    "Shuffle Bytes Written", 0
                )
    return totals


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--repeats", type=int, required=True)
    parser.add_argument("--result", required=True)
    args = parser.parse_args()
    manifest = read_manifest(args.batch_id)
    spark = SparkSession.builder.appName("bank-benchmark").getOrCreate()
    app_id = spark.sparkContext.applicationId
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bench")

    def paths(entity):
        return [
            f"hdfs://namenode:8020/sources/{args.batch_id}/{e['name']}"
            for e in manifest["files"]
            if e["entity"] == entity
        ]

    transactions = spark.read.schema(
        "transaction_id STRING,account_id STRING,event_at STRING,amount STRING,currency STRING,status STRING,channel STRING"
    ).json(paths("transactions"))
    accounts = spark.read.schema("account_id STRING,customer_id STRING").json(paths("accounts"))
    records, checksums = [], set()
    for repetition in range(args.repeats):
        # Alternating order reduces one-sided warm-cache bias; no cached DataFrames.
        names = list(CASES) if repetition % 2 == 0 else list(reversed(CASES))
        for name in names:
            settings = CASES[name]
            group = f"{name}-{repetition}"
            spark.sparkContext.setJobGroup(group, group)
            spark.conf.set("spark.sql.shuffle.partitions", settings["shuffle"])
            spark.conf.set("spark.sql.adaptive.enabled", "false")
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            reference = F.broadcast(accounts) if settings["broadcast"] else accounts
            joined = transactions.join(reference, "account_id").select(
                "transaction_id",
                "customer_id",
                F.to_timestamp("event_at").alias("event_at"),
                F.col("amount").cast("decimal(18,2)").alias("amount"),
                "currency",
                "status",
                "channel",
            )
            began = time.monotonic()
            writer = (
                joined.writeTo(f"lake.bench.{name}")
                .tableProperty("format-version", "2")
                .tableProperty("write.target-file-size-bytes", str(settings["target_bytes"]))
            )
            if settings["partitioned"]:
                writer = writer.partitionedBy(F.days("event_at"))
            writer.createOrReplace()
            table = spark.table(f"lake.bench.{name}")
            check = table.agg(F.count("*").alias("n"), F.sum("amount").alias("amount")).first()
            checksums.add((check.n, str(check.amount)))
            filtered_start = time.monotonic()
            table.where(F.to_date("event_at") == manifest["processing_date"]).groupBy(
                "currency", "channel"
            ).agg(F.sum("amount")).count()
            filtered_seconds = time.monotonic() - filtered_start
            seconds = time.monotonic() - began
            files = spark.sql(
                f"SELECT count(*) n,sum(file_size_in_bytes) bytes FROM lake.bench.{name}.files"
            ).first()
            records.append(
                {
                    "group": group,
                    "variant": name,
                    "repetition": repetition,
                    "settings": settings,
                    "seconds": seconds,
                    "filtered_query_seconds": filtered_seconds,
                    "files": files.n,
                    "file_bytes": files.bytes,
                    "rows": check.n,
                    "sum_amount": str(check.amount),
                    "plan": joined._jdf.queryExecution().executedPlan().toString(),
                }
            )
    spark.stop()
    assert len(checksums) == 1, "Benchmark variants produced different outputs"
    metrics = event_metrics(Path("/opt/spark-events") / app_id)
    for record in records:
        record.update(metrics.get(record["group"], {}))
        if "shuffle_write_bytes" not in record:
            raise ValueError("Missing Spark event metrics")
    report = {
        "machine": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "meminfo": Path("/proc/meminfo").read_text(),
            "cgroup_memory_limit": Path("/sys/fs/cgroup/memory.max").read_text(),
        },
        "batch_id": args.batch_id,
        "repeats": args.repeats,
        "spark": "3.5.3",
        "event_log": app_id,
        "cold_cache_guaranteed": False,
        "results": records,
    }
    Path(args.result).write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
