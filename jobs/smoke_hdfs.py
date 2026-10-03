"""Actual distributed Spark + HDFS + Iceberg smoke check, without Hive/Trino."""

import json
import tempfile

from pyspark.sql import SparkSession

from bank.generator import generate
from bank.hdfs import read_manifest
from bank.manifest import validate as validate_manifest
from jobs.pipeline import dds, initialize, raw, validate

spark = SparkSession.builder.appName("bank-distributed-hdfs-smoke").getOrCreate()
try:
    with tempfile.TemporaryDirectory() as temporary:
        folder = generate(temporary, "2025-01-01", seed=987, count=20)
        manifest = validate_manifest(folder)
        assert read_manifest(manifest["batch_id"]) == manifest
    initialize(spark)
    raw(spark, manifest)
    validate(spark, manifest)
    dds(spark, manifest)
    count = spark.table("lake.dds.transactions").count()
    assert count == 20
    print(json.dumps({"distributed_spark_hdfs_iceberg": "passed", "dds_transactions": count}))
finally:
    spark.stop()
