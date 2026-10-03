"""Production Spark rules on an embedded HadoopCatalog; no substitutes for Spark/Iceberg."""

import json
import tempfile
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from bank.demo import fixtures
from bank.manifest import validate as manifest_validate
from jobs.pipeline import dds, initialize, raw, schema_demo, validate


def main():
    spark = SparkSession.builder.appName("bank-production-spark-checks").getOrCreate()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        initial, correction, late, history, recovery, bad, evolved = fixtures(root)
        initialize(spark)

        def process(folder):
            manifest = manifest_validate(folder)
            raw(spark, manifest, root.as_uri())
            validate(spark, manifest)
            return dds(spark, manifest)

        def total(currency):
            return spark.sql(
                f"SELECT sum(amount) FROM lake.dds.transactions WHERE status='POSTED' AND currency='{currency}'"
            ).first()[0]

        process(initial)
        assert total("RUB") == Decimal("150.00")
        initial_raw_count = spark.table("lake.raw.records").count()
        assert process(initial)["changed_count"] == 0
        assert spark.table("lake.raw.records").count() == initial_raw_count
        process(correction)
        assert total("RUB") == Decimal("120.00")
        assert (
            spark.sql("SELECT count(*) FROM lake.dds.transactions WHERE transaction_id='D1'").first()[0] == 1
        )
        process(late)
        assert total("RUB") == Decimal("150.00")
        process(history)
        assert spark.table("lake.dds.customer_history").count() == 2
        assert spark.sql("SELECT count(*) FROM lake.dds.customer_history WHERE is_current").first()[0] == 1
        assert (
            spark.sql("""SELECT count(*) FROM lake.dds.transactions t JOIN lake.dds.customer_history h
            ON t.customer_version_id=h.customer_version_id WHERE h.segment<>'PREMIUM'""").first()[0]
            == 0
        )
        process(recovery)
        assert total("RUB") == Decimal("190.00")
        manifest = manifest_validate(bad)
        raw(spark, manifest, root.as_uri())
        try:
            validate(spark, manifest)
        except ValueError as error:
            assert "Critical DQ" in str(error)
        else:
            raise AssertionError("DQ should have failed")
        assert total("RUB") == Decimal("190.00")
        schema_demo(spark)
        process(evolved)
        process(initial)
        assert total("RUB") == Decimal("191.00")
        assert total("USD") == Decimal("7.50")
        assert spark.table("lake.dds.transactions").count() == 7
        print(json.dumps({"production_spark_checks": "passed", "rub_total": "191.00", "usd_total": "7.50"}))
    spark.stop()


if __name__ == "__main__":
    main()
