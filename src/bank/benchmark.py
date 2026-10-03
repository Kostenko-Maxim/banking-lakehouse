import os
import subprocess
import uuid

from bank.config import DATA, ROOT
from bank.generator import generate
from bank.hdfs import upload
from bank.manifest import validate
from bank.pipeline import writer


def benchmark(day, profile, repeats):
    if repeats < 2:
        raise ValueError("At least two repetitions are required")
    folder = generate(DATA / "sources", day, profile)
    manifest = validate(folder)
    with writer() as pg:
        if pg.execute("SELECT count(*) FROM pipeline_lock").fetchone()[0]:
            raise RuntimeError("Finish the active pipeline before benchmarking")
        upload(folder, manifest)
        output = DATA / "benchmarks" / (uuid.uuid4().hex + ".json")
        output.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                "/opt/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--driver-memory",
                os.getenv("SPARK_DRIVER_MEMORY", "1g"),
                "--executor-memory",
                os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
                "--conf",
                "spark.driver.host=" + os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
                str(ROOT / "jobs" / "benchmark.py"),
                "--batch-id",
                manifest["batch_id"],
                "--repeats",
                str(repeats),
                "--result",
                str(output),
            ],
            check=True,
            timeout=14400,
        )
    print(output.read_text())
