import contextlib
import hashlib
import json
import os
import re
import subprocess
import time
import uuid

from bank import hdfs, publisher
from bank.config import DATA, ROOT, SQL, pg_connect
from bank.manifest import validate

STAGES = ("manifest", "raw", "validate", "dds", "calculate", "stage", "reconcile", "publish", "finish")
LOCK_KEY = 821735491


class SparkFailure(RuntimeError):
    def __init__(self, message, metrics):
        super().__init__(message)
        self.metrics = metrics


def init():
    with pg_connect() as pg:
        pg.execute((SQL / "postgres" / "schema.sql").read_text(encoding="utf-8"))


@contextlib.contextmanager
def writer():
    with pg_connect() as pg:
        pg.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        pg.commit()
        try:
            yield pg
        finally:
            pg.rollback()
            pg.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
            pg.commit()


def claim(pg, run_id):
    pg.execute("INSERT INTO pipeline_lock(id,owner) VALUES(1,%s) ON CONFLICT DO NOTHING", (run_id,))
    owner = pg.execute("SELECT owner FROM pipeline_lock WHERE id=1", ()).fetchone()[0]
    if owner != run_id:
        raise RuntimeError(f"Pipeline is owned by {owner}; finish/recover it before another run")
    pg.commit()


def spark_job(stage, batch_id=None, run_id=None):
    result = DATA / "results" / f"{run_id or uuid.uuid4().hex}-{stage}.json"
    result.parent.mkdir(parents=True, exist_ok=True)
    if result.exists():
        result.unlink()
    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--driver-memory",
        os.getenv("SPARK_DRIVER_MEMORY", "1g"),
        "--executor-memory",
        os.getenv("SPARK_EXECUTOR_MEMORY", "1g"),
        "--conf",
        "spark.sql.shuffle.partitions=" + os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        "--conf",
        "spark.driver.host=" + os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
        str(ROOT / "jobs" / "pipeline.py"),
        stage,
        "--result",
        str(result),
    ]
    if batch_id:
        command += ["--batch-id", batch_id]
    log = result.with_suffix(".log")
    print(f"Spark stage {stage}; log={log}", flush=True)
    with log.open("wb") as output:
        try:
            subprocess.run(command, check=True, timeout=3600, stdout=output, stderr=subprocess.STDOUT)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            output.flush()
            with log.open("rb") as source:
                source.seek(max(0, log.stat().st_size - 16384))
                tail = source.read().decode("utf-8", errors="replace")
            metrics = json.loads(result.read_text()) if result.exists() else {}
            raise SparkFailure(f"Spark {stage} failed: {error}\n{tail}", metrics) from error
    return json.loads(result.read_text(encoding="utf-8"))


def alert(kind, payload):
    import requests
    from psycopg.types.json import Jsonb

    # Persist first; receiver failure cannot hide the original task error.
    with pg_connect() as pg:
        pg.execute("INSERT INTO alerts(kind,payload) VALUES(%s,%s)", (kind, Jsonb(payload)))
    try:
        requests.post(
            os.getenv("ALERT_URL", "http://alerts:8090/events"),
            json={"kind": kind, "payload": payload, "persisted": True},
            timeout=5,
        ).raise_for_status()
    except requests.RequestException as error:
        print(f"Local receiver unavailable: {error}", flush=True)


def abort(run_id, error):
    with writer() as pg:
        row = pg.execute("SELECT batch_id FROM runs WHERE run_id=%s", (run_id,)).fetchone()
        if row:
            pg.execute(
                "UPDATE runs SET state='failed',error=%s,finished_at=now() WHERE run_id=%s",
                (str(error), run_id),
            )
            pg.execute("UPDATE batches SET state='failed',error=%s WHERE batch_id=%s", (str(error), row[0]))
        pg.execute("DELETE FROM pipeline_lock WHERE owner=%s", (run_id,))
        pg.commit()
    alert("task_failure", {"run_id": run_id, "error": str(error)})


def run_stage(stage, folder, run_id, failure=None):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("Invalid run ID")
    started = time.monotonic()
    folder = DATA / "sources" / folder if isinstance(folder, str) else folder
    manifest = validate(folder)
    batch_id = manifest["batch_id"]
    with writer() as pg:
        claim(pg, run_id)
        pg.execute(
            """INSERT INTO batches(batch_id,processing_date,manifest,state) VALUES(%s,%s,%s::jsonb,'started')
                      ON CONFLICT(batch_id) DO NOTHING""",
            (batch_id, manifest["processing_date"], json.dumps(manifest)),
        )
        existing = pg.execute("SELECT manifest FROM batches WHERE batch_id=%s", (batch_id,)).fetchone()[0]
        if existing != manifest:
            raise ValueError("Batch manifest changed")
        pg.execute(
            "INSERT INTO runs(run_id,batch_id,state) VALUES(%s,%s,'started') ON CONFLICT DO NOTHING",
            (run_id, batch_id),
        )
        pg.commit()
        try:
            metrics = {}
            if stage == "manifest":
                hdfs.upload(folder, manifest)
            elif stage in ("raw", "validate", "dds"):
                metrics = spark_job(stage, batch_id, run_id)
            elif stage == "calculate":
                metrics = publisher.calculate(pg, run_id)
            elif stage == "stage":
                metrics = publisher.stage(pg, run_id)
            elif stage == "reconcile":
                metrics = publisher.reconcile(pg, run_id)
                if failure == "reconcile":
                    raise ValueError("Injected critical reconciliation error")
            elif stage == "publish":
                if failure == "before-publish":
                    raise RuntimeError("Injected failure before publication")
                publisher.publish(pg, run_id, batch_id, fail=failure == "inside-publish")
            elif stage == "finish":
                if (
                    pg.execute("SELECT state FROM runs WHERE run_id=%s", (run_id,)).fetchone()[0]
                    != "published"
                ):
                    raise ValueError("Finish requires successful publication")
                pg.execute("UPDATE runs SET finished_at=now() WHERE run_id=%s", (run_id,))
                pg.execute("DELETE FROM pipeline_lock WHERE owner=%s", (run_id,))
            else:
                raise ValueError(stage)
            if stage not in ("publish", "finish"):
                pg.execute("UPDATE batches SET state=%s,error=NULL WHERE batch_id=%s", (stage, batch_id))
                pg.execute("UPDATE runs SET state=%s,error=NULL WHERE run_id=%s", (stage, run_id))
            pg.execute(
                """INSERT INTO stage_metrics(run_id,stage,duration_seconds,read_count,accepted_count,rejected_count,changed_count)
                 VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id,stage) DO UPDATE SET
                 duration_seconds=excluded.duration_seconds,read_count=excluded.read_count,
                 accepted_count=excluded.accepted_count,rejected_count=excluded.rejected_count,
                 changed_count=excluded.changed_count,error=NULL,recorded_at=now()""",
                (
                    run_id,
                    stage,
                    time.monotonic() - started,
                    *(
                        metrics.get(k, 0)
                        for k in ("read_count", "accepted_count", "rejected_count", "changed_count")
                    ),
                ),
            )
            pg.commit()
        except Exception as error:
            pg.rollback()
            metrics = getattr(error, "metrics", {})
            pg.execute(
                """INSERT INTO stage_metrics(run_id,stage,duration_seconds,error,read_count,accepted_count,rejected_count)
                        VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id,stage) DO UPDATE SET
                        error=excluded.error,duration_seconds=excluded.duration_seconds,
                        read_count=excluded.read_count,accepted_count=excluded.accepted_count,
                        rejected_count=excluded.rejected_count""",
                (
                    run_id,
                    stage,
                    time.monotonic() - started,
                    str(error),
                    metrics.get("read_count", 0),
                    metrics.get("accepted_count", 0),
                    metrics.get("rejected_count", 0),
                ),
            )
            pg.commit()
            raise
    print(json.dumps({"run_id": run_id, "stage": stage, "metrics": metrics}), flush=True)


def run(folder, failure=None, run_id=None):
    run_id = run_id or uuid.uuid4().hex
    try:
        for stage in STAGES:
            run_stage(stage, folder, run_id, failure)
    except Exception as error:
        abort(run_id, error)
        raise
    return run_id


def airflow_run_id(dag_run_id):
    return "af-" + hashlib.sha256(dag_run_id.encode()).hexdigest()[:32]
