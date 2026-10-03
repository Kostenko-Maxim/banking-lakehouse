from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from bank.config import DATA
from bank.generator import generate
from bank.pipeline import STAGES, abort, airflow_run_id, run_stage


def execute_stage(stage, **context):
    conf = context["dag_run"].conf or {}
    day = conf.get("processing_date") or context["data_interval_start"].date().isoformat()
    batch_id = conf.get("batch_id")
    if batch_id:
        folder = DATA / "sources" / batch_id
    else:
        folder = generate(DATA / "sources", day, scenario="daily")
    run_stage(stage, folder, airflow_run_id(context["dag_run"].run_id), conf.get("failure"))


def failed(context):
    abort(airflow_run_id(context["dag_run"].run_id), context.get("exception", "Airflow DAG failed"))


with DAG(
    "banking_daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(hours=1),
    },
    dagrun_timeout=timedelta(hours=6),
    on_failure_callback=failed,
    tags=["banking", "iceberg", "local"],
) as dag:
    previous = None
    for stage in STAGES:
        task = PythonOperator(
            task_id=stage, python_callable=execute_stage, op_kwargs={"stage": stage}, do_xcom_push=False
        )
        if previous:
            previous >> task
        previous = task
