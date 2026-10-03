"""Real Compose demo + restart persistence check. Run from the project directory."""

import hashlib
import json
import subprocess
from pathlib import Path


def command(*args):
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True).stdout


def fingerprint():
    query = """SELECT md5(coalesce(string_agg(row_to_json(t)::text,'' ORDER BY payment_date,currency,channel),''))
               FROM mart_payments_daily t"""
    return command("exec", "-T", "postgres", "psql", "-U", "bank", "-d", "analytics", "-Atc", query).strip()


errors = json.loads(
    command("exec", "-T", "airflow-scheduler", "airflow", "dags", "list-import-errors", "--output", "json")
)
assert errors == [], errors
command("exec", "-T", "airflow-scheduler", "python", "-m", "bank.postgres_checks")
command("exec", "-T", "airflow-scheduler", "python", "-m", "bank.monitoring_checks")
result = command("exec", "-T", "airflow-scheduler", "python", "-m", "bank.cli", "demo")
before = fingerprint()
source = command(
    "exec",
    "-T",
    "airflow-scheduler",
    "python",
    "-c",
    "from bank.demo import scalar; print(scalar('SELECT count(*) FROM iceberg.raw.records'))",
).strip()
command("stop")
command("up", "-d", "--wait", "--wait-timeout", "600")
assert fingerprint() == before
after = command(
    "exec",
    "-T",
    "airflow-scheduler",
    "python",
    "-c",
    "from bank.demo import scalar; print(scalar('SELECT count(*) FROM iceberg.raw.records'))",
).strip()
assert after == source
output = {
    "demo": result.strip(),
    "restart_persistence": "passed",
    "mart_fingerprint": before,
    "resource_sample": command("stats", "--no-stream", "--format", "json"),
    "compose_config_sha256": hashlib.sha256(command("config").encode()).hexdigest(),
}
Path("artifacts").mkdir(exist_ok=True)
Path("artifacts/integration.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
print("Integration and restart persistence: passed")
