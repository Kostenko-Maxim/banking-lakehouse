"""Local HTTP receiver and real PostgreSQL alert journal check."""

import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer

import requests

from bank.alerts import Receiver, check_freshness
from bank.config import pg_connect
from bank.pipeline import alert


def checks():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    previous_url = os.environ.get("ALERT_URL")
    base = f"http://127.0.0.1:{server.server_port}"
    os.environ["ALERT_URL"] = base + "/events"
    try:
        assert requests.get(base + "/health", timeout=5).status_code == 200
        alert("task_failure", {"test": "monitoring-check", "error": "controlled failure"})
        assert not check_freshness(86400, now=datetime.now(timezone.utc) + timedelta(days=2))
        response = requests.get(base, timeout=5)
        response.raise_for_status()
        assert {row[1] for row in response.json()} >= {"task_failure", "freshness"}
        with pg_connect() as pg:
            assert (
                pg.execute(
                    "SELECT count(*) FROM alerts WHERE payload->>'test'='monitoring-check'"
                ).fetchone()[0]
                > 0
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        if previous_url is None:
            os.environ.pop("ALERT_URL", None)
        else:
            os.environ["ALERT_URL"] = previous_url
    print("Local webhook receiver, task failure and freshness alerts: passed")


if __name__ == "__main__":
    checks()
