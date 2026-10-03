import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bank.config import pg_connect


def check_freshness(max_age, now=None):
    from bank.pipeline import alert

    now = now or datetime.now(timezone.utc)
    with pg_connect() as pg:
        last = pg.execute("SELECT max(published_at) FROM batches").fetchone()[0]
    if last is None or (now - last).total_seconds() > max_age:
        alert("freshness", {"last_published_at": str(last), "max_age_seconds": max_age})
        return False
    return True


class Receiver(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        with pg_connect() as pg:
            rows = pg.execute(
                "SELECT created_at,kind,payload FROM alerts ORDER BY id DESC LIMIT 100"
            ).fetchall()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(rows, default=str).encode())

    def do_POST(self):
        if self.path != "/events":
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", 0))
            if not 0 < size <= 65536:
                raise ValueError("Body too large or empty")
            payload = json.loads(self.rfile.read(size))
            if not payload.get("persisted"):
                from psycopg.types.json import Jsonb

                with pg_connect() as pg:
                    pg.execute(
                        "INSERT INTO alerts(kind,payload) VALUES(%s,%s)",
                        (payload["kind"], Jsonb(payload["payload"])),
                    )
            print(json.dumps(payload), flush=True)
            self.send_response(202)
            self.end_headers()
        except (ValueError, KeyError) as error:
            self.send_error(400, str(error))


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Receiver).serve_forever()
