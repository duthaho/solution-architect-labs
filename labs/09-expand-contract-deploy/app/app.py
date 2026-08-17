"""The application under deployment. One file, four behaviors.

The version ladder — the only thing that changes between "deploys":

    version   writes                     reads
    v1        name                       name
    v1.5      name + first/last          name          (expand-aware)
    v2        name + first/last          first/last    (reads flipped)
    v3        first/last                 first/last    (old shape abandoned)

Two invariants the lab depends on:

- The HTTP API never changes: {"name": "First Last"} in, {"name": ...} out.
  The client can't tell versions apart except by the `served_by` header —
  which is exactly what "backward compatible deploy" means.
- The version is read ONCE, at boot, from /state/<SERVICE_NAME>.version.
  A "deploy" is: write that file, restart the container. There is no
  hot-reload — real pods don't hot-swap their code either.

Split rule (duplicated in scripts/common.py and in backfill SQL — the
verifier proves all three agree): last_name = everything after the LAST
space; a name with no space keeps last_name = ''.
"""
import json
import os
import re
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql

SERVICE = os.environ.get("SERVICE_NAME", "app")
VERSION_FILE = f"/state/{SERVICE}.version"
try:
    VERSION = open(VERSION_FILE).read().strip() or "v1"
except FileNotFoundError:
    VERSION = "v1"

WRITES_OLD = VERSION in ("v1", "v1.5", "v2")
WRITES_NEW = VERSION in ("v1.5", "v2", "v3")
READS_NEW = VERSION in ("v2", "v3")

_local = threading.local()


def conn() -> pymysql.Connection:
    c = getattr(_local, "conn", None)
    if c is not None:
        try:
            c.ping(reconnect=True)
            return c
        except Exception:
            pass
    c = pymysql.connect(host="mysql", user="root", password="lab", database="lab09",
                        autocommit=True, charset="utf8mb4", connect_timeout=3)
    _local.conn = c
    return c


def split_name(name: str) -> tuple[str, str]:
    parts = name.rsplit(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (name, "")


def compose_name(first: str | None, last: str | None) -> str | None:
    if first is None and last is None:
        return None
    return " ".join(p for p in (first, last) if p)


def create_user(name: str, email: str) -> int:
    first, last = split_name(name)
    with conn().cursor() as cur:
        if WRITES_OLD and WRITES_NEW:
            cur.execute("INSERT INTO users (name, first_name, last_name, email) "
                        "VALUES (%s, %s, %s, %s)", (name, first, last, email))
        elif WRITES_OLD:
            cur.execute("INSERT INTO users (name, email) VALUES (%s, %s)", (name, email))
        else:
            cur.execute("INSERT INTO users (first_name, last_name, email) "
                        "VALUES (%s, %s, %s)", (first, last, email))
        return cur.lastrowid


def update_user(user_id: int, name: str) -> int:
    first, last = split_name(name)
    with conn().cursor() as cur:
        if WRITES_OLD and WRITES_NEW:
            cur.execute("UPDATE users SET name=%s, first_name=%s, last_name=%s WHERE id=%s",
                        (name, first, last, user_id))
        elif WRITES_OLD:
            cur.execute("UPDATE users SET name=%s WHERE id=%s", (name, user_id))
        else:
            cur.execute("UPDATE users SET first_name=%s, last_name=%s WHERE id=%s",
                        (first, last, user_id))
        return cur.rowcount


def read_user(user_id: int) -> dict | None:
    with conn().cursor() as cur:
        if READS_NEW:
            cur.execute("SELECT first_name, last_name FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if row is None:
                return None
            return {"id": user_id, "name": compose_name(row[0], row[1])}
        cur.execute("SELECT name FROM users WHERE id=%s", (user_id,))
        row = cur.fetchone()
        if row is None:
            return None
        return {"id": user_id, "name": row[0]}


USER_PATH = re.compile(r"^/users/(\d+)$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # journaling is the client's job, not ours
        pass

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Served-By", f"{SERVICE}/{VERSION}")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, {"service": SERVICE, "version": VERSION})
        m = USER_PATH.match(self.path)
        if not m:
            return self._send(404, {"error": "not found"})
        try:
            user = read_user(int(m.group(1)))
        except Exception as e:
            return self._send(500, {"error": str(e)[:300]})
        if user is None:
            return self._send(404, {"error": "no such user"})
        self._send(200, user)

    def do_POST(self):
        if self.path != "/users":
            return self._send(404, {"error": "not found"})
        try:
            body = self._body()
            user_id = create_user(body["name"], body.get("email", "x@example.com"))
        except Exception as e:
            return self._send(500, {"error": str(e)[:300]})
        self._send(201, {"id": user_id})

    def do_PUT(self):
        m = USER_PATH.match(self.path)
        if not m:
            return self._send(404, {"error": "not found"})
        try:
            body = self._body()
            matched = update_user(int(m.group(1)), body["name"])
        except Exception as e:
            return self._send(500, {"error": str(e)[:300]})
        if matched == 0:
            return self._send(404, {"error": "no such user"})
        self._send(200, {"updated": True})


def main():
    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    # Graceful stop: docker sends SIGTERM; finish in-flight requests, then exit.
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    print(f"{SERVICE} serving as {VERSION} "
          f"(writes_old={WRITES_OLD} writes_new={WRITES_NEW} reads_new={READS_NEW})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
