"""Register the Debezium MySQL source connector and wait until it runs.

Uses PUT /connectors/<name>/config, which is idempotent — create or update,
safe to rerun. The connector then:
  1. takes a consistent snapshot of lab03.orders (existing rows emitted as
     op:'r' events, keyed by PK), holding a global read lock only long enough
     to note the binlog position (milliseconds, watch traffic.log), then
  2. streams the binlog from exactly that position. No gap, no overlap by
     design — same fencepost as lab 02's "position before backfill".
"""
import json
import sys
import time
import urllib.error
import urllib.request

from common import CONNECT_URL, LAB_DIR, log

NAME = "orders-source"


def api(method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        CONNECT_URL + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def main() -> None:
    config = json.loads((LAB_DIR / "connectors" / "orders-source.json").read_text())

    deadline = time.time() + 180
    while True:
        try:
            api("GET", "/connectors")
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("Kafka Connect not reachable")
            time.sleep(2)

    api("PUT", f"/connectors/{NAME}/config", config)
    log.info("Connector %s registered, waiting for RUNNING...", NAME)

    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            status = api("GET", f"/connectors/{NAME}/status")
            conn_state = status["connector"]["state"]
            task_states = [t["state"] for t in status.get("tasks", [])]
            if conn_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
                log.info("Connector RUNNING (tasks: %s) — snapshotting, then streaming", task_states)
                return
            if "FAILED" in [conn_state, *task_states]:
                trace = next((t.get("trace", "") for t in status["tasks"]
                              if t["state"] == "FAILED"), "")
                log.error("Connector FAILED: %s", trace[:2000])
                sys.exit(1)
        except urllib.error.HTTPError:
            pass
        time.sleep(2)
    raise SystemExit("Connector did not reach RUNNING in time")


if __name__ == "__main__":
    main()
