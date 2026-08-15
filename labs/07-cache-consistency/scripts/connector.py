"""Register the Debezium MySQL source connector (cdc profile) and wait for
RUNNING. PUT /connectors/<name>/config is idempotent — safe to rerun.

snapshot.mode=no_data: an invalidator has no use for a snapshot. A cache
delete for a row that changed before the cache existed is a no-op, and the
cold cache is already correct by definition. We only need the binlog stream
from "now" — schema is captured, historical rows are not replayed.
"""
import json
import sys
import time
import urllib.error
import urllib.request

from common import CONNECT_URL, LAB_DIR, log

NAME = "products-source"


def api(method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        CONNECT_URL + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def main() -> None:
    config = json.loads((LAB_DIR / "connectors" / f"{NAME}.json").read_text())

    deadline = time.time() + 180
    while True:
        try:
            api("GET", "/connectors")
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("Kafka Connect not reachable — did you run make up-cdc?")
            time.sleep(2)

    api("PUT", f"/connectors/{NAME}/config", config)
    log.info("connector %s registered, waiting for RUNNING...", NAME)

    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            status = api("GET", f"/connectors/{NAME}/status")
            conn_state = status["connector"]["state"]
            task_states = [t["state"] for t in status.get("tasks", [])]
            if conn_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states):
                log.info("connector RUNNING — streaming the binlog from now on")
                return
            if "FAILED" in [conn_state, *task_states]:
                trace = next((t.get("trace", "") for t in status["tasks"]
                              if t["state"] == "FAILED"), "")
                log.error("connector FAILED: %s", trace[:2000])
                sys.exit(1)
        except urllib.error.HTTPError:
            pass
        time.sleep(2)
    raise SystemExit("connector did not reach RUNNING in time")


if __name__ == "__main__":
    main()
