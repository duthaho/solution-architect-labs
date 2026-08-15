"""Shared config and helpers for the failover drill lab.

Topology model
--------------
Node names are their role AT BOOT ("primary", "replica1", "replica2") — after a
failover the names stop matching the roles on purpose: that mismatch is what a
real incident looks like. The single source of truth for "who is the primary
right now" is router.json; everything (writer, drills, verify) resolves the
current primary through it.

GTID model
----------
`gtid_executed`   = what a node has APPLIED.
`Retrieved_Gtid_Set` (replica status) = what a node has RECEIVED into its relay
log (superset of applied while the SQL thread catches up). Promotion candidates
are compared on received, not applied — a replica that received everything but
hasn't applied it yet loses nothing; it just needs to drain its relay log.
"""
import json
import logging
import os
import subprocess
import time
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_USER = "root"
MYSQL_PASSWORD = "lab"
DB = "lab04"
REPL_USER = "repl"
REPL_PASSWORD = "repl"

# name -> how to reach it from the host (port) and from inside the compose
# network (service), and how to strangle it with docker (container).
NODES = {
    "primary":  {"container": "lab04-primary",  "service": "primary",  "port": 3307},
    "replica1": {"container": "lab04-replica1", "service": "replica1", "port": 3308},
    "replica2": {"container": "lab04-replica2", "service": "replica2", "port": 3309},
}

ROUTER_FILE = LAB_DIR / "router.json"
JOURNAL = LAB_DIR / "journal.jsonl"
TIMELINE = LAB_DIR / "timeline.json"
RESULTS = LAB_DIR / "results.jsonl"

SEED_ROWS = int(os.environ.get("SEED_ROWS", "10000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab04")


# ---------------------------------------------------------------- connections

def connect(node: str, db: str | None = DB, timeout_s: float = 3.0) -> pymysql.Connection:
    """Connect to a node by name. Short timeouts: 'is it dead?' probes must not hang."""
    return pymysql.connect(
        host="127.0.0.1", port=NODES[node]["port"],
        user=MYSQL_USER, password=MYSQL_PASSWORD, database=db,
        autocommit=True, charset="utf8mb4", cursorclass=DictCursor,
        connect_timeout=timeout_s, read_timeout=30, write_timeout=30,
    )


def is_alive(node: str) -> bool:
    try:
        conn = connect(node, db=None, timeout_s=2.0)
        conn.close()
        return True
    except Exception:
        return False


def wait_for_node(node: str, timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if is_alive(node):
            log.info("%s is up (port %d)", node, NODES[node]["port"])
            return
        time.sleep(2)
    raise RuntimeError(f"{node} not reachable after {timeout_s}s")


# --------------------------------------------------------------------- router

def read_router() -> dict:
    """{'node': name, 'host': '127.0.0.1', 'port': N}"""
    return json.loads(ROUTER_FILE.read_text())


def write_router(node: str) -> None:
    """Atomic flip: temp file + rename. A reader never sees a half-written router."""
    tmp = ROUTER_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(
        {"node": node, "host": "127.0.0.1", "port": NODES[node]["port"]}, indent=2))
    tmp.rename(ROUTER_FILE)
    log.info("router.json -> %s (port %d)", node, NODES[node]["port"])


def current_primary() -> str:
    return read_router()["node"]


def connect_via_router(timeout_s: float = 3.0) -> pymysql.Connection:
    return connect(current_primary(), timeout_s=timeout_s)


# ----------------------------------------------------------------------- GTID

def query_one(conn, sql: str, args=None) -> dict:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


def gtid_executed(conn) -> str:
    return query_one(conn, "SELECT @@global.gtid_executed AS s")["s"].replace("\n", "")


def replica_status(conn) -> dict | None:
    row = query_one(conn, "SHOW REPLICA STATUS")
    return row or None


def retrieved_gtid_set(conn) -> str:
    st = replica_status(conn)
    return (st["Retrieved_Gtid_Set"] if st else "").replace("\n", "")


def gtid_union(conn, a: str, b: str) -> str:
    """What a node effectively HAS (received ∪ applied). MySQL has no GTID_UNION
    builtin; a ∪ b == a + (b − a)."""
    if not a:
        return b
    if not b:
        return a
    diff = query_one(conn, "SELECT GTID_SUBTRACT(%s, %s) AS d", (b, a))["d"]
    return a if not diff else f"{a},{diff}"


def gtid_subtract(conn, a: str, b: str) -> str:
    """GTIDs in a but not in b. Empty string means a ⊆ b."""
    return query_one(conn, "SELECT GTID_SUBTRACT(%s, %s) AS d", (a, b))["d"]


def gtid_count(gtid_set: str) -> int:
    """Number of transactions in a GTID set like 'uuid:1-5:7,uuid2:1-3' -> 8."""
    total = 0
    for uuid_part in filter(None, gtid_set.replace("\n", "").split(",")):
        for rng in uuid_part.split(":")[1:]:
            lo, _, hi = rng.partition("-")
            total += int(hi or lo) - int(lo) + 1
    return total


# --------------------------------------------------------------------- docker

def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], check=True, capture_output=True,
                          text=True).stdout.strip()


def docker_signal(node: str, signal: str) -> None:
    docker("kill", "-s", signal, NODES[node]["container"])
    log.info("sent %s to %s", signal, NODES[node]["container"])


def now_ms() -> int:
    return int(time.time() * 1000)


# ------------------------------------------------------------------- timeline

def timeline_put(**events: int | str) -> None:
    """Merge events (name -> epoch-ms or str) into timeline.json."""
    data = json.loads(TIMELINE.read_text()) if TIMELINE.exists() else {}
    data.update(events)
    TIMELINE.write_text(json.dumps(data, indent=2))


def timeline_get() -> dict:
    return json.loads(TIMELINE.read_text()) if TIMELINE.exists() else {}
