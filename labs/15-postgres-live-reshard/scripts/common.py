import json
import logging
import os
import pathlib
import random

import psycopg

logging.basicConfig(format="%(asctime)s %(levelname)-7s %(message)s", level=logging.INFO)
log = logging.getLogger("lab15")

LAB_DIR = pathlib.Path(__file__).resolve().parent.parent
ROUTER_STATE = LAB_DIR / "router_state.json"
JOURNAL = LAB_DIR / "journal.jsonl"

SEED = int(os.environ.get("SEED", "1503"))
N_ROWS = int(os.environ.get("N_ROWS", "500000"))
N_WORKSPACES = int(os.environ.get("N_WORKSPACES", "200"))
N_SHARDS = 2

NODES = {
    "mono": int(os.environ.get("MONO_PORT", "5440")),
    "shard0": int(os.environ.get("SHARD0_PORT", "5441")),
    "shard1": int(os.environ.get("SHARD1_PORT", "5442")),
}

SHARDS = ["shard0", "shard1"]


def conn(node, autocommit=True):
    return psycopg.connect(
        host=os.environ.get("PG_HOST", "127.0.0.1"),
        port=NODES[node],
        dbname="lab15",
        user="postgres",
        password="lab",
        autocommit=autocommit,
    )


def container_dsn(node):
    """DSN usable from INSIDE the compose network (subscriptions dial these)."""
    return f"host={node} dbname=lab15 user=postgres password=lab"


def shard_for(workspace_id):
    """Must match the publication row filters in replicate.py exactly."""
    return f"shard{workspace_id % N_SHARDS}"


def shard_filter(shard_index):
    return f"(workspace_id % {N_SHARDS} = {shard_index})"


def rng(salt=""):
    return random.Random(f"{SEED}:{salt}")


def read_router_state():
    if not ROUTER_STATE.exists():
        return {"authoritative": "mono", "writes_gated": False}
    return json.loads(ROUTER_STATE.read_text())


def write_router_state(state):
    tmp = ROUTER_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state))
    tmp.rename(ROUTER_STATE)


def drain_writes(settle=0.4, timeout=5.0):
    """After gating, wait until the journal stops growing: acked writes are
    journaled post-commit, so a quiet journal means in-flight ops have
    landed. Observation, not a hard barrier — the production-grade stop is
    PgBouncer PAUSE + REVOKE (see README) — but it beats a blind sleep."""
    import time

    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        size = JOURNAL.stat().st_size if JOURNAL.exists() else 0
        if size == last:
            return
        last = size
        time.sleep(settle)
    log.warning("journal still growing after %.1fs — a writer is ignoring the gate", timeout)


def append_jsonl(path, record):
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path):
    p = pathlib.Path(path)
    if not p.exists():
        return []
    with open(p) as f:
        return [json.loads(line) for line in f if line.strip()]
