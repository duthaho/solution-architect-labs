"""Shared config + the router for the live-resharding lab.

The router is a LIBRARY, not a proxy. The application (traffic.py) calls
Router.insert / update / read_* and never thinks about shards. Which nodes
those calls actually touch is decided per-op by the mode in router_state.json,
flipped atomically by phase.py (temp file + rename — the lab 04 pattern):

    single       reads+writes -> mono. The starting state.
    double-write writes -> mono FIRST (authoritative, this write is the ack),
                 then upsert onto the owning shard. reads -> mono.
    shadow-read  writes as in double-write. reads served from mono AND
                 re-executed on the owning shard; mismatches are journaled to
                 shadow_diffs.jsonl — the read path proves itself before it
                 takes over.
    sharded      writes -> owning shard FIRST (authoritative), then mirrored
                 back onto mono (the rollback window). reads -> owning shard.

The consistency model, one sentence: the authoritative write is the only one
that can fail the request; every non-authoritative leg is a FULL-STATE upsert
(so late/missing rows self-heal on the next touch), and the backfill only ever
INSERT IGNOREs (so it can never overwrite a fresher double-written row).

Partial-write policy (drill 5): if a non-authoritative leg fails, the request
still succeeds — the row's full state is appended to repair_queue.jsonl and
replayed later (backfill.py --repair / rollback.py). Queue-and-repair, chosen
over fail-the-request, because the authoritative copy is intact and 2PC across
mono+shard would couple the monolith's availability to every shard's. README §3.3.
"""
import json
import logging
import os
import time
import zlib
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_USER = "root"
MYSQL_PASSWORD = "lab"
DB = "lab06"
TABLE = "orders"

N_SHARDS = 2
SHARDS = [f"shard{i}" for i in range(N_SHARDS)]
NODES = {
    "mono":   {"container": "lab06-mono",   "port": int(os.environ.get("MONO_PORT", "3312"))},
    "shard0": {"container": "lab06-shard0", "port": int(os.environ.get("SHARD0_PORT", "3313"))},
    "shard1": {"container": "lab06-shard1", "port": int(os.environ.get("SHARD1_PORT", "3314"))},
}

SEED_ROWS = int(os.environ.get("SEED_ROWS", "500000"))
N_USERS = int(os.environ.get("N_USERS", "2000"))

ROUTER_STATE = LAB_DIR / "router_state.json"
JOURNAL = LAB_DIR / "journal.jsonl"          # traffic's acked-write ground truth
REPAIR_QUEUE = LAB_DIR / "repair_queue.jsonl"  # failed non-authoritative legs
SHADOW_DIFFS = LAB_DIR / "shadow_diffs.jsonl"  # read-path mismatches
BACKFILL_STATE = LAB_DIR / "backfill_state.json"
INJECT_STATE = LAB_DIR / "inject_state.json"

MODES = ["single", "double-write", "shadow-read", "sharded"]

STATUSES = ["pending", "paid", "shipped", "cancelled"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab06")


# ------------------------------------------------------------------- sharding

def shard_for(user_id: int) -> str:
    """crc32 of the DECIMAL STRING of user_id, mod N_SHARDS.

    The string, not the integer bytes, because MySQL's CRC32() coerces its
    argument to a string — keeping the two implementations identical lets the
    verifier push the misplacement scan and partition filters into SQL
    (`CRC32(user_id) % 2`) instead of dragging every row into Python.
    bootstrap.py asserts the two functions agree before anything else runs.
    """
    return SHARDS[zlib.crc32(str(user_id).encode()) % N_SHARDS]


def shard_filter_sql(shard: str) -> str:
    """SQL predicate selecting the rows a shard owns. MOD() instead of the %
    operator: pymysql would read a literal % as a format directive whenever
    the query also carries parameters."""
    return f"MOD(CRC32(user_id), {N_SHARDS}) = {SHARDS.index(shard)}"


# ---------------------------------------------------------------- connections

def connect(node: str, db: str | None = DB, timeout_s: float = 5.0) -> pymysql.Connection:
    """CLIENT.FOUND_ROWS: UPDATE rowcount must mean 'row matched', not 'values
    changed' — the traffic journal needs to know the row existed."""
    return pymysql.connect(
        host="127.0.0.1", port=NODES[node]["port"],
        user=MYSQL_USER, password=MYSQL_PASSWORD, database=db,
        autocommit=True, charset="utf8mb4", client_flag=CLIENT.FOUND_ROWS,
        connect_timeout=timeout_s, read_timeout=30, write_timeout=30,
    )


def wait_for_node(node: str, timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect(node, db=None, timeout_s=2.0).close()
            log.info("%s is up (port %d)", node, NODES[node]["port"])
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"{node} not reachable after {timeout_s}s")


# --------------------------------------------------------------- router state

def read_mode() -> str:
    if not ROUTER_STATE.exists():
        return "single"
    return json.loads(ROUTER_STATE.read_text())["mode"]


def write_mode(mode: str) -> None:
    """Atomic flip: temp file + rename. A reader never sees a torn state."""
    assert mode in MODES, mode
    tmp = ROUTER_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"mode": mode, "changed_at_ms": int(time.time() * 1000)}, indent=2))
    tmp.rename(ROUTER_STATE)
    log.info("router_state.json -> %s", mode)


# --------------------------------------------------------------------- router

UPSERT = (
    f"INSERT INTO {TABLE} (user_id, seq, status, amount, note) "
    "VALUES (%s, %s, %s, %s, %s) AS new "
    "ON DUPLICATE KEY UPDATE status=new.status, amount=new.amount, note=new.note"
)
INSERT = f"INSERT INTO {TABLE} (user_id, seq, status, amount, note) VALUES (%s, %s, %s, %s, %s)"
UPDATE = f"UPDATE {TABLE} SET status=%s, amount=%s, note=%s WHERE user_id=%s AND seq=%s"


class Router:
    """execute-by-user_id with mode-dependent fan-out. NOT thread-safe: one
    router per process, ops in order. Per-key write ordering is what makes
    full-state upserts a valid repair mechanism (README §3.2)."""

    def __init__(self):
        self._conns: dict[str, pymysql.Connection] = {}
        self._mode_cache = ("", 0.0)
        self.repair_queued = 0
        self.shadow_reads = 0
        self.shadow_diffs = 0

    # -- plumbing -------------------------------------------------------------

    def mode(self) -> str:
        """Re-read at most every 100ms; a flip propagates within one op or two."""
        mode, ts = self._mode_cache
        if time.time() - ts > 0.1:
            mode = read_mode()
            self._mode_cache = (mode, time.time())
        return mode

    def conn(self, node: str) -> pymysql.Connection:
        c = self._conns.get(node)
        if c is not None:
            try:
                c.ping(reconnect=True)
                return c
            except Exception:
                pass
        c = connect(node)
        self._conns[node] = c
        return c

    def _exec(self, node: str, sql: str, params: tuple) -> int:
        with self.conn(node).cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def _best_effort_upsert(self, node: str, user_id: int, seq: int,
                            status: str, amount: float, note: str) -> None:
        """A non-authoritative leg: failure queues a repair, never fails the op."""
        try:
            self._exec(node, UPSERT, (user_id, seq, status, amount, note))
        except Exception as e:
            self.repair_queued += 1
            with open(REPAIR_QUEUE, "a") as f:
                f.write(json.dumps({
                    "target": node, "user_id": user_id, "seq": seq,
                    "status": status, "amount": amount, "note": note,
                    "ts": int(time.time() * 1000), "error": str(e)[:200],
                }) + "\n")
            self._conns.pop(node, None)
            log.warning("non-authoritative write to %s failed (%s) — queued for repair", node, e)

    # -- writes ---------------------------------------------------------------

    def write(self, op: str, user_id: int, seq: int, status: str, amount: float, note: str) -> int:
        """op is 'insert' or 'update'. Returns authoritative rowcount (update:
        0 means the row is MISSING on the authoritative node — the app just
        detected data loss). Raises on authoritative failure: only the
        authoritative leg may fail the request."""
        mode = self.mode()
        auth_sql = INSERT if op == "insert" else UPDATE
        auth_params = ((user_id, seq, status, amount, note) if op == "insert"
                       else (status, amount, note, user_id, seq))
        shard = shard_for(user_id)

        if mode == "single":
            return self._exec("mono", auth_sql, auth_params)

        if mode in ("double-write", "shadow-read"):
            rc = self._exec("mono", auth_sql, auth_params)  # authoritative first
            self._best_effort_upsert(shard, user_id, seq, status, amount, note)
            return rc

        # sharded: roles swap symmetrically — shard authoritative, mono mirrored.
        rc = self._exec(shard, auth_sql, auth_params)
        self._best_effort_upsert("mono", user_id, seq, status, amount, note)
        return rc

    # -- reads ----------------------------------------------------------------

    def _read_state(self, node: str, user_id: int) -> tuple[int, int]:
        with self.conn(node).cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM {TABLE} WHERE user_id=%s",
                (user_id,))
            count, max_seq = cur.fetchone()
            return int(count), int(max_seq)

    def _read_order(self, node: str, user_id: int, seq: int) -> tuple | None:
        with self.conn(node).cursor() as cur:
            cur.execute(
                f"SELECT status, amount, note FROM {TABLE} WHERE user_id=%s AND seq=%s",
                (user_id, seq))
            return cur.fetchone()

    def _shadow(self, kind: str, user_id: int, served, shadow) -> None:
        self.shadow_reads += 1
        if served != shadow:
            self.shadow_diffs += 1
            with open(SHADOW_DIFFS, "a") as f:
                f.write(json.dumps({
                    "kind": kind, "user_id": user_id, "shard": shard_for(user_id),
                    "served": repr(served), "shadow": repr(shadow),
                    "ts": int(time.time() * 1000),
                }) + "\n")
            log.warning("SHADOW DIFF %s user=%d served=%r shard=%r", kind, user_id, served, shadow)

    def read_state(self, user_id: int) -> tuple[int, int]:
        """(count, max_seq) for a user — the traffic generator's invariant probe."""
        mode = self.mode()
        if mode == "sharded":
            return self._read_state(shard_for(user_id), user_id)
        served = self._read_state("mono", user_id)
        if mode == "shadow-read":
            self._shadow("state", user_id, served, self._read_state(shard_for(user_id), user_id))
        return served

    def read_order(self, user_id: int, seq: int) -> tuple | None:
        mode = self.mode()
        if mode == "sharded":
            return self._read_order(shard_for(user_id), user_id, seq)
        served = self._read_order("mono", user_id, seq)
        if mode == "shadow-read":
            self._shadow("order", user_id, served, self._read_order(shard_for(user_id), user_id, seq))
        return served

    def close(self) -> None:
        for c in self._conns.values():
            try:
                c.close()
            except Exception:
                pass
        self._conns.clear()


# -------------------------------------------------------------------- helpers

def make_values(rng) -> tuple[str, float, str]:
    return (rng.choice(STATUSES),
            round(rng.uniform(1.0, 500.0), 2),
            f"order note {rng.randrange(1, 100_000)}")


def replay_repair_queue(targets: set[str] | None = None) -> int:
    """Upsert every queued full-state row onto its target node. Idempotent
    (upserts), safe to run repeatedly. Returns rows replayed. Entries for
    other targets are kept."""
    if not REPAIR_QUEUE.exists():
        return 0
    entries = [json.loads(line) for line in REPAIR_QUEUE.read_text().splitlines() if line]
    keep, replayed = [], 0
    conns: dict[str, pymysql.Connection] = {}
    try:
        for e in entries:
            if targets is not None and e["target"] not in targets:
                keep.append(e)
                continue
            conn = conns.setdefault(e["target"], connect(e["target"]))
            with conn.cursor() as cur:
                cur.execute(UPSERT, (e["user_id"], e["seq"], e["status"], e["amount"], e["note"]))
            replayed += 1
    finally:
        for c in conns.values():
            c.close()
    tmp = REPAIR_QUEUE.with_suffix(".jsonl.tmp")
    tmp.write_text("".join(json.dumps(e) + "\n" for e in keep))
    tmp.rename(REPAIR_QUEUE)
    if replayed:
        log.info("repair queue: replayed %d rows (%d kept for other targets)", replayed, len(keep))
    return replayed
