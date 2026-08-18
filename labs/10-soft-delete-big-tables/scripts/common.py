"""Shared configuration and helpers for the soft-delete lab."""
import json
import logging
import os
import random
import statistics
import time
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3316"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")

SEED_ROWS = int(os.environ.get("SEED_ROWS", "500000"))  # orders per family
SEED = int(os.environ.get("SEED", "10"))

# Parent-first order; reverse it for child-first deletes.
TABLES = ["users", "orders", "order_items"]

# One isolated schema family per strategy: identical seed data, so every
# measurement compares like with like and strategies never contaminate each
# other's state.
FAMILIES = {
    "a": {"live": "lab10_a"},
    "b": {"live": "lab10_b", "deleted": "lab10_b_deleted"},
    "c": {"live": "lab10_c", "archive": "lab10_c_archive"},
}

STATUSES = ["pending", "paid", "shipped", "cancelled"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab10")


def connect(db: str | None = None, autocommit: bool = True) -> pymysql.Connection:
    """New connection. CLIENT.FOUND_ROWS makes UPDATE rowcount mean 'rows
    matched' instead of 'rows changed' — the journal needs to know whether the
    row existed, not whether the values differed."""
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, autocommit=autocommit, client_flag=CLIENT.FOUND_ROWS,
        charset="utf8mb4",
    )


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect().close()
            log.info("MySQL is up at %s:%d", MYSQL_HOST, MYSQL_PORT)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable at {MYSQL_HOST}:{MYSQL_PORT} after {timeout_s}s")


def journal_path(family: str) -> Path:
    return LAB_DIR / f"traffic_{family}.jsonl"


def outcome_path(family: str) -> Path:
    return LAB_DIR / f"outcome_{family}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def now_millis() -> int:
    return int(time.time() * 1000)


def table_bytes(conn, schema: str, table: str) -> tuple[int, int]:
    """(data_bytes, index_bytes) from information_schema. Call
    ANALYZE TABLE first if you need fresh numbers."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data_length, index_length FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
        row = cur.fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)


def count(conn, schema: str, table: str, where: str = "1=1") -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE {where}")
        return cur.fetchone()[0]


def percentiles(samples_ms: list[float]) -> tuple[float, float]:
    """(p50, p95) in milliseconds."""
    if not samples_ms:
        return (0.0, 0.0)
    qs = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return (qs[49], qs[94])


# ------------------------------------------------------------------ seed data

def make_user(rng: random.Random, i: int) -> tuple:
    return (f"user{i}@example.com", f"user {rng.randrange(1, 10**6)}")


def make_order(rng: random.Random, user_id: int) -> tuple:
    return (user_id, rng.choice(STATUSES), round(rng.uniform(1.0, 500.0), 2))


def make_item(rng: random.Random, order_id: int) -> tuple:
    return (order_id, f"SKU-{rng.randrange(1, 20_000)}", rng.randrange(1, 5),
            round(rng.uniform(1.0, 200.0), 2))
