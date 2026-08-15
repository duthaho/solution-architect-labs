"""Shared config + connections for the cache-consistency lab.

Everything the lab measures flows through two append-only journals:

    journal_writes.jsonl   every COMMITTED price, stamped after commit
    journal_reads.jsonl    every price SERVED to a reader, stamped at serve time

auditor.py joins them: a read is stale iff the price it served is not the
latest price committed before the read. Staleness is measured, never inferred
from the strategy's marketing claims.
"""
import json
import logging
import os
import time
from pathlib import Path

import pymysql
import redis as redis_lib

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3315"))
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6381"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", f"localhost:{os.environ.get('KAFKA_PORT', '29093')}")
CONNECT_URL = os.environ.get("CONNECT_URL", f"http://localhost:{os.environ.get('CONNECT_PORT', '8084')}")

DB = "lab07"
TABLE = "products"
N_PRODUCTS = int(os.environ.get("N_PRODUCTS", "1000"))

STRATEGIES = ["ttl", "delete", "versioned", "cdc"]

WRITE_JOURNAL = LAB_DIR / "journal_writes.jsonl"
READ_JOURNAL = LAB_DIR / "journal_reads.jsonl"
LAG_JOURNAL = LAB_DIR / "journal_cdc_lag.jsonl"   # invalidator: per-event pipeline lag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab07")


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------- connections

def connect_mysql(db: str | None = DB, timeout_s: float = 5.0) -> pymysql.Connection:
    return pymysql.connect(
        host="127.0.0.1", port=MYSQL_PORT, user="root", password="lab",
        database=db, autocommit=True, charset="utf8mb4",
        connect_timeout=timeout_s, read_timeout=30, write_timeout=30,
    )


def connect_redis() -> redis_lib.Redis:
    return redis_lib.Redis(host="127.0.0.1", port=REDIS_PORT, decode_responses=True)


def wait_for_infra(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    for name, probe in [("mysql", lambda: connect_mysql(db=None, timeout_s=2.0).close()),
                        ("redis", lambda: connect_redis().ping())]:
        while True:
            try:
                probe()
                log.info("%s is up", name)
                break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError(f"{name} not reachable after {timeout_s}s")
                time.sleep(2)


# ------------------------------------------------------------------- journals

def journal(path: Path, record: dict) -> None:
    """One JSON line, one os.write: O_APPEND keeps concurrent writers from
    interleaving partial lines, so the auditor never sees a torn record."""
    line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def read_journal(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]
