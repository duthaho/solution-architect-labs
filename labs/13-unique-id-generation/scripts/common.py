"""Shared configuration and helpers for lab 13 (unique ID generation)."""

import json
import logging
import os
import time
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3319"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = "lab13"

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6391"))

WORKER_SLOTS = int(os.environ.get("WORKER_SLOTS", "16"))
LEASE_TTL_MS = int(os.environ.get("LEASE_TTL_MS", "3000"))
LEASE_MARGIN_MS = int(os.environ.get("LEASE_MARGIN_MS", "500"))

LEASE_EVENTS_PATH = LAB_DIR / "lease_events.jsonl"
CLOCK_PATH = LAB_DIR / "clock.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab13")


def connect(db: str | None = DB, autocommit: bool = False) -> pymysql.Connection:
    """MySQL connection; autocommit off by default — transactions are explicit.

    CLIENT.FOUND_ROWS makes UPDATE rowcount mean rows *matched*, not rows
    *changed* — without it, a lease heartbeat that lands in the same
    millisecond as the previous renewal (values unchanged) would read
    rowcount 0 and falsely report the lease lost.
    """
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        autocommit=autocommit,
        charset="utf8mb4",
        client_flag=CLIENT.FOUND_ROWS,
    )


def redis_client():
    import redis as redis_lib

    return redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            connect(db=None).close()
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)


def ids_journal_path(name: str) -> Path:
    return LAB_DIR / f"ids_{name}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def percentiles(samples_ms: list[float]) -> tuple[float, float]:
    if not samples_ms:
        return 0.0, 0.0
    xs = sorted(samples_ms)
    p50 = xs[int(0.50 * (len(xs) - 1))]
    p95 = xs[int(0.95 * (len(xs) - 1))]
    return p50, p95
