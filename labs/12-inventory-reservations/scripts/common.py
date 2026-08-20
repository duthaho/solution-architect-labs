"""Shared configuration and helpers for lab 12 (inventory reservations)."""

import json
import logging
import os
import time
from pathlib import Path

import pymysql
import redis as redis_lib

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3318"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = "lab12"

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6390"))

ITEM_ID = 1
CAPACITY = int(os.environ.get("CAPACITY", "50"))
WORKERS = int(os.environ.get("WORKERS", "8"))
ROUNDS = int(os.environ.get("ROUNDS", "12"))
TTL_S = int(os.environ.get("TTL_S", "120"))

MODES = ("naive", "a", "b", "c", "legacy")
PHASES = ("redis", "shadow", "mysql")

SEED_PATH = LAB_DIR / "seed.json"
PHASE_PATH = LAB_DIR / "phase.txt"
REDIS_KEY = f"lab12:item:{ITEM_ID}:remaining"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab12")


def connect(db: str | None = DB, autocommit: bool = False) -> pymysql.Connection:
    """MySQL connection; autocommit off by default — transactions are explicit."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=db,
        autocommit=autocommit,
        charset="utf8mb4",
    )


def redis_client() -> redis_lib.Redis:
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


def journal_path(mode: str) -> Path:
    return LAB_DIR / f"burst_{mode}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def read_seed() -> dict:
    return json.loads(SEED_PATH.read_text())


def read_phase() -> str:
    return PHASE_PATH.read_text().strip() if PHASE_PATH.exists() else "mysql"


def write_phase(phase: str) -> None:
    assert phase in PHASES, phase
    PHASE_PATH.write_text(phase + "\n")


def item_row(conn) -> tuple[int, int, int]:
    """Return (capacity, reserved, sold) for the hot item."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT capacity, reserved, sold FROM items WHERE id = %s", (ITEM_ID,)
        )
        return cur.fetchone()


def percentiles(samples_ms: list[float]) -> tuple[float, float]:
    if not samples_ms:
        return 0.0, 0.0
    xs = sorted(samples_ms)
    p50 = xs[int(0.50 * (len(xs) - 1))]
    p95 = xs[int(0.95 * (len(xs) - 1))]
    return p50, p95
