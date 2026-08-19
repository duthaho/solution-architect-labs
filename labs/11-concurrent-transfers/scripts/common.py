"""Shared configuration and helpers for the concurrent-transfers lab."""
import json
import logging
import os
import statistics
import time
from decimal import Decimal
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3317"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = "lab11"

ACCOUNTS = int(os.environ.get("ACCOUNTS", "10"))
BALANCE = Decimal(os.environ.get("BALANCE", "1000.00"))

MODES = ("naive", "a", "b", "c", "d")

SEED_PATH = LAB_DIR / "seed.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab11")


def connect(db: str | None = DB, autocommit: bool = False) -> pymysql.Connection:
    """New connection. autocommit=False: this lab is about transactions, so
    every script owns its BEGIN/COMMIT explicitly. CLIENT.FOUND_ROWS is left
    OFF on purpose: strategy b/c decide success by 'rows CHANGED', which is
    exactly rowcount's default meaning."""
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, autocommit=autocommit, charset="utf8mb4",
    )


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect(db=None).close()
            log.info("MySQL is up at %s:%d", MYSQL_HOST, MYSQL_PORT)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable at {MYSQL_HOST}:{MYSQL_PORT} after {timeout_s}s")


def journal_path(mode: str) -> Path:
    return LAB_DIR / f"race_{mode}.jsonl"


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


def read_seed() -> dict:
    with SEED_PATH.open() as f:
        return json.load(f)


def balance_table(mode: str) -> tuple[str, str]:
    """(table, id_column) holding balances for a mode. Strategies naive/a/b/c
    mutate accounts; strategy d's data model is the ledger, whose readable
    balance is balance_cache."""
    if mode == "d":
        return ("balance_cache", "account_id")
    return ("accounts", "id")


def read_balances(conn, mode: str) -> dict[int, Decimal]:
    table, idcol = balance_table(mode)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {idcol}, balance FROM {table} ORDER BY {idcol}")
        return {row[0]: Decimal(row[1]) for row in cur.fetchall()}


def sum_balances(conn, mode: str = "a") -> Decimal:
    table, _ = balance_table(mode)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(SUM(balance), 0) FROM {table}")
        return Decimal(cur.fetchone()[0])


def negative_accounts(conn, mode: str = "a") -> list[tuple[int, Decimal]]:
    table, idcol = balance_table(mode)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {idcol}, balance FROM {table} WHERE balance < 0")
        return [(r[0], Decimal(r[1])) for r in cur.fetchall()]


def percentiles(samples_ms: list[float]) -> tuple[float, float]:
    """(p50, p95) in milliseconds. Degrades gracefully for tiny samples."""
    if not samples_ms:
        return (0.0, 0.0)
    if len(samples_ms) < 2:
        return (samples_ms[0], samples_ms[0])
    qs = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return (qs[49], qs[94])
