"""Shared configuration and helpers for the online migration lab."""
import logging
import os
import random
import time
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = os.environ.get("MYSQL_DB", "lab02")

TABLE = "orders"                    # the live table the app talks to
GHOST_TABLE = "_orders_gst"         # new-schema shadow table being built
OLD_TABLE = "_orders_old"           # where v1 is parked after cutover
NEW_PARKED_TABLE = "_orders_new"    # where v2 is parked after a rollback
CHANGELOG_TABLE = "_migration_changelog"  # heartbeats + cutover markers (gh-ost's *_ghc)

SEED_ROWS = int(os.environ.get("SEED_ROWS", "500000"))

JOURNAL = LAB_DIR / "journal.jsonl"
STATE_FILE = LAB_DIR / "migration_state.json"

STATUSES = ["pending", "paid", "shipped", "cancelled"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab02")
# mysql-replication logs its full connection settings (password included) at
# INFO. No thanks.
logging.getLogger("pymysqlreplication").setLevel(logging.WARNING)


def connect(db: str | None = DB, autocommit: bool = True) -> pymysql.Connection:
    """New connection. CLIENT.FOUND_ROWS makes UPDATE rowcount mean 'rows
    matched' instead of 'rows changed' — the traffic journal needs to know
    whether the row existed, not whether the values differed."""
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, autocommit=autocommit, client_flag=CLIENT.FOUND_ROWS,
        charset="utf8mb4",
    )


def connection_settings() -> dict:
    """Settings dict in the shape mysql-replication expects."""
    return {"host": MYSQL_HOST, "port": MYSQL_PORT, "user": MYSQL_USER, "passwd": MYSQL_PASSWORD}


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            conn = connect(db=None)
            conn.close()
            log.info("MySQL is up at %s:%d", MYSQL_HOST, MYSQL_PORT)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable at {MYSQL_HOST}:{MYSQL_PORT} after {timeout_s}s")


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (DB, table),
        )
        return cur.fetchone()[0] > 0


def table_columns(conn, table: str) -> list[str]:
    """Column names in ordinal order."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (DB, table),
        )
        return [r[0] for r in cur.fetchall()]


def binlog_position(conn) -> tuple[str, int]:
    with conn.cursor() as cur:
        cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Binary logging is not enabled (SHOW MASTER STATUS empty)")
        return row[0], int(row[1])


def now_millis() -> int:
    return int(time.time() * 1000)


def make_order(rng: random.Random) -> dict:
    return {
        "customer_id": rng.randrange(1, 50_000),
        "status": rng.choice(STATUSES),
        "amount": round(rng.uniform(1.0, 500.0), 2),
        "note": f"order note {rng.randrange(1, 100_000)}",
    }
