"""Shared configuration and helpers for the idempotency lab."""
import json
import logging
import os
import subprocess
import time
from pathlib import Path

import pymysql

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3310"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = os.environ.get("MYSQL_DB", "lab08")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", f"localhost:{os.environ.get('KAFKA_PORT', '9096')}")
TOPIC = "payments"
DLQ_TOPIC = "payments.dlq"
PARTITIONS = 3
CONSUMER_GROUP = "balance-consumer"

ACCOUNTS = int(os.environ.get("ACCOUNTS", "50"))
SEED_BALANCE_CENTS = 1_000_00  # every account starts at $1000.00

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab08")


def connect_mysql(db: str | None = DB, autocommit: bool = True) -> pymysql.Connection:
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, autocommit=autocommit, charset="utf8mb4",
    )


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect_mysql(db=None).close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable after {timeout_s}s")


def event_bytes(event_id: str, account_id: int, amount_cents) -> bytes:
    """The wire format. amount_cents is typed loosely on purpose: the poison
    pill (inject.py) ships a string here and the schema won't stop it —
    exactly how malformed events reach real topics."""
    return json.dumps({
        "event_id": event_id,
        "account_id": account_id,
        "amount_cents": amount_cents,
    }).encode()


def kafka_cli(*args: str) -> str:
    """Run a Kafka CLI tool inside the broker container (no host install)."""
    out = subprocess.run(
        ["docker", "exec", "lab08-kafka", *args],
        check=True, capture_output=True, text=True)
    return out.stdout


def producer_conf() -> dict:
    """acks=all + idempotent producer: the broker dedupes *transport* retries
    (same session, same sequence number). What it can NOT dedupe is the
    application retrying send() with a fresh call after a timeout — that is a
    new record as far as Kafka is concerned. --retry-storm simulates exactly
    those, which is why dedupe must exist downstream anyway."""
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    }
