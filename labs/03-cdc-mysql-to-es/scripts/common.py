"""Shared configuration and helpers for the CDC lab."""
import json
import logging
import os
import random
import time
from pathlib import Path

import pymysql
from elasticsearch import Elasticsearch
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = os.environ.get("MYSQL_DB", "lab03")
TABLE = "orders"

ES_URL = os.environ.get("ES_URL", f"http://localhost:{os.environ.get('ES_PORT', '9201')}")
ES_INDEX = "orders"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", f"localhost:{os.environ.get('KAFKA_PORT', '29092')}")
CONNECT_URL = os.environ.get("CONNECT_URL", f"http://localhost:{os.environ.get('CONNECT_PORT', '8083')}")
TOPIC = "lab03.lab03.orders"          # <topic.prefix>.<database>.<table>
CONSUMER_GROUP = "es-projector"

SEED_ROWS = int(os.environ.get("SEED_ROWS", "200000"))
JOURNAL = LAB_DIR / "journal.jsonl"

STATUSES = ["pending", "paid", "shipped", "cancelled"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab03")
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


def connect_mysql(db: str | None = DB, autocommit: bool = True) -> pymysql.Connection:
    """CLIENT.FOUND_ROWS: UPDATE rowcount means 'rows matched' — the journal
    needs to know whether the row existed, not whether values changed."""
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=db, autocommit=autocommit, client_flag=CLIENT.FOUND_ROWS,
        charset="utf8mb4",
    )


def es_client(timeout: int = 60) -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=timeout, retry_on_timeout=True, max_retries=3)


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect_mysql(db=None).close()
            log.info("MySQL is up at %s:%d", MYSQL_HOST, MYSQL_PORT)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"MySQL not reachable after {timeout_s}s")


def wait_for_es(es: Elasticsearch, timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            es.cluster.health(wait_for_status="yellow", timeout="5s")
            log.info("Elasticsearch is up at %s", ES_URL)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Elasticsearch not reachable at {ES_URL} after {timeout_s}s")


def load_mapping() -> dict:
    with open(LAB_DIR / "mappings" / "orders.json") as f:
        return json.load(f)


def make_order(rng: random.Random) -> dict:
    return {
        "customer_id": rng.randrange(1, 50_000),
        "status": rng.choice(STATUSES),
        "amount": round(rng.uniform(1.0, 500.0), 2),
        "note": f"order note {rng.randrange(1, 100_000)}",
    }


def row_to_doc(after: dict) -> dict:
    """Debezium envelope `after` image -> ES document.

    Two type traps handled here, both README §3.5 material:
    - DECIMAL arrives as a *string* because the connector is configured with
      decimal.handling.mode=string (the default, 'precise', would give you
      base64-encoded unscaled bytes — unreadable without the schema).
    - DATETIME(3) arrives as epoch millis (io.debezium.time.Timestamp).
    """
    return {
        "id": after["id"],
        "customer_id": after["customer_id"],
        "status": after["status"],
        "amount": float(after["amount"]),
        "note": after["note"],
        "created_at": after["created_at"],
        "updated_at": after["updated_at"],
    }
