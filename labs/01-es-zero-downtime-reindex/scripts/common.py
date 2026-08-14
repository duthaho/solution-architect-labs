"""Shared configuration and helpers for the reindex lab."""
import json
import logging
import os
import time
from pathlib import Path

from elasticsearch import Elasticsearch

LAB_DIR = Path(__file__).resolve().parent.parent

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
READ_ALIAS = os.environ.get("READ_ALIAS", "products-read")
WRITE_ALIAS = os.environ.get("WRITE_ALIAS", "products-write")
INDEX_V1 = os.environ.get("INDEX_V1", "products_v1")
INDEX_V2 = os.environ.get("INDEX_V2", "products_v2")
SEED_DOCS = int(os.environ.get("SEED_DOCS", "300000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab01")
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


def es_client(timeout: int = 120) -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=timeout, retry_on_timeout=True, max_retries=3)


def wait_for_es(es: Elasticsearch, timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            health = es.cluster.health(wait_for_status="yellow", timeout="5s")
            log.info("Elasticsearch is up (status=%s)", health["status"])
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"Elasticsearch not reachable at {ES_URL} after {timeout_s}s")


def load_index_body(version: str) -> dict:
    with open(LAB_DIR / "mappings" / f"{version}.json") as f:
        return json.load(f)


def now_millis() -> int:
    return int(time.time() * 1000)


def alias_target(es: Elasticsearch, alias: str) -> str:
    """Resolve an alias to its single concrete index."""
    indices = list(es.indices.get_alias(name=alias).keys())
    if len(indices) != 1:
        raise RuntimeError(f"Alias {alias} points to {indices}, expected exactly one index")
    return indices[0]
