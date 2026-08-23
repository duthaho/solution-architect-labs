"""Shared configuration and helpers for lab 14 (top-K heavy hitters)."""

import bisect
import json
import logging
import os
import random
import time
from pathlib import Path

import pymysql
from pymysql.constants import CLIENT

LAB_DIR = Path(__file__).resolve().parent.parent

MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3320"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "lab")
DB = "lab14"

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6392"))

SEED = int(os.environ.get("SEED", "14"))
N_KEYS = int(os.environ.get("N_KEYS", "1000000"))
N_EVENTS = int(os.environ.get("N_EVENTS", "1000000"))
ZIPF_S = float(os.environ.get("ZIPF_S", "1.1"))
TOP_K = int(os.environ.get("TOP_K", "100"))
MINUTES = int(os.environ.get("MINUTES", "60"))

# Sketch hash parameters derive from this, never from SEED: the stream and
# the sketches must be independently reproducible.
SKETCH_SEED = int(os.environ.get("SKETCH_SEED", "1414"))

MERSENNE_P = (1 << 61) - 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab14")


def connect(db: str | None = DB, autocommit: bool = False) -> pymysql.Connection:
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


def journal_path(name: str) -> Path:
    return LAB_DIR / f"{name}.jsonl"


def append_jsonl(path: Path, record: dict) -> None:
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def percentiles(samples: list[float]) -> tuple[float, float]:
    if not samples:
        return 0.0, 0.0
    xs = sorted(samples)
    p50 = xs[int(0.50 * (len(xs) - 1))]
    p95 = xs[int(0.95 * (len(xs) - 1))]
    return p50, p95


def product_id(rank: int) -> int:
    """Map a popularity rank to a stable, non-sequential product id.

    Multiplication by an odd constant mod 2^32 is a bijection, so ids are
    unique; the scramble stops sketch hashes from seeing sequential keys.
    """
    return (rank * 2654435761) & 0xFFFFFFFF


def zipf_cdf(n_keys: int = N_KEYS, s: float = ZIPF_S) -> list[float]:
    cum: list[float] = []
    total = 0.0
    for rank in range(1, n_keys + 1):
        total += rank**-s
        cum.append(total)
    return cum


def zipf_stream(
    n_events: int = N_EVENTS,
    n_keys: int = N_KEYS,
    s: float = ZIPF_S,
    seed: int = SEED,
):
    """Yield n_events product ids, Zipf(s)-distributed over n_keys ranks."""
    cum = zipf_cdf(n_keys, s)
    total = cum[-1]
    rng = random.Random(seed)
    for _ in range(n_events):
        rank = bisect.bisect_left(cum, rng.random() * total)
        yield product_id(rank + 1)


def minute_of(event_index: int, n_events: int = N_EVENTS, minutes: int = MINUTES) -> int:
    return min(event_index * minutes // n_events, minutes - 1)


def hash_params(depth: int, seed: int = SKETCH_SEED) -> list[tuple[int, int]]:
    """Fixed (a, b) pairs for multiply-shift hashing mod 2^61-1."""
    rng = random.Random(seed)
    return [
        (rng.randrange(1, MERSENNE_P), rng.randrange(0, MERSENNE_P))
        for _ in range(depth)
    ]


def row_index(key: int, a: int, b: int, width: int) -> int:
    return ((a * key + b) % MERSENNE_P) % width
