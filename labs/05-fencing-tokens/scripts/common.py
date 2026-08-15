"""Shared helpers for the fencing-tokens lab.

The lock
--------
A single Redis instance. Acquire = `SET lock <owner> NX PX <ttl>`. The fencing
token is `INCR lock:fence`, and issuance MUST be atomic with acquisition (one
Lua script): if they were two round-trips, a client could acquire, pause before
the INCR, and a later owner would grab a *lower* token than an earlier one —
the monotonicity that fencing depends on would be gone before we even start.

Runs both on the host (drills, verify — redis at 127.0.0.1:6380, storage at
127.0.0.1:8091) and inside worker containers (compose sets REDIS_URL /
STORAGE_URL to the in-network addresses).
"""
import json
import logging
import os
import subprocess
import time
from pathlib import Path

import redis as redislib
import requests

LAB_DIR = Path(__file__).resolve().parent.parent

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6380/0")
STORAGE_URL = os.environ.get("STORAGE_URL", "http://127.0.0.1:8091")

LOCK_KEY = "lab05:lock"
FENCE_KEY = "lab05:lock:fence"

RESULTS = LAB_DIR / "results.jsonl"

WORKERS = {
    "A": {"container": "lab05-worker-a"},
    "B": {"container": "lab05-worker-b"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lab05")


# ------------------------------------------------------------------- the lock

# Returns the fencing token (int) on success, false/nil on contention.
ACQUIRE_LUA = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return false
"""

# Compare-owner-then-delete: never delete a lock someone else now holds.
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class Lock:
    """TTL lock + fencing token, both from Redis."""

    def __init__(self, r: redislib.Redis):
        self.r = r
        self._acquire = r.register_script(ACQUIRE_LUA)
        self._release = r.register_script(RELEASE_LUA)

    def acquire(self, owner: str, ttl_ms: int) -> int | None:
        """One attempt. Returns the fencing token, or None if held by someone else."""
        token = self._acquire(keys=[LOCK_KEY, FENCE_KEY], args=[owner, ttl_ms])
        return int(token) if token else None

    def release(self, owner: str) -> bool:
        return bool(self._release(keys=[LOCK_KEY], args=[owner]))

    def holder(self) -> str | None:
        v = self.r.get(LOCK_KEY)
        return v.decode() if v else None


def redis_client() -> redislib.Redis:
    return redislib.Redis.from_url(REDIS_URL, socket_timeout=5)


# -------------------------------------------------------------------- storage

def ledger() -> dict:
    """{'fencing': bool, 'max_token': int, 'entries': [...], 'rejected': [...]}"""
    return requests.get(f"{STORAGE_URL}/ledger", timeout=5).json()


def append_entry(owner: str, token: int, seq: int) -> requests.Response:
    return requests.post(f"{STORAGE_URL}/append", timeout=5,
                         json={"owner": owner, "token": token, "seq": seq})


# --------------------------------------------------------------------- docker

def docker(*args: str, check: bool = True) -> str:
    res = subprocess.run(["docker", *args], check=check,
                         capture_output=True, text=True)
    return res.stdout.strip()


def compose(*args: str, env: dict | None = None) -> None:
    full_env = {**os.environ, **(env or {})}
    subprocess.run(["docker", "compose", *args], check=True,
                   cwd=LAB_DIR, env=full_env)


def worker_logs(worker: str) -> str:
    return docker("logs", WORKERS[worker]["container"], check=False)


def now_ms() -> int:
    return int(time.time() * 1000)


# -------------------------------------------------------------------- results

def record_result(**fields) -> None:
    with RESULTS.open("a") as f:
        f.write(json.dumps(fields) + "\n")
