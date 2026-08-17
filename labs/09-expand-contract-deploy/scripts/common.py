"""Shared config + deploy plumbing for the expand/contract lab.

The moving parts, and who owns them:

    state/<pod>.version       what version a pod BOOTS as (deploy.py writes,
                              app reads once at container start)
    state/nginx/upstreams.conf  which pods RECEIVE traffic (deploy.py writes,
                              nginx re-reads on `nginx -s reload`)
    state/deploy_state.json   the deploy system's own memory: active color +
                              per-pod versions (atomic rename, lab-04 pattern)
    client_journal.jsonl      the client's anomaly log — the ground truth every
                              drill is graded against
    client_state.json         the client's final acked-writes snapshot, dumped
                              on shutdown; verify.py audits the DB against it

Everything the "platform" does is one of two verbs: write a file, or restart
a container. That is deliberate — a deploy system is a state machine over
boring primitives, and keeping the primitives boring is what makes the
failure modes legible.
"""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

import pymysql

LAB_DIR = Path(__file__).resolve().parent.parent
STATE_DIR = LAB_DIR / "state"
NGINX_DIR = STATE_DIR / "nginx"

MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3311"))
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "8080"))
GATEWAY_URL = f"http://127.0.0.1:{GATEWAY_PORT}"

DB = "lab09"
TABLE = "users"

SEED_ROWS = int(os.environ.get("SEED_ROWS", "200000"))

# Pods: service name -> management port (host) for health/version probes.
# Traffic NEVER uses these ports; only deploy.py and verify.py do.
PODS = {
    "app-blue-a":  {"port": int(os.environ.get("BLUE_A_PORT", "18081")), "color": "blue"},
    "app-blue-b":  {"port": int(os.environ.get("BLUE_B_PORT", "18082")), "color": "blue"},
    "app-green-a": {"port": int(os.environ.get("GREEN_A_PORT", "18083")), "color": "green"},
    "app-green-b": {"port": int(os.environ.get("GREEN_B_PORT", "18084")), "color": "green"},
}
COLORS = {"blue": ["app-blue-a", "app-blue-b"], "green": ["app-green-a", "app-green-b"]}

DEPLOY_STATE = STATE_DIR / "deploy_state.json"
UPSTREAMS_CONF = NGINX_DIR / "upstreams.conf"
CLIENT_JOURNAL = LAB_DIR / "client_journal.jsonl"
CLIENT_STATE = LAB_DIR / "client_state.json"
BACKFILL_STATE = LAB_DIR / "backfill_state.json"

VERSIONS = ["v1", "v1.5", "v2", "v3"]


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


# ------------------------------------------------------------------ name shape

def split_name(name: str) -> tuple[str, str]:
    """MUST match app.py's split and the SQL in backfill.py. bootstrap.py
    asserts the Python/SQL pair agree before anything runs."""
    parts = name.rsplit(" ", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (name, "")


def compose_name(first: str | None, last: str | None) -> str | None:
    if first is None and last is None:
        return None
    return " ".join(p for p in (first, last) if p)


# SQL twin of split_name(): last = after the LAST space; no space -> last=''.
SQL_FIRST = ("IF(LOCATE(' ', name) = 0, name, "
             "LEFT(name, CHAR_LENGTH(name) - CHAR_LENGTH(SUBSTRING_INDEX(name, ' ', -1)) - 1))")
SQL_LAST = "IF(LOCATE(' ', name) = 0, '', SUBSTRING_INDEX(name, ' ', -1))"


# ----------------------------------------------------------------------- mysql

def connect(db: str | None = DB, timeout_s: float = 5.0) -> pymysql.Connection:
    return pymysql.connect(host="127.0.0.1", port=MYSQL_PORT, user="root", password="lab",
                           database=db, autocommit=True, charset="utf8mb4",
                           connect_timeout=timeout_s, read_timeout=60, write_timeout=60)


def wait_for_mysql(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            connect(db=None, timeout_s=2.0).close()
            log(f"mysql is up (port {MYSQL_PORT})")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"mysql not reachable after {timeout_s}s")


def columns() -> set[str]:
    with connect().cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s", (DB, TABLE))
        return {r[0] for r in cur.fetchall()}


# ---------------------------------------------------------------- deploy state

def read_deploy_state() -> dict:
    if not DEPLOY_STATE.exists():
        return {"active_color": "blue", "versions": {p: "v1" for p in PODS}}
    return json.loads(DEPLOY_STATE.read_text())


def write_deploy_state(state: dict) -> None:
    """Atomic: temp + rename. A crashed deploy never leaves a torn state file."""
    STATE_DIR.mkdir(exist_ok=True)
    tmp = DEPLOY_STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2) + "\n")
    tmp.rename(DEPLOY_STATE)


def pod_version_file(pod: str) -> Path:
    return STATE_DIR / f"{pod}.version"


def write_pod_version(pod: str, version: str) -> None:
    assert version in VERSIONS, version
    STATE_DIR.mkdir(exist_ok=True)
    tmp = pod_version_file(pod).with_suffix(".version.tmp")
    tmp.write_text(version + "\n")
    tmp.rename(pod_version_file(pod))


# ---------------------------------------------------------------- nginx switch

def write_upstreams(pods: list[str]) -> None:
    """Generate the upstream block. Rename INSIDE the bind-mounted directory so
    nginx (which sees the directory, not a pinned inode) reads a complete file."""
    assert pods, "refusing to write an empty upstream set"
    NGINX_DIR.mkdir(parents=True, exist_ok=True)
    body = "upstream app {\n"
    for pod in pods:
        body += f"    server {pod}:8000 max_fails=0;\n"
    body += "}\n"
    tmp = NGINX_DIR / ".upstreams.conf.tmp"
    tmp.write_text(body)
    tmp.rename(UPSTREAMS_CONF)


def in_lb() -> list[str]:
    if not UPSTREAMS_CONF.exists():
        return []
    return [line.split()[1].split(":")[0]
            for line in UPSTREAMS_CONF.read_text().splitlines()
            if line.strip().startswith("server ")]


def reload_gateway() -> None:
    compose("exec", "-T", "gateway", "nginx", "-s", "reload")


# --------------------------------------------------------------------- compose

def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """All compose calls carry --profile green so the green pair is always
    addressable (profiles hide services from commands that don't name them)."""
    return subprocess.run(["docker", "compose", "--profile", "green", *args],
                          cwd=LAB_DIR, check=check, capture_output=True, text=True)


# ---------------------------------------------------------------------- probes

def probe_pod(pod: str, timeout_s: float = 2.0) -> str | None:
    """The version a pod is ACTUALLY serving, or None if it isn't answering.
    Asks the pod itself — never trust the state file for what's running."""
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{PODS[pod]['port']}/healthz", timeout=timeout_s) as r:
            return json.loads(r.read())["version"]
    except Exception:
        return None


def wait_pod_version(pod: str, version: str, timeout_s: float = 60.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if probe_pod(pod) == version:
            return
        time.sleep(0.5)
    raise RuntimeError(f"{pod} did not come up as {version} within {timeout_s}s "
                       f"(currently: {probe_pod(pod)})")
