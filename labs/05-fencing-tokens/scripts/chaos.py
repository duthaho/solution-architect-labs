"""Chaos primitives: freeze/thaw workers, and event-gated waits.

The drills are deterministic because nothing is timed by guesswork — every
step waits for an OBSERVED condition (a ledger entry, a log line, the lock key
expiring) before proceeding. SIGSTOP/SIGCONT via `docker kill -s` freezes the
worker's PID 1, which is worker.py itself: a perfect, scriptable stand-in for
a stop-the-world GC pause, a VM migration stall, or a laptop lid closing.

Also usable manually:  python scripts/chaos.py pause A   /   resume A
"""
import sys
import time

from common import WORKERS, docker, ledger, log, redis_client, worker_logs, LOCK_KEY


def pause(worker: str) -> None:
    docker("kill", "-s", "STOP", WORKERS[worker]["container"])
    log.info("chaos: SIGSTOP %s (frozen mid-critical-section)",
             WORKERS[worker]["container"])


def resume(worker: str) -> None:
    docker("kill", "-s", "CONT", WORKERS[worker]["container"])
    log.info("chaos: SIGCONT %s (wakes up with stale beliefs)",
             WORKERS[worker]["container"])


def wait_for(desc: str, pred, timeout_s: float = 30.0, poll_s: float = 0.05):
    """Poll pred() until truthy. Returns its value. Raises on timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        val = pred()
        if val:
            log.info("chaos: observed %s", desc)
            return val
        time.sleep(poll_s)
    raise TimeoutError(f"timed out after {timeout_s}s waiting for: {desc}")


def wait_ledger_entries(owner: str, n: int, timeout_s: float = 30.0) -> None:
    wait_for(f"ledger has >= {n} entries from {owner}",
             lambda: sum(1 for e in ledger()["entries"] if e["owner"] == owner) >= n,
             timeout_s)


def wait_lock_expired(timeout_s: float = 30.0) -> None:
    r = redis_client()
    wait_for("lock TTL expired (key gone)",
             lambda: not r.exists(LOCK_KEY), timeout_s)


def wait_log_line(worker: str, needle: str, timeout_s: float = 30.0) -> None:
    wait_for(f"{worker} logged {needle!r}",
             lambda: needle in worker_logs(worker), timeout_s)


def wait_worker_exit(worker: str, timeout_s: float = 60.0) -> None:
    name = WORKERS[worker]["container"]
    wait_for(f"{worker} exited",
             lambda: docker("inspect", "-f", "{{.State.Running}}",
                            name, check=False) != "true",
             timeout_s, poll_s=0.2)


if __name__ == "__main__":
    cmd, worker = sys.argv[1], sys.argv[2].upper()
    {"pause": pause, "resume": resume}[cmd](worker)
