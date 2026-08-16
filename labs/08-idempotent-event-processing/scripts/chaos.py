"""Chaos supervisor: run the consumer, let it die, restart it. Forever.

This is what Kubernetes / systemd does for you in production — which is
exactly why crash-loops are not hypothetical: every restart re-reads the
uncommitted tail of the log. The supervisor makes the lab's crash schedule
survivable (drills 1-2) and makes the poison crash-loop visible (drill 4,
--max-restarts caps it so the drill can move on and show the stalled lag).

Exit codes: 0 = consumer drained cleanly; 2 = gave up (crash loop).
"""
import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import log

SCRIPTS = Path(__file__).resolve().parent
CHILD = None


def _terminate(*_):
    if CHILD is not None and CHILD.poll() is None:
        CHILD.terminate()
    sys.exit(0)


def main() -> None:
    global CHILD
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["naive", "idempotent"], required=True)
    ap.add_argument("--crash-every", type=int, default=0)
    ap.add_argument("--dlq", action="store_true")
    ap.add_argument("--max-restarts", type=int, default=100)
    ap.add_argument("--timeout", type=float, default=600, help="wall-clock guard (s)")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)

    cmd = [sys.executable, str(SCRIPTS / "consumer.py"), "--mode", args.mode]
    if args.crash_every:
        cmd += ["--crash-every", str(args.crash_every)]
    if args.dlq:
        cmd += ["--dlq"]

    restarts = 0
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        CHILD = subprocess.Popen(cmd)  # stdout/stderr inherit -> chaos.log
        rc = CHILD.wait()
        if rc == 0:
            log.info("Consumer drained cleanly after %d restart(s)", restarts)
            sys.exit(0)
        restarts += 1
        log.warning("Consumer died (rc=%d) — restart #%d", rc, restarts)
        if restarts >= args.max_restarts:
            log.error("CRASH LOOP: %d restarts, giving up. The group is parked — "
                      "run `make lag` to see the stalled partition.", restarts)
            sys.exit(2)
        time.sleep(0.5)
    log.error("Timeout after %.0fs", args.timeout)
    sys.exit(2)


if __name__ == "__main__":
    main()
