"""A worker that acquires the lock, appends N entries to the ledger, releases.

Every line it prints is its OWN BELIEF at that moment. During the drills the
beliefs go stale (the container gets SIGSTOPed past the lock TTL) — read the
log next to the ledger timeline and watch the worker be confidently wrong.

Modes:
  plain     acquire once, then write. Trusts the TTL. (What everyone ships.)
  recheck   before EVERY append, re-check "do I still hold the lock?" and only
            write if yes. Feels safe; is a textbook TOCTOU race — the pause can
            land between the check and the write. --post-check-delay widens
            that window so the chaos script can hit it deterministically; in
            production the window is any GC pause, and it only has to lose once.

On a 409 from storage (fencing): log it, abort the critical section. Storage
just told us the truth our lock couldn't: someone with a newer token exists.
"""
import argparse
import sys
import time

import requests

from common import Lock, append_entry, redis_client, LOCK_KEY


def say(worker_id: str, msg: str) -> None:
    print(f"{time.time():.3f} [{worker_id}] {msg}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="worker id, e.g. A or B")
    parser.add_argument("--ttl-ms", type=int, default=3000)
    parser.add_argument("--appends", type=int, default=6)
    parser.add_argument("--interval", type=float, default=0.4,
                        help="seconds between appends")
    parser.add_argument("--mode", choices=["plain", "recheck"], default="plain")
    parser.add_argument("--post-check-delay", type=float, default=0.0,
                        help="recheck mode: sleep between check and write "
                             "(the TOCTOU window, made visible)")
    parser.add_argument("--acquire-timeout", type=float, default=30.0)
    args = parser.parse_args()

    r = redis_client()
    lock = Lock(r)
    me = args.id

    # ---- acquire (retry until timeout: B politely waits for A's release) ----
    deadline = time.time() + args.acquire_timeout
    token = None
    while time.time() < deadline:
        token = lock.acquire(me, args.ttl_ms)
        if token is not None:
            break
        time.sleep(0.1)
    if token is None:
        say(me, f"FAILED to acquire lock within {args.acquire_timeout}s")
        return 2
    say(me, f"ACQUIRED lock, fencing token={token}, ttl={args.ttl_ms}ms — "
            f"I believe I am the sole owner")

    # ---- critical section: N slow appends ----
    aborted = False
    for seq in range(1, args.appends + 1):
        if args.mode == "recheck":
            holder = lock.holder()
            if holder != me:
                say(me, f"CHECK_FAILED before seq={seq}: lock holder is "
                        f"{holder!r}, not me — aborting (no write)")
                aborted = True
                break
            say(me, f"CHECK_OK seq={seq}: redis says I still hold the lock")
            if args.post_check_delay:
                time.sleep(args.post_check_delay)   # <-- the race lives here

        resp = append_entry(me, token, seq)
        if resp.status_code == 409:
            say(me, f"APPEND seq={seq} REJECTED 409 (token={token}): "
                    f"{resp.json().get('reason', 'stale token')} — storage "
                    f"fenced me off. I believed I held the lock; I was wrong. "
                    f"Aborting.")
            aborted = True
            break
        say(me, f"APPEND seq={seq} ok (token={token})")
        if seq < args.appends:
            time.sleep(args.interval)

    # ---- release ----
    if lock.release(me):
        say(me, "RELEASED lock cleanly")
    else:
        say(me, "RELEASE FAILED: lock is not mine anymore "
                "(expired and/or taken while I was busy)")

    say(me, f"DONE aborted={aborted}")
    return 1 if aborted else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (requests.RequestException, ConnectionError) as exc:
        print(f"worker fatal: {exc}", flush=True)
        sys.exit(3)
