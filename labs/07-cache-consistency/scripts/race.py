"""Deterministic reproduction of the stale-set race — scheduled, not lucky.

The classic incident, as an exact interleaving:

    reader                          writer
    ------                          ------
    GET p:42            -> miss
    SELECT price        -> 10.00 (old)
    ...paused...                    UPDATE price = 99.00 (commit)
    ...paused...                    DEL p:42        <- "invalidation" fires
    SETEX p:42 10.00    <- sets the OLD value AFTER the delete
                                    cache now serves 10.00 until TTL expiry
                                    while the DB says 99.00

race.py drives one reader client and one writer client through exactly that
schedule using the rendezvous hooks: the reader thread parks at
`after_db_read`, the writer runs its complete write path, the reader resumes.
No sleeps-and-pray, no 10k iterations: the interleaving is enforced by
threading.Events, so it reproduces on run 1 and on run 10000.

    --strategy delete    --expect stale   drill-stale-set: proves delete-on-write loses
    --strategy versioned --expect fresh   drill-versioned: same schedule, race neutralized
    --strategy ttl       --expect stale   no invalidation at all (baseline, same outcome)

Exit 0 iff every run matches --expect.
"""
import argparse
import sys
import threading

from cache_client import CacheClient, Hooks
from common import TABLE, connect_mysql, connect_redis, log

OLD_PRICE, NEW_PRICE = 10.00, 99.00


class RendezvousHooks(Hooks):
    """Turns named hook points into thread rendezvous. expect(name) arms a
    pause; the client thread parks inside fire(name) until release(name)."""

    def __init__(self):
        self._armed: dict[str, tuple[threading.Event, threading.Event]] = {}

    def expect(self, name: str) -> None:
        self._armed[name] = (threading.Event(), threading.Event())

    def fire(self, name: str) -> None:
        pair = self._armed.get(name)
        if pair:
            reached, resume = pair
            reached.set()
            if not resume.wait(timeout=30):
                raise RuntimeError(f"driver never resumed hook {name}")

    def wait_reached(self, name: str, timeout: float = 30) -> None:
        if not self._armed[name][0].wait(timeout):
            raise RuntimeError(f"client never reached hook {name}")

    def release(self, name: str) -> None:
        self._armed[name][1].set()


def reset(pid: int) -> None:
    """Known-cold start: DB row at OLD_PRICE, no trace of pid in Redis."""
    db = connect_mysql()
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TABLE} SET price=%s, version=version+1 WHERE id=%s",
                    (OLD_PRICE, pid))
    db.close()
    r = connect_redis()
    keys = [f"p:{pid}", f"v:{pid}", *r.keys(f"p:{pid}:v*")]
    r.delete(*keys)
    r.close()


def probe(strategy: str, pid: int, ttl_s: int) -> tuple[float, str]:
    """What a NEXT, uninvolved reader gets — the observable damage."""
    c = CacheClient(strategy, ttl_s=ttl_s, name="probe")
    before = c.db_reads
    price = c.read(pid)
    source = "cache" if c.db_reads == before else "db"
    c.close()
    return price, source


def run_once(strategy: str, pid: int, ttl_s: int, narrate: bool) -> bool:
    """Returns True iff the post-race probe read is STALE."""
    def say(actor: str, msg: str) -> None:
        if narrate:
            print(f"  {actor:<8} {msg}")

    reset(pid)
    hooks = RendezvousHooks()
    hooks.expect("after_db_read")
    reader = CacheClient(strategy, hooks=hooks, ttl_s=ttl_s, name="reader")
    writer = CacheClient(strategy, ttl_s=ttl_s, name="writer")

    result: dict = {}
    t = threading.Thread(target=lambda: result.update(price=reader.read(pid)))

    say("reader", f"GET p:{pid} -> MISS; SELECT price -> {OLD_PRICE:.2f} (the old value)")
    t.start()
    hooks.wait_reached("after_db_read")
    say("reader", "*** PARKED between its DB read and its cache SET ***")

    say("writer", f"UPDATE price = {NEW_PRICE:.2f} (committed)")
    inv = {"delete": f"DEL p:{pid}  (delete-on-write fires — and changes nothing)",
           "versioned": f"INCR v:{pid}  (version pointer advances past the parked reader)",
           "ttl": "(no invalidation: ttl strategy writes touch only the DB)"}
    writer.write(pid, NEW_PRICE)
    say("writer", inv[strategy])

    say("driver", "resume the reader: its cache SET happens AFTER the writer finished")
    hooks.release("after_db_read")
    t.join(timeout=30)
    say("reader", f"SETEX with {result['price']:.2f} -> returns {result['price']:.2f}")

    served, source = probe(strategy, pid, ttl_s)
    stale = served != NEW_PRICE
    if narrate:
        r = connect_redis()
        ttl_left = max(r.ttl(f"p:{pid}"), *(r.ttl(k) for k in r.keys(f"p:{pid}:v*")), -1)
        r.close()
        print(f"\n  next reader gets: {served:.2f} (from {source})   DB truth: {NEW_PRICE:.2f}")
        if stale:
            print(f"  ❌ STALE — and it will stay stale for TTL = {ttl_left}s more "
                  f"(nothing left to invalidate: the delete already happened)")
        else:
            print("  ✅ FRESH — the stale SET landed on an unreachable version key; "
                  "the pointer already moved on")
    reader.close()
    writer.close()
    return stale


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--strategy", required=True, choices=["ttl", "delete", "versioned"])
    ap.add_argument("--expect", required=True, choices=["stale", "fresh"])
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--ttl", type=int, default=3600,
                    help="cache TTL for the drill (long, to show 'stale until TTL' hurts)")
    args = ap.parse_args()

    print(f"=== the exact interleaving, strategy={args.strategy} (run 1 of {args.runs}) ===")
    outcomes = [run_once(args.strategy, pid=1, ttl_s=args.ttl, narrate=True)]
    for i in range(2, args.runs + 1):
        outcomes.append(run_once(args.strategy, pid=i, ttl_s=args.ttl, narrate=False))

    stale_runs = sum(outcomes)
    want_stale = args.expect == "stale"
    matched = sum(1 for o in outcomes if o == want_stale)
    print(f"\n=== {args.runs} runs: {stale_runs} stale, {args.runs - stale_runs} fresh "
          f"(expected {args.expect} on all) ===")
    if matched == args.runs:
        log.info("✅ %d/%d runs %s — deterministic, as scheduled", matched, args.runs, args.expect.upper())
    else:
        log.error("❌ only %d/%d runs matched expectation %s", matched, args.runs, args.expect)
        sys.exit(1)


if __name__ == "__main__":
    main()
