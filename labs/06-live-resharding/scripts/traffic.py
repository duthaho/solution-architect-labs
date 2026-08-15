"""Continuous user-scoped traffic through the router — correctness measured
AT THE APPLICATION, not just in the after-the-fact verifier.

Op mix: ~55% insert (next seq for a random user), ~30% update (random
existing seq, full new state), ~15% read-check. No deletes — deletes under an
INSERT IGNORE backfill would resurrect rows and need tombstones or a binlog
stream (README §5); this lab keeps the invariant machine-checkable instead:

    for every user:  COUNT(*) == MAX(seq) == my acked insert count

The read-check asserts exactly that through the router's CURRENT read path —
so after cutover it is asserting against the shards. A lost, duplicated, or
stale row surfaces as a `check_fail` journal entry and a nonzero exit within
seconds, while the reshard is still in progress. That is the difference
between "the migration finished" and "the migration was correct".

Every acked write is journaled to journal.jsonl (append + flush before the
next op): verify.py replays it later as ground truth. Mode flips arrive
mid-run via router_state.json; this process never restarts across the ladder.
"""
import json
import random
import signal
import sys
import time

import pymysql

from common import JOURNAL, Router, TABLE, connect, log, make_values, read_mode, shard_for

RUNNING = True
RETRYABLE = {1205, 1213, 2003, 2006, 2013}  # lock wait, deadlock, conn failures


def _stop(*_):
    global RUNNING
    RUNNING = False


def load_user_state() -> dict[int, int]:
    """user -> current max seq, from the CURRENT authoritative node(s)."""
    mode = read_mode()
    nodes = ["shard0", "shard1"] if mode == "sharded" else ["mono"]
    state: dict[int, int] = {}
    for node in nodes:
        conn = connect(node)
        with conn.cursor() as cur:
            cur.execute(f"SELECT user_id, MAX(seq), COUNT(*) FROM {TABLE} GROUP BY user_id")
            for user_id, max_seq, count in cur.fetchall():
                if max_seq != count:
                    raise SystemExit(
                        f"pre-existing gap on {node}: user {user_id} max_seq={max_seq} "
                        f"count={count} — refusing to start on top of corruption")
                state[int(user_id)] = int(max_seq)
        conn.close()
    log.info("loaded %d users from %s (authoritative for mode=%s)", len(state), nodes, mode)
    return state


def with_retry(fn, max_wait_s: float = 60.0):
    """Retry transient authoritative-side errors; anything else propagates."""
    deadline = time.time() + max_wait_s
    backoff = 0.2
    while True:
        try:
            return fn()
        except pymysql.MySQLError as e:
            code = e.args[0] if e.args else 0
            if code in RETRYABLE and time.time() < deadline:
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
                continue
            raise


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    router = Router()
    expected = load_user_state()
    users = sorted(expected)
    rng = random.Random()
    ops = checks = failures = 0

    log.info("traffic started: %d users, journal=%s", len(users), JOURNAL)
    with open(JOURNAL, "a") as journal:
        while RUNNING:
            user = rng.choice(users)
            roll = rng.random()
            try:
                if roll < 0.55:  # ---- insert: the next seq for this user
                    seq = expected[user] + 1
                    status, amount, note = make_values(rng)
                    with_retry(lambda: router.write("insert", user, seq, status, amount, note))
                    expected[user] = seq
                    journal.write(json.dumps({
                        "op": "insert", "user": user, "seq": seq, "status": status,
                        "amount": amount, "note": note, "ts": int(time.time() * 1000)}) + "\n")
                elif roll < 0.85:  # ---- update: full new state for an existing row
                    seq = rng.randint(1, expected[user])
                    status, amount, note = make_values(rng)
                    rc = with_retry(lambda: router.write("update", user, seq, status, amount, note))
                    if rc == 0:
                        failures += 1
                        entry = {"op": "check_fail", "kind": "update_missing_row",
                                 "user": user, "seq": seq, "mode": router.mode(),
                                 "ts": int(time.time() * 1000)}
                        journal.write(json.dumps(entry) + "\n")
                        log.error("LOST ROW: update matched nothing: user=%d seq=%d "
                                  "(mode=%s, authoritative node missing an acked row)",
                                  user, seq, router.mode())
                    else:
                        journal.write(json.dumps({
                            "op": "update", "user": user, "seq": seq, "status": status,
                            "amount": amount, "note": note, "ts": int(time.time() * 1000)}) + "\n")
                else:  # ---- read-check: the invariant, through the live read path
                    checks += 1
                    count, max_seq = with_retry(lambda: router.read_state(user))
                    if not (count == max_seq == expected[user]):
                        failures += 1
                        entry = {"op": "check_fail", "kind": "state_mismatch", "user": user,
                                 "count": count, "max_seq": max_seq,
                                 "expected": expected[user], "shard": shard_for(user),
                                 "mode": router.mode(), "ts": int(time.time() * 1000)}
                        journal.write(json.dumps(entry) + "\n")
                        log.error("CHECK FAIL user=%d: count=%d max_seq=%d expected=%d "
                                  "(mode=%s shard=%s)", user, count, max_seq,
                                  expected[user], router.mode(), shard_for(user))
                journal.flush()
            except Exception as e:
                failures += 1
                log.error("authoritative write failed hard: %s (mode=%s)", e, router.mode())
                journal.write(json.dumps({"op": "check_fail", "kind": "write_error",
                                          "error": str(e)[:200], "mode": router.mode(),
                                          "ts": int(time.time() * 1000)}) + "\n")
                journal.flush()
                time.sleep(1)

            ops += 1
            if ops % 500 == 0:
                log.info("  %d ops (%d read-checks, %d failures, %d repairs queued, "
                         "%d shadow reads / %d diffs) mode=%s",
                         ops, checks, failures, router.repair_queued,
                         router.shadow_reads, router.shadow_diffs, router.mode())
            time.sleep(rng.uniform(0.005, 0.02))

    router.close()
    log.info("traffic stopped: %d ops, %d read-checks, %d FAILURES, %d shadow diffs",
             ops, checks, failures, router.shadow_diffs)
    sys.exit(2 if failures else 0)


if __name__ == "__main__":
    main()
