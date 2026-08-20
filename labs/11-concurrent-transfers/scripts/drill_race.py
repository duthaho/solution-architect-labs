"""The core race drill: W workers × R rounds of transfers over a hot account
set, all workers released together by a barrier.

Workload shape (deterministic by construction, not by luck):
  * in naive mode a second barrier sits INSIDE the handler, between its
    balance read and its write: no worker writes until every worker has
    read, so the stale-read overlap is a certainty, not a scheduler win;
  * every round, ALL workers debit the SAME hot src account — maximum
    write-write contention on one row;
  * each worker credits a DIFFERENT dst with a DIFFERENT amount, so lost
    updates are asymmetric: when two read-windows overlap, the surviving
    literal write cannot equal the sum of acked debits — money is created,
    and SUM(balance) drifts. (Symmetric lost updates would cancel out and
    hide the bug.)
  * one round per cycle is an OVERDRAFT round: the amounts are sized so the
    hot account cannot fund them all. Correct strategies ack at most what
    the balance covers and reject the rest with `insufficient`; the naive
    handler happily acks all of them from the same stale snapshot — the
    literal double-spend.

Verdict (exit code is the assertion):
  * MODE=naive  → exit 0 iff the run VIOLATES conservation (bug reproduced).
  * MODE=a|b|c|d → exit 0 iff the run CONSERVES money, no account is
    negative, and every acked delta is reflected in balances exactly once.
"""
import os
import sys
import threading
import time
import uuid
from collections import defaultdict
from decimal import Decimal

import common
import strategies

MODE = os.environ.get("MODE", "naive")
WORKERS = int(os.environ.get("WORKERS", "8"))
ROUNDS = int(os.environ.get("ROUNDS", "12"))
HOT = int(os.environ.get("HOT", "4"))  # size of the hot account set
BARRIER_TIMEOUT_S = 30  # a dead worker must fail the drill, not hang it
SYNC_TIMEOUT_S = 5      # read→write rendezvous; short, degrades to a hold

HANDLERS = {
    "naive": strategies.transfer_naive,
    "a": strategies.transfer_pessimistic,
    "b": strategies.transfer_optimistic,
    "c": strategies.transfer_atomic,
    "d": strategies.transfer_ledger,
}


def plan_op(round_i: int, worker: int) -> tuple[int, int, Decimal]:
    """Deterministic workload. All workers share src in a round; dst and
    amount differ per worker. Every HOT-th round is an overdraft round."""
    hot = list(range(1, HOT + 1))
    src = hot[round_i % HOT]
    dst = hot[(round_i + 1 + worker) % HOT]
    if dst == src:
        dst = hot[(round_i + 2 + worker) % HOT]
        if dst == src:  # HOT >= 3 makes this unreachable; belt and braces
            dst = hot[(round_i + 3 + worker) % HOT]
    overdraft_round = (round_i % HOT) == 0
    base = Decimal("400.00") if overdraft_round else Decimal("5.00")
    amount = base + Decimal(worker)  # distinct per worker → asymmetric race
    return src, dst, amount


def worker_loop(worker: int, barrier: threading.Barrier,
                sync_barrier: threading.Barrier, results: list, jpath) -> None:
    handler = HANDLERS[MODE]

    def sync() -> None:
        # Naive-only rendezvous: every worker reaches its handler's read
        # before any worker writes. Falls back to the think-time hold if a
        # peer died and broke the barrier — degraded, but never hung.
        try:
            sync_barrier.wait(timeout=SYNC_TIMEOUT_S)
        except threading.BrokenBarrierError:
            time.sleep(strategies.HOLD_MS / 1000.0)

    conn = common.connect()
    try:
        for r in range(ROUNDS):
            src, dst, amount = plan_op(r, worker)
            tid = str(uuid.uuid4())
            try:
                barrier.wait(timeout=BARRIER_TIMEOUT_S)
            except threading.BrokenBarrierError:
                break  # a peer died; stop cleanly, the report shows the gap
            t0 = time.monotonic()
            try:
                out = handler(conn, tid, src, dst, amount,
                              sync=sync if MODE == "naive" else None)
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass  # connection may be gone; the journal line records it
                code = e.args[0] if getattr(e, "args", None) else 0
                out = {"ok": False, "reason": f"error:{code}", "retries": 0,
                       "deadlocks": 0}
            ms = (time.monotonic() - t0) * 1000.0
            rec = {"transfer_id": tid, "mode": MODE, "worker": worker,
                   "src": src, "dst": dst, "amount": str(amount),
                   "ok": out["ok"], "reason": out["reason"],
                   "retries": out["retries"],
                   "deadlocks": out.get("deadlocks", 0), "ms": round(ms, 2)}
            results.append(rec)
            common.append_jsonl(jpath, rec)
    finally:
        # However this thread exits, never strand the peers at a barrier.
        barrier.abort()
        sync_barrier.abort()
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    if MODE not in HANDLERS:
        sys.exit(f"unknown MODE={MODE} (have: {sorted(HANDLERS)})")
    if HOT < 2:
        sys.exit(f"HOT={HOT} is degenerate: transfers need src != dst (HOT >= 2)")
    if common.ACCOUNTS < HOT:
        sys.exit(f"ACCOUNTS={common.ACCOUNTS} < HOT={HOT}: hot set must exist")

    # Journals APPEND across drill runs; only seed.py (the world reset)
    # clears them. That keeps verify.py's reconciliation valid for repeated
    # drills on the same seeded world.
    jpath = common.journal_path(MODE)

    conn = common.connect()
    start = common.read_balances(conn, MODE)
    conn.rollback()  # close the read snapshot
    start_total = sum(start.values())

    barrier = threading.Barrier(WORKERS)
    sync_barrier = threading.Barrier(WORKERS)
    results: list[dict] = []
    threads = [
        threading.Thread(target=worker_loop,
                         args=(w, barrier, sync_barrier, results, jpath))
        for w in range(WORKERS)
    ]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    end = common.read_balances(conn, MODE)
    conn.rollback()
    end_total = sum(end.values())
    negatives = common.negative_accounts(conn, MODE)

    cache_drift = []
    if MODE == "d":
        # The read model must equal the ledger it materializes.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.account_id FROM balance_cache c "
                "JOIN (SELECT account_id, SUM(amount) s FROM entries "
                "      GROUP BY account_id) e ON e.account_id = c.account_id "
                "WHERE c.balance <> e.s"
            )
            cache_drift = [r[0] for r in cur.fetchall()]
        conn.rollback()
    conn.close()

    # Journal-implied expectation: start balance + sum of acked deltas.
    expected = dict(start)
    acked = rejected = errors = 0
    retries = deadlocks = 0
    for rec in results:
        retries += rec["retries"]
        deadlocks += rec["deadlocks"]
        if rec["ok"]:
            acked += 1
            amt = Decimal(rec["amount"])
            expected[rec["src"]] -= amt
            expected[rec["dst"]] += amt
        elif rec["reason"].startswith("error:"):
            errors += 1
        else:
            rejected += 1
    lost = [aid for aid in start if expected[aid] != end[aid]]

    drift = end_total - start_total
    print(f"\n--- drill_race MODE={MODE} "
          f"({WORKERS} workers x {ROUNDS} rounds, hot set {HOT}) ---")
    print(f"acked={acked} rejected={rejected} errors={errors} "
          f"retries={retries} deadlocks={deadlocks} wall={wall:.1f}s")
    planned = WORKERS * ROUNDS
    if len(results) < planned:
        print(f"WARNING: only {len(results)}/{planned} ops ran "
              f"(a worker died; barriers aborted)")
    print(f"start_total={start_total} end_total={end_total} drift={drift:+}")
    print(f"accounts where balance != journal-implied expectation: {len(lost)}"
          + (f" -> {lost}" if lost else ""))
    if negatives:
        print(f"NEGATIVE balances: {negatives}")
    if MODE == "d":
        print(f"balance_cache != SUM(entries) for {len(cache_drift)} accounts"
              + (f" -> {cache_drift}" if cache_drift else ""))

    violated = bool(drift != 0 or lost or negatives or cache_drift)
    if MODE == "naive":
        if violated:
            print("VERDICT: CONSERVATION VIOLATED — lost update / double-spend "
                  "reproduced (as expected for the naive handler)")
            sys.exit(0)
        print("VERDICT: naive run conserved money — the race FAILED to reproduce")
        sys.exit(1)
    else:
        if acked == 0:
            # Conservation over zero successful transfers proves nothing —
            # a dead database would "pass". No work, no verdict.
            print(f"VERDICT: strategy '{MODE}' acked NOTHING "
                  f"({errors} errors) — cannot claim conservation")
            sys.exit(1)
        if not violated:
            print(f"VERDICT: money conserved, no negatives — strategy '{MODE}' holds")
            sys.exit(0)
        print(f"VERDICT: strategy '{MODE}' VIOLATED the invariant")
        sys.exit(1)


if __name__ == "__main__":
    main()
