"""The deadlock drill: why strategy a locks accounts in SORTED id order.

Two workers run opposing transfers — A→B and B→A — taking FOR UPDATE locks
one at a time with a short hold between the two acquisitions (the same app
think time as the race drill). In ARRIVAL order each worker grabs its own
src first and then wants the other's — a textbook lock cycle, and InnoDB
shoots one victim per round with error 1213. In SORTED order both workers
want the locks in the same sequence, so one simply waits: zero deadlocks,
same throughput of acked work.

Exit 0 iff arrival order produced deadlocks AND sorted order produced none.
"""
import os
import sys
import threading
import time
from decimal import Decimal

import pymysql

import common

ROUNDS = int(os.environ.get("ROUNDS", "10"))
HOLD_MS = int(os.environ.get("HOLD_MS", "50"))
AMOUNT = Decimal("1.00")
BARRIER_TIMEOUT_S = 30

_stats_lock = threading.Lock()


def opposing_transfer(order: str, src: int, dst: int,
                      barrier: threading.Barrier, stats: dict) -> None:
    conn = common.connect()
    try:
        _rounds(order, src, dst, barrier, stats, conn)
    finally:
        barrier.abort()  # never strand the peer, however we exit
        try:
            conn.close()
        except Exception:
            pass


def _rounds(order, src, dst, barrier, stats, conn) -> None:
    for _ in range(ROUNDS):
        try:
            barrier.wait(timeout=BARRIER_TIMEOUT_S)
        except threading.BrokenBarrierError:
            return  # peer died; stop cleanly
        lock_seq = (src, dst) if order == "arrival" else tuple(sorted((src, dst)))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT balance FROM accounts WHERE id = %s FOR UPDATE",
                    (lock_seq[0],),
                )
                time.sleep(HOLD_MS / 1000.0)  # hold lock 1, then want lock 2
                cur.execute(
                    "SELECT balance FROM accounts WHERE id = %s FOR UPDATE",
                    (lock_seq[1],),
                )
                cur.execute(
                    "UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (AMOUNT, src),
                )
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (AMOUNT, dst),
                )
            conn.commit()
            with _stats_lock:
                stats["acked"] += 1
        except pymysql.MySQLError as e:
            conn.rollback()
            code = e.args[0] if e.args else 0
            if code == 1213:
                with _stats_lock:
                    stats["deadlocks"] += 1
            else:
                raise


def run(order: str) -> dict:
    stats = {"order": order, "acked": 0, "deadlocks": 0}
    barrier = threading.Barrier(2)
    t1 = threading.Thread(target=opposing_transfer,
                          args=(order, 1, 2, barrier, stats))
    t2 = threading.Thread(target=opposing_transfer,
                          args=(order, 2, 1, barrier, stats))
    t1.start(); t2.start(); t1.join(); t2.join()
    print(f"{order:>7} lock order: {ROUNDS} opposing rounds -> "
          f"acked={stats['acked']} deadlocks(1213)={stats['deadlocks']}")
    return stats


def main() -> None:
    print(f"--- drill_deadlock (A->B vs B->A, hold {HOLD_MS}ms between locks) ---")

    # This drill moves money without journaling it (deadlocks, not
    # accounting, are its subject). Snapshot the two accounts and restore
    # them afterwards so a standalone run never trips verify.py.
    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT id, balance FROM accounts WHERE id IN (1, 2)")
        snapshot = {row[0]: row[1] for row in cur.fetchall()}
    conn.rollback()

    try:
        arrival = run("arrival")
        sorted_ = run("sorted")
    finally:
        with conn.cursor() as cur:
            for aid, bal in snapshot.items():
                cur.execute("UPDATE accounts SET balance = %s WHERE id = %s",
                            (bal, aid))
        conn.commit()
        conn.close()
        print("accounts 1 and 2 restored to their pre-drill balances")

    if arrival["deadlocks"] > 0 and sorted_["deadlocks"] == 0:
        print("VERDICT: arrival order deadlocks, sorted order does not — "
              "lock ordering is the fix")
        sys.exit(0)
    print("VERDICT: drill did not behave as expected")
    sys.exit(1)


if __name__ == "__main__":
    main()
