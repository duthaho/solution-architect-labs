"""Cutover drill: flip the source of truth Redis -> MySQL in the middle of a
live burst, losing nothing and overselling nothing.

Workers run the usual burst (WORKERS x ROUNDS, demand > capacity), reading
the phase flag BEFORE EVERY OPERATION:
  phase=shadow -> reserve in Redis (truth), dual-write MySQL
  phase=mysql  -> reserve in MySQL only (SKIP LOCKED pool)

The coordinator pauses the world after ROUNDS/2 (barrier — workers are
quiescent), runs the comparator, and only flips if mismatches == 0: the
dual-write has kept MySQL current, so the flip transfers no state [plan
amendment 5]. If the gate fails, the drill aborts without flipping.

Exit 0 iff the gate passed, total acks == capacity, and MySQL's consumed
count equals total acks (verify.py re-checks this as cross-store-total).
"""

import sys
import threading
import time
import uuid

import common
import redis_store
import strategies


def main() -> int:
    common.write_phase("shadow")
    store = redis_store.LegacyStore()
    journal = common.journal_path("cutover")
    start = threading.Barrier(common.WORKERS + 1)
    end = threading.Barrier(common.WORKERS + 1)
    results: list[dict] = []
    lock = threading.Lock()
    abort = threading.Event()

    def worker(w: int) -> None:
        conn = common.connect()
        for rnd in range(common.ROUNDS):
            start.wait()
            if abort.is_set():
                end.wait()
                continue
            phase = common.read_phase()
            rid = str(uuid.uuid4())
            t0 = time.monotonic()
            if phase == "shadow":
                res = store.reserve(rid)
                dual = False
                if res["ok"]:
                    dual = strategies.reserve_pool(conn, rid)["ok"]
                rec_store = "redis"
            else:
                r = strategies.reserve_pool(conn, rid)
                res, dual, rec_store = r, False, "mysql"
            ms = (time.monotonic() - t0) * 1000
            with lock:
                results.append({
                    "mode": "cutover", "phase": phase, "store": rec_store,
                    "dual": dual, "reservation_id": rid, "ok": res["ok"],
                    "reason": res["reason"], "retries": res.get("retries", 0),
                    "round": rnd, "worker": w, "ms": round(ms, 2),
                })
            end.wait()
        conn.close()

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(common.WORKERS)]
    for t in threads:
        t.start()

    gate_ok = True
    conn = common.connect()
    for rnd in range(common.ROUNDS):
        start.wait()
        end.wait()  # workers finished round rnd and are parked at next start
        if rnd == common.ROUNDS // 2 - 1:
            only_redis, only_mysql = redis_store.compare_stores(conn, store)
            mismatches = len(only_redis) + len(only_mysql)
            if mismatches == 0:
                common.write_phase("mysql")
                common.log.info(
                    "gate after round %d: comparator mismatches=0 -> FLIPPED to mysql",
                    rnd + 1,
                )
            else:
                gate_ok = False
                abort.set()
                common.log.error(
                    "gate after round %d: mismatches=%d -> cutover ABORTED", rnd + 1, mismatches
                )
    for t in threads:
        t.join()

    for rec in results:
        common.append_jsonl(journal, rec)

    acks = [r for r in results if r["ok"]]
    pre = sum(1 for r in acks if r["store"] == "redis")
    post = sum(1 for r in acks if r["store"] == "mysql")
    # End the coordinator's REPEATABLE READ snapshot (pinned at the gate
    # query) so the final count sees the post-flip rows.
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM reservations WHERE state IN ('active','committed')"
        )
        consumed = cur.fetchone()[0]
    conn.close()

    ok = gate_ok and len(acks) == common.CAPACITY and consumed == len(acks)
    common.log.info(
        "cutover: acks=%d (redis-era=%d, mysql-era=%d) rejects=%d | "
        "mysql consumed=%d capacity=%d -> %s",
        len(acks), pre, post, len(results) - len(acks),
        consumed, common.CAPACITY, "ok" if ok else "BROKEN",
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
