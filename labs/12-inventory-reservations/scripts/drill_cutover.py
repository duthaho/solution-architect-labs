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

CUTOVER_INJECT=1 proves the abort path: a phantom Redis ack is injected
before the gate, the comparator must catch it, and the drill exits 0 iff
the gate REFUSED to flip (phase still shadow, zero MySQL-era acks).
"""

import os
import sys
import threading
import time
import uuid

import common
import redis_store
import strategies

INJECT = os.environ.get("CUTOVER_INJECT", "0") == "1"


def main() -> int:
    common.write_phase("shadow")
    store = redis_store.LegacyStore()
    journal = common.journal_path("cutover")
    start = threading.Barrier(common.WORKERS + 1)
    end = threading.Barrier(common.WORKERS + 1)
    results: list[dict] = []
    lock = threading.Lock()
    abort = threading.Event()

    errors: list[str] = []

    def worker(w: int) -> None:
        conn = common.connect()
        try:
            run_rounds(conn, w)
        except threading.BrokenBarrierError:
            pass  # another thread failed; main reports it
        except Exception as e:  # noqa: BLE001 — break the barriers, never hang
            with lock:
                errors.append(f"worker {w}: {e!r}")
            start.abort()
            end.abort()
        finally:
            conn.close()

    def run_rounds(conn, w: int) -> None:
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

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(common.WORKERS)]
    for t in threads:
        t.start()

    gate_ok = True
    conn = common.connect()
    try:
        for rnd in range(common.ROUNDS):
            start.wait()
            end.wait()  # workers finished round rnd, parked at next start
            if rnd == common.ROUNDS // 2 - 1:
                if INJECT:
                    store.r.sadd(common.REDIS_KEY + ":acks", "phantom-" + str(uuid.uuid4()))
                    common.log.info("INJECT: phantom Redis ack planted before the gate")
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
    except threading.BrokenBarrierError:
        pass
    for t in threads:
        t.join()

    if errors:
        for e in errors:
            common.log.error("aborted: %s", e)
        return 1

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

    if INJECT:
        proven = (not gate_ok) and common.read_phase() == "shadow" and post == 0
        common.log.info(
            "inject mode: gate %s, phase=%s, mysql-era acks=%d -> abort path %s",
            "aborted" if not gate_ok else "FLIPPED ANYWAY",
            common.read_phase(), post, "PROVEN" if proven else "BROKEN",
        )
        return 0 if proven else 1

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
