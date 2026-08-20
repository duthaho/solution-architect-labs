"""The flash-sale burst: WORKERS barrier-synced threads x ROUNDS, one hot item.

Demand (WORKERS*ROUNDS, default 96) deliberately exceeds CAPACITY (50), so a
correct strategy must cleanly reject the excess and the naive strategy
oversells.

Exit-code contract (lab-11 style):
  MODE=naive  -> exit 0 iff oversell WAS reproduced (the bug is the deliverable)
  MODE=a|b|c  -> exit 0 iff no oversell, acks == min(capacity, demand) (no
                 undersell — a strategy that rejects everything is not
                 "correct"), and the DB row count equals the acks
Run after `make seed`; the contract assumes a fresh world.
"""

import os
import sys
import threading
import time
import uuid

import common
import redis_store
import strategies

MODE = os.environ.get("MODE", "naive")


def main() -> int:
    legacy = redis_store.LegacyStore() if MODE == "legacy" else None
    handler = None if legacy else strategies.HANDLERS[MODE]
    journal = common.journal_path(MODE)
    round_barrier = threading.Barrier(common.WORKERS)
    sync_barrier = threading.Barrier(common.WORKERS)
    results: list[dict] = []
    lock = threading.Lock()

    errors: list[str] = []

    def worker(w: int) -> None:
        conn = None if legacy else common.connect()
        try:
            for rnd in range(common.ROUNDS):
                round_barrier.wait()
                rid = str(uuid.uuid4())
                sync = sync_barrier.wait if MODE == "naive" else None
                t0 = time.monotonic()
                if legacy:
                    res = legacy.reserve(rid)
                else:
                    res = handler(conn, rid, sync=sync)
                ms = (time.monotonic() - t0) * 1000
                rec = {
                    "mode": MODE,
                    "phase": "redis" if legacy else "mysql",
                    "store": "redis" if legacy else "mysql",
                    "dual": False,
                    "reservation_id": rid, "ok": res["ok"], "reason": res["reason"],
                    "retries": res["retries"], "round": rnd, "worker": w,
                    "ms": round(ms, 2),
                }
                with lock:
                    results.append(rec)
        except threading.BrokenBarrierError:
            pass  # another worker failed; exit quietly, main reports it
        except Exception as e:  # noqa: BLE001 — abort the barriers so the
            # drill exits non-zero instead of hanging every other worker
            with lock:
                errors.append(f"worker {w}: {e!r}")
            round_barrier.abort()
            sync_barrier.abort()
        finally:
            if conn:
                conn.close()

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(common.WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        for e in errors:
            common.log.error("aborted: %s", e)
        return 1

    for rec in results:
        common.append_jsonl(journal, rec)

    acks = sum(1 for r in results if r["ok"])
    rejects = len(results) - acks
    retries = sum(r["retries"] for r in results)

    if legacy:
        remaining = legacy.remaining()
        common.log.info(
            "MODE=legacy demand=%d acks=%d rejects=%d | redis remaining=%d -> %s",
            len(results), acks, rejects, remaining,
            "ok" if remaining >= 0 and acks == common.CAPACITY else "BROKEN",
        )
        return 0 if remaining >= 0 and acks == common.CAPACITY else 1

    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM reservations WHERE state IN ('active','committed')"
        )
        consumed = cur.fetchone()[0]
    capacity, reserved, sold = common.item_row(conn)
    conn.close()

    oversold = consumed > capacity
    common.log.info(
        "MODE=%s demand=%d acks=%d rejects=%d retries=%d | reservations=%d "
        "capacity=%d counter(reserved+sold)=%d -> %s",
        MODE, len(results), acks, rejects, retries, consumed, capacity,
        reserved + sold, "OVERSOLD" if oversold else "ok",
    )

    if MODE == "naive":
        if oversold:
            common.log.info("naive oversell reproduced (expected) — exit 0")
            return 0
        common.log.error("naive did NOT oversell — drill failed to reproduce the bug")
        return 1
    if oversold:
        common.log.error("strategy %s OVERSOLD — bug", MODE)
        return 1
    expected = min(capacity, len(results))
    if acks != expected or consumed != acks:
        common.log.error(
            "strategy %s undersold or lost writes: acks=%d expected=%d db-rows=%d",
            MODE, acks, expected, consumed,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
