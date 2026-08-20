"""Shadow-mode dual-write drill: Redis stays the source of truth, every
reservation is also written to MySQL, and a comparator measures how far the
stores have drifted — the metric that decides whether cutover is safe.

Act 1 (clean): 30 dual-written reservations -> comparator must report 0
  mismatches.
Act 2 (injected divergence): 10 more, but every 2nd MySQL write is skipped —
  journaled honestly as dual=false — proving the comparator actually detects
  drift (the metric is not vacuous). Expect exactly 5 mismatches.

MySQL shadow writes are idempotently keyed by reservation_id (the PK of
`reservations`); a failed/skipped write is counted, never hidden.

Exit 0 iff act 1 shows 0 mismatches AND act 2 shows exactly the injected 5.
"""

import sys
import uuid

import common
import redis_store
import strategies

CLEAN_OPS = 30
INJECT_OPS = 10


def dual_write(conn, store, rid: str, inject_skip: bool) -> dict:
    res = store.reserve(rid)  # Redis is the source of truth
    if not res["ok"]:
        return {"ok": False, "reason": res["reason"], "dual": False}
    if inject_skip:
        return {"ok": True, "reason": "reserved", "dual": False}  # counted, not hidden
    shadow = strategies.reserve_pool(conn, rid)
    return {"ok": True, "reason": "reserved", "dual": shadow["ok"]}


def main() -> int:
    common.write_phase("shadow")
    conn = common.connect()
    store = redis_store.LegacyStore()
    journal = common.journal_path("shadow")

    def run(ops: int, label: str, inject_every: int | None) -> None:
        for i in range(ops):
            rid = str(uuid.uuid4())
            skip = inject_every is not None and i % inject_every == 1
            res = dual_write(conn, store, rid, inject_skip=skip)
            common.append_jsonl(journal, {
                "mode": "shadow", "phase": "shadow", "store": "redis",
                "dual": res["dual"], "reservation_id": rid, "ok": res["ok"],
                "reason": ("injected_skip" if skip else res["reason"]),
                "retries": 0, "round": label, "worker": 0, "ms": 0,
            })

    run(CLEAN_OPS, "clean", inject_every=None)
    only_redis, only_mysql = redis_store.compare_stores(conn, store)
    clean_mismatches = len(only_redis) + len(only_mysql)
    common.log.info("act1 clean: %d dual-writes, comparator mismatches=%d (want 0)",
                    CLEAN_OPS, clean_mismatches)

    run(INJECT_OPS, "injected", inject_every=2)
    only_redis, only_mysql = redis_store.compare_stores(conn, store)
    injected_expected = INJECT_OPS // 2
    injected_found = len(only_redis) + len(only_mysql)
    common.log.info(
        "act2 injected: skipped %d MySQL writes, comparator mismatches=%d (want %d) "
        "— the metric detects drift",
        injected_expected, injected_found, injected_expected,
    )
    conn.close()

    return 0 if clean_mismatches == 0 and injected_found == injected_expected else 1


if __name__ == "__main__":
    sys.exit(main())
