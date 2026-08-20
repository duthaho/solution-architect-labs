"""Lifecycle drill (mode c): commit converts to a sale, TTL returns the rest.

1. Exhaust the pool with short-TTL reservations (TTL_S=2) — pool empty,
   further attempts reject.
2. COMMIT the first COMMITS of them: active -> committed. A committed
   reservation is a sale — the sweep must never touch it.
3. Wait past the TTL, run the sweep — only the still-active remainder
   expires and returns to the pool.
4. Prove exactly (capacity - COMMITS) new reservations succeed — the
   committed sales still hold their slots.

Exit 0 iff all four acts play out exactly.
"""

import sys
import time
import uuid

import common
import strategies
import sweep

COMMITS = 10


def reserve_n(conn, n: int, mode_label: str) -> list[str]:
    acked: list[str] = []
    journal = common.journal_path("c")
    for _ in range(n):
        rid = str(uuid.uuid4())
        res = strategies.reserve_pool(conn, rid)
        common.append_jsonl(journal, {
            "mode": "c", "phase": "mysql", "store": "mysql", "dual": False,
            "reservation_id": rid, "ok": res["ok"], "reason": res["reason"],
            "retries": res["retries"], "round": mode_label, "worker": 0, "ms": 0,
        })
        if res["ok"]:
            acked.append(rid)
    return acked


def main() -> int:
    conn = common.connect()
    cap = common.CAPACITY

    # Act 1 — exhaust with 2-second TTLs (wide enough to outlive the act
    # itself on a slow machine; the ops take milliseconds)
    common.TTL_S = 2
    t0 = time.monotonic()
    acked = reserve_n(conn, cap, "exhaust")
    probe = reserve_n(conn, 5, "probe-full")
    common.log.info("act1: acked=%d/%d, probe-after-full acked=%d (want 0)",
                    len(acked), cap, len(probe))
    if len(acked) != cap or probe:
        return 1
    if time.monotonic() - t0 >= 2:
        common.log.error("act1 took longer than the TTL — machine too slow for the drill")
        return 1

    # Act 2 — commit the first COMMITS reservations: they become sales
    committed = sum(1 for rid in acked[:COMMITS]
                    if strategies.commit_reservation(conn, rid))
    common.log.info("act2: committed=%d (want %d) — sales survive the sweep",
                    committed, COMMITS)
    if committed != COMMITS:
        return 1

    # Act 3 — TTL passes; sweep expires only the still-active remainder
    time.sleep(2.5)
    released = sweep.sweep()
    common.log.info("act3: sweep released=%d (want %d)", released, cap - COMMITS)
    if released != cap - COMMITS:
        return 1

    # Act 4 — exactly the released capacity is reclaimable
    common.TTL_S = 120
    acked2 = reserve_n(conn, cap, "reclaim")
    common.log.info("act4: acked=%d of %d attempts (want exactly %d)",
                    len(acked2), cap, cap - COMMITS)
    conn.close()
    return 0 if len(acked2) == cap - COMMITS else 1


if __name__ == "__main__":
    sys.exit(main())
