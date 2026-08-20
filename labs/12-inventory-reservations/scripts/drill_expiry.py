"""TTL-expiry drill (mode c): abandoned checkouts must return capacity.

1. Exhaust the pool with short-TTL reservations (TTL_S=1) — pool empty,
   further attempts reject.
2. Wait past the TTL, run the sweep — slots return to the pool.
3. Prove new reservations succeed again, up to capacity and no further.

Exit 0 iff all three acts play out exactly.
"""

import sys
import time
import uuid

import common
import strategies
import sweep


def reserve_n(conn, n: int, mode_label: str) -> int:
    acked = 0
    journal = common.journal_path("c")
    for _ in range(n):
        rid = str(uuid.uuid4())
        res = strategies.reserve_pool(conn, rid)
        common.append_jsonl(journal, {
            "mode": "c", "phase": "mysql", "store": "mysql", "dual": False,
            "reservation_id": rid, "ok": res["ok"], "reason": res["reason"],
            "retries": res["retries"], "round": mode_label, "worker": 0, "ms": 0,
        })
        acked += 1 if res["ok"] else 0
    return acked


def main() -> int:
    conn = common.connect()
    cap = common.CAPACITY

    # Act 1 — exhaust with 1-second TTLs
    common.TTL_S = 1
    acked = reserve_n(conn, cap, "exhaust")
    rejected_probe = reserve_n(conn, 5, "probe-full")
    common.log.info("act1: acked=%d/%d, probe-after-full acked=%d (want 0)",
                    acked, cap, rejected_probe)
    if acked != cap or rejected_probe != 0:
        return 1

    # Act 2 — TTL passes, sweep releases
    time.sleep(1.5)
    released = sweep.sweep()
    common.log.info("act2: sweep released=%d (want %d)", released, cap)
    if released != cap:
        return 1

    # Act 3 — capacity is back: exactly cap more reservations succeed
    common.TTL_S = 120
    acked2 = reserve_n(conn, cap + 5, "reclaim")
    common.log.info("act3: acked=%d of %d attempts (want exactly %d)",
                    acked2, cap + 5, cap)
    conn.close()
    return 0 if acked2 == cap else 1


if __name__ == "__main__":
    sys.exit(main())
