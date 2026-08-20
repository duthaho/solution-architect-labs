"""Invariant gate: joins seed.json, the burst journals, MySQL and Redis.

Exit code is the assertion: 0 iff every check passes (VERIFY_INVERT=1 flips
it — used by `make verify-naive`, which must FAIL against the naive strategy,
proving the checker actually catches oversell).

Journal record schema (written by the drills):
  {"mode","phase","store","dual","reservation_id","ok","reason","round","worker","ms"}
"""

import os
import sys

import common

INVERT = os.environ.get("VERIFY_INVERT", "0") == "1"


def main() -> int:
    seed = common.read_seed()
    capacity = seed["capacity"]

    journal: list[dict] = []
    for mode in common.MODES:
        journal.extend(common.read_jsonl(common.journal_path(mode)))
    acks = [r for r in journal if r["ok"]]
    mysql_acks = [r for r in acks if r["store"] == "mysql"]
    redis_acks = [r for r in acks if r["store"] == "redis"]

    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state, COUNT(*) FROM reservations GROUP BY state"
        )
        by_state = {s: n for s, n in cur.fetchall()}
        cur.execute(
            "SELECT state, COUNT(*) FROM reservations WHERE mode='c' GROUP BY state"
        )
        c_by_state = {s: n for s, n in cur.fetchall()}
        cur.execute("SELECT state, COUNT(*) FROM slots GROUP BY state")
        slots_by_state = {s: n for s, n in cur.fetchall()}
        cur.execute(
            "SELECT COUNT(*) FROM slots s JOIN reservations r "
            "ON r.reservation_id = s.reservation_id "
            "WHERE s.state='claimed' AND r.state='expired'"
        )
        claimed_expired = cur.fetchone()[0]
        cur.execute("SELECT reservation_id FROM reservations")
        db_ids = {row[0] for row in cur.fetchall()}
    capacity_db, reserved_db, sold_db = common.item_row(conn)
    conn.close()

    consumed = by_state.get("active", 0) + by_state.get("committed", 0)
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    # C1 — never oversold (the headline invariant)
    check(
        "never-oversold",
        consumed <= capacity,
        f"active+committed={consumed} capacity={capacity}",
    )

    # C2 — counter consistency (modes a/b write the items counter row)
    counter_acks = [r for r in mysql_acks if r["mode"] in ("a", "b")]
    if counter_acks:
        check(
            "counter-consistency",
            reserved_db + sold_db == consumed,
            f"items.reserved+sold={reserved_db + sold_db} reservation-rows={consumed}",
        )

    # C3 — slot conservation (pool mode c)
    c_consumed = c_by_state.get("active", 0) + c_by_state.get("committed", 0)
    if c_consumed or slots_by_state.get("claimed", 0):
        free, claimed = slots_by_state.get("free", 0), slots_by_state.get("claimed", 0)
        check(
            "slot-conservation",
            free + claimed == capacity and claimed == c_consumed,
            f"free={free} claimed={claimed} capacity={capacity} mode-c-consumed={c_consumed}",
        )
        check(
            "expired-slots-freed",
            claimed_expired == 0,
            f"claimed slots pointing at expired reservations={claimed_expired}",
        )

    # C4 — exactly-once in MySQL: every acked op has exactly one row
    missing = [r["reservation_id"] for r in mysql_acks if r["reservation_id"] not in db_ids]
    check(
        "exactly-once-mysql",
        not missing and len({r["reservation_id"] for r in mysql_acks}) == len(mysql_acks),
        f"acks={len(mysql_acks)} missing-rows={len(missing)}",
    )

    # C5 — exactly-once in Redis (legacy/shadow phases)
    if redis_acks:
        r = common.redis_client()
        acked_set = r.smembers(common.REDIS_KEY + ":acks")
        remaining = int(r.get(common.REDIS_KEY) or 0)
        missing_r = [x["reservation_id"] for x in redis_acks if x["reservation_id"] not in acked_set]
        check(
            "exactly-once-redis",
            not missing_r and len(acked_set) == len(redis_acks),
            f"acks={len(redis_acks)} set={len(acked_set)} missing={len(missing_r)}",
        )
        check(
            "redis-conservation",
            remaining == capacity - len(acked_set) and remaining >= 0,
            f"remaining={remaining} expected={capacity - len(acked_set)}",
        )

    # C6 — cross-store: dual-written Redis acks must have their MySQL row;
    # total acks across stores never exceed capacity.
    dual_acks = [r for r in redis_acks if r.get("dual")]
    if dual_acks:
        missing_d = [r["reservation_id"] for r in dual_acks if r["reservation_id"] not in db_ids]
        check(
            "cross-store-dual-write",
            not missing_d,
            f"dual-written acks={len(dual_acks)} missing mysql rows={len(missing_d)}",
        )
    if redis_acks and mysql_acks:  # cutover ran: both stores acked
        check(
            "cross-store-total",
            len(acks) <= capacity and consumed == len(acks),
            f"total acks={len(acks)} mysql consumed={consumed} capacity={capacity}",
        )

    ok = not failures
    print(("ALL PASS" if ok else f"FAILED: {', '.join(failures)}")
          + (" (inverted)" if INVERT else ""))
    if INVERT:
        return 0 if not ok else 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
