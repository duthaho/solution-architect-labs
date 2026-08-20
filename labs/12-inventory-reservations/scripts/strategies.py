"""Reservation strategies. One handler per MODE.

Signature: fn(conn, reservation_id, sync=None) -> {"ok", "reason", "retries"}
`sync` is a barrier callback used only by the naive handler to force the
read/write interleaving deterministically (every worker reads, THEN every
worker writes).

Transaction contract (all handlers): one explicit transaction per reservation
(autocommit off, InnoDB default REPEATABLE READ); every code path ends in
COMMIT or ROLLBACK; retry once on lock errors 1213/1205, then clean reject.
"""

import pymysql

import common

RETRYABLE = (1213, 1205)  # deadlock, lock wait timeout


def _insert_reservation(cur, reservation_id: str, mode: str) -> None:
    cur.execute(
        "INSERT INTO reservations (reservation_id, item_id, mode, state, expires_at) "
        "VALUES (%s, %s, %s, 'active', NOW(3) + INTERVAL %s SECOND)",
        (reservation_id, common.ITEM_ID, mode, common.TTL_S),
    )


def reserve_naive(conn, reservation_id: str, sync=None) -> dict:
    """Read available -> check -> write COMPUTED values. Loses updates under
    concurrency: two workers read the same `reserved`, both see room, both
    write reserved+1 — one increment vanishes, and both insert a reservation
    row. The reservation rows are the truth, so the item oversells."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT capacity, reserved, sold FROM items WHERE id = %s",
            (common.ITEM_ID,),
        )
        capacity, reserved, sold = cur.fetchone()
        if sync:
            sync()  # every worker has now read the same stale counter
        if reserved + sold >= capacity:
            conn.rollback()
            return {"ok": False, "reason": "sold_out", "retries": 0}
        cur.execute(
            "UPDATE items SET reserved = %s WHERE id = %s",
            (reserved + 1, common.ITEM_ID),  # computed literal — the bug
        )
        _insert_reservation(cur, reservation_id, "naive")
    conn.commit()
    return {"ok": True, "reason": "reserved", "retries": 0}


def _with_retry(fn):
    """Run fn(); on deadlock/lock-timeout roll back and retry ONCE, then
    reject cleanly. Never silently swallows other errors."""
    def wrapper(conn, reservation_id: str, sync=None) -> dict:
        for attempt in (0, 1):
            try:
                res = fn(conn, reservation_id, sync=sync)
                res["retries"] = attempt
                return res
            except pymysql.err.OperationalError as e:
                conn.rollback()
                if e.args[0] not in RETRYABLE or attempt == 1:
                    if e.args[0] in RETRYABLE:
                        return {"ok": False, "reason": f"lock_error_{e.args[0]}", "retries": attempt + 1}
                    raise
        raise AssertionError("unreachable")
    return wrapper


@_with_retry
def reserve_pessimistic(conn, reservation_id: str, sync=None) -> dict:
    """Strategy a: lock the single counter row with SELECT ... FOR UPDATE.
    Correct — every reservation serializes on one hot row, which is exactly
    the throughput problem Shopify hit."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT capacity, reserved, sold FROM items WHERE id = %s FOR UPDATE",
            (common.ITEM_ID,),
        )
        capacity, reserved, sold = cur.fetchone()
        if reserved + sold >= capacity:
            conn.rollback()
            return {"ok": False, "reason": "sold_out"}
        cur.execute(
            "UPDATE items SET reserved = reserved + 1 WHERE id = %s",
            (common.ITEM_ID,),
        )
        _insert_reservation(cur, reservation_id, "a")
    conn.commit()
    return {"ok": True, "reason": "reserved"}


@_with_retry
def reserve_atomic(conn, reservation_id: str, sync=None) -> dict:
    """Strategy b: push the check into the UPDATE's WHERE clause. The row
    lock is held only for the statement, not across a read-modify-write.
    affected_rows == 0 means sold out."""
    with conn.cursor() as cur:
        n = cur.execute(
            "UPDATE items SET reserved = reserved + 1 "
            "WHERE id = %s AND reserved + sold < capacity",
            (common.ITEM_ID,),
        )
        if n == 0:
            conn.rollback()
            return {"ok": False, "reason": "sold_out"}
        _insert_reservation(cur, reservation_id, "b")
    conn.commit()
    return {"ok": True, "reason": "reserved"}


@_with_retry
def reserve_pool(conn, reservation_id: str, sync=None) -> dict:
    """Strategy c (Shopify): claim one free slot from the capped pool with
    FOR UPDATE SKIP LOCKED — contending workers skip each other's locked
    rows instead of queueing on them. Empty result means every free slot is
    locked by someone else or the pool is exhausted: run a single-flight
    replenish (GET_LOCK) that frees slots held by expired reservations, then
    retry the claim once; if still empty, reject cleanly."""
    def try_claim(cur) -> tuple[int, int] | None:
        cur.execute(
            "SELECT item_id, slot_id FROM slots "
            "WHERE item_id = %s AND state = 'free' "
            "LIMIT 1 FOR UPDATE SKIP LOCKED",
            (common.ITEM_ID,),
        )
        return cur.fetchone()

    with conn.cursor() as cur:
        row = try_claim(cur)
        if row is None:
            conn.rollback()  # don't hold anything while replenishing
            _replenish_single_flight(conn)
            row = try_claim(cur)
        if row is None:
            conn.rollback()
            return {"ok": False, "reason": "sold_out"}
        _, slot_id = row
        cur.execute(
            "UPDATE slots SET state = 'claimed', reservation_id = %s "
            "WHERE item_id = %s AND slot_id = %s",
            (reservation_id, common.ITEM_ID, slot_id),
        )
        _insert_reservation(cur, reservation_id, "c")
    conn.commit()
    return {"ok": True, "reason": "reserved"}


def _replenish_single_flight(conn) -> None:
    """Free the slots of expired reservations. GET_LOCK ensures only ONE
    contender replenishes while the rest wait briefly — the thundering-herd
    guard from the Shopify design."""
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK('lab12_replenish', 2)")
        got = cur.fetchone()[0]
        if not got:
            return  # someone else is replenishing; caller just retries
        try:
            cur.execute(
                "UPDATE reservations SET state = 'expired' "
                "WHERE item_id = %s AND state = 'active' AND expires_at < NOW(3)",
                (common.ITEM_ID,),
            )
            cur.execute(
                "UPDATE slots s JOIN reservations r ON r.reservation_id = s.reservation_id "
                "SET s.state = 'free', s.reservation_id = NULL "
                "WHERE s.item_id = %s AND s.state = 'claimed' AND r.state = 'expired'",
                (common.ITEM_ID,),
            )
            conn.commit()
        finally:
            cur.execute("SELECT RELEASE_LOCK('lab12_replenish')")


HANDLERS = {
    "naive": reserve_naive,
    "a": reserve_pessimistic,
    "b": reserve_atomic,
    "c": reserve_pool,
}
