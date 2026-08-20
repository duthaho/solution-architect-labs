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


HANDLERS = {
    "naive": reserve_naive,
}
