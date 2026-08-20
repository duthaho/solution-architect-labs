"""Expiry sweep: release reservations past their TTL.

In ONE transaction: count the overdue `active` rows, give counter-mode
capacity back to items.reserved, free pool-mode slots, then mark the rows
`expired`. Idempotent — a second run finds zero overdue active rows.
"""

import common


def sweep() -> int:
    conn = common.connect()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM reservations "
            "WHERE item_id = %s AND state = 'active' AND expires_at < NOW(3) "
            "AND mode IN ('naive','a','b')",
            (common.ITEM_ID,),
        )
        counter_overdue = cur.fetchone()[0]
        if counter_overdue:
            cur.execute(
                "UPDATE items SET reserved = reserved - %s WHERE id = %s",
                (counter_overdue, common.ITEM_ID),
            )
        # pool mode: free slots whose reservation is overdue
        cur.execute(
            "UPDATE slots s JOIN reservations r ON r.reservation_id = s.reservation_id "
            "SET s.state = 'free', s.reservation_id = NULL "
            "WHERE s.item_id = %s AND s.state = 'claimed' "
            "AND r.state = 'active' AND r.expires_at < NOW(3)",
            (common.ITEM_ID,),
        )
        n = cur.execute(
            "UPDATE reservations SET state = 'expired' "
            "WHERE item_id = %s AND state = 'active' AND expires_at < NOW(3)",
            (common.ITEM_ID,),
        )
    conn.commit()
    conn.close()
    common.log.info("sweep: expired %d reservations (%d counter-mode)", n, counter_overdue)
    return n


if __name__ == "__main__":
    sweep()
