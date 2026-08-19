"""The five transfer handlers. One signature:

    fn(conn, transfer_id, src, dst, amount) -> {"ok": bool, "reason": str, "retries": int}

Every handler that acks a transfer also inserts its row into `transfers`
inside the same transaction — the applied-work journal that verify.py joins
against the client-side race_<mode>.jsonl.
"""
import os
import time
from decimal import Decimal

import pymysql

# The naive handler holds this long between reading balances and writing them
# back. This is not a hack — it models the app-server think time (fee
# calculation, fraud check, an RPC) that exists in every real transfer path.
# It makes the read-windows of concurrent workers overlap deterministically,
# so the lost update reproduces every run instead of "sometimes".
HOLD_MS = int(os.environ.get("HOLD_MS", "50"))

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "10"))


def _record(cur, tid: str, mode: str, src: int, dst: int, amount: Decimal) -> None:
    cur.execute(
        "INSERT INTO transfers (transfer_id, mode, src, dst, amount) "
        "VALUES (%s, %s, %s, %s, %s)",
        (tid, mode, src, dst, amount),
    )


# --------------------------------------------------------------------- naive
def transfer_naive(conn, tid, src, dst, amount):
    """Read → check → think → write computed values. It is inside a
    transaction, and REPEATABLE READ does not save it: both racers read the
    same snapshot, both pass the balance check, and the second COMMIT
    silently overwrites the first one's debit. The double-spend."""
    with conn.cursor() as cur:
        cur.execute("SELECT balance FROM accounts WHERE id = %s", (src,))
        src_bal = Decimal(cur.fetchone()[0])
        cur.execute("SELECT balance FROM accounts WHERE id = %s", (dst,))
        dst_bal = Decimal(cur.fetchone()[0])

        if src_bal < amount:
            conn.rollback()
            return {"ok": False, "reason": "insufficient", "retries": 0}

        time.sleep(HOLD_MS / 1000.0)  # app think time — the race window

        cur.execute("UPDATE accounts SET balance = %s WHERE id = %s",
                    (src_bal - amount, src))
        cur.execute("UPDATE accounts SET balance = %s WHERE id = %s",
                    (dst_bal + amount, dst))
        _record(cur, tid, "naive", src, dst, amount)
    conn.commit()
    return {"ok": True, "reason": "", "retries": 0}
