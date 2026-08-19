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


# --------------------------------------------------- a: pessimistic locking
def transfer_pessimistic(conn, tid, src, dst, amount):
    """SELECT ... FOR UPDATE on BOTH accounts, always locking in sorted-id
    order so two opposing transfers can never hold one lock each and wait on
    the other (the deadlock drill shows what happens without this). The
    balance check then reads a locked, current row — the race window is
    gone. Deadlock/lock-wait (1213/1205) still gets a bounded retry: under
    load InnoDB may pick us as a victim for unrelated reasons."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                balances = {}
                for acct in sorted((src, dst)):
                    cur.execute(
                        "SELECT balance FROM accounts WHERE id = %s FOR UPDATE",
                        (acct,),
                    )
                    balances[acct] = Decimal(cur.fetchone()[0])
                if balances[src] < amount:
                    conn.rollback()
                    return {"ok": False, "reason": "insufficient", "retries": attempt}
                cur.execute(
                    "UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (amount, src),
                )
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (amount, dst),
                )
                _record(cur, tid, "a", src, dst, amount)
            conn.commit()
            return {"ok": True, "reason": "", "retries": attempt}
        except pymysql.MySQLError as e:
            conn.rollback()
            code = e.args[0] if e.args else 0
            if code in (1205, 1213) and attempt < MAX_RETRIES:
                time.sleep(0.02 * (attempt + 1))
                continue
            raise
    return {"ok": False, "reason": "retries_exhausted", "retries": MAX_RETRIES}


# --------------------------------------------------- b: optimistic locking
def transfer_optimistic(conn, tid, src, dst, amount):
    """No locks on read. Every write is conditional on the version observed:
    UPDATE ... WHERE id=? AND version=? bumps the version, and rowcount 0
    means someone got there first — roll back and retry from a fresh read.
    The stale write can never land; the cost is retries under contention
    (count them: bench makes this the story)."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT balance, version FROM accounts WHERE id = %s", (src,))
                src_bal, src_ver = cur.fetchone()
                src_bal = Decimal(src_bal)
                cur.execute(
                    "SELECT balance, version FROM accounts WHERE id = %s", (dst,))
                dst_bal, dst_ver = cur.fetchone()
                dst_bal = Decimal(dst_bal)

                if src_bal < amount:
                    conn.rollback()
                    return {"ok": False, "reason": "insufficient", "retries": attempt}

                cur.execute(
                    "UPDATE accounts SET balance = %s, version = version + 1 "
                    "WHERE id = %s AND version = %s",
                    (src_bal - amount, src, src_ver),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    continue  # version moved under us — retry
                cur.execute(
                    "UPDATE accounts SET balance = %s, version = version + 1 "
                    "WHERE id = %s AND version = %s",
                    (dst_bal + amount, dst, dst_ver),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    continue
                _record(cur, tid, "b", src, dst, amount)
            conn.commit()
            return {"ok": True, "reason": "", "retries": attempt}
        except pymysql.MySQLError as e:
            conn.rollback()
            code = e.args[0] if e.args else 0
            if code in (1205, 1213) and attempt < MAX_RETRIES:
                time.sleep(0.02 * (attempt + 1))
                continue
            raise
    return {"ok": False, "reason": "conflict", "retries": MAX_RETRIES}


# ------------------------------------------------ c: atomic conditional write
def transfer_atomic(conn, tid, src, dst, amount):
    """No read at all. The debit IS the check:

        UPDATE accounts SET balance = balance - x WHERE id = ? AND balance >= x

    InnoDB evaluates the predicate on the current, locked row — there is no
    snapshot to go stale. rowcount 0 means insufficient funds, atomically.
    The smallest correct fix, and the one that can't express business logic
    that needs the read value (fees, limits, fraud checks)."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE accounts SET balance = balance - %s "
                    "WHERE id = %s AND balance >= %s",
                    (amount, src, amount),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return {"ok": False, "reason": "insufficient", "retries": attempt}
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (amount, dst),
                )
                _record(cur, tid, "c", src, dst, amount)
            conn.commit()
            return {"ok": True, "reason": "", "retries": attempt}
        except pymysql.MySQLError as e:
            conn.rollback()
            code = e.args[0] if e.args else 0
            if code in (1205, 1213) and attempt < MAX_RETRIES:
                time.sleep(0.02 * (attempt + 1))
                continue
            raise
    return {"ok": False, "reason": "retries_exhausted", "retries": MAX_RETRIES}
