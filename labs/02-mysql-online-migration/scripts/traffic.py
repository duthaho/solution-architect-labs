"""Continuous live write traffic — the thing that makes this problem hard.

Simulates the application writing during the migration:
- ~60% new orders (INSERT), ~35% updates, ~5% HARD deletes.
- Every acknowledged op is appended to journal.jsonl. The journal is our
  ground truth: verify.py later proves that every acknowledged write survived
  the migration — that is the "no data gap" guarantee.

Hard deletes are deliberate. Lab 01 (timestamp catch-up on Elasticsearch) had
to soft-delete because a deleted row leaves nothing to copy. Binlog-based
migration captures DELETE as a first-class event, so this lab deletes for
real — run verify.py and watch it prove the deletes propagated.

The cutover contract, seen from the app side: during the ~1s cutover the
migration holds a WRITE lock on the table. Unlike Elasticsearch (which
*rejects* writes with an error the client must retry), MySQL simply makes the
statement *wait* in the lock queue and then proceed. So a well-behaved app
needs no special code at all — it just observes a brief latency spike. The
retry loop below exists for deadlocks and connection failures, which are
routine MySQL client hygiene, not cutover-specific.
"""
import json
import random
import signal
import sys
import time

import pymysql

from common import JOURNAL, SEED_ROWS, TABLE, connect, log, make_order, wait_for_mysql

RUNNING = True
RETRYABLE = {
    1205,  # lock wait timeout
    1213,  # deadlock
    2003,  # can't connect
    2006,  # server gone away
    2013,  # lost connection during query
}


def _stop(*_):
    global RUNNING
    RUNNING = False


def exec_with_retry(get_conn, sql: str, params: tuple, max_wait_s: float = 60.0):
    """Run one write, retrying transient errors. Returns (rowcount, lastrowid)."""
    deadline = time.time() + max_wait_s
    backoff = 0.2
    while time.time() < deadline:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount, cur.lastrowid
        except pymysql.MySQLError as e:
            code = e.args[0] if e.args else 0
            if code in RETRYABLE:
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
                continue
            raise
    return None, None


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    wait_for_mysql()
    conn_holder = {"conn": connect()}

    def get_conn():
        try:
            conn_holder["conn"].ping(reconnect=True)
        except Exception:
            conn_holder["conn"] = connect()
        return conn_holder["conn"]

    rng = random.Random()
    ops = delayed = 0

    log.info("Traffic generator started (journal: %s)", JOURNAL)
    with open(JOURNAL, "a") as journal:
        while RUNNING:
            roll = rng.random()
            order = make_order(rng)
            if roll < 0.60:
                sql = f"INSERT INTO {TABLE} (customer_id, status, amount, note) VALUES (%s, %s, %s, %s)"
                params = (order["customer_id"], order["status"], order["amount"], order["note"])
                op = "insert"
            elif roll < 0.95:
                target = rng.randrange(1, SEED_ROWS + 1)
                sql = f"UPDATE {TABLE} SET status=%s, amount=%s, note=%s WHERE id=%s"
                params = (order["status"], order["amount"], order["note"], target)
                op = "update"
            else:
                target = rng.randrange(1, SEED_ROWS + 1)
                sql = f"DELETE FROM {TABLE} WHERE id=%s"
                params = (target,)
                op = "delete"

            t0 = time.time()
            rowcount, lastrowid = exec_with_retry(get_conn, sql, params)
            wait = time.time() - t0
            if wait > 0.5:
                delayed += 1
                log.warning("Write op=%s delayed %.2fs (cutover lock queue?)", op, wait)
            if rowcount is None:
                log.error("Write op=%s NOT acked within retry budget — data gap!", op)
                sys.exit(2)

            # Journal only ops that touched a row. rowcount==0 means the target
            # id was already deleted — the op was a no-op, and journaling it
            # would make verification demand a row that legitimately isn't there.
            if rowcount > 0:
                entry = {"op": op, "ts": int(time.time() * 1000)}
                if op == "insert":
                    entry.update(id=lastrowid, status=order["status"], amount=order["amount"])
                elif op == "update":
                    entry.update(id=params[3], status=order["status"], amount=order["amount"])
                else:
                    entry.update(id=params[0])
                journal.write(json.dumps(entry) + "\n")
                journal.flush()

            ops += 1
            if ops % 500 == 0:
                log.info("  %d ops journaled (%d delayed >0.5s)", ops, delayed)
            time.sleep(rng.uniform(0.005, 0.02))

    log.info("Traffic stopped: %d ops, %d delayed writes, 0 lost", ops, delayed)


if __name__ == "__main__":
    main()
