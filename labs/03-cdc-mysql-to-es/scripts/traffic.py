"""Continuous live write traffic against MySQL — and ONLY against MySQL.

That restriction is the whole architecture (README §3.1): the application
writes to exactly one place, the source of truth. It does not dual-write to
Elasticsearch, it does not publish events, it does not know the pipeline
exists. Everything downstream is derived from the binlog.

- ~60% INSERTs, ~35% UPDATEs, ~5% hard DELETEs.
- Every op that touched a row is journaled (journal.jsonl). verify.py later
  proves every acknowledged MySQL write became visible in Elasticsearch —
  including the deletes.
"""
import json
import random
import signal
import sys
import time

import pymysql

from common import JOURNAL, SEED_ROWS, TABLE, connect_mysql, log, make_order, wait_for_mysql

RUNNING = True
RETRYABLE = {1205, 1213, 2003, 2006, 2013}


def _stop(*_):
    global RUNNING
    RUNNING = False


def exec_with_retry(get_conn, sql: str, params: tuple, max_wait_s: float = 60.0):
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
    holder = {"conn": connect_mysql()}

    def get_conn():
        try:
            holder["conn"].ping(reconnect=True)
        except Exception:
            holder["conn"] = connect_mysql()
        return holder["conn"]

    rng = random.Random()
    ops = 0

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

            rowcount, lastrowid = exec_with_retry(get_conn, sql, params)
            if rowcount is None:
                log.error("Write op=%s NOT acked within retry budget", op)
                sys.exit(2)

            if rowcount > 0:  # no-ops (target already deleted) make no claims
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
                log.info("  %d ops journaled", ops)
            time.sleep(rng.uniform(0.005, 0.02))

    log.info("Traffic stopped: %d ops journaled", ops)


if __name__ == "__main__":
    main()
