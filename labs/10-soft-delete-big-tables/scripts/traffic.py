"""Live traffic against ONE family's live schema: `traffic.py <family>`.

Journal contract (traffic_<family>.jsonl, acked ops only):
    {"op": "insert"|"update"|"delete_request", "table": ..., "id": ..., "ts_ms": ...}

A delete_request is journaled, not executed — deciding what "delete" means is
the job of the strategy under test (strategy_a / strategy_b / archiver). The
strategy consumes requests and appends outcomes to outcome_<family>.jsonl;
verify.py joins the two files against the database.

Updates use the *correct* app-side filter (AND deleted_at IS NULL where the
family has the column) — the forgotten-filter bug is demonstrated separately
in strategy_a, on purpose, not by accident here.
"""
import os
import random
import signal
import sys
import time

import pymysql

from common import (FAMILIES, connect, journal_path, log, make_order,
                    now_millis, append_jsonl, wait_for_mysql)

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
    family = sys.argv[1] if len(sys.argv) > 1 else "a"
    if family not in FAMILIES:
        sys.exit(f"usage: traffic.py a|b|c (got {family!r})")
    live = FAMILIES[family]["live"]
    has_flag = family in ("a", "c")  # live tables carry deleted_at
    alive = "AND deleted_at IS NULL" if has_flag else ""

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

    def pick_live_id(table: str) -> int | None:
        """Random existing (non-deleted) row id via bounded point probes."""
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(id) FROM `{live}`.`{table}`")
            max_id = cur.fetchone()[0] or 0
            for _ in range(8):
                candidate = rng.randrange(1, max_id + 1)
                cur.execute(
                    f"SELECT id FROM `{live}`.`{table}` WHERE id=%s {alive}",
                    (candidate,),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
        return None

    rng = random.Random()
    journal = journal_path(family)
    seq = 0
    ops = 0
    log.info("[%s] traffic started (journal: %s)", family, journal)

    while RUNNING:
        roll = rng.random()
        entry = None
        if roll < 0.10:
            seq += 1
            email = f"traffic-{family}-{os.getpid()}-{seq}@example.com"
            rc, rowid = exec_with_retry(
                get_conn,
                f"INSERT INTO `{live}`.users (email, name) VALUES (%s, %s)",
                (email, f"traffic user {seq}"),
            )
            if rc:
                entry = {"op": "insert", "table": "users", "id": rowid}
        elif roll < 0.55:
            uid = pick_live_id("users")
            if uid:
                order = make_order(rng, uid)
                rc, rowid = exec_with_retry(
                    get_conn,
                    f"INSERT INTO `{live}`.orders (user_id, status, amount) VALUES (%s, %s, %s)",
                    order,
                )
                if rc:
                    entry = {"op": "insert", "table": "orders", "id": rowid}
        elif roll < 0.85:
            oid = pick_live_id("orders")
            if oid:
                rc, _ = exec_with_retry(
                    get_conn,
                    f"UPDATE `{live}`.orders SET status=%s WHERE id=%s {alive}",
                    (rng.choice(["paid", "shipped", "cancelled"]), oid),
                )
                if rc:
                    entry = {"op": "update", "table": "orders", "id": oid}
        else:
            table = "users" if roll >= 0.95 else "orders"
            target = pick_live_id(table)
            if target:
                entry = {"op": "delete_request", "table": table, "id": target}

        if entry:
            entry["ts_ms"] = now_millis()
            append_jsonl(journal, entry)
            ops += 1
            if ops % 500 == 0:
                log.info("[%s] %d ops journaled", family, ops)
        time.sleep(rng.uniform(0.005, 0.02))

    log.info("[%s] traffic stopped: %d ops journaled", family, ops)


if __name__ == "__main__":
    main()
