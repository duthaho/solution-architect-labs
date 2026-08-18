"""Strategy A — the `deleted_at` column. Rows never leave the table.

What this script does, in order:
  1. Consumes pending delete_requests from traffic_a.jsonl (cascade soft
     delete in one transaction), appending outcomes to outcome_a.jsonl.
  2. Mass-soft-deletes ~30% of seeded orders (deterministic id % 10 < 3) so
     the table has enough tombstones to measure anything.
  3. Drill A1 — the forgotten WHERE: runs the revenue report with and without
     the filter and shows the silent wrong answer.
  4. Drill A2 — resurrection: soft-deletes a user, then tries to re-register
     the same email. UNIQUE(email) doesn't know about deleted_at.
  5. Measures: read p50/p95 filtered vs unfiltered, and table/index bytes —
     proving that "deleting" 30% of the table freed exactly nothing.

Exit code 0 only if both drills reproduce and every consumed request got an
outcome.
"""
import random
import time

import pymysql

from common import (connect, count, journal_path, log, outcome_path,
                    append_jsonl, percentiles, read_jsonl, table_bytes)

LIVE = "lab10_a"
FAMILY = "a"


def soft_delete_user(conn, user_id: int) -> int:
    """Cascade soft delete user -> orders -> items in ONE transaction.
    Returns rows flagged."""
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {LIVE}.order_items oi JOIN {LIVE}.orders o ON oi.order_id=o.id "
            f"SET oi.deleted_at=NOW(6) WHERE o.user_id=%s AND oi.deleted_at IS NULL",
            (user_id,))
        n = cur.rowcount
        cur.execute(f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                    f"WHERE user_id=%s AND deleted_at IS NULL", (user_id,))
        n += cur.rowcount
        cur.execute(f"UPDATE {LIVE}.users SET deleted_at=NOW(6) "
                    f"WHERE id=%s AND deleted_at IS NULL", (user_id,))
        n += cur.rowcount
    conn.commit()
    return n


def soft_delete_order(conn, order_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(f"UPDATE {LIVE}.order_items SET deleted_at=NOW(6) "
                    f"WHERE order_id=%s AND deleted_at IS NULL", (order_id,))
        n = cur.rowcount
        cur.execute(f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                    f"WHERE id=%s AND deleted_at IS NULL", (order_id,))
        n += cur.rowcount
    conn.commit()
    return n


def consume_delete_requests(conn) -> list[float]:
    done = {(r["table"], r["id"]) for r in read_jsonl(outcome_path(FAMILY))}
    pending = [r for r in read_jsonl(journal_path(FAMILY))
               if r["op"] == "delete_request" and (r["table"], r["id"]) not in done]
    lat = []
    for req in pending:
        t0 = time.perf_counter()
        if req["table"] == "users":
            soft_delete_user(conn, req["id"])
        else:
            soft_delete_order(conn, req["id"])
        lat.append((time.perf_counter() - t0) * 1000)
        append_jsonl(outcome_path(FAMILY),
                     {"id": req["id"], "table": req["table"], "action": "soft_deleted"})
    p50, p95 = percentiles(lat)
    log.info("consumed %d delete_requests  delete p50=%.1fms p95=%.1fms",
             len(pending), p50, p95)
    return lat


def mass_soft_delete(conn) -> None:
    """Flag ~30% of seeded orders + their items, in id-range batches."""
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(id) FROM {LIVE}.orders")
        max_id = cur.fetchone()[0]
        step = 20000
        for lo in range(0, max_id + 1, step):
            cur.execute(
                f"UPDATE {LIVE}.order_items oi JOIN {LIVE}.orders o ON oi.order_id=o.id "
                f"SET oi.deleted_at=NOW(6) "
                f"WHERE o.id BETWEEN %s AND %s AND o.id %% 10 < 3 AND oi.deleted_at IS NULL",
                (lo, lo + step - 1))
            cur.execute(
                f"UPDATE {LIVE}.orders SET deleted_at=NOW(6) "
                f"WHERE id BETWEEN %s AND %s AND id %% 10 < 3 AND deleted_at IS NULL",
                (lo, lo + step - 1))
            conn.commit()
    dead = count(conn, LIVE, "orders", "deleted_at IS NOT NULL")
    log.info("mass soft delete done in %.1fs — %d orders now flagged", time.time() - t0, dead)


def drill_forgotten_where(conn) -> None:
    log.info("--- Drill A1: the forgotten WHERE ---")
    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(SUM(amount),0) FROM {LIVE}.orders WHERE status='paid'")
        wrong = float(cur.fetchone()[0])
        cur.execute(f"SELECT COALESCE(SUM(amount),0) FROM {LIVE}.orders "
                    f"WHERE status='paid' AND deleted_at IS NULL")
        right = float(cur.fetchone()[0])
    log.info("revenue report WITHOUT filter: %12.2f   <- silently includes deleted rows", wrong)
    log.info("revenue report WITH    filter: %12.2f", right)
    assert wrong > right, "expected the unfiltered report to overcount"
    log.info("overcount: %.2f (%.1f%%) — no error, no warning, just a wrong number",
             wrong - right, (wrong - right) / float(right) * 100)


def drill_resurrection(conn) -> None:
    log.info("--- Drill A2: resurrection vs UNIQUE(email) ---")
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, email FROM {LIVE}.users WHERE deleted_at IS NULL "
                    f"AND email LIKE 'user%%@example.com' LIMIT 1")
        uid, email = cur.fetchone()
    soft_delete_user(conn, uid)
    log.info("user %d (%s) soft-deleted — the product says the account is gone", uid, email)
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {LIVE}.users (email, name) VALUES (%s, %s)",
                        (email, "returning customer"))
        conn.commit()
        raise AssertionError("re-registration unexpectedly succeeded")
    except pymysql.err.IntegrityError as e:
        conn.rollback()
        log.info("re-registration FAILED: %s", e)
        log.info("the 'deleted' row still owns the email. Fixes and their traps: README §3.2")


def measure(conn) -> None:
    log.info("--- Measurements ---")
    rng = random.Random(7)
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(id) FROM {LIVE}.users")
        max_uid = cur.fetchone()[0]

        def bench(where: str) -> tuple[float, float]:
            lat = []
            for _ in range(300):
                uid = rng.randrange(1, max_uid + 1)
                t0 = time.perf_counter()
                cur.execute(f"SELECT id, status, amount FROM {LIVE}.orders "
                            f"WHERE user_id=%s {where} ORDER BY id DESC LIMIT 10", (uid,))
                cur.fetchall()
                lat.append((time.perf_counter() - t0) * 1000)
            return percentiles(lat)

        p50u, p95u = bench("")
        p50f, p95f = bench("AND deleted_at IS NULL")
        cur.execute(f"ANALYZE TABLE {LIVE}.orders")
        cur.fetchall()
    live_n = count(conn, LIVE, "orders", "deleted_at IS NULL")
    dead_n = count(conn, LIVE, "orders", "deleted_at IS NOT NULL")
    data_b, index_b = table_bytes(conn, LIVE, "orders")
    log.info("read p50/p95 unfiltered: %.2f/%.2f ms   filtered: %.2f/%.2f ms",
             p50u, p95u, p50f, p95f)
    log.info("orders: %d live + %d dead rows — data %.1f MB, index %.1f MB",
             live_n, dead_n, data_b / 1e6, index_b / 1e6)
    log.info("every query pays for the dead rows forever; DELETE freed 0 bytes "
             "because there was no DELETE")


def main() -> None:
    conn = connect(autocommit=False)
    consume_delete_requests(conn)
    mass_soft_delete(conn)
    drill_forgotten_where(conn)
    drill_resurrection(conn)
    measure(conn)
    conn.close()
    log.info("strategy A done")


if __name__ == "__main__":
    main()
