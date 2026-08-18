"""Strategy B — the mirror "deleted" schema (the case-study design).

DELETE means: move the row family (user -> orders -> items) into
lab10_b_deleted inside ONE transaction. Ordering is forced by the FKs:
mirror-inserts parent-first, live-deletes child-first. Restore is the exact
reverse.

Correctness detail worth stealing: we snapshot the id sets FOR UPDATE before
copying. Without the locks, traffic can insert a new order for the user
between our mirror-INSERT and our live-DELETE — the DELETE would then destroy
a row that was never mirrored. Silent data loss, found only at restore time.

The move INSERT is positional (`SELECT t.*, NOW(6), %s`). That is exactly how
these systems get written — and exactly why `drift` mode breaks: ALTER the
live table, forget the mirror, and every delete starts failing (1136: column
count doesn't match). Run `strategy_b.py drift` to reproduce and repair.

Modes:
    (none)   consume delete_requests + restore round-trip demo
    drift    schema-drift drill
"""
import hashlib
import sys
import time

import pymysql

from common import (connect, count, journal_path, log, outcome_path,
                    append_jsonl, percentiles, read_jsonl, retry_txn)

LIVE = "lab10_b"
MIRROR = "lab10_b_deleted"
FAMILY = "b"


def _in(ids: list[int]) -> str:
    return "(" + ",".join(str(i) for i in ids) + ")"


def move_user(conn, user_id: int, reason: str = "user_req") -> int:
    """Move user + orders + items live -> mirror in one transaction."""
    with conn.cursor() as cur:
        # 1. Lock the family. FK inserts from traffic (new orders for this
        # user) must wait behind the X lock on the parent row.
        cur.execute(f"SELECT id FROM {LIVE}.users WHERE id=%s FOR UPDATE", (user_id,))
        if not cur.fetchone():
            conn.rollback()
            return 0
        cur.execute(f"SELECT id FROM {LIVE}.orders WHERE user_id=%s FOR UPDATE", (user_id,))
        order_ids = [r[0] for r in cur.fetchall()]
        item_ids = []
        if order_ids:
            cur.execute(f"SELECT id FROM {LIVE}.order_items "
                        f"WHERE order_id IN {_in(order_ids)} FOR UPDATE")
            item_ids = [r[0] for r in cur.fetchall()]

        # 2. Mirror-insert parent-first, from the locked id snapshot.
        cur.execute(f"INSERT INTO {MIRROR}.users SELECT u.*, NOW(6), %s "
                    f"FROM {LIVE}.users u WHERE u.id=%s", (reason, user_id))
        if order_ids:
            cur.execute(f"INSERT INTO {MIRROR}.orders SELECT o.*, NOW(6), %s "
                        f"FROM {LIVE}.orders o WHERE o.id IN {_in(order_ids)}", (reason,))
        if item_ids:
            cur.execute(f"INSERT INTO {MIRROR}.order_items SELECT i.*, NOW(6), %s "
                        f"FROM {LIVE}.order_items i WHERE i.id IN {_in(item_ids)}", (reason,))

        # 3. Live-delete child-first, same snapshot.
        if item_ids:
            cur.execute(f"DELETE FROM {LIVE}.order_items WHERE id IN {_in(item_ids)}")
        if order_ids:
            cur.execute(f"DELETE FROM {LIVE}.orders WHERE id IN {_in(order_ids)}")
        cur.execute(f"DELETE FROM {LIVE}.users WHERE id=%s", (user_id,))
    conn.commit()
    return 1 + len(order_ids) + len(item_ids)


def move_order(conn, order_id: int, reason: str = "user_req") -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {LIVE}.orders WHERE id=%s FOR UPDATE", (order_id,))
        if not cur.fetchone():
            conn.rollback()
            return 0
        cur.execute(f"SELECT id FROM {LIVE}.order_items WHERE order_id=%s FOR UPDATE",
                    (order_id,))
        item_ids = [r[0] for r in cur.fetchall()]
        cur.execute(f"INSERT INTO {MIRROR}.orders SELECT o.*, NOW(6), %s "
                    f"FROM {LIVE}.orders o WHERE o.id=%s", (reason, order_id))
        if item_ids:
            cur.execute(f"INSERT INTO {MIRROR}.order_items SELECT i.*, NOW(6), %s "
                        f"FROM {LIVE}.order_items i WHERE i.id IN {_in(item_ids)}", (reason,))
        if item_ids:
            cur.execute(f"DELETE FROM {LIVE}.order_items WHERE id IN {_in(item_ids)}")
        cur.execute(f"DELETE FROM {LIVE}.orders WHERE id=%s", (order_id,))
    conn.commit()
    return 1 + len(item_ids)


def restore_user(conn, user_id: int) -> int:
    """Reverse move: mirror -> live, inserts parent-first, deletes child-first.
    Explicit column lists — the live table must not receive _deleted_at."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {MIRROR}.orders WHERE user_id=%s", (user_id,))
        order_ids = [r[0] for r in cur.fetchall()]
        item_ids = []
        if order_ids:
            cur.execute(f"SELECT id FROM {MIRROR}.order_items "
                        f"WHERE order_id IN {_in(order_ids)}")
            item_ids = [r[0] for r in cur.fetchall()]
        cur.execute(f"INSERT INTO {LIVE}.users (id, email, name, created_at) "
                    f"SELECT id, email, name, created_at FROM {MIRROR}.users WHERE id=%s",
                    (user_id,))
        if order_ids:
            cur.execute(f"INSERT INTO {LIVE}.orders (id, user_id, status, amount, created_at) "
                        f"SELECT id, user_id, status, amount, created_at "
                        f"FROM {MIRROR}.orders WHERE id IN {_in(order_ids)}")
        if item_ids:
            cur.execute(f"INSERT INTO {LIVE}.order_items (id, order_id, sku, qty, price) "
                        f"SELECT id, order_id, sku, qty, price "
                        f"FROM {MIRROR}.order_items WHERE id IN {_in(item_ids)}")
        if item_ids:
            cur.execute(f"DELETE FROM {MIRROR}.order_items WHERE id IN {_in(item_ids)}")
        if order_ids:
            cur.execute(f"DELETE FROM {MIRROR}.orders WHERE id IN {_in(order_ids)}")
        cur.execute(f"DELETE FROM {MIRROR}.users WHERE id=%s", (user_id,))
    conn.commit()
    return 1 + len(order_ids) + len(item_ids)


def family_checksum(conn, schema: str, user_id: int) -> str:
    """Order-independent digest of one user's row family in `schema`."""
    h = hashlib.sha256()
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, email, name, created_at FROM {schema}.users "
                    f"WHERE id=%s", (user_id,))
        h.update(repr(cur.fetchall()).encode())
        cur.execute(f"SELECT id, user_id, status, amount, created_at FROM {schema}.orders "
                    f"WHERE user_id=%s ORDER BY id", (user_id,))
        orders = cur.fetchall()
        h.update(repr(orders).encode())
        if orders:
            cur.execute(f"SELECT id, order_id, sku, qty, price FROM {schema}.order_items "
                        f"WHERE order_id IN {_in([o[0] for o in orders])} ORDER BY id")
            h.update(repr(cur.fetchall()).encode())
    return h.hexdigest()[:16]


def consume_delete_requests(conn) -> None:
    done = {(r["table"], r["id"]) for r in read_jsonl(outcome_path(FAMILY))}
    pending = [r for r in read_jsonl(journal_path(FAMILY))
               if r["op"] == "delete_request" and (r["table"], r["id"]) not in done]
    lat, moved_rows = [], 0
    for req in pending:
        t0 = time.perf_counter()
        fn = move_user if req["table"] == "users" else move_order
        n = retry_txn(conn, fn, req["id"])
        lat.append((time.perf_counter() - t0) * 1000)
        moved_rows += n
        append_jsonl(outcome_path(FAMILY),
                     {"id": req["id"], "table": req["table"], "action": "moved"})
    p50, p95 = percentiles(lat)
    log.info("consumed %d delete_requests (%d rows moved)  move p50=%.1fms p95=%.1fms",
             len(pending), moved_rows, p50, p95)


def restore_roundtrip_demo(conn) -> None:
    log.info("--- Restore round-trip ---")
    with conn.cursor() as cur:
        cur.execute(f"SELECT u.id FROM {LIVE}.users u JOIN {LIVE}.orders o ON o.user_id=u.id "
                    f"GROUP BY u.id HAVING COUNT(*) >= 2 LIMIT 3")
        candidates = [r[0] for r in cur.fetchall()]
    conn.commit()  # end the read snapshot
    assert candidates, "no user with >=2 orders in family b — re-run make seed"
    # Live traffic can mutate a chosen user's orders between our checksums,
    # failing the compare even though move/restore was correct. Retry with a
    # different user; two straight mismatches would mean a real bug.
    for uid in candidates:
        before = family_checksum(conn, LIVE, uid)
        n = move_user(conn, uid, reason="roundtrip")
        in_mirror = family_checksum(conn, MIRROR, uid)
        restore_user(conn, uid)
        after = family_checksum(conn, LIVE, uid)
        log.info("user %d: moved %d rows out, restored them back", uid, n)
        log.info("checksum live-before=%s mirror=%s live-after=%s", before, in_mirror, after)
        if before == after:
            log.info("round-trip OK — this restore path is strategy B's one real advantage")
            return
        log.warning("checksum mismatch for user %d (traffic raced the compare?) "
                    "— retrying with another user", uid)
    raise AssertionError("restore did not round-trip byte-identical for any candidate")


def drill_drift(conn) -> None:
    log.info("--- Drill B1: schema drift ---")
    with conn.cursor() as cur:
        # idempotent cleanup from previous runs
        for schema in (LIVE, MIRROR):
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='orders' AND column_name='coupon'",
                (schema,))
            if cur.fetchone()[0]:
                cur.execute(f"ALTER TABLE {schema}.orders DROP COLUMN coupon")
        conn.commit()

        log.info("a feature team ships: ALTER TABLE %s.orders ADD COLUMN coupon ...", LIVE)
        cur.execute(f"ALTER TABLE {LIVE}.orders ADD COLUMN coupon VARCHAR(32) NULL")
        cur.execute(f"SELECT id FROM {LIVE}.orders LIMIT 1")
        oid = cur.fetchone()[0]
    try:
        move_order(conn, oid, reason="drift_drill")
        raise AssertionError("move unexpectedly succeeded despite drift")
    except pymysql.err.OperationalError as e:
        conn.rollback()
        log.info("next delete FAILED: %s", e)
        log.info("nobody told the mirror schema. Every delete in prod now errors "
                 "(or worse: silently maps columns wrong if counts still match)")
    with conn.cursor() as cur:
        log.info("repair: apply the same ALTER to %s.orders", MIRROR)
        cur.execute(f"ALTER TABLE {MIRROR}.orders ADD COLUMN coupon VARCHAR(32) NULL "
                    f"AFTER created_at")
    conn.commit()
    n = move_order(conn, oid, reason="drift_drill")
    log.info("after repair: move of order %d succeeded (%d rows) — "
             "every ALTER must now ship twice, forever", oid, n)
    with conn.cursor() as cur:  # leave the schema as we found it
        cur.execute(f"ALTER TABLE {LIVE}.orders DROP COLUMN coupon")
        cur.execute(f"ALTER TABLE {MIRROR}.orders DROP COLUMN coupon")
    conn.commit()


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    conn = connect(autocommit=False)
    if mode == "drift":
        drill_drift(conn)
    else:
        consume_delete_requests(conn)
        restore_roundtrip_demo(conn)
        for t in ("users", "orders", "order_items"):
            log.info("  %-12s live=%-7d mirror=%d", t, count(conn, LIVE, t),
                     count(conn, MIRROR, t))
    conn.close()
    log.info("strategy B done (mode=%s)", mode)


if __name__ == "__main__":
    main()
