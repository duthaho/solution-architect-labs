"""Seed all three families with identical data from the same RNG seed.

SEED_ROWS is the number of orders per family; users = SEED_ROWS/5,
order_items = SEED_ROWS*3 (so ratios are users:orders:items = 1:5:15).
"""
import random
import sys

from common import (FAMILIES, SEED, SEED_ROWS, connect, log, make_item,
                    make_order, make_user)

BATCH = 5000


def seed_family(family: str, live: str) -> None:
    rng = random.Random(SEED)  # same seed per family => identical content
    n_users = max(SEED_ROWS // 5, 1)
    n_orders = SEED_ROWS
    n_items = SEED_ROWS * 3

    conn = connect(autocommit=False)
    cur = conn.cursor()

    def flush(table: str, cols: str, rows: list[tuple]) -> None:
        if not rows:
            return
        ph = "(" + ",".join(["%s"] * len(rows[0])) + ")"
        cur.executemany(f"INSERT INTO `{live}`.`{table}` ({cols}) VALUES {ph}", rows)
        conn.commit()

    rows = []
    for i in range(1, n_users + 1):
        rows.append(make_user(rng, i))
        if len(rows) >= BATCH:
            flush("users", "email, name", rows)
            rows = []
    flush("users", "email, name", rows)
    log.info("[%s] seeded %d users", family, n_users)

    rows, done = [], 0
    for _ in range(n_orders):
        rows.append(make_order(rng, rng.randrange(1, n_users + 1)))
        if len(rows) >= BATCH:
            flush("orders", "user_id, status, amount", rows)
            done += len(rows)
            rows = []
            if done % 100_000 < BATCH:
                log.info("[%s] seeded %d/%d orders", family, done, n_orders)
    flush("orders", "user_id, status, amount", rows)
    log.info("[%s] seeded %d orders", family, n_orders)

    rows, done = [], 0
    for _ in range(n_items):
        rows.append(make_item(rng, rng.randrange(1, n_orders + 1)))
        if len(rows) >= BATCH:
            flush("order_items", "order_id, sku, qty, price", rows)
            done += len(rows)
            rows = []
            if done % 300_000 < BATCH:
                log.info("[%s] seeded %d/%d order_items", family, done, n_items)
    flush("order_items", "order_id, sku, qty, price", rows)
    log.info("[%s] seeded %d order_items", family, n_items)

    cur.close()
    conn.close()


def main() -> None:
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for family, cfg in FAMILIES.items():
        if only and family != only:
            continue
        seed_family(family, cfg["live"])
    log.info("seed done (SEED_ROWS=%d per family)", SEED_ROWS)


if __name__ == "__main__":
    main()
