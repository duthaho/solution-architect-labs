"""Bulk-load the orders table with multi-row INSERTs.

Nothing clever here on purpose: seeding is the boring part. The only
production-relevant detail is batching — one row per INSERT would take
an hour; 5000 rows per statement takes seconds.
"""
import random
import time

from common import SEED_ROWS, TABLE, connect, log, make_order, wait_for_mysql

BATCH = 5000


def main() -> None:
    wait_for_mysql()
    conn = connect()
    rng = random.Random(42)
    t0 = time.time()
    inserted = 0

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        if cur.fetchone()[0] > 0:
            raise SystemExit(f"{TABLE} is not empty — run `make clean` for a fresh start")

        while inserted < SEED_ROWS:
            n = min(BATCH, SEED_ROWS - inserted)
            rows = [make_order(rng) for _ in range(n)]
            cur.executemany(
                f"INSERT INTO {TABLE} (customer_id, status, amount, note) VALUES (%s, %s, %s, %s)",
                [(r["customer_id"], r["status"], r["amount"], r["note"]) for r in rows],
            )
            inserted += n
            if inserted % 100_000 == 0:
                rate = inserted / (time.time() - t0)
                log.info("  seeded %d/%d rows (%.0f rows/s)", inserted, SEED_ROWS, rate)

    conn.close()
    log.info("Seeded %d rows in %.1fs", inserted, time.time() - t0)


if __name__ == "__main__":
    main()
