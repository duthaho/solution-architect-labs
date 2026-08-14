"""Bulk-load the source table BEFORE the connector exists.

Deliberate ordering: the interesting problem is syncing a table that is
already big — Debezium's initial consistent snapshot (existing rows as op:'r'
events) followed by a seamless handoff to binlog streaming is the whole
point. Registering the connector first would make the snapshot trivially
empty and hide it.
"""
import random
import time

from common import SEED_ROWS, TABLE, connect_mysql, log, make_order, wait_for_mysql

BATCH = 5000


def main() -> None:
    wait_for_mysql()
    conn = connect_mysql()
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
                log.info("  seeded %d/%d rows (%.0f rows/s)",
                         inserted, SEED_ROWS, inserted / (time.time() - t0))

    conn.close()
    log.info("Seeded %d rows in %.1fs", inserted, time.time() - t0)


if __name__ == "__main__":
    main()
