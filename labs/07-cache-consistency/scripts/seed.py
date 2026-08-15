"""Seed N_PRODUCTS rows at a known price. Idempotent (full-state upsert)."""
from common import N_PRODUCTS, TABLE, connect_mysql, log

SEED_PRICE = 10.00


def main() -> None:
    db = connect_mysql()
    rows = [(pid, SEED_PRICE) for pid in range(1, N_PRODUCTS + 1)]
    with db.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {TABLE} (id, price) VALUES (%s, %s) AS new "
            "ON DUPLICATE KEY UPDATE price=new.price, version=0",
            rows)
    db.close()
    log.info("seeded %d products at %.2f", N_PRODUCTS, SEED_PRICE)


if __name__ == "__main__":
    main()
