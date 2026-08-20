"""Reset the world to a known-good state: one hot item at CAPACITY.

MySQL: items counter row, CAPACITY free slots, no reservations.
Redis: the legacy remaining-counter set to CAPACITY.
Artifacts: journals deleted, seed.json baseline written, phase set to 'redis'
(the migration story starts with Redis as the source of truth).
"""

import json

import common


def main() -> None:
    conn = common.connect(autocommit=True)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE items")
        cur.execute("TRUNCATE TABLE reservations")
        cur.execute("TRUNCATE TABLE slots")
        cur.execute(
            "INSERT INTO items (id, capacity, reserved, sold) VALUES (%s, %s, 0, 0)",
            (common.ITEM_ID, common.CAPACITY),
        )
        cur.executemany(
            "INSERT INTO slots (item_id, slot_id, state) VALUES (%s, %s, 'free')",
            [(common.ITEM_ID, s) for s in range(1, common.CAPACITY + 1)],
        )
    conn.close()

    r = common.redis_client()
    r.set(common.REDIS_KEY, common.CAPACITY)
    r.delete(common.REDIS_KEY + ":acks")

    for p in common.LAB_DIR.glob("burst_*.jsonl"):
        p.unlink()

    common.SEED_PATH.write_text(
        json.dumps({"item_id": common.ITEM_ID, "capacity": common.CAPACITY}) + "\n"
    )
    common.write_phase("redis")
    common.log.info(
        "seeded: item %d capacity=%d, %d free slots, redis=%d, phase=redis",
        common.ITEM_ID, common.CAPACITY, common.CAPACITY, common.CAPACITY,
    )


if __name__ == "__main__":
    main()
