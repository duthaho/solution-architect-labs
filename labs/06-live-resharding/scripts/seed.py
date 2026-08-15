"""Seed the monolith with SEED_ROWS orders across N_USERS users.

Per-user seq is contiguous 1..k — that contiguity IS the correctness probe:
from here on, for every user, COUNT(*) == MAX(seq) must hold on whatever node
serves reads (traffic has no deletes). A lost row shows up as count < max on
the very next read-check, at the application layer, not just in the verifier.

User sizes are drawn from a wide range (50..450 rows) so shards end up close
to balanced in aggregate while individual users differ — like real tenants.
"""
import random
import time

from common import N_USERS, SEED_ROWS, TABLE, connect, log, make_values, wait_for_node

BATCH = 5000


def main() -> None:
    wait_for_node("mono")
    conn = connect("mono")
    rng = random.Random(42)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        if cur.fetchone()[0] > 0:
            raise SystemExit(f"{TABLE} on mono is not empty — run `make clean` for a fresh start")

    # Draw per-user row counts, then scale to hit SEED_ROWS exactly.
    counts = {u: rng.randint(50, 450) for u in range(1, N_USERS + 1)}
    scale = SEED_ROWS / sum(counts.values())
    counts = {u: max(1, round(c * scale)) for u, c in counts.items()}
    counts[1] += SEED_ROWS - sum(counts.values())  # absorb rounding drift

    t0 = time.time()
    inserted = 0
    batch: list[tuple] = []
    with conn.cursor() as cur:
        for user, k in counts.items():
            for seq in range(1, k + 1):
                status, amount, note = make_values(rng)
                batch.append((user, seq, status, amount, note))
                if len(batch) >= BATCH:
                    cur.executemany(
                        f"INSERT INTO {TABLE} (user_id, seq, status, amount, note) "
                        "VALUES (%s, %s, %s, %s, %s)", batch)
                    inserted += len(batch)
                    batch.clear()
                    if inserted % 100_000 == 0:
                        log.info("  seeded %d/%d rows (%.0f rows/s)",
                                 inserted, SEED_ROWS, inserted / (time.time() - t0))
        if batch:
            cur.executemany(
                f"INSERT INTO {TABLE} (user_id, seq, status, amount, note) "
                "VALUES (%s, %s, %s, %s, %s)", batch)
            inserted += len(batch)

    conn.close()
    log.info("Seeded %d rows for %d users in %.1fs (mono only — shards stay empty until backfill)",
             inserted, N_USERS, time.time() - t0)


if __name__ == "__main__":
    main()
