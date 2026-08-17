"""Seed SEED_ROWS users in the v1 shape (name only). Idempotent: tops up to
SEED_ROWS and does nothing if already there."""
import random
import time

import common as c

FIRST = ["Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Leslie", "Radia",
         "Ken", "Dennis", "Margaret", "Tim", "Vint", "Frances", "John", "Kathleen"]
LAST = ["Lovelace", "Hopper", "Turing", "Dijkstra", "Liskov", "Knuth", "Lamport",
        "Perlman", "Thompson", "Ritchie", "Hamilton", "Berners-Lee", "Cerf",
        "Allen", "Backus", "Booth"]


def main() -> None:
    rng = random.Random(9)
    with c.connect().cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {c.TABLE}")
        have = cur.fetchone()[0]
        if have >= c.SEED_ROWS:
            c.log(f"already seeded ({have} rows)")
            return
        t0 = time.time()
        batch, todo = [], c.SEED_ROWS - have
        for i in range(todo):
            name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
            batch.append((name, f"user{have + i}@example.com"))
            if len(batch) == 5000:
                cur.executemany(
                    f"INSERT INTO {c.TABLE} (name, email) VALUES (%s, %s)", batch)
                batch.clear()
        if batch:
            cur.executemany(f"INSERT INTO {c.TABLE} (name, email) VALUES (%s, %s)", batch)
        c.log(f"seeded {todo} rows in {time.time() - t0:.1f}s "
              f"(total {c.SEED_ROWS})")


if __name__ == "__main__":
    main()
