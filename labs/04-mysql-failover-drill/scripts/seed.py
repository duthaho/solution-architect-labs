"""Seed the events table. Volume is not the point of this lab (lab 02 covers
big-table pain); 10k rows just makes the table non-trivial and gives the writer
a starting seq."""
from common import SEED_ROWS, connect_via_router, log, query_one


def main() -> None:
    conn = connect_via_router()
    start = (query_one(conn, "SELECT COALESCE(MAX(seq), 0) AS m FROM events")["m"]) + 1
    batch = 1000
    with conn.cursor() as cur:
        for lo in range(start, start + SEED_ROWS, batch):
            rows = [(seq, f"seed-{seq}") for seq in range(lo, min(lo + batch, start + SEED_ROWS))]
            cur.executemany("INSERT INTO events (seq, payload) VALUES (%s, %s)", rows)
    total = query_one(conn, "SELECT COUNT(*) AS c, MAX(seq) AS m FROM events")
    log.info("seeded %d rows (table now: %d rows, max seq %d)", SEED_ROWS, total["c"], total["m"])
    conn.close()


if __name__ == "__main__":
    main()
