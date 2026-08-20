"""Apply sql/schema.sql and pre-seed the worker-lease rows (0..WORKER_SLOTS-1)."""

import sys

import common


def main() -> int:
    common.wait_for_mysql()
    sql = (common.LAB_DIR / "sql" / "schema.sql").read_text()
    conn = common.connect(db=None, autocommit=True)
    with conn.cursor() as cur:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            cur.execute(stmt)
        # Pre-seed the claimable worker-id rows so a claim is always a contest
        # over existing rows (no racy INSERT path).
        cur.executemany(
            "INSERT IGNORE INTO lab13.worker_leases (worker_id, owner, expires_at) "
            "VALUES (%s, NULL, 0)",
            [(w,) for w in range(common.WORKER_SLOTS)],
        )
        cur.execute("SELECT COUNT(*) FROM lab13.worker_leases")
        n = cur.fetchone()[0]
    conn.close()
    common.log.info("schema applied; %d worker-lease slots seeded", n)
    return 0 if n == common.WORKER_SLOTS else 1


if __name__ == "__main__":
    sys.exit(main())
