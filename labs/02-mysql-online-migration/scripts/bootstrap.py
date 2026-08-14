"""Create the database, the v1 `orders` table, and the migration changelog table.

The changelog table is created up-front, in peacetime, because it is
infrastructure, not migration state: the heartbeats that measure replication
lag and the cutover marker both travel through it (this is gh-ost's `*_ghc`
table). It is tiny and permanent.
"""
from common import CHANGELOG_TABLE, DB, LAB_DIR, TABLE, connect, log, table_exists, wait_for_mysql


def main() -> None:
    wait_for_mysql()
    conn = connect(db=None)
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
        cur.execute(f"USE {DB}")

        if table_exists(conn, TABLE):
            raise SystemExit(f"Table {TABLE} already exists — run `make clean` for a fresh start")

        cur.execute((LAB_DIR / "sql" / "v1.sql").read_text())
        log.info("Created %s.%s (v1 schema: amount FLOAT, no composite index)", DB, TABLE)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CHANGELOG_TABLE} (
                id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                kind       VARCHAR(16) NOT NULL,
                value      VARCHAR(64) NOT NULL,
                created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                PRIMARY KEY (id)
            ) ENGINE=InnoDB
        """)
        log.info("Created %s.%s (heartbeats + cutover markers)", DB, CHANGELOG_TABLE)
    conn.close()
    log.info("Bootstrap complete. Next: make seed")


if __name__ == "__main__":
    main()
