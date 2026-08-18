"""Create (or reset) the three schema families from sql/schema.sql."""
from common import LAB_DIR, connect, log, wait_for_mysql


def main() -> None:
    wait_for_mysql()
    raw = (LAB_DIR / "sql" / "schema.sql").read_text()
    # Drop `--` comment lines before splitting on ';' — comments may contain
    # semicolons.
    sql = "\n".join(l for l in raw.splitlines() if not l.lstrip().startswith("--"))
    conn = connect()
    with conn.cursor() as cur:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_schema, COUNT(*) FROM information_schema.tables "
            "WHERE table_schema LIKE 'lab10%' GROUP BY table_schema ORDER BY table_schema"
        )
        for schema, n in cur.fetchall():
            log.info("schema %-18s %d tables", schema, n)
    conn.close()
    log.info("bootstrap done")


if __name__ == "__main__":
    main()
