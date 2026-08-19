"""Create the lab11 schema from sql/schema.sql."""
import common


def main() -> None:
    common.wait_for_mysql()
    raw = (common.LAB_DIR / "sql" / "schema.sql").read_text()
    # Strip comment lines BEFORE splitting: comments may contain semicolons.
    sql = "\n".join(
        line for line in raw.splitlines() if not line.strip().startswith("--")
    )
    conn = common.connect(db=None, autocommit=True)
    with conn.cursor() as cur:
        for stmt in sql.split(";"):
            if stmt.strip():
                cur.execute(stmt)
    conn.close()
    common.log.info("schema ready: accounts, transfers, entries, balance_cache")


if __name__ == "__main__":
    main()
