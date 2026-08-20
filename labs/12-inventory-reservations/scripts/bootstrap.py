"""Create the lab12 schema from sql/schema.sql."""

import common


def main() -> None:
    common.wait_for_mysql()
    sql = (common.LAB_DIR / "sql" / "schema.sql").read_text()
    statements = [
        s.strip()
        for s in "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        ).split(";")
        if s.strip()
    ]
    conn = common.connect(db=None, autocommit=True)
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.close()
    common.log.info("schema applied: %d statements", len(statements))


if __name__ == "__main__":
    main()
