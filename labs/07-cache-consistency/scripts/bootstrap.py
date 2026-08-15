"""Apply the schema, flush the cache, truncate the journals. Idempotent."""
from common import (LAB_DIR, LAG_JOURNAL, READ_JOURNAL, WRITE_JOURNAL,
                    connect_mysql, connect_redis, log, wait_for_infra)


def main() -> None:
    wait_for_infra()
    schema = (LAB_DIR / "sql" / "schema.sql").read_text()
    db = connect_mysql()
    with db.cursor() as cur:
        for stmt in [s.strip() for s in schema.split(";") if s.strip()]:
            cur.execute(stmt)
    db.close()
    log.info("schema applied")

    r = connect_redis()
    r.flushdb()
    r.close()
    log.info("redis flushed (a cold cache is a correct cache)")

    for j in (WRITE_JOURNAL, READ_JOURNAL, LAG_JOURNAL):
        j.unlink(missing_ok=True)
    log.info("journals reset")


if __name__ == "__main__":
    main()
