"""The B-tree insert-locality bench: what your PK choice does to the index.

Same N rows, same 64-byte payload, batch-inserted into three tables that
differ only in primary key:

  pk_uuid4      BINARY(16), random    — every insert lands on a random leaf
                page: cold pages faulted in, pages split mid-tree
  pk_uuid7      BINARY(16), time-ordered — inserts land on the rightmost
                pages
  pk_snowflake  BIGINT, time-ordered  — rightmost pages AND half the key size

Reports rows/s and the on-disk clustered-index size
(information_schema.INNODB_TABLESPACES.FILE_SIZE). Numbers are
hardware-sensitive, so the exit code only asserts the bench ran to
completion and every table holds exactly N rows — the comparison itself is
narrated, not gated. Requires `make bootstrap`.
"""

import os
import sys
import time
import uuid

import alternatives
import common
import snowflake

N_ROWS = int(os.environ.get("BTREE_ROWS", "200000"))
BATCH = 1000
PAYLOAD = "x" * 64

TABLES = {
    "pk_uuid4": "BINARY(16)",
    "pk_uuid7": "BINARY(16)",
    "pk_snowflake": "BIGINT",
}


def keys_for(table: str) -> list:
    if table == "pk_uuid4":
        return [uuid.uuid4().bytes for _ in range(N_ROWS)]
    if table == "pk_uuid7":
        return [alternatives.uuid7().bytes for _ in range(N_ROWS)]
    gen = snowflake.Generator(1, policy="error")
    return [gen.next_id()[0] for _ in range(N_ROWS)]


def file_size(cur, table: str) -> int:
    cur.execute(
        "SELECT FILE_SIZE FROM information_schema.INNODB_TABLESPACES "
        "WHERE NAME = %s", (f"{common.DB}/{table}",),
    )
    row = cur.fetchone()
    return row[0] if row else 0


def main() -> int:
    conn = common.connect(autocommit=False)
    results = []
    with conn.cursor() as cur:
        for table, key_type in TABLES.items():
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(
                f"CREATE TABLE {table} (id {key_type} NOT NULL, "
                f"payload VARCHAR(64) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB"
            )
            conn.commit()
            keys = keys_for(table)  # generated up front: insert loop measures MySQL
            t0 = time.monotonic()
            for i in range(0, N_ROWS, BATCH):
                cur.executemany(
                    f"INSERT INTO {table} (id, payload) VALUES (%s, %s)",
                    [(k, PAYLOAD) for k in keys[i:i + BATCH]],
                )
                conn.commit()
            secs = time.monotonic() - t0
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            size = file_size(cur, table)
            results.append((table, count, secs, size))
            common.log.info("%s: %d rows in %.1fs", table, count, secs)

    print(f"\n{'table':14} {'rows':>8} {'rows/s':>8} {'file MB':>8}")
    for table, count, secs, size in results:
        print(f"{table:14} {count:>8} {count / secs:>8.0f} {size / 1e6:>8.1f}")
    print()

    ok = all(count == N_ROWS for _, count, _, _ in results)
    conn.close()
    if ok:
        common.log.info("btree bench complete — exit 0 (comparison is narrated, "
                        "not gated)")
        return 0
    common.log.error("btree bench: row counts wrong")
    return 1


if __name__ == "__main__":
    sys.exit(main())
