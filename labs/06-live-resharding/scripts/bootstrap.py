"""Create the orders table on all three nodes and set the router to `single`.

Also asserts, before anything else runs, that Python's shard_for() and MySQL's
CRC32() agree on who owns which user. The entire lab — router, backfill
filter, misplacement scan — leans on those two functions being identical; a
silent divergence here would make every later check lie. Never trust a hash
function you haven't cross-checked.
"""
from common import DB, LAB_DIR, NODES, SHARDS, TABLE, connect, log, shard_for, wait_for_node, write_mode


def table_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (DB, TABLE))
        return cur.fetchone()[0] > 0


def check_hash_agreement(conn) -> None:
    probe = [1, 7, 42, 1999, 123456789]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT " + ", ".join(f"MOD(CRC32({u}), {len(SHARDS)})" for u in probe))
        sql_shards = [SHARDS[int(x)] for x in cur.fetchone()]
    py_shards = [shard_for(u) for u in probe]
    if sql_shards != py_shards:
        raise SystemExit(
            f"SHARD FUNCTION MISMATCH: python {py_shards} vs mysql {sql_shards} — "
            "CRC32 coercion drifted; nothing built on top of this can be trusted")
    log.info("shard function cross-check OK: python crc32(str) == mysql CRC32 on %s", probe)


def main() -> None:
    for node in NODES:
        wait_for_node(node)

    schema = (LAB_DIR / "sql" / "schema.sql").read_text()
    for node in NODES:
        conn = connect(node)
        if table_exists(conn):
            raise SystemExit(f"{node} already has {DB}.{TABLE} — run `make clean` for a fresh start")
        with conn.cursor() as cur:
            cur.execute(schema)
        if node == "mono":
            check_hash_agreement(conn)
        conn.close()
        log.info("created %s.%s on %s", DB, TABLE, node)

    write_mode("single")
    log.info("Bootstrap complete: schema on mono + %d shards, router=single. Next: make seed",
             len(SHARDS))
    log.info("State files land in %s", LAB_DIR)


if __name__ == "__main__":
    main()
