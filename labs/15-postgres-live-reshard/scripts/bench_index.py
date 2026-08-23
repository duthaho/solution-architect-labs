"""Notion cut initial sync from ~3 days to ~12 hours, Figma from days to
hours, with the same trick: drop the destination's secondary indexes before
the initial copy, rebuild them after. Logical replication bulk-copies rows
but maintains indexes row by row. Measure it on this dataset.

Final stage: this bench tears down whatever replication exists and leaves
shard0 empty — run `make reset-shards` afterwards to rebuild the baseline."""
import time

from bootstrap import clear_replication
from common import SHARDS, conn, container_dsn, log, shard_filter

SECONDARY_INDEXES = {
    "docs_updated_idx": "CREATE INDEX docs_updated_idx ON docs (updated_at)",
    "docs_ws_updated_idx": "CREATE INDEX docs_ws_updated_idx ON docs (workspace_id, updated_at)",
}


def reset_target():
    with conn("shard0") as c:
        c.execute("TRUNCATE docs")
        c.execute("ALTER SEQUENCE docs_id_seq RESTART WITH 1 INCREMENT BY 1")
        for name, ddl in SECONDARY_INDEXES.items():
            if not c.execute(
                "SELECT 1 FROM pg_indexes WHERE indexname = %s", (name,)
            ).fetchone():
                c.execute(ddl)


def wait_synced(expected, timeout=900):
    deadline = time.monotonic() + timeout
    with conn("shard0") as c:
        while True:
            states = [s for (s,) in c.execute(
                "SELECT srsubstate FROM pg_subscription_rel").fetchall()]
            if states and all(s == "r" for s in states):
                if c.execute("SELECT count(*) FROM docs").fetchone()[0] >= expected:
                    return
            if time.monotonic() > deadline:
                raise TimeoutError(f"initial sync timed out, states={states}")
            time.sleep(0.25)


def one_round(drop_indexes):
    reset_target()
    rebuild = 0.0
    with conn("shard0") as c:
        if drop_indexes:
            for name in SECONDARY_INDEXES:
                c.execute(f"DROP INDEX {name}")
        t0 = time.monotonic()
        c.execute(
            f"CREATE SUBSCRIPTION sub_bench "
            f"CONNECTION '{container_dsn('mono')}' PUBLICATION pub_bench"
        )
    wait_synced(EXPECTED)
    copy = time.monotonic() - t0
    if drop_indexes:
        t1 = time.monotonic()
        with conn("shard0") as c:
            for ddl in SECONDARY_INDEXES.values():
                c.execute(ddl)
        rebuild = time.monotonic() - t1
    with conn("shard0") as c:
        c.execute("DROP SUBSCRIPTION sub_bench")  # also drops its slot on mono
    return copy, rebuild


def main():
    global EXPECTED
    clear_replication(["mono"] + SHARDS)
    with conn("mono") as c:
        c.execute(f"CREATE PUBLICATION pub_bench FOR TABLE docs WHERE {shard_filter(0)}")
        EXPECTED = c.execute(
            f"SELECT count(*) FROM docs WHERE {shard_filter(0)}"
        ).fetchone()[0]
    log.info("benching initial sync of %d rows (shard0 partition), 2 rounds", EXPECTED)

    kept, _ = one_round(drop_indexes=False)
    dropped, rebuild = one_round(drop_indexes=True)

    with conn("mono") as c:
        c.execute("DROP PUBLICATION pub_bench")
    reset_target()

    print()
    print(f"initial sync of {EXPECTED} rows onto shard0:")
    print(f"  indexes kept during copy      : {kept:8.2f}s")
    print(f"  indexes dropped, copy only    : {dropped:8.2f}s")
    print(f"  index rebuild afterwards      : {rebuild:8.2f}s")
    print(f"  dropped + rebuilt, total      : {dropped + rebuild:8.2f}s")
    print()
    print("(Notion: ~3 days -> ~12 hours; Figma: days -> hours. At laptop scale")
    print(" the gap is smaller — the mechanism is the same. No winner asserted:")
    print(" measure on your data before you trust anyone's ratio, including this one.)")
    print("bench left shard0 empty and replication torn down — run `make reset-shards`")


if __name__ == "__main__":
    main()
