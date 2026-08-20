"""Observational deep dive: what does a slot claim actually lock?

Shopify's observation: with a surrogate auto-increment PK the claim locked
TWO index records (the secondary index it searched, plus the clustered PK
record); making the filtered columns the PK prefix cut it to one.

We reproduce the comparison on the same table: the claim path (searches via
the composite PK (item_id, slot_id)) vs the same query forced through the
idx_state secondary index. For each, the transaction is held open while a
second connection reads performance_schema.data_locks.

Evidence is observational — lock layouts are engine/plan dependent — so this
exits 0 iff both listings printed, with no count assertion.
"""

import sys

import common

CLAIM_PK = (
    "SELECT item_id, slot_id FROM slots "
    "WHERE item_id = %s AND state = 'free' LIMIT 1 FOR UPDATE SKIP LOCKED"
)
CLAIM_SECONDARY = (
    "SELECT item_id, slot_id FROM slots FORCE INDEX (idx_state) "
    "WHERE state = 'free' LIMIT 1 FOR UPDATE SKIP LOCKED"
)


def show_locks(label: str, query: str, args: tuple) -> int:
    holder = common.connect()
    observer = common.connect(autocommit=True)
    try:
        with holder.cursor() as cur:
            cur.execute(query, args)
            row = cur.fetchone()
        with observer.cursor() as cur:
            cur.execute(
                "SELECT INDEX_NAME, LOCK_TYPE, LOCK_MODE, LOCK_DATA "
                "FROM performance_schema.data_locks "
                "WHERE OBJECT_NAME = 'slots' AND LOCK_TYPE = 'RECORD'"
            )
            locks = cur.fetchall()
        print(f"\n{label} (claimed slot: {row})")
        print(f"  record locks held: {len(locks)}")
        for index_name, lock_type, lock_mode, lock_data in locks:
            print(f"    index={index_name:<12} mode={lock_mode:<12} data={lock_data}")
        return len(locks)
    finally:
        holder.rollback()
        holder.close()
        observer.close()


def main() -> int:
    n_pk = show_locks("[1] claim via composite PK (item_id, slot_id)", CLAIM_PK,
                      (common.ITEM_ID,))
    n_sec = show_locks("[2] same claim forced through secondary index idx_state",
                       CLAIM_SECONDARY, ())
    print(
        f"\ncomposite-PK path held {n_pk} record lock(s); "
        f"secondary-index path held {n_sec}.\n"
        "Searching via a secondary index locks the secondary-index record AND\n"
        "the clustered (PK) record it points at; searching via the PK prefix\n"
        "locks only the clustered record. Fewer lock rows per claim = less\n"
        "lock-manager work and fewer conflict points on the hot path."
    )
    return 0 if (n_pk > 0 and n_sec > 0) else 1


if __name__ == "__main__":
    sys.exit(main())
