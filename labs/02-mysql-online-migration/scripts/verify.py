"""Prove the migration lost nothing. Three independent checks:

1. JOURNAL REPLAY (the strongest): replay journal.jsonl to compute the final
   expected state of every row the app touched — last op wins. Then assert it
   against the live table: updated rows exist with the journaled values,
   deleted rows are GONE. An acknowledged write that is missing (or a deleted
   row that came back — the classic catch-up resurrection bug) is a data gap.
2. COUNT RECONCILIATION: seeded + surviving inserts - deletes == COUNT(*).
3. CONTENT SAMPLING: random rows compared field-by-field between the parked
   old table and the live table (rows untouched since cutover only), to catch
   copy corruption — e.g. FLOAT -> DECIMAL conversion garbage.

Amounts are compared with 0.01 tolerance: v1 stored money in FLOAT (that sin
is *why* we migrated), so 19.99 was really 19.9899997711...; the DECIMAL(12,2)
conversion rounds it back. The journal holds the exact intended value.

This script works against whatever schema `orders` currently has — it only
relies on v1 columns — so it also validates the post-rollback state.
"""
import json
import random
import sys
from decimal import Decimal

from common import JOURNAL, OLD_TABLE, SEED_ROWS, STATE_FILE, TABLE, connect, log, table_exists

AMOUNT_TOL = 0.01
SAMPLE = 500


def replay_journal() -> tuple[dict, int, int]:
    """Returns ({id: expected}, inserts_alive, seeded_deleted); expected is
    {'status','amount'} for live rows or None for deleted ones."""
    final: dict[int, dict | None] = {}
    if not JOURNAL.exists():
        log.warning("No journal file — was traffic running?")
        return {}, 0, 0
    with open(JOURNAL) as f:
        for line in f:
            e = json.loads(line)
            if e["op"] in ("insert", "update"):
                final[e["id"]] = {"status": e["status"], "amount": e["amount"]}
            else:
                final[e["id"]] = None
    inserts_alive = sum(1 for i, v in final.items() if i > SEED_ROWS and v is not None)
    seeded_deleted = sum(1 for i, v in final.items() if i <= SEED_ROWS and v is None)
    return final, inserts_alive, seeded_deleted


def check_journal(conn, final: dict) -> int:
    failures = 0
    ids = list(final.keys())
    rows: dict[int, tuple] = {}
    with conn.cursor() as cur:
        for i in range(0, len(ids), 1000):
            batch = ids[i:i + 1000]
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"SELECT id, status, amount FROM {TABLE} WHERE id IN ({placeholders})", batch)
            for r in cur.fetchall():
                rows[r[0]] = (r[1], float(r[2]))

    for id_, expected in final.items():
        row = rows.get(id_)
        if expected is None:
            if row is not None:
                log.error("  GAP: id %d was deleted (acked) but EXISTS — resurrected row", id_)
                failures += 1
        elif row is None:
            log.error("  GAP: id %d was written (acked) but is MISSING", id_)
            failures += 1
        elif row[0] != expected["status"] or abs(row[1] - expected["amount"]) > AMOUNT_TOL:
            log.error("  STALE: id %d is %s, journal says %s", id_, row, expected)
            failures += 1
    return failures


def check_counts(conn, inserts_alive: int, seeded_deleted: int) -> int:
    expected = SEED_ROWS + inserts_alive - seeded_deleted
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        actual = cur.fetchone()[0]
    if actual != expected:
        log.error("  COUNT mismatch: expected %d (%d seeded + %d inserts - %d deletes), got %d",
                  expected, SEED_ROWS, inserts_alive, seeded_deleted, actual)
        return 1
    log.info("  count OK: %d rows (%d seeded + %d inserts - %d deletes)",
             actual, SEED_ROWS, inserts_alive, seeded_deleted)
    return 0


def check_sampling(conn, journaled_ids: set) -> int:
    """Compare untouched rows between the parked v1 table and the live table."""
    if not (table_exists(conn, OLD_TABLE) and STATE_FILE.exists()):
        log.info("  sampling skipped (%s not present — before migration or after rollback)", OLD_TABLE)
        return 0
    failures = checked = 0
    rng = random.Random(7)
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(id) FROM {OLD_TABLE}")
        max_id = cur.fetchone()[0]
        candidate_ids = [rng.randrange(1, max_id + 1) for _ in range(SAMPLE * 2)]
        candidate_ids = [i for i in candidate_ids if i not in journaled_ids][:SAMPLE]
        for id_ in candidate_ids:
            cur.execute(
                f"SELECT customer_id, status, amount, note, created_at FROM {OLD_TABLE} WHERE id=%s",
                (id_,))
            old = cur.fetchone()
            if old is None:
                continue  # id gap in the old table (pre-migration delete)
            cur.execute(
                f"SELECT customer_id, status, amount, note, created_at FROM {TABLE} WHERE id=%s",
                (id_,))
            new = cur.fetchone()
            checked += 1
            if new is None:
                log.error("  SAMPLE GAP: id %d in %s but missing from %s (not journaled)",
                          id_, OLD_TABLE, TABLE)
                failures += 1
                continue
            old_f = (old[0], old[1], round(float(old[2]), 2), old[3], old[4])
            new_f = (new[0], new[1], float(new[2]) if isinstance(new[2], Decimal) else new[2],
                     new[3], new[4])
            if old_f[:2] != new_f[:2] or abs(old_f[2] - new_f[2]) > AMOUNT_TOL or old_f[3:] != new_f[3:]:
                log.error("  SAMPLE CORRUPT: id %d old=%s new=%s", id_, old_f, new_f)
                failures += 1
    log.info("  sampled %d untouched rows field-by-field", checked)
    return failures


def main() -> None:
    conn = connect()
    final, inserts_alive, seeded_deleted = replay_journal()
    deletes = sum(1 for v in final.values() if v is None)
    log.info("Journal: %d touched rows (%d net new, %d deleted)", len(final), inserts_alive, deletes)

    log.info("CHECK 1: journal replay (every acked write, last-op-wins)")
    f1 = check_journal(conn, final)
    log.info("CHECK 2: count reconciliation")
    f2 = check_counts(conn, inserts_alive, seeded_deleted)
    log.info("CHECK 3: content sampling old vs new")
    f3 = check_sampling(conn, set(final.keys()))
    conn.close()

    total = f1 + f2 + f3
    if total:
        log.error("❌ VERIFICATION FAILED: %d problems (journal=%d count=%d sample=%d)",
                  total, f1, f2, f3)
        sys.exit(1)
    log.info("✅ VERIFIED: every acked write present, deletes propagated, counts match, samples clean")


if __name__ == "__main__":
    main()
