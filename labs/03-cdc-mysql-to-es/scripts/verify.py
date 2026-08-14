"""Prove the pipeline lost nothing — across THREE systems this time.

0. CONVERGENCE WAIT: CDC is asynchronous by nature; "in sync" only means
   "once the pipeline drains". Poll until MySQL count == ES count (stable),
   with a timeout — if it never converges, something is stuck (check
   `make status`).
1. JOURNAL REPLAY: replay journal.jsonl (last op wins) and assert every
   acknowledged MySQL write is visible in Elasticsearch: upserts present
   with journaled values, deletes ABSENT. A resurrected or missing doc is
   an end-to-end pipeline bug, regardless of which hop dropped it.
2. COUNT RECONCILIATION: journal-derived expected == MySQL == ES.
3. CONTENT SAMPLING: random rows compared field-by-field MySQL vs ES —
   catches type-mapping corruption (DECIMAL-as-string parsing, epoch-millis
   dates) that count checks can't see.
"""
import json
import sys
import time
from datetime import timezone

from common import ES_INDEX, JOURNAL, SEED_ROWS, TABLE, connect_mysql, es_client, log, wait_for_es

AMOUNT_TOL = 0.01
SAMPLE = 500
CONVERGE_TIMEOUT_S = 120


def wait_converged(conn, es) -> None:
    deadline = time.time() + CONVERGE_TIMEOUT_S
    last = None
    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            mysql_count = cur.fetchone()[0]
        es.indices.refresh(index=ES_INDEX)
        es_count = es.count(index=ES_INDEX)["count"]
        if mysql_count == es_count and (mysql_count, es_count) == last:
            log.info("Converged: MySQL == ES == %d rows", mysql_count)
            return
        if (mysql_count, es_count) != last:
            log.info("  waiting for pipeline drain: MySQL=%d ES=%d", mysql_count, es_count)
            last = (mysql_count, es_count)
        time.sleep(2)
    raise SystemExit("Pipeline never converged — is the projector/connector running? (make status)")


def replay_journal() -> tuple[dict, int, int]:
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


def check_journal(es, final: dict) -> int:
    failures = 0
    ids = list(final.keys())
    for i in range(0, len(ids), 1000):
        batch = ids[i:i + 1000]
        resp = es.mget(index=ES_INDEX, ids=[str(x) for x in batch])
        for id_, hit in zip(batch, resp["docs"]):
            expected = final[id_]
            if expected is None:
                if hit["found"]:
                    log.error("  GAP: id %d deleted in MySQL (acked) but EXISTS in ES", id_)
                    failures += 1
            elif not hit["found"]:
                log.error("  GAP: id %d written to MySQL (acked) but MISSING in ES", id_)
                failures += 1
            else:
                src = hit["_source"]
                if (src["status"] != expected["status"]
                        or abs(src["amount"] - expected["amount"]) > AMOUNT_TOL):
                    log.error("  STALE: id %d ES=%s journal=%s", id_,
                              (src["status"], src["amount"]), expected)
                    failures += 1
    return failures


def check_counts(conn, es, inserts_alive: int, seeded_deleted: int) -> int:
    expected = SEED_ROWS + inserts_alive - seeded_deleted
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        mysql_count = cur.fetchone()[0]
    es_count = es.count(index=ES_INDEX)["count"]
    if not (expected == mysql_count == es_count):
        log.error("  COUNT mismatch: journal-expected=%d MySQL=%d ES=%d",
                  expected, mysql_count, es_count)
        return 1
    log.info("  counts OK: journal-expected == MySQL == ES == %d", es_count)
    return 0


def check_sampling(conn, es, journaled_ids: set) -> int:
    import random
    failures = checked = 0
    rng = random.Random(7)
    candidates = [i for i in (rng.randrange(1, SEED_ROWS + 1) for _ in range(SAMPLE * 2))
                  if i not in journaled_ids][:SAMPLE]
    with conn.cursor() as cur:
        for id_ in candidates:
            cur.execute(
                f"SELECT customer_id, status, amount, note, updated_at FROM {TABLE} WHERE id=%s",
                (id_,))
            row = cur.fetchone()
            if row is None:
                continue  # deleted pre-journal? (shouldn't happen, but not our claim)
            doc = es.options(ignore_status=404).get(index=ES_INDEX, id=str(id_))
            checked += 1
            if not doc.get("found"):
                log.error("  SAMPLE GAP: id %d in MySQL but missing in ES", id_)
                failures += 1
                continue
            src = doc["_source"]
            updated_ms = int(row[4].replace(tzinfo=timezone.utc).timestamp() * 1000)
            if (src["customer_id"] != row[0] or src["status"] != row[1]
                    or abs(src["amount"] - float(row[2])) > AMOUNT_TOL
                    or src["note"] != row[3]
                    or src["updated_at"] != updated_ms):
                log.error("  SAMPLE MISMATCH: id %d mysql=%s es=%s", id_, row, src)
                failures += 1
    log.info("  sampled %d untouched rows field-by-field across both stores", checked)
    return failures


def main() -> None:
    conn = connect_mysql()
    es = es_client()
    wait_for_es(es)

    log.info("CHECK 0: convergence (CDC is async — 'consistent' means 'after drain')")
    wait_converged(conn, es)

    final, inserts_alive, seeded_deleted = replay_journal()
    deletes = sum(1 for v in final.values() if v is None)
    log.info("Journal: %d touched rows (%d net new, %d deleted)", len(final), inserts_alive, deletes)

    log.info("CHECK 1: journal replay against ES (every acked write, last-op-wins)")
    f1 = check_journal(es, final)
    log.info("CHECK 2: three-way count reconciliation")
    f2 = check_counts(conn, es, inserts_alive, seeded_deleted)
    log.info("CHECK 3: content sampling MySQL vs ES")
    f3 = check_sampling(conn, es, set(final.keys()))
    conn.close()

    total = f1 + f2 + f3
    if total:
        log.error("❌ VERIFICATION FAILED: %d problems (journal=%d count=%d sample=%d)",
                  total, f1, f2, f3)
        sys.exit(1)
    log.info("✅ VERIFIED: every acked write reached ES, deletes propagated, "
             "counts match across all three systems, samples clean")


if __name__ == "__main__":
    main()
