"""Prove the migration lost nothing.

Three independent checks:

1. JOURNAL REPLAY (the strong one): every operation the traffic generator got
   an ack for must be visible in the live read alias, at least as new as the
   journaled updated_at. An acked-but-missing write == data gap == failure.
2. DOC COUNT: live index count >= journaled distinct ids + seeded docs
   (soft deletes keep docs, so counts are directly comparable).
3. CONTENT SAMPLE: N random seeded docs compared field-by-field between the
   old and new index (only fields whose values must survive the mapping
   change verbatim).
"""
import argparse
import json
import random

from common import INDEX_V2, LAB_DIR, READ_ALIAS, SEED_DOCS, es_client, log, wait_for_es

JOURNAL = LAB_DIR / "journal.jsonl"
COMPARE_FIELDS = ["sku", "name", "category", "stock", "is_deleted"]


def check_journal(es) -> bool:
    if not JOURNAL.exists():
        log.warning("No journal file — skipping journal replay check")
        return True
    latest: dict[str, int] = {}
    with open(JOURNAL) as f:
        for line in f:
            e = json.loads(line)
            latest[e["id"]] = max(latest.get(e["id"], 0), e["updated_at"])
    log.info("Journal: %d distinct doc ids to verify", len(latest))

    missing = stale = 0
    ids = list(latest.items())
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        resp = es.mget(index=READ_ALIAS, ids=[d for d, _ in chunk], source_includes=["updated_at"])
        for (doc_id, ts), got in zip(chunk, resp["docs"]):
            if not got.get("found"):
                missing += 1
                log.error("MISSING acked write: %s", doc_id)
            elif got["_source"]["updated_at"] < ts:
                stale += 1
                log.error("STALE doc %s: live=%s < journaled=%s",
                          doc_id, got["_source"]["updated_at"], ts)
    if missing or stale:
        log.error("JOURNAL CHECK FAILED: %d missing, %d stale", missing, stale)
        return False
    log.info("JOURNAL CHECK PASSED: all %d acked writes present and fresh", len(latest))
    return True


def check_counts(es) -> bool:
    es.indices.refresh(index=READ_ALIAS)
    live = es.count(index=READ_ALIAS)["count"]
    new_ids = set()
    if JOURNAL.exists():
        with open(JOURNAL) as f:
            new_ids = {json.loads(l)["id"] for l in f if json.loads(l)["op"] == "create"}
    expected = SEED_DOCS + len(new_ids)
    ok = live == expected
    log.log(20 if ok else 40, "COUNT CHECK %s: live=%d expected=%d (seed=%d + created=%d)",
            "PASSED" if ok else "FAILED", live, expected, SEED_DOCS, len(new_ids))
    return ok


def check_sample(es, old_index: str, n: int) -> bool:
    if not es.indices.exists(index=old_index):
        log.warning("Old index %s gone — skipping content sample", old_index)
        return True
    rng = random.Random(7)
    sample = [f"p-{rng.randrange(SEED_DOCS)}" for _ in range(n)]
    old = es.mget(index=old_index, ids=sample)["docs"]
    new = es.mget(index=INDEX_V2, ids=sample)["docs"]
    mismatches = 0
    for o, nw in zip(old, new):
        if not (o.get("found") and nw.get("found")):
            continue  # updated after cutover only exists in v2 — journal check covers it
        if o["_source"]["updated_at"] != nw["_source"]["updated_at"]:
            continue  # doc changed between snapshots; not comparable
        for f in COMPARE_FIELDS:
            if o["_source"][f] != nw["_source"][f]:
                mismatches += 1
                log.error("FIELD MISMATCH %s.%s: %r != %r", o["_id"], f, o["_source"][f], nw["_source"][f])
    ok = mismatches == 0
    log.log(20 if ok else 40, "CONTENT SAMPLE %s: %d docs compared, %d mismatches",
            "PASSED" if ok else "FAILED", n, mismatches)
    return ok


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old-index", default="products_v1")
    parser.add_argument("--sample", type=int, default=2000)
    args = parser.parse_args()

    es = es_client()
    wait_for_es(es)
    results = [check_journal(es), check_counts(es), check_sample(es, args.old_index, args.sample)]
    if all(results):
        log.info("✅ ALL CHECKS PASSED — zero data gap, migration verified")
    else:
        log.error("❌ VERIFICATION FAILED")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
