"""Reconciliation: the safety net under the pipeline. Trust, but verify.

CDC done right still drifts in real life: someone writes to ES directly, a
bug in the projector maps a field wrong for a week, Kafka retention expires
past an outage, a mapping change silently drops a field. The fix is a
periodic full comparison against the source of truth — MySQL wins, always.

Chunked by PK range, three defect classes per chunk:
  MISSING  in MySQL, not in ES         -> repair: upsert from MySQL
  STALE    in both, fields differ      -> repair: upsert from MySQL
  ORPHAN   in ES, not in MySQL         -> repair: delete from ES

Run it while the pipeline is LIVE and you must expect false positives: a row
can change between reading MySQL and reading ES. Production reconcilers
re-check candidates after a delay (or compare updated_at watermarks) instead
of repairing on first sight. This lab keeps the single-pass version and
documents the caveat; run it after traffic stops (or accept a re-run).

This is also your DR story: `reconcile.py --repair` against an EMPTY index
is a full rebuild of the projection from the source of truth.
"""
import argparse
import sys
import time
from datetime import timezone

from elasticsearch.helpers import bulk

from common import ES_INDEX, TABLE, connect_mysql, es_client, log, row_to_doc, wait_for_es

CHUNK = 5000
AMOUNT_TOL = 0.005


def mysql_chunk(cur, lo: int, hi: int) -> dict[int, dict]:
    cur.execute(
        f"SELECT id, customer_id, status, amount, note, created_at, updated_at "
        f"FROM {TABLE} WHERE id >= %s AND id < %s", (lo, hi))
    out = {}
    for r in cur.fetchall():
        out[r[0]] = {
            "id": r[0], "customer_id": r[1], "status": r[2], "amount": str(r[3]),
            "note": r[4],
            "created_at": int(r[5].replace(tzinfo=timezone.utc).timestamp() * 1000),
            "updated_at": int(r[6].replace(tzinfo=timezone.utc).timestamp() * 1000),
        }
    return out


def es_chunk(es, lo: int, hi: int) -> dict[int, dict]:
    docs = {}
    resp = es.search(
        index=ES_INDEX,
        query={"range": {"id": {"gte": lo, "lt": hi}}},
        size=CHUNK * 2, _source=True)
    for hit in resp["hits"]["hits"]:
        docs[int(hit["_id"])] = hit["_source"]
    return docs


def differs(mysql_row: dict, es_doc: dict) -> bool:
    return (es_doc["customer_id"] != mysql_row["customer_id"]
            or es_doc["status"] != mysql_row["status"]
            or abs(es_doc["amount"] - float(mysql_row["amount"])) > AMOUNT_TOL
            or es_doc["note"] != mysql_row["note"]
            or es_doc["updated_at"] != mysql_row["updated_at"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repair", action="store_true",
                        help="fix drift (MySQL is the source of truth)")
    args = parser.parse_args()

    conn = connect_mysql()
    es = es_client()
    wait_for_es(es)
    es.indices.refresh(index=ES_INDEX)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),0) FROM {TABLE}")
        # MIN/MAX on BIGINT UNSIGNED come back as Decimal — a tiny cousin of
        # the §3.5 type traps, this time in the Python driver.
        min_id, max_id = (int(x) for x in cur.fetchone())

        missing, stale, orphans = [], [], []
        scanned = 0
        t0 = time.time()
        for lo in range(min_id, max_id + 1, CHUNK):
            hi = lo + CHUNK
            rows = mysql_chunk(cur, lo, hi)
            docs = es_chunk(es, lo, hi)
            scanned += len(rows)
            for id_, row in rows.items():
                if id_ not in docs:
                    missing.append(row)
                elif differs(row, docs[id_]):
                    stale.append(row)
            orphans.extend(id_ for id_ in docs if id_ not in rows)

        # Anything in ES OUTSIDE MySQL's id span is an orphan by definition —
        # no chunk sweep needed, two range queries cover ±infinity. (First
        # version of this lab swept one chunk past MAX(id) and an orphan at
        # id=99999999 sailed straight past it. Found by drill 3.)
        for outside in ({"lt": min_id}, {"gt": max_id}):
            resp = es.search(index=ES_INDEX, query={"range": {"id": outside}},
                             size=10000, _source=False)
            orphans.extend(int(h["_id"]) for h in resp["hits"]["hits"])
        log.info("Scanned %d rows / chunks of %d in %.1fs", scanned, CHUNK, time.time() - t0)

    drift = len(missing) + len(stale) + len(orphans)
    log.info("Drift report: %d missing, %d stale, %d orphans", len(missing), len(stale), len(orphans))
    for r in missing[:5]:
        log.info("  MISSING id=%d", r["id"])
    for r in stale[:5]:
        log.info("  STALE   id=%d", r["id"])
    for i in orphans[:5]:
        log.info("  ORPHAN  id=%d", i)

    if not drift:
        log.info("✅ No drift: ES is a faithful projection of MySQL")
        return

    if not args.repair:
        log.error("❌ Drift detected. Rerun with --repair to fix from the source of truth.")
        sys.exit(1)

    actions = [{"_op_type": "index", "_index": ES_INDEX, "_id": r["id"],
                "_source": row_to_doc(r)} for r in missing + stale]
    actions += [{"_op_type": "delete", "_index": ES_INDEX, "_id": i} for i in orphans]
    bulk(es, actions, raise_on_error=False)
    es.indices.refresh(index=ES_INDEX)
    log.info("Repaired %d docs from MySQL. Re-run reconcile to confirm clean.", len(actions))


if __name__ == "__main__":
    main()
