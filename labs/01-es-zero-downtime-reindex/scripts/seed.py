"""Seed the v1 index with a large volume of product documents.

Production-relevant techniques used here:
- Bulk indexing with parallel workers (the only sane way to load millions of docs).
- refresh_interval=-1 during load, restored afterwards: segments are not
  refreshed per second while loading, which speeds up ingestion dramatically.
- Deterministic doc IDs (p-<n>) so updates later hit real documents.
"""
import random
import time

from elasticsearch.helpers import parallel_bulk

from common import SEED_DOCS, WRITE_ALIAS, alias_target, es_client, log, now_millis, wait_for_es

WORDS = (
    "alpha nova terra quantum pixel forge ember crystal falcon cedar onyx atlas "
    "zephyr cobalt lumen raven summit delta harbor prism willow ridge aurora nimbus"
).split()
CATEGORIES = ["electronics", "home", "outdoors", "toys", "books", "fashion", "grocery"]


def make_doc(i: int, rng: random.Random) -> dict:
    name = " ".join(rng.sample(WORDS, 3)).title()
    return {
        "sku": f"SKU-{i:09d}",
        "name": name,
        "description": " ".join(rng.choices(WORDS, k=12)),
        "category": rng.choice(CATEGORIES),
        "tags": rng.sample(WORDS, 2),
        "price": round(rng.uniform(1, 999), 2),
        "stock": rng.randint(0, 500),
        "is_deleted": False,
        "updated_at": now_millis(),
    }


def gen_actions(n: int):
    rng = random.Random(42)
    for i in range(n):
        yield {"_index": WRITE_ALIAS, "_id": f"p-{i}", "_source": make_doc(i, rng)}


def main() -> None:
    es = es_client()
    wait_for_es(es)
    index = alias_target(es, WRITE_ALIAS)

    log.info("Seeding %d docs into %s (via alias %s)", SEED_DOCS, index, WRITE_ALIAS)
    es.indices.put_settings(index=index, settings={"refresh_interval": "-1"})
    start = time.time()

    ok = failed = 0
    for success, item in parallel_bulk(
        es, gen_actions(SEED_DOCS), thread_count=4, chunk_size=2000, raise_on_error=False
    ):
        if success:
            ok += 1
        else:
            failed += 1
            log.error("Bulk failure: %s", item)
        if ok % 100_000 == 0 and ok:
            rate = ok / (time.time() - start)
            log.info("  %d docs indexed (%.0f docs/s)", ok, rate)

    es.indices.put_settings(index=index, settings={"refresh_interval": "1s"})
    es.indices.refresh(index=index)
    elapsed = time.time() - start
    log.info("Done: %d ok, %d failed in %.1fs (%.0f docs/s)", ok, failed, elapsed, ok / elapsed)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
