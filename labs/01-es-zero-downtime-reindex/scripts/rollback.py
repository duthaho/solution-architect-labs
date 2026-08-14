"""Emergency rollback: atomically point both aliases back at the old index.

IMPORTANT production caveat (explained in the README): writes accepted by v2
after the cutover do NOT exist in v1. Rolling back is instant for reads, but
if writes happened on v2 you must either replay them into v1 (reverse
catch-up using the same updated_at technique) or accept losing them.
This script performs a reverse catch-up first, then swaps.
"""
import argparse

from common import INDEX_V2, READ_ALIAS, WRITE_ALIAS, alias_target, es_client, log, wait_for_es
from reindex import reindex_range


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--to", default="products_v1", help="index to roll back to")
    parser.add_argument("--since-ms", type=int, default=None,
                        help="replay v2 writes newer than this into the old index first")
    args = parser.parse_args()

    es = es_client()
    wait_for_es(es)
    current = alias_target(es, WRITE_ALIAS)
    if current == args.to:
        raise SystemExit(f"Aliases already point at {args.to}")

    if args.since_ms:
        log.info("Reverse catch-up: replaying %s writes since %d into %s",
                 current, args.since_ms, args.to)
        es.indices.put_settings(index=args.to, settings={"index.blocks.write": None})
        es.indices.refresh(index=current)
        reindex_range(es, current, args.to, args.since_ms, "reverse catch-up")

    es.indices.update_aliases(
        actions=[
            {"remove": {"index": current, "alias": READ_ALIAS}},
            {"remove": {"index": current, "alias": WRITE_ALIAS}},
            {"add": {"index": args.to, "alias": READ_ALIAS}},
            {"add": {"index": args.to, "alias": WRITE_ALIAS}},
        ]
    )
    log.info("ROLLED BACK: %s / %s -> %s (was %s)", READ_ALIAS, WRITE_ALIAS, args.to, current)
    log.info("%s left intact for diagnosis", INDEX_V2)


if __name__ == "__main__":
    main()
