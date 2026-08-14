"""Create the v1 index and point both aliases at it.

The single most important production habit demonstrated here: the application
NEVER talks to a concrete index name. Reads go through READ_ALIAS, writes go
through WRITE_ALIAS. That indirection is what makes an atomic, zero-downtime
cutover possible later.
"""
from common import INDEX_V1, READ_ALIAS, WRITE_ALIAS, es_client, load_index_body, log, wait_for_es


def main() -> None:
    es = es_client()
    wait_for_es(es)

    if es.indices.exists(index=INDEX_V1):
        log.info("Index %s already exists, skipping bootstrap", INDEX_V1)
        return

    body = load_index_body("v1")
    es.indices.create(index=INDEX_V1, settings=body["settings"], mappings=body["mappings"])
    log.info("Created index %s", INDEX_V1)

    es.indices.update_aliases(
        actions=[
            {"add": {"index": INDEX_V1, "alias": READ_ALIAS}},
            {"add": {"index": INDEX_V1, "alias": WRITE_ALIAS}},
        ]
    )
    log.info("Aliases ready: %s -> %s, %s -> %s", READ_ALIAS, INDEX_V1, WRITE_ALIAS, INDEX_V1)


if __name__ == "__main__":
    main()
