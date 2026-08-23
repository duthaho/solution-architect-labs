DROP TABLE IF EXISTS docs;

CREATE TABLE docs (
    id           bigserial,
    workspace_id int         NOT NULL,
    title        text        NOT NULL,
    body         text        NOT NULL,
    rev          int         NOT NULL DEFAULT 1,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

-- The shard key must be covered by the replica identity: a row-filtered
-- publication that publishes UPDATE/DELETE rejects every update on the table
-- otherwise ("cannot update table"). Must exist on every node BEFORE any
-- publication or subscription is created — replica identity changes are not
-- retroactive, and one pre-change record poisons a subscription permanently.
CREATE UNIQUE INDEX docs_shardkey_idx ON docs (workspace_id, id);
ALTER TABLE docs REPLICA IDENTITY USING INDEX docs_shardkey_idx;

-- Secondary indexes: realistic read paths, and the payload for the
-- keep-vs-drop initial-sync bench.
CREATE INDEX docs_updated_idx ON docs (updated_at);
CREATE INDEX docs_ws_updated_idx ON docs (workspace_id, updated_at);
