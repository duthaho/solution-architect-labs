-- Lab 08 schema. Four tables, one lesson: correctness lives in which of
-- these are written in the SAME transaction.

-- The effect target. Balance updates are the least forgiving effect there
-- is: `balance = balance + x` applied twice is silent corruption, not an
-- error. (Compare lab 03, where the ES upsert was naturally idempotent —
-- here nothing is free.)
CREATE TABLE IF NOT EXISTS accounts (
    id            INT PRIMARY KEY,
    balance_cents BIGINT NOT NULL
);

-- The producer's source of truth: the payment service records what it
-- decided BEFORE telling anyone. One row per logical payment, written by
-- producer.py. audit.py replays this table to compute expected balances —
-- each unique event applied exactly once. Any drift between that and
-- `accounts` is corruption, measured in cents.
CREATE TABLE IF NOT EXISTS payments (
    event_id     CHAR(36) PRIMARY KEY,
    account_id   INT NOT NULL,
    amount_cents BIGINT NOT NULL,
    created_at   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- The consumer's dedupe ledger. The PRIMARY KEY *is* the mechanism: the
-- idempotent consumer INSERTs the event_id and applies the balance UPDATE
-- in the same transaction; a redelivered event hits a duplicate key and the
-- whole effect is skipped. This only works because insert and effect commit
-- or roll back together — a Redis SETNX "did I see this?" check cannot give
-- you that (README §4).
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     CHAR(36) PRIMARY KEY,
    processed_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- The producer's outbox (phase 3). In --mode outbox the producer writes
-- payments + outbox in ONE transaction and never touches Kafka; relay.py
-- polls unpublished rows and publishes them. Crash anywhere: the event is
-- either in the DB (relay will deliver it) or nowhere (caller sees the
-- error) — the "DB committed but publish lost" ghost of drill 3 cannot
-- exist. published_at is bookkeeping, not a guarantee: relay can crash
-- between publish and mark, so downstream still needs the dedupe above.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id     CHAR(36) NOT NULL,
    account_id   INT NOT NULL,
    amount_cents BIGINT NOT NULL,
    created_at   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    published_at TIMESTAMP(3) NULL,
    KEY idx_unpublished (published_at, id)
);
