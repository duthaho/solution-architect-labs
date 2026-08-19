-- Lab 11 — concurrent money transfers.
--
-- Design notes:
--
-- * accounts.balance has deliberately NO CHECK constraint. MySQL 8.0.16+
--   enforces CHECK, and a DB-level floor would change the failure mode we
--   want to observe: the naive handler's corruption must stay SILENT (it
--   writes stale computed values that are individually >= 0 while creating
--   money), and unconditional delta-writes would turn overdraft into a
--   noisy constraint error instead of a lesson. Non-negativity is the
--   business invariant; verify.py enforces it after the fact, each strategy
--   enforces it (or fails to) in flight.
--
-- * accounts.version exists only for strategy b (optimistic locking).
--   The other strategies ignore it.
--
-- * transfers is the applied-work journal: one row per transfer that a
--   strategy acked, keyed by client-generated transfer_id so verify.py can
--   join DB state against the race_<mode>.jsonl journal exactly-once.
--
-- * entries + balance_cache belong to strategy d (append-only ledger).
--   entries is the source of truth (per-account monotonic seq, opening
--   balance is entry seq=1); balance_cache is the materialized read model
--   whose row lock doubles as the per-account serialization point.

CREATE DATABASE IF NOT EXISTS lab11;
USE lab11;

CREATE TABLE IF NOT EXISTS accounts (
    id      INT PRIMARY KEY,
    balance DECIMAL(18,2) NOT NULL,
    version INT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id CHAR(36) PRIMARY KEY,
    mode        VARCHAR(8)    NOT NULL,
    src         INT           NOT NULL,
    dst         INT           NOT NULL,
    amount      DECIMAL(18,2) NOT NULL,
    created_at  TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS entries (
    account_id  INT           NOT NULL,
    seq         INT           NOT NULL,
    amount      DECIMAL(18,2) NOT NULL,  -- signed: debit < 0, credit > 0
    transfer_id CHAR(36)      NULL,      -- NULL for the opening entry
    PRIMARY KEY (account_id, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS balance_cache (
    account_id INT PRIMARY KEY,
    balance    DECIMAL(18,2) NOT NULL,
    last_seq   INT           NOT NULL
) ENGINE=InnoDB;
