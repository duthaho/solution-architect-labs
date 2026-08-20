-- Lab 12: flash-sale inventory reservations.
--
-- Design notes:
--
-- * `items` carries the counter model (strategies naive/a/b): `reserved` and
--   `sold` are mutated under contention; capacity is never mutated. There is
--   deliberately NO CHECK constraint stopping oversell — the point of the lab
--   is to show which *access patterns* prevent it, and the naive strategy
--   must be able to oversell so the verifier can catch it.
--
-- * `reservations` is the applied-work journal, keyed by the CLIENT-generated
--   reservation_id (UUID). The PK is the idempotency guard for dual-writes
--   and retries: a duplicate write collides on the PK instead of silently
--   double-booking. Lifecycle: active -> committed | expired.
--   Release-after-commit (refunds) is out of scope.
--
-- * `slots` is the Shopify-style capped pool (strategy c): exactly `capacity`
--   rows per item, each row a claimable unit of inventory. Claiming is
--   `SELECT ... FOR UPDATE SKIP LOCKED` on state='free'. The PRIMARY KEY is
--   the composite (item_id, slot_id) ON PURPOSE: the columns we filter on are
--   the PK prefix, so a claim locks index rows of the PK only — Shopify saw
--   2 lock rows per claim with a surrogate auto-increment PK vs 1 with the
--   composite PK (see scripts/locks.py for the evidence).

CREATE DATABASE IF NOT EXISTS lab12;
USE lab12;

CREATE TABLE IF NOT EXISTS items (
  id        INT           NOT NULL PRIMARY KEY,
  capacity  INT           NOT NULL,
  reserved  INT           NOT NULL DEFAULT 0,
  sold      INT           NOT NULL DEFAULT 0
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS reservations (
  reservation_id  CHAR(36)     NOT NULL PRIMARY KEY,
  item_id         INT          NOT NULL,
  mode            VARCHAR(8)   NOT NULL,
  state           ENUM('active','committed','expired') NOT NULL DEFAULT 'active',
  expires_at      TIMESTAMP(3) NULL,
  created_at      TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_item_state (item_id, state)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS slots (
  item_id         INT       NOT NULL,
  slot_id         INT       NOT NULL,
  state           ENUM('free','claimed') NOT NULL DEFAULT 'free',
  reservation_id  CHAR(36)  NULL,
  PRIMARY KEY (item_id, slot_id),
  KEY idx_state (state)
) ENGINE=InnoDB;
