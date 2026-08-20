CREATE DATABASE IF NOT EXISTS lab13;
USE lab13;

-- Worker-ID leases: the coordination table. A generator owns a worker_id only
-- while its lease is unexpired and the owner token matches. Rows are
-- pre-seeded by bootstrap (0..15) so claiming is always an UPDATE-style
-- contest over existing rows, never a racy INSERT.
CREATE TABLE IF NOT EXISTS worker_leases (
  worker_id   INT          NOT NULL,
  owner       VARCHAR(64)  NULL,      -- claimant's unique token (NULL = free)
  expires_at  BIGINT       NOT NULL DEFAULT 0,  -- unix ms (past = reclaimable)
  PRIMARY KEY (worker_id)
) ENGINE=InnoDB;

-- The DB auto-increment comparison target (D6): "just let MySQL hand out IDs".
CREATE TABLE IF NOT EXISTS seq_autoinc (
  id      BIGINT NOT NULL AUTO_INCREMENT,
  payload TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB;
