-- The orders table, identical on the monolith and on every shard.
--
-- Two deliberate design points:
--
-- 1. PRIMARY KEY (user_id, seq) — no AUTO_INCREMENT. A global auto-increment
--    id dies the moment there is more than one writer: shard0 and shard1
--    would both mint id=500001. Real systems switch to app-generated ids
--    (Snowflake), interleaved increments, or — as here — a key that is
--    naturally scoped to the shard key. (user_id, seq) is unique, orderable,
--    and every row's shard is computable from its own PK. See README §3.1.
--
-- 2. updated_at is EXCLUDED from consistency checksums: the mono copy and
--    the shard copy of the same logical write are stamped at slightly
--    different times. It exists for humans debugging, not for the verifier.
CREATE TABLE orders (
    user_id    BIGINT       NOT NULL,
    seq        BIGINT       NOT NULL,
    status     VARCHAR(16)  NOT NULL,
    amount     DECIMAL(12,2) NOT NULL,
    note       VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                            ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, seq)
) ENGINE=InnoDB;
