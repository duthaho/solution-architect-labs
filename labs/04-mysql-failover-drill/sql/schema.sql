-- The only table in this lab. `seq` is the writer's monotonically increasing
-- application-level sequence number; the UNIQUE key is what lets a retried
-- insert (ambiguous commit during failover) fail loudly instead of duplicating.
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    seq        BIGINT          NOT NULL,
    payload    VARCHAR(255)    NOT NULL,
    created_at TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_seq (seq)
) ENGINE = InnoDB;
