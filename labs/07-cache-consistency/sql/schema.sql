-- The cached entity. Deliberately tiny: this lab is about the cache protocol,
-- not the data model.
--
-- `version` is bumped by every price write. The `versioned` strategy does NOT
-- read it (its version pointer lives in Redis, see README §3.4) — the column
-- exists so drills and the auditor can always ask the DB "how many times has
-- this row changed", and so humans can eyeball staleness at a glance.
CREATE TABLE IF NOT EXISTS products (
    id         INT PRIMARY KEY,
    price      DECIMAL(10, 2) NOT NULL,
    version    BIGINT         NOT NULL DEFAULT 0,
    updated_at TIMESTAMP(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                              ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE = InnoDB;
