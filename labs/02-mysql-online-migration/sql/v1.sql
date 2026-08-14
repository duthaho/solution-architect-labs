-- Original schema. Deliberate v1 sins that force a real migration:
--   * amount FLOAT        -> money as float: imprecise, must become DECIMAL.
--                            FLOAT->DECIMAL is a type change, which in MySQL
--                            forces ALGORITHM=COPY (no INSTANT, no INPLACE).
--   * no (customer_id, created_at) index -> the query the app now needs.
CREATE TABLE orders (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id INT UNSIGNED    NOT NULL,
    status      ENUM('pending','paid','shipped','cancelled') NOT NULL DEFAULT 'pending',
    amount      FLOAT           NOT NULL,
    note        VARCHAR(255)    NOT NULL DEFAULT '',
    created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_customer (customer_id)
) ENGINE=InnoDB;
