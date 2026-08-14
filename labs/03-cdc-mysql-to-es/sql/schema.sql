-- Source-of-truth table. Same shape as lab 02's, with the money column
-- already DECIMAL (lab 02 fixed that sin; this lab inherits the fix).
CREATE TABLE orders (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id INT UNSIGNED    NOT NULL,
    status      ENUM('pending','paid','shipped','cancelled') NOT NULL DEFAULT 'pending',
    amount      DECIMAL(12,2)   NOT NULL,
    note        VARCHAR(255)    NOT NULL DEFAULT '',
    created_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at  DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_customer (customer_id)
) ENGINE=InnoDB;
