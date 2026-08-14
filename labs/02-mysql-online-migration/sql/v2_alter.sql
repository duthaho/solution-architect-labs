-- The migration, applied to the (empty) ghost table.
-- MODIFY amount FLOAT -> DECIMAL(12,2) is the one that matters: a column type
-- change cannot be done INSTANT or INPLACE, so a plain ALTER on the live table
-- would rebuild 100M rows under a copy lock. That is why this lab exists.
ALTER TABLE _orders_gst
    MODIFY amount DECIMAL(12,2) NOT NULL,
    ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'USD',
    ADD KEY idx_customer_created (customer_id, created_at);
