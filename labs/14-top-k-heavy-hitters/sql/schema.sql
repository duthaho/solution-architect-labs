-- Lab 14: top-K most-viewed products (heavy hitters).
-- views_minute is the "classic interview answer" contender: an exact
-- per-minute rollup, one row per (minute, product), maintained with
-- INSERT .. ON DUPLICATE KEY UPDATE. Top-K = GROUP BY + ORDER BY SUM(cnt).

DROP DATABASE IF EXISTS lab14;
CREATE DATABASE lab14 DEFAULT CHARSET utf8mb4;
USE lab14;

CREATE TABLE views_minute (
  minute_no  INT UNSIGNED NOT NULL,
  product_id BIGINT UNSIGNED NOT NULL,
  cnt        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (minute_no, product_id),
  KEY idx_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
