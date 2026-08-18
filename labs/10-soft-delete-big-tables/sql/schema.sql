-- Lab 10: three isolated schema families, one per delete strategy.
-- Identical seed data goes into each, so measurements compare like with like.
--
--   lab10_a          strategy A: deleted_at column, rows never leave
--   lab10_b          strategy B: hard DELETE + move to mirror schema
--   lab10_b_deleted  strategy B's mirror ("deleted" schema from the case study)
--   lab10_c          strategy C: deleted_at for instant UX + background archiver
--   lab10_c_archive  strategy C's archive target
--
-- Deliberate design points (discussed in README §3):
--   * live tables carry real FKs; mirror/archive tables keep the same PK but
--     drop FKs and drop UNIQUE(email) — deleted rows of the same email must be
--     allowed to pile up.
--   * mirror tables append trailing metadata columns (_deleted_at, _deleted_by).
--     Strategy B's move uses positional INSERT ... SELECT t.*, which is exactly
--     what breaks when the live table is ALTERed and the mirror is forgotten.

-- ============================================================ family A
DROP DATABASE IF EXISTS lab10_a;
CREATE DATABASE lab10_a CHARACTER SET utf8mb4;

CREATE TABLE lab10_a.users (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email       VARCHAR(255) NOT NULL,
  name        VARCHAR(64)  NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_email (email),
  KEY idx_deleted_at (deleted_at)
) ENGINE=InnoDB;

CREATE TABLE lab10_a.orders (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  KEY idx_user (user_id),
  KEY idx_deleted_at (deleted_at),
  CONSTRAINT fk_a_orders_user FOREIGN KEY (user_id) REFERENCES lab10_a.users (id)
) ENGINE=InnoDB;

CREATE TABLE lab10_a.order_items (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id    BIGINT UNSIGNED NOT NULL,
  sku         VARCHAR(32)  NOT NULL,
  qty         INT          NOT NULL,
  price       DECIMAL(10,2) NOT NULL,
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  KEY idx_order (order_id),
  KEY idx_deleted_at (deleted_at),
  CONSTRAINT fk_a_items_order FOREIGN KEY (order_id) REFERENCES lab10_a.orders (id)
) ENGINE=InnoDB;

-- ============================================================ family B
-- No deleted_at on live tables: here "delete" means the row leaves the schema.
DROP DATABASE IF EXISTS lab10_b;
CREATE DATABASE lab10_b CHARACTER SET utf8mb4;

CREATE TABLE lab10_b.users (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email       VARCHAR(255) NOT NULL,
  name        VARCHAR(64)  NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uq_email (email)
) ENGINE=InnoDB;

CREATE TABLE lab10_b.orders (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_user (user_id),
  CONSTRAINT fk_b_orders_user FOREIGN KEY (user_id) REFERENCES lab10_b.users (id)
) ENGINE=InnoDB;

CREATE TABLE lab10_b.order_items (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id    BIGINT UNSIGNED NOT NULL,
  sku         VARCHAR(32)  NOT NULL,
  qty         INT          NOT NULL,
  price       DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_order (order_id),
  CONSTRAINT fk_b_items_order FOREIGN KEY (order_id) REFERENCES lab10_b.orders (id)
) ENGINE=InnoDB;

DROP DATABASE IF EXISTS lab10_b_deleted;
CREATE DATABASE lab10_b_deleted CHARACTER SET utf8mb4;

CREATE TABLE lab10_b_deleted.users (
  id          BIGINT UNSIGNED NOT NULL,
  email       VARCHAR(255) NOT NULL,
  name        VARCHAR(64)  NOT NULL,
  created_at  DATETIME(6)  NOT NULL,
  _deleted_at DATETIME(6)  NOT NULL,
  _deleted_by VARCHAR(32)  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE lab10_b_deleted.orders (
  id          BIGINT UNSIGNED NOT NULL,
  user_id     BIGINT UNSIGNED NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  created_at  DATETIME(6)  NOT NULL,
  _deleted_at DATETIME(6)  NOT NULL,
  _deleted_by VARCHAR(32)  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

CREATE TABLE lab10_b_deleted.order_items (
  id          BIGINT UNSIGNED NOT NULL,
  order_id    BIGINT UNSIGNED NOT NULL,
  sku         VARCHAR(32)  NOT NULL,
  qty         INT          NOT NULL,
  price       DECIMAL(10,2) NOT NULL,
  _deleted_at DATETIME(6)  NOT NULL,
  _deleted_by VARCHAR(32)  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

-- ============================================================ family C
DROP DATABASE IF EXISTS lab10_c;
CREATE DATABASE lab10_c CHARACTER SET utf8mb4;

CREATE TABLE lab10_c.users (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email       VARCHAR(255) NOT NULL,
  name        VARCHAR(64)  NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_email (email),
  KEY idx_deleted_at (deleted_at)
) ENGINE=InnoDB;

CREATE TABLE lab10_c.orders (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  status      VARCHAR(16)  NOT NULL,
  amount      DECIMAL(10,2) NOT NULL,
  created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  KEY idx_user (user_id),
  KEY idx_deleted_at (deleted_at),
  CONSTRAINT fk_c_orders_user FOREIGN KEY (user_id) REFERENCES lab10_c.users (id)
) ENGINE=InnoDB;

CREATE TABLE lab10_c.order_items (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id    BIGINT UNSIGNED NOT NULL,
  sku         VARCHAR(32)  NOT NULL,
  qty         INT          NOT NULL,
  price       DECIMAL(10,2) NOT NULL,
  deleted_at  DATETIME(6)  NULL,
  PRIMARY KEY (id),
  KEY idx_order (order_id),
  KEY idx_deleted_at (deleted_at),
  CONSTRAINT fk_c_items_order FOREIGN KEY (order_id) REFERENCES lab10_c.orders (id)
) ENGINE=InnoDB;

DROP DATABASE IF EXISTS lab10_c_archive;
CREATE DATABASE lab10_c_archive CHARACTER SET utf8mb4;

CREATE TABLE lab10_c_archive.users (
  id           BIGINT UNSIGNED NOT NULL,
  email        VARCHAR(255) NOT NULL,
  name         VARCHAR(64)  NOT NULL,
  created_at   DATETIME(6)  NOT NULL,
  deleted_at   DATETIME(6)  NOT NULL,
  _archived_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_archived_at (_archived_at)
) ENGINE=InnoDB;

CREATE TABLE lab10_c_archive.orders (
  id           BIGINT UNSIGNED NOT NULL,
  user_id      BIGINT UNSIGNED NOT NULL,
  status       VARCHAR(16)  NOT NULL,
  amount       DECIMAL(10,2) NOT NULL,
  created_at   DATETIME(6)  NOT NULL,
  deleted_at   DATETIME(6)  NOT NULL,
  _archived_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_archived_at (_archived_at)
) ENGINE=InnoDB;

CREATE TABLE lab10_c_archive.order_items (
  id           BIGINT UNSIGNED NOT NULL,
  order_id     BIGINT UNSIGNED NOT NULL,
  sku          VARCHAR(32)  NOT NULL,
  qty          INT          NOT NULL,
  price        DECIMAL(10,2) NOT NULL,
  deleted_at   DATETIME(6)  NOT NULL,
  _archived_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_archived_at (_archived_at)
) ENGINE=InnoDB;
