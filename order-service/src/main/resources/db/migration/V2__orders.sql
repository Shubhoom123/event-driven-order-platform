CREATE TABLE orders (
  id               UUID PRIMARY KEY,
  customer_id      TEXT NOT NULL,
  amount_cents     BIGINT NOT NULL CHECK (amount_cents > 0),
  currency         CHAR(3) NOT NULL,
  status           TEXT NOT NULL,
  idempotency_key  TEXT UNIQUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
  order_id  UUID NOT NULL REFERENCES orders (id),
  sku       TEXT NOT NULL,
  quantity  INT NOT NULL CHECK (quantity > 0),
  PRIMARY KEY (order_id, sku)
);
