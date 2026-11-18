CREATE TABLE payments (
  order_id      UUID PRIMARY KEY,
  status        TEXT NOT NULL,
  amount_cents  BIGINT NOT NULL,
  currency      TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
