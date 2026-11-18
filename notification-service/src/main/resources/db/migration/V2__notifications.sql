CREATE TABLE notifications (
  id          BIGSERIAL PRIMARY KEY,
  order_id    UUID NOT NULL,
  type        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (order_id, type)
);
