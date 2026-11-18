CREATE TABLE stock (
  sku        TEXT PRIMARY KEY,
  available  INT NOT NULL CHECK (available >= 0),
  reserved   INT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  sold       INT NOT NULL DEFAULT 0 CHECK (sold >= 0)
);

CREATE TABLE reservations (
  order_id    UUID PRIMARY KEY,
  status      TEXT NOT NULL,
  items       JSONB NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
