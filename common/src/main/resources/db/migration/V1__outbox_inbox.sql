CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  event_id      UUID NOT NULL UNIQUE,
  topic         TEXT NOT NULL,
  msg_key       TEXT NOT NULL,
  type          TEXT NOT NULL,
  payload       JSONB NOT NULL,
  headers       JSONB NOT NULL DEFAULT '{}',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE inbox (
  consumer      TEXT NOT NULL,
  event_id      UUID NOT NULL,
  processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
