# ADR-0003: Inbox table for idempotent consumers (exactly-once effect)

Status: Accepted | Date: 2026-11-04

## Context

Kafka plus the outbox relay deliver **at least once**. Duplicates arise from relay crashes (F2), consumer crashes between DB commit and offset commit (F3), and rebalances (F9). A duplicate must not cause a second charge or a second stock deduction (F4).

## Options considered

| Criterion (weight) | Inbox table, same txn | Kafka EOS (txn read-process-write) | Natural idempotency only | In-memory dedup |
|---|---|---|---|---|
| Correctness of the effect (35) | 5 | 3 | 3 | 1 |
| Works with DB side effects (20) | 5 | 2 | 4 | 1 |
| Complexity (15) | 4 | 3 | 5 | 5 |
| Throughput cost (10) | 3 | 4 | 5 | 5 |
| Interview / learning value (20) | 5 | 3 | 2 | 1 |
| **Weighted total** | **4.65** | **2.90** | **3.50** | **2.00** |

Notes:
- Kafka EOS guarantees atomicity only between Kafka offsets and Kafka output. The side effect here is a Postgres write, outside that boundary.
- "Natural idempotency" (upserts, unique constraints) is used *as well*, as a second line of defence, but alone it cannot protect effects such as "decrement stock", which are not naturally idempotent.
- An in-memory cache loses state on restart, exactly when duplicates appear.

## Sensitivity

Winner unchanged under +/-10 shifts of the top two weights.

## Decision

Every consumer handler runs in one DB transaction:

```
BEGIN
  INSERT INTO inbox(consumer, event_id) VALUES (...) ON CONFLICT DO NOTHING
  -- 0 rows inserted  => duplicate: COMMIT, skip, ack
  apply domain change
  INSERT INTO outbox(...)         -- any outgoing events
COMMIT
ack offset (manual, after commit)
```

- Dedup key is the producer-assigned `eventId` (UUID in the envelope), not Kafka offset or content hash. A relay re-publish keeps the same `eventId`.
- Auto-commit is disabled; the offset is acknowledged only after the DB commit.
- Second line of defence: `UNIQUE(order_id)` on payments and reservations, so a logic bug cannot double-charge.
- "Exactly-once **effect**" = at-least-once delivery + idempotent processing. We do not claim exactly-once delivery.

## Consequences

- One extra insert per message; inbox grows and needs pruning (retention must exceed the maximum replay window; default 7 days, replay tool must refuse older events or document the limit).
- Handlers must be deterministic with respect to the event: all effects inside the same transaction. Calling external systems (the simulated gateway) is the hard case; see the architecture doc (charge is keyed by `order_id` so the gateway call itself is idempotent).
- Duplicate counts become a metric (`inbox_duplicates_total`), useful in tests and dashboards.

## Revisit when

- Inbox insert is the throughput bottleneck (consider batch dedup or partition-scoped dedup).
- A consumer's side effect is not transactional with Postgres (needs a different strategy, e.g. idempotency key sent to the downstream system).
