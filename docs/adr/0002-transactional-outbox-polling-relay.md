# ADR-0002: Transactional outbox with a polling relay

Status: Accepted | Date: 2026-11-04

## Context

A service must change its database state *and* publish an event. Writing to the DB and then to Kafka (a dual write) loses events when the process dies or the broker is down between the two, and publishing first can announce state that never committed. Requirement: no accepted order is lost while Kafka is unavailable (`00-requirements.md`, F1/F2).

## Options considered

| Criterion (weight) | Polling outbox relay | Debezium CDC | Dual write | Kafka txns only |
|---|---|---|---|---|
| Delivery guarantee: no lost events (30) | 5 | 5 | 1 | 2 |
| Operational complexity (20) | 4 | 2 | 5 | 3 |
| Latency (10) | 3 | 5 | 5 | 4 |
| Debuggability / testability (15) | 5 | 3 | 3 | 2 |
| Learning fit (15) | 5 | 4 | 1 | 2 |
| Extra infra / lock-in (10) | 5 | 2 | 5 | 4 |
| **Weighted total** | **4.60** | **3.65** | **2.90** | **2.60** |

Notes:
- Kafka transactions only cover Kafka-to-Kafka read-process-write. They cannot make a Postgres commit and a Kafka send atomic, so they do not remove the dual write.
- Debezium reads the WAL, so it adds latency headroom and removes polling load, but adds Kafka Connect, replication slots and connector operations. That is a real production option; it scored lower here for operational weight and because it hides the relay logic we want to understand and test.
- Latency score of 3 for polling reflects the capacity math: the poll interval is added to every hop.

## Sensitivity

Winner unchanged under +/-10 shifts of the top two weights.

## Decision

Each service writes its state change and an `outbox` row **in the same local Postgres transaction**. A relay inside the service claims unpublished rows with

```sql
SELECT ... FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT :batch
FOR UPDATE SKIP LOCKED
```

publishes them with an idempotent producer (`acks=all`, `enable.idempotence=true`), waits for broker acknowledgement, then sets `published_at`. Delivery is **at-least-once**; duplicates (relay dies after send, before the update) are removed by the consumer inbox (ADR-0003).

Defaults: poll 50 ms, immediate re-poll while a batch is full, batch size 100, pruning of published rows older than 7 days.

## Consequences

- Added latency of up to one poll interval per hop; tunable, must be measured in Phase 8.
- Extra read load on the DB even when idle (20 queries/s at 50 ms per service). Accepted locally; a LISTEN/NOTIFY wake-up is a possible optimization.
- `SKIP LOCKED` lets several relay instances run safely, but batches can interleave, so **cross-batch ordering for one key is not guaranteed with multiple relays**. Consumers must not rely on it: the saga state machine tolerates stale or duplicate events (F10). Alternative if strict ordering is needed: one relay per service (leader via advisory lock).
- Table growth requires the pruning job (F12), covered by a test.
- Outbox rows carry `topic`, `key`, `headers` (including `traceparent`) so tracing survives the async hop.

## Revisit when

- Per-hop latency from polling exceeds the budget in the load test even at 50 ms.
- Relay DB load is a measurable share of Postgres CPU.
- Either triggers a Debezium + outbox event router spike.
