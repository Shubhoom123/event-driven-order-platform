# 01 - Capacity and Failure Modes

Arithmetic was executed in code (not estimated by eye). Assumptions are explicit so they can be challenged.

## 1. Capacity sketch

### Load-test target (what we will actually stress)

| Quantity | Value | Note |
|---|---|---|
| Sustained orders/s | 200 | Test target on laptop hardware |
| Peak factor | 3x -> 600 orders/s | Burst test |
| Kafka messages per order | 6 (happy), **8 (failure path)** | 8 is used as the budget (worst case) |
| Kafka messages/s sustained | 1,600 | 200 x 8 |
| Kafka messages/s at peak | 4,800 | 600 x 8 |
| Avg message size | ~600 B (assumption) | JSON envelope + payload |
| Kafka throughput sustained | ~0.96 MB/s | Far below one partition's limit |
| Latency budget | p99 < 500 ms over 8 hops = ~62 ms/hop | Each hop = DB txn + relay poll + broker + consumer |

Verdict: **Kafka throughput is not the bottleneck.** A single partition could carry this bandwidth. Partitions (6 per topic) are chosen for consumer parallelism and to make rebalancing observable, not for bandwidth. The first bottleneck is expected to be **Postgres commit latency and the outbox relay poll interval**, because every hop does a synchronous transaction and a poll.

### Illustrative production scale (to show the monolith comparison honestly)

| Quantity | Value |
|---|---|
| Orders/day | 1,000,000 |
| Average | ~11.6 orders/s |
| Peak (x5) | ~58 orders/s |
| Bytes written per order across all tables | ~7.2 KB (order, saga, 8 outbox rows, 6 inbox rows) |
| Unpruned growth | ~2.6 TB/year |
| Steady state with 7-day outbox/inbox pruning | ~45 GB |

Two conclusions: (1) a single well-tuned Postgres handles the illustrative prod load, so microservices + Kafka are chosen for learning, not necessity; (2) **outbox and inbox tables must be pruned**, or they dominate storage. Pruning is a Phase 2/3 task with a test.

### Relay poll interval trade-off

| Poll interval | Worst-case added latency per hop | Idle queries/s per relay |
|---|---|---|
| 50 ms | 50 ms | 20 |
| 100 ms | 100 ms | 10 |
| 250 ms | 250 ms | 4 |

With ~62 ms/hop, a 250 ms poll alone blows the budget. Default 50 ms with immediate re-poll while the batch is full; this is exactly the kind of thing the load test must tune (ADR-0002 revisit trigger).

## 2. Consistency per entity

| Entity | Needs | Mechanism |
|---|---|---|
| Stock counts | Strong, single writer | Inventory DB, `CHECK (available >= 0)`, conditional UPDATE |
| Payment per order | At most one | `UNIQUE(order_id)` on payments |
| Order / saga state | Strong within Order service | Single DB txn per transition, optimistic `version` |
| Cross-service state | **Eventual**, bounded by timeout | Saga + outbox + inbox |
| Notifications | At-least-once acceptable, deduped by event ID | Inbox |

CAP/PACELC in plain terms: each service is CP for its own data (one Postgres primary). Across services the system is AP-ish: during a partition the Order service still accepts orders (outbox), and the saga completes later. Users see `PENDING` until then.

Ordering: per-order ordering via Kafka key = `orderId` (all events for one order land on one partition). No global ordering is needed or provided.

## 3. Correctness invariants (what the tests assert)

| ID | Invariant |
|---|---|
| I1 | Every order accepted by `POST /orders` (HTTP 201) reaches a terminal state (`CONFIRMED` or `CANCELLED`) within a bounded time after faults clear |
| I2 | At most one `payments` row per order |
| I3 | Stock conservation: `initial = available + reserved + sold`; no reservation remains for a `CANCELLED` order |
| I4 | No outbox row stays unpublished after the broker has been healthy for T seconds |
| I5 | Each inbox `(consumer, event_id)` appears once; a replayed event causes no additional state change |

I1-I5 are implemented as one **reconciliation query set** reused by every failure-injection test and runnable against a live stack.

## 4. Failure-mode table (per hop)

"Crash window" means: what if the process dies between these two lines?

| # | Hop / scenario | What can go wrong | Mitigation | Test (phase) |
|---|---|---|---|---|
| F1 | Order accepted, broker down | Event lost (dual write) | Outbox row in same txn; relay retries | Broker-down test (2, 7) |
| F2 | Relay publishes, dies before marking row sent | Duplicate publish | Accepted; consumer inbox dedups | Relay-kill test (2) |
| F3 | Consumer commits DB txn, dies before offset commit | Redelivery -> double effect | Inbox row in same txn as effect | Consumer-kill test (3, 7) |
| F4 | Duplicate delivery (rebalance, retry) | Double charge / double deduct | Inbox + `UNIQUE(order_id)` | Duplicate-delivery test (3, 7) |
| F5 | Payment gateway fails | Stock stuck reserved | Compensation: release reservation | Saga failure test (4) |
| F6 | Reply never arrives (lost, slow) | Saga stuck forever | `deadline_at` + recovery job re-drives | Timeout test (4) |
| F7 | Compensation itself fails | Stock stuck reserved | Retry with backoff; after N -> `COMPENSATION_FAILED` + alert | Compensation-failure test (4) |
| F8 | Poison message | Partition blocked | Non-blocking retry topics -> DLT | DLQ test (5) |
| F9 | Consumer rebalance mid-processing | Duplicate / reordered work | Inbox; per-order saga state checks | Rebalance test (7, 8) |
| F10 | Out-of-order events for one order | Invalid transition | Saga state machine rejects/ignores stale events | Unit tests (4) |
| F11 | Postgres down | Intake fails | Readiness probe fails; HTTP 503; no partial state | Manual (6) |
| F12 | Unbounded outbox growth | Relay slows, disk fills | Pruning job + `outbox_pending` alert | Metric test (6) |
| F13 | Payment outcome unknown (timeout after charge) | Cancel an already-charged order | **Never cancel on timeout while charging**; re-drive idempotently until a definite result, else `MANUAL_REVIEW` | Timeout test (4) |

Design note on F13: payment is the **pivot transaction** of the saga. Before it, steps are compensable (release stock). After it succeeds, the saga only moves forward. If the charge outcome is unknown, retrying the idempotent charge is safe; cancelling is not.

## 5. Timeouts, retries, backoff

- Every external call has an explicit timeout (DB, Kafka producer `delivery.timeout.ms`, simulated gateway).
- Consumer retries are **non-blocking** (retry topics with exponential backoff and jitter), so one bad message does not stall a partition.
- Saga re-drive and compensation use bounded attempts with exponential backoff; limits are config, not constants.
- Retries are safe only because every operation is idempotent (inbox + unique constraints).

## 6. SLIs and SLOs

| SLI | SLO (target) | Measured by |
|---|---|---|
| Intake availability: `POST /orders` 2xx rate | 99.9% while Postgres is up, including during broker outage | k6, http metrics |
| End-to-end latency: accepted -> terminal | p99 < 500 ms at 200 orders/s | k6 + saga duration timer |
| Correctness: invariants I1-I5 | 100% (zero violations) | Reconciliation queries |
| Pipeline health: oldest unpublished outbox row | < 5 s under normal operation | `outbox_oldest_age_seconds` |

Alerts (symptom-based, Phase 6): outbox oldest age > 30 s; DLT message count increased; any saga in `COMPENSATION_FAILED`.

## 7. Security basics (deferred, but listed)

- No secrets in repo; DB credentials via environment with local-only defaults in Compose.
- Input validation on `POST /orders` (quantity bounds, SKU format, amount > 0).
- No authN/authZ and no rate limiting in v1 (non-goal). Listed as a known limitation in the README.
- Dependency scanning: Dependabot config file only (no push performed by the assistant).

## 8. Checklist coverage

| Checklist item | Status |
|---|---|
| Capacity estimates | Done (above) |
| Consistency per entity | Done |
| Dual-write avoided | Outbox (ADR-0002) |
| Idempotency keys | Inbox + unique constraints (ADR-0003) |
| Poison messages / DLQ / replay | Phase 5 |
| Clock assumptions | No dependence on synchronized wall clocks for correctness; `deadline_at` uses the DB clock of the owning service only |
| Leader election / singleton roles | Relay and recovery job run on N instances safely via `FOR UPDATE SKIP LOCKED` |
| Circuit breakers / bulkheads | Deferred; backpressure observed in Phase 8 load test |
| Backup/restore | Deferred (local-only); noted as limitation |
| Observability | Phase 6 |
