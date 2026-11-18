# ADR-0004: Apache Kafka (KRaft) as the broker

Status: Accepted | Date: 2026-11-04

## Context

Services must communicate asynchronously with per-order ordering, replay (for the DLQ replay tool and for recovery), and consumer lag visibility. This project is learning-first and targets Kafka experience explicitly (`00-requirements.md`).

## Options considered

| Criterion (weight) | Kafka (KRaft) | RabbitMQ | Redpanda | Pulsar |
|---|---|---|---|---|
| Partitioned log, ordering, replay (25) | 5 | 3 | 5 | 5 |
| Ecosystem: Spring, Testcontainers (20) | 5 | 4 | 4 | 2 |
| Operational complexity locally (15) | 3 | 4 | 4 | 2 |
| Observability / lag metrics (10) | 5 | 4 | 4 | 4 |
| Resume / learning fit (20) | 5 | 2 | 3 | 2 |
| Maturity / adoption (10) | 5 | 5 | 3 | 3 |
| **Weighted total** | **4.70** | **3.45** | **3.95** | **3.05** |

Notes:
- The weight on learning/resume fit is deliberate and stated; with it set to 0 and redistributed, Redpanda and RabbitMQ get closer, but Kafka's log semantics (offsets, consumer groups, replay) are what the DLQ/replay design relies on.
- Redpanda speaks the Kafka API with a single binary and is a fair "simpler local broker"; it is the runner-up. The Kafka-compatibility claim means client code would not change, which makes this a cheap future swap.
- RabbitMQ would force different designs for replay and per-key ordering.

## Sensitivity

Winner unchanged under +/-10 shifts of the top two weights.

## Decision

Apache Kafka in **KRaft mode** (no ZooKeeper). Kafka 4.x runs KRaft only, so there is no ZooKeeper option; sources: Kafka 4.2.0 release announcement (2026-02) and the ZooKeeper removal notes. Local Compose uses a single node with combined broker+controller roles and replication factor 1; production topology (3 brokers, RF=3, `min.insync.replicas=2`) is documented, not run.

Topic conventions: see `02-architecture.md`. Key = `orderId`, 6 partitions, JSON envelope with `schemaVersion`.

Image/version: `apache/kafka` 4.2.x (**unverified tag, confirm on Docker Hub**). Kafka 4.2.0 notes mention share groups ("queues") as production-ready; they are not used because the log + consumer-group model is what we want to learn, and ordering per key matters.

## Consequences

- Heavier local footprint than Rabbit/Redpanda; acceptable on 8 GB.
- JSON without a schema registry means contract drift is possible; mitigated by a shared `common` event module and contract tests (Phase 1/5).
- Rebalancing behaviour is a deliberate learning topic: we will observe it with 6 partitions and 1-3 instances per consumer group, and record lag/throughput effects.

## Revisit when

- Local resource use blocks the test suite (try Redpanda).
- Multiple teams/consumers need enforced schemas (add Schema Registry + Avro/Protobuf).
