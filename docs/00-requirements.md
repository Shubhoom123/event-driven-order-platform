# 00 - Requirements

Status: Phase 0 draft, 2026-11-04. Nothing in this repo has been compiled or run yet (see "Verification status" in README once it exists).

## Purpose

An e-commerce order backend split into four services that coordinate only through Kafka. The one thing it must do well:

> **Never lose an accepted order, and never apply an order's side effects twice (no double charge, no double stock deduction), even when the broker, a consumer, or a service dies mid-flight.**

Everything else (throughput, UI, features) is secondary to that invariant and to being able to *prove* it with failure-injection tests.

## Portfolio goals (why this project exists)

Mapped to the gaps in the candidate profile:

| Gap | How this project closes it | Evidence it must leave behind |
|---|---|---|
| Event-driven / messaging | Kafka, outbox, idempotent consumers, saga, DLQ | Failure-injection test results, ADRs |
| Production-style rigor | CI, Testcontainers, SLOs, dashboards, runbook | Green CI, Grafana dashboard, `docs/runbook.md` |
| Multi-service under load | k6 load test with backpressure behaviour | Published p99 and throughput numbers |
| Java / Spring Boot on resume | Four Spring Boot services | The code itself |

Mode: **learning-first**. Kafka and microservices are used because the user wants to learn the patterns, not because the workload demands them. A modular monolith with an in-process event bus would be the honest "ship a product" answer at the scale in `01-capacity-and-failure-modes.md`. This is stated in the README so interviewers hear it from the candidate first.

## Services and ownership

| Service | Owns | Does |
|---|---|---|
| Order | orders, saga state | REST intake, saga orchestrator, timeout recovery |
| Inventory | stock, reservations | Reserve / release stock |
| Payment | payments | Charge via a simulated gateway (injectable failures) |
| Notification | notification log | Consumes order events, records "sent" notifications |

One writer per piece of data. No shared tables, no cross-service DB reads, no synchronous calls between services.

## Core user journeys

1. **Happy path.** `POST /orders` -> reserve stock -> charge -> order `CONFIRMED` -> notification recorded.
2. **Payment failure.** Charge fails -> stock released -> order `CANCELLED` -> notification recorded.
3. **Out of stock.** Reserve rejected -> order `CANCELLED` (nothing to compensate).
4. **Client retry.** Same `Idempotency-Key` twice -> one order.
5. **Stuck saga.** A reply never arrives -> timeout recovery re-drives or compensates.
6. **Poison message.** Un-processable event -> retry topics -> dead-letter topic -> operator replay.

## v1 scope

- REST: `POST /orders`, `GET /orders/{id}`, admin `PUT /stock/{sku}` (seed stock)
- Kafka topics, outbox relay, inbox dedup, saga, retry/DLT, replay tool
- Observability: trace IDs through Kafka headers, Prometheus metrics, Grafana dashboard
- Tests: unit, integration (Testcontainers), failure-injection, k6 load
- CI on GitHub Actions (workflow file only; **no pushes are made by the assistant**)

## Non-goals (v1)

- Real payment gateway, real email/SMS
- AuthN/AuthZ, multi-tenancy, rate limiting (documented as deferred; local-only)
- Multi-broker / multi-region Kafka (single broker locally; replication factor 3 is described, not run)
- Frontend UI
- Schema registry / Avro (JSON with explicit `schemaVersion`; revisit trigger in ADR-0004)
- Kubernetes / IaC (a possible follow-up project, tracked in the candidate profile)

## Non-functional targets

| Target | Value | Basis |
|---|---|---|
| Correctness | 0 lost, 0 double-processed orders across all injection runs | Invariants I1-I4 in the capacity doc |
| Sustained throughput | 200 orders/s on a laptop-class machine | Chosen to stress, not to mimic prod (see capacity doc) |
| End-to-end latency | p99 order accepted -> terminal state < 500 ms at 200 orders/s | 62 ms/hop over 8 hops |
| Recovery | Orders complete after broker returns, no manual step | Outbox guarantees |
| Availability of intake | `POST /orders` succeeds while Kafka is down | Outbox decouples intake from broker |

These are **targets, not claims**. Resume numbers come only from measured runs.

## Constraints and assumptions

- Local-first: Docker Compose on a developer laptop (assumed 2-4 cores, 8 GB RAM).
- Java 21 (installed in the authoring workspace; user confirms their local JDK).
- The assistant delivers code only. The user builds, runs and verifies locally; no GitHub actions are performed by the assistant.
- The authoring workspace cannot reach Maven Central and has no Docker daemon, so **all code is unverified until the user runs it**. Each phase therefore ends with explicit commands and expected results.
- Timeline: 3-4 weeks part-time.

## Open questions (defaults assumed)

| Question | Default |
|---|---|
| Items per order | Multiple line items, small (1-5) |
| Payment failure rate in load test | 5% injected, configurable |
| Money | Integer cents + currency code |
