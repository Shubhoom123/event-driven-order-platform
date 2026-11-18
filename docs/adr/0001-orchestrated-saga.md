# ADR-0001: Orchestrated saga for the order workflow

Status: Accepted | Date: 2026-11-04

## Context

An order touches three services that each own their data (stock, payments, orders). A payment failure must undo the stock reservation, and no distributed transaction is allowed (see `00-requirements.md`). We need a way to run a multi-step business transaction with compensation.

## Options considered

Scores 1-5 (5 = best). Weighted totals computed in code. Scores are engineering judgment from the pattern's known properties, not benchmarks; none are "measured".

| Criterion (weight) | Orchestrated saga | Choreographed saga | 2PC / XA | Temporal engine |
|---|---|---|---|---|
| Consistency & failure handling (25) | 4 | 3 | 5 | 5 |
| Debuggability / visibility (20) | 5 | 2 | 3 | 5 |
| Operational complexity (15) | 4 | 4 | 2 | 2 |
| Coupling / service autonomy (10) | 3 | 5 | 1 | 3 |
| Learning fit: saga + compensation (20) | 5 | 4 | 1 | 2 |
| Lock-in / extra infra (10) | 5 | 5 | 3 | 2 |
| **Weighted total** | **4.40** | **3.55** | **2.75** | **3.45** |

Notes:
- Choreography scores low on visibility because the workflow exists only as an emergent chain of reactions; "where is order X stuck?" has no single answer.
- 2PC/XA scores high on correctness but is excluded by requirement (blocking coordinator, poor fit with Kafka, availability cost).
- Temporal is a legitimate production answer. It lost mainly on learning fit and extra infrastructure: it hides exactly the mechanics (timeouts, compensation, retries) this project exists to teach.

## Sensitivity

Shifting the two heaviest weights (consistency, then debuggability/learning) by +/-10 never changed the winner. Robust.

## Decision

Orchestrated saga, with the **Order service as orchestrator** and a persisted state machine (`saga_instances`). Reasons that decided it:
1. One place answers "what state is order X in and why".
2. Timeouts and compensation are explicit, testable code paths.
3. Participants (Inventory, Payment) stay simple command handlers that know nothing about the workflow.

## Consequences

- The Order service is a coordination hotspot and a coupling point (it knows the participants' command contracts). Accepted.
- The orchestrator's own state must be durable and crash-safe, which is what the outbox/inbox provide.
- We must handle **compensation failure** (F7) and **unknown outcomes** (F13) explicitly. Payment is the pivot transaction: before it, steps are compensable; after it, only forward.
- Order status and saga state are separate: `orders.status` is the customer-facing summary, `saga_instances.state` is the internal workflow position.

## Revisit when

- Workflow grows beyond ~5 steps or needs human approval steps/long timers -> evaluate Temporal.
- Orchestrator throughput, not Postgres, becomes the load-test bottleneck.
