# Roadmap

Each phase ends with a runnable check. Because the assistant cannot compile or run this project in its own workspace, **you** run each check locally and report results; the next phase is written against what actually happened.

| Phase | Deliverable | Done when (you run it) | Est. |
|---|---|---|---|
| 0 | Requirements, capacity, ADRs, architecture (this folder) | You can explain each decision and its runner-up | 2-3 d |
| 1 | Walking skeleton: Maven multi-module, `common`, 4 services, Compose (Kafka + Postgres), CI workflow file | `docker compose up`, then one curl yields an event consumed end to end; `mvn verify` green | 3-4 d |
| 2 | Transactional outbox + relay | Testcontainers test stops Kafka mid-run; all events still arrive after restart | 3-4 d |
| 3 | Inbox + idempotent consumers | Same event delivered 5x causes one stock change and one charge | 2-3 d |
| 4 | Orchestrated saga + compensation + timeout recovery | Forced payment failure restores stock; stuck saga recovers; compensation failure reaches `COMPENSATION_FAILED` | 4-5 d |
| 5 | Retry topics, DLT, replay tool | Poison message lands in DLT without blocking its partition; replay works | 2-3 d |
| 6 | Observability | One order is one trace across 4 services; Grafana shows lag, outbox age, DLT | 2-3 d |
| 7 | Failure-injection suite + reconciliation queries | Suite passes repeatedly; record run count (resume `[N]`) | 3-4 d |
| 8 | k6 load test, tuning, README, resume notes | Real `[X]` orders/s and `[Y]` ms p99 recorded with method | 2-3 d |

## Working agreement

- Code is delivered as files for you to copy; no git commits or pushes are made by the assistant. A GitHub Actions workflow file is provided as a file only.
- Nothing here is claimed to work until you have run it. Expect first-build friction (see ADR-0005) and send errors back verbatim.
- Resume numbers come only from your real runs, with the method recorded in the README.

## First-run checklist for every phase

1. `java -version` shows 21.
2. `docker compose up -d` healthy (`docker compose ps`).
3. `mvn -q verify` (Docker running for Testcontainers).
4. Phase-specific check from the table above.
5. Paste any failure output back for diagnosis.
