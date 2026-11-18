# Event-Driven Order Processing Platform

Order and Inventory services (Payment and Notification come in later phases) coordinating through Kafka with a transactional outbox. Java 21, Spring Boot 4.1, Kafka (KRaft), PostgreSQL.

Status: Phases 1-3 verified (outbox, broker-outage test, idempotent inventory and payment consumers; 9 tests green). Phase 4 (orchestrated saga) verified. Phase 5 (DLQ + replay) verified. Phase 6 (tracing, metrics, Grafana) verified. Notification service and end-to-end smoke script written, not yet run. Design: [docs/](docs/00-requirements.md), [roadmap](docs/ROADMAP.md).

## Run (Java 21, Maven, Docker)

```bash
docker compose up -d
mvn -B verify && mvn -q install -DskipTests
mvn -pl inventory-service spring-boot:run    # terminal 1
mvn -pl order-service spring-boot:run        # terminal 2

curl -i -X POST localhost:8091/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"customerId":"c-1","items":[{"sku":"SKU-1","quantity":2}],"amountCents":2599,"currency":"USD"}'
docker compose exec postgres psql -U postgres -d orders -c "select id, type, published_at from outbox"
```

Expect `201`; the same call again returns `200` with the same `orderId`; inventory logs the `ReserveInventory` message; both outbox rows have `published_at` set.

`mvn verify` also runs `OutboxTest` (order-service) and `InventoryIdempotencyTest` (inventory-service), `PaymentIdempotencyTest` (payment-service) with Testcontainers: Docker must be running; allow a minute or two.

## Failed messages

A consumer retries a failing message 3 times (1 s apart) on the same partition, then parks it in `<topic>.dlq`. Retries are blocking on purpose: non-blocking retry topics would reorder events for the same order. After fixing the cause, replay a DLQ (replays only what is new since the last run):

```
curl -X POST 'localhost:8092/admin/dlq/replay?topic=inventory.commands'
```

## Observability

`docker compose up -d` also starts Prometheus (http://localhost:9090) and Grafana (http://localhost:3000, dashboard "Order platform"). Services expose `/actuator/prometheus` on 8091 (order), 8092 (inventory), 8093 (payment). Every order carries one trace id: send `X-Trace-Id` or let the order service generate it; it is stored in the outbox, sent as a Kafka header, and appears in the log line of every service that handles the order (`[trace-id]` after the level).

## Run everything and smoke-test it

```
docker compose up -d
docker exec -i $(docker ps -qf name=postgres) psql -U postgres -c "CREATE DATABASE payments;" -c "CREATE DATABASE notifications;"   # only if the volume predates these
mvn -B install -DskipTests
mvn -pl order-service spring-boot:run          # one terminal each: order 8091,
mvn -pl inventory-service spring-boot:run      # inventory 8092, payment 8093,
mvn -pl payment-service spring-boot:run        # notification 8094
mvn -pl notification-service spring-boot:run
./scripts/smoke.sh
```

The smoke script places a normal order (expects CONFIRMED, stock sold, customer notified), an order whose payment is declined (expects CANCELLED, stock released, customer notified), and a repeated request with the same `Idempotency-Key` (expects the same order back).
