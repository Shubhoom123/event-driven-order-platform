# Event-Driven Order Processing Platform

Order and Inventory services (Payment and Notification come in later phases) coordinating through Kafka with a transactional outbox. Java 21, Spring Boot 4.1, Kafka (KRaft), PostgreSQL.

Status: Phases 1-3 verified (outbox, broker-outage test, idempotent inventory and payment consumers; 9 tests green). Phase 4 (orchestrated saga) verified. Phase 5 (retry + DLQ + replay) written, not yet run. Design: [docs/](docs/00-requirements.md), [roadmap](docs/ROADMAP.md).

## Run (Java 21, Maven, Docker)

```bash
docker compose up -d
mvn -B verify && mvn -q install -DskipTests
mvn -pl inventory-service spring-boot:run    # terminal 1
mvn -pl order-service spring-boot:run        # terminal 2

curl -i -X POST localhost:8081/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
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
