# Event-Driven Order Processing Platform

Order, Inventory, Payment and Notification services coordinating through Kafka, with a transactional outbox, idempotent consumers, and an orchestrated saga with compensation. Java 21, Spring Boot 4.1, Kafka (KRaft), PostgreSQL.

**Status: Phase 1 (walking skeleton) written. Not yet compiled or run by anyone.**

One `POST /orders` writes the order and two outbox rows in one transaction, the relay publishes them to Kafka, and the Inventory service consumes `ReserveInventory`. Payment and Notification are empty shells for now.

## Read in this order

1. [Requirements](docs/00-requirements.md)
2. [Capacity and failure modes](docs/01-capacity-and-failure-modes.md)
3. [Architecture](docs/02-architecture.md)
4. ADRs: [saga](docs/adr/0001-orchestrated-saga.md), [outbox](docs/adr/0002-transactional-outbox-polling-relay.md), [inbox](docs/adr/0003-inbox-table-idempotent-consumers.md), [broker](docs/adr/0004-kafka-kraft-broker.md), [runtime](docs/adr/0005-java21-spring-boot-4.md)
5. [Roadmap](docs/ROADMAP.md)

## Run it (needs Java 21, Maven 3.9+, Docker)

```bash
docker compose up -d
docker compose ps            # postgres and kafka healthy; kafka-init exited (0)

mvn -B verify                # builds all modules, runs the 3 unit tests in :common
mvn -q install -DskipTests

mvn -pl inventory-service spring-boot:run    # terminal 1
mvn -pl order-service spring-boot:run        # terminal 2

curl -i -X POST localhost:8081/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"customerId":"c-1","items":[{"sku":"SKU-1","quantity":2}],"amountCents":2599,"currency":"USD"}'

docker compose exec postgres psql -U postgres -d orders \
  -c "select id, type, topic, published_at from outbox order by id"
```

Expected: `201` with `PENDING`; repeating the call returns `200` with the same `orderId`; the inventory terminal logs a `received topic=inventory.commands` line; both outbox rows have `published_at` set.

## Verification status

| Item | State |
|---|---|
| Design docs, ADR matrices | Written; matrix arithmetic executed in code |
| Mermaid diagrams | Rendered successfully |
| pom.xml and application.yml files | Parsed as well-formed; not resolved or built |
| Java code, SQL, Compose, CI | Written, never compiled or executed |
| Version pins (Boot 4.1.x patch, starter names, Kafka image tag, Jackson 3) | Unverified; first `mvn verify` is the test |
