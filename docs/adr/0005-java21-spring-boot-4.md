# ADR-0005: Java 21 and Spring Boot 4.1.x (thin Spring surface)

Status: Accepted, **low confidence** | Date: 2026-11-04

## Context

The user wants Java / Spring Boot. Code is written without being compilable in the authoring workspace (Maven Central is blocked there), so the user is the first to compile it. That makes "code I can write correctly from knowledge" a real criterion, not a footnote.

## Options considered

| Criterion (weight) | Boot 4.1.x | Boot 4.0.x (latest patch) | Boot 3.5.x |
|---|---|---|---|
| Currency and support window (25) | 5 | 4 | 2 |
| Known-correct code without compiling (25) | 3 | 3 | 5 |
| Ecosystem fit: Kafka, JDBC, Micrometer (20) | 5 | 5 | 5 |
| Learning fit (15) | 4 | 4 | 3 |
| Migration risk (15) | 3 | 3 | 5 |
| **Weighted total** | **4.05** | **3.80** | **3.95** |

## Sensitivity

**The winner flips.** Lowering "currency" by 10 or raising "known-correct code" by 10 makes 3.5.x win. This is a near tie (4.05 vs 3.95). Treat it as one.

## Evidence

- Boot 4.1.0 was released 2026-06-10; 4.0.8 on 2026-08-20 (spring.io release posts).
- Boot 4.1 release notes list Spring Framework 7.0.x, **Spring Kafka 4.1.0**, Micrometer 1.17.0, Micrometer Tracing 1.7.0, OpenTelemetry 1.62.0, Flyway 12.4.0, Prometheus client 1.5.1, and new Kafka observation conventions with no listed breaking changes.
- Boot 3.5 support: Spring's policy gives minor releases at least 13 months of OSS support. 3.5 shipped in May 2025, so OSS support has most likely ended (about June 2026). That is an **inference, not a confirmed date**; the policy page does not list end dates.
- Not confirmed from sources: the exact current 4.1.x patch, the Jackson major version used by default, and the managed Testcontainers version. These must be checked when generating the project.

## Decision

Java 21 (LTS) with **Spring Boot 4.1.x**, pinning the latest 4.1 patch available from start.spring.io at build time. Because the decision is close and the code is unverified, we reduce exposure to Boot 4 migration surprises by design:

1. **No JPA/Hibernate.** Use `JdbcClient`/`JdbcTemplate` with explicit SQL. The outbox claim query (`FOR UPDATE SKIP LOCKED`) is SQL anyway, and this removes the JPA bootstrap behaviour changes noted in the 4.1 release notes.
2. **Isolate JSON.** One `EventSerde` class in `common` wraps the ObjectMapper/JsonMapper; the Jackson 2 vs 3 package difference is confined to that file.
3. **Thin Spring Kafka usage.** `KafkaTemplate` for sending, `@KafkaListener`/`@RetryableTopic` for consuming, manual ack. No exotic features.
4. **Flyway for schema** and **Micrometer/Actuator** for metrics: standard, version-managed by the Boot BOM.

Fallback is explicit: if more than a day is lost to Boot 4 build problems, drop to 3.5.x (or 4.0.x) and record it here.

## Consequences

- Possible compile or configuration friction on first build; every phase ships with a "first run" checklist to catch it early.
- Slightly more hand-written SQL than JPA; also better for teaching the transaction boundaries.

## Revisit when

- First `mvn verify` fails for framework-migration reasons that cost more than a day.
- A newer 4.x patch fixes issues we hit.
