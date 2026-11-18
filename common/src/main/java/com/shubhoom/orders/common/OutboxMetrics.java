package com.shubhoom.orders.common;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

// Backlog size and age of the oldest unpublished event: the two numbers that show a stuck relay or broker.
@Component
class OutboxMetrics {

    OutboxMetrics(MeterRegistry registry, JdbcClient jdbc) {
        Gauge.builder("outbox.unpublished", () ->
                        jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single())
                .register(registry);
        Gauge.builder("outbox.oldest.age.seconds", () ->
                        jdbc.sql("SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) "
                                + "FROM outbox WHERE published_at IS NULL").query(Double.class).single())
                .register(registry);
    }
}
