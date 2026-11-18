package com.shubhoom.orders.payment;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest
@Testcontainers
class PaymentIdempotencyTest {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("outbox.relay.enabled", () -> "false");
        r.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired PaymentService payments;
    @Autowired PaymentGateway gateway;
    @Autowired JdbcClient jdbc;

    // The same command delivered 5 times concurrently, and again under a new eventId (a saga re-send):
    // the gateway is charged once and one reply is emitted per distinct delivery path that was not deduplicated.
    @Test
    void orderIsChargedOnce() throws Exception {
        String orderId = id();
        String eventId = id();
        int before = gateway.calls();
        Callable<Void> deliver = () -> {
            payments.charge(eventId, orderId, 2000, "USD");
            return null;
        };

        ExecutorService pool = Executors.newFixedThreadPool(5);
        for (Future<Void> f : pool.invokeAll(Collections.nCopies(5, deliver))) {
            f.get();
        }
        pool.shutdown();
        payments.charge(id(), orderId, 2000, "USD");

        assertEquals(1, gateway.calls() - before);
        assertEquals(1L, count("SELECT count(*) FROM payments WHERE order_id = CAST(:o AS uuid)", orderId));
        assertEquals("CHARGED", status(orderId));
        assertEquals(2L, count("SELECT count(*) FROM outbox WHERE msg_key = :o AND type = 'PaymentCharged'", orderId));
    }

    @Test
    void declinedOrderStaysDeclined() {
        String orderId = id();
        payments.charge(id(), orderId, 5_000_000, "USD");
        payments.charge(id(), orderId, 5_000_000, "USD");

        assertEquals("DECLINED", status(orderId));
        assertEquals(2L, count("SELECT count(*) FROM outbox WHERE msg_key = :o AND type = 'PaymentDeclined'", orderId));
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    private String status(String orderId) {
        return jdbc.sql("SELECT status FROM payments WHERE order_id = CAST(:o AS uuid)")
                .param("o", orderId).query(String.class).single();
    }

    private long count(String sql, String orderId) {
        return jdbc.sql(sql).param("o", orderId).query(Long.class).single();
    }
}
