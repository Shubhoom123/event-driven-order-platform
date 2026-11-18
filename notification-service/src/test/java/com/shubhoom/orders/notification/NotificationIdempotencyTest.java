package com.shubhoom.orders.notification;

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
class NotificationIdempotencyTest {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired NotificationService notifications;
    @Autowired JdbcClient jdbc;

    @Test
    void sameEventDeliveredConcurrentlyNotifiesOnce() throws Exception {
        String orderId = id();
        String eventId = id();
        Callable<Void> deliver = () -> {
            notifications.notify(eventId, orderId, "OrderConfirmed");
            return null;
        };
        ExecutorService pool = Executors.newFixedThreadPool(5);
        for (Future<Void> f : pool.invokeAll(Collections.nCopies(5, deliver))) {
            f.get();
        }
        pool.shutdown();

        assertEquals(1L, count(orderId));
    }

    @Test
    void resentEventWithNewIdStillNotifiesOnce() {
        String orderId = id();
        notifications.notify(id(), orderId, "OrderCancelled");
        notifications.notify(id(), orderId, "OrderCancelled");

        assertEquals(1L, count(orderId));
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    private long count(String orderId) {
        return jdbc.sql("SELECT count(*) FROM notifications WHERE order_id = CAST(:o AS uuid)")
                .param("o", orderId).query(Long.class).single();
    }
}
