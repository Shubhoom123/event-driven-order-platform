package com.shubhoom.orders.order;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
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
class SagaTest {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("outbox.relay.enabled", () -> "false");
        r.add("saga.sweeper.enabled", () -> "false");
        r.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired OrderService orders;
    @Autowired OrderSaga saga;
    @Autowired JdbcClient jdbc;
    @Autowired MeterRegistry registry;

    @Test
    void happyPathConfirmsOrder() {
        String id = newOrder();
        saga.on(ev(), "InventoryReserved", id);
        assertEquals("AWAITING_PAYMENT", status(id));
        assertEquals(1L, sent(id, "ChargePayment"));

        saga.on(ev(), "PaymentCharged", id);
        assertEquals("CONFIRMED", status(id));
        assertEquals(1L, sent(id, "OrderConfirmed"));
        assertEquals(1.0, registry.counter("saga.transition", "to", "CONFIRMED").count());
        assertEquals(1L, registry.get("saga.duration").tag("outcome", "CONFIRMED").timer().count());
    }

    @Test
    void declinedPaymentReleasesInventoryThenCancels() {
        String id = newOrder();
        saga.on(ev(), "InventoryReserved", id);
        saga.on(ev(), "PaymentDeclined", id);
        assertEquals("RELEASING", status(id));
        assertEquals(1L, sent(id, "ReleaseInventory"));

        saga.on(ev(), "InventoryReleased", id);
        assertEquals("CANCELLED", status(id));
        assertEquals(1L, sent(id, "OrderCancelled"));
    }

    @Test
    void rejectedInventoryCancelsOrder() {
        String id = newOrder();
        saga.on(ev(), "InventoryRejected", id);
        assertEquals("CANCELLED", status(id));
        assertEquals(0L, sent(id, "ChargePayment"));
    }

    @Test
    void duplicateAndLateEventsAreIgnored() {
        String id = newOrder();
        String reserved = ev();
        saga.on(reserved, "InventoryReserved", id);
        saga.on(reserved, "InventoryReserved", id);
        saga.on(ev(), "InventoryReserved", id);
        assertEquals(1L, sent(id, "ChargePayment"));

        saga.on(ev(), "PaymentDeclined", id);
        saga.on(ev(), "PaymentCharged", id);
        assertEquals("RELEASING", status(id));
        assertEquals(0L, sent(id, "OrderConfirmed"));
    }

    @Test
    void stuckOrderIsRetriedThenSentToManualReview() {
        String id = newOrder();
        for (int i = 0; i < 3; i++) {
            saga.sweep(0, 3);
        }
        assertEquals(4L, sent(id, "ReserveInventory"));
        assertEquals("PENDING", status(id));

        saga.sweep(0, 3);
        assertEquals("MANUAL_REVIEW", status(id));
        assertEquals(4L, sent(id, "ReserveInventory"));
        assertEquals(1L, sent(id, "OrderNeedsReview"));
    }

    private String newOrder() {
        return orders.create(new CreateOrderRequest("c1", List.of(new CreateOrderRequest.Item("SKU-1", 1)), 1000, "USD"),
                null).order().orderId().toString();
    }

    private static String ev() {
        return UUID.randomUUID().toString();
    }

    private String status(String id) {
        return jdbc.sql("SELECT status FROM orders WHERE id = CAST(:id AS uuid)").param("id", id)
                .query(String.class).single();
    }

    private long sent(String id, String type) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE msg_key = :k AND type = :t")
                .param("k", id).param("t", type).query(Long.class).single();
    }
}
