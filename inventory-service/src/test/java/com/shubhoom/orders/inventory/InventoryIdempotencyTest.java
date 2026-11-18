package com.shubhoom.orders.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.shubhoom.orders.inventory.InventoryService.Item;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
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
class InventoryIdempotencyTest {

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

    @Autowired InventoryService inventory;
    @Autowired JdbcClient jdbc;

    // The same event delivered 5 times, concurrently: stock moves once, one reply is emitted.
    @Test
    void duplicateDeliveryReservesOnce() throws Exception {
        seed("dup", 10);
        String orderId = id();
        String eventId = id();
        Callable<Void> deliver = () -> {
            inventory.reserve(eventId, orderId, List.of(new Item("dup", 3)));
            return null;
        };

        ExecutorService pool = Executors.newFixedThreadPool(5);
        for (Future<Void> f : pool.invokeAll(Collections.nCopies(5, deliver))) {
            f.get();
        }
        pool.shutdown();

        assertEquals(7, stock("dup", "available"));
        assertEquals(3, stock("dup", "reserved"));
        assertEquals(1L, outbox(orderId, "InventoryReserved"));
    }

    @Test
    void rejectedOrderPutsPartialReservationBack() {
        seed("rej-a", 5);
        seed("rej-b", 1);
        String orderId = id();

        inventory.reserve(id(), orderId, List.of(new Item("rej-a", 1), new Item("rej-b", 99)));

        assertEquals(5, stock("rej-a", "available"));
        assertEquals(0, stock("rej-a", "reserved"));
        assertEquals(1, stock("rej-b", "available"));
        assertEquals(1L, outbox(orderId, "InventoryRejected"));
        assertEquals(0L, outbox(orderId, "InventoryReserved"));
    }

    @Test
    void releaseRestoresStockOnlyOnce() {
        seed("rel", 10);
        String orderId = id();
        inventory.reserve(id(), orderId, List.of(new Item("rel", 4)));
        assertEquals(6, stock("rel", "available"));

        inventory.release(id(), orderId);
        inventory.release(id(), orderId);

        assertEquals(10, stock("rel", "available"));
        assertEquals(0, stock("rel", "reserved"));
    }

    @Test
    void commitMovesReservedToSoldOnce() {
        seed("com", 10);
        String orderId = id();
        inventory.reserve(id(), orderId, List.of(new Item("com", 4)));

        inventory.commit(id(), orderId);
        inventory.commit(id(), orderId);
        inventory.release(id(), orderId);

        assertEquals(6, stock("com", "available"));
        assertEquals(0, stock("com", "reserved"));
        assertEquals(4, stock("com", "sold"));
    }

    @Test
    void outboxRowsCarryTheCurrentTraceId() {
        seed("trace", 5);
        String orderId = id();
        MDC.put("traceId", "trace-abc");
        try {
            inventory.reserve(id(), orderId, List.of(new Item("trace", 1)));
        } finally {
            MDC.clear();
        }
        assertEquals("trace-abc", jdbc.sql("SELECT headers->>'traceId' FROM outbox WHERE msg_key = :k")
                .param("k", orderId).query(String.class).single());
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    private void seed(String sku, int available) {
        jdbc.sql("INSERT INTO stock (sku, available) VALUES (:sku, :a)")
                .param("sku", sku).param("a", available).update();
    }

    private int stock(String sku, String column) {
        return jdbc.sql("SELECT " + column + " FROM stock WHERE sku = :sku")
                .param("sku", sku).query(Integer.class).single();
    }

    private long outbox(String orderId, String type) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE msg_key = :k AND type = :t")
                .param("k", orderId).param("t", type).query(Long.class).single();
    }
}
