package com.shubhoom.orders.order;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.OutboxRelay;
import com.shubhoom.orders.common.Topics;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest
@Testcontainers
class OutboxTest {

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.2.0");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.kafka.producer.properties.max.block.ms", () -> "5000");
        r.add("spring.kafka.listener.auto-startup", () -> "false");
        r.add("saga.sweeper.enabled", () -> "false");
    }

    @Autowired OrderService orders;
    @Autowired OutboxRelay relay;
    @Autowired JdbcClient jdbc;

    // Broker frozen (docker pause) while orders arrive: intake must still succeed, nothing may be
    // published, and after the broker resumes every event must arrive. Duplicates are allowed
    // (at-least-once); losses are not.
    @Test
    void brokerOutageLosesNoEvents() throws Exception {
        Set<String> orderIds = new HashSet<>();
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
        try {
            for (int i = 0; i < 20; i++) {
                orderIds.add(orders.create(request(), null).order().orderId().toString());
            }
            Thread.sleep(6000);
            assertEquals(40L, unpublished(), "events must stay in the outbox while the broker is down");
        } finally {
            kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        await(() -> unpublished() == 0, 60);

        Map<String, EventEnvelope> received = consume(40);
        assertEquals(40, received.size(), "every event must arrive at least once");
        Set<String> receivedOrderIds = new HashSet<>();
        received.values().forEach(e -> receivedOrderIds.add(e.aggregateId()));
        assertEquals(orderIds, receivedOrderIds);
    }

    @Test
    void pruneDeletesOnlyOldPublishedRows() {
        insertPublished("prune-old", "now() - interval '8 days'");
        insertPublished("prune-recent", "now()");

        relay.prune();

        assertEquals(0L, countByKey("prune-old"));
        assertEquals(1L, countByKey("prune-recent"));
    }

    private void insertPublished(String key, String publishedAt) {
        jdbc.sql("INSERT INTO outbox (event_id, topic, msg_key, type, payload, published_at) "
                        + "VALUES (gen_random_uuid(), 't', :k, 'X', CAST('{}' AS jsonb), " + publishedAt + ")")
                .param("k", key)
                .update();
    }

    private long countByKey(String key) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE msg_key = :k").param("k", key).query(Long.class).single();
    }

    private long unpublished() {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single();
    }

    private static CreateOrderRequest request() {
        return new CreateOrderRequest("c-1", List.of(new CreateOrderRequest.Item("SKU-1", 1)), 1000, "USD");
    }

    private Map<String, EventEnvelope> consume(int expected) {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "test-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Map<String, EventEnvelope> byEventId = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(Topics.INVENTORY_COMMANDS, Topics.ORDER_EVENTS));
            long deadline = System.currentTimeMillis() + 30_000;
            while (byEventId.size() < expected && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    EventEnvelope e = EventSerde.fromJson(r.value(), EventEnvelope.class);
                    byEventId.put(e.eventId(), e);
                }
            }
        }
        return byEventId;
    }

    private static void await(BooleanSupplier condition, int seconds) throws InterruptedException {
        long end = System.currentTimeMillis() + seconds * 1000L;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > end) {
                throw new AssertionError("timed out waiting for condition");
            }
            Thread.sleep(200);
        }
    }
}
