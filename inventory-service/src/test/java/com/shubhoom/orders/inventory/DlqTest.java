package com.shubhoom.orders.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.shubhoom.orders.common.DlqReplay;
import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest
@Testcontainers
class DlqTest {

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
        r.add("outbox.relay.enabled", () -> "false");
        r.add("kafka.consumer.retry-attempts", () -> "2");
        r.add("kafka.consumer.retry-backoff-ms", () -> "100");
    }

    @Autowired KafkaTemplate<String, String> producer;
    @Autowired DlqReplay replay;
    @Autowired JdbcClient jdbc;

    // A message that can never be processed must land in the DLQ, and the messages behind it must still flow.
    @Test
    void poisonMessageGoesToDlqAndDoesNotBlockTheQueue() throws Exception {
        String poisonKey = UUID.randomUUID().toString();
        producer.send(Topics.INVENTORY_COMMANDS, poisonKey, "this is not json").get(10, TimeUnit.SECONDS);

        seed("flow", 5);
        String orderId = UUID.randomUUID().toString();
        producer.send(Topics.INVENTORY_COMMANDS, orderId, reserve(orderId, "flow")).get(10, TimeUnit.SECONDS);

        await(() -> reservations(orderId) == 1, 60);
        assertEquals("this is not json", firstDlqValue(poisonKey));
    }

    // A parked message, once the cause is fixed, can be replayed and is then processed normally.
    @Test
    void replayRedeliversParkedMessages() throws Exception {
        seed("replayed", 5);
        String orderId = UUID.randomUUID().toString();
        producer.send(Topics.INVENTORY_COMMANDS + ".dlq", orderId, reserve(orderId, "replayed")).get(10, TimeUnit.SECONDS);
        assertEquals(0L, reservations(orderId));

        int n = replay.replay(Topics.INVENTORY_COMMANDS);

        await(() -> reservations(orderId) == 1, 60);
        assertEquals(true, n >= 1);
        assertEquals(0, replay.replay(Topics.INVENTORY_COMMANDS), "a second replay must find nothing new");
    }

    // The trace id in a Kafka header must reach the logging context and be written to the outbox of the reply.
    @Test
    void traceIdFromKafkaHeaderReachesTheOutbox() throws Exception {
        seed("traced", 5);
        String orderId = UUID.randomUUID().toString();
        ProducerRecord<String, String> record =
                new ProducerRecord<>(Topics.INVENTORY_COMMANDS, orderId, reserve(orderId, "traced"));
        record.headers().add("traceId", "trace-from-kafka".getBytes(StandardCharsets.UTF_8));
        producer.send(record).get(10, TimeUnit.SECONDS);

        await(() -> reservations(orderId) == 1, 60);
        assertEquals("trace-from-kafka", jdbc.sql(
                        "SELECT headers->>'traceId' FROM outbox WHERE msg_key = :k AND type = 'InventoryReserved'")
                .param("k", orderId).query(String.class).single());
    }

    private static String reserve(String orderId, String sku) {
        return EventSerde.toJson(EventEnvelope.of("ReserveInventory", orderId,
                Map.of("orderId", orderId, "items", List.of(Map.of("sku", sku, "quantity", 1)))));
    }

    private void seed(String sku, int available) {
        jdbc.sql("INSERT INTO stock (sku, available) VALUES (:sku, :a)")
                .param("sku", sku).param("a", available).update();
    }

    private long reservations(String orderId) {
        return jdbc.sql("SELECT count(*) FROM reservations WHERE order_id = CAST(:id AS uuid)")
                .param("id", orderId).query(Long.class).single();
    }

    private String firstDlqValue(String key) {
        Properties p = new Properties();
        p.put("bootstrap.servers", kafka.getBootstrapServers());
        p.put("group.id", "test-" + UUID.randomUUID());
        p.put("auto.offset.reset", "earliest");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(Topics.INVENTORY_COMMANDS + ".dlq"));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(r.key())) {
                        return r.value();
                    }
                }
            }
        }
        throw new AssertionError("poison message never reached the DLQ");
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
