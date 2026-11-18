package com.shubhoom.orders.common;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Skeleton relay: claim unpublished rows, send, wait for the broker ack, mark published.
 * Delivery is at-least-once (a crash after send, before the UPDATE, republishes the row).
 * Phase 2 hardens this and adds the broker-down test.
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true")
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private record Row(long id, String eventId, String topic, String key, String type,
                       String value, String headers) {}

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final int batchSize;
    private final long sendTimeoutMs;

    public OutboxRelay(JdbcClient jdbc,
                       KafkaTemplate<String, String> kafka,
                       PlatformTransactionManager txManager,
                       @Value("${outbox.relay.batch-size:100}") int batchSize,
                       @Value("${outbox.relay.send-timeout-ms:10000}") long sendTimeoutMs) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = new TransactionTemplate(txManager);
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-ms:50}")
    public void poll() {
        try {
            int published;
            do {
                Integer n = tx.execute(status -> publishBatch());
                published = n == null ? 0 : n;
            } while (published >= batchSize);
        } catch (Exception e) {
            // Rows stay unpublished (transaction rolled back); the next poll retries.
            log.warn("outbox relay batch failed, will retry: {}", e.toString());
        }
    }

    private int publishBatch() {
        List<Row> rows = jdbc.sql("""
                SELECT id, event_id::text AS event_id, topic, msg_key, type,
                       payload::text AS payload, headers::text AS headers
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT :n
                FOR UPDATE SKIP LOCKED
                """)
                .param("n", batchSize)
                .query((rs, i) -> new Row(rs.getLong("id"), rs.getString("event_id"),
                        rs.getString("topic"), rs.getString("msg_key"), rs.getString("type"),
                        rs.getString("payload"), rs.getString("headers")))
                .list();

        for (Row row : rows) {
            send(row);
        }
        if (!rows.isEmpty()) {
            List<Long> ids = rows.stream().map(Row::id).toList();
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", ids)
                    .update();
        }
        return rows.size();
    }

    private void send(Row row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.value());
        record.headers().add("eventId", row.eventId().getBytes(StandardCharsets.UTF_8));
        record.headers().add("type", row.type().getBytes(StandardCharsets.UTF_8));
        EventSerde.toStringMap(row.headers())
                .forEach((k, v) -> record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        try {
            kafka.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing outbox row " + row.id(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("publish failed for outbox row " + row.id(), e);
        }
    }
}
