package com.shubhoom.orders.common;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true")
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 100;

    private record Row(long id, String topic, String key, String value, String traceId) {}

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-ms:50}")
    void poll() {
        try {
            int n;
            do {
                n = tx.execute(s -> publishBatch());
            } while (n == BATCH);
        } catch (Exception e) {
            log.warn("outbox publish failed, will retry: {}", e.toString());
        }
    }

    @Scheduled(fixedDelay = 3_600_000)
    public void prune() {
        jdbc.sql("DELETE FROM outbox WHERE published_at < now() - interval '7 days'").update();
    }

    // At-least-once: a crash after send but before the UPDATE republishes the row.
    private int publishBatch() {
        List<Row> rows = jdbc.sql("""
                SELECT id, topic, msg_key, payload::text, headers->>'traceId' FROM outbox
                WHERE published_at IS NULL ORDER BY id LIMIT :n FOR UPDATE SKIP LOCKED
                """)
                .param("n", BATCH)
                .query((rs, i) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)))
                .list();

        for (Row r : rows) {
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(r.topic(), r.key(), r.value());
                if (r.traceId() != null) {
                    record.headers().add(Trace.KEY, r.traceId().getBytes(StandardCharsets.UTF_8));
                }
                kafka.send(record).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("publish failed for outbox row " + r.id(), e);
            }
        }
        if (!rows.isEmpty()) {
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", rows.stream().map(Row::id).toList())
                    .update();
        }
        return rows.size();
    }
}
