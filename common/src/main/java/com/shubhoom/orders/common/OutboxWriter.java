package com.shubhoom.orders.common;

import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Appends an event to the outbox. MANDATORY propagation means calling this without an
 * enclosing transaction fails loudly, which is the point: the outbox row must commit
 * atomically with the state change that caused it.
 */
@Component
public class OutboxWriter {

    private final JdbcClient jdbc;

    public OutboxWriter(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public String append(String topic, String key, String type, Map<String, Object> payload) {
        EventEnvelope envelope = EventEnvelope.of(type, key, payload);
        jdbc.sql("""
                INSERT INTO outbox (event_id, topic, msg_key, type, payload)
                VALUES (CAST(:eventId AS uuid), :topic, :key, :type, CAST(:payload AS jsonb))
                """)
                .param("eventId", envelope.eventId())
                .param("topic", topic)
                .param("key", key)
                .param("type", type)
                .param("payload", EventSerde.toJson(envelope))
                .update();
        return envelope.eventId();
    }
}
