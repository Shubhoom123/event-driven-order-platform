package com.shubhoom.orders.common;

import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final JdbcClient jdbc;

    public OutboxWriter(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // MANDATORY: fails fast if called outside the transaction that changes state.
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, String key, String type, Map<String, Object> payload) {
        EventEnvelope e = EventEnvelope.of(type, key, payload);
        jdbc.sql("""
                INSERT INTO outbox (event_id, topic, msg_key, type, payload, headers)
                VALUES (CAST(:id AS uuid), :topic, :key, :type, CAST(:payload AS jsonb), CAST(:headers AS jsonb))
                """)
                .param("id", e.eventId())
                .param("topic", topic)
                .param("key", key)
                .param("type", type)
                .param("payload", EventSerde.toJson(e))
                .param("headers", EventSerde.toJson(Map.of(Trace.KEY, Trace.current())))
                .update();
    }
}
