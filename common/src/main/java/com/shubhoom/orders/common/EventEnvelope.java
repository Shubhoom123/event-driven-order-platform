package com.shubhoom.orders.common;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Wire format for every event/command. Strings for ids and time keep JSON handling trivial. */
public record EventEnvelope(
        String eventId,
        String type,
        int schemaVersion,
        String aggregateId,
        String occurredAt,
        Map<String, Object> payload) {

    public static EventEnvelope of(String type, String aggregateId, Map<String, Object> payload) {
        return new EventEnvelope(
                UUID.randomUUID().toString(), type, 1, aggregateId, Instant.now().toString(), payload);
    }
}
