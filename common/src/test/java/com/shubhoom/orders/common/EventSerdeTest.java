package com.shubhoom.orders.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class EventSerdeTest {

    @Test
    void envelopeRoundTrips() {
        EventEnvelope original = EventEnvelope.of("ReserveInventory", "order-1", Map.of("quantity", 2));

        EventEnvelope back = EventSerde.fromJson(EventSerde.toJson(original), EventEnvelope.class);

        assertEquals(original.eventId(), back.eventId());
        assertEquals("ReserveInventory", back.type());
        assertEquals("order-1", back.aggregateId());
        assertEquals(2, ((Number) back.payload().get("quantity")).intValue());
    }

    @Test
    void eachEnvelopeGetsAFreshEventId() {
        assertNotEquals(EventEnvelope.of("T", "k", Map.of()).eventId(),
                EventEnvelope.of("T", "k", Map.of()).eventId());
    }
}
