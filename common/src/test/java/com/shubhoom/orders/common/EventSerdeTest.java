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
        assertEquals(1, back.schemaVersion());
        assertEquals(2, ((Number) back.payload().get("quantity")).intValue());
    }

    @Test
    void eachEnvelopeGetsAFreshEventId() {
        EventEnvelope a = EventEnvelope.of("T", "k", Map.of());
        EventEnvelope b = EventEnvelope.of("T", "k", Map.of());
        assertNotEquals(a.eventId(), b.eventId());
    }

    @Test
    void headersMapConvertsValuesToStrings() {
        Map<String, String> headers = EventSerde.toStringMap("{\"traceparent\":\"00-abc-def-01\",\"n\":3}");
        assertEquals("00-abc-def-01", headers.get("traceparent"));
        assertEquals("3", headers.get("n"));
    }
}
