package com.shubhoom.orders.common;

import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

/**
 * The only class that imports Jackson. Spring Boot 4 uses Jackson 3 (tools.jackson).
 * If your build resolves Jackson 2 instead, change the import to
 * com.fasterxml.jackson.databind.ObjectMapper and use new ObjectMapper(); nothing else changes.
 */
public final class EventSerde {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private EventSerde() {}

    public static String toJson(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> toStringMap(String json) {
        Map<String, Object> raw = MAPPER.readValue(json, Map.class);
        Map<String, String> out = new LinkedHashMap<>();
        raw.forEach((k, v) -> out.put(k, String.valueOf(v)));
        return out;
    }
}
