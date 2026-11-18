package com.shubhoom.orders.common;

import tools.jackson.databind.json.JsonMapper;

// Only class that imports Jackson (Boot 4 uses Jackson 3). If you get Jackson 2, swap to com.fasterxml ObjectMapper.
public final class EventSerde {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private EventSerde() {}

    public static String toJson(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static <T> T fromJson(String json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }
}
