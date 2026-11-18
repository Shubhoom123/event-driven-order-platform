package com.shubhoom.orders.common;

import java.util.UUID;
import org.slf4j.MDC;

public final class Trace {
    public static final String KEY = "traceId";

    private Trace() {}

    // The trace of the current thread, or a new one when this is where the flow starts (e.g. the sweeper).
    public static String current() {
        String id = MDC.get(KEY);
        return id != null ? id : UUID.randomUUID().toString();
    }
}
