package com.shubhoom.orders.common;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class Inbox {

    private final JdbcClient jdbc;

    public Inbox(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // True the first time this event is seen. Must run in the same transaction as the effect it guards,
    // so a crash before commit leaves no trace and a redelivery after commit is skipped.
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstTime(String consumer, String eventId) {
        return jdbc.sql("""
                INSERT INTO inbox (consumer, event_id) VALUES (:consumer, CAST(:eventId AS uuid))
                ON CONFLICT DO NOTHING
                """)
                .param("consumer", consumer)
                .param("eventId", eventId)
                .update() == 1;
    }
}
