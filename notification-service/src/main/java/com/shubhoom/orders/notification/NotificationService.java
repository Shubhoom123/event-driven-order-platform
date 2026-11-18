package com.shubhoom.orders.notification;

import com.shubhoom.orders.common.Inbox;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final JdbcClient jdbc;
    private final Inbox inbox;

    public NotificationService(JdbcClient jdbc, Inbox inbox) {
        this.jdbc = jdbc;
        this.inbox = inbox;
    }

    // One notification per (order, type), however many times the event is delivered.
    // "Sending" is a log line; the recorded row is what a real sender would key its idempotency on.
    @Transactional
    public void notify(String eventId, String orderId, String type) {
        if (!inbox.firstTime("notification", eventId)) {
            return;
        }
        int inserted = jdbc.sql("INSERT INTO notifications (order_id, type) VALUES (:id, :type) "
                        + "ON CONFLICT (order_id, type) DO NOTHING")
                .param("id", UUID.fromString(orderId)).param("type", type).update();
        if (inserted == 1) {
            log.info("notification {} for order {}", type, orderId);
        }
    }
}
