package com.shubhoom.orders.notification;

import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Topics;
import java.util.Set;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class NotificationListener {

    private static final Set<String> NOTIFY_ON = Set.of("OrderConfirmed", "OrderCancelled", "OrderNeedsReview");

    private final NotificationService service;

    NotificationListener(NotificationService service) {
        this.service = service;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "notification-order.events")
    void onMessage(String value) {
        EventEnvelope e = EventSerde.fromJson(value, EventEnvelope.class);
        if (NOTIFY_ON.contains(e.type())) {
            service.notify(e.eventId(), e.aggregateId(), e.type());
        }
    }
}
