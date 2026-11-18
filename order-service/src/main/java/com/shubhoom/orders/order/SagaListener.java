package com.shubhoom.orders.order;

import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class SagaListener {

    private final OrderSaga saga;

    SagaListener(OrderSaga saga) {
        this.saga = saga;
    }

    @KafkaListener(topics = {Topics.INVENTORY_EVENTS, Topics.PAYMENT_EVENTS}, groupId = "order-saga")
    void onMessage(String value) {
        EventEnvelope e = EventSerde.fromJson(value, EventEnvelope.class);
        saga.on(e.eventId(), e.type(), e.aggregateId());
    }
}
