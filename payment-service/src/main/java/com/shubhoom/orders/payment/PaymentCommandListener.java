package com.shubhoom.orders.payment;

import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class PaymentCommandListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);

    private final PaymentService service;

    PaymentCommandListener(PaymentService service) {
        this.service = service;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "payment-payment.commands")
    void onMessage(String value) {
        EventEnvelope e = EventSerde.fromJson(value, EventEnvelope.class);
        if (e.type().equals("ChargePayment")) {
            service.charge(e.eventId(), e.aggregateId(),
                    ((Number) e.payload().get("amountCents")).longValue(), (String) e.payload().get("currency"));
        } else {
            log.warn("ignoring unknown command {}", e.type());
        }
    }
}
