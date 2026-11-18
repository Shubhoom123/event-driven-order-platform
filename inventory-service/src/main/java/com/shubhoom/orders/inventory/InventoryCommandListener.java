package com.shubhoom.orders.inventory;

import com.shubhoom.orders.common.EventEnvelope;
import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Topics;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    private final InventoryService service;

    InventoryCommandListener(InventoryService service) {
        this.service = service;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-inventory.commands")
    void onMessage(String value) {
        EventEnvelope e = EventSerde.fromJson(value, EventEnvelope.class);
        switch (e.type()) {
            case "ReserveInventory" -> service.reserve(e.eventId(), e.aggregateId(), items(e));
            case "ReleaseInventory" -> service.release(e.eventId(), e.aggregateId());
            default -> log.warn("ignoring unknown command {}", e.type());
        }
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "inventory-order.events")
    void onOrderEvent(String value) {
        EventEnvelope e = EventSerde.fromJson(value, EventEnvelope.class);
        if (e.type().equals("OrderConfirmed")) {
            service.commit(e.eventId(), e.aggregateId());
        }
    }

    @SuppressWarnings("unchecked")
    private static List<InventoryService.Item> items(EventEnvelope e) {
        return ((List<Map<String, Object>>) e.payload().get("items")).stream()
                .map(m -> new InventoryService.Item((String) m.get("sku"), ((Number) m.get("quantity")).intValue()))
                .toList();
    }
}
