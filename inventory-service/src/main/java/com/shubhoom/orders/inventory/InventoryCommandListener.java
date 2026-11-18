package com.shubhoom.orders.inventory;

import com.shubhoom.orders.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-inventory.commands")
    void onMessage(String value) {
        log.info("received {}", value);
    }
}
