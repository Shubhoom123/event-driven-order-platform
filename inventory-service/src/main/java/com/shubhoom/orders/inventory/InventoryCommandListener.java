package com.shubhoom.orders.inventory;

import com.shubhoom.orders.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Skeleton only: proves a command crosses the whole pipeline. Real handling arrives in Phase 3. */
@Component
class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-inventory.commands")
    void onMessage(ConsumerRecord<String, String> record) {
        log.info("received topic={} partition={} offset={} key={} value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
}
