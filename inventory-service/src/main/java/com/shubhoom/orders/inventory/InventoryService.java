package com.shubhoom.orders.inventory;

import com.shubhoom.orders.common.EventSerde;
import com.shubhoom.orders.common.Inbox;
import com.shubhoom.orders.common.OutboxWriter;
import com.shubhoom.orders.common.Topics;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {

    public record Item(String sku, int quantity) {}

    private static final String CONSUMER = "inventory";

    private final JdbcClient jdbc;
    private final Inbox inbox;
    private final OutboxWriter outbox;

    public InventoryService(JdbcClient jdbc, Inbox inbox, OutboxWriter outbox) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.outbox = outbox;
    }

    @Transactional
    public void reserve(String eventId, String orderId, List<Item> items) {
        if (!inbox.firstTime(CONSUMER, eventId)) {
            return;
        }
        UUID id = UUID.fromString(orderId);

        int inserted = jdbc.sql("""
                INSERT INTO reservations (order_id, status, items)
                VALUES (:id, 'RESERVED', CAST(:items AS jsonb))
                ON CONFLICT (order_id) DO NOTHING
                """)
                .param("id", id)
                .param("items", EventSerde.toJson(items))
                .update();

        if (inserted == 0) {
            // Same order, new command (a saga re-send): repeat the earlier answer, change nothing.
            String status = jdbc.sql("SELECT status FROM reservations WHERE order_id = :id")
                    .param("id", id).query(String.class).single();
            switch (status) {
                case "REJECTED" -> emit(orderId, "InventoryRejected");
                case "RESERVED", "COMMITTED" -> emit(orderId, "InventoryReserved");
                default -> { }
            }
            return;
        }

        // Sorted SKU order avoids deadlocks between concurrent multi-item orders.
        List<Item> applied = new ArrayList<>();
        for (Item item : items.stream().sorted(Comparator.comparing(Item::sku)).toList()) {
            if (take(item) == 0) {
                applied.forEach(this::putBack);
                jdbc.sql("UPDATE reservations SET status = 'REJECTED' WHERE order_id = :id")
                        .param("id", id).update();
                emit(orderId, "InventoryRejected");
                return;
            }
            applied.add(item);
        }
        emit(orderId, "InventoryReserved");
    }

    @Transactional
    public void release(String eventId, String orderId) {
        if (!inbox.firstTime(CONSUMER, eventId)) {
            return;
        }
        // Flip RESERVED -> RELEASED and read the items in one statement, so stock is restored at most once.
        jdbc.sql("UPDATE reservations SET status = 'RELEASED' WHERE order_id = :id AND status = 'RESERVED' "
                        + "RETURNING items::text")
                .param("id", UUID.fromString(orderId))
                .query(String.class)
                .optional()
                .ifPresent(json -> {
                    for (Item item : EventSerde.fromJson(json, Item[].class)) {
                        putBack(item);
                    }
                });
        emit(orderId, "InventoryReleased");
    }

    private int take(Item item) {
        return jdbc.sql("UPDATE stock SET available = available - :q, reserved = reserved + :q "
                        + "WHERE sku = :sku AND available >= :q")
                .param("q", item.quantity()).param("sku", item.sku()).update();
    }

    private void putBack(Item item) {
        jdbc.sql("UPDATE stock SET available = available + :q, reserved = reserved - :q WHERE sku = :sku")
                .param("q", item.quantity()).param("sku", item.sku()).update();
    }

    private void emit(String orderId, String type) {
        outbox.append(Topics.INVENTORY_EVENTS, orderId, type, Map.of("orderId", orderId));
    }
}
