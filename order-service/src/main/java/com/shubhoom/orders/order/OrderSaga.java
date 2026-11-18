package com.shubhoom.orders.order;

import com.shubhoom.orders.common.Inbox;
import com.shubhoom.orders.common.OutboxWriter;
import com.shubhoom.orders.common.Topics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// PENDING -> AWAITING_PAYMENT -> CONFIRMED
//    |              |
//    |              +-> RELEASING -> CANCELLED   (payment declined: compensate by releasing stock)
//    +-> CANCELLED                               (stock rejected)
// A step stuck too long is re-sent, then parked in MANUAL_REVIEW.
@Service
public class OrderSaga {

    private record Stuck(UUID id, String status, int attempts) {}

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);
    private static final String CONSUMER = "order-saga";

    private final JdbcClient jdbc;
    private final Inbox inbox;
    private final OutboxWriter outbox;

    public OrderSaga(JdbcClient jdbc, Inbox inbox, OutboxWriter outbox) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.outbox = outbox;
    }

    // Each transition is a guarded UPDATE, so duplicate or late events change nothing.
    @Transactional
    public void on(String eventId, String type, String orderId) {
        if (!inbox.firstTime(CONSUMER, eventId)) {
            return;
        }
        UUID id = UUID.fromString(orderId);
        boolean moved = switch (type) {
            case "InventoryReserved" -> move(id, "PENDING", "AWAITING_PAYMENT");
            case "InventoryRejected" -> move(id, "PENDING", "CANCELLED");
            case "PaymentCharged" -> move(id, "AWAITING_PAYMENT", "CONFIRMED");
            case "PaymentDeclined" -> move(id, "AWAITING_PAYMENT", "RELEASING");
            case "InventoryReleased" -> move(id, "RELEASING", "CANCELLED");
            default -> false;
        };
        if (!moved) {
            log.info("ignored {} for order {}", type, orderId);
            return;
        }
        switch (type) {
            case "InventoryReserved" -> command(id, "AWAITING_PAYMENT");
            case "PaymentDeclined" -> command(id, "RELEASING");
            case "PaymentCharged" -> event(orderId, "OrderConfirmed");
            default -> event(orderId, "OrderCancelled");
        }
    }

    // Re-sends the command of any step stuck longer than timeoutSeconds; after maxAttempts parks the order.
    @Transactional
    public int sweep(int timeoutSeconds, int maxAttempts) {
        List<Stuck> stuck = jdbc.sql("""
                SELECT id, status, attempts FROM orders
                WHERE status IN ('PENDING', 'AWAITING_PAYMENT', 'RELEASING')
                  AND updated_at < now() - make_interval(secs => CAST(:t AS double precision))
                ORDER BY updated_at LIMIT 100 FOR UPDATE SKIP LOCKED
                """)
                .param("t", timeoutSeconds)
                .query((rs, i) -> new Stuck(rs.getObject("id", UUID.class), rs.getString("status"), rs.getInt("attempts")))
                .list();

        for (Stuck s : stuck) {
            if (s.attempts() >= maxAttempts) {
                jdbc.sql("UPDATE orders SET status = 'MANUAL_REVIEW', updated_at = now() WHERE id = :id")
                        .param("id", s.id()).update();
                event(s.id().toString(), "OrderNeedsReview");
            } else {
                jdbc.sql("UPDATE orders SET attempts = attempts + 1, updated_at = now() WHERE id = :id")
                        .param("id", s.id()).update();
                command(s.id(), s.status());
            }
        }
        return stuck.size();
    }

    private boolean move(UUID id, String from, String to) {
        return jdbc.sql("UPDATE orders SET status = :to, attempts = 0, updated_at = now() "
                        + "WHERE id = :id AND status = :from")
                .param("to", to).param("id", id).param("from", from)
                .update() == 1;
    }

    private void command(UUID id, String status) {
        String orderId = id.toString();
        switch (status) {
            case "PENDING" -> {
                List<Map<String, Object>> items = jdbc.sql("SELECT sku, quantity FROM order_items WHERE order_id = :id")
                        .param("id", id)
                        .query((rs, i) -> Map.<String, Object>of("sku", rs.getString(1), "quantity", rs.getInt(2)))
                        .list();
                outbox.append(Topics.INVENTORY_COMMANDS, orderId, "ReserveInventory",
                        Map.of("orderId", orderId, "items", items));
            }
            case "AWAITING_PAYMENT" -> {
                Map<String, Object> payload = new HashMap<>(
                        jdbc.sql("SELECT amount_cents, currency FROM orders WHERE id = :id")
                                .param("id", id)
                                .query((rs, i) -> Map.<String, Object>of(
                                        "amountCents", rs.getLong(1), "currency", rs.getString(2)))
                                .single());
                payload.put("orderId", orderId);
                outbox.append(Topics.PAYMENT_COMMANDS, orderId, "ChargePayment", payload);
            }
            case "RELEASING" -> outbox.append(Topics.INVENTORY_COMMANDS, orderId, "ReleaseInventory",
                    Map.of("orderId", orderId));
            default -> { }
        }
    }

    private void event(String orderId, String type) {
        outbox.append(Topics.ORDER_EVENTS, orderId, type, Map.of("orderId", orderId));
    }
}
