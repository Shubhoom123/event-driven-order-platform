package com.shubhoom.orders.order;

import com.shubhoom.orders.common.OutboxWriter;
import com.shubhoom.orders.common.Topics;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    public record Result(OrderView order, boolean created) {}

    private static final String SELECT_ORDER = """
            SELECT id, status, customer_id, amount_cents, currency, created_at FROM orders
            """;

    private final JdbcClient jdbc;
    private final OutboxWriter outbox;

    public OrderService(JdbcClient jdbc, OutboxWriter outbox) {
        this.jdbc = jdbc;
        this.outbox = outbox;
    }

    /**
     * One transaction: order row, items, and two outbox rows. Either all commit or none.
     * Idempotency uses ON CONFLICT DO NOTHING rather than catching a unique-violation,
     * because a failed statement would poison the whole Postgres transaction.
     */
    @Transactional
    public Result create(CreateOrderRequest req, String idempotencyKey) {
        UUID id = UUID.randomUUID();

        int inserted = jdbc.sql("""
                INSERT INTO orders (id, customer_id, amount_cents, currency, status, idempotency_key)
                VALUES (:id, :customerId, :amount, :currency, 'PENDING', :key)
                ON CONFLICT (idempotency_key) DO NOTHING
                """)
                .param("id", id)
                .param("customerId", req.customerId())
                .param("amount", req.amountCents())
                .param("currency", req.currency())
                .param("key", idempotencyKey)
                .update();

        if (inserted == 0) {
            OrderView existing = findByKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("conflict without existing order"));
            return new Result(existing, false);
        }

        for (CreateOrderRequest.Item item : req.items()) {
            jdbc.sql("INSERT INTO order_items (order_id, sku, quantity) VALUES (:id, :sku, :qty)")
                    .param("id", id)
                    .param("sku", item.sku())
                    .param("qty", item.quantity())
                    .update();
        }

        List<Map<String, Object>> items = req.items().stream()
                .map(i -> Map.<String, Object>of("sku", i.sku(), "quantity", i.quantity()))
                .toList();

        String orderId = id.toString();
        outbox.append(Topics.INVENTORY_COMMANDS, orderId, "ReserveInventory",
                Map.of("orderId", orderId, "items", items));
        outbox.append(Topics.ORDER_EVENTS, orderId, "OrderCreated",
                Map.of("orderId", orderId, "customerId", req.customerId(),
                        "amountCents", req.amountCents(), "currency", req.currency()));

        return new Result(find(id).orElseThrow(), true);
    }

    @Transactional(readOnly = true)
    public Optional<OrderView> find(UUID id) {
        return jdbc.sql(SELECT_ORDER + " WHERE id = :id")
                .param("id", id)
                .query((rs, i) -> map(rs))
                .optional();
    }

    private Optional<OrderView> findByKey(String key) {
        return jdbc.sql(SELECT_ORDER + " WHERE idempotency_key = :key")
                .param("key", key)
                .query((rs, i) -> map(rs))
                .optional();
    }

    private static OrderView map(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new OrderView(
                rs.getObject("id", UUID.class),
                rs.getString("status"),
                rs.getString("customer_id"),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                rs.getObject("created_at", OffsetDateTime.class).toInstant());
    }
}
