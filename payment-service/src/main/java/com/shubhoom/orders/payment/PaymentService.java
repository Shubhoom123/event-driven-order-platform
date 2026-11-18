package com.shubhoom.orders.payment;

import com.shubhoom.orders.common.Inbox;
import com.shubhoom.orders.common.OutboxWriter;
import com.shubhoom.orders.common.Topics;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final String CONSUMER = "payment";

    private final JdbcClient jdbc;
    private final Inbox inbox;
    private final OutboxWriter outbox;
    private final PaymentGateway gateway;

    public PaymentService(JdbcClient jdbc, Inbox inbox, OutboxWriter outbox, PaymentGateway gateway) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.outbox = outbox;
        this.gateway = gateway;
    }

    // The payments row (PK order_id) is the "one charge per order" guard: a concurrent duplicate blocks on the
    // insert until the first transaction commits, then sees the row and only repeats the earlier answer.
    // The gateway call runs inside the transaction: fine for a simulator, a real gateway needs its own idempotency key.
    @Transactional
    public void charge(String eventId, String orderId, long amountCents, String currency) {
        if (!inbox.firstTime(CONSUMER, eventId)) {
            return;
        }
        UUID id = UUID.fromString(orderId);

        int inserted = jdbc.sql("""
                INSERT INTO payments (order_id, status, amount_cents, currency)
                VALUES (:id, 'PENDING', :amount, :currency)
                ON CONFLICT (order_id) DO NOTHING
                """)
                .param("id", id).param("amount", amountCents).param("currency", currency)
                .update();

        if (inserted == 0) {
            String status = jdbc.sql("SELECT status FROM payments WHERE order_id = :id")
                    .param("id", id).query(String.class).single();
            emit(orderId, status);
            return;
        }

        String status = gateway.charge(amountCents) ? "CHARGED" : "DECLINED";
        jdbc.sql("UPDATE payments SET status = :s WHERE order_id = :id")
                .param("s", status).param("id", id).update();
        emit(orderId, status);
    }

    private void emit(String orderId, String status) {
        String type = status.equals("CHARGED") ? "PaymentCharged" : "PaymentDeclined";
        outbox.append(Topics.PAYMENT_EVENTS, orderId, type, Map.of("orderId", orderId));
    }
}
