package com.shubhoom.orders.order;

import java.time.Instant;
import java.util.UUID;

public record OrderView(
        UUID orderId, String status, String customerId,
        long amountCents, String currency, Instant createdAt) {}
