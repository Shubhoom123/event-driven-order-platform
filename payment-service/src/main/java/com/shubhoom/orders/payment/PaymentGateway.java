package com.shubhoom.orders.payment;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

// Simulated gateway. Declines amounts above a limit so failure paths can be exercised.
@Component
public class PaymentGateway {

    private final long declineAboveCents;
    private final AtomicInteger calls = new AtomicInteger();

    PaymentGateway(@Value("${payment.decline-above-cents:100000}") long declineAboveCents) {
        this.declineAboveCents = declineAboveCents;
    }

    public boolean charge(long amountCents) {
        calls.incrementAndGet();
        return amountCents <= declineAboveCents;
    }

    public int calls() {
        return calls.get();
    }
}
