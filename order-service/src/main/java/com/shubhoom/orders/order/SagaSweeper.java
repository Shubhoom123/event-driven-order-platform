package com.shubhoom.orders.order;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "saga.sweeper.enabled", havingValue = "true", matchIfMissing = true)
class SagaSweeper {

    private final OrderSaga saga;
    private final int timeoutSeconds;
    private final int maxAttempts;

    SagaSweeper(OrderSaga saga,
                @Value("${saga.step-timeout-seconds:30}") int timeoutSeconds,
                @Value("${saga.max-attempts:3}") int maxAttempts) {
        this.saga = saga;
        this.timeoutSeconds = timeoutSeconds;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(fixedDelayString = "${saga.sweep-ms:5000}")
    void run() {
        saga.sweep(timeoutSeconds, maxAttempts);
    }
}
