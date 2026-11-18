package com.shubhoom.orders.common;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

// Blocking retries on the same partition (keeps per-order ordering, which the saga relies on),
// then the record is parked in "<topic>.dlq" so the partition keeps moving.
@Configuration
class KafkaErrorConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka, MeterRegistry metrics,
                                          @Value("${kafka.consumer.retry-attempts:3}") long attempts,
                                          @Value("${kafka.consumer.retry-backoff-ms:1000}") long backoffMs) {
        DeadLetterPublishingRecoverer toDlq = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> {
                    metrics.counter("kafka.dlq", "topic", record.topic()).increment();
                    return new TopicPartition(record.topic() + ".dlq", -1);
                });
        return new DefaultErrorHandler(toDlq, new FixedBackOff(backoffMs, attempts));
    }
}
