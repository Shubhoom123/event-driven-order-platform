package com.shubhoom.orders.common;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.RecordInterceptor;

// Puts the trace id carried in the Kafka header into the logging context for the duration of the record.
@Configuration
class KafkaTraceInterceptor {

    @Bean
    RecordInterceptor<Object, Object> traceRecordInterceptor() {
        return new RecordInterceptor<>() {
            @Override
            public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record,
                                                            Consumer<Object, Object> consumer) {
                Header h = record.headers().lastHeader(Trace.KEY);
                if (h != null) {
                    MDC.put(Trace.KEY, new String(h.value(), StandardCharsets.UTF_8));
                }
                return record;
            }

            @Override
            public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
                MDC.remove(Trace.KEY);
            }
        };
    }
}
