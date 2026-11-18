package com.shubhoom.orders.common;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// Re-publishes everything in "<topic>.dlq" to <topic>. Progress is a committed consumer group offset,
// so calling it again only replays messages parked since the last run.
@Component
public class DlqReplay {

    private final KafkaTemplate<String, String> kafka;
    private final String bootstrapServers;

    public DlqReplay(KafkaTemplate<String, String> kafka,
                     @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.kafka = kafka;
        this.bootstrapServers = bootstrapServers;
    }

    public int replay(String topic) throws Exception {
        String dlq = topic + ".dlq";
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("group.id", "dlq-replay-" + dlq);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(dlq));
            int emptyPolls = 0;
            while (emptyPolls < 3) {
                var records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : records) {
                    kafka.send(topic, r.key(), r.value()).get(10, TimeUnit.SECONDS);
                    replayed++;
                }
                consumer.commitSync();
            }
        }
        return replayed;
    }
}
