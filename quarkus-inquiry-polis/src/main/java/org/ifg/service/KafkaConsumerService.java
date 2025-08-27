package org.ifg.service;

import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.ifg.config.KafkaConfigService;
import org.ifg.processor.PolicyProcessor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class KafkaConsumerService {

    @Inject
    KafkaConfigService configService;

    @Inject
    PolicyProcessor processor;

    private ExecutorService executor;

    void onStart(@Observes StartupEvent ev) {
        executor = Executors.newFixedThreadPool(configService.getConsumerThreads(), r -> {
            Thread t = new Thread(r);
            t.setName("KafkaConsumerThread-" + (Thread.activeCount()));
            return t;
        });

        for (int i = 0; i < configService.getConsumerThreads(); i++) {
            final int threadNumber = i + 1; // untuk log
            executor.submit(() -> consumeLoop(threadNumber));
        }
        System.out.println("[KafkaConsumerService] Consumer threads started.");
    }

    private void consumeLoop(int threadNumber) {
        Properties props = configService.getConsumerProps();
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        try (consumer) {
            consumer.subscribe(Collections.singletonList(configService.getTopicIn()));
            System.out.println("[KafkaConsumerService][Thread-" + threadNumber + "] Subscribed to topic: " + configService.getTopicIn());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    records.forEach(record -> {
                        System.out.println("[KafkaConsumerService][Thread-" + threadNumber + "] key=" + record.key() +
                                ", value=" + record.value() +
                                ", partition=" + record.partition() +
                                ", offset=" + record.offset());
                        offsetsToCommit.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );

                        try {
                            processor.process(record.key(), record.value());
                        } catch (Exception e) {
                            System.err.println("[Processor][Thread-" + threadNumber + "] Error: " + e.getMessage());
                        }
                    });

                    if (!offsetsToCommit.isEmpty()) {
                        consumer.commitSync(offsetsToCommit);
                        System.out.println("[KafkaConsumerService][Thread-" + threadNumber + "] Offsets committed: " + offsetsToCommit);
                    }
                }
            }
        }
    }
}
