package org.ifg.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class KafkaConsumerService {

    private final KafkaConfigService configService;
    private final PolicyProcessor processor;
    private final ExecutorService executor;

    @Inject
    public KafkaConsumerService(KafkaConfigService configService, PolicyProcessor processor) {
        this.configService = configService;
        this.processor = processor;
        this.executor = Executors.newFixedThreadPool(configService.getConsumerThreads());
    }

    public void run() {
        for (int i = 0; i < configService.getConsumerThreads(); i++) {
            executor.submit(this::consumeLoop);
        }
    }

    private void consumeLoop() {
        Consumer<String, String> consumer = new KafkaConsumer<>(configService.getConsumerProps());
        consumer.subscribe(Collections.singletonList(configService.getTopicIn()));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                records.forEach(record -> {
                    try {
                        processor.process(record.key(), record.value());
                        offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    } catch (Exception e) {
                        System.err.println("Processing error: " + e.getMessage());
                    }
                });

                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            }
        } catch (WakeupException e) {
            // shutdown
        } finally {
            consumer.close();
        }
    }
}
