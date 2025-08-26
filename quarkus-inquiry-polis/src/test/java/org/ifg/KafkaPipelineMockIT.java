package org.ifg;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class KafkaPipelineMockIT {

    @Test
    void testConsumeProcessProduceWithMock() {
        // Mock input consumer
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(Collections.singletonList(new org.apache.kafka.common.TopicPartition("policy-in", 0)));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(new org.apache.kafka.common.TopicPartition("policy-in", 0), 0L));
        mockConsumer.addRecord(new ConsumerRecord<>("policy-in", 0, 0, "key1", "{\"policyId\":\"12345\"}"));

        // Mock output producer
        MockProducer<String, String> mockProducer = new MockProducer<>(true,
                org.apache.kafka.common.serialization.StringSerializer.class,
                org.apache.kafka.common.serialization.StringSerializer.class);

        // Proses: consume -> modify -> produce
        mockConsumer.poll(java.time.Duration.ofMillis(100)).forEach(record -> {
            String value = record.value();
            // manipulasi sederhana, tambah field processedAt
            String modifiedValue = value.substring(0, value.length() - 1) + ", \"processedAt\":\"2025-08-26T10:00:00\"}";
            // produce ke mockProducer
            mockProducer.send(new ProducerRecord<>("policy-out", record.key(), modifiedValue));
        });

        // Verifikasi hasil produce
        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, String> produced = mockProducer.history().get(0);
        assertEquals("policy-out", produced.topic());
        assertTrue(produced.value().contains("processedAt"));
        assertTrue(produced.value().contains("policyId"));
    }
}
