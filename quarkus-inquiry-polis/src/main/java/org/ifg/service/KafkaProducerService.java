package org.ifg.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Properties;

@ApplicationScoped
public class KafkaProducerService {

    private final Producer<String, String> producer;
    private final KafkaConfigService configService;

    @Inject
    public KafkaProducerService(KafkaConfigService configService) {
        this.configService = configService;
        Properties props = configService.getProducerProps();
        producer = new KafkaProducer<>(props);
    }

    public void send(String key, String value) {
        producer.send(new ProducerRecord<>(configService.getTopicOut(), key, value), (meta, ex) -> {
            if (ex != null) System.err.println("Produce error: " + ex.getMessage());
        });
    }
}
