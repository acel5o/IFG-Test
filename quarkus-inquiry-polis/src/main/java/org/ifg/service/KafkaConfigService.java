package org.ifg.service;

import org.ifg.config.KafkaConfig;
import org.ifg.config.CacheConfig;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Properties;

@ApplicationScoped
public class KafkaConfigService {

    @Inject
    KafkaConfig kafkaConfig;

    @Inject
    CacheConfig cacheConfig;

    public Properties getConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.bootstrap);
        props.put("group.id", kafkaConfig.groupId);
        props.put("key.deserializer", kafkaConfig.keyDeserializer);
        props.put("value.deserializer", kafkaConfig.valueDeserializer);
        props.put("enable.auto.commit", String.valueOf(kafkaConfig.enableAutoCommit));
        props.put("auto.offset.reset", kafkaConfig.autoOffsetReset);
        return props;
    }

    public Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.bootstrap);
        props.put("key.serializer", kafkaConfig.keySerializer);
        props.put("value.serializer", kafkaConfig.valueSerializer);
        return props;
    }

    public String getTopicIn() { return kafkaConfig.topicIn; }
    public String getTopicOut() { return kafkaConfig.topicOut; }
    public int getConsumerThreads() { return kafkaConfig.consumerThreads; }
    public long getCacheTtlMinutes() { return cacheConfig.ttlMinutes; }
    public long getCacheMaxSize() { return cacheConfig.maxSize; }
}
