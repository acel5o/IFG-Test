package org.ifg.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaConfigService {

    @ConfigProperty(name = "kafka.bootstrap")
    String bootstrapServers;

    @ConfigProperty(name = "kafka.group.id")
    String groupId;

    @ConfigProperty(name = "kafka.key.deserializer")
    String keyDeserializer;

    @ConfigProperty(name = "kafka.value.deserializer")
    String valueDeserializer;

    @ConfigProperty(name = "kafka.enable.auto.commit")
    String enableAutoCommit;

    @ConfigProperty(name = "kafka.auto.offset.reset")
    String autoOffsetReset;

    @ConfigProperty(name = "kafka.key.serializer")
    String keySerializer;

    @ConfigProperty(name = "kafka.value.serializer")
    String valueSerializer;

    @ConfigProperty(name = "kafka.topic.in")
    String topicIn;

    @ConfigProperty(name = "kafka.topic.out")
    String topicOut;

    @ConfigProperty(name = "kafka.consumer.threads", defaultValue = "1")
    int consumerThreads;

    @ConfigProperty(name = "cache.ttl.minutes", defaultValue = "10")
    long cacheTtlMinutes;

    @ConfigProperty(name = "cache.max.size", defaultValue = "1000")
    long cacheMaxSize;

    public java.util.Properties getConsumerProps() {
        java.util.Properties consumerProps = new java.util.Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("group.id", groupId);
        consumerProps.put("key.deserializer", keyDeserializer);
        consumerProps.put("value.deserializer", valueDeserializer);
        consumerProps.put("enable.auto.commit", enableAutoCommit);
        consumerProps.put("auto.offset.reset", autoOffsetReset);
        return consumerProps;
    }

    public java.util.Properties getProducerProps() {
        java.util.Properties producerProps = new java.util.Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", keySerializer);
        producerProps.put("value.serializer", valueSerializer);
        return producerProps;
    }

    public String getTopicIn() {
        return topicIn;
    }

    public String getTopicOut() {
        return topicOut;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public long getCacheTtlMinutes() {
        return cacheTtlMinutes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }
}
