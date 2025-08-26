package org.ifg.config;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class KafkaConfig {

    @ConfigProperty(name = "kafka.bootstrap")
    public String bootstrap;

    @ConfigProperty(name = "kafka.keyDeserializer")
    public String keyDeserializer;

    @ConfigProperty(name = "kafka.valueDeserializer")
    public String valueDeserializer;

    @ConfigProperty(name = "kafka.keySerializer")
    public String keySerializer;

    @ConfigProperty(name = "kafka.valueSerializer")
    public String valueSerializer;

    @ConfigProperty(name = "kafka.groupId")
    public String groupId;

    @ConfigProperty(name = "kafka.topicIn")
    public String topicIn;

    @ConfigProperty(name = "kafka.topicOut")
    public String topicOut;

    @ConfigProperty(name = "kafka.enableAutoCommit")
    public boolean enableAutoCommit;

    @ConfigProperty(name = "kafka.autoOffsetReset")
    public String autoOffsetReset;

    @ConfigProperty(name = "kafka.consumerThreads")
    public int consumerThreads;
}
