package org.ifg.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.ifg.config.KafkaConfigService;
import org.ifg.service.KafkaProducerService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class PolicyProcessor {

    private final KafkaProducerService producer;
    private final Cache<String, String> cache;
    private final ObjectMapper mapper = new ObjectMapper();

    @Inject
    public PolicyProcessor(KafkaProducerService producer, KafkaConfigService configService) {
        this.producer = producer;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(configService.getCacheTtlMinutes(), TimeUnit.MINUTES)
                .maximumSize(configService.getCacheMaxSize())
                .build();
    }

    public void process(String key, String value) throws Exception {
        if (cache.getIfPresent(key) != null) {
            System.out.println("[Key-Stored] - Already Processed, key=" + key);
            return;
        }

        var node = mapper.readTree(value);
        ((com.fasterxml.jackson.databind.node.ObjectNode) node).put("status", "processed");

        String modified = mapper.writeValueAsString(node);
        producer.send(key, modified);
        cache.put(key, modified);
        System.out.println("[Processor] key=" + key + ", value=" + modified);
    }
}
