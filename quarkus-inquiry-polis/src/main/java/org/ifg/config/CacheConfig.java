package org.ifg.config;

import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class CacheConfig {

    @ConfigProperty(name = "cache.ttlMinutes")
    public long ttlMinutes;

    @ConfigProperty(name = "cache.maxSize")
    public long maxSize;
}
