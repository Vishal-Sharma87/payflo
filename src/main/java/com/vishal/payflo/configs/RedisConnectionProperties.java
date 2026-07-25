package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.redis")
public record RedisConnectionProperties(
        String host,
        int port
) {
}
