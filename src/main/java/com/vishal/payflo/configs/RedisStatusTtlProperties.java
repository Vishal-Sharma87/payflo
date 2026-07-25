package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.redis.ttl")
public record RedisStatusTtlProperties(
        int statusTtlHours
) {
}