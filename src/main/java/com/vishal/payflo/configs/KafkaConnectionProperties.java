package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.kafka.connection")
public record KafkaConnectionProperties(
        String bootstrapServers,
        String groupId,
        String trustedPackages
) {
}