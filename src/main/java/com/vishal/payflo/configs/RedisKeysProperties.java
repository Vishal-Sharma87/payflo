package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.redis.keys")
public record RedisKeysProperties(
        String paymentTransactionHashPrefix,
        String processingTransactionsZSetKey,
        String paymentTransactionHashStatusKey
) {

}