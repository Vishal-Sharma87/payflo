package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.redis.notification-hash-key")
public record NotificationHashKeyProperties(
        String paymentInitiatedNotification,
        String paymentCompletedNotification,
        String paymentFailedNotification,
        String paymentTimedOutNotification
) {
}
