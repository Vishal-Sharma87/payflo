package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.notification.message-templates")
public record NotificationMessageProperties(
        String paymentInitiatedNotification,
        String paymentCompletedNotification,
        String paymentFailedNotification,
        String paymentTimedOutNotification
) {
}
