package com.vishal.payflo.kafka.topics;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.kafka.topics")
public record KafkaTopicsProperties(
        String paymentInitiated,
        String notificationPaymentInitiated,
        String paymentReceived,
        String notificationPaymentCompleted,
        String paymentFailed,
        String notificationPaymentFailed,
        String paymentTimedOut,
        String notificationPaymentTimedOut,
        String dlt
) {}