package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.payment-transaction.status-message")
public record PaymentStatusMessagesProperties(
        String processing,
        String completed,
        String failed,
        String timedOut
) {}