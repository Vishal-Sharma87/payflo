package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.payment")
public record PaymentTimeoutProperties(
        int timeoutBufferMinutes
) {
}