package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "payflo.exception.message")
public record ExceptionMessagesProperties(
        String transactionNotFound,
        String vpaSeparatorCountMismatch,
        String vpaInvalidIdentifierFormat,
        String vpaInvalidPaymentServiceProviderFormat,
        String vpaUnknownPaymentServiceProvider
) {}