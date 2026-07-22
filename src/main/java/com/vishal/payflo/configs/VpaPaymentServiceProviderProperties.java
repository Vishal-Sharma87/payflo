package com.vishal.payflo.configs;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;


@ConfigurationProperties(prefix = "payflo.validation.vpa")
public record VpaPaymentServiceProviderProperties(
        List<String> paymentServiceProviders
) {
}
