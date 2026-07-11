package com.vishal.payflo.events;

import com.vishal.payflo.enums.PaymentType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentInitiatedEvent(UUID transactionId, BigDecimal amount, PaymentType paymentType, Instant startedAt) {
}
