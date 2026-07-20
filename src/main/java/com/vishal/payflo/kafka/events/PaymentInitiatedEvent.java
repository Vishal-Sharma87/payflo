package com.vishal.payflo.kafka.events;

import com.vishal.payflo.enums.PaymentType;
import com.vishal.payflo.kafka.topics.KafkaTopic;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentInitiatedEvent(UUID transactionId, BigDecimal amount, PaymentType paymentType, Instant startedAt) implements PaymentEvent{
    @Override
    public String key() {
        return transactionId.toString();
    }

    @Override
    public KafkaTopic topic() {
        return KafkaTopic.PAYMENT_INITIATED;
    }

}
