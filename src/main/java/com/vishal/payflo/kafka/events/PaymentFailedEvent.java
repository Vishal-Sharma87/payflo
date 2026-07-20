package com.vishal.payflo.kafka.events;

import com.vishal.payflo.kafka.topics.KafkaTopic;

import java.util.UUID;

public record PaymentFailedEvent(UUID transactionId) implements PaymentEvent{
    @Override
    public String key() {
        return transactionId.toString();
    }

    @Override
    public KafkaTopic topic() {
        return KafkaTopic.PAYMENT_FAILED;
    }
}
