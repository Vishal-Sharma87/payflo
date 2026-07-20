package com.vishal.payflo.kafka.events;

import com.vishal.payflo.kafka.topics.KafkaTopic;

import java.util.UUID;

public record PaymentTimedOutNotificationEvent(UUID transactionId, String payload) implements PaymentEvent {
    @Override
    public String key() {
        return transactionId.toString();
    }

    @Override
    public KafkaTopic topic() {
        return KafkaTopic.PAYMENT_TIMED_OUT_NOTIFICATION;
    }
}
