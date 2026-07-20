package com.vishal.payflo.kafka.events;

import com.vishal.payflo.kafka.topics.KafkaTopic;

public interface PaymentEvent {
    String key();
    KafkaTopic topic();
}
