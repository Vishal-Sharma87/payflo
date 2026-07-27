package com.vishal.payflo.notifications;

import com.vishal.payflo.configs.NotificationHashKeyProperties;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import org.springframework.stereotype.Service;

@Service
public class NotificationHashKeyResolver {

    private final NotificationHashKeyProperties hashKeyProperties;

    public NotificationHashKeyResolver(NotificationHashKeyProperties hashKeyProperties){
        this.hashKeyProperties = hashKeyProperties;
    }

    public String resolve(KafkaTopic kafkaTopic){
        return switch (kafkaTopic){
            case PAYMENT_INITIATED_NOTIFICATION -> hashKeyProperties.paymentInitiatedNotification();
            case PAYMENT_COMPLETED_NOTIFICATION -> hashKeyProperties.paymentCompletedNotification();
            case PAYMENT_FAILED_NOTIFICATION -> hashKeyProperties.paymentFailedNotification();
            case PAYMENT_TIMED_OUT_NOTIFICATION -> hashKeyProperties.paymentTimedOutNotification();
            default -> throw new IllegalArgumentException("Unknown notification topic: " + kafkaTopic);
        };
    }
}
