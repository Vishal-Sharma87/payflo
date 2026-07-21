package com.vishal.payflo.notifications;

import com.vishal.payflo.configs.NotificationMessageProperties;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import org.springframework.stereotype.Component;

@Component
public class NotificationMessageResolver {

    private final NotificationMessageProperties notificationMessageProperties;

    public NotificationMessageResolver(NotificationMessageProperties notificationMessageProperties){
        this.notificationMessageProperties = notificationMessageProperties;
    }

    public String resolve(KafkaTopic topic){
        return switch (topic){
            case PAYMENT_INITIATED -> notificationMessageProperties.paymentInitiatedNotification();
            case PAYMENT_RECEIVED -> notificationMessageProperties.paymentCompletedNotification();
            case PAYMENT_FAILED -> notificationMessageProperties.paymentFailedNotification();
            case PAYMENT_TIMED_OUT -> notificationMessageProperties.paymentTimedOutNotification();
            default -> throw new RuntimeException("Invalid Topic");
        };
    }

}
