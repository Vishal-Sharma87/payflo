package com.vishal.payflo.notifications;

import com.vishal.payflo.kafka.topics.KafkaTopic;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class NotificationMessageTemplateBuilder {

    private final NotificationMessageResolver notificationMessageResolver;

    public NotificationMessageTemplateBuilder(NotificationMessageResolver notificationMessageResolver){
        this.notificationMessageResolver = notificationMessageResolver;
    }


    public String build(KafkaTopic topic, UUID transactionId){
        String template = notificationMessageResolver.resolve(topic);
        return template.formatted(transactionId);
    }
}
