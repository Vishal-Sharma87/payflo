package com.vishal.payflo.notifications;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class NotificationPublisher {

    private  final RedisHashService hashService;

    public NotificationPublisher(RedisHashService hashService){
        this.hashService = hashService;
    }


    public void publish(UUID transactionId, String payload, KafkaTopic kafkaTopic){
        if (!hashService.isNotificationSent(transactionId, kafkaTopic)){
            log.info(payload);

            log.info("Notification sent for transactionId:{}, KafkaTopic: {}", transactionId, kafkaTopic);
            hashService.notificationProcessed(transactionId, kafkaTopic);
        }
    }
}
