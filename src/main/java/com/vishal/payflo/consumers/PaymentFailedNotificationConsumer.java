package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentFailedNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentFailedNotificationConsumer {


    private final NotificationPublisher notificationPublisher;
    public PaymentFailedNotificationConsumer(NotificationPublisher notificationPublisher){
        this.notificationPublisher = notificationPublisher;
    }


    @KafkaListener(topics = "payflo.notification.payment-failed", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionFailedNotification(PaymentFailedNotificationEvent paymentFailedNotificationEvent){
        UUID transactionId = paymentFailedNotificationEvent.transactionId();
        KafkaTopic kafkaTopic = paymentFailedNotificationEvent.topic();
        String payload = paymentFailedNotificationEvent.payload();

        notificationPublisher.publish(transactionId, payload, kafkaTopic);

    }

}
