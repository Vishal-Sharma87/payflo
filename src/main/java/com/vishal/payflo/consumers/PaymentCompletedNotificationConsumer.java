package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentCompletedNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentCompletedNotificationConsumer {

    private final NotificationPublisher notificationPublisher;

    public PaymentCompletedNotificationConsumer(NotificationPublisher notificationPublisher){
        this.notificationPublisher = notificationPublisher;
    }


    @KafkaListener(topics= "payflo.notification.payment-completed", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionCompletedNotification(PaymentCompletedNotificationEvent paymentCompletedNotificationEvent){
        UUID transactionId = paymentCompletedNotificationEvent.transactionId();
        KafkaTopic kafkaTopic = paymentCompletedNotificationEvent.topic();
        String payload = paymentCompletedNotificationEvent.payload();

        log.info("Received notification event for transactionId:{} topic:{}", transactionId, kafkaTopic);
        notificationPublisher.publish(transactionId, payload, kafkaTopic);
    }
}
