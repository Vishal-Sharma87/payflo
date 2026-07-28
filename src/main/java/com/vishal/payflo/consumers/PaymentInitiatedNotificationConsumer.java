package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentInitiatedNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentInitiatedNotificationConsumer {

    private final NotificationPublisher notificationPublisher;
    public PaymentInitiatedNotificationConsumer(NotificationPublisher notificationPublisher){
        this.notificationPublisher = notificationPublisher;
    }


    @KafkaListener(topics="payflo.notification.payment-initiated", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionInitiatedNotification(PaymentInitiatedNotificationEvent paymentInitiatedNotificationEvent){
        UUID transactionId = paymentInitiatedNotificationEvent.transactionId();
        KafkaTopic kafkaTopic = paymentInitiatedNotificationEvent.topic();
        String payload = paymentInitiatedNotificationEvent.payload();

        log.info("Received notification event for transactionId:{} topic:{}", transactionId, kafkaTopic);
        notificationPublisher.publish(transactionId, payload, kafkaTopic);
    }

}
