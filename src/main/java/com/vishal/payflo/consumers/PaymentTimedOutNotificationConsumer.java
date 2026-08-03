package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentTimedOutNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentTimedOutNotificationConsumer {


    private final NotificationPublisher notificationPublisher;

    public PaymentTimedOutNotificationConsumer(
            NotificationPublisher notificationPublisher
    ){
        this.notificationPublisher = notificationPublisher;
    }

    @KafkaListener(
            topics= "${payflo.kafka.topics.payment-timed-out}",
            groupId = "${payflo.kafka.groups.notification-payment-timed-out}"
    )
    public void sendPaymentTransactionTimedOutNotification(PaymentTimedOutNotificationEvent paymentTimedOutNotificationEvent){
        UUID transactionId = paymentTimedOutNotificationEvent.transactionId();
        KafkaTopic kafkaTopic = paymentTimedOutNotificationEvent.topic();
        String payload = paymentTimedOutNotificationEvent.payload();

        log.info("Received notification event for transactionId:{} topic:{}", transactionId, kafkaTopic);
        notificationPublisher.publish(transactionId, payload, kafkaTopic);
    }

}
