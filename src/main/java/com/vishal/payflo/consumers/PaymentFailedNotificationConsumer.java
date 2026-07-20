package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentFailedNotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentFailedNotificationConsumer {

    @KafkaListener(topics = "payflo.notification.payment-failed", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionFailedNotification(PaymentFailedNotificationEvent paymentFailedNotificationEvent){
        log.info(paymentFailedNotificationEvent.payload());

        log.info("Notification sent for transactionId:{}, event:{}", paymentFailedNotificationEvent.transactionId(), "Payment-Failed-Notification");
    }

}
