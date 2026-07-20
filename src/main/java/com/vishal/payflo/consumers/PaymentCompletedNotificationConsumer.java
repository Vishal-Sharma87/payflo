package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentCompletedNotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentCompletedNotificationConsumer {

    @KafkaListener(topics="payflo.notification.payment-completed", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionCompletedNotification(PaymentCompletedNotificationEvent paymentCompletedNotificationEvent){
        log.info(paymentCompletedNotificationEvent.payload());

        log.info("Notification sent for transactionId:{}, event:{}", paymentCompletedNotificationEvent.transactionId(), "Payment-Completed-Notification");
    }
}
