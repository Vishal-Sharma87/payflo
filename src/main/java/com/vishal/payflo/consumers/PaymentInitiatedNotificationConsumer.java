package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentInitiatedNotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentInitiatedNotificationConsumer {


    @KafkaListener(topics="payflo.notification.payment-initiated", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionInitiatedNotification(PaymentInitiatedNotificationEvent paymentInitiatedNotificationEvent){
        log.info(paymentInitiatedNotificationEvent.payload());

        log.info("Notification sent for transactionId:{}, event:{}", paymentInitiatedNotificationEvent.transactionId(), paymentInitiatedNotificationEvent.topic());
    }

}
