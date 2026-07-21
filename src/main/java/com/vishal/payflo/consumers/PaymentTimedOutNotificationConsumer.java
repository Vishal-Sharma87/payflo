package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentTimedOutNotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentTimedOutNotificationConsumer {

    @KafkaListener(topics="payflo.notification.payment-timed-out", groupId = "payflo-consumer-group")
    public void sendPaymentTransactionTimedOutNotification(PaymentTimedOutNotificationEvent paymentTimedOutNotificationEvent){
        log.info(paymentTimedOutNotificationEvent.payload());

        log.info("Notification sent for transactionId:{}, event:{}", paymentTimedOutNotificationEvent.transactionId(), paymentTimedOutNotificationEvent.topic());
    }

}
