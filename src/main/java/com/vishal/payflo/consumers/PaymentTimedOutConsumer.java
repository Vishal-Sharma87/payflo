package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutNotificationEvent;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentTimedOutConsumer {


    private final PaymentTransactionService paymentTransactionService;
    private final NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;
    private final EventPublisher eventPublisher;

    public PaymentTimedOutConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
    }


    @KafkaListener(topics = "payflo.payment-timed-out", groupId = "payflo-consumer.group")
    public void consumePaymentTimedOutEvent(PaymentTimedOutEvent paymentTimedOutEvent){
        UUID transactionId = paymentTimedOutEvent.transactionId();

        paymentTransactionService.markTransactionStatusTimedOut(transactionId);

        log.info("REDIS simulation of payment timed-out for transactionId:{} is successful", transactionId);

        String message = notificationMessageTemplateBuilder.build(paymentTimedOutEvent.topic(), transactionId);
        PaymentEvent paymentEvent = new PaymentTimedOutNotificationEvent(transactionId, message);
        eventPublisher.publish(paymentEvent);

        log.info("Termination Notification for transactionId:{}, event:{} is successful", transactionId, "Payment-TimedOut");
    }
}
