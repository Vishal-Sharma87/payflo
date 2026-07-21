package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentCompletedNotificationEvent;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentReceivedEvent;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class PaymentReceivedConsumer {

    private final PaymentTransactionService paymentTransactionService;
    private final NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;
    private final EventPublisher eventPublisher;

    public PaymentReceivedConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
    }


    @KafkaListener(topics = "payflo.payment-received", groupId = "payflo-consumer.group")
    public void completeTransaction(PaymentReceivedEvent paymentReceivedEvent){
        UUID transactionId = paymentReceivedEvent.transactionId();

        paymentTransactionService.markTransactionStatusCompleted(transactionId);

        log.info("REDIS simulation of payment received for transactionId:{} is successful", transactionId);

        String message = notificationMessageTemplateBuilder.build(paymentReceivedEvent.topic(), transactionId);
        PaymentEvent paymentEvent = new PaymentCompletedNotificationEvent(transactionId, message);
        eventPublisher.publish(paymentEvent);

        log.info("Termination Notification (payment-completed) for transactionId:{} is successful", transactionId);
    }

}
