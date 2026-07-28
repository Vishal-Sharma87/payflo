package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.TransactionInitializationService;
import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentInitiatedEvent;
import com.vishal.payflo.kafka.events.PaymentInitiatedNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentInitiatedConsumer {

    private final PaymentTransactionService paymentTransactionService;
    private final NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;
    private final EventPublisher eventPublisher;
    private final TransactionInitializationService transactionInitializationService;

    public PaymentInitiatedConsumer(PaymentTransactionService paymentTransactionService,
                                    NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                    EventPublisher eventPublisher,
                                    TransactionInitializationService transactionInitializationService){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.transactionInitializationService = transactionInitializationService;
    }

    @KafkaListener(topics = "payflo.payment-initiated", groupId = "payflo-consumer.group")
    public void initiateTransaction(PaymentInitiatedEvent paymentInitiatedEvent) {
        try {
            PaymentTransaction paymentTransaction = PaymentTransaction.from(paymentInitiatedEvent);
            paymentTransactionService.persistNewTransaction(paymentTransaction);
            finalizeInitiation(paymentInitiatedEvent);
        } catch (DuplicateKeyException e) {
            finalizeInitiation(paymentInitiatedEvent);
        }
    }

    private void finalizeInitiation(PaymentInitiatedEvent paymentInitiatedEvent) {
        UUID transactionId = paymentInitiatedEvent.transactionId();
        KafkaTopic kafkaTopic = paymentInitiatedEvent.topic();

        transactionInitializationService.initialize(transactionId, paymentInitiatedEvent.startedAt());

        String message = notificationMessageTemplateBuilder.build(kafkaTopic, transactionId);
        PaymentEvent notificationEvent = new PaymentInitiatedNotificationEvent(transactionId, message);
        eventPublisher.publish(notificationEvent);
    }
}
