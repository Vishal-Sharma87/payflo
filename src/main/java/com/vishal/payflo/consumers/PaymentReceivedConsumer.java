package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentCompletedNotificationEvent;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentReceivedEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
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
    private final TransactionOwnershipService transactionOwnershipService;
    private final RedisHashService hashService;

    public PaymentReceivedConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher,
                                   TransactionOwnershipService transactionOwnershipService,
                                   RedisHashService hashService){

        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.transactionOwnershipService = transactionOwnershipService;
        this.hashService = hashService;
    }


    @KafkaListener(
            topics = "${payflo.kafka.topics.payment-received}",
            groupId = "${payflo.kafka.groups.payment-received}"
    )
    public void completeTransaction(PaymentReceivedEvent paymentReceivedEvent) {
        UUID transactionId = paymentReceivedEvent.transactionId();
        KafkaTopic kafkaTopic = paymentReceivedEvent.topic();
        log.info("Received payment event for transactionId:{} topic:{}", transactionId, kafkaTopic);

        if(transactionOwnershipService.tryClaim(transactionId, TransactionStatus.COMPLETED_PENDING)){
            log.info("Processing transactionId:{}", transactionId);
            paymentTransactionService.markTransactionStatusCompleted(transactionId);

            String message = notificationMessageTemplateBuilder.build(kafkaTopic, transactionId);
            PaymentEvent paymentEvent = new PaymentCompletedNotificationEvent(transactionId, message);
            eventPublisher.publish(paymentEvent);

            hashService.finalizeStatus(transactionId, TransactionStatus.COMPLETED);
            log.info("Transaction finalized as COMPLETED for transactionId:{}", transactionId);
        } else {
            log.warn("Skipping transactionId:{} because ownership was not claimed", transactionId);
        }
    }

}
