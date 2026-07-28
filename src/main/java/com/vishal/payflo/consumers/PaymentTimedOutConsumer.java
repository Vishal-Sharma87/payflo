package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
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
    private final RedisHashService redisHashService;
    private final TransactionOwnershipService transactionOwnershipService;


    public PaymentTimedOutConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher,
                                   RedisHashService redisHashService,
                                   TransactionOwnershipService transactionOwnershipService){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.redisHashService  = redisHashService;
        this.transactionOwnershipService = transactionOwnershipService;
    }


    @KafkaListener(topics = "payflo.payment-timed-out", groupId = "payflo-consumer.group")
    public void consumePaymentTimedOutEvent(PaymentTimedOutEvent paymentTimedOutEvent){
        UUID transactionId = paymentTimedOutEvent.transactionId();
        KafkaTopic topic = paymentTimedOutEvent.topic();
        log.info("Received payment event for transactionId:{} topic:{}", transactionId, topic);

        if (transactionOwnershipService.tryClaim(transactionId, TransactionStatus.TIMED_OUT_PENDING)){
            log.info("Processing transactionId:{}", transactionId);
            paymentTransactionService.markTransactionStatusTimedOut(transactionId);

            String message = notificationMessageTemplateBuilder.build(topic, transactionId);
            PaymentEvent paymentEvent = new PaymentTimedOutNotificationEvent(transactionId, message);
            eventPublisher.publish(paymentEvent);

            redisHashService.finalizeStatus(transactionId, TransactionStatus.TIMED_OUT);
            log.info("Transaction finalized as TIMED_OUT for transactionId:{}", transactionId);
        } else {
            log.warn("Skipping transactionId:{} because ownership was not claimed", transactionId);
        }

    }
}
