package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.RedisZSetService;
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
    private final RedisHashService redisHashService;
    private final RedisZSetService redisZSetService;

    public PaymentReceivedConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher,
                                   RedisHashService redisHashService,
                                   RedisZSetService redisZSetService){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.redisHashService = redisHashService;
        this.redisZSetService = redisZSetService;
    }


    @KafkaListener(topics = "payflo.payment-received", groupId = "payflo-consumer.group")
    public void completeTransaction(PaymentReceivedEvent paymentReceivedEvent) {
        UUID transactionId = paymentReceivedEvent.transactionId();
        KafkaTopic topic = paymentReceivedEvent.topic();

        paymentTransactionService.markTransactionStatusCompleted(transactionId);

        redisHashService.finalizeStatus(transactionId, TransactionStatus.COMPLETED);
        redisZSetService.remove(transactionId);

        String message = notificationMessageTemplateBuilder.build(topic, transactionId);
        PaymentEvent paymentEvent = new PaymentCompletedNotificationEvent(transactionId, message);
        eventPublisher.publish(paymentEvent);
    }

}
