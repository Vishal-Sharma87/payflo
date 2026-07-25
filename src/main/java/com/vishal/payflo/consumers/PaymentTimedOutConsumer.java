package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.RedisZSetService;
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
    private final RedisZSetService redisZSetService;


    public PaymentTimedOutConsumer(PaymentTransactionService paymentTransactionService,
                                   NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                   EventPublisher eventPublisher,
                                   RedisHashService redisHashService,
                                   RedisZSetService redisZSetService){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.redisHashService  = redisHashService;
        this.redisZSetService = redisZSetService;
    }


    @KafkaListener(topics = "payflo.payment-timed-out", groupId = "payflo-consumer.group")
    public void consumePaymentTimedOutEvent(PaymentTimedOutEvent paymentTimedOutEvent){
        UUID transactionId = paymentTimedOutEvent.transactionId();
        KafkaTopic topic = paymentTimedOutEvent.topic();

        paymentTransactionService.markTransactionStatusTimedOut(transactionId);

        redisHashService.finalizeStatus(transactionId, TransactionStatus.TIMED_OUT);
        redisZSetService.remove(transactionId);

        String message = notificationMessageTemplateBuilder.build(topic, transactionId);
        PaymentEvent paymentEvent = new PaymentTimedOutNotificationEvent(transactionId, message);
        eventPublisher.publish(paymentEvent);

    }
}
