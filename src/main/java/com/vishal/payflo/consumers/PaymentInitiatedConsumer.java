package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.RedisZSetService;
import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentInitiatedEvent;
import com.vishal.payflo.kafka.events.PaymentInitiatedNotificationEvent;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class PaymentInitiatedConsumer {

    private final PaymentTransactionService paymentTransactionService;
    private final NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;
    private final EventPublisher eventPublisher;
    private final RedisHashService redisHashService;
    private final RedisZSetService redisZSetService;

    public PaymentInitiatedConsumer(PaymentTransactionService paymentTransactionService,
                                    NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                    EventPublisher eventPublisher,
                                    RedisZSetService redisZSetService,
                                    RedisHashService redisHashService){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.redisHashService = redisHashService;
        this.redisZSetService = redisZSetService;
    }


    @KafkaListener(topics = "payflo.payment-initiated", groupId = "payflo-consumer-group")
    public void initiatePayment(PaymentInitiatedEvent paymentInitiatedEvent) {
        UUID transactionId = paymentInitiatedEvent.transactionId();
        Instant startedAt = paymentInitiatedEvent.startedAt();

        try {
            PaymentTransaction paymentTransaction = PaymentTransaction.from(paymentInitiatedEvent);
            paymentTransactionService.createNewTransaction(paymentTransaction);

            redisHashService.createStatus(transactionId);
            redisZSetService.createEntry(transactionId, startedAt);

            String message = notificationMessageTemplateBuilder.build(paymentInitiatedEvent.topic(), transactionId);
            PaymentEvent paymentEvent = new PaymentInitiatedNotificationEvent(transactionId, message);
            eventPublisher.publish(paymentEvent);
        } catch (DataIntegrityViolationException e) {
            log.warn("PaymentTransaction with transactionId: {} already exists.", transactionId);
        }

    }
}
