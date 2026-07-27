package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentFailedEvent;
import com.vishal.payflo.kafka.events.PaymentFailedNotificationEvent;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentFailedConsumer {

    public final PaymentTransactionService paymentTransactionService;
    private final NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;
    private final EventPublisher eventPublisher;
    private final RedisHashService redisHashService;
    private final TransactionOwnershipService transactionOwnershipService;



    public PaymentFailedConsumer(PaymentTransactionService paymentTransactionService,
                                 NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                 EventPublisher eventPublisher,
                                 RedisHashService redisHashService,
                                 TransactionOwnershipService transactionOwnershipServic){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
        this.redisHashService = redisHashService;
        this.transactionOwnershipService = transactionOwnershipServic;
    }


    @KafkaListener(topics = "payflo.payment-failed", groupId="payflo-consumer-group")
    public void consumePaymentFailedEvent(PaymentFailedEvent paymentFailedEvent){
        UUID transactionId = paymentFailedEvent.transactionId();
        KafkaTopic topic = paymentFailedEvent.topic();

        if(transactionOwnershipService.tryClaim(transactionId, TransactionStatus.TIMED_OUT_PENDING)){
            paymentTransactionService.markPaymentTransactionFailed(transactionId);

            String message = notificationMessageTemplateBuilder.build(topic, transactionId);
            PaymentEvent paymentEvent = new PaymentFailedNotificationEvent(transactionId, message);
            eventPublisher.publish(paymentEvent);

            redisHashService.finalizeStatus(transactionId, TransactionStatus.FAILED);
        }

    }

}
