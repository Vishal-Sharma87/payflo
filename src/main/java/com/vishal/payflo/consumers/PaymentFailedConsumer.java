package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentFailedEvent;
import com.vishal.payflo.kafka.events.PaymentFailedNotificationEvent;
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



    public PaymentFailedConsumer(PaymentTransactionService paymentTransactionService,
                                 NotificationMessageTemplateBuilder notificationMessageTemplateBuilder,
                                 EventPublisher eventPublisher){
        this.paymentTransactionService = paymentTransactionService;
        this.notificationMessageTemplateBuilder = notificationMessageTemplateBuilder;
        this.eventPublisher = eventPublisher;
    }


    @KafkaListener(topics = "payflo.payment-failed", groupId="payflo-consumer-group")
    public void consumePaymentFailedEvent(PaymentFailedEvent paymentFailedEvent){
        UUID transactionId = paymentFailedEvent.transactionId();

        paymentTransactionService.markPaymentTransactionFailed(transactionId);

        log.info("Redis simulation for transactionId:{} event:{}", transactionId, "Payment-Failed");

        String message = notificationMessageTemplateBuilder.build(paymentFailedEvent.topic(), transactionId);
        PaymentEvent paymentEvent = new PaymentFailedNotificationEvent(transactionId, message);
        eventPublisher.publish(paymentEvent);

        log.info("Termination notification for transactionId:{}, event:{}", transactionId, "Payment-Failed");
    }

}
