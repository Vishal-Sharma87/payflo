package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentFailedEvent;
import com.vishal.payflo.kafka.events.PaymentFailedNotificationEvent;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class PaymentFailedConsumerTest {

    @Mock
    private PaymentTransactionService paymentTransactionService;

    @Mock
    private NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private RedisHashService redisHashService;

    @Mock
    private TransactionOwnershipService transactionOwnershipService;

    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final PaymentFailedEvent PAYMENT_FAILED_EVENT = new PaymentFailedEvent(TRANSACTION_ID);

    private PaymentFailedConsumer consumer;


    @BeforeEach
    public void setup(){
        consumer = new PaymentFailedConsumer(
                paymentTransactionService,
                notificationMessageTemplateBuilder,
                eventPublisher,
                redisHashService,
                transactionOwnershipService);
    }


    @Test
    public void testOwnershipClaimedCase(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.FAILED_PENDING
        )).thenReturn(true);

        Mockito.when(notificationMessageTemplateBuilder.build(
                PAYMENT_FAILED_EVENT.topic(),
                TRANSACTION_ID
        )).thenReturn("dummy");

        consumer.consumePaymentFailedEvent(PAYMENT_FAILED_EVENT);

        Mockito.verify(notificationMessageTemplateBuilder)
                .build(
                        PAYMENT_FAILED_EVENT.topic(),
                        TRANSACTION_ID
                );

        Mockito.verify(eventPublisher)
                .publish(new PaymentFailedNotificationEvent(TRANSACTION_ID, "dummy"));

        Mockito.verify(paymentTransactionService).markPaymentTransactionFailed(TRANSACTION_ID);

        Mockito.verify(redisHashService).finalizeStatus(TRANSACTION_ID, TransactionStatus.FAILED);
    }

    @Test
    public void testOwnershipClaimFailedCase(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.FAILED_PENDING
        )).thenReturn(false);

        consumer.consumePaymentFailedEvent(PAYMENT_FAILED_EVENT);

        Mockito.verify(notificationMessageTemplateBuilder, Mockito.never())
                .build(PAYMENT_FAILED_EVENT.topic(),
                        TRANSACTION_ID);

        Mockito.verify(eventPublisher, Mockito.never())
                .publish(new PaymentFailedNotificationEvent(TRANSACTION_ID, "dummy"));

        Mockito.verify(paymentTransactionService, Mockito.never())
                .markPaymentTransactionFailed(TRANSACTION_ID);

        Mockito.verify(redisHashService, Mockito.never())
                .finalizeStatus(TRANSACTION_ID, TransactionStatus.FAILED);
    }
}
