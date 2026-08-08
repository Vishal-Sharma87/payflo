package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentTimedOutEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutNotificationEvent;
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
public class PaymentTimedOutConsumerTest {

    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final PaymentTimedOutEvent PAYMENT_TIMED_OUT_EVENT = new PaymentTimedOutEvent(TRANSACTION_ID);
    private static final String NOTIFICATION_MESSAGE = "dummyMessage";

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

    private PaymentTimedOutConsumer consumer;

    @BeforeEach
    public void setup(){
        consumer = new PaymentTimedOutConsumer(
                paymentTransactionService,
                notificationMessageTemplateBuilder,
                eventPublisher,
                redisHashService,
                transactionOwnershipService
        );
    }

    @Test
    public void testOwnershipClaimedByPaymentTimedOutConsumer(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.TIMED_OUT_PENDING
        )).thenReturn(true);

        Mockito.when(notificationMessageTemplateBuilder.build(
                PAYMENT_TIMED_OUT_EVENT.topic(),
                TRANSACTION_ID
        )).thenReturn(NOTIFICATION_MESSAGE);

        consumer.consumePaymentTimedOutEvent(PAYMENT_TIMED_OUT_EVENT);

        Mockito.verify(paymentTransactionService)
                .markTransactionStatusTimedOut(TRANSACTION_ID);

        Mockito.verify(notificationMessageTemplateBuilder)
                .build(PAYMENT_TIMED_OUT_EVENT.topic(), TRANSACTION_ID);

        Mockito.verify(eventPublisher)
                .publish(new PaymentTimedOutNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));

        Mockito.verify(redisHashService)
                .finalizeStatus(TRANSACTION_ID, TransactionStatus.TIMED_OUT);
    }

    @Test
    public void testOwnershipNotClaimedByPaymentTimedOutConsumer(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.TIMED_OUT_PENDING
        )).thenReturn(false);

        consumer.consumePaymentTimedOutEvent(PAYMENT_TIMED_OUT_EVENT);

        Mockito.verify(paymentTransactionService, Mockito.never())
                .markTransactionStatusTimedOut(TRANSACTION_ID);

        Mockito.verify(notificationMessageTemplateBuilder, Mockito.never())
                .build(PAYMENT_TIMED_OUT_EVENT.topic(), TRANSACTION_ID);

        Mockito.verify(eventPublisher, Mockito.never()).
                publish(new PaymentTimedOutNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));

        Mockito.verify(redisHashService, Mockito.never()).
                finalizeStatus(TRANSACTION_ID, TransactionStatus.TIMED_OUT);
    }
}

