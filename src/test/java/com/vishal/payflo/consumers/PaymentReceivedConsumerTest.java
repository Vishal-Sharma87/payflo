package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.cache.service.TransactionOwnershipService;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentCompletedNotificationEvent;
import com.vishal.payflo.kafka.events.PaymentReceivedEvent;
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
public class PaymentReceivedConsumerTest {
    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final PaymentReceivedEvent PAYMENT_RECEIVED_EVENT = new PaymentReceivedEvent(TRANSACTION_ID);
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

    private PaymentReceivedConsumer consumer;

    @BeforeEach
    public void setup(){
        consumer = new PaymentReceivedConsumer(
                paymentTransactionService,
                notificationMessageTemplateBuilder,
                eventPublisher,
                transactionOwnershipService,
                redisHashService
        );
    }

    @Test
    public void testOwnershipClaimedByPaymentReceivedConsumer(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.COMPLETED_PENDING
        )).thenReturn(true);

        Mockito.when(notificationMessageTemplateBuilder.build(
                PAYMENT_RECEIVED_EVENT.topic(),
                TRANSACTION_ID
        )).thenReturn(NOTIFICATION_MESSAGE);

        consumer.completeTransaction(PAYMENT_RECEIVED_EVENT);

        Mockito.verify(paymentTransactionService)
                .markTransactionStatusCompleted(TRANSACTION_ID);

        Mockito.verify(notificationMessageTemplateBuilder)
                .build(PAYMENT_RECEIVED_EVENT.topic(), TRANSACTION_ID);

        Mockito.verify(eventPublisher)
                .publish(new PaymentCompletedNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));

        Mockito.verify(redisHashService)
                .finalizeStatus(TRANSACTION_ID, TransactionStatus.COMPLETED);
    }

    @Test
    public void testOwnershipNotClaimedByPaymentReceivedConsumer(){
        Mockito.when(transactionOwnershipService.tryClaim(
                TRANSACTION_ID,
                TransactionStatus.COMPLETED_PENDING
        )).thenReturn(false);

        consumer.completeTransaction(PAYMENT_RECEIVED_EVENT);

        Mockito.verify(paymentTransactionService, Mockito.never())
                .markTransactionStatusCompleted(TRANSACTION_ID);

        Mockito.verify(notificationMessageTemplateBuilder, Mockito.never())
                .build(PAYMENT_RECEIVED_EVENT.topic(), TRANSACTION_ID);

        Mockito.verify(eventPublisher, Mockito.never()).
                publish(new PaymentCompletedNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));

        Mockito.verify(redisHashService, Mockito.never()).
                finalizeStatus(TRANSACTION_ID, TransactionStatus.COMPLETED);
    }
}
