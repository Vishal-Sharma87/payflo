package com.vishal.payflo.consumers;

import com.vishal.payflo.cache.service.TransactionInitializationService;
import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.enums.PaymentType;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentInitiatedEvent;
import com.vishal.payflo.kafka.events.PaymentInitiatedNotificationEvent;
import com.vishal.payflo.notifications.NotificationMessageTemplateBuilder;
import com.vishal.payflo.services.PaymentTransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class PaymentInitiatedConsumerTest {
    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final BigDecimal AMOUNT = BigDecimal.ONE;
    private static final PaymentType PAYMENT_TYPE = PaymentType.UPI;
    private static final Instant STARTED_AT = Instant.now();
    private static final PaymentInitiatedEvent PAYMENT_INITIATED_EVENT = new PaymentInitiatedEvent(TRANSACTION_ID,
            AMOUNT,
            PAYMENT_TYPE,
            STARTED_AT
    );


    private static final String NOTIFICATION_MESSAGE = "dummyMessage";

    @Mock
    private PaymentTransactionService paymentTransactionService;

    @Mock
    private NotificationMessageTemplateBuilder notificationMessageTemplateBuilder;

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private TransactionInitializationService transactionInitializationService;

    private PaymentInitiatedConsumer consumer;


    @BeforeEach
    public void setup(){
        consumer = new PaymentInitiatedConsumer(
                paymentTransactionService,
                notificationMessageTemplateBuilder,
                eventPublisher,
                transactionInitializationService
        );
    }


    @Test
    public void testTransactionPersistedByPaymentInitiatedEvent(){
        Mockito.when(notificationMessageTemplateBuilder.build(PAYMENT_INITIATED_EVENT.topic(), TRANSACTION_ID))
                        .thenReturn(NOTIFICATION_MESSAGE);

        consumer.initiateTransaction(PAYMENT_INITIATED_EVENT);

        Mockito.verify(paymentTransactionService).persistNewTransaction(
                Mockito.any(PaymentTransaction.class)
        );

        Mockito.verify(transactionInitializationService)
                .initialize(TRANSACTION_ID, STARTED_AT);

        Mockito.verify(notificationMessageTemplateBuilder)
                .build(PAYMENT_INITIATED_EVENT.topic(),TRANSACTION_ID);

        Mockito.verify(eventPublisher)
                .publish(new PaymentInitiatedNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));
    }

    @Test
    public void testTransactionNotPersistedByPaymentInitiatedEvent(){
        Mockito.when(notificationMessageTemplateBuilder.build(
                PAYMENT_INITIATED_EVENT.topic(),
                        TRANSACTION_ID))
                .thenReturn(NOTIFICATION_MESSAGE);

        Mockito.doThrow(new DataIntegrityViolationException("Duplicated"))
                .when(paymentTransactionService)
                .persistNewTransaction(Mockito.any(PaymentTransaction.class));

        consumer.initiateTransaction(PAYMENT_INITIATED_EVENT);

        Mockito.verify(transactionInitializationService)
                .initialize(TRANSACTION_ID, STARTED_AT);

        Mockito.verify(notificationMessageTemplateBuilder)
                .build(PAYMENT_INITIATED_EVENT.topic(),TRANSACTION_ID);

        Mockito.verify(eventPublisher)
                .publish(new PaymentInitiatedNotificationEvent(TRANSACTION_ID, NOTIFICATION_MESSAGE));
    }

}
