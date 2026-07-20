package com.vishal.payflo.consumers;

import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.kafka.events.PaymentInitiatedEvent;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentInitiatedConsumer {

    private final PaymentTransactionService paymentTransactionService;

    public PaymentInitiatedConsumer(PaymentTransactionService paymentTransactionService){
        this.paymentTransactionService = paymentTransactionService;
    }


    @KafkaListener(topics = "payflo.payment-initiated", groupId = "payflo-consumer-group")
    public void initiatePayment(PaymentInitiatedEvent paymentInitiatedEvent) {
        UUID transactionId = paymentInitiatedEvent.transactionId();

        try {
            PaymentTransaction paymentTransaction = PaymentTransaction.from(paymentInitiatedEvent);
            paymentTransactionService.createNewTransaction(paymentTransaction);

//            TODO CREATE REDIS ENTRY
            log.info("Redis sorted sets insertion Simulation for transactionId:{} successful", transactionId);

//            TODO FIRE THE payflo.notifiaction.payment-initiated event
            log.info("Notification event simulation for transactionId: {} successful", transactionId);

        } catch (DataIntegrityViolationException e) {
            log.warn("PaymentTransaction with transactionId: {} already exists.", transactionId);
        }

    }
}
