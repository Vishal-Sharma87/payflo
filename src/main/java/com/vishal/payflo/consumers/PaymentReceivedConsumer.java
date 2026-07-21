package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentReceivedEvent;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PaymentReceivedConsumer {

    private final PaymentTransactionService paymentTransactionService;

    public PaymentReceivedConsumer(PaymentTransactionService paymentTransactionService){
        this.paymentTransactionService = paymentTransactionService;
    }


    @KafkaListener(topics = "payflo.payment-received", groupId = "payflo-consumer.group")
    public void completeTransaction(PaymentReceivedEvent paymentReceivedEvent){
        paymentTransactionService.markTransactionStatusCompleted(paymentReceivedEvent.transactionId());

        log.info("REDIS simulation of payment received for transactionId:{} is successful", paymentReceivedEvent.transactionId());

        log.info("Termination Notification (payment-completed) for transactionId:{} is successful", paymentReceivedEvent.transactionId());
    }

}
