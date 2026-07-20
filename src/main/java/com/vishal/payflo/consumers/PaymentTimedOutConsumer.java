package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentTimedOutEvent;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentTimedOutConsumer {


    private final PaymentTransactionService paymentTransactionService;

    public PaymentTimedOutConsumer(PaymentTransactionService paymentTransactionService){
        this.paymentTransactionService = paymentTransactionService;
    }


    @KafkaListener(topics = "payflo.payment-timed-out", groupId = "payflo-consumer.group")
    public void consumePaymentTimedOutEvent(PaymentTimedOutEvent paymentTimedOutEvent){
        paymentTransactionService.markTransactionStatusTimedOut(paymentTimedOutEvent.transactionId());

//        TODO SIMULATE WITH ACTUAL REDIS
        log.info("REDIS simulation of payment timed-out for transactionId:{} is successful", paymentTimedOutEvent.transactionId());

//        TODO fire the termination notification (payment-timed-out) event
        log.info("Termination Notification for transactionId:{}, event:{} is successful", paymentTimedOutEvent.transactionId(), "Payment-TimedOut");
    }
}
