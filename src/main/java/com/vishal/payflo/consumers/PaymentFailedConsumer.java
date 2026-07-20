package com.vishal.payflo.consumers;

import com.vishal.payflo.kafka.events.PaymentFailedEvent;
import com.vishal.payflo.services.PaymentTransactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentFailedConsumer {

    public final PaymentTransactionService paymentTransactionService;

    public PaymentFailedConsumer(PaymentTransactionService paymentTransactionService){
        this.paymentTransactionService = paymentTransactionService;
    }


    @KafkaListener(topics = "payflo.payment-failed", groupId="payflo-consumer-group")
    public void consumePaymentFailedEvent(PaymentFailedEvent paymentFailedEvent){
        paymentTransactionService.markPaymentTransactionFailed(paymentFailedEvent.transactionId());

        log.info("Redis simulation for transactionId:{} event:{}", paymentFailedEvent.transactionId(), "Payment-Failed");

        log.info("Termination notification for transactionId:{}, event:{}", paymentFailedEvent.transactionId(), "Payment-Failed");
    }

}
