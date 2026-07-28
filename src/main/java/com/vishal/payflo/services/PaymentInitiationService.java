package com.vishal.payflo.services;

import com.github.f4b6a3.uuid.UuidCreator;
import com.vishal.payflo.dtos.requestdtos.PaymentInitiateRequestDto;
import com.vishal.payflo.dtos.responsedtos.PaymentInitiateResponseDto;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentInitiatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class PaymentInitiationService {

    private final PaymentDetailsValidatorService paymentDetailsValidatorService;
    private final EventPublisher eventPublisher;

    public PaymentInitiationService(PaymentDetailsValidatorService paymentDetailsValidatorService, EventPublisher eventPublisher){
        this.paymentDetailsValidatorService = paymentDetailsValidatorService;
        this.eventPublisher = eventPublisher;
    }

    public PaymentInitiateResponseDto initiate(PaymentInitiateRequestDto requestDto) {
        log.info("Starting payment initiation for payment type:{}", requestDto.paymentDetails().type());
        paymentDetailsValidatorService.validate(requestDto.paymentDetails());

        UUID transactionId = UuidCreator.getTimeOrderedEpoch();

        log.info("Publishing payment initiation event for transactionId:{}", transactionId);
        eventPublisher.publish(new PaymentInitiatedEvent(
                transactionId,
                requestDto.amount(),
                requestDto.paymentDetails().type(),
                Instant.now()));

        log.info("Payment initiation accepted for transactionId:{}", transactionId);
        return new PaymentInitiateResponseDto(transactionId, "Payment initiation accepted. Awaiting completion.");
    }
}
