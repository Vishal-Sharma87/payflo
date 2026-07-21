package com.vishal.payflo.services;

import com.vishal.payflo.dtos.requestdtos.PaymentConfirmRequestDto;
import com.vishal.payflo.enums.GatewayStatus;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentFailedEvent;
import com.vishal.payflo.kafka.events.PaymentReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
public class PaymentGatewayService {
    private final EventPublisher eventPublisher;

    public PaymentGatewayService (EventPublisher eventPublisher){
        this.eventPublisher = eventPublisher;
    }

    public void completeTransaction(PaymentConfirmRequestDto paymentConfirmRequestDto) {
        UUID transactionId = paymentConfirmRequestDto.transactionId();
        GatewayStatus gatewayStatus = paymentConfirmRequestDto.status();
        log.info("PaymentGateway's final GatewayStatus: {} for transactionId: {}",gatewayStatus, transactionId);

        PaymentEvent paymentEvent = switch (gatewayStatus){
            case RECEIVED -> new PaymentReceivedEvent(transactionId);
            case FAILED -> new PaymentFailedEvent(transactionId);
        };

        eventPublisher.publish(paymentEvent);
        log.info("GatewayStatus: {} for transactionId: {}, event fired: {}",gatewayStatus, transactionId, paymentEvent.key());
    }
}
