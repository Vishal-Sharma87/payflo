package com.vishal.payflo.services;

import com.vishal.payflo.advice.exceptions.PaymentTransactionNotFoundException;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.PaymentStatusMessagesProperties;
import com.vishal.payflo.dtos.responsedtos.PaymentStatusResponseDto;
import com.vishal.payflo.enums.TransactionStatus;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
public class PaymentStatusService {

    private final PaymentTransactionService paymentTransactionService;
    private final PaymentStatusMessagesProperties statusMessagesProperties;
    private final ExceptionMessagesProperties exceptionMessagesProperties;

    public PaymentStatusService(PaymentTransactionService paymentTransactionService,
                                PaymentStatusMessagesProperties statusMessagesProperties,
                                ExceptionMessagesProperties exceptionMessagesProperties){
        this.paymentTransactionService = paymentTransactionService;
        this.statusMessagesProperties = statusMessagesProperties;
        this.exceptionMessagesProperties = exceptionMessagesProperties;
    }

    public PaymentStatusResponseDto check(UUID transactionId){

        TransactionStatus status = paymentTransactionService.findTransactionStatusById(transactionId)
                .orElseThrow(() -> new PaymentTransactionNotFoundException(exceptionMessagesProperties.transactionNotFound()));

        String message = switch (status){
            case PROCESSING -> statusMessagesProperties.processing();
            case FAILED -> statusMessagesProperties.failed();
            case TIMED_OUT -> statusMessagesProperties.timedOut();
            case COMPLETED -> statusMessagesProperties.completed();
        };

        return new PaymentStatusResponseDto(transactionId, status, message);
    }
}
