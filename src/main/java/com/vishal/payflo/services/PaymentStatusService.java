package com.vishal.payflo.services;

import com.vishal.payflo.advice.exceptions.PaymentTransactionNotFoundException;
import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.PaymentStatusMessagesProperties;
import com.vishal.payflo.dtos.responsedtos.PaymentStatusResponseDto;
import com.vishal.payflo.enums.TransactionStatus;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
public class PaymentStatusService {

    private final PaymentStatusMessagesProperties statusMessagesProperties;
    private final ExceptionMessagesProperties exceptionMessagesProperties;
    private final RedisHashService redisHashService;

    public PaymentStatusService(PaymentStatusMessagesProperties statusMessagesProperties,
                                ExceptionMessagesProperties exceptionMessagesProperties,
                                RedisHashService redisHashService){

        this.statusMessagesProperties = statusMessagesProperties;
        this.exceptionMessagesProperties = exceptionMessagesProperties;
        this.redisHashService = redisHashService;
    }

    public PaymentStatusResponseDto check(UUID transactionId){
        String hashedStatus = redisHashService.getStatus(transactionId);
        if (hashedStatus == null)
            throw new PaymentTransactionNotFoundException(exceptionMessagesProperties.transactionNotFound());

        TransactionStatus transactionStatus= TransactionStatus.valueOf(hashedStatus);

        String message = switch (transactionStatus){
            case PROCESSING -> statusMessagesProperties.processing();
            case FAILED -> statusMessagesProperties.failed();
            case TIMED_OUT -> statusMessagesProperties.timedOut();
            case COMPLETED -> statusMessagesProperties.completed();
        };

        return new PaymentStatusResponseDto(transactionId, transactionStatus, message);
    }
}
