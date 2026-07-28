package com.vishal.payflo.services;

import com.vishal.payflo.advice.exceptions.PaymentTransactionNotFoundException;
import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.PaymentStatusMessagesProperties;
import com.vishal.payflo.dtos.responsedtos.PaymentStatusResponseDto;
import com.vishal.payflo.enums.TransactionStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
@Slf4j
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
        log.info("Checking payment status for transactionId:{}", transactionId);
        String hashedStatus = redisHashService.getStatus(transactionId);
        if (hashedStatus == null)
            throw new PaymentTransactionNotFoundException(exceptionMessagesProperties.transactionNotFound());

        TransactionStatus transactionStatus= TransactionStatus.valueOf(hashedStatus);

        PaymentStatusResponseDto response = switch (transactionStatus){
            case PROCESSING, TIMED_OUT_PENDING, COMPLETED_PENDING, FAILED_PENDING -> new PaymentStatusResponseDto(transactionId, TransactionStatus.PROCESSING, statusMessagesProperties.processing());
            case FAILED -> new PaymentStatusResponseDto(transactionId, transactionStatus, statusMessagesProperties.failed());
            case TIMED_OUT -> new PaymentStatusResponseDto(transactionId, transactionStatus, statusMessagesProperties.timedOut());
            case COMPLETED -> new PaymentStatusResponseDto(transactionId, transactionStatus, statusMessagesProperties.completed());
        };
        log.info("Resolved payment status {} for transactionId:{}", response.status(), transactionId);
        return response;
    }
}
