package com.vishal.payflo.dtos.responsedtos;

import com.vishal.payflo.enums.TransactionStatus;

import java.util.UUID;

public record PaymentStatusResponseDto(UUID transactionId, TransactionStatus status, String message) {}