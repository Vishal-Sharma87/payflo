package com.vishal.payflo.dtos.responsedtos;

import com.vishal.payflo.enums.TransactionStatus;

import java.util.UUID;

public record PaymentInitiateResponseDto(UUID transactionId, String message) {}