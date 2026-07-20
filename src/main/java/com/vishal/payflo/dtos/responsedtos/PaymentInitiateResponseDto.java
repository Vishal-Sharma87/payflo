package com.vishal.payflo.dtos.responsedtos;

import java.util.UUID;

public record PaymentInitiateResponseDto(UUID transactionId, String message) {}