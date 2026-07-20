package com.vishal.payflo.dtos.requestdtos;

import com.vishal.payflo.enums.GatewayStatus;

import java.util.UUID;

public record PaymentConfirmRequestDto(UUID transactionId, GatewayStatus status) {}