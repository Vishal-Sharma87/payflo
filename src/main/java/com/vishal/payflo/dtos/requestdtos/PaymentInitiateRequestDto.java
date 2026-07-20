package com.vishal.payflo.dtos.requestdtos;


import com.vishal.payflo.dtos.paymentdetails.PaymentDetails;

import java.math.BigDecimal;

public record PaymentInitiateRequestDto(BigDecimal amount, PaymentDetails paymentDetails) {
}