package com.vishal.payflo.dtos.paymentdetails;

import com.vishal.payflo.enums.PaymentType;

import java.time.YearMonth;

public record CardDetails(String cardNumber, YearMonth expiry, String cvv) implements PaymentDetails {
    @Override
    public PaymentType type() {
        return PaymentType.CARD;
    }
}
