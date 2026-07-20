package com.vishal.payflo.dtos.paymentdetails;

import com.vishal.payflo.enums.PaymentType;

public record UpiDetails(String vpa) implements PaymentDetails {
    @Override
    public PaymentType type() {
        return PaymentType.UPI;
    }
}
