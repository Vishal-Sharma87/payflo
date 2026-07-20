package com.vishal.payflo.validators;

import com.vishal.payflo.dtos.paymentdetails.PaymentDetails;

public interface PaymentValidator <T extends PaymentDetails> {
    void validate(T paymentDetails);
}
