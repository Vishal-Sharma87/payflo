package com.vishal.payflo.validators;

import com.vishal.payflo.dtos.paymentdetails.UpiDetails;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UpiValidator implements PaymentValidator<UpiDetails> {
    @Override
    public void validate(UpiDetails paymentDetails) {
        log.info("Upi validation for vpa: {}", paymentDetails.vpa());
    }
}
