package com.vishal.payflo.validators;

import com.vishal.payflo.dtos.paymentdetails.CardDetails;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CardValidator implements PaymentValidator<CardDetails> {

    @Override
    public void validate(CardDetails cardDetails){
        log.info("Card validation for  cardNumber: {}", cardDetails.cardNumber());
    }

}
