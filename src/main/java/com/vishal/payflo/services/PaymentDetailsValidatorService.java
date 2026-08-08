package com.vishal.payflo.services;

import com.vishal.payflo.dtos.paymentdetails.CardDetails;
import com.vishal.payflo.dtos.paymentdetails.PaymentDetails;
import com.vishal.payflo.dtos.paymentdetails.UpiDetails;
import com.vishal.payflo.enums.PaymentType;
import com.vishal.payflo.validators.CardValidator;
import com.vishal.payflo.validators.UpiValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.YearMonth;

@Service
@Slf4j
public class PaymentDetailsValidatorService {
    private final UpiValidator upiValidator;
    private final CardValidator cardValidator;

    public PaymentDetailsValidatorService(UpiValidator upiValidator,
                                          CardValidator cardValidator){
        this.cardValidator = cardValidator;
        this.upiValidator = upiValidator;
    }

    public void validate(PaymentDetails paymentDetails){
        log.info("Validating payment details of type:{}", paymentDetails.type());
        switch (paymentDetails){
            case UpiDetails(String vpa) -> {
                upiValidator.validate(new UpiDetails(vpa));
                logValidationPassed(paymentDetails.type());
            }
            case CardDetails(String cardNumber, YearMonth expiry, String cvv) -> {
                cardValidator.validate(new CardDetails(cardNumber, expiry, cvv));
                logValidationPassed(paymentDetails.type());
            }
        }
    }

    private void logValidationPassed(PaymentType type) {
        log.info("Validation passed for payment details type:{}", type);
    }
}
