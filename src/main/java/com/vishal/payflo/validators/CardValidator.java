package com.vishal.payflo.validators;

import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.InvalidCardDetailsException;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.dtos.paymentdetails.CardDetails;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.YearMonth;
import java.util.regex.Pattern;

@Component
@Slf4j
public class CardValidator implements PaymentValidator<CardDetails> {

    private static final Pattern cardNumberPattern = Pattern.compile("^[0-9]{15,16}$");
    private static final Pattern cvvPattern = Pattern.compile("^[0-9]{3,4}$");

    private final ExceptionMessagesProperties exceptionMessages;

    public CardValidator(ExceptionMessagesProperties exceptionMessages){
        this.exceptionMessages = exceptionMessages;
    }

    @Override
    public void validate(CardDetails cardDetails){
        String cardNumber =  cardDetails.cardNumber();
        String cvv = cardDetails.cvv();
        YearMonth expiry = cardDetails.expiry();

        if (!cardNumberPattern.matcher(cardNumber).matches()){
            throw new InvalidCardDetailsException(exceptionMessages.invalidCardNumberFormat(), ErrorCode.INVALID_CARD_NUMBER_FORMAT);
        }

        if(!cvvPattern.matcher(cvv).matches()){
            throw new InvalidCardDetailsException(exceptionMessages.invalidCvvFormat(), ErrorCode.INVALID_CVV_FORMAT);
        }

        if(YearMonth.now().isAfter(expiry)){
            throw new InvalidCardDetailsException(exceptionMessages.expiredCard(), ErrorCode.EXPIRED_CARD);
        }

        if(!isValidLuhn(cardNumber)){
            throw new InvalidCardDetailsException(exceptionMessages.invalidCardNumber(), ErrorCode.INVALID_CARD_NUMBER);
        }
    }

    public boolean isValidLuhn(String cardNumber) {
        int counter = 0;
        int digitSum = 0;
        int it = cardNumber.length() - 1;
        while(it >= 0){
            int digit = cardNumber.charAt(it) - '0';
            if((counter & 1) == 1){
                digit *= 2;
                if (digit > 9) digit -= 9;
            }

            digitSum += digit;
            counter++;
            it--;
        }

        return digitSum % 10 == 0;
    }

}
