package com.vishal.payflo.vallidators;

import com.vishal.payflo.validators.CardValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CardValidatorTest {


    private final CardValidator cardValidator = new CardValidator(null);

    @Test
    public void testCardDetails(){
        String invalidCardNumber = "4532011112830366";
        String validCardNumber = "4532015112830366";

        Assertions.assertFalse(cardValidator.isValidLuhn(invalidCardNumber));
        Assertions.assertTrue(cardValidator.isValidLuhn(validCardNumber));
    }

}
