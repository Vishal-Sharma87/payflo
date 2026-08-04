package com.vishal.payflo.validators;

import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.InvalidCardDetailsException;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.dtos.paymentdetails.CardDetails;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.YearMonth;
import java.util.List;
import java.util.stream.Stream;

public class CardValidatorTest {
    private static final String transactionNotFound = "transactionNotFound";
    private static final String vpaSeparatorCountMismatch = "vpaSeparatorCountMismatch";
    private static final String vpaInvalidIdentifierFormat = "vpaInvalidIdentifierFormat";
    private static final String vpaInvalidPaymentServiceProviderFormat = "vpaInvalidPaymentServiceProviderFormat";
    private static final String vpaUnknownPaymentServiceProvider = "vpaUnknownPaymentServiceProvider";
    private static final String invalidCardNumberFormat = "invalidCardNumberFormat";
    private static final String invalidCardNumber = "invalidCardNumber";
    private static final String expiredCard = "expiredCard";
    private static final String invalidCvvFormat = "invalidCvvFormat";
    private static final String malformedRequestBody = "malformedRequestBody";

    private static final YearMonth NOT_EXPIRED_CARD = YearMonth.now();
    private static final YearMonth EXPIRED_CARD = YearMonth.now().minusMonths(1);
    private static final String VALID_CVV = "123";
    private static final String VALID_CARD_NUMBER = "4111111111111111";

    private final ExceptionMessagesProperties exceptionMessagesProperties =
            new ExceptionMessagesProperties(
                    transactionNotFound,
                    vpaSeparatorCountMismatch,
                    vpaInvalidIdentifierFormat,
                    vpaInvalidPaymentServiceProviderFormat,
                    vpaUnknownPaymentServiceProvider,
                    invalidCardNumberFormat,
                    invalidCardNumber,
                    expiredCard,
                    invalidCvvFormat,
                    malformedRequestBody
            );

    private final CardValidator cardValidator = new CardValidator(exceptionMessagesProperties);



    @ParameterizedTest
    @MethodSource("validCardDetailsStream")
    public void testValidCardDetails(CardDetails validCardDetails){
        Assertions.assertDoesNotThrow(() -> cardValidator.validate(validCardDetails));
    }

    @ParameterizedTest
    @MethodSource("invalidCardDetailsStream")
    public void testInvalidCardDetails(CardDetails invalidCardDetails, ErrorCode expectedErrorCode){
        InvalidCardDetailsException exception = Assertions.assertThrows(InvalidCardDetailsException.class, () -> cardValidator.validate(invalidCardDetails));

        Assertions.assertEquals(expectedErrorCode, exception.getErrorCode());
    }

    public static Stream<Arguments> validCardDetailsStream(){
        return validLuhnCardNumbers()
                .stream()
                .map(number ->
                        Arguments.of(
                                new CardDetails(number, NOT_EXPIRED_CARD, VALID_CVV)
                        )).toList().stream();

    }

    public static Stream<Arguments> invalidCardDetailsStream() {

        return Stream.of(
                invalidCardNumberFormatCardDetails(),
                invalidCardNumberLuhnCardDetails(),
                expiredCardCardDetails(),
                invalidCvvFormatCardDetails())
                .flatMap(List::stream);
    }

    private static List<Arguments> invalidCardNumberFormatCardDetails() {

        return invalidCardNumberFormats()
                .stream()
                .map(format ->Arguments.of(
                        new CardDetails(format, NOT_EXPIRED_CARD, VALID_CVV),
                        ErrorCode.INVALID_CARD_NUMBER_FORMAT))
                .toList();
    }

    private static List<Arguments> invalidCardNumberLuhnCardDetails() {

        return invalidLuhnCardNumbers().stream()
                .map(number ->
                        Arguments.of(
                                new CardDetails(number, NOT_EXPIRED_CARD,VALID_CVV),
                                ErrorCode.INVALID_CARD_NUMBER))
                .toList();

    }

    private static List<Arguments> expiredCardCardDetails() {
        return validLuhnCardNumbers()
                .stream()
                .map(number ->
                        Arguments.of(
                                new CardDetails(number, EXPIRED_CARD, VALID_CVV),
                                ErrorCode.EXPIRED_CARD
                        ))
                .toList();

    }

    private static List<Arguments> invalidCvvFormatCardDetails() {
        return invalidCvvFormat()
                .stream()
                .map(cvv -> Arguments.of(
                        new CardDetails(VALID_CARD_NUMBER, NOT_EXPIRED_CARD, cvv),
                        ErrorCode.INVALID_CVV_FORMAT
                )).toList();
    }

    private static List<String> invalidCvvFormat() {
        return List.of(
                "",
                "abc",
                "a1",
                "   ",
                "12",
                "a2345",
                "a12"
        );
    }

    private static List<String> invalidCardNumberFormats() {
        return List.of(
                "",
                "     ",
                "noDigits",
                "12345678901234",
                "12345678901234567"
        );
    }

    private static List<String> invalidLuhnCardNumbers() {
        return List.of(
                "4111111111111112",
                "5555555555554445",
                "4012888888881882",
                "378282246310006",
                "6011111111111118",
                "3530111333300001"
        );
    }

    public static List<String> validLuhnCardNumbers() {
        return List.of(
                "4111111111111111",
                "4012888888881881",
                "5555555555554444",
                "5105105105105100",
                "378282246310005",
                "371449635398431",
                "6011111111111117",
                "6011000990139424",
                "3530111333300000",
                "3566002020360505"
        );
    }
}
