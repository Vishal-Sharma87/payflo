package com.vishal.payflo.validators;

import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.InvalidVpaException;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.VpaPaymentServiceProviderProperties;
import com.vishal.payflo.dtos.paymentdetails.UpiDetails;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class UpiValidatorTest {
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

    private final VpaPaymentServiceProviderProperties vpaProperties = new VpaPaymentServiceProviderProperties(List.of("okaxis", "oksbi", "ybl"));
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

    private static final ErrorCode SEPARATOR_MISMATCH = ErrorCode.VPA_SEPARATOR_COUNT_MISMATCH;
    private static final ErrorCode INVALID_IDENTIFIER_FORMAT = ErrorCode.VPA_INVALID_IDENTIFIER_FORMAT;
    private static final ErrorCode INVALID_PSP_FORMAT = ErrorCode.VPA_INVALID_PAYMENT_SERVICE_PROVIDER_FORMAT;
    private static final ErrorCode UNKNOWN_PSP = ErrorCode.VPA_UNKNOWN_PAYMENT_SERVICE_PROVIDER;

    private final UpiValidator upiValidator = new UpiValidator(vpaProperties, exceptionMessagesProperties);



    @ParameterizedTest
    @MethodSource("invalidVpaAndErrorCodeStream")
    public void testInvalidVpaValidation(String vpa, ErrorCode expectedErrorCode) {
        UpiDetails upiDetails = new UpiDetails(vpa);

        InvalidVpaException exception = Assertions.assertThrows(
                InvalidVpaException.class,
                () -> upiValidator.validate(upiDetails));

        Assertions.assertEquals(expectedErrorCode, exception.getErrorCode());
    }

    @ParameterizedTest
    @MethodSource("validVpaStream")
    public void testValidVpaValidation(String vpa){
        Assertions.assertDoesNotThrow(() -> upiValidator.validate(new UpiDetails(vpa)));
    }

    public static Stream<Arguments> validVpaStream() {
        return Stream.of(
                Arguments.of("someone@okaxis"),
                Arguments.of("some_123@ybl"),
                Arguments.of("12.vishal@ybl"),
                Arguments.of("._.@oksbi"),
                Arguments.of("...@ybl"),
                Arguments.of("123@okaxis"),
                Arguments.of("___@ybl"),
                Arguments.of("---@ybl"),
                Arguments.of("a1.@oksbi"),
                Arguments.of("exactly_sixty_characters_long_valid_virtual_payment_address1@ybl")
        );
    }


    public static Stream<Arguments> invalidVpaAndErrorCodeStream() {
        return Stream.of(
                separatorCountMismatchVpa(),
                invalidIdentifierFormatVpa(),
                invalidPspFormatVpa(),
                unknownPspVpa())
                .flatMap(List::stream);
    }


    private static List<Arguments> separatorCountMismatchVpa() {
        return List.of(
                Arguments.of("0separator", SEPARATOR_MISMATCH),
                Arguments.of("@moreThanOne@Separators@", SEPARATOR_MISMATCH),
                Arguments.of("exactly@Two@Separators", SEPARATOR_MISMATCH)
        );
    }

    private static List<Arguments> invalidIdentifierFormatVpa() {
        return List.of(
                Arguments.of("@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("aa@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("12@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("..@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("__@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("--@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("identifier_with_restricted_char_%@handle", INVALID_IDENTIFIER_FORMAT),
                Arguments.of("invalid_vpa_that_could_be_valid_if_not_exceeded_sixty_characters_count@handle", INVALID_IDENTIFIER_FORMAT)
        );
    }

    private static List<Arguments> invalidPspFormatVpa() {
        return List.of(
                Arguments.of("no_psp@", INVALID_PSP_FORMAT),
                Arguments.of("short_psp@ok", INVALID_PSP_FORMAT),
                Arguments.of("upper_cased_psp@YBL", INVALID_PSP_FORMAT),
                Arguments.of("psp_with_special_character@ok%axis", INVALID_PSP_FORMAT),
                Arguments.of("psp_length_more_than_39_chars@abcdefghijklmnopqrstuvwxyzonetwothreefourfive", INVALID_PSP_FORMAT)
        );
    }

    private static List<Arguments> unknownPspVpa() {
        return List.of(
                Arguments.of("someone@fakepsp", UNKNOWN_PSP),
                Arguments.of("someone@fraud", UNKNOWN_PSP),
                Arguments.of("someone@tralalart", UNKNOWN_PSP)
        );
    }
}
