package com.vishal.payflo.services;


import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.PaymentTransactionNotFoundException;
import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.configs.ExceptionMessagesProperties;
import com.vishal.payflo.configs.PaymentStatusMessagesProperties;
import com.vishal.payflo.dtos.responsedtos.PaymentStatusResponseDto;
import com.vishal.payflo.enums.TransactionStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;
import java.util.stream.Stream;


@ExtendWith(MockitoExtension.class)
public class PaymentStatusServiceTest {

    private static final UUID MISSING_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID COMPLETED_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID PROCESSING_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID FAILED_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID TIMED_OUT_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID COMPLETED_PENDING_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID FAILED_PENDING_TRANSACTION_ID = UUID.randomUUID();
    private static final UUID TIMED_OUT_PENDING_TRANSACTION_ID = UUID.randomUUID();

    private static final String processing = "being processed";
    private static final String completed = "payment terminated with status COMPLETED";
    private static final String failed = "payment terminated with status FAILED";
    private static final String timedOut = "payment terminated with status TIMED_OUT";

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


    private final PaymentStatusMessagesProperties statusMessagesProperties =
            new PaymentStatusMessagesProperties(
                    processing,
                    completed,
                    failed,
                    timedOut
            );

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

    @Mock
    private RedisHashService redisHashService;

    private PaymentStatusService paymentStatusService;

    @BeforeEach
    void setup() {
        paymentStatusService = new PaymentStatusService(
                statusMessagesProperties,
                exceptionMessagesProperties,
                redisHashService);
    }


    @Test
    public void testMissingTransactionIdStatus() {
        Mockito.when(redisHashService.getStatus(MISSING_TRANSACTION_ID)).thenReturn(null);

        PaymentTransactionNotFoundException exception = Assertions.assertThrows(
                PaymentTransactionNotFoundException.class,
                () -> paymentStatusService.check(MISSING_TRANSACTION_ID));

        Assertions.assertEquals(ErrorCode.PAYMENT_TRANSACTION_NOT_FOUND, exception.getErrorCode());
    }


    @ParameterizedTest
    @MethodSource("validTransactionIdStream")
    public void testValidTransactionIdStatus(
            UUID transactionId,
            String statusInRedis,
            String expectedStatusMessage,
            TransactionStatus expectedTransactionStatus) {

        Mockito.when(redisHashService.getStatus(transactionId)).thenReturn(statusInRedis);

        PaymentStatusResponseDto actualDto = Assertions.assertDoesNotThrow(
                () -> paymentStatusService.check(transactionId)
        );

        Assertions.assertEquals(expectedStatusMessage, actualDto.message());
        Assertions.assertEquals(expectedTransactionStatus, actualDto.status());
    }

    @ParameterizedTest
    @MethodSource("midTerminationTransactionIdStream")
    public void testMidTerminationTransactionIdStatus(
            UUID transactionID,
            String statusInRedis){

        Mockito.when(redisHashService.getStatus(transactionID)).thenReturn(statusInRedis);

        PaymentStatusResponseDto dto = Assertions.assertDoesNotThrow(
                () -> paymentStatusService.check(transactionID)
        );

        Assertions.assertEquals(processing, dto.message());
        Assertions.assertEquals(TransactionStatus.PROCESSING, dto.status());
    }

    public static Stream<Arguments> validTransactionIdStream() {
        return Stream.of(
                Arguments.of(COMPLETED_TRANSACTION_ID, "COMPLETED", completed, TransactionStatus.COMPLETED),
                Arguments.of(PROCESSING_TRANSACTION_ID, "PROCESSING", processing, TransactionStatus.PROCESSING),
                Arguments.of(FAILED_TRANSACTION_ID, "FAILED", failed, TransactionStatus.FAILED),
                Arguments.of(TIMED_OUT_TRANSACTION_ID, "TIMED_OUT", timedOut, TransactionStatus.TIMED_OUT)
        );
    }

    public static Stream<Arguments> midTerminationTransactionIdStream() {
        return Stream.of(
                Arguments.of(COMPLETED_PENDING_TRANSACTION_ID, "COMPLETED_PENDING"),
                Arguments.of(FAILED_PENDING_TRANSACTION_ID, "FAILED_PENDING"),
                Arguments.of(TIMED_OUT_PENDING_TRANSACTION_ID, "TIMED_OUT_PENDING")
        );
    }
}
