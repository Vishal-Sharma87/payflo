package com.vishal.payflo.advice;

import com.vishal.payflo.advice.enums.ErrorCode;
import com.vishal.payflo.advice.exceptions.InvalidVpaException;
import com.vishal.payflo.advice.exceptions.PaymentTransactionNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(PaymentTransactionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handlePaymentTransactionWithTransactionIdNotFound(PaymentTransactionNotFoundException exception){
        ErrorResponse error = ErrorResponse.of(exception.getMessage(),exception.getErrorCode());
        return ResponseEntity
                .status(HttpStatus.NOT_FOUND)
                .body(error);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException exception) {
        String message = "Invalid value for parameter '" + exception.getName() + "', expected type: " + exception.getRequiredType().getSimpleName();
        ErrorResponse error = ErrorResponse.of(message, ErrorCode.METHOD_ARGUMENT_TYPE_MISMATCH);
        return ResponseEntity
                .badRequest()
                .body(error);
    }

    @ExceptionHandler(InvalidVpaException.class)
    public ResponseEntity<ErrorResponse> handleInvalidVpaException(InvalidVpaException exception){
        ErrorResponse error = ErrorResponse.of(exception.getMessage(), exception.getErrorCode());

        return ResponseEntity
                .badRequest()
                .body(error);
    }


    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> catchAll(Exception ex) {
        log.error("CAUGHT: {}", ex.getClass().getName());
        return ResponseEntity.status(500).body("Something Went wrong. Please retry after some time.");
    }
}
