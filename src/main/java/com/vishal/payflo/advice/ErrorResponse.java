package com.vishal.payflo.advice;

import com.vishal.payflo.advice.enums.ErrorCode;

import java.time.Instant;

public record ErrorResponse (
    String message,
    ErrorCode errorCode,
    Instant timestamp){

    public  static ErrorResponse of(String message, ErrorCode errorCode){
        return new ErrorResponse(message, errorCode, Instant.now());
    }

}
