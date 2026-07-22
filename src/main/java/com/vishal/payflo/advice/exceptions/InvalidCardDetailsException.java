package com.vishal.payflo.advice.exceptions;

import com.vishal.payflo.advice.enums.ErrorCode;

public class InvalidCardDetailsException extends PayfloException{
    public InvalidCardDetailsException(String message, ErrorCode errorCode) {
        super(message, errorCode);
    }
}
