package com.vishal.payflo.advice.exceptions;

import com.vishal.payflo.advice.enums.ErrorCode;

public class InvalidVpaException extends PayfloException{
    public InvalidVpaException(String message, ErrorCode errorCode) {
        super(message, errorCode);
    }
}
