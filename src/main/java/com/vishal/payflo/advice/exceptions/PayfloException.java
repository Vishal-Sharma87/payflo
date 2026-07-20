package com.vishal.payflo.advice.exceptions;

import com.vishal.payflo.advice.enums.ErrorCode;
import lombok.Getter;

public abstract class PayfloException extends RuntimeException{
    @Getter
    private ErrorCode errorCode;

    public PayfloException(String message, ErrorCode errorCode){
        super(message);
        this.errorCode = errorCode;
    }
}
