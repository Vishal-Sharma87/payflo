package com.vishal.payflo.advice.exceptions;


import com.vishal.payflo.advice.enums.ErrorCode;

public class PaymentTransactionNotFoundException extends PayfloException{

    public PaymentTransactionNotFoundException(String message){
        super(message, ErrorCode.PAYMENT_TRANSACTION_NOT_FOUND);
    }
}
