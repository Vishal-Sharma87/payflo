package com.vishal.payflo.dtos.responsedtos;

import com.vishal.payflo.enums.PaymentType;

import java.util.List;

public record PaymentOptionsResponse(List<PaymentType> options){}

