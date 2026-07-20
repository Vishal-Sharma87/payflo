package com.vishal.payflo.controllers;

import com.vishal.payflo.dtos.ApiResponse;
import com.vishal.payflo.dtos.requestdtos.PaymentInitiateRequestDto;
import com.vishal.payflo.dtos.responsedtos.PaymentInitiateResponseDto;
import com.vishal.payflo.services.PaymentInitiationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentInitiationController {


    private final PaymentInitiationService paymentInitiationService;

    public PaymentInitiationController(PaymentInitiationService paymentInitiationService){
        this.paymentInitiationService = paymentInitiationService;
    }

    @PostMapping("/payment/initiate")
    public ResponseEntity<ApiResponse<PaymentInitiateResponseDto>> initiatePayment(@RequestBody PaymentInitiateRequestDto requestDto){
        PaymentInitiateResponseDto paymentInitiateResponseDto = paymentInitiationService.initiate(requestDto);

        return ResponseEntity.ok(ApiResponse.of(paymentInitiateResponseDto));
    }


}
