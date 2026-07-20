package com.vishal.payflo.controllers;

import com.vishal.payflo.dtos.ApiResponse;
import com.vishal.payflo.dtos.responsedtos.PaymentStatusResponseDto;
import com.vishal.payflo.services.PaymentStatusService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentStatusController {

    private final PaymentStatusService paymentStatusService;

    public PaymentStatusController(PaymentStatusService paymentStatusService){
        this.paymentStatusService = paymentStatusService;
    }

    @GetMapping("/payment/status/{transactionId}")
    public ResponseEntity<ApiResponse<PaymentStatusResponseDto>> getStatusIfExists(@PathVariable String transactionId){
        PaymentStatusResponseDto paymentStatusResponseDto = paymentStatusService.check(transactionId);

        return ResponseEntity.ok(ApiResponse.of(paymentStatusResponseDto));
    }

}
