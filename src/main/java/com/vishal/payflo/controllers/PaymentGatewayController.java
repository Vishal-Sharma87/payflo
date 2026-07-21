package com.vishal.payflo.controllers;

import com.vishal.payflo.dtos.requestdtos.PaymentConfirmRequestDto;
import com.vishal.payflo.services.PaymentGatewayService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class PaymentGatewayController {


    private final PaymentGatewayService paymentGatewayService;

    public PaymentGatewayController(PaymentGatewayService paymentGatewayService){
        this.paymentGatewayService = paymentGatewayService;
    }


    @PostMapping("/payment/confirm")
    public ResponseEntity<Void> completePaymentTransaction(@RequestBody PaymentConfirmRequestDto paymentConfirmRequestDto){
        paymentGatewayService.completeTransaction(paymentConfirmRequestDto);

        return ResponseEntity.accepted().build();
    }

}
