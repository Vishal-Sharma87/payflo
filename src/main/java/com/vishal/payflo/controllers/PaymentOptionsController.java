package com.vishal.payflo.controllers;

import com.vishal.payflo.dtos.ApiResponse;
import com.vishal.payflo.enums.PaymentType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentOptionsController {

    @GetMapping("/payment/options")
    public ResponseEntity<ApiResponse<PaymentType[]>> getAvailablePaymentOptions(){
        return ResponseEntity.ok(ApiResponse.of(PaymentType.values()));
    }

}
