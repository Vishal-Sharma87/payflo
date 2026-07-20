package com.vishal.payflo.controllers;

import com.vishal.payflo.dtos.ApiResponse;
import com.vishal.payflo.dtos.responsedtos.PaymentOptionsResponse;
import com.vishal.payflo.enums.PaymentType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class PaymentOptionsController {

    @GetMapping("/payment/options")
    public ResponseEntity<ApiResponse<PaymentOptionsResponse>> getAvailablePaymentOptions(){
        return ResponseEntity.ok(ApiResponse.of(new PaymentOptionsResponse(List.of(PaymentType.values()))));
    }

}
