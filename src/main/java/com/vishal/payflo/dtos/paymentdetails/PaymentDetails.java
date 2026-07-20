package com.vishal.payflo.dtos.paymentdetails;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.vishal.payflo.enums.PaymentType;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = UpiDetails.class, name = "UPI"),
        @JsonSubTypes.Type(value = CardDetails.class, name = "CARD")
})
public sealed interface PaymentDetails permits UpiDetails, CardDetails {
    PaymentType type();
}