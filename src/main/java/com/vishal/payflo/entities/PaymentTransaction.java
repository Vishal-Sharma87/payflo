package com.vishal.payflo.entities;


import com.vishal.payflo.enums.PaymentType;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.events.PaymentInitiatedEvent;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_transactions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PaymentTransaction {

    @Id
    @Column(name = "transaction_id")
    private UUID transactionId;

    @Column(nullable = false, precision =  10, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_type", nullable = false)
    private PaymentType paymentType;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "transaction_status", nullable = false)
    private TransactionStatus transactionStatus;

    public static PaymentTransaction from(PaymentInitiatedEvent event){
        PaymentTransaction transaction = new PaymentTransaction();
        transaction.transactionId = event.transactionId();
        transaction.amount = event.amount();
        transaction.paymentType = event.paymentType();
        transaction.startedAt = event.startedAt();
        transaction.transactionStatus = TransactionStatus.PROCESSING;
        return  transaction;
    }

}
