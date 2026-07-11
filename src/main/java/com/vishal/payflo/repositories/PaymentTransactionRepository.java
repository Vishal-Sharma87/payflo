package com.vishal.payflo.repositories;

import com.vishal.payflo.entities.PaymentTransaction;
import io.lettuce.core.dynamic.annotation.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PaymentTransactionRepository extends JpaRepository<PaymentTransaction, UUID> {

    @Modifying
    @Query("UPDATE PaymentTransaction pt1 SET pt1.transactionStatus = TransactionStatus.COMPLETED WHERE pt1.transactionId = :transactionId")
    void markTransactionStatusCompleted(@Param("transactionId") UUID transactionId);

    @Modifying
    @Query("UPDATE PaymentTransaction pt1 SET pt1.transactionStatus = TransactionStatus.FAILED WHERE pt1.transactionId = :transactionId")
    void markTransactionStatusFailed(@Param("transactionId") UUID transactionId);

    @Modifying
    @Query("UPDATE PaymentTransaction pt1 SET pt1.transactionStatus = TransactionStatus.TIMED_OUT WHERE pt1.transactionId = :transactionId")
    void markTransactionStatusTimedOut(UUID transactionId);
}
