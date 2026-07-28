package com.vishal.payflo.services;

import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.repositories.PaymentTransactionRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentTransactionService {

    @PersistenceContext
    private EntityManager entityManager;

    private final PaymentTransactionRepository paymentTransactionRepository;

    public PaymentTransactionService(PaymentTransactionRepository paymentTransactionRepository){
        this.paymentTransactionRepository = paymentTransactionRepository;
    }


    @Transactional
    public void markTransactionStatusCompleted(UUID transactionId) {
        log.info("Marking transaction as COMPLETED for transactionId:{}", transactionId);
        paymentTransactionRepository.markTransactionStatusCompleted(transactionId);
        log.info("Marked transaction as COMPLETED for transactionId:{}", transactionId);
    }

    @Transactional
    public void markPaymentTransactionFailed(UUID transactionId) {
        log.info("Marking transaction as FAILED for transactionId:{}", transactionId);
        paymentTransactionRepository.markTransactionStatusFailed(transactionId);
        log.info("Marked transaction as FAILED for transactionId:{}", transactionId);
    }

    @Transactional
    public void markTransactionStatusTimedOut(UUID transactionId) {
        log.info("Marking transaction as TIMED_OUT for transactionId:{}", transactionId);
        paymentTransactionRepository.markTransactionStatusTimedOut(transactionId);
        log.info("Marked transaction as TIMED_OUT for transactionId:{}", transactionId);
    }

    @Transactional
    public void persistNewTransaction(PaymentTransaction paymentTransaction) {
        log.info("Persisting new transaction for transactionId:{}", paymentTransaction.getTransactionId());
        entityManager.persist(paymentTransaction);
        log.info("Persisted new transaction for transactionId:{}", paymentTransaction.getTransactionId());
    }
}
