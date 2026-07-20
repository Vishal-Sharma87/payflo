package com.vishal.payflo.services;

import com.vishal.payflo.entities.PaymentTransaction;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.repositories.PaymentTransactionRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentTransactionService {

    @PersistenceContext
    private EntityManager entityManager;

    private final PaymentTransactionRepository paymentTransactionRepository;

    public PaymentTransactionService(PaymentTransactionRepository paymentTransactionRepository){
        this.paymentTransactionRepository = paymentTransactionRepository;
    }


    @Transactional
    public void createNewTransaction(PaymentTransaction transaction) {
        entityManager.persist(transaction);
    }

    @Transactional
    public void markTransactionStatusCompleted(UUID transactionId) {
        paymentTransactionRepository.markTransactionStatusCompleted(transactionId);
    }

    @Transactional
    public void markPaymentTransactionFailed(UUID transactionId) {
        paymentTransactionRepository.markTransactionStatusFailed(transactionId);
    }

    @Transactional
    public void markTransactionStatusTimedOut(UUID transactionId) {
        paymentTransactionRepository.markTransactionStatusTimedOut(transactionId);
    }

    public Optional<TransactionStatus> findTransactionStatusById(UUID transactionId) {
        return paymentTransactionRepository.findTransactionStatusById(transactionId);
    }
}
