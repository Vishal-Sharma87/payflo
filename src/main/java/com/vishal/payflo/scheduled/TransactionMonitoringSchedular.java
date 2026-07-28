package com.vishal.payflo.scheduled;


import com.github.f4b6a3.uuid.UuidCreator;
import com.vishal.payflo.cache.service.RedisZSetService;
import com.vishal.payflo.kafka.EventPublisher;
import com.vishal.payflo.kafka.events.PaymentEvent;
import com.vishal.payflo.kafka.events.PaymentTimedOutEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
public class TransactionMonitoringSchedular {

    private final RedisZSetService zSetService;
    private final EventPublisher eventPublisher;

    public TransactionMonitoringSchedular(RedisZSetService zSetService,
                                          EventPublisher eventPublisher){
        this.eventPublisher = eventPublisher;
        this.zSetService = zSetService;
    }

    @Scheduled(fixedDelayString = "${payflo.scheduler.transaction-monitor-fixed-delay-ms}")
    public void finalizeTimedOutPaymentTransaction(){
        Set<String> transactionIds = zSetService.findExpiredTransactionBefore(Instant.now());
        if (transactionIds != null && !transactionIds.isEmpty()) {
            log.info("Publishing timed out events for {} expired transaction(s)", transactionIds.size());
            transactionIds.forEach(this::terminateTransaction);
        }

    }

    private void terminateTransaction(String id) {
        UUID transactionId = UuidCreator.fromString(id);

        PaymentEvent paymentEvent = new PaymentTimedOutEvent(transactionId);

        eventPublisher.publish(paymentEvent);
    }
}
