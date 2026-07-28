package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.RedisZSetRepository;
import com.vishal.payflo.configs.PaymentTimeoutProperties;
import com.vishal.payflo.configs.RedisKeysProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

@Service
@Slf4j
public class RedisZSetService {

    private final RedisZSetRepository redisZSetRepository;
    private final PaymentTimeoutProperties paymentTimeoutProperties;
    private final RedisKeysProperties redisKeysProperties;

    public RedisZSetService(RedisZSetRepository redisZSetRepository,
                            PaymentTimeoutProperties paymentTimeoutProperties,
                            RedisKeysProperties redisKeysProperties){

        this.redisZSetRepository = redisZSetRepository;
        this.paymentTimeoutProperties =paymentTimeoutProperties;
        this.redisKeysProperties = redisKeysProperties;
    }

    public Set<String> findExpiredTransactionBefore(Instant deadline) {
        String zsetKey = getZSetKey();
        long maxScore = deadline.toEpochMilli();

        Set<String> transactionIds = redisZSetRepository.rangeByScore(zsetKey, 0, maxScore);

        if (transactionIds != null && !transactionIds.isEmpty()) {
            log.info("Found {} expired transaction(s) before deadline:{}", transactionIds.size(), deadline);
        }

        return transactionIds;
    }

    public String getZSetKey(){
        return redisKeysProperties.processingTransactionsZSetKey();
    }

    /**
     * Computes the sorted-set score for a transaction: the instant after which
     * it should be considered timed out, expressed as epoch millis.
     * Score = startedAt + configured timeout buffer.
     */
    public long calculateScore(Instant startedAt){
        int minutesToAdd = paymentTimeoutProperties.timeoutBufferMinutes();
        return startedAt.plus(Duration.ofMinutes(minutesToAdd)).toEpochMilli();
    }
}
