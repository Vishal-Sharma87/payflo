package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.RedisZSetRepository;
import com.vishal.payflo.configs.PaymentTimeoutProperties;
import com.vishal.payflo.configs.RedisKeysProperties;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Service
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

    public void createEntry(UUID transactionId, Instant startedAt) {
        String zsetKey = getZSetKey();
        long score = calculateScore(startedAt);
        String member = transactionId.toString();

        redisZSetRepository.put(zsetKey, score, member);
    }

    public void remove(UUID transactionId) {
        String zsetKey = getZSetKey();
        String member = transactionId.toString();

        redisZSetRepository.remove(zsetKey, member);
    }


    public Set<String> findExpiredTransactionBefore(Instant deadline) {
        String zsetKey = getZSetKey();
        long maxScore = deadline.toEpochMilli();

        return redisZSetRepository.rangeByScore(zsetKey, 0, maxScore);
    }

    private String getZSetKey(){
        return redisKeysProperties.processingTransactionsZSetKey();
    }

    /**
     * Computes the sorted-set score for a transaction: the instant after which
     * it should be considered timed out, expressed as epoch millis.
     * Score = startedAt + configured timeout buffer.
     */
    private long calculateScore(Instant startedAt){
        int minutesToAdd = paymentTimeoutProperties.timeoutBufferMinutes();
        return startedAt.plus(Duration.ofMinutes(minutesToAdd)).toEpochMilli();
    }
}
