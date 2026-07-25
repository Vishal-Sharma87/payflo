package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.RedisHashRepository;
import com.vishal.payflo.configs.RedisKeysProperties;
import com.vishal.payflo.configs.RedisStatusTtlProperties;
import com.vishal.payflo.enums.TransactionStatus;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class RedisHashService {

    private final RedisHashRepository redisHashRepository;
    private final RedisKeysProperties redisKeysProperties;
    private final RedisStatusTtlProperties ttlProperties;

    public RedisHashService(RedisHashRepository redisHashRepository,
                            RedisKeysProperties redisKeysProperties,
                            RedisStatusTtlProperties ttlProperties){
        this.redisHashRepository = redisHashRepository;
        this.redisKeysProperties = redisKeysProperties;
        this.ttlProperties = ttlProperties;
    }

    public String getStatus(UUID transactionId) {
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();
        return redisHashRepository.findStatus(key, hashKey);
    }

    /**
     * Creates the initial status hash entry for a transaction.
     * Always writes {@link TransactionStatus#PROCESSING} — by design, a hash
     * entry is only ever created at payment-initiation time, so no other
     * status is possible at creation.
     */
    public void createStatus(UUID transactionId) {
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();

        redisHashRepository.set(key, hashKey, TransactionStatus.PROCESSING.toString());
    }

    public void finalizeStatus(UUID transactionId, TransactionStatus transactionStatus) {
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();

        long statusTtl = ttlProperties.statusTtlHours();

        redisHashRepository.set(key, hashKey, transactionStatus.toString());
        redisHashRepository.expire(key, statusTtl);
    }

    private String buildKey(UUID transactionId){
        return redisKeysProperties.paymentTransactionHashPrefix() + transactionId;
    }

    private String getStatusHashKey(){
        return redisKeysProperties.paymentTransactionHashStatusKey();
    }

}
