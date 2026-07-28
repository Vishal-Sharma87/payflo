package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.RedisHashRepository;
import com.vishal.payflo.configs.RedisKeysProperties;
import com.vishal.payflo.configs.RedisStatusTtlProperties;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationHashKeyResolver;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class RedisHashService {

    private final RedisHashRepository redisHashRepository;
    private final RedisKeysProperties redisKeysProperties;
    private final RedisStatusTtlProperties ttlProperties;
    private final NotificationHashKeyResolver notificationHashKeyResolver;

    public RedisHashService(RedisHashRepository redisHashRepository,
                            RedisKeysProperties redisKeysProperties,
                            RedisStatusTtlProperties ttlProperties,
                            NotificationHashKeyResolver notificationHashKeyResolver){

        this.redisHashRepository = redisHashRepository;
        this.redisKeysProperties = redisKeysProperties;
        this.ttlProperties = ttlProperties;
        this.notificationHashKeyResolver = notificationHashKeyResolver;
    }

    public String getStatus(UUID transactionId) {
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();
        return redisHashRepository.findStatus(key, hashKey);
    }

    public void finalizeStatus(UUID transactionId, TransactionStatus transactionStatus) {
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();

        long statusTtl = ttlProperties.statusTtlHours();

        redisHashRepository.set(key, hashKey, transactionStatus.toString());
        redisHashRepository.expire(key, statusTtl);
    }

    public boolean isNotificationSent(UUID transactionId, KafkaTopic kafkaTopic) {
        String key = buildKey(transactionId);
        String notificationHashKey = getNotificationHashKey(kafkaTopic);

        return redisHashRepository.hasKey(key, notificationHashKey);
    }

    public void notificationProcessed(UUID transactionId, KafkaTopic kafkaTopic) {
        String key = buildKey(transactionId);
        String notificationHashKey = getNotificationHashKey(kafkaTopic);

        redisHashRepository.set(key, notificationHashKey,Boolean.toString(true));
    }

    private String getNotificationHashKey(KafkaTopic kafkaTopic) {
        return notificationHashKeyResolver.resolve(kafkaTopic);
    }


    public String buildKey(UUID transactionId){
        return redisKeysProperties.paymentTransactionHashPrefix() + transactionId;
    }

    public String getStatusHashKey(){
        return redisKeysProperties.paymentTransactionHashStatusKey();
    }


}
