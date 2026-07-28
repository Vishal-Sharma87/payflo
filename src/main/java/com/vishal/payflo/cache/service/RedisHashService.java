package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.RedisHashRepository;
import com.vishal.payflo.configs.RedisKeysProperties;
import com.vishal.payflo.configs.RedisStatusTtlProperties;
import com.vishal.payflo.enums.TransactionStatus;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationHashKeyResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
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
        log.info("Reading transaction status for transactionId:{}", transactionId);
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();
        String status = redisHashRepository.findStatus(key, hashKey);
        log.info("Read transaction status {} for transactionId:{}", status, transactionId);
        return status;
    }

    public void finalizeStatus(UUID transactionId, TransactionStatus transactionStatus) {
        log.info("Finalizing transaction status {} for transactionId:{}", transactionStatus, transactionId);
        String key = buildKey(transactionId);
        String hashKey = getStatusHashKey();

        long statusTtl = ttlProperties.statusTtlHours();

        redisHashRepository.set(key, hashKey, transactionStatus.toString());
        redisHashRepository.expire(key, statusTtl);
        log.info("Transaction status finalized as {} for transactionId:{}", transactionStatus, transactionId);
    }

    public boolean isNotificationSent(UUID transactionId, KafkaTopic kafkaTopic) {
        String key = buildKey(transactionId);
        String notificationHashKey = getNotificationHashKey(kafkaTopic);

        boolean sent = redisHashRepository.hasKey(key, notificationHashKey);
        if (!sent) {
            log.info("Notification not yet sent for transactionId:{} topic:{}", transactionId, kafkaTopic);
        }
        return sent;
    }

    public void notificationProcessed(UUID transactionId, KafkaTopic kafkaTopic) {
        log.info("Marking notification processed for transactionId:{} topic:{}", transactionId, kafkaTopic);
        String key = buildKey(transactionId);
        String notificationHashKey = getNotificationHashKey(kafkaTopic);

        redisHashRepository.set(key, notificationHashKey,Boolean.toString(true));
        log.info("Notification marked processed for transactionId:{} topic:{}", transactionId, kafkaTopic);
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
