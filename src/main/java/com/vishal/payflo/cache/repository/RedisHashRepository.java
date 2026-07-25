package com.vishal.payflo.cache.repository;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;

@Repository
public class RedisHashRepository {

    private final StringRedisTemplate redisTemplate;
    private final HashOperations<String, String, String> hashOperations;

    public RedisHashRepository(StringRedisTemplate stringRedisTemplate){
        this.redisTemplate = stringRedisTemplate;
        this.hashOperations = stringRedisTemplate.opsForHash();
    }

    public String findStatus(String key, String hashKey) {
        return hashOperations.get(key, hashKey);
    }

    public void set(String key, String hashKey, String transactionStatus) {
        hashOperations.put(key, hashKey, transactionStatus);
    }

    public void expire(String key, long hours) {
        redisTemplate.expire(key, Duration.ofHours(hours));
    }
}
