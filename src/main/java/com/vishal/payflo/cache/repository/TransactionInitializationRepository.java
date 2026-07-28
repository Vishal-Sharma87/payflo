package com.vishal.payflo.cache.repository;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class TransactionInitializationRepository {

    private final StringRedisTemplate redisTemplate;

    public TransactionInitializationRepository(StringRedisTemplate redisTemplate){
        this.redisTemplate = redisTemplate;
    }

    public void initialize(RedisScript<Void> initializeScript, List<String> keys, String score, String member) {
        redisTemplate.execute(initializeScript, keys, score, member);
    }
}
