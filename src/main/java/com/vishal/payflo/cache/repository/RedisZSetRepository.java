package com.vishal.payflo.cache.repository;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

@Repository
public class RedisZSetRepository {

    private final ZSetOperations<String, String> zSetOperations;

    public RedisZSetRepository(StringRedisTemplate stringRedisTemplate){
        this.zSetOperations = stringRedisTemplate.opsForZSet();
    }


    public void put(String zsetKey, long score, String transactionId) {
        zSetOperations.add(zsetKey, transactionId, score);
    }

    public void remove(String zsetKey, String transactionId) {
        zSetOperations.remove(zsetKey, transactionId);
    }
}
