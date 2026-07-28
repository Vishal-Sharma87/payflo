package com.vishal.payflo.cache.repository;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public class RedisZSetRepository {

    private final ZSetOperations<String, String> zSetOperations;

    public RedisZSetRepository(StringRedisTemplate stringRedisTemplate){
        this.zSetOperations = stringRedisTemplate.opsForZSet();
    }

    public Set<String> rangeByScore(String key, long minScore, long maxScore) {
        return zSetOperations.rangeByScore(key, minScore, maxScore);
    }
}
