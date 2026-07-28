package com.vishal.payflo.cache.repository;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class TransactionOwnershipRepository {

    private final StringRedisTemplate redisTemplate;

    public TransactionOwnershipRepository(StringRedisTemplate redisTemplate){
        this.redisTemplate = redisTemplate;
    }

    public boolean tryClaim(RedisScript<Long> tryClaimScript, List<String> keys, String statusToPass, String member) {
        Long claimed = redisTemplate.execute(tryClaimScript, keys, statusToPass, member);

        return claimed != null && claimed.equals(1L);
    }
}
