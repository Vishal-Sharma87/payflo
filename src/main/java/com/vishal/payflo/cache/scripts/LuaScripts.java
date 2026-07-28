package com.vishal.payflo.cache.scripts;

import org.springframework.data.redis.core.script.RedisScript;

public final class LuaScripts {

    public static RedisScript<Long> tryClaimScript(){
        String script = """
                local hashKey = KEYS[1]
                local transactionStatusHashKey = KEYS[2]
                local zsetKey = KEYS[3]
                local intermediateTransactionStatus = ARGV[1]
                local zsetMember = ARGV[2]
                
                local currentTransactionStatus = redis.call('HGET', hashKey, transactionStatusHashKey)
                if currentTransactionStatus == nil or not(currentTransactionStatus == 'PROCESSING' or currentTransactionStatus == intermediateTransactionStatus) then
                    return 0
                end
                
                redis.call('HSET', hashKey, transactionStatusHashKey, intermediateTransactionStatus)
                redis.call('ZREM', zsetKey, zsetMember)
                
                return 1
                """;

        return RedisScript.of(script, Long.class);
    }

    public static RedisScript<Void> getPaymentTransactionInitializationScript() {
        String script = """
                local hashKey = KEYS[1]
                local statusHashKey = KEYS[2]
                local zsetKey = KEYS[3]
                
                local score = tonumber(ARGV[1])
                local member = ARGV[2]
                
                redis.call('HSET', hashKey, statusHashKey,'PROCESSING')
                redis.call('ZADD', zsetKey, score, member)
                """;

            return RedisScript.of(script, Void.class);
    }
}
