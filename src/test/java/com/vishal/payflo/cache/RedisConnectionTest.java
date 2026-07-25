package com.vishal.payflo.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

@SpringBootTest
public class RedisConnectionTest {


    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    public void redisConnectionTest(){
        String key = "name";
        String value = "vishal";

        String expectedValue = "vishal";

        redisTemplate.opsForValue().set(key, value);
        String retrievedValue = redisTemplate.opsForValue().get(key);
        Assertions.assertEquals(expectedValue, retrievedValue);
    }

}
