package com.vishal.payflo.cache.repository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class TransactionOwnershipRepositoryTest {

    private static final RedisScript<Long> TRY_CLAIM_SCRIPT = RedisScript.of("tryClaimScript", Long.class);
    private static final List<String> KEYS = new ArrayList<>();
    private static final String STATUS_TO_PASS = "STATUS_PENDING";
    private static final String ZSET_MEMBER = "12345";

    @Mock
    private StringRedisTemplate redisTemplate;

    private TransactionOwnershipRepository repository;

    @BeforeEach
    public void setup(){
        repository = new TransactionOwnershipRepository(redisTemplate);
    }


    @Test
    public void testNotClaimed(){

        Mockito.when(redisTemplate.execute(
                Mockito.any(RedisScript.class),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.anyString())
        ).thenReturn(0L);


        boolean claimed = repository.tryClaim(TRY_CLAIM_SCRIPT, KEYS, STATUS_TO_PASS, ZSET_MEMBER);
        Assertions.assertFalse( claimed);

    }
    @Test
    public void testClaimed(){

        Mockito.when(redisTemplate.execute(
                Mockito.any(RedisScript.class),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.anyString())
        ).thenReturn(1L);


        boolean claimed = repository.tryClaim(TRY_CLAIM_SCRIPT, KEYS, STATUS_TO_PASS, ZSET_MEMBER);

        Assertions.assertTrue(claimed);
    }

    @Test
    public void redisReturnsNullFallBackToFalseTest(){

        Mockito.when(redisTemplate.execute(
                Mockito.any(RedisScript.class),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.anyString())
        ).thenReturn(null);


        boolean claimed = repository.tryClaim(TRY_CLAIM_SCRIPT, KEYS, STATUS_TO_PASS, ZSET_MEMBER);

        Assertions.assertFalse(claimed);
    }


}
