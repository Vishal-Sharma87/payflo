package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.TransactionInitializationRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class TransactionInitializationServiceTest {

    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final Instant STARTED_AT = Instant.now();

    private static final String HASH_KEY_PREFIX = "hashKey";
    private static final String STATUS_HASH_KEY = "status";
    private static final String ZSET_KEY = "zset";

    private static final Long ZSET_SCORE = STARTED_AT.plus(Duration.ofMinutes(10)).toEpochMilli();


    @Mock
    private RedisHashService hashService;

    @Mock
    private RedisZSetService zSetService;

    @Mock
    private TransactionInitializationRepository repository;

    private TransactionInitializationService initializationService;

    @BeforeEach
    public void setup() {
        initializationService = new TransactionInitializationService(hashService, zSetService, repository);
    }


    @Test
    public void testKeysOrderingForLua() {
        Mockito.when(hashService.buildKey(TRANSACTION_ID)).thenReturn(HASH_KEY_PREFIX + TRANSACTION_ID);
        Mockito.when(hashService.getStatusHashKey()).thenReturn(STATUS_HASH_KEY);
        Mockito.when(zSetService.getZSetKey()).thenReturn(ZSET_KEY);
        Mockito.when(zSetService.calculateScore(STARTED_AT)).thenReturn(ZSET_SCORE);

        ArgumentCaptor<List<String>> keys = ArgumentCaptor.captor();

        initializationService.initialize(TRANSACTION_ID, STARTED_AT);

        Mockito.verify(repository).initialize(
                Mockito.any(),
                keys.capture(),
                Mockito.anyString(),
                Mockito.anyString());

        Assertions.assertEquals(
                List.of(
                        HASH_KEY_PREFIX + TRANSACTION_ID,
                        STATUS_HASH_KEY, ZSET_KEY),
                keys.getValue());

    }
}
