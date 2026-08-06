package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.TransactionOwnershipRepository;
import com.vishal.payflo.enums.TransactionStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class TransactionOwnershipServiceTest {

    private static final String HASH_KEY_PREFIX = "hashKey";
    private static final String STATUS_HASH_KEY_PREFIX = "status";
    private static final String ZSET_KEY_PREFIX = "zsetKey";

    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final TransactionStatus INTERMEDIATE_STATUS = TransactionStatus.COMPLETED_PENDING;


    @Mock
    private RedisHashService hashService;

    @Mock
    private RedisZSetService zSetService;

    @Mock
    private TransactionOwnershipRepository repository;

    private TransactionOwnershipService ownershipService;

    @BeforeEach
    public void setup(){
        ownershipService = new TransactionOwnershipService(
                repository,
                hashService,
                zSetService);
    }


    @Test
    public void testClaimingOwnership() {
        Mockito.when(hashService.buildKey(TRANSACTION_ID)).thenReturn(HASH_KEY_PREFIX + TRANSACTION_ID);
        Mockito.when(hashService.getStatusHashKey()).thenReturn(STATUS_HASH_KEY_PREFIX);
        Mockito.when(zSetService.getZSetKey()).thenReturn(ZSET_KEY_PREFIX);

        Mockito.when(repository.tryClaim(
                Mockito.any(),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.anyString()
        )).thenReturn(true);

        ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.captor();

        ownershipService.tryClaim(TRANSACTION_ID, INTERMEDIATE_STATUS);

        Mockito.verify(repository).tryClaim(
                Mockito.any(),
                keysCaptor.capture(),
                Mockito.anyString(),
                Mockito.anyString());


        List<String> keys = keysCaptor.getValue();

        Assertions.assertEquals(
                List.of(
                        HASH_KEY_PREFIX + TRANSACTION_ID,
                        STATUS_HASH_KEY_PREFIX,
                        ZSET_KEY_PREFIX),
                keys
        );
    }
}
