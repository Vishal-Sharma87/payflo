package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.TransactionInitializationRepository;
import com.vishal.payflo.cache.scripts.LuaScripts;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
public class TransactionInitializationService {

    private final RedisHashService hashService;
    private final RedisZSetService zSetService;
    private final TransactionInitializationRepository transactionInitializationRepository;

    public TransactionInitializationService(
            RedisHashService hashService,
            RedisZSetService zSetService,
            TransactionInitializationRepository transactionInitializationRepository){
        this.hashService =hashService;
        this.zSetService = zSetService;
        this.transactionInitializationRepository = transactionInitializationRepository;
    }

    public void initialize(UUID transactionId, Instant startedAt) {
        RedisScript<Void> initializeScript = LuaScripts.getPaymentTransactionInitializationScript();

        String hashKey = hashService.buildKey(transactionId);
        String statusHashKey = hashService.getStatusHashKey();
        String zsetKey = zSetService.getZSetKey();
        List<String> keys = new ArrayList<>(Arrays.asList(hashKey, statusHashKey, zsetKey));

        String member = transactionId.toString();
        String score = String.valueOf(zSetService.calculateScore(startedAt));

        transactionInitializationRepository.initialize(initializeScript, keys, score, member);
    }
}
