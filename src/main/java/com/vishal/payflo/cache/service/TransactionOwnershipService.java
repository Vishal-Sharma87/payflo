package com.vishal.payflo.cache.service;

import com.vishal.payflo.cache.repository.TransactionOwnershipRepository;
import com.vishal.payflo.cache.scripts.LuaScripts;
import com.vishal.payflo.enums.TransactionStatus;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
public class TransactionOwnershipService {

    private final TransactionOwnershipRepository transactionOwnershipRepository;
    private final RedisHashService hashService;
    private final RedisZSetService zSetService;

    public TransactionOwnershipService(TransactionOwnershipRepository transactionOwnershipRepository,
                                       RedisHashService hashService,
                                       RedisZSetService zSetService){

        this.transactionOwnershipRepository = transactionOwnershipRepository;
        this.hashService = hashService;
        this.zSetService = zSetService;
    }


    public boolean tryClaim(UUID transactionId, TransactionStatus intermediateStatus){
        RedisScript<Long> tryClaimScript = LuaScripts.tryClaimScript();

        String hashKey = hashService.buildKey(transactionId);
        String statusHashKey = hashService.getStatusHashKey();
        String zsetKey = zSetService.getZSetKey();
        List<String> keys = new ArrayList<>(Arrays.asList(hashKey, statusHashKey, zsetKey));

        String statusToPass = intermediateStatus.name();
        String member = transactionId.toString();

        return transactionOwnershipRepository.tryClaim(tryClaimScript, keys, statusToPass, member);
    }
}
