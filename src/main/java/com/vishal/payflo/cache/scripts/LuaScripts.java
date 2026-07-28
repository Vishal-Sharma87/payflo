package com.vishal.payflo.cache.scripts;

import org.springframework.data.redis.core.script.RedisScript;

public final class LuaScripts {

    /**
     * Atomically claims ownership of a transaction's termination for the calling consumer.
     * <p>
     * Guards against two distinct races:
     * <ul>
     *   <li><b>Cross-consumer race</b> — {@code payment-received}, {@code payment-failed}, and
     *       {@code payment-timed-out} consumers may fire close together for the same transactionId.
     *       Only the consumer that wins this atomic check-and-set proceeds; the other sees a
     *       non-matching status and no-ops.</li>
     *   <li><b>Crash-retry redelivery</b> — if this consumer already flipped the status to its own
     *       {@code *_PENDING} value on a prior run but crashed before finishing (MySQL write /
     *       notification / finalize), redelivery of the same message is allowed to proceed again,
     *       since the current status still matches this consumer's own pending state.</li>
     * </ul>
     * Proceeds (returns claimed) only if current status is {@code PROCESSING} (fresh claim) or
     * already equals this consumer's own {@code intermediateStatus} (crash-retry). Any other value —
     * including an already-finalized end status, or another consumer's {@code *_PENDING}/end state —
     * results in a skip. The script never needs to know the literal end-status value: "not PROCESSING
     * and not my own PENDING" already correctly implies "already finalized or owned elsewhere."
     * <p>
     * On successful claim, atomically sets the intermediate {@code *_PENDING} status and removes the
     * transaction from the timeout-tracking sorted set (zset), so the {@code TransactionMonitoringScheduler}
     * can no longer misread it as expired mid-termination.
     * <p>
     * Redis has no native boolean RESP type — Lua {@code false}/{@code true} convert to RESP
     * {@code nil}/integer {@code 1} respectively, which would NPE against {@code RedisScript<Boolean>}.
     * Returns {@code Long} instead: {@code 0} = claim failed/skipped, {@code 1} = claim succeeded.
     * <p>
     * <b>KEYS[1]</b> — transaction hash key ({@code payflo:payment-transaction:{transactionId}})<br>
     * <b>KEYS[2]</b> — status field name within the hash<br>
     * <b>KEYS[3]</b> — timeout-tracking zset key<br>
     * <b>ARGV[1]</b> — this consumer's intermediate {@code *_PENDING} status to claim with<br>
     * <b>ARGV[2]</b> — transactionId, as the zset member to remove
     *
     * @return a {@link RedisScript} yielding {@code 1L} if ownership was claimed, {@code 0L} otherwise
     */
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

    /**
     * Atomically initializes Redis state for a newly-initiated transaction: creates the status
     * hash entry (hardcoded to {@code PROCESSING}, since a hash entry only ever gets created at
     * initiation) and adds the transaction to the timeout-tracking sorted set in a single round trip.
     * <p>
     * No ownership/CAS check is needed here — {@code PaymentInitiatedConsumer} is the only consumer
     * that ever writes these keys for a given transactionId, so there's no cross-consumer race to
     * guard against. Combining both writes into one script is purely a round-trip-time optimization,
     * not a correctness fix; both {@code HSET} and {@code ZADD} are independently idempotent under
     * redelivery ({@code HSET} always writes the same {@code PROCESSING} value, and {@code ZADD}'s
     * score is deterministically derived from the event's own {@code startedAt}, so redelivery
     * produces an identical score every time).
     * <p>
     * <b>KEYS[1]</b> — transaction hash key<br>
     * <b>KEYS[2]</b> — status field name within the hash<br>
     * <b>KEYS[3]</b> — timeout-tracking zset key<br>
     * <b>ARGV[1]</b> — zset score, i.e. {@code startedAt + timeoutBuffer} as epoch millis<br>
     * <b>ARGV[2]</b> — transactionId, as the zset member
     *
     * @return a {@link RedisScript} with no meaningful return value (fire-and-forget)
     */
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