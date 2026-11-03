package com.llmgateway.service;

import com.llmgateway.exception.InsufficientBudgetException;
import com.llmgateway.model.ApiKey;
import com.llmgateway.repository.ApiKeyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Service
public class BudgetService {

    private static final Logger log = LoggerFactory.getLogger(BudgetService.class);
    private static final String BUDGET_PREFIX = "budget:";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ApiKeyRepository apiKeyRepository;

    private static final String RESERVE_LUA = """
            local exists = redis.call('EXISTS', KEYS[1])
            if exists == 0 then
                redis.call('HSET', KEYS[1], 'budget', ARGV[2], 'spent', ARGV[3], 'reserved', '0')
                redis.call('EXPIRE', KEYS[1], 604800)
            end

            local budget = tonumber(redis.call('HGET', KEYS[1], 'budget') or ARGV[2])
            local spent = tonumber(redis.call('HGET', KEYS[1], 'spent') or '0')
            local reserved = tonumber(redis.call('HGET', KEYS[1], 'reserved') or '0')
            local reservation = tonumber(ARGV[1])

            if (spent + reserved + reservation) > budget or (budget > 0 and spent >= budget) then
                return '0:' .. tostring(budget) .. ':' .. tostring(spent) .. ':' .. tostring(reserved)
            else
                local new_reserved = redis.call('HINCRBY', KEYS[1], 'reserved', reservation)
                return '1:' .. tostring(budget) .. ':' .. tostring(spent) .. ':' .. tostring(new_reserved)
            end
            """;

    private static final String SETTLE_LUA = """
            local reserved = tonumber(ARGV[1])
            local actual_cost = tonumber(ARGV[2])

            local cur_reserved = tonumber(redis.call('HGET', KEYS[1], 'reserved') or '0')
            local refund = math.min(cur_reserved, reserved)
            local new_reserved = redis.call('HINCRBY', KEYS[1], 'reserved', -refund)
            if new_reserved < 0 then
                redis.call('HSET', KEYS[1], 'reserved', '0')
                new_reserved = 0
            end

            local new_spent = redis.call('HINCRBY', KEYS[1], 'spent', actual_cost)
            local budget = tonumber(redis.call('HGET', KEYS[1], 'budget') or '0')

            return tostring(new_spent) .. ':' .. tostring(budget) .. ':' .. tostring(new_reserved)
            """;

    private final RedisScript<String> reserveScript = RedisScript.of(RESERVE_LUA, String.class);
    private final RedisScript<String> settleScript = RedisScript.of(SETTLE_LUA, String.class);

    public BudgetService(ReactiveStringRedisTemplate redisTemplate, ApiKeyRepository apiKeyRepository) {
        this.redisTemplate = redisTemplate;
        this.apiKeyRepository = apiKeyRepository;
    }

    public record BudgetState(long budgetMicroUsd, long spentMicroUsd, long reservedMicroUsd) {
        public long remainingMicroUsd() {
            return Math.max(0, budgetMicroUsd - spentMicroUsd);
        }
    }

    /**
     * Atomically reserves the estimated cost before executing the LLM call.
     * Throws InsufficientBudgetException (402) if budget is exhausted.
     */
    public Mono<Long> reserve(ApiKey apiKey, long estimatedCostMicroUsd) {
        if (apiKey == null || apiKey.getId() == null) {
            return Mono.just(0L);
        }

        String redisKey = BUDGET_PREFIX + apiKey.getId().toString();
        List<String> keys = List.of(redisKey);
        List<String> args = List.of(
                String.valueOf(estimatedCostMicroUsd),
                String.valueOf(apiKey.getBudgetMicroUsd()),
                String.valueOf(apiKey.getSpentMicroUsd())
        );

        return redisTemplate.execute(reserveScript, keys, args)
                .next()
                .flatMap(result -> {
                    String[] parts = result.split(":");
                    boolean allowed = "1".equals(parts[0]);
                    long budget = Long.parseLong(parts[1]);
                    long spent = Long.parseLong(parts[2]);
                    long reserved = Long.parseLong(parts[3]);

                    if (!allowed) {
                        double budgetUsd = budget / 1_000_000.0;
                        double spentUsd = spent / 1_000_000.0;
                        log.warn("Budget exhausted for key [{}]. Budget: ${}, Spent: ${}, Reserved: {}",
                                apiKey.getKeyPrefix(), budgetUsd, spentUsd, reserved);
                        return Mono.error(new InsufficientBudgetException(
                                String.format("Budget exceeded for this API key. Total budget: $%.4f, Spent: $%.4f",
                                        budgetUsd, spentUsd), budget, spent));
                    }

                    log.debug("Budget reserved {} micro-USD for key [{}]. Total in-flight reserved: {}",
                            estimatedCostMicroUsd, apiKey.getKeyPrefix(), reserved);
                    return Mono.just(estimatedCostMicroUsd);
                });
    }

    /**
     * Atomically settles the actual cost and refunds any unused reservation delta.
     */
    public Mono<BudgetState> settle(UUID apiKeyId, long reservedAmount, long actualCostMicroUsd) {
        if (apiKeyId == null) {
            return Mono.just(new BudgetState(0, 0, 0));
        }

        String redisKey = BUDGET_PREFIX + apiKeyId.toString();
        List<String> keys = List.of(redisKey);
        List<String> args = List.of(
                String.valueOf(reservedAmount),
                String.valueOf(actualCostMicroUsd)
        );

        return redisTemplate.execute(settleScript, keys, args)
                .next()
                .flatMap(result -> {
                    String[] parts = result.split(":");
                    long newSpent = Long.parseLong(parts[0]);
                    long budget = Long.parseLong(parts[1]);
                    long newReserved = Long.parseLong(parts[2]);

                    log.debug("Budget settled for key [{}]: actualCost={}, newSpent={}, reservedRemaining={}",
                            apiKeyId, actualCostMicroUsd, newSpent, newReserved);

                    // Asynchronously persist new spent to Postgres DB
                    if (actualCostMicroUsd > 0) {
                        apiKeyRepository.addSpent(apiKeyId, actualCostMicroUsd)
                                .subscribe(
                                        updated -> log.trace("Updated spent in DB for key [{}] to {}", apiKeyId, updated),
                                        err -> log.error("Failed to update spent in DB for key [{}]: {}", apiKeyId, err.getMessage())
                                );
                    }

                    return Mono.just(new BudgetState(budget, newSpent, newReserved));
                });
    }

    /**
     * Queries current budget state for an API key.
     */
    public Mono<BudgetState> getBudgetState(ApiKey apiKey) {
        if (apiKey == null || apiKey.getId() == null) {
            return Mono.just(new BudgetState(0, 0, 0));
        }

        String redisKey = BUDGET_PREFIX + apiKey.getId().toString();
        return redisTemplate.opsForHash().entries(redisKey)
                .collectMap(e -> e.getKey().toString(), e -> e.getValue().toString())
                .flatMap(map -> {
                    if (map.isEmpty() || !map.containsKey("budget")) {
                        // Seed into Redis from ApiKey
                        return syncBudgetToRedis(apiKey)
                                .thenReturn(new BudgetState(apiKey.getBudgetMicroUsd(), apiKey.getSpentMicroUsd(), 0L));
                    }
                    long budget = Long.parseLong(map.getOrDefault("budget", String.valueOf(apiKey.getBudgetMicroUsd())));
                    long spent = Long.parseLong(map.getOrDefault("spent", String.valueOf(apiKey.getSpentMicroUsd())));
                    long reserved = Long.parseLong(map.getOrDefault("reserved", "0"));
                    return Mono.just(new BudgetState(budget, spent, reserved));
                });
    }

    public Mono<Void> syncBudgetToRedis(ApiKey apiKey) {
        String redisKey = BUDGET_PREFIX + apiKey.getId().toString();
        return redisTemplate.opsForHash().put(redisKey, "budget", String.valueOf(apiKey.getBudgetMicroUsd()))
                .then(redisTemplate.opsForHash().put(redisKey, "spent", String.valueOf(apiKey.getSpentMicroUsd())))
                .then(redisTemplate.opsForHash().put(redisKey, "reserved", "0"))
                .then();
    }
}

