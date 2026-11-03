package com.llmgateway.service;

import com.llmgateway.exception.RateLimitException;
import com.llmgateway.model.ApiKey;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.Refill;
import io.github.bucket4j.distributed.AsyncBucketProxy;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
public class RateLimitService {

    private static final Logger log = LoggerFactory.getLogger(RateLimitService.class);

    private final LettuceConnectionFactory connectionFactory;
    private volatile LettuceBasedProxyManager proxyManager;

    public RateLimitService(LettuceConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    private synchronized LettuceBasedProxyManager getProxyManager() {
        if (this.proxyManager == null) {
            Object nativeClient = connectionFactory.getNativeClient();
            if (nativeClient instanceof RedisClient redisClient) {
                this.proxyManager = LettuceBasedProxyManager.builderFor(redisClient)
                        .withExpirationStrategy(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(Duration.ofMinutes(5)))
                        .build();
                log.info("Initialized distributed LettuceBasedProxyManager for rate limiting");
            } else {
                throw new IllegalStateException("Redis native client is not an instance of io.lettuce.core.RedisClient: " + nativeClient);
            }
        }
        return this.proxyManager;
    }

    /**
     * Tries to consume 1 request token for the given API key.
     * Completes successfully if allowed, or errors with RateLimitException (429) if exceeded.
     */
    public Mono<Void> checkRateLimit(ApiKey apiKey) {
        if (apiKey == null) {
            return Mono.empty();
        }

        int rpm = apiKey.getRateLimitRpm() > 0 ? apiKey.getRateLimitRpm() : 60;
        byte[] keyBytes = ("ratelimit:" + apiKey.getId().toString()).getBytes(StandardCharsets.UTF_8);

        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.classic(rpm, Refill.greedy(rpm, Duration.ofMinutes(1))))
                .build();

        try {
            AsyncBucketProxy bucket = getProxyManager().asAsync().builder().build(keyBytes, configuration);
            return Mono.fromFuture(() -> bucket.tryConsumeAndReturnRemaining(1))
                    .flatMap(probe -> {
                        if (probe.isConsumed()) {
                            log.debug("Rate limit OK for key [{}]. Remaining tokens: {}",
                                    apiKey.getKeyPrefix(), probe.getRemainingTokens());
                            return Mono.empty();
                        } else {
                            long retryAfterSec = Math.max(1, TimeUnit.NANOSECONDS.toSeconds(probe.getNanosToWaitForRefill()));
                            log.warn("Rate limit exceeded for key [{}]. Retry after {} seconds",
                                    apiKey.getKeyPrefix(), retryAfterSec);
                            return Mono.error(new RateLimitException(
                                    "Rate limit exceeded. Please slow down your requests.", retryAfterSec));
                        }
                    });
        } catch (Exception e) {
            log.error("Error evaluating distributed rate limit for key [{}]: {}", apiKey.getKeyPrefix(), e.getMessage());
            // Fall open if rate limiting storage is unavailable
            return Mono.empty();
        }
    }
}

