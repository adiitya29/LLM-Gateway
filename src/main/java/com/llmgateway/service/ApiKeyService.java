package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.dto.ApiKeyCreateResponse;
import com.llmgateway.dto.ApiKeySummaryResponse;
import com.llmgateway.dto.CreateApiKeyRequest;
import com.llmgateway.model.ApiKey;
import com.llmgateway.repository.ApiKeyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

@Service
public class ApiKeyService {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyService.class);
    private static final String REDIS_KEY_PREFIX = "apikey:";
    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    private final ApiKeyRepository apiKeyRepository;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final SecureRandom secureRandom = new SecureRandom();

    public ApiKeyService(ApiKeyRepository apiKeyRepository,
                         ReactiveStringRedisTemplate redisTemplate,
                         ObjectMapper objectMapper) {
        this.apiKeyRepository = apiKeyRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Authenticates an API key by computing its SHA-256 hash and looking up Redis (with DB fallback).
     */
    public Mono<ApiKey> authenticateKey(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return Mono.empty();
        }
        String hash = sha256Hex(rawKey.trim());
        String redisKey = REDIS_KEY_PREFIX + hash;

        return redisTemplate.opsForValue().get(redisKey)
                .flatMap(json -> {
                    try {
                        ApiKey cached = objectMapper.readValue(json, ApiKey.class);
                        return Mono.just(cached);
                    } catch (Exception e) {
                        log.warn("Failed to parse cached ApiKey: {}", e.getMessage());
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(
                        apiKeyRepository.findByKeyHash(hash)
                                .flatMap(dbKey -> {
                                    cacheKeyInRedis(hash, dbKey);
                                    return Mono.just(dbKey);
                                })
                );
    }

    /**
     * Generates a new API key, stores the hash in the database, and returns the full raw key once.
     */
    public Mono<ApiKeyCreateResponse> createKey(CreateApiKeyRequest request) {
        String rawKey = generateSecureKey();
        String prefix = rawKey.substring(0, Math.min(rawKey.length(), 11));
        String hash = sha256Hex(rawKey);

        UUID keyId = UUID.randomUUID();
        long budget = request.getBudgetMicroUsd() != null ? request.getBudgetMicroUsd() : 10_000_000L;
        int rateLimit = request.getRateLimitRpm() != null ? request.getRateLimitRpm() : 60;

        ApiKey apiKey = ApiKey.builder()
                .id(keyId)
                .name(request.getName() != null ? request.getName() : "API Key")
                .keyPrefix(prefix)
                .keyHash(hash)
                .isActive(true)
                .allowedModels(request.getAllowedModels() != null ? request.getAllowedModels() : java.util.Collections.emptyList())
                .budgetMicroUsd(budget)
                .spentMicroUsd(0L)
                .rateLimitRpm(rateLimit)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();

        return apiKeyRepository.save(apiKey)
                .doOnSuccess(saved -> {
                    cacheKeyInRedis(hash, saved);
                    log.info("Created new API Key id [{}] prefix [{}] name [{}]", saved.getId(), saved.getKeyPrefix(), saved.getName());
                })
                .map(saved -> ApiKeyCreateResponse.builder()
                        .id(saved.getId())
                        .name(saved.getName())
                        .key(rawKey) // Exposed ONLY once at creation
                        .keyPrefix(saved.getKeyPrefix())
                        .allowedModels(saved.getAllowedModels())
                        .budgetMicroUsd(saved.getBudgetMicroUsd())
                        .rateLimitRpm(saved.getRateLimitRpm())
                        .isActive(saved.isActive())
                        .createdAt(saved.getCreatedAt())
                        .build());
    }

    /**
     * Lists all keys (exposes prefix, hides raw key & hash).
     */
    public Flux<ApiKeySummaryResponse> listKeys() {
        return apiKeyRepository.findAll()
                .map(key -> ApiKeySummaryResponse.builder()
                        .id(key.getId())
                        .name(key.getName())
                        .keyPrefix(key.getKeyPrefix())
                        .allowedModels(key.getAllowedModels())
                        .budgetMicroUsd(key.getBudgetMicroUsd())
                        .spentMicroUsd(key.getSpentMicroUsd())
                        .rateLimitRpm(key.getRateLimitRpm())
                        .isActive(key.isActive())
                        .createdAt(key.getCreatedAt())
                        .updatedAt(key.getUpdatedAt())
                        .build());
    }

    /**
     * Revokes a key in the database and purges it from Redis cache.
     */
    public Mono<Void> revokeKey(UUID id) {
        return apiKeyRepository.findById(id)
                .flatMap(key -> apiKeyRepository.revokeById(id)
                        .then(redisTemplate.delete(REDIS_KEY_PREFIX + key.getKeyHash()))
                        .then(redisTemplate.delete("budget:" + id))
                        .then(redisTemplate.delete("ratelimit:" + id))
                        .doOnSuccess(v -> log.info("Revoked API Key id [{}] and purged Redis cache", id))
                )
                .then();
    }

    private void cacheKeyInRedis(String hash, ApiKey apiKey) {
        try {
            String json = objectMapper.writeValueAsString(apiKey);
            redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + hash, json, CACHE_TTL).subscribe();
        } catch (Exception e) {
            log.warn("Failed to cache ApiKey in Redis: {}", e.getMessage());
        }
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm unavailable", e);
        }
    }

    private String generateSecureKey() {
        byte[] randomBytes = new byte[16];
        secureRandom.nextBytes(randomBytes);
        return "sk-gw-" + HexFormat.of().formatHex(randomBytes);
    }
}

