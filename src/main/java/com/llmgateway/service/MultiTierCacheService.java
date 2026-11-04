package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.dto.*;
import com.llmgateway.model.SemanticCacheEntry;
import com.llmgateway.repository.SemanticCacheRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;

@Service
public class MultiTierCacheService {

    private static final Logger log = LoggerFactory.getLogger(MultiTierCacheService.class);
    private static final String EXACT_KEY_PREFIX = "cache:exact:";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final SemanticCacheRepository semanticCacheRepository;
    private final EmbeddingService embeddingService;
    private final GatewayProperties properties;
    private final ObjectMapper objectMapper;

    public MultiTierCacheService(ReactiveStringRedisTemplate redisTemplate,
                                 SemanticCacheRepository semanticCacheRepository,
                                 EmbeddingService embeddingService,
                                 GatewayProperties properties,
                                 ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.semanticCacheRepository = semanticCacheRepository;
        this.embeddingService = embeddingService;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public enum CacheHitType {
        NONE,
        HIT_EXACT,
        HIT_SEMANTIC
    }

    public record CacheResult(CacheHitType hitType, ChatCompletionResponse response) {
        public boolean isHit() {
            return hitType != CacheHitType.NONE && response != null;
        }

        public String getHeaderValue() {
            return switch (hitType) {
                case HIT_EXACT -> "HIT_EXACT";
                case HIT_SEMANTIC -> "HIT_SEMANTIC";
                case NONE -> "MISS";
            };
        }

        public static CacheResult miss() {
            return new CacheResult(CacheHitType.NONE, null);
        }

        public static CacheResult exact(ChatCompletionResponse resp) {
            return new CacheResult(CacheHitType.HIT_EXACT, resp);
        }

        public static CacheResult semantic(ChatCompletionResponse resp) {
            return new CacheResult(CacheHitType.HIT_SEMANTIC, resp);
        }
    }

    /**
     * Performs multi-tier cache lookup (Tier 1: Redis Exact Match, Tier 2: pgvector Semantic Match).
     */
    public Mono<CacheResult> lookup(ChatCompletionRequest request, boolean bypassCache) {
        if (!properties.getCache().isEnabled() || bypassCache) {
            return Mono.just(CacheResult.miss());
        }

        // Tier 1: Exact Match in Redis
        String exactKey = computeExactCacheKey(request);
        return redisTemplate.opsForValue().get(exactKey)
                .flatMap(json -> {
                    try {
                        ChatCompletionResponse resp = objectMapper.readValue(json, ChatCompletionResponse.class);
                        log.info("Tier 1 Exact Cache HIT for model [{}]", request.getModel());
                        return Mono.just(CacheResult.exact(resp));
                    } catch (Exception e) {
                        log.warn("Failed to deserialize exact cache response: {}", e.getMessage());
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(Mono.defer(() -> lookupSemanticCache(request)));
    }

    /**
     * Tier 2: Semantic Match in pgvector
     */
    private Mono<CacheResult> lookupSemanticCache(ChatCompletionRequest request) {
        // Temperature check: semantic cache is only eligible for deterministic / low-temperature queries
        double maxTemp = properties.getCache().getMaxTemperatureForSemantic();
        if (request.getTemperature() != null && request.getTemperature() > maxTemp) {
            log.debug("Skipping semantic cache: temperature {} > max {}", request.getTemperature(), maxTemp);
            return Mono.just(CacheResult.miss());
        }

        String userPrompt = extractUserPrompt(request);
        if (userPrompt.isBlank()) {
            return Mono.just(CacheResult.miss());
        }

        String systemPrompt = extractSystemPrompt(request);
        String scopeHash = sha256(request.getModel() + ":" + systemPrompt);
        double threshold = properties.getCache().getSemanticSimilarityThreshold();
        double maxDistance = Math.max(0.001, 1.0 - threshold);

        return embeddingService.embed(userPrompt)
                .flatMap(vector -> {
                    String vectorStr = EmbeddingService.formatVectorForPg(vector);
                    return semanticCacheRepository.findNearest(scopeHash, vectorStr, maxDistance);
                })
                .flatMap(entry -> {
                    try {
                        ChatCompletionResponse resp = objectMapper.readValue(entry.getResponseJson(), ChatCompletionResponse.class);
                        log.info("Tier 2 Semantic Cache HIT for model [{}], similarity: {}",
                                request.getModel(), entry.getSimilarity());
                        return Mono.just(CacheResult.semantic(resp));
                    } catch (Exception e) {
                        log.warn("Failed to deserialize semantic cache response: {}", e.getMessage());
                        return Mono.just(CacheResult.miss());
                    }
                })
                .defaultIfEmpty(CacheResult.miss());
    }

    /**
     * Caches response in Tier 1 (Redis) and Tier 2 (pgvector) asynchronously.
     */
    public Mono<Void> cacheResponse(ChatCompletionRequest request, ChatCompletionResponse response) {
        if (!properties.getCache().isEnabled() || response == null) {
            return Mono.empty();
        }

        String exactKey = computeExactCacheKey(request);
        Duration exactTtl = Duration.ofSeconds(properties.getCache().getExactTtlSeconds());

        try {
            String json = objectMapper.writeValueAsString(response);

            // 1. Tier 1: Save exact match to Redis
            Mono<Boolean> exactSave = redisTemplate.opsForValue().set(exactKey, json, exactTtl);

            // 2. Tier 2: Save semantic match to pgvector if eligible
            Mono<Void> semanticSave = Mono.empty();
            double maxTemp = properties.getCache().getMaxTemperatureForSemantic();
            if (request.getTemperature() == null || request.getTemperature() <= maxTemp) {
                String userPrompt = extractUserPrompt(request);
                if (!userPrompt.isBlank()) {
                    String systemPrompt = extractSystemPrompt(request);
                    String scopeHash = sha256(request.getModel() + ":" + systemPrompt);
                    Duration semanticTtl = Duration.ofSeconds(properties.getCache().getSemanticTtlSeconds());

                    semanticSave = embeddingService.embed(userPrompt)
                            .flatMap(vector -> {
                                String vectorStr = EmbeddingService.formatVectorForPg(vector);
                                return semanticCacheRepository.save(scopeHash, userPrompt, vectorStr, json, semanticTtl);
                            });
                }
            }

            return exactSave.then(semanticSave);
        } catch (Exception e) {
            log.error("Failed to serialize response for caching: {}", e.getMessage());
            return Mono.empty();
        }
    }

    /**
     * SSE Replay Engine: synthesizes cached responses into standard streaming SSE chunks so client
     * streaming consumers receive seamless responses with sub-10ms latencies.
     */
    public Flux<ServerSentEvent<String>> replaySseStream(ChatCompletionResponse cachedResponse) {
        String content = "";
        if (cachedResponse.getChoices() != null && !cachedResponse.getChoices().isEmpty()
                && cachedResponse.getChoices().get(0).getMessage() != null) {
            content = cachedResponse.getChoices().get(0).getMessage().getContentAsString();
        }

        long created = cachedResponse.getCreated() != null ? cachedResponse.getCreated() : (System.currentTimeMillis() / 1000);
        String id = cachedResponse.getId() != null ? cachedResponse.getId() : "chatcmpl-cached-" + UUID.randomUUID().toString().substring(0, 8);
        String model = cachedResponse.getModel();

        List<ChatCompletionChunk> chunks = new ArrayList<>();

        if (content.isEmpty()) {
            chunks.add(ChatCompletionChunk.builder()
                    .id(id)
                    .created(created)
                    .model(model)
                    .choices(List.of(StreamChoice.builder()
                            .index(0)
                            .delta(DeltaMessage.builder().role("assistant").content("").build())
                            .finishReason("stop")
                            .build()))
                    .usage(cachedResponse.getUsage())
                    .build());
        } else {
            // Split content into small natural token-like chunks for realistic SSE streaming
            String[] words = content.split("(?<=\\s)");
            StringBuilder buffer = new StringBuilder();

            for (int i = 0; i < words.length; i++) {
                buffer.append(words[i]);
                if ((i + 1) % 3 == 0 || i == words.length - 1) {
                    DeltaMessage delta = (i < 3)
                            ? DeltaMessage.builder().role("assistant").content(buffer.toString()).build()
                            : DeltaMessage.builder().content(buffer.toString()).build();

                    String finishReason = (i == words.length - 1) ? "stop" : null;
                    Usage usage = (i == words.length - 1) ? cachedResponse.getUsage() : null;

                    chunks.add(ChatCompletionChunk.builder()
                            .id(id)
                            .created(created)
                            .model(model)
                            .choices(List.of(StreamChoice.builder()
                                    .index(0)
                                    .delta(delta)
                                    .finishReason(finishReason)
                                    .build()))
                            .usage(usage)
                            .build());

                    buffer.setLength(0);
                }
            }
        }

        return Flux.fromIterable(chunks)
                .map(chunk -> {
                    try {
                        String json = objectMapper.writeValueAsString(chunk);
                        return ServerSentEvent.<String>builder().data(json).build();
                    } catch (Exception e) {
                        throw new RuntimeException("SSE serialization error", e);
                    }
                })
                .concatWith(Mono.just(ServerSentEvent.<String>builder().data("[DONE]").build()));
    }

    private String computeExactCacheKey(ChatCompletionRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append(request.getModel()).append("|");

        if (request.getMessages() != null) {
            for (ChatMessage msg : request.getMessages()) {
                sb.append(msg.getRole()).append(":").append(msg.getContentAsString()).append(";");
            }
        }

        double temp = request.getTemperature() != null ? request.getTemperature() : 1.0;
        double topP = request.getTopP() != null ? request.getTopP() : 1.0;
        int maxTokens = request.getMaxTokens() != null ? request.getMaxTokens() : 0;
        sb.append("temp=").append(temp).append("|");
        sb.append("topP=").append(topP).append("|");
        sb.append("maxTokens=").append(maxTokens).append("|");
        sb.append("stop=").append(request.getStop());

        return EXACT_KEY_PREFIX + sha256(sb.toString());
    }

    private String extractUserPrompt(ChatCompletionRequest request) {
        if (request.getMessages() == null) {
            return "";
        }
        for (int i = request.getMessages().size() - 1; i >= 0; i--) {
            ChatMessage msg = request.getMessages().get(i);
            if ("user".equalsIgnoreCase(msg.getRole())) {
                return msg.getContentAsString().trim();
            }
        }
        return "";
    }

    private String extractSystemPrompt(ChatCompletionRequest request) {
        if (request.getMessages() == null) {
            return "";
        }
        for (ChatMessage msg : request.getMessages()) {
            if ("system".equalsIgnoreCase(msg.getRole())) {
                return msg.getContentAsString().trim();
            }
        }
        return "";
    }

    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            return Integer.toHexString(input.hashCode());
        }
    }
}
