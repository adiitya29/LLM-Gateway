package com.llmgateway.controller;

import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.exception.InsufficientBudgetException;
import com.llmgateway.exception.RateLimitException;
import com.llmgateway.model.ApiKey;
import com.llmgateway.model.UsageLog;
import com.llmgateway.repository.UsageLogRepository;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/v1")
public class ChatCompletionController {

    private static final Logger log = LoggerFactory.getLogger(ChatCompletionController.class);

    private final LlmProxyService proxyService;
    private final ModelRoutingService routingService;
    private final TokenizerService tokenizerService;
    private final PricingService pricingService;
    private final BudgetService budgetService;
    private final RateLimitService rateLimitService;
    private final MultiTierCacheService cacheService;
    private final UsageLogRepository usageLogRepository;

    public ChatCompletionController(LlmProxyService proxyService,
                                  ModelRoutingService routingService,
                                  TokenizerService tokenizerService,
                                  PricingService pricingService,
                                  BudgetService budgetService,
                                  RateLimitService rateLimitService,
                                  MultiTierCacheService cacheService,
                                  UsageLogRepository usageLogRepository) {
        this.proxyService = proxyService;
        this.routingService = routingService;
        this.tokenizerService = tokenizerService;
        this.pricingService = pricingService;
        this.budgetService = budgetService;
        this.rateLimitService = rateLimitService;
        this.cacheService = cacheService;
        this.usageLogRepository = usageLogRepository;
    }

    @PostMapping(value = "/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<?>> createChatCompletion(
            @RequestBody ChatCompletionRequest request,
            ServerWebExchange exchange) {
        log.info("Received chat completion request for model: {}, stream: {}",
                request.getModel(), request.isStreaming());

        // 1. Model Authorization check against authenticated API key
        ApiKey apiKey = exchange.getAttribute(GatewayAuthFilter.API_KEY_ATTR);
        if (apiKey != null && !apiKey.isModelAllowed(request.getModel())) {
            log.warn("API key [{}] not authorized for requested model [{}]",
                    apiKey.getKeyPrefix(), request.getModel());

            Map<String, Object> errorBody = Map.of(
                    "error", Map.of(
                            "message", "Model '" + request.getModel() + "' is not permitted for this API key",
                            "type", "permission_denied",
                            "code", "model_not_allowed"
                    )
            );
            return Mono.just(ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(errorBody));
        }

        // 2. Cache bypass header check
        String bypassHeader = exchange.getRequest().getHeaders().getFirst("X-Gateway-Bypass-Cache");
        boolean bypassCache = "true".equalsIgnoreCase(bypassHeader);
        long startTime = System.currentTimeMillis();

        // 3. Distributed Rate Limiting (Bucket4j with Redis Lettuce)
        return rateLimitService.checkRateLimit(apiKey)
                .then(cacheService.lookup(request, bypassCache))
                .flatMap(cacheResult -> {
                    if (cacheResult.isHit()) {
                        long latencyMs = System.currentTimeMillis() - startTime;
                        String cacheHeader = cacheResult.getHeaderValue(); // HIT_EXACT or HIT_SEMANTIC
                        UUID apiKeyId = apiKey != null ? apiKey.getId() : null;
                        String requestId = "req_cached_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);

                        ChatCompletionResponse cachedResponse = cacheResult.response();
                        int promptTokens = cachedResponse.getUsage() != null ? cachedResponse.getUsage().getPromptTokens() : 0;
                        int completionTokens = cachedResponse.getUsage() != null ? cachedResponse.getUsage().getCompletionTokens() : 0;
                        int totalTokens = promptTokens + completionTokens;

                        UsageLog logEntry = UsageLog.builder()
                                .apiKeyId(apiKeyId)
                                .requestId(requestId)
                                .modelAlias(request.getModel())
                                .providerUsed("cache")
                                .actualModel(cachedResponse.getModel() != null ? cachedResponse.getModel() : request.getModel())
                                .promptTokens(promptTokens)
                                .completionTokens(completionTokens)
                                .totalTokens(totalTokens)
                                .costMicroUsd(0L) // 0 cost on cache hit!
                                .latencyMs((int) latencyMs)
                                .timeToFirstTokenMs(0)
                                .cacheHit(cacheHeader)
                                .fallbackUsed(false)
                                .statusCode(200)
                                .build();
                        usageLogRepository.save(logEntry).subscribe();

                        if (request.isStreaming()) {
                            Flux<ServerSentEvent<String>> stream = cacheService.replaySseStream(cachedResponse);
                            return Mono.just(ResponseEntity.ok()
                                    .header("X-Gateway-Cache", cacheHeader)
                                    .contentType(MediaType.TEXT_EVENT_STREAM)
                                    .body(stream));
                        } else {
                            return Mono.just(ResponseEntity.ok()
                                    .header("X-Gateway-Cache", cacheHeader)
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .body(cachedResponse));
                        }
                    }

                    // 4. Cache MISS -> Resolve route & token estimate
                    ResolvedRoute route = routingService.resolveRoute(request.getModel());
                    int promptTokens = tokenizerService.countPromptTokens(request);
                    long estimatedCost = pricingService.estimateMaxCost(
                            route.getPrimaryProvider().getName(),
                            route.getTargetModel(),
                            promptTokens,
                            request.getMaxTokens()
                    );

                    // 5. Reserve-then-settle pre-flight budget reservation
                    return budgetService.reserve(apiKey, estimatedCost)
                            .flatMap(reservedCost -> {
                                if (request.isStreaming()) {
                                    Flux<ServerSentEvent<String>> stream = proxyService.forwardStreaming(
                                            request, apiKey, route, reservedCost, promptTokens);
                                    return Mono.just(ResponseEntity.ok()
                                            .header("X-Gateway-Cache", "MISS")
                                            .contentType(MediaType.TEXT_EVENT_STREAM)
                                            .body(stream));
                                } else {
                                    return proxyService.forwardNonStreaming(
                                            request, apiKey, route, reservedCost, promptTokens)
                                            .map(response -> ResponseEntity.ok()
                                                    .header("X-Gateway-Cache", "MISS")
                                                    .contentType(MediaType.APPLICATION_JSON)
                                                    .body(response));
                                }
                            });
                })
                .onErrorResume(RateLimitException.class, ex -> {
                    Map<String, Object> errorBody = Map.of(
                            "error", Map.of(
                                    "message", ex.getMessage(),
                                    "type", "requests",
                                    "code", "rate_limit_exceeded"
                            )
                    );
                    return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .header(HttpHeaders.RETRY_AFTER, String.valueOf(ex.getRetryAfterSeconds()))
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(errorBody));
                })
                .onErrorResume(InsufficientBudgetException.class, ex -> {
                    Map<String, Object> errorBody = Map.of(
                            "error", Map.of(
                                    "message", ex.getMessage(),
                                    "type", "insufficient_quota",
                                    "code", "budget_exceeded"
                            )
                    );
                    return Mono.just(ResponseEntity.status(HttpStatus.PAYMENT_REQUIRED)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(errorBody));
                });
    }
}
