package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.config.GatewayProperties.RouteTarget;
import com.llmgateway.dto.*;
import com.llmgateway.model.ApiKey;
import com.llmgateway.model.UsageLog;
import com.llmgateway.provider.LlmProvider;
import com.llmgateway.repository.UsageLogRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class LlmProxyService {

    private static final Logger log = LoggerFactory.getLogger(LlmProxyService.class);

    private final ModelRoutingService routingService;
    private final ProviderRegistry providerRegistry;
    private final ObjectMapper objectMapper;
    private final PricingService pricingService;
    private final TokenizerService tokenizerService;
    private final BudgetService budgetService;
    private final UsageLogRepository usageLogRepository;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final GatewayProperties properties;

    public LlmProxyService(ModelRoutingService routingService,
                           ProviderRegistry providerRegistry,
                           ObjectMapper objectMapper,
                           PricingService pricingService,
                           TokenizerService tokenizerService,
                           BudgetService budgetService,
                           UsageLogRepository usageLogRepository,
                           CircuitBreakerRegistry circuitBreakerRegistry,
                           GatewayProperties properties) {
        this.routingService = routingService;
        this.providerRegistry = providerRegistry;
        this.objectMapper = objectMapper;
        this.pricingService = pricingService;
        this.tokenizerService = tokenizerService;
        this.budgetService = budgetService;
        this.usageLogRepository = usageLogRepository;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.properties = properties;
    }

    public Mono<ChatCompletionResponse> forwardNonStreaming(ChatCompletionRequest request) {
        ResolvedRoute route = routingService.resolveRoute(request.getModel());
        int promptTokens = tokenizerService.countPromptTokens(request);
        return forwardNonStreaming(request, null, route, 0L, promptTokens);
    }

    public Mono<ChatCompletionResponse> forwardNonStreaming(
            ChatCompletionRequest request,
            ApiKey apiKey,
            ResolvedRoute route,
            long reservedCost,
            int estimatedPromptTokens) {

        List<RouteTarget> targets = getRouteTargets(route);
        long startTime = System.currentTimeMillis();
        String requestId = "req_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        return executeNonStreamingWithFallback(
                request, apiKey, route, targets, 0, false, reservedCost, estimatedPromptTokens, startTime, requestId
        );
    }

    private Mono<ChatCompletionResponse> executeNonStreamingWithFallback(
            ChatCompletionRequest request,
            ApiKey apiKey,
            ResolvedRoute route,
            List<RouteTarget> targets,
            int targetIndex,
            boolean fallbackUsed,
            long reservedCost,
            int estimatedPromptTokens,
            long startTime,
            String requestId) {

        if (targetIndex >= targets.size()) {
            return Mono.error(new IllegalStateException("No available targets for alias: " + route.getRequestedAlias()));
        }

        RouteTarget currentTarget = targets.get(targetIndex);
        LlmProvider provider = providerRegistry.getProvider(currentTarget.getProvider())
                .orElseThrow(() -> new IllegalStateException("Provider not found: " + currentTarget.getProvider()));
        String targetModel = currentTarget.getModel();

        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker(provider.getName());
        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, targetModel, false);

        log.debug("Executing non-streaming request for alias [{}] via target [{}:{}] (fallback={})",
                route.getRequestedAlias(), provider.getName(), targetModel, fallbackUsed);

        return provider.chat(outboundRequest)
                .transformDeferred(CircuitBreakerOperator.of(cb))
                .timeout(Duration.ofMillis(getReadTimeout(provider.getName())))
                .map(response -> {
                    response.setModel(request.getModel()); // Original alias reflected back to client
                    return response;
                })
                .flatMap(response -> {
                    long latencyMs = System.currentTimeMillis() - startTime;

                    int promptTokens;
                    int completionTokens;
                    if (response.getUsage() != null && response.getUsage().getTotalTokens() > 0) {
                        promptTokens = response.getUsage().getPromptTokens();
                        completionTokens = response.getUsage().getCompletionTokens();
                    } else {
                        promptTokens = estimatedPromptTokens;
                        String content = (response.getChoices() != null && !response.getChoices().isEmpty()
                                && response.getChoices().get(0).getMessage() != null)
                                ? response.getChoices().get(0).getMessage().getContentAsString() : "";
                        completionTokens = tokenizerService.countTokens(content, targetModel);
                        response.setUsage(new Usage(promptTokens, completionTokens, promptTokens + completionTokens));
                    }
                    int totalTokens = promptTokens + completionTokens;

                    long actualCost = pricingService.calculateCost(provider.getName(), targetModel, promptTokens, completionTokens);
                    UUID apiKeyId = apiKey != null ? apiKey.getId() : null;

                    return budgetService.settle(apiKeyId, reservedCost, actualCost)
                            .doOnSuccess(state -> {
                                UsageLog logEntry = UsageLog.builder()
                                        .apiKeyId(apiKeyId)
                                        .requestId(requestId)
                                        .modelAlias(request.getModel())
                                        .providerUsed(provider.getName())
                                        .actualModel(targetModel)
                                        .promptTokens(promptTokens)
                                        .completionTokens(completionTokens)
                                        .totalTokens(totalTokens)
                                        .costMicroUsd(actualCost)
                                        .latencyMs((int) latencyMs)
                                        .fallbackUsed(fallbackUsed)
                                        .statusCode(200)
                                        .build();
                                usageLogRepository.save(logEntry).subscribe();
                            })
                            .thenReturn(response);
                })
                .onErrorResume(err -> {
                    if (isRetryableFailoverError(err) && targetIndex + 1 < targets.size()) {
                        RouteTarget nextTarget = targets.get(targetIndex + 1);
                        log.warn("Target [{}:{}] failed for alias [{}] with [{}]. Initiating failover to next target [{}:{}]...",
                                provider.getName(), targetModel, route.getRequestedAlias(), err.toString(),
                                nextTarget.getProvider(), nextTarget.getModel());
                        return executeNonStreamingWithFallback(
                                request, apiKey, route, targets, targetIndex + 1,
                                true, // fallbackUsed = true
                                reservedCost, estimatedPromptTokens, startTime, requestId
                        );
                    }

                    // If no more targets or unretryable, refund reservation and fail
                    log.error("All fallback targets failed or unretryable error for alias [{}]: {}",
                            route.getRequestedAlias(), err.getMessage());
                    UUID apiKeyId = apiKey != null ? apiKey.getId() : null;
                    if (apiKeyId != null && reservedCost > 0) {
                        budgetService.settle(apiKeyId, reservedCost, 0L).subscribe();
                    }
                    return Mono.error(err);
                });
    }

    public Flux<ServerSentEvent<String>> forwardStreaming(ChatCompletionRequest request) {
        ResolvedRoute route = routingService.resolveRoute(request.getModel());
        int promptTokens = tokenizerService.countPromptTokens(request);
        return forwardStreaming(request, null, route, 0L, promptTokens);
    }

    public Flux<ServerSentEvent<String>> forwardStreaming(
            ChatCompletionRequest request,
            ApiKey apiKey,
            ResolvedRoute route,
            long reservedCost,
            int estimatedPromptTokens) {

        List<RouteTarget> targets = getRouteTargets(route);
        long startTime = System.currentTimeMillis();
        String requestId = "req_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        AtomicBoolean firstTokenEmitted = new AtomicBoolean(false);
        AtomicLong ttftMs = new AtomicLong(-1);
        AtomicReference<RouteTarget> activeTargetRef = new AtomicReference<>(targets.get(0));
        AtomicBoolean fallbackUsedRef = new AtomicBoolean(false);
        AtomicInteger promptTokensRef = new AtomicInteger(estimatedPromptTokens);
        AtomicInteger compTokensRef = new AtomicInteger(0);
        AtomicBoolean usageReported = new AtomicBoolean(false);
        StringBuilder accumulatedContent = new StringBuilder();
        AtomicBoolean settled = new AtomicBoolean(false);

        return executeStreamingWithFallback(
                request, route, targets, 0, false,
                firstTokenEmitted, ttftMs, startTime,
                activeTargetRef, fallbackUsedRef,
                promptTokensRef, compTokensRef,
                usageReported, accumulatedContent
        )
        .doFinally(signalType -> {
            if (settled.compareAndSet(false, true)) {
                RouteTarget winningTarget = activeTargetRef.get() != null ? activeTargetRef.get() : targets.get(0);
                boolean wasFallbackUsed = fallbackUsedRef.get();

                int promptTokens = promptTokensRef.get();
                int compTokens = compTokensRef.get();
                if (!usageReported.get()) {
                    compTokens = tokenizerService.countTokens(accumulatedContent.toString(), winningTarget.getModel());
                }
                int totalTokens = promptTokens + compTokens;
                long actualCost = pricingService.calculateCost(winningTarget.getProvider(), winningTarget.getModel(), promptTokens, compTokens);
                long latencyMs = System.currentTimeMillis() - startTime;
                Integer ttft = ttftMs.get() >= 0 ? (int) ttftMs.get() : null;

                UUID apiKeyId = apiKey != null ? apiKey.getId() : null;
                budgetService.settle(apiKeyId, reservedCost, actualCost).subscribe();

                int statusCode = (signalType == SignalType.CANCEL) ? 499 : (signalType == SignalType.ON_ERROR ? 500 : 200);
                UsageLog logEntry = UsageLog.builder()
                        .apiKeyId(apiKeyId)
                        .requestId(requestId)
                        .modelAlias(request.getModel())
                        .providerUsed(winningTarget.getProvider())
                        .actualModel(winningTarget.getModel())
                        .promptTokens(promptTokens)
                        .completionTokens(compTokens)
                        .totalTokens(totalTokens)
                        .costMicroUsd(actualCost)
                        .latencyMs((int) latencyMs)
                        .timeToFirstTokenMs(ttft)
                        .fallbackUsed(wasFallbackUsed)
                        .statusCode(statusCode)
                        .build();
                usageLogRepository.save(logEntry).subscribe();
            }
        })
        .map(chunk -> {
            try {
                String json = objectMapper.writeValueAsString(chunk);
                return ServerSentEvent.<String>builder().data(json).build();
            } catch (Exception e) {
                log.error("Failed to serialize SSE chunk: {}", e.getMessage());
                throw new RuntimeException("SSE serialization error", e);
            }
        })
        .concatWith(Mono.just(ServerSentEvent.<String>builder().data("[DONE]").build()));
    }

    private Flux<ChatCompletionChunk> executeStreamingWithFallback(
            ChatCompletionRequest request,
            ResolvedRoute route,
            List<RouteTarget> targets,
            int targetIndex,
            boolean fallbackUsed,
            AtomicBoolean firstTokenEmitted,
            AtomicLong ttftMs,
            long startTime,
            AtomicReference<RouteTarget> activeTargetRef,
            AtomicBoolean fallbackUsedRef,
            AtomicInteger promptTokensRef,
            AtomicInteger compTokensRef,
            AtomicBoolean usageReported,
            StringBuilder accumulatedContent) {

        if (targetIndex >= targets.size()) {
            return Flux.error(new IllegalStateException("No available targets for alias: " + route.getRequestedAlias()));
        }

        RouteTarget currentTarget = targets.get(targetIndex);
        LlmProvider provider = providerRegistry.getProvider(currentTarget.getProvider())
                .orElseThrow(() -> new IllegalStateException("Provider not found: " + currentTarget.getProvider()));
        String targetModel = currentTarget.getModel();

        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker(provider.getName());
        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, targetModel, true);

        log.debug("Executing streaming request for alias [{}] via target [{}:{}] (fallback={})",
                route.getRequestedAlias(), provider.getName(), targetModel, fallbackUsed);

        Flux<ChatCompletionChunk> chunkFlux = provider.stream(outboundRequest)
                .transformDeferred(CircuitBreakerOperator.of(cb))
                .map(chunk -> {
                    chunk.setModel(request.getModel()); // Original alias back to client
                    return chunk;
                })
                .doOnNext(chunk -> {
                    // Lock provider upon receiving the first valid chunk
                    if (firstTokenEmitted.compareAndSet(false, true)) {
                        long ttft = System.currentTimeMillis() - startTime;
                        ttftMs.set(ttft);
                        activeTargetRef.set(currentTarget);
                        fallbackUsedRef.set(fallbackUsed);
                        log.debug("First token received! Provider locked to [{}:{}] for alias [{}], TTFT: {}ms",
                                provider.getName(), targetModel, route.getRequestedAlias(), ttft);
                    }

                    if (chunk.getChoices() != null) {
                        for (StreamChoice sc : chunk.getChoices()) {
                            if (sc.getDelta() != null && sc.getDelta().getContent() != null) {
                                accumulatedContent.append(sc.getDelta().getContent());
                            }
                        }
                    }
                    if (chunk.getUsage() != null && chunk.getUsage().getTotalTokens() > 0) {
                        usageReported.set(true);
                        promptTokensRef.set(chunk.getUsage().getPromptTokens());
                        compTokensRef.set(chunk.getUsage().getCompletionTokens());
                    }
                });

        return chunkFlux.onErrorResume(err -> {
            if (firstTokenEmitted.get()) {
                // Post-stream error: first token was already sent to client, do NOT corrupt stream with failover
                log.error("Stream failed after first token emitted on provider [{}:{}] for alias [{}]: {}",
                        provider.getName(), targetModel, route.getRequestedAlias(), err.getMessage());
                return Flux.error(err);
            }

            // Pre-stream error: safe to failover to next configured target
            if (isRetryableFailoverError(err) && targetIndex + 1 < targets.size()) {
                RouteTarget nextTarget = targets.get(targetIndex + 1);
                log.warn("Pre-stream error from target [{}:{}] for alias [{}] with [{}]. Initiating failover to next target [{}:{}]...",
                        provider.getName(), targetModel, route.getRequestedAlias(), err.toString(),
                        nextTarget.getProvider(), nextTarget.getModel());
                return executeStreamingWithFallback(
                        request, route, targets, targetIndex + 1,
                        true, // fallbackUsed = true
                        firstTokenEmitted, ttftMs, startTime,
                        activeTargetRef, fallbackUsedRef,
                        promptTokensRef, compTokensRef,
                        usageReported, accumulatedContent
                );
            }

            log.error("Stream failed before first token and no more fallback targets available for alias [{}]: {}",
                    route.getRequestedAlias(), err.getMessage());
            return Flux.error(err);
        });
    }

    private List<RouteTarget> getRouteTargets(ResolvedRoute route) {
        if (route.getAllTargets() != null && !route.getAllTargets().isEmpty()) {
            return route.getAllTargets();
        }
        return List.of(new RouteTarget(route.getPrimaryProvider().getName(), route.getTargetModel()));
    }

    private boolean isRetryableFailoverError(Throwable t) {
        if (t == null) {
            return false;
        }
        if (t instanceof CallNotPermittedException) {
            return true;
        }
        if (t instanceof TimeoutException) {
            return true;
        }
        if (t instanceof WebClientRequestException) {
            return true;
        }
        if (t instanceof WebClientResponseException wcre) {
            int status = wcre.getStatusCode().value();
            return status == 429 || status == 408 || (status >= 500 && status < 600);
        }
        if (t instanceof IOException) {
            return true;
        }
        if (t.getCause() != null && t.getCause() != t) {
            return isRetryableFailoverError(t.getCause());
        }
        return false;
    }

    private int getReadTimeout(String providerName) {
        if (properties.getProviders() != null && properties.getProviders().containsKey(providerName)) {
            return properties.getProviders().get(providerName).getReadTimeoutMs();
        }
        return properties.getUpstream().getReadTimeoutMs();
    }

    private ChatCompletionRequest copyWithTargetModel(ChatCompletionRequest source, String targetModel, boolean forceIncludeUsage) {
        StreamOptions streamOptions = source.getStreamOptions();
        if (forceIncludeUsage && streamOptions == null) {
            streamOptions = StreamOptions.builder().includeUsage(true).build();
        }

        return ChatCompletionRequest.builder()
                .model(targetModel)
                .messages(source.getMessages())
                .temperature(source.getTemperature())
                .topP(source.getTopP())
                .n(source.getN())
                .stream(source.getStream())
                .streamOptions(streamOptions)
                .maxTokens(source.getMaxTokens())
                .presencePenalty(source.getPresencePenalty())
                .frequencyPenalty(source.getFrequencyPenalty())
                .user(source.getUser())
                .responseFormat(source.getResponseFormat())
                .stop(source.getStop())
                .build();
    }
}
