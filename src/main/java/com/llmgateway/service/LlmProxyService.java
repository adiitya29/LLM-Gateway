package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.dto.*;
import com.llmgateway.model.ApiKey;
import com.llmgateway.model.UsageLog;
import com.llmgateway.provider.LlmProvider;
import com.llmgateway.repository.UsageLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class LlmProxyService {

    private static final Logger log = LoggerFactory.getLogger(LlmProxyService.class);

    private final ModelRoutingService routingService;
    private final ObjectMapper objectMapper;
    private final PricingService pricingService;
    private final TokenizerService tokenizerService;
    private final BudgetService budgetService;
    private final UsageLogRepository usageLogRepository;

    public LlmProxyService(ModelRoutingService routingService,
                           ObjectMapper objectMapper,
                           PricingService pricingService,
                           TokenizerService tokenizerService,
                           BudgetService budgetService,
                           UsageLogRepository usageLogRepository) {
        this.routingService = routingService;
        this.objectMapper = objectMapper;
        this.pricingService = pricingService;
        this.tokenizerService = tokenizerService;
        this.budgetService = budgetService;
        this.usageLogRepository = usageLogRepository;
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

        LlmProvider provider = route.getPrimaryProvider();
        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, route.getTargetModel(), false);
        long startTime = System.currentTimeMillis();
        String requestId = "req_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        log.debug("Routing non-streaming request [{}] -> provider [{}] with target model [{}]",
                request.getModel(), provider.getName(), route.getTargetModel());

        return provider.chat(outboundRequest)
                .map(response -> {
                    response.setModel(request.getModel()); // Guarantee original alias returned to client
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
                        completionTokens = tokenizerService.countTokens(content, route.getTargetModel());
                        response.setUsage(new Usage(promptTokens, completionTokens, promptTokens + completionTokens));
                    }
                    int totalTokens = promptTokens + completionTokens;

                    long actualCost = pricingService.calculateCost(provider.getName(), route.getTargetModel(), promptTokens, completionTokens);
                    UUID apiKeyId = apiKey != null ? apiKey.getId() : null;

                    return budgetService.settle(apiKeyId, reservedCost, actualCost)
                            .doOnSuccess(state -> {
                                UsageLog logEntry = UsageLog.builder()
                                        .apiKeyId(apiKeyId)
                                        .requestId(requestId)
                                        .modelAlias(request.getModel())
                                        .providerUsed(provider.getName())
                                        .actualModel(route.getTargetModel())
                                        .promptTokens(promptTokens)
                                        .completionTokens(completionTokens)
                                        .totalTokens(totalTokens)
                                        .costMicroUsd(actualCost)
                                        .latencyMs((int) latencyMs)
                                        .statusCode(200)
                                        .build();
                                usageLogRepository.save(logEntry).subscribe();
                            })
                            .thenReturn(response);
                })
                .onErrorResume(error -> {
                    UUID apiKeyId = apiKey != null ? apiKey.getId() : null;
                    if (apiKeyId != null && reservedCost > 0) {
                        budgetService.settle(apiKeyId, reservedCost, 0L).subscribe();
                    }
                    return Mono.error(error);
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

        LlmProvider provider = route.getPrimaryProvider();
        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, route.getTargetModel(), true);
        long startTime = System.currentTimeMillis();
        String requestId = "req_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        AtomicLong ttftMs = new AtomicLong(-1);
        AtomicInteger promptTokensRef = new AtomicInteger(estimatedPromptTokens);
        AtomicInteger compTokensRef = new AtomicInteger(0);
        AtomicBoolean usageReported = new AtomicBoolean(false);
        StringBuilder accumulatedContent = new StringBuilder();
        AtomicBoolean settled = new AtomicBoolean(false);

        log.debug("Routing streaming request [{}] -> provider [{}] with target model [{}]",
                request.getModel(), provider.getName(), route.getTargetModel());

        return provider.stream(outboundRequest)
                .map(chunk -> {
                    chunk.setModel(request.getModel()); // Guarantee original alias in chunk
                    return chunk;
                })
                .doOnNext(chunk -> {
                    if (ttftMs.compareAndSet(-1, System.currentTimeMillis() - startTime)) {
                        log.debug("TTFT: {}ms for alias [{}] from [{}]", ttftMs.get(), request.getModel(), provider.getName());
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
                })
                .doFinally(signalType -> {
                    if (settled.compareAndSet(false, true)) {
                        int promptTokens = promptTokensRef.get();
                        int compTokens = compTokensRef.get();
                        if (!usageReported.get()) {
                            compTokens = tokenizerService.countTokens(accumulatedContent.toString(), route.getTargetModel());
                        }
                        int totalTokens = promptTokens + compTokens;
                        long actualCost = pricingService.calculateCost(provider.getName(), route.getTargetModel(), promptTokens, compTokens);
                        long latencyMs = System.currentTimeMillis() - startTime;
                        Integer ttft = ttftMs.get() >= 0 ? (int) ttftMs.get() : null;

                        UUID apiKeyId = apiKey != null ? apiKey.getId() : null;
                        budgetService.settle(apiKeyId, reservedCost, actualCost).subscribe();

                        int statusCode = (signalType == SignalType.CANCEL) ? 499 : (signalType == SignalType.ON_ERROR ? 500 : 200);
                        UsageLog logEntry = UsageLog.builder()
                                .apiKeyId(apiKeyId)
                                .requestId(requestId)
                                .modelAlias(request.getModel())
                                .providerUsed(provider.getName())
                                .actualModel(route.getTargetModel())
                                .promptTokens(promptTokens)
                                .completionTokens(compTokens)
                                .totalTokens(totalTokens)
                                .costMicroUsd(actualCost)
                                .latencyMs((int) latencyMs)
                                .timeToFirstTokenMs(ttft)
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
