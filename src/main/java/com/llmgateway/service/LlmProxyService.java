package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.dto.ChatCompletionChunk;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.provider.LlmProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class LlmProxyService {

    private static final Logger log = LoggerFactory.getLogger(LlmProxyService.class);

    private final ModelRoutingService routingService;
    private final ObjectMapper objectMapper;

    public LlmProxyService(ModelRoutingService routingService, ObjectMapper objectMapper) {
        this.routingService = routingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Resolves the route for the requested model/alias and executes non-streaming completion.
     */
    public Mono<ChatCompletionResponse> forwardNonStreaming(ChatCompletionRequest request) {
        ResolvedRoute route = routingService.resolveRoute(request.getModel());
        LlmProvider provider = route.getPrimaryProvider();

        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, route.getTargetModel());
        log.debug("Routing non-streaming request [{}] -> provider [{}] with target model [{}]",
                request.getModel(), provider.getName(), route.getTargetModel());

        return provider.chat(outboundRequest)
                .map(response -> {
                    response.setModel(request.getModel()); // Guarantee original alias returned to client
                    return response;
                })
                .doOnSuccess(resp -> {
                    if (resp != null && resp.getUsage() != null) {
                        log.debug("Call completed via [{}]. Tokens: prompt={}, completion={}, total={}",
                                provider.getName(),
                                resp.getUsage().getPromptTokens(),
                                resp.getUsage().getCompletionTokens(),
                                resp.getUsage().getTotalTokens());
                    }
                });
    }

    /**
     * Resolves the route and streams typed domain chunks.
     */
    public Flux<ChatCompletionChunk> forwardStreamingChunks(ChatCompletionRequest request) {
        ResolvedRoute route = routingService.resolveRoute(request.getModel());
        LlmProvider provider = route.getPrimaryProvider();

        ChatCompletionRequest outboundRequest = copyWithTargetModel(request, route.getTargetModel());
        log.debug("Routing streaming request [{}] -> provider [{}] with target model [{}]",
                request.getModel(), provider.getName(), route.getTargetModel());

        AtomicBoolean firstChunkReceived = new AtomicBoolean(false);

        return provider.stream(outboundRequest)
                .map(chunk -> {
                    chunk.setModel(request.getModel()); // Guarantee original alias in chunk
                    return chunk;
                })
                .doOnNext(chunk -> {
                    if (firstChunkReceived.compareAndSet(false, true)) {
                        log.debug("First chunk received from provider [{}] for alias [{}]",
                                provider.getName(), request.getModel());
                    }
                })
                .doOnCancel(() -> log.info("Client disconnected from stream for alias [{}]", request.getModel()))
                .doOnComplete(() -> log.debug("Streaming finished from provider [{}] for alias [{}]",
                        provider.getName(), request.getModel()));
    }

    /**
     * Formats typed chunks into wire-compliant OpenAI SSE stream ending with [DONE].
     */
    public Flux<ServerSentEvent<String>> forwardStreaming(ChatCompletionRequest request) {
        return forwardStreamingChunks(request)
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

    private ChatCompletionRequest copyWithTargetModel(ChatCompletionRequest source, String targetModel) {
        return ChatCompletionRequest.builder()
                .model(targetModel)
                .messages(source.getMessages())
                .temperature(source.getTemperature())
                .topP(source.getTopP())
                .n(source.getN())
                .stream(source.getStream())
                .streamOptions(source.getStreamOptions())
                .maxTokens(source.getMaxTokens())
                .presencePenalty(source.getPresencePenalty())
                .frequencyPenalty(source.getFrequencyPenalty())
                .user(source.getUser())
                .responseFormat(source.getResponseFormat())
                .stop(source.getStop())
                .build();
    }
}
