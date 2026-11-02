package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.dto.ChatCompletionChunk;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.dto.StreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class LlmProxyService {

    private static final Logger log = LoggerFactory.getLogger(LlmProxyService.class);
    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE = new ParameterizedTypeReference<>() {};

    private final WebClient webClient;
    private final GatewayProperties properties;
    private final ObjectMapper objectMapper;

    public LlmProxyService(WebClient.Builder webClientBuilder, GatewayProperties properties, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder.build();
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * Non-streaming proxy call returning full JSON response.
     */
    public Mono<ChatCompletionResponse> forwardNonStreaming(ChatCompletionRequest request) {
        String targetUrl = resolveChatCompletionsUrl(properties.getUpstream().getBaseUrl());
        log.debug("Forwarding non-streaming request to upstream: {}", targetUrl);

        return webClient.post()
                .uri(targetUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .headers(this::applyAuthHeaders)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(ChatCompletionResponse.class)
                .doOnSuccess(resp -> {
                    if (resp != null && resp.getUsage() != null) {
                        log.debug("Non-streaming call completed. Tokens: prompt={}, completion={}, total={}",
                                resp.getUsage().getPromptTokens(),
                                resp.getUsage().getCompletionTokens(),
                                resp.getUsage().getTotalTokens());
                    }
                })
                .doOnError(err -> log.error("Upstream non-streaming call failed: {}", err.getMessage()));
    }

    /**
     * Streaming proxy call emitting ServerSentEvents with zero buffering and reactive backpressure.
     */
    public Flux<ServerSentEvent<String>> forwardStreaming(ChatCompletionRequest request) {
        String targetUrl = resolveChatCompletionsUrl(properties.getUpstream().getBaseUrl());
        log.debug("Forwarding streaming request to upstream: {}", targetUrl);

        ChatCompletionRequest preparedRequest = prepareStreamingRequest(request);
        AtomicBoolean firstChunkReceived = new AtomicBoolean(false);

        return webClient.post()
                .uri(targetUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .headers(this::applyAuthHeaders)
                .bodyValue(preparedRequest)
                .retrieve()
                .bodyToFlux(SSE_TYPE)
                .doOnNext(sse -> {
                    if (firstChunkReceived.compareAndSet(false, true)) {
                        log.debug("First SSE chunk received from upstream for model: {}", request.getModel());
                    }
                    tapStreamChunk(sse.data());
                })
                .doOnCancel(() -> log.info("Downstream client disconnected. Canceling upstream stream for model: {}", request.getModel()))
                .doOnError(err -> log.error("Upstream streaming call encountered error: {}", err.getMessage()))
                .doOnComplete(() -> log.debug("Upstream streaming finished cleanly for model: {}", request.getModel()));
    }

    private void applyAuthHeaders(HttpHeaders headers) {
        String apiKey = properties.getUpstream().getApiKey();
        if (apiKey != null && !apiKey.isBlank()) {
            headers.setBearerAuth(apiKey);
        }
    }

    private ChatCompletionRequest prepareStreamingRequest(ChatCompletionRequest source) {
        StreamOptions streamOptions = source.getStreamOptions();
        if (streamOptions == null) {
            streamOptions = StreamOptions.builder().includeUsage(true).build();
        } else if (streamOptions.getIncludeUsage() == null) {
            streamOptions.setIncludeUsage(true);
        }

        return ChatCompletionRequest.builder()
                .model(source.getModel())
                .messages(source.getMessages())
                .temperature(source.getTemperature())
                .topP(source.getTopP())
                .n(source.getN())
                .stream(true)
                .streamOptions(streamOptions)
                .maxTokens(source.getMaxTokens())
                .presencePenalty(source.getPresencePenalty())
                .frequencyPenalty(source.getFrequencyPenalty())
                .user(source.getUser())
                .responseFormat(source.getResponseFormat())
                .stop(source.getStop())
                .build();
    }

    /**
     * Side-channel tap to inspect chunk usage and tokens without interfering with reactive stream.
     */
    private void tapStreamChunk(String data) {
        if (data == null || data.isBlank() || "[DONE]".equals(data.trim())) {
            return;
        }

        try {
            ChatCompletionChunk chunk = objectMapper.readValue(data, ChatCompletionChunk.class);
            if (chunk.getUsage() != null) {
                log.debug("Stream usage reported: prompt={}, completion={}, total={}",
                        chunk.getUsage().getPromptTokens(),
                        chunk.getUsage().getCompletionTokens(),
                        chunk.getUsage().getTotalTokens());
            }
        } catch (Exception e) {
            // Ignore side-channel deserialization exceptions so the stream is not broken
            log.trace("Non-fatal side channel parse: {}", e.getMessage());
        }
    }

    private String resolveChatCompletionsUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isBlank()) {
            return "http://localhost:8089/v1/chat/completions";
        }
        String trimmed = baseUrl.trim();
        if (trimmed.endsWith("/chat/completions")) {
            return trimmed;
        }
        if (trimmed.endsWith("/")) {
            return trimmed + "chat/completions";
        }
        return trimmed + "/chat/completions";
    }
}

