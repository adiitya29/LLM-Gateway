package com.llmgateway.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Adapter for any upstream provider exposing an OpenAI-compatible API
 * (Google Gemini OpenAI endpoint, Groq, OpenAI, Mistral, WireMock, etc.).
 */
public class OpenAiCompatibleProvider implements LlmProvider {

    private static final Logger log = LoggerFactory.getLogger(OpenAiCompatibleProvider.class);
    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE = new ParameterizedTypeReference<>() {};

    private final String name;
    private final String baseUrl;
    private final String apiKey;
    private final Map<String, String> customHeaders;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public OpenAiCompatibleProvider(
            String name,
            String baseUrl,
            String apiKey,
            Map<String, String> customHeaders,
            WebClient.Builder webClientBuilder,
            ObjectMapper objectMapper) {
        this.name = name;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.customHeaders = customHeaders != null ? customHeaders : Map.of();
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Mono<ChatCompletionResponse> chat(ChatCompletionRequest request) {
        String targetUrl = resolveChatCompletionsUrl(baseUrl);
        log.debug("Provider [{}] executing non-streaming chat completion at: {}", name, targetUrl);

        return Mono.deferContextual(contextView -> {
            String requestId = contextView.getOrDefault("X-Request-Id", null);
            return webClient.post()
                    .uri(targetUrl)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .headers(headers -> {
                        applyHeaders(headers);
                        if (requestId != null && !requestId.isBlank()) {
                            headers.set("X-Request-Id", requestId);
                        }
                    })
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(ChatCompletionResponse.class)
                    .map(response -> {
                        // Ensure the requested model/alias is reflected back to the client
                        if (request.getModel() != null) {
                            response.setModel(request.getModel());
                        }
                        return response;
                    })
                    .doOnError(err -> log.error("Provider [{}] chat call failed: {}", name, err.getMessage()));
        });
    }

    @Override
    public Flux<ChatCompletionChunk> stream(ChatCompletionRequest request) {
        String targetUrl = resolveChatCompletionsUrl(baseUrl);
        log.debug("Provider [{}] executing streaming chat completion at: {}", name, targetUrl);

        ChatCompletionRequest preparedRequest = prepareStreamingRequest(request);

        return Flux.deferContextual(contextView -> {
            String requestId = contextView.getOrDefault("X-Request-Id", null);
            return webClient.post()
                    .uri(targetUrl)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.TEXT_EVENT_STREAM)
                    .headers(headers -> {
                        applyHeaders(headers);
                        if (requestId != null && !requestId.isBlank()) {
                            headers.set("X-Request-Id", requestId);
                        }
                    })
                    .bodyValue(preparedRequest)
                    .retrieve()
                    .bodyToFlux(SSE_TYPE)
                    .takeWhile(sse -> sse.data() != null && !"[DONE]".equals(sse.data().trim()))
                    .flatMap(sse -> {
                        String data = sse.data().trim();
                        if (data.isBlank()) {
                            return Mono.empty();
                        }
                        try {
                            ChatCompletionChunk chunk = objectMapper.readValue(data, ChatCompletionChunk.class);
                            if (request.getModel() != null) {
                                chunk.setModel(request.getModel());
                            }
                            return Mono.just(chunk);
                        } catch (Exception e) {
                            log.warn("Provider [{}] failed to parse chunk: {}", name, e.getMessage());
                            return Mono.empty();
                        }
                    })
                    .doOnError(err -> log.error("Provider [{}] stream call encountered error: {}", name, err.getMessage()));
        });
    }

    private void applyHeaders(HttpHeaders headers) {
        if (apiKey != null && !apiKey.isBlank()) {
            headers.setBearerAuth(apiKey);
        }
        customHeaders.forEach(headers::set);
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

    private String resolveChatCompletionsUrl(String url) {
        if (url == null || url.isBlank()) {
            return "http://localhost:8089/v1/chat/completions";
        }
        String trimmed = url.trim();
        if (trimmed.endsWith("/chat/completions")) {
            return trimmed;
        }
        if (trimmed.endsWith("/")) {
            return trimmed + "chat/completions";
        }
        return trimmed + "/chat/completions";
    }
}

