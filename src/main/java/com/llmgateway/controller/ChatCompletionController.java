package com.llmgateway.controller;

import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.model.ApiKey;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.LlmProxyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@RestController
@RequestMapping("/v1")
public class ChatCompletionController {

    private static final Logger log = LoggerFactory.getLogger(ChatCompletionController.class);

    private final LlmProxyService proxyService;

    public ChatCompletionController(LlmProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @PostMapping(value = "/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createChatCompletion(
            @RequestBody ChatCompletionRequest request,
            ServerWebExchange exchange) {
        log.info("Received chat completion request for model: {}, stream: {}",
                request.getModel(), request.isStreaming());

        // Model Authorization check against authenticated API key
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
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Mono.just(errorBody));
        }

        if (request.isStreaming()) {
            Flux<ServerSentEvent<String>> stream = proxyService.forwardStreaming(request);
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .body(stream);
        } else {
            Mono<ChatCompletionResponse> response = proxyService.forwardNonStreaming(request);
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(response);
        }
    }
}
