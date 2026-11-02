package com.llmgateway.controller;

import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.service.LlmProxyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/v1")
public class ChatCompletionController {

    private static final Logger log = LoggerFactory.getLogger(ChatCompletionController.class);

    private final LlmProxyService proxyService;

    public ChatCompletionController(LlmProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @PostMapping(value = "/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createChatCompletion(@RequestBody ChatCompletionRequest request) {
        log.info("Received chat completion request for model: {}, stream: {}",
                request.getModel(), request.isStreaming());

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

