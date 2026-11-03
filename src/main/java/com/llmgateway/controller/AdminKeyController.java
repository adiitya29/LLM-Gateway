package com.llmgateway.controller;

import com.llmgateway.dto.ApiKeyCreateResponse;
import com.llmgateway.dto.ApiKeySummaryResponse;
import com.llmgateway.dto.CreateApiKeyRequest;
import com.llmgateway.service.ApiKeyService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/v1/admin/keys")
public class AdminKeyController {

    private final ApiKeyService apiKeyService;

    public AdminKeyController(ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ApiKeyCreateResponse> createKey(@RequestBody CreateApiKeyRequest request) {
        return apiKeyService.createKey(request);
    }

    @GetMapping
    public Flux<ApiKeySummaryResponse> listKeys() {
        return apiKeyService.listKeys();
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> revokeKey(@PathVariable("id") UUID id) {
        return apiKeyService.revokeKey(id)
                .thenReturn(ResponseEntity.noContent().build());
    }
}

