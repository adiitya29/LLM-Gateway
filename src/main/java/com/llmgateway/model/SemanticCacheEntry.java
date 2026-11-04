package com.llmgateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SemanticCacheEntry {
    private UUID id;
    private String scopeHash;
    private String promptText;
    private String responseJson;
    private Instant createdAt;
    private Instant expiresAt;
    private Double similarity;
}

