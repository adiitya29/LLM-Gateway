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
public class UsageLog {
    private UUID id;
    private UUID apiKeyId;
    private String requestId;
    private String modelAlias;
    private String providerUsed;
    private String actualModel;
    @Builder.Default
    private int promptTokens = 0;
    @Builder.Default
    private int completionTokens = 0;
    @Builder.Default
    private int totalTokens = 0;
    @Builder.Default
    private long costMicroUsd = 0L;
    @Builder.Default
    private int latencyMs = 0;
    private Integer timeToFirstTokenMs;
    @Builder.Default
    private String cacheHit = "NONE";
    @Builder.Default
    private boolean fallbackUsed = false;
    @Builder.Default
    private int statusCode = 200;
    @Builder.Default
    private Instant createdAt = Instant.now();
}

