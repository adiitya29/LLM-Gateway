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
public class ModelPricing {
    private UUID id;
    private String provider;
    private String modelName;
    private int promptTokenMicroUsd;
    private int completionTokenMicroUsd;
    @Builder.Default
    private boolean isActive = true;
    @Builder.Default
    private Instant updatedAt = Instant.now();
}

