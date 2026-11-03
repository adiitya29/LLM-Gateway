package com.llmgateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiKey {

    private UUID id;
    private String name;
    private String keyPrefix;
    private String keyHash;
    @Builder.Default
    private boolean isActive = true;
    @Builder.Default
    private List<String> allowedModels = new ArrayList<>();
    @Builder.Default
    private long budgetMicroUsd = 10000000L; // $10.00 default
    @Builder.Default
    private long spentMicroUsd = 0L;
    @Builder.Default
    private int rateLimitRpm = 60;
    @Builder.Default
    private Instant createdAt = Instant.now();
    @Builder.Default
    private Instant updatedAt = Instant.now();

    /**
     * Checks if the requested model or alias is authorized for this API key.
     * Empty or null allowedModels means all models are allowed.
     */
    public boolean isModelAllowed(String requestedModel) {
        if (allowedModels == null || allowedModels.isEmpty()) {
            return true;
        }
        if (requestedModel == null) {
            return false;
        }
        return allowedModels.stream()
                .anyMatch(allowed -> allowed.equalsIgnoreCase(requestedModel.trim()));
    }
}

