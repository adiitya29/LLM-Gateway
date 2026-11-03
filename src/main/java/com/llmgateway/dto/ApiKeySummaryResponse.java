package com.llmgateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiKeySummaryResponse {

    @JsonProperty("id")
    private UUID id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("key_prefix")
    private String keyPrefix;

    @JsonProperty("allowed_models")
    private List<String> allowedModels;

    @JsonProperty("budget_micro_usd")
    private Long budgetMicroUsd;

    @JsonProperty("spent_micro_usd")
    private Long spentMicroUsd;

    @JsonProperty("rate_limit_rpm")
    private Integer rateLimitRpm;

    @JsonProperty("is_active")
    private Boolean isActive;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;
}

