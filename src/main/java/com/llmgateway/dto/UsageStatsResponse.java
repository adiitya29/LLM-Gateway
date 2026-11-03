package com.llmgateway.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsageStatsResponse {
    @JsonProperty("api_key_id")
    private UUID apiKeyId;

    @JsonProperty("key_prefix")
    private String keyPrefix;

    @JsonProperty("budget_micro_usd")
    private long budgetMicroUsd;

    @JsonProperty("spent_micro_usd")
    private long spentMicroUsd;

    @JsonProperty("remaining_micro_usd")
    private long remainingMicroUsd;

    @JsonProperty("budget_usd")
    private double budgetUsd;

    @JsonProperty("spent_usd")
    private double spentUsd;

    @JsonProperty("remaining_usd")
    private double remainingUsd;

    @JsonProperty("total_requests")
    private long totalRequests;

    @JsonProperty("total_prompt_tokens")
    private long totalPromptTokens;

    @JsonProperty("total_completion_tokens")
    private long totalCompletionTokens;

    @JsonProperty("total_tokens")
    private long totalTokens;
}

