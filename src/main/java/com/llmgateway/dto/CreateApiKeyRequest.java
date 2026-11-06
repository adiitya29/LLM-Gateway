package com.llmgateway.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateApiKeyRequest {

    @JsonProperty("name")
    private String name;

    @JsonProperty("allowed_models")
    @JsonAlias({"allowedModels", "allowed_models"})
    private List<String> allowedModels;

    @JsonProperty("budget_micro_usd")
    @JsonAlias({"budgetMicroUsd", "budget_micro_usd"})
    private Long budgetMicroUsd;

    @JsonProperty("rate_limit_rpm")
    @JsonAlias({"rateLimitRpm", "rate_limit_rpm"})
    private Integer rateLimitRpm;
}

