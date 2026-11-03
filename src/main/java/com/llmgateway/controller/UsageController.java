package com.llmgateway.controller;

import com.llmgateway.dto.UsageStatsResponse;
import com.llmgateway.model.ApiKey;
import com.llmgateway.repository.UsageLogRepository;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.BudgetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/v1")
public class UsageController {

    private static final Logger log = LoggerFactory.getLogger(UsageController.class);

    private final BudgetService budgetService;
    private final UsageLogRepository usageLogRepository;

    public UsageController(BudgetService budgetService, UsageLogRepository usageLogRepository) {
        this.budgetService = budgetService;
        this.usageLogRepository = usageLogRepository;
    }

    @GetMapping(value = "/usage", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<?>> getUsage(ServerWebExchange exchange) {
        ApiKey apiKey = exchange.getAttribute(GatewayAuthFilter.API_KEY_ATTR);
        if (apiKey == null) {
            Map<String, Object> error = Map.of(
                    "error", Map.of(
                            "message", "Authentication required to query usage",
                            "type", "authentication_error",
                            "code", "unauthorized"
                    )
            );
            return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(error));
        }

        return budgetService.getBudgetState(apiKey)
                .zipWith(usageLogRepository.getAggregateStats(apiKey.getId()))
                .map(tuple -> {
                    BudgetService.BudgetState budgetState = tuple.getT1();
                    UsageLogRepository.AggregateStats stats = tuple.getT2();

                    long budgetMicro = budgetState.budgetMicroUsd();
                    long spentMicro = budgetState.spentMicroUsd();
                    long remainingMicro = budgetState.remainingMicroUsd();

                    UsageStatsResponse response = UsageStatsResponse.builder()
                            .apiKeyId(apiKey.getId())
                            .keyPrefix(apiKey.getKeyPrefix())
                            .budgetMicroUsd(budgetMicro)
                            .spentMicroUsd(spentMicro)
                            .remainingMicroUsd(remainingMicro)
                            .budgetUsd(budgetMicro / 1_000_000.0)
                            .spentUsd(spentMicro / 1_000_000.0)
                            .remainingUsd(remainingMicro / 1_000_000.0)
                            .totalRequests(stats.totalRequests())
                            .totalPromptTokens(stats.totalPromptTokens())
                            .totalCompletionTokens(stats.totalCompletionTokens())
                            .totalTokens(stats.totalTokens())
                            .build();

                    return ResponseEntity.ok(response);
                });
    }
}

