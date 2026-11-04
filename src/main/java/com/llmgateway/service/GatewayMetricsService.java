package com.llmgateway.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class GatewayMetricsService {

    private final MeterRegistry meterRegistry;

    public GatewayMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Records request count and end-to-end latency.
     * Metrics:
     * - llm_gateway_requests_total (tags: provider, model, status, cache_hit)
     * - llm_gateway_latency_seconds (tags: provider, model, status, cache_hit)
     */
    public void recordRequest(String provider, String model, int statusCode, String cacheHit, Duration latency) {
        String safeProvider = (provider != null && !provider.isBlank()) ? provider : "unknown";
        String safeModel = (model != null && !model.isBlank()) ? model : "unknown";
        String safeCacheHit = (cacheHit != null && !cacheHit.isBlank()) ? cacheHit : "NONE";

        Counter.builder("llm_gateway_requests_total")
                .description("Total number of LLM gateway chat completion requests")
                .tag("provider", safeProvider)
                .tag("model", safeModel)
                .tag("status", String.valueOf(statusCode))
                .tag("cache_hit", safeCacheHit)
                .register(meterRegistry)
                .increment();

        if (latency != null) {
            Timer.builder("llm_gateway_latency_seconds")
                    .description("End-to-end request latency in seconds")
                    .tag("provider", safeProvider)
                    .tag("model", safeModel)
                    .tag("status", String.valueOf(statusCode))
                    .tag("cache_hit", safeCacheHit)
                    .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                    .register(meterRegistry)
                    .record(latency);
        }
    }

    /**
     * Records streaming time to first token (TTFT).
     * Metric: llm_gateway_time_to_first_token_seconds (distribution summary)
     */
    public void recordTtft(String provider, String model, Duration ttft) {
        if (ttft == null || ttft.isNegative()) {
            return;
        }
        String safeProvider = (provider != null && !provider.isBlank()) ? provider : "unknown";
        String safeModel = (model != null && !model.isBlank()) ? model : "unknown";

        DistributionSummary.builder("llm_gateway_time_to_first_token_seconds")
                .description("Time to first token in seconds for streaming responses")
                .baseUnit("seconds")
                .tag("provider", safeProvider)
                .tag("model", safeModel)
                .publishPercentiles(0.5, 0.9, 0.99)
                .register(meterRegistry)
                .record(ttft.toMillis() / 1000.0);
    }

    /**
     * Records token usage for prompt and completion.
     * Metric: llm_gateway_tokens_total (tags: type=prompt/completion, provider, model)
     */
    public void recordTokens(String provider, String model, int promptTokens, int completionTokens) {
        String safeProvider = (provider != null && !provider.isBlank()) ? provider : "unknown";
        String safeModel = (model != null && !model.isBlank()) ? model : "unknown";

        if (promptTokens > 0) {
            Counter.builder("llm_gateway_tokens_total")
                    .description("Total number of tokens processed")
                    .tag("type", "prompt")
                    .tag("provider", safeProvider)
                    .tag("model", safeModel)
                    .register(meterRegistry)
                    .increment(promptTokens);
        }
        if (completionTokens > 0) {
            Counter.builder("llm_gateway_tokens_total")
                    .description("Total number of tokens processed")
                    .tag("type", "completion")
                    .tag("provider", safeProvider)
                    .tag("model", safeModel)
                    .register(meterRegistry)
                    .increment(completionTokens);
        }
    }

    /**
     * Records micro-USD cost billed to API keys.
     * Metric: llm_gateway_cost_micro_usd_total (tags: api_key, provider, model)
     */
    public void recordCost(String apiKeyPrefix, String provider, String model, long costMicroUsd) {
        if (costMicroUsd <= 0) {
            return;
        }
        String safeKey = (apiKeyPrefix != null && !apiKeyPrefix.isBlank()) ? apiKeyPrefix : "anonymous";
        String safeProvider = (provider != null && !provider.isBlank()) ? provider : "unknown";
        String safeModel = (model != null && !model.isBlank()) ? model : "unknown";

        Counter.builder("llm_gateway_cost_micro_usd_total")
                .description("Cumulative cost in micro-USD billed to API keys")
                .tag("api_key", safeKey)
                .tag("provider", safeProvider)
                .tag("model", safeModel)
                .register(meterRegistry)
                .increment(costMicroUsd);
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
}

