package com.llmgateway.service;

import com.llmgateway.model.ModelPricing;
import com.llmgateway.repository.ModelPricingRepository;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PricingService {

    private static final Logger log = LoggerFactory.getLogger(PricingService.class);

    private final ModelPricingRepository pricingRepository;
    // Cache: provider:model -> ModelPricing
    private final Map<String, ModelPricing> pricingCache = new ConcurrentHashMap<>();

    private static final int DEFAULT_ESTIMATED_MAX_TOKENS = 1000;

    public PricingService(ModelPricingRepository pricingRepository, @Autowired(required = false) Flyway flyway) {
        this.pricingRepository = pricingRepository;
    }

    @PostConstruct
    public void init() {
        refreshPricing().subscribe();
    }

    public Mono<Void> refreshPricing() {
        return pricingRepository.findAllActive()
                .doOnNext(pricing -> {
                    String key = cacheKey(pricing.getProvider(), pricing.getModelName());
                    pricingCache.put(key, pricing);
                    log.info("Loaded pricing: [{}] -> prompt: {} micro-USD/1M, comp: {} micro-USD/1M",
                            key, pricing.getPromptTokenMicroUsd(), pricing.getCompletionTokenMicroUsd());
                })
                .then();
    }

    public ModelPricing getPricing(String provider, String model) {
        if (provider == null || model == null) {
            return null;
        }
        ModelPricing exact = pricingCache.get(cacheKey(provider, model));
        if (exact != null) {
            return exact;
        }

        // Fuzzy match if model has suffixes or aliases
        for (Map.Entry<String, ModelPricing> entry : pricingCache.entrySet()) {
            if (entry.getKey().startsWith(provider.toLowerCase() + ":") &&
                    model.toLowerCase().contains(entry.getValue().getModelName().toLowerCase())) {
                return entry.getValue();
            }
        }

        // Fallback default pricing for active Groq models if pricing cache is not loaded yet
        if ("groq".equalsIgnoreCase(provider)) {
            return ModelPricing.builder()
                    .provider("groq")
                    .modelName(model)
                    .promptTokenMicroUsd(150_000)
                    .completionTokenMicroUsd(600_000)
                    .isActive(true)
                    .build();
        }

        return null;
    }

    /**
     * Calculates actual cost in micro-USD:
     * micro-USD = (prompt_tokens * p_rate + comp_tokens * c_rate) / 1,000,000
     */
    public long calculateCost(String provider, String model, int promptTokens, int completionTokens) {
        ModelPricing pricing = getPricing(provider, model);
        if (pricing == null) {
            // Default 0 for mock/unknown free tier
            return 0L;
        }

        double rawCost = ((double) promptTokens * pricing.getPromptTokenMicroUsd()
                + (double) completionTokens * pricing.getCompletionTokenMicroUsd()) / 1_000_000.0;

        return (long) Math.ceil(rawCost);
    }

    /**
     * Estimates maximum cost before calling upstream for reserve-then-settle pattern.
     */
    public long estimateMaxCost(String provider, String model, int estimatedPromptTokens, Integer maxTokens) {
        ModelPricing pricing = getPricing(provider, model);
        if (pricing == null) {
            return 0L;
        }

        int compTokens = (maxTokens != null && maxTokens > 0) ? maxTokens : DEFAULT_ESTIMATED_MAX_TOKENS;
        double rawCost = ((double) estimatedPromptTokens * pricing.getPromptTokenMicroUsd()
                + (double) compTokens * pricing.getCompletionTokenMicroUsd()) / 1_000_000.0;

        return Math.max(1L, (long) Math.ceil(rawCost));
    }

    private String cacheKey(String provider, String model) {
        return (provider.trim().toLowerCase() + ":" + model.trim().toLowerCase());
    }
}

