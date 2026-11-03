package com.llmgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.provider.LlmProvider;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ProviderRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProviderRegistry.class);

    private final GatewayProperties properties;
    private final WebClient.Builder webClientBuilder;
    private final ObjectMapper objectMapper;
    private final Map<String, LlmProvider> providers = new ConcurrentHashMap<>();

    public ProviderRegistry(GatewayProperties properties, WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.properties = properties;
        this.webClientBuilder = webClientBuilder;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        // 1. Initialize providers configured under gateway.providers
        if (properties.getProviders() != null && !properties.getProviders().isEmpty()) {
            properties.getProviders().forEach((name, config) -> {
                OpenAiCompatibleProvider provider = new OpenAiCompatibleProvider(
                        name,
                        config.getBaseUrl(),
                        config.getApiKey(),
                        config.getHeaders(),
                        webClientBuilder,
                        objectMapper
                );
                registerProvider(provider);
            });
        }

        // 2. Fallback: if no providers are defined, register a default provider from gateway.upstream
        if (providers.isEmpty() && properties.getUpstream() != null) {
            log.info("No named providers configured. Registering default provider from gateway.upstream");
            OpenAiCompatibleProvider defaultProvider = new OpenAiCompatibleProvider(
                    "default",
                    properties.getUpstream().getBaseUrl(),
                    properties.getUpstream().getApiKey(),
                    Map.of(),
                    webClientBuilder,
                    objectMapper
            );
            registerProvider(defaultProvider);
        }

        log.info("Initialized ProviderRegistry with {} providers: {}", providers.size(), providers.keySet());
    }

    public void registerProvider(LlmProvider provider) {
        providers.put(provider.getName().toLowerCase(), provider);
        log.info("Registered LLM Provider: [{}]", provider.getName());
    }

    public Optional<LlmProvider> getProvider(String name) {
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(providers.get(name.toLowerCase()));
    }

    public Map<String, LlmProvider> getAllProviders() {
        return Collections.unmodifiableMap(providers);
    }
}

