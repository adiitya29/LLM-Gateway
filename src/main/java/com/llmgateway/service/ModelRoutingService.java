package com.llmgateway.service;

import com.llmgateway.config.GatewayProperties;
import com.llmgateway.config.GatewayProperties.ModelRoute;
import com.llmgateway.config.GatewayProperties.RouteTarget;
import com.llmgateway.dto.ModelCard;
import com.llmgateway.provider.LlmProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class ModelRoutingService {

    private static final Logger log = LoggerFactory.getLogger(ModelRoutingService.class);

    private final GatewayProperties properties;
    private final ProviderRegistry providerRegistry;

    public ModelRoutingService(GatewayProperties properties, ProviderRegistry providerRegistry) {
        this.properties = properties;
        this.providerRegistry = providerRegistry;
    }

    /**
     * Resolves a model alias or model name to an ordered chain of provider targets.
     */
    public ResolvedRoute resolveRoute(String modelOrAlias) {
        if (modelOrAlias == null || modelOrAlias.isBlank()) {
            throw new IllegalArgumentException("Model parameter is required");
        }

        ModelRoute route = properties.getRoutes().get(modelOrAlias);
        if (route != null && route.getTargets() != null && !route.getTargets().isEmpty()) {
            RouteTarget primaryTarget = route.getTargets().get(0);
            LlmProvider provider = providerRegistry.getProvider(primaryTarget.getProvider())
                    .orElseThrow(() -> new IllegalStateException(
                            "Configured provider '" + primaryTarget.getProvider() + "' for alias '" + modelOrAlias + "' not found"));

            log.debug("Resolved alias [{}] -> provider [{}], target model [{}]",
                    modelOrAlias, provider.getName(), primaryTarget.getModel());

            return ResolvedRoute.builder()
                    .requestedAlias(modelOrAlias)
                    .primaryProvider(provider)
                    .targetModel(primaryTarget.getModel())
                    .allTargets(route.getTargets())
                    .build();
        }

        // Direct model fallback: if alias not configured, try default provider
        Optional<LlmProvider> defaultProvider = providerRegistry.getProvider("default");
        if (defaultProvider.isPresent()) {
            log.debug("Alias [{}] not in routing table. Routing directly to default provider.", modelOrAlias);
            RouteTarget directTarget = RouteTarget.builder()
                    .provider("default")
                    .model(modelOrAlias)
                    .build();

            return ResolvedRoute.builder()
                    .requestedAlias(modelOrAlias)
                    .primaryProvider(defaultProvider.get())
                    .targetModel(modelOrAlias)
                    .allTargets(List.of(directTarget))
                    .build();
        }

        throw new IllegalArgumentException("Model or alias '" + modelOrAlias + "' is not supported by any configured route");
    }

    /**
     * Lists all models and aliases configured in the gateway.
     */
    public List<ModelCard> listModels() {
        List<ModelCard> models = new ArrayList<>();

        if (properties.getRoutes() != null && !properties.getRoutes().isEmpty()) {
            properties.getRoutes().forEach((alias, route) -> {
                String ownedBy = "llm-gateway";
                if (route.getTargets() != null && !route.getTargets().isEmpty()) {
                    ownedBy = route.getTargets().get(0).getProvider();
                }

                models.add(ModelCard.builder()
                        .id(alias)
                        .ownedBy(ownedBy)
                        .build());
            });
        } else {
            // If no specific routes configured, expose default model aliases
            models.add(ModelCard.builder().id("fast-chat").ownedBy("groq").build());
            models.add(ModelCard.builder().id("smart-chat").ownedBy("gemini").build());
            models.add(ModelCard.builder().id("gemini-flash").ownedBy("gemini").build());
        }

        return models;
    }

    /**
     * Gets a specific model card by model ID / alias.
     */
    public Optional<ModelCard> getModel(String modelId) {
        return listModels().stream()
                .filter(m -> m.getId().equalsIgnoreCase(modelId))
                .findFirst();
    }
}

