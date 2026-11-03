package com.llmgateway.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private Admin admin = new Admin();
    private Upstream upstream = new Upstream();
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();
    private Map<String, ProviderConfig> providers = new HashMap<>();
    private Map<String, ModelRoute> routes = new HashMap<>();

    @Data
    public static class CircuitBreakerProperties {
        private float failureRateThreshold = 50.0f;
        private int slidingWindowSize = 10;
        private int minimumNumberOfCalls = 5;
        private int waitDurationInOpenStateSeconds = 10;
        private int permittedNumberOfCallsInHalfOpenState = 3;
    }

    @Data
    public static class Admin {
        private String masterKey = "dev-admin-key";
    }

    @Data
    public static class Upstream {
        private String baseUrl = "http://localhost:8089/v1";
        private String apiKey = "";
        private int connectTimeoutMs = 5000;
        private int readTimeoutMs = 60000;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProviderConfig {
        private String baseUrl;
        @Builder.Default
        private String apiKey = "";
        @Builder.Default
        private int connectTimeoutMs = 5000;
        @Builder.Default
        private int readTimeoutMs = 60000;
        @Builder.Default
        private int ttftTimeoutMs = 15000;
        @Builder.Default
        private Map<String, String> headers = new HashMap<>();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ModelRoute {
        @Builder.Default
        private String description = "";
        @Builder.Default
        private List<RouteTarget> targets = new ArrayList<>();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RouteTarget {
        private String provider;
        private String model;
    }
}
