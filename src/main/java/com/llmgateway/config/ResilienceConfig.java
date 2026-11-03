package com.llmgateway.config;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

@Configuration
public class ResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(ResilienceConfig.class);

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(GatewayProperties properties) {
        GatewayProperties.CircuitBreakerProperties cbProps = properties.getCircuitBreaker();

        CircuitBreakerConfig defaultConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(cbProps.getFailureRateThreshold())
                .slidingWindowSize(cbProps.getSlidingWindowSize())
                .minimumNumberOfCalls(cbProps.getMinimumNumberOfCalls())
                .waitDurationInOpenState(Duration.ofSeconds(cbProps.getWaitDurationInOpenStateSeconds()))
                .permittedNumberOfCallsInHalfOpenState(cbProps.getPermittedNumberOfCallsInHalfOpenState())
                .recordExceptions(
                        WebClientResponseException.class,
                        WebClientRequestException.class,
                        TimeoutException.class,
                        IOException.class,
                        CallNotPermittedException.class
                )
                .recordException(throwable -> {
                    if (throwable instanceof WebClientResponseException wcre) {
                        int code = wcre.getStatusCode().value();
                        // 429 (rate limited) and 5xx (server errors) trip breaker
                        return code == 429 || code == 408 || (code >= 500 && code < 600);
                    }
                    return throwable instanceof TimeoutException
                            || throwable instanceof WebClientRequestException
                            || throwable instanceof IOException;
                })
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(defaultConfig);

        // Pre-create circuit breakers for all configured providers
        if (properties.getProviders() != null) {
            properties.getProviders().keySet().forEach(providerName -> {
                registry.circuitBreaker(providerName);
                log.info("Registered Resilience4j CircuitBreaker for provider: [{}]", providerName);
            });
        }

        return registry;
    }
}

