package com.llmgateway.security;

import com.llmgateway.config.GatewayProperties;
import com.llmgateway.model.ApiKey;
import com.llmgateway.service.ApiKeyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@Component
public class GatewayAuthFilter implements WebFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(GatewayAuthFilter.class);
    public static final String API_KEY_ATTR = "GATEWAY_API_KEY";

    private final ApiKeyService apiKeyService;
    private final GatewayProperties properties;

    public GatewayAuthFilter(ApiKeyService apiKeyService, GatewayProperties properties) {
        this.apiKeyService = apiKeyService;
        this.properties = properties;
    }

    @Override
    public int getOrder() {
        return -100; // Early execution in filter chain
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // 1. Actuator endpoints are public
        if (path.startsWith("/actuator")) {
            return chain.filter(exchange);
        }

        // 2. Admin endpoints protected by X-Admin-Master-Key
        if (path.startsWith("/v1/admin")) {
            String masterKeyHeader = exchange.getRequest().getHeaders().getFirst("X-Admin-Master-Key");
            String configuredMasterKey = properties.getAdmin().getMasterKey();

            if (masterKeyHeader == null || !masterKeyHeader.equals(configuredMasterKey)) {
                log.warn("Unauthorized access attempt to admin endpoint: {}", path);
                return writeJsonError(exchange, HttpStatus.UNAUTHORIZED,
                        "Invalid or missing X-Admin-Master-Key header", "authentication_error", "unauthorized");
            }
            return chain.filter(exchange);
        }

        // 3. Gateway proxy endpoints (/v1/chat/completions, /v1/models, /v1/usage) require a valid API key
        if (path.startsWith("/v1/chat/completions") || path.startsWith("/v1/models") || path.startsWith("/v1/usage")) {
            String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                return writeJsonError(exchange, HttpStatus.UNAUTHORIZED,
                        "Missing or malformed Authorization header. Expected 'Bearer <key>'",
                        "invalid_request_error", "missing_api_key");
            }

            String rawKey = authHeader.substring(7).trim();
            if (rawKey.isEmpty()) {
                return writeJsonError(exchange, HttpStatus.UNAUTHORIZED,
                        "Empty API key provided", "invalid_request_error", "invalid_api_key");
            }

            return apiKeyService.authenticateKey(rawKey)
                    .switchIfEmpty(Mono.error(new UnauthorizedApiKeyException("Invalid API key")))
                    .flatMap(apiKey -> {
                        if (!apiKey.isActive()) {
                            return Mono.error(new UnauthorizedApiKeyException("API key has been revoked"));
                        }
                        exchange.getAttributes().put(API_KEY_ATTR, apiKey);
                        return chain.filter(exchange);
                    })
                    .onErrorResume(UnauthorizedApiKeyException.class, ex ->
                            writeJsonError(exchange, HttpStatus.UNAUTHORIZED, ex.getMessage(), "invalid_request_error", "invalid_api_key"));
        }

        return chain.filter(exchange);
    }

    private Mono<Void> writeJsonError(ServerWebExchange exchange, HttpStatus status, String message, String type, String code) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = String.format("{\"error\":{\"message\":\"%s\",\"type\":\"%s\",\"code\":\"%s\"}}",
                escapeJson(message), escapeJson(type), escapeJson(code));
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    private String escapeJson(String s) {
        return s.replace("\"", "\\\"");
    }

    public static class UnauthorizedApiKeyException extends RuntimeException {
        public UnauthorizedApiKeyException(String message) {
            super(message);
        }
    }
}

