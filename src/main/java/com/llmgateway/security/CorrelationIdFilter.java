package com.llmgateway.security;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class CorrelationIdFilter implements WebFilter, Ordered {

    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String REQUEST_ID_ATTR = "com.llmgateway.requestId";

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = exchange.getRequest().getHeaders().getFirst(REQUEST_ID_HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = "req_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        }

        // Attach correlation ID to response headers
        exchange.getResponse().getHeaders().set(REQUEST_ID_HEADER, requestId);
        // Store in exchange attributes for downstream controller retrieval
        exchange.getAttributes().put(REQUEST_ID_ATTR, requestId);

        final String finalRequestId = requestId;
        return chain.filter(exchange)
                .doOnEach(signal -> {
                    if (signal.isOnNext() || signal.isOnComplete() || signal.isOnError()) {
                        MDC.put("requestId", finalRequestId);
                    }
                })
                .doFinally(signalType -> MDC.remove("requestId"))
                .contextWrite(context -> context.put(REQUEST_ID_HEADER, finalRequestId).put("requestId", finalRequestId));
    }
}

