package com.llmgateway.repository;

import com.llmgateway.model.UsageLog;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class UsageLogRepository {

    private final DatabaseClient databaseClient;

    public UsageLogRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<UsageLog> save(UsageLog log) {
        UUID id = log.getId() != null ? log.getId() : UUID.randomUUID();
        String sql = """
                INSERT INTO usage_logs (id, api_key_id, request_id, model_alias, provider_used, actual_model,
                                       prompt_tokens, completion_tokens, total_tokens, cost_micro_usd,
                                       latency_ms, time_to_first_token_ms, cache_hit, fallback_used,
                                       status_code, created_at)
                VALUES (:id, :apiKeyId, :requestId, :modelAlias, :providerUsed, :actualModel,
                        :promptTokens, :completionTokens, :totalTokens, :costMicroUsd,
                        :latencyMs, :timeToFirstTokenMs, :cacheHit, :fallbackUsed,
                        :statusCode, :createdAt)
                RETURNING id, api_key_id, request_id, model_alias, provider_used, actual_model,
                          prompt_tokens, completion_tokens, total_tokens, cost_micro_usd,
                          latency_ms, time_to_first_token_ms, cache_hit, fallback_used,
                          status_code, created_at
                """;

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql)
                .bind("id", id)
                .bind("requestId", log.getRequestId())
                .bind("modelAlias", log.getModelAlias())
                .bind("providerUsed", log.getProviderUsed())
                .bind("actualModel", log.getActualModel())
                .bind("promptTokens", log.getPromptTokens())
                .bind("completionTokens", log.getCompletionTokens())
                .bind("totalTokens", log.getTotalTokens())
                .bind("costMicroUsd", log.getCostMicroUsd())
                .bind("latencyMs", log.getLatencyMs())
                .bind("cacheHit", log.getCacheHit())
                .bind("fallbackUsed", log.isFallbackUsed())
                .bind("statusCode", log.getStatusCode())
                .bind("createdAt", log.getCreatedAt() != null ? log.getCreatedAt() : Instant.now());

        if (log.getApiKeyId() != null) {
            spec = spec.bind("apiKeyId", log.getApiKeyId());
        } else {
            spec = spec.bindNull("apiKeyId", UUID.class);
        }

        if (log.getTimeToFirstTokenMs() != null) {
            spec = spec.bind("timeToFirstTokenMs", log.getTimeToFirstTokenMs());
        } else {
            spec = spec.bindNull("timeToFirstTokenMs", Integer.class);
        }

        return spec.map(this::mapRow).one();
    }

    public record AggregateStats(
            long totalRequests,
            long totalPromptTokens,
            long totalCompletionTokens,
            long totalTokens,
            long totalCostMicroUsd
    ) {}

    public Mono<AggregateStats> getAggregateStats(UUID apiKeyId) {
        String sql = """
                SELECT COUNT(*) as req_count,
                       COALESCE(SUM(prompt_tokens), 0) as p_tokens,
                       COALESCE(SUM(completion_tokens), 0) as c_tokens,
                       COALESCE(SUM(total_tokens), 0) as t_tokens,
                       COALESCE(SUM(cost_micro_usd), 0) as total_cost
                FROM usage_logs
                WHERE api_key_id = :apiKeyId
                """;

        return databaseClient.sql(sql)
                .bind("apiKeyId", apiKeyId)
                .map((row, meta) -> new AggregateStats(
                        row.get("req_count", Long.class) != null ? row.get("req_count", Long.class) : 0L,
                        row.get("p_tokens", Long.class) != null ? row.get("p_tokens", Long.class) : 0L,
                        row.get("c_tokens", Long.class) != null ? row.get("c_tokens", Long.class) : 0L,
                        row.get("t_tokens", Long.class) != null ? row.get("t_tokens", Long.class) : 0L,
                        row.get("total_cost", Long.class) != null ? row.get("total_cost", Long.class) : 0L
                ))
                .one()
                .defaultIfEmpty(new AggregateStats(0, 0, 0, 0, 0));
    }

    private UsageLog mapRow(Row row, RowMetadata metadata) {
        UUID id = row.get("id", UUID.class);
        UUID apiKeyId = row.get("api_key_id", UUID.class);
        String requestId = row.get("request_id", String.class);
        String modelAlias = row.get("model_alias", String.class);
        String providerUsed = row.get("provider_used", String.class);
        String actualModel = row.get("actual_model", String.class);
        Integer promptTokens = row.get("prompt_tokens", Integer.class);
        Integer completionTokens = row.get("completion_tokens", Integer.class);
        Integer totalTokens = row.get("total_tokens", Integer.class);
        Long costMicroUsd = row.get("cost_micro_usd", Long.class);
        Integer latencyMs = row.get("latency_ms", Integer.class);
        Integer timeToFirstTokenMs = row.get("time_to_first_token_ms", Integer.class);
        String cacheHit = row.get("cache_hit", String.class);
        Boolean fallbackUsed = row.get("fallback_used", Boolean.class);
        Integer statusCode = row.get("status_code", Integer.class);
        Instant createdAt = toInstant(row.get("created_at"));

        return UsageLog.builder()
                .id(id)
                .apiKeyId(apiKeyId)
                .requestId(requestId)
                .modelAlias(modelAlias)
                .providerUsed(providerUsed)
                .actualModel(actualModel)
                .promptTokens(promptTokens != null ? promptTokens : 0)
                .completionTokens(completionTokens != null ? completionTokens : 0)
                .totalTokens(totalTokens != null ? totalTokens : 0)
                .costMicroUsd(costMicroUsd != null ? costMicroUsd : 0L)
                .latencyMs(latencyMs != null ? latencyMs : 0)
                .timeToFirstTokenMs(timeToFirstTokenMs)
                .cacheHit(cacheHit != null ? cacheHit : "NONE")
                .fallbackUsed(fallbackUsed != null && fallbackUsed)
                .statusCode(statusCode != null ? statusCode : 200)
                .createdAt(createdAt != null ? createdAt : Instant.now())
                .build();
    }

    private Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        } else if (value instanceof OffsetDateTime odt) {
            return odt.toInstant();
        }
        return null;
    }
}
