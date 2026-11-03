package com.llmgateway.repository;

import com.llmgateway.model.ApiKey;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

@Repository
public class ApiKeyRepository {

    private final DatabaseClient databaseClient;

    public ApiKeyRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<ApiKey> findByKeyHash(String keyHash) {
        String sql = """
                SELECT id, name, key_prefix, key_hash, is_active, allowed_models,
                       budget_micro_usd, spent_micro_usd, rate_limit_rpm, created_at, updated_at
                FROM api_keys
                WHERE key_hash = :keyHash
                """;

        return databaseClient.sql(sql)
                .bind("keyHash", keyHash)
                .map(this::mapRow)
                .one();
    }

    public Mono<ApiKey> findById(UUID id) {
        String sql = """
                SELECT id, name, key_prefix, key_hash, is_active, allowed_models,
                       budget_micro_usd, spent_micro_usd, rate_limit_rpm, created_at, updated_at
                FROM api_keys
                WHERE id = :id
                """;

        return databaseClient.sql(sql)
                .bind("id", id)
                .map(this::mapRow)
                .one();
    }

    public Flux<ApiKey> findAll() {
        String sql = """
                SELECT id, name, key_prefix, key_hash, is_active, allowed_models,
                       budget_micro_usd, spent_micro_usd, rate_limit_rpm, created_at, updated_at
                FROM api_keys
                ORDER BY created_at DESC
                """;

        return databaseClient.sql(sql)
                .map(this::mapRow)
                .all();
    }

    public Mono<ApiKey> save(ApiKey apiKey) {
        String sql = """
                INSERT INTO api_keys (id, name, key_prefix, key_hash, is_active, allowed_models,
                                     budget_micro_usd, spent_micro_usd, rate_limit_rpm, created_at, updated_at)
                VALUES (:id, :name, :keyPrefix, :keyHash, :isActive, :allowedModels,
                        :budgetMicroUsd, :spentMicroUsd, :rateLimitRpm, :createdAt, :updatedAt)
                RETURNING id, name, key_prefix, key_hash, is_active, allowed_models,
                          budget_micro_usd, spent_micro_usd, rate_limit_rpm, created_at, updated_at
                """;

        String[] allowedModelsArray = apiKey.getAllowedModels() != null
                ? apiKey.getAllowedModels().toArray(new String[0])
                : new String[0];

        return databaseClient.sql(sql)
                .bind("id", apiKey.getId())
                .bind("name", apiKey.getName())
                .bind("keyPrefix", apiKey.getKeyPrefix())
                .bind("keyHash", apiKey.getKeyHash())
                .bind("isActive", apiKey.isActive())
                .bind("allowedModels", allowedModelsArray)
                .bind("budgetMicroUsd", apiKey.getBudgetMicroUsd())
                .bind("spentMicroUsd", apiKey.getSpentMicroUsd())
                .bind("rateLimitRpm", apiKey.getRateLimitRpm())
                .bind("createdAt", apiKey.getCreatedAt())
                .bind("updatedAt", apiKey.getUpdatedAt())
                .map(this::mapRow)
                .one();
    }

    public Mono<Long> addSpent(UUID id, long additionalSpentMicroUsd) {
        String sql = """
                UPDATE api_keys
                SET spent_micro_usd = spent_micro_usd + :additionalSpent, updated_at = NOW()
                WHERE id = :id
                RETURNING spent_micro_usd
                """;
        return databaseClient.sql(sql)
                .bind("id", id)
                .bind("additionalSpent", additionalSpentMicroUsd)
                .map(row -> row.get("spent_micro_usd", Long.class))
                .one();
    }

    public Mono<Boolean> revokeById(UUID id) {
        String sql = "UPDATE api_keys SET is_active = false, updated_at = NOW() WHERE id = :id";
        return databaseClient.sql(sql)
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .map(count -> count > 0);
    }

    public Mono<Boolean> deleteById(UUID id) {
        String sql = "DELETE FROM api_keys WHERE id = :id";
        return databaseClient.sql(sql)
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .map(count -> count > 0);
    }

    private ApiKey mapRow(Row row, RowMetadata metadata) {
        UUID id = row.get("id", UUID.class);
        String name = row.get("name", String.class);
        String keyPrefix = row.get("key_prefix", String.class);
        String keyHash = row.get("key_hash", String.class);
        Boolean isActive = row.get("is_active", Boolean.class);
        String[] allowedModels = row.get("allowed_models", String[].class);
        Long budgetMicroUsd = row.get("budget_micro_usd", Long.class);
        Long spentMicroUsd = row.get("spent_micro_usd", Long.class);
        Integer rateLimitRpm = row.get("rate_limit_rpm", Integer.class);

        Instant createdAt = toInstant(row.get("created_at"));
        Instant updatedAt = toInstant(row.get("updated_at"));

        return ApiKey.builder()
                .id(id)
                .name(name)
                .keyPrefix(keyPrefix)
                .keyHash(keyHash)
                .isActive(isActive != null && isActive)
                .allowedModels(allowedModels != null ? Arrays.asList(allowedModels) : Collections.emptyList())
                .budgetMicroUsd(budgetMicroUsd != null ? budgetMicroUsd : 0L)
                .spentMicroUsd(spentMicroUsd != null ? spentMicroUsd : 0L)
                .rateLimitRpm(rateLimitRpm != null ? rateLimitRpm : 60)
                .createdAt(createdAt != null ? createdAt : Instant.now())
                .updatedAt(updatedAt != null ? updatedAt : Instant.now())
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

