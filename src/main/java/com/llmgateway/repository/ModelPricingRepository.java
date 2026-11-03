package com.llmgateway.repository;

import com.llmgateway.model.ModelPricing;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class ModelPricingRepository {

    private final DatabaseClient databaseClient;

    public ModelPricingRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Flux<ModelPricing> findAllActive() {
        String sql = """
                SELECT id, provider, model_name, prompt_token_micro_usd, completion_token_micro_usd, is_active, updated_at
                FROM model_pricing
                WHERE is_active = true
                """;
        return databaseClient.sql(sql)
                .map(this::mapRow)
                .all();
    }

    public Mono<ModelPricing> findByProviderAndModel(String provider, String modelName) {
        String sql = """
                SELECT id, provider, model_name, prompt_token_micro_usd, completion_token_micro_usd, is_active, updated_at
                FROM model_pricing
                WHERE LOWER(provider) = LOWER(:provider) AND LOWER(model_name) = LOWER(:modelName) AND is_active = true
                """;
        return databaseClient.sql(sql)
                .bind("provider", provider)
                .bind("modelName", modelName)
                .map(this::mapRow)
                .one();
    }

    private ModelPricing mapRow(Row row, RowMetadata metadata) {
        UUID id = row.get("id", UUID.class);
        String provider = row.get("provider", String.class);
        String modelName = row.get("model_name", String.class);
        Integer promptTokenMicroUsd = row.get("prompt_token_micro_usd", Integer.class);
        Integer completionTokenMicroUsd = row.get("completion_token_micro_usd", Integer.class);
        Boolean isActive = row.get("is_active", Boolean.class);
        Instant updatedAt = toInstant(row.get("updated_at"));

        return ModelPricing.builder()
                .id(id)
                .provider(provider)
                .modelName(modelName)
                .promptTokenMicroUsd(promptTokenMicroUsd != null ? promptTokenMicroUsd : 0)
                .completionTokenMicroUsd(completionTokenMicroUsd != null ? completionTokenMicroUsd : 0)
                .isActive(isActive != null && isActive)
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

