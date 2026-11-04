package com.llmgateway.repository;

import com.llmgateway.model.SemanticCacheEntry;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public class SemanticCacheRepository {

    private static final Logger log = LoggerFactory.getLogger(SemanticCacheRepository.class);

    private final DatabaseClient databaseClient;

    public SemanticCacheRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    /**
     * Queries the nearest cached entry using pgvector cosine distance (<=>).
     * Cosine similarity = 1 - cosine_distance.
     * With similarity >= threshold, cosine_distance <= (1 - threshold).
     */
    public Mono<SemanticCacheEntry> findNearest(String scopeHash, String vectorString, double maxDistance) {
        String sql = """
                SELECT id, scope_hash, prompt_text, response_json, created_at, expires_at,
                       (1.0 - (prompt_embedding <=> CAST(:embedding AS vector))) as similarity
                FROM semantic_cache
                WHERE scope_hash = :scopeHash
                  AND expires_at > NOW()
                  AND (prompt_embedding <=> CAST(:embedding AS vector)) <= :maxDistance
                ORDER BY (prompt_embedding <=> CAST(:embedding AS vector)) ASC
                LIMIT 1
                """;

        return databaseClient.sql(sql)
                .bind("scopeHash", scopeHash)
                .bind("embedding", vectorString)
                .bind("maxDistance", maxDistance)
                .map(this::mapRow)
                .one();
    }

    /**
     * Stores a new semantic cache entry with an expiration interval.
     */
    public Mono<Void> save(String scopeHash, String promptText, String vectorString, String responseJson, Duration ttl) {
        Instant now = Instant.now();
        Instant expiresAt = now.plus(ttl);
        UUID id = UUID.randomUUID();
        String sql = """
                INSERT INTO semantic_cache (id, scope_hash, prompt_text, prompt_embedding, response_json, created_at, expires_at)
                VALUES (:id, :scopeHash, :promptText, CAST(:embedding AS vector), :responseJson, :createdAt, :expiresAt)
                """;

        return databaseClient.sql(sql)
                .bind("id", id)
                .bind("scopeHash", scopeHash)
                .bind("promptText", promptText)
                .bind("embedding", vectorString)
                .bind("responseJson", responseJson)
                .bind("createdAt", now)
                .bind("expiresAt", expiresAt)
                .then()
                .doOnError(err -> log.error("Failed to save semantic cache entry: {}", err.getMessage()));
    }

    private SemanticCacheEntry mapRow(Row row, RowMetadata metadata) {
        UUID id = row.get("id", UUID.class);
        String scopeHash = row.get("scope_hash", String.class);
        String promptText = row.get("prompt_text", String.class);
        String responseJson = row.get("response_json", String.class);
        Instant createdAt = toInstant(row.get("created_at"));
        Instant expiresAt = toInstant(row.get("expires_at"));
        Double similarity = row.get("similarity", Double.class);

        return SemanticCacheEntry.builder()
                .id(id)
                .scopeHash(scopeHash)
                .promptText(promptText)
                .responseJson(responseJson)
                .createdAt(createdAt)
                .expiresAt(expiresAt)
                .similarity(similarity)
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
