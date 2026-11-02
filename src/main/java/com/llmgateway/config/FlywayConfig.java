package com.llmgateway.config;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Executes Flyway database migrations synchronously at startup using standard JDBC,
 * preventing any blocking operations from leaking into the reactive R2DBC event loop.
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.flyway", name = "enabled", matchIfMissing = true)
public class FlywayConfig {

    private static final Logger log = LoggerFactory.getLogger(FlywayConfig.class);

    @Bean(initMethod = "migrate")
    public Flyway flyway(
            @Value("${spring.flyway.url}") String url,
            @Value("${spring.flyway.user}") String user,
            @Value("${spring.flyway.password}") String password,
            @Value("${spring.flyway.baseline-on-migrate:true}") boolean baselineOnMigrate,
            @Value("${spring.flyway.locations:classpath:db/migration}") String locations) {
        log.info("Initializing Flyway migration on database URL: {}", url);
        return Flyway.configure()
                .dataSource(url, user, password)
                .locations(locations)
                .baselineOnMigrate(baselineOnMigrate)
                .load();
    }
}

