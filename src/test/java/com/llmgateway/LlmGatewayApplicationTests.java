package com.llmgateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@TestPropertySource(properties = {
    "spring.flyway.enabled=false",
    "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
    "spring.data.redis.host=localhost",
    "spring.data.redis.port=6379"
})
class LlmGatewayApplicationTests {

    @Test
    void contextLoads() {
    }
}

