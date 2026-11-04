package com.llmgateway.testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.dto.*;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "gateway.admin.master-key=tc-admin-master-key",
        "gateway.cache.enabled=true",
        "gateway.cache.exact-ttl-seconds=3600",
        "gateway.cache.semantic-ttl-seconds=86400",
        "gateway.cache.embedding-provider=mock"
})
class EndToEndTestcontainersIntegrationTest {

    private static WireMockServer wireMockServer;

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres")
    ).withDatabaseName("llmgateway")
     .withUsername("postgres")
     .withPassword("postgrespassword");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/llmgateway");
        registry.add("spring.flyway.url", () -> "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/llmgateway");
        registry.add("spring.flyway.user", postgres::getUsername);
        registry.add("spring.flyway.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ProviderRegistry providerRegistry;

    @Autowired
    private WebClient.Builder webClientBuilder;

    @Autowired
    private ObjectMapper objectMapper;

    private WebTestClient webTestClient;
    private String testApiKey;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(0);
        wireMockServer.start();
        WireMock.configureFor("localhost", wireMockServer.port());
    }

    @AfterAll
    static void stopWireMock() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void setup() {
        this.webTestClient = WebTestClient.bindToServer()
                .responseTimeout(Duration.ofSeconds(10))
                .baseUrl("http://localhost:" + port)
                .build();

        wireMockServer.resetAll();

        // Register Groq WireMock provider
        OpenAiCompatibleProvider mockGroq = new OpenAiCompatibleProvider(
                "groq",
                "http://localhost:" + wireMockServer.port() + "/groq/v1",
                "mock-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(mockGroq);

        // Provision an API key via Admin API
        CreateApiKeyRequest keyReq = CreateApiKeyRequest.builder()
                .name("Testcontainers-Key")
                .allowedModels(List.of("fast-chat", "smart-chat"))
                .budgetMicroUsd(5_000_000L) // $5.00
                .rateLimitRpm(120)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "tc-admin-master-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(keyReq)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        this.testApiKey = keyResp.getKey();
    }

    @Test
    void testEndToEndChatStreamingAndCacheValidation() throws Exception {
        // 1. Verify healthcheck on isolated container stack
        webTestClient.get()
                .uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> assertThat(body).contains("UP"));

        // 2. Upstream mock response for non-streaming
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-tc-1")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Testcontainers validation successful.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(10, 8, 18))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Validate Testcontainers").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        // 3. First call -> Cache MISS
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS")
                .expectHeader().exists("X-Request-Id")
                .expectBody(ChatCompletionResponse.class)
                .value(resp -> assertThat(resp.getChoices().get(0).getMessage().getContentAsString())
                        .isEqualTo("Testcontainers validation successful."));

        Thread.sleep(200);

        // 4. Second call -> Cache HIT_EXACT
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "HIT_EXACT")
                .expectBody(ChatCompletionResponse.class)
                .value(resp -> assertThat(resp.getChoices().get(0).getMessage().getContentAsString())
                        .isEqualTo("Testcontainers validation successful."));

        // Upstream was only invoked once because of caching in Testcontainers Redis
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // 5. Query Actuator Prometheus endpoint for metrics
        webTestClient.get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(metrics -> {
                    assertThat(metrics).contains("llm_gateway_requests_total");
                    assertThat(metrics).contains("llm_gateway_tokens_total");
                });
    }
}

