package com.llmgateway.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.dto.*;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.repository.UsageLogRepository;
import com.llmgateway.service.GatewayMetricsService;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "gateway.admin.master-key=test-admin-key",
        "gateway.cache.enabled=false"
})
class ObservabilityAndTelemetryIntegrationTest {

    private static WireMockServer wireMockServer;

    @LocalServerPort
    private int port;

    @Autowired
    private ProviderRegistry providerRegistry;

    @Autowired
    private GatewayMetricsService metricsService;

    @Autowired
    private UsageLogRepository usageLogRepository;

    @Autowired
    private ReactiveStringRedisTemplate redisTemplate;

    @Autowired
    private DatabaseClient databaseClient;

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

        // Clear redis and postgres tables for isolation
        redisTemplate.getConnectionFactory().getReactiveConnection().serverCommands().flushDb().block();
        databaseClient.sql("DELETE FROM usage_logs").then().block();
        databaseClient.sql("DELETE FROM semantic_cache").then().block();

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

        // Create test API key
        CreateApiKeyRequest keyReq = CreateApiKeyRequest.builder()
                .name("Observability-Key")
                .allowedModels(List.of("fast-chat", "smart-chat"))
                .budgetMicroUsd(10_000_000L)
                .rateLimitRpm(100)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
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
    void testDistributedCorrelationRequestIdPropagation() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-corr-1")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Correlated answer.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(10, 5, 15))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        String customTraceId = "trace-client-xyz-12345";
        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Test correlation").build()))
                .stream(false)
                .build();

        // 1. Send request with incoming X-Request-Id header
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .header("X-Request-Id", customTraceId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Request-Id", customTraceId);

        // 2. Verify downstream provider received X-Request-Id: trace-client-xyz-12345
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions"))
                .withHeader("X-Request-Id", equalTo(customTraceId)));

        // 3. Verify usage_logs recorded the correlated request_id
        Thread.sleep(150);
        Long count = databaseClient.sql("SELECT COUNT(*) FROM usage_logs WHERE request_id = :reqId")
                .bind("reqId", customTraceId)
                .map((row, meta) -> row.get(0, Long.class))
                .one()
                .block();
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void testAutoGeneratedRequestIdWhenHeaderOmitted() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-auto-id")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Auto id answer.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(8, 4, 12))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Hello without request id").build()))
                .stream(false)
                .build();

        // Send request WITHOUT X-Request-Id header
        String generatedRequestId = webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().exists("X-Request-Id")
                .returnResult(String.class)
                .getResponseHeaders()
                .getFirst("X-Request-Id");

        assertThat(generatedRequestId).startsWith("req_");

        // Verify downstream call received the exact same generated ID
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions"))
                .withHeader("X-Request-Id", equalTo(generatedRequestId)));
    }

    @Test
    void testPrometheusEndpointExposesMetricsAndCircuitBreakerStates() throws Exception {
        // Upstream SSE streaming mock chunks
        String chunk1 = "data: {\"id\":\"chk1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Streamed \"}}]}\n\n";
        String chunk2 = "data: {\"id\":\"chk2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"observability metric.\"}}],\"usage\":{\"prompt_tokens\":14,\"completion_tokens\":6,\"total_tokens\":20}}\n\n";
        String doneChunk = "data: [DONE]\n\n";

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/event-stream")
                        .withBody(chunk1 + chunk2 + doneChunk)));

        ChatCompletionRequest streamRequest = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Stream with metrics").build()))
                .stream(true)
                .build();

        // 1. Perform streaming request
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(streamRequest)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("Streamed ");
                    assertThat(body).contains("observability metric.");
                    assertThat(body).contains("[DONE]");
                });

        // Wait brief instant for async settlement & metrics emission
        Thread.sleep(800);

        // 2. Scrape /actuator/prometheus
        String prometheusScrape = webTestClient.get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(prometheusScrape).isNotNull();

        // Verify custom Micrometer gateway metrics in Prometheus format:
        // - Requests total with tags (provider, model, status, cache_hit)
        assertThat(prometheusScrape).contains("llm_gateway_requests_total");
        assertThat(prometheusScrape).contains("provider=\"groq\"");
        assertThat(prometheusScrape).contains("model=\"fast-chat\"");

        // - Tokens total (prompt & completion)
        assertThat(prometheusScrape).contains("llm_gateway_tokens_total");
        assertThat(prometheusScrape).contains("type=\"prompt\"");
        assertThat(prometheusScrape).contains("type=\"completion\"");

        // - Cost micro-USD
        assertThat(prometheusScrape).contains("llm_gateway_cost_micro_usd_total");
        assertThat(prometheusScrape).contains("api_key=");

        // - Time to first token (TTFT) distribution summary
        assertThat(prometheusScrape).contains("llm_gateway_time_to_first_token_seconds");

        // - Latency seconds
        assertThat(prometheusScrape).contains("llm_gateway_latency_seconds");

        // - Circuit breaker states exposed to Actuator via TaggedCircuitBreakerMetrics
        assertThat(prometheusScrape).contains("resilience4j_circuitbreaker_state");
        assertThat(prometheusScrape).contains("name=\"groq\"");
        assertThat(prometheusScrape).contains("state=\"closed\"");
    }
}
