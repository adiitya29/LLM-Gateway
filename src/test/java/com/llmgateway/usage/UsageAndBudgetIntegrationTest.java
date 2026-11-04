package com.llmgateway.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.controller.AdminKeyController;
import com.llmgateway.controller.ChatCompletionController;
import com.llmgateway.controller.UsageController;
import com.llmgateway.dto.*;
import com.llmgateway.model.ApiKey;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.ApiKeyService;
import com.llmgateway.service.BudgetService;
import com.llmgateway.service.PricingService;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "gateway.admin.master-key=test-admin-key",
        "gateway.cache.enabled=false"
})
class UsageAndBudgetIntegrationTest {

    private static WireMockServer wireMockServer;

    @Autowired
    private ChatCompletionController chatCompletionController;

    @Autowired
    private AdminKeyController adminKeyController;

    @Autowired
    private UsageController usageController;

    @Autowired
    private GatewayAuthFilter gatewayAuthFilter;

    @Autowired
    private ApiKeyService apiKeyService;

    @Autowired
    private ProviderRegistry providerRegistry;

    @Autowired
    private PricingService pricingService;

    @Autowired
    private BudgetService budgetService;

    @Autowired
    private WebClient.Builder webClientBuilder;

    @Autowired
    private ObjectMapper objectMapper;

    private WebTestClient webTestClient;

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
        this.webTestClient = WebTestClient.bindToController(
                        chatCompletionController, adminKeyController, usageController)
                .webFilter(gatewayAuthFilter)
                .build();
        wireMockServer.resetAll();

        OpenAiCompatibleProvider groqProvider = new OpenAiCompatibleProvider(
                "groq",
                "http://localhost:" + wireMockServer.port() + "/groq/v1",
                "groq-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(groqProvider);
    }

    @Test
    void testUsageEndpointRequiresAuth() {
        // Without auth -> 401
        webTestClient.get()
                .uri("/v1/usage")
                .exchange()
                .expectStatus().isUnauthorized();

        // With invalid auth -> 401
        webTestClient.get()
                .uri("/v1/usage")
                .header(HttpHeaders.AUTHORIZATION, "Bearer invalid-dummy-key")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void testTokenExtractionAndUsageAccounting() throws Exception {
        // 1. Create a key with $10.00 budget
        CreateApiKeyRequest createReq = CreateApiKeyRequest.builder()
                .name("Usage Test Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(10_000_000L) // $10.00
                .rateLimitRpm(100)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(createReq)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        String apiKey = keyResp.getKey();

        // 2. Mock upstream response with explicit usage
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-test-usage")
                .model("llama-3.1-8b-instant")
                .choices(List.of(
                        Choice.builder()
                                .index(0)
                                .message(new ChatMessage("assistant", "Hello! Here is your answer."))
                                .finishReason("stop")
                                .build()
                ))
                .usage(new Usage(20, 30, 50))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        // 3. Make chat completion call
        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello there")))
                .stream(false)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.usage.prompt_tokens").isEqualTo(20)
                .jsonPath("$.usage.completion_tokens").isEqualTo(30)
                .jsonPath("$.usage.total_tokens").isEqualTo(50);

        // Allow async persistence to finish
        Thread.sleep(100);

        // 4. Query /v1/usage
        webTestClient.get()
                .uri("/v1/usage")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.budget_micro_usd").isEqualTo(10_000_000)
                .jsonPath("$.budget_usd").isEqualTo(10.0)
                .jsonPath("$.total_requests").isEqualTo(1)
                .jsonPath("$.total_prompt_tokens").isEqualTo(20)
                .jsonPath("$.total_completion_tokens").isEqualTo(30)
                .jsonPath("$.total_tokens").isEqualTo(50)
                .jsonPath("$.spent_micro_usd").isNumber();
    }

    @Test
    void testBudgetExhaustionRejection402() throws Exception {
        // 1. Create a key with only $0.01 (10,000 micro-USD) budget
        CreateApiKeyRequest createReq = CreateApiKeyRequest.builder()
                .name("Low Budget Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(10_000L) // $0.01
                .rateLimitRpm(100)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(createReq)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        String apiKey = keyResp.getKey();

        // Settle a cost in Redis that exhausts the 10,000 micro-USD budget
        budgetService.settle(keyResp.getId(), 0L, 10_000L).block();

        // 2. Next request should immediately be rejected with 402 Payment Required
        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "This should fail with 402")))
                .stream(false)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isEqualTo(402)
                .expectBody()
                .jsonPath("$.error.type").isEqualTo("insufficient_quota")
                .jsonPath("$.error.code").isEqualTo("budget_exceeded");
    }

    @Test
    void testDistributedRateLimiting429() throws Exception {
        // 1. Create an API key with 1 RPM rate limit
        CreateApiKeyRequest createReq = CreateApiKeyRequest.builder()
                .name("Rate Limited Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(10_000_000L)
                .rateLimitRpm(1) // Only 1 request per minute allowed
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(createReq)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        String apiKey = keyResp.getKey();

        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-test-rate")
                .model("llama-3.1-8b-instant")
                .choices(List.of(
                        Choice.builder()
                                .index(0)
                                .message(new ChatMessage("assistant", "First request allowed"))
                                .finishReason("stop")
                                .build()
                ))
                .usage(new Usage(10, 10, 20))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello")))
                .stream(false)
                .build();

        // First request consumes 1 token -> 200 OK
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk();

        // Second request immediately exceeds 1 RPM rate limit -> 429 Too Many Requests
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isEqualTo(429)
                .expectHeader().exists(HttpHeaders.RETRY_AFTER)
                .expectBody()
                .jsonPath("$.error.type").isEqualTo("requests")
                .jsonPath("$.error.code").isEqualTo("rate_limit_exceeded");
    }

    @Test
    void testStreamingUsageAndTokenizerFallback() throws Exception {
        // 1. Create API key
        CreateApiKeyRequest createReq = CreateApiKeyRequest.builder()
                .name("Streaming Usage Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(10_000_000L)
                .rateLimitRpm(100)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(createReq)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        String apiKey = keyResp.getKey();

        // 2. Stub SSE response with chunks
        String chunk1 = "data: {\"id\":\"chunk-1\",\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\n";
        String chunk2 = "data: {\"id\":\"chunk-2\",\"choices\":[{\"delta\":{\"content\":\"world!\"}}]}\n\n";
        String done = "data: [DONE]\n\n";

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/event-stream")
                        .withBody(chunk1 + chunk2 + done)));

        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello streaming")))
                .stream(true)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(String.class)
                .getResponseBody()
                .blockLast();

        Thread.sleep(100);

        // 3. Verify /v1/usage aggregated statistics after streaming completion
        webTestClient.get()
                .uri("/v1/usage")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.total_requests").isEqualTo(1)
                .jsonPath("$.total_prompt_tokens").isNumber()
                .jsonPath("$.total_completion_tokens").isNumber();
    }
}
