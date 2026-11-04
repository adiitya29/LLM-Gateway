package com.llmgateway.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.controller.AdminKeyController;
import com.llmgateway.controller.ChatCompletionController;
import com.llmgateway.dto.*;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.MultiTierCacheService;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
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
        "gateway.cache.enabled=true",
        "gateway.cache.exact-ttl-seconds=3600",
        "gateway.cache.semantic-ttl-seconds=86400",
        "gateway.cache.semantic-similarity-threshold=0.95",
        "gateway.cache.max-temperature-for-semantic=0.2",
        "gateway.cache.embedding-provider=mock"
})
class MultiTierCacheIntegrationTest {

    private static WireMockServer wireMockServer;

    @Autowired
    private ChatCompletionController chatCompletionController;

    @Autowired
    private AdminKeyController adminKeyController;

    @Autowired
    private GatewayAuthFilter gatewayAuthFilter;

    @Autowired
    private ProviderRegistry providerRegistry;

    @Autowired
    private MultiTierCacheService multiTierCacheService;

    @Autowired
    private ReactiveStringRedisTemplate redisTemplate;

    @Autowired
    private org.springframework.r2dbc.core.DatabaseClient databaseClient;

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
        this.webTestClient = WebTestClient.bindToController(chatCompletionController, adminKeyController)
                .webFilter(gatewayAuthFilter)
                .build();

        wireMockServer.resetAll();

        // Clear redis keys and postgres semantic cache for clean test isolation
        redisTemplate.getConnectionFactory().getReactiveConnection().serverCommands().flushDb().block();
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

        // Create test API key with $10 budget
        CreateApiKeyRequest keyReq = CreateApiKeyRequest.builder()
                .name("Cache-Test-Key")
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
    void testExactCacheHitAndZeroBilling() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-mock-france")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Paris is the capital of France.").build())
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
                .messages(List.of(ChatMessage.builder().role("user").content("What is the capital of France?").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        // 1. Initial request -> Cache MISS
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS")
                .expectBody(ChatCompletionResponse.class)
                .value(resp -> assertThat(resp.getChoices().get(0).getMessage().getContentAsString())
                        .isEqualTo("Paris is the capital of France."));

        // Wait brief instant for async cache save
        Thread.sleep(150);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // 2. Exact same request -> Tier 1 HIT_EXACT
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
                        .isEqualTo("Paris is the capital of France."));

        // Verify upstream WireMock was NOT called again
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
    }

    @Test
    void testSemanticCacheHitWithParaphrasedQuery() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-mock-germany")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Berlin is the capital of Germany.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(10, 8, 18))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest request1 = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("What is the capital of Germany?").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        // 1. Initial request -> Cache MISS
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request1)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS")
                .expectBody(ChatCompletionResponse.class)
                .value(resp -> assertThat(resp.getChoices().get(0).getMessage().getContentAsString())
                        .isEqualTo("Berlin is the capital of Germany."));

        // Wait brief instant for async vector embedding & pgvector save
        Thread.sleep(350);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // 2. Paraphrased query with identical semantic meaning -> Tier 2 HIT_SEMANTIC
        ChatCompletionRequest request2 = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Tell me the capital of Germany, please").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request2)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "HIT_SEMANTIC")
                .expectBody(ChatCompletionResponse.class)
                .value(resp -> assertThat(resp.getChoices().get(0).getMessage().getContentAsString())
                        .isEqualTo("Berlin is the capital of Germany."));

        // Upstream was still only called once!
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
    }

    @Test
    void testBypassCacheHeaderForcesUpstreamCall() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-mock-bypass")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Direct answer.").build())
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
                .messages(List.of(ChatMessage.builder().role("user").content("Calculate 2+2").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        // 1. Initial request -> Cache MISS
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS");

        Thread.sleep(150);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // 2. Request with X-Gateway-Bypass-Cache: true -> Forces upstream call, header MISS
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .header("X-Gateway-Bypass-Cache", "true")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS");

        // WireMock received 2 calls because cache was bypassed!
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
    }

    @Test
    void testStreamingCacheHitWithSseReplayEngine() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-mock-stream-seed")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("Reactive streams are non blocking.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(12, 6, 18))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest nonStreamReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Explain reactive streams briefly.").build()))
                .temperature(0.1)
                .stream(false)
                .build();

        // 1. Seed cache via non-streaming request
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(nonStreamReq)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS");

        Thread.sleep(150);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // 2. Request same prompt as streaming -> SSE Replay Engine synthesizes chunks from cache!
        ChatCompletionRequest streamReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Explain reactive streams briefly.").build()))
                .temperature(0.1)
                .stream(true)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(streamReq)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "HIT_EXACT")
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("data:");
                    assertThat(body).contains("[DONE]");
                    assertThat(body).contains("Reactive streams");
                });

        // Upstream was still only called once!
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
    }

    @Test
    void testHighTemperatureBypassesSemanticCache() throws Exception {
        ChatCompletionResponse mockResp = ChatCompletionResponse.builder()
                .id("chatcmpl-mock-poem")
                .model("llama-3.1-8b-instant")
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(ChatMessage.builder().role("assistant").content("The blue ocean whispers.").build())
                        .finishReason("stop")
                        .build()))
                .usage(new Usage(8, 6, 14))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(mockResp))));

        ChatCompletionRequest req1 = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Write a poem about the sea").build()))
                .temperature(0.8) // > 0.2 max temperature threshold
                .stream(false)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(req1)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS");

        Thread.sleep(150);
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));

        // Paraphrased query with temperature = 0.8 should NOT match semantic cache
        ChatCompletionRequest req2 = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(ChatMessage.builder().role("user").content("Compose a poem about the sea, please").build()))
                .temperature(0.8) // > 0.2 max temperature threshold
                .stream(false)
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(req2)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Gateway-Cache", "MISS");

        // WireMock received 2 calls because high temperature skips semantic cache!
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
    }
}
