package com.llmgateway.resilience;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.controller.AdminKeyController;
import com.llmgateway.controller.ChatCompletionController;
import com.llmgateway.dto.*;
import com.llmgateway.model.UsageLog;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.repository.UsageLogRepository;
import com.llmgateway.security.GatewayAuthFilter;
import com.llmgateway.service.ApiKeyService;
import com.llmgateway.service.ProviderRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "gateway.admin.master-key=test-admin-key",
        "gateway.providers.groq.read-timeout-ms=2000",
        "gateway.providers.gemini.read-timeout-ms=5000"
})
class ResilienceAndFailoverIntegrationTest {

    private static WireMockServer wireMockServer;

    @Autowired
    private ChatCompletionController chatCompletionController;

    @Autowired
    private AdminKeyController adminKeyController;

    @Autowired
    private GatewayAuthFilter gatewayAuthFilter;

    @Autowired
    private ApiKeyService apiKeyService;

    @Autowired
    private ProviderRegistry providerRegistry;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @Autowired
    private UsageLogRepository usageLogRepository;

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

        // Register Groq and Gemini targeting the WireMock server
        OpenAiCompatibleProvider groqProvider = new OpenAiCompatibleProvider(
                "groq",
                "http://localhost:" + wireMockServer.port() + "/groq/v1",
                "groq-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(groqProvider);

        OpenAiCompatibleProvider geminiProvider = new OpenAiCompatibleProvider(
                "gemini",
                "http://localhost:" + wireMockServer.port() + "/gemini/v1",
                "gemini-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(geminiProvider);

        // Reset circuit breakers
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(CircuitBreaker::reset);

        // Create an active test API key with high budget
        CreateApiKeyRequest req = CreateApiKeyRequest.builder()
                .name("Failover Test Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(10_000_000L)
                .rateLimitRpm(100)
                .build();

        ApiKeyCreateResponse keyResp = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(req)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(keyResp).isNotNull();
        this.testApiKey = keyResp.getKey();
    }

    @Test
    void testNonStreamingFailoverOnPrimary503() throws Exception {
        // 1. Primary provider (Groq) fails with 503 Service Unavailable
        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody("{\"error\": \"Groq upstream overloaded\"}")));

        // 2. Secondary provider (Gemini) succeeds with 200 OK
        ChatCompletionResponse geminiResp = ChatCompletionResponse.builder()
                .id("chatcmpl-gemini-fallback")
                .model("gemini-1.5-flash")
                .choices(List.of(
                        Choice.builder()
                                .index(0)
                                .message(new ChatMessage("assistant", "Response from backup provider Gemini!"))
                                .finishReason("stop")
                                .build()
                ))
                .usage(new Usage(15, 25, 40))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/gemini/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(geminiResp))));

        // 3. Request alias 'fast-chat' (targets: [groq, gemini])
        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello failover")))
                .stream(false)
                .build();

        // 4. Verify client transparently receives 200 OK from secondary provider
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.model").isEqualTo("fast-chat") // Preserves requested alias
                .jsonPath("$.choices[0].message.content").isEqualTo("Response from backup provider Gemini!");

        // 5. Verify both WireMock endpoints were hit (Groq tried first, Gemini succeeded)
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/gemini/v1/chat/completions")));
    }

    @Test
    void testStreamingPreStreamFailoverOnPrimary500() throws Exception {
        // 1. Primary provider fails with 500 before sending any SSE chunks
        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withBody("{\"error\": \"Groq internal error\"}")));

        // 2. Secondary provider succeeds with valid SSE stream
        String chunk1 = "data: {\"id\":\"chk-1\",\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\n";
        String chunk2 = "data: {\"id\":\"chk-2\",\"choices\":[{\"delta\":{\"content\":\"from Gemini!\"}}]}\n\n";
        String done = "data: [DONE]\n\n";

        wireMockServer.stubFor(post(urlEqualTo("/gemini/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/event-stream")
                        .withBody(chunk1 + chunk2 + done)));

        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello streaming failover")))
                .stream(true)
                .build();

        // 3. Client receives 200 OK SSE stream transparently
        List<String> chunks = webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(String.class)
                .getResponseBody()
                .collectList()
                .block();

        assertThat(chunks).isNotNull();
        assertThat(chunks).anyMatch(c -> c.contains("from Gemini!"));

        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/gemini/v1/chat/completions")));
    }

    @Test
    void testCircuitBreakerTrippingEnablesInstantFailover() throws Exception {
        CircuitBreaker groqCb = circuitBreakerRegistry.circuitBreaker("groq");

        // Manually trip the circuit breaker for Groq to OPEN state
        groqCb.transitionToOpenState();
        assertThat(groqCb.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Configure Gemini with 200 OK
        ChatCompletionResponse geminiResp = ChatCompletionResponse.builder()
                .id("chatcmpl-cb-test")
                .model("gemini-1.5-flash")
                .choices(List.of(
                        Choice.builder()
                                .index(0)
                                .message(new ChatMessage("assistant", "Instant failover via Circuit Breaker!"))
                                .finishReason("stop")
                                .build()
                ))
                .build();

        wireMockServer.stubFor(post(urlEqualTo("/gemini/v1/chat/completions"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody(objectMapper.writeValueAsString(geminiResp))));

        ChatCompletionRequest chatReq = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Test CB")))
                .stream(false)
                .build();

        // Client call succeeds immediately through secondary provider
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + testApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatReq)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.choices[0].message.content").isEqualTo("Instant failover via Circuit Breaker!");

        // Groq was NEVER called over the network because the circuit breaker short-circuited!
        wireMockServer.verify(0, postRequestedFor(urlEqualTo("/groq/v1/chat/completions")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/gemini/v1/chat/completions")));
    }
}
