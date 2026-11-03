package com.llmgateway.security;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.controller.AdminKeyController;
import com.llmgateway.controller.ChatCompletionController;
import com.llmgateway.dto.ApiKeyCreateResponse;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatMessage;
import com.llmgateway.dto.CreateApiKeyRequest;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.service.ApiKeyService;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.flyway.enabled=false",
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "gateway.admin.master-key=test-admin-key"
})
class AuthAndApiKeyIntegrationTest {

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
    private WebClient.Builder webClientBuilder;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private GatewayProperties gatewayProperties;

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
        this.webTestClient = WebTestClient.bindToController(chatCompletionController, adminKeyController)
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

        OpenAiCompatibleProvider geminiProvider = new OpenAiCompatibleProvider(
                "gemini",
                "http://localhost:" + wireMockServer.port() + "/gemini/v1",
                "gemini-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(geminiProvider);
    }

    @Test
    void testAdminEndpointsRequireMasterKey() {
        // Missing master key
        webTestClient.post()
                .uri("/v1/admin/keys")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(CreateApiKeyRequest.builder().name("Test").build())
                .exchange()
                .expectStatus().isUnauthorized();

        // Invalid master key
        webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "invalid-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(CreateApiKeyRequest.builder().name("Test").build())
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void testChatCompletionRequiresValidApiKey() {
        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello")))
                .build();

        // Missing Authorization header -> 401
        webTestClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isUnauthorized()
                .expectBody()
                .jsonPath("$.error.code").isEqualTo("missing_api_key");

        // Invalid API Key -> 401
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer invalid-dummy-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isUnauthorized()
                .expectBody()
                .jsonPath("$.error.code").isEqualTo("invalid_api_key");
    }

    @Test
    void testCreateKeyAndEnforceModelAllowlist() {
        // 1. Create a key restricted to only "fast-chat"
        CreateApiKeyRequest createRequest = CreateApiKeyRequest.builder()
                .name("Fast Chat Only Key")
                .allowedModels(List.of("fast-chat"))
                .budgetMicroUsd(5_000_000L)
                .rateLimitRpm(30)
                .build();

        ApiKeyCreateResponse createdKey = webTestClient.post()
                .uri("/v1/admin/keys")
                .header("X-Admin-Master-Key", "test-admin-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(createRequest)
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ApiKeyCreateResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(createdKey).isNotNull();
        assertThat(createdKey.getKey()).isNotBlank();
        assertThat(createdKey.getAllowedModels()).containsExactly("fast-chat");

        String fullApiKey = createdKey.getKey();

        // 2. Request with unauthorized model ("gemini-flash") -> 403 Forbidden
        ChatCompletionRequest unauthorizedRequest = ChatCompletionRequest.builder()
                .model("gemini-flash")
                .messages(List.of(new ChatMessage("user", "Hello Gemini")))
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + fullApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(unauthorizedRequest)
                .exchange()
                .expectStatus().isForbidden()
                .expectBody()
                .jsonPath("$.error.code").isEqualTo("model_not_allowed");

        // 3. Request with authorized model ("fast-chat") -> 200 OK
        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "id": "chatcmpl-test-ok",
                                  "object": "chat.completion",
                                  "created": 1700000000,
                                  "model": "llama-3.1-8b-instant",
                                  "choices": [{"index":0,"message":{"role":"assistant","content":"Allowed!"},"finish_reason":"stop"}]
                                }
                                """)));

        ChatCompletionRequest authorizedRequest = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello Groq")))
                .build();

        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + fullApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(authorizedRequest)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.choices[0].message.content").isEqualTo("Allowed!");

        // 4. Revoke key via Admin API -> 204 No Content
        webTestClient.delete()
                .uri("/v1/admin/keys/" + createdKey.getId())
                .header("X-Admin-Master-Key", "test-admin-key")
                .exchange()
                .expectStatus().isNoContent();

        // 5. Subsequent request with revoked key -> 401 Unauthorized
        webTestClient.post()
                .uri("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + fullApiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(authorizedRequest)
                .exchange()
                .expectStatus().isUnauthorized();
    }
}

