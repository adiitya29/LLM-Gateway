package com.llmgateway.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.dto.ChatMessage;
import com.llmgateway.dto.ModelCard;
import com.llmgateway.dto.ModelListResponse;
import com.llmgateway.provider.OpenAiCompatibleProvider;
import com.llmgateway.service.ProviderRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.flyway.enabled=true",
        "spring.r2dbc.url=r2dbc:postgresql://localhost:5433/llmgateway",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "gateway.cache.enabled=false"
})
class ModelRoutingIntegrationTest {

    private static WireMockServer wireMockServer;

    @Autowired
    private ChatCompletionController chatCompletionController;

    @Autowired
    private ModelController modelController;

    @Autowired
    private ProviderRegistry providerRegistry;

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
        this.webTestClient = WebTestClient.bindToController(chatCompletionController, modelController).build();
        wireMockServer.resetAll();

        // Register groq pointing to WireMock /groq/v1
        OpenAiCompatibleProvider groqProvider = new OpenAiCompatibleProvider(
                "groq",
                "http://localhost:" + wireMockServer.port() + "/groq/v1",
                "groq-test-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(groqProvider);

        // Register gemini pointing to WireMock /gemini/v1
        OpenAiCompatibleProvider geminiProvider = new OpenAiCompatibleProvider(
                "gemini",
                "http://localhost:" + wireMockServer.port() + "/gemini/v1",
                "gemini-test-key",
                Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(geminiProvider);
    }

    @Test
    void testListModelsEndpoint() {
        ModelListResponse response = webTestClient.get()
                .uri("/v1/models")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(ModelListResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(response).isNotNull();
        assertThat(response.getObject()).isEqualTo("list");
        assertThat(response.getData()).isNotEmpty();

        List<String> modelIds = response.getData().stream().map(ModelCard::getId).toList();
        assertThat(modelIds).contains("fast-chat", "smart-chat", "gemini-flash", "gpt-4o-mini");
    }

    @Test
    void testGetSingleModelEndpoint() {
        ModelCard card = webTestClient.get()
                .uri("/v1/models/fast-chat")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(ModelCard.class)
                .returnResult()
                .getResponseBody();

        assertThat(card).isNotNull();
        assertThat(card.getId()).isEqualTo("fast-chat");
        assertThat(card.getObject()).isEqualTo("model");
    }

    @Test
    void testRouteFastChatToGroqWithModelRewriting() {
        // Groq upstream mock response
        String groqResponseBody = """
                {
                  "id": "chatcmpl-groq-123",
                  "object": "chat.completion",
                  "created": 1700000000,
                  "model": "llama-3.1-8b-instant",
                  "choices": [
                    {
                      "index": 0,
                      "message": {
                        "role": "assistant",
                        "content": "Fast response from Groq Llama 3.1 8B!"
                      },
                      "finish_reason": "stop"
                    }
                  ],
                  "usage": {
                    "prompt_tokens": 15,
                    "completion_tokens": 10,
                    "total_tokens": 25
                  }
                }
                """;

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(groqResponseBody)));

        // Request with model="fast-chat"
        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Hello Groq")))
                .stream(false)
                .build();

        ChatCompletionResponse response = webTestClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(ChatCompletionResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo("chatcmpl-groq-123");
        // Must reflect original alias back to client
        assertThat(response.getModel()).isEqualTo("fast-chat");
        assertThat(response.getChoices().get(0).getMessage().getContentAsString())
                .isEqualTo("Fast response from Groq Llama 3.1 8B!");

        // Verify Groq received model rewritten to "openai/gpt-oss-120b"
        wireMockServer.verify(postRequestedFor(urlEqualTo("/groq/v1/chat/completions"))
                .withRequestBody(matchingJsonPath("$.model", equalTo("openai/gpt-oss-120b"))));
    }

    @Test
    void testRouteGeminiFlashToGeminiWithModelRewriting() {
        // Gemini upstream mock response
        String geminiResponseBody = """
                {
                  "id": "chatcmpl-gemini-456",
                  "object": "chat.completion",
                  "created": 1700000000,
                  "model": "gemini-1.5-flash",
                  "choices": [
                    {
                      "index": 0,
                      "message": {
                        "role": "assistant",
                        "content": "Hello from Google Gemini Flash!"
                      },
                      "finish_reason": "stop"
                    }
                  ],
                  "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 6,
                    "total_tokens": 14
                  }
                }
                """;

        wireMockServer.stubFor(post(urlEqualTo("/gemini/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(geminiResponseBody)));

        // Request with model="gemini-flash"
        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("gemini-flash")
                .messages(List.of(new ChatMessage("user", "Hello Gemini")))
                .stream(false)
                .build();

        ChatCompletionResponse response = webTestClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody(ChatCompletionResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo("chatcmpl-gemini-456");
        // Must reflect original alias back to client
        assertThat(response.getModel()).isEqualTo("gemini-flash");
        assertThat(response.getChoices().get(0).getMessage().getContentAsString())
                .isEqualTo("Hello from Google Gemini Flash!");

        // Verify Gemini received model rewritten to "gemini-1.5-flash"
        wireMockServer.verify(postRequestedFor(urlEqualTo("/gemini/v1/chat/completions"))
                .withRequestBody(matchingJsonPath("$.model", equalTo("gemini-1.5-flash"))));
    }

    @Test
    void testStreamingWithAliasRouting() {
        String ssePayload = """
                data: {"id":"chatcmpl-str-1","object":"chat.completion.chunk","created":1700000000,"model":"llama-3.1-8b-instant","choices":[{"index":0,"delta":{"role":"assistant","content":"Streaming"},"finish_reason":null}]}

                data: {"id":"chatcmpl-str-1","object":"chat.completion.chunk","created":1700000000,"model":"llama-3.1-8b-instant","choices":[{"index":0,"delta":{"content":" from Groq!"},"finish_reason":null}]}

                data: [DONE]

                """;

        wireMockServer.stubFor(post(urlEqualTo("/groq/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/event-stream")
                        .withBody(ssePayload)));

        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("fast-chat")
                .messages(List.of(new ChatMessage("user", "Stream Groq")))
                .stream(true)
                .build();

        ParameterizedTypeReference<ServerSentEvent<String>> sseType = new ParameterizedTypeReference<>() {};

        Flux<ServerSentEvent<String>> sseFlux = webTestClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(sseType)
                .getResponseBody();

        StepVerifier.create(sseFlux)
                .assertNext(sse -> {
                    assertThat(sse.data()).contains("\"content\":\"Streaming\"");
                    assertThat(sse.data()).contains("\"model\":\"fast-chat\"");
                })
                .assertNext(sse -> {
                    assertThat(sse.data()).contains("\"content\":\" from Groq!\"");
                    assertThat(sse.data()).contains("\"model\":\"fast-chat\"");
                })
                .assertNext(sse -> assertThat(sse.data()).isEqualTo("[DONE]"))
                .verifyComplete();

        // Verify Groq received model rewritten to "openai/gpt-oss-120b"
        wireMockServer.verify(postRequestedFor(urlEqualTo("/groq/v1/chat/completions"))
                .withRequestBody(matchingJsonPath("$.model", equalTo("openai/gpt-oss-120b"))));
    }
}

