package com.llmgateway.controller;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.llmgateway.config.GatewayProperties;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import com.llmgateway.dto.ChatMessage;
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
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

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
class ChatCompletionControllerTest {

    private static WireMockServer wireMockServer;

    @Autowired
    private ChatCompletionController chatCompletionController;

    @Autowired
    private GatewayProperties gatewayProperties;

    @Autowired
    private com.llmgateway.service.ProviderRegistry providerRegistry;

    @Autowired
    private org.springframework.web.reactive.function.client.WebClient.Builder webClientBuilder;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    private WebTestClient webTestClient;

    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(0); // Random port
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
        this.webTestClient = WebTestClient.bindToController(chatCompletionController).build();
        wireMockServer.resetAll();

        com.llmgateway.provider.OpenAiCompatibleProvider openai = new com.llmgateway.provider.OpenAiCompatibleProvider(
                "openai",
                "http://localhost:" + wireMockServer.port() + "/v1",
                "test-key",
                java.util.Map.of(),
                webClientBuilder,
                objectMapper
        );
        providerRegistry.registerProvider(openai);
    }

    @Test
    void testNonStreamingChatCompletion() {
        String mockResponseBody = """
                {
                  "id": "chatcmpl-test12345",
                  "object": "chat.completion",
                  "created": 1700000000,
                  "model": "gpt-4o-mini",
                  "choices": [
                    {
                      "index": 0,
                      "message": {
                        "role": "assistant",
                        "content": "Hello! I am an AI assistant running behind the LLM Gateway."
                      },
                      "finish_reason": "stop"
                    }
                  ],
                  "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 15,
                    "total_tokens": 27
                  }
                }
                """;

        wireMockServer.stubFor(post(urlEqualTo("/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(mockResponseBody)));

        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("gpt-4o-mini")
                .messages(List.of(new ChatMessage("user", "Hello world")))
                .stream(false)
                .build();

        ChatCompletionResponse response = webTestClient.post()
                .uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_JSON)
                .expectBody(ChatCompletionResponse.class)
                .returnResult()
                .getResponseBody();

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo("chatcmpl-test12345");
        assertThat(response.getModel()).isEqualTo("gpt-4o-mini");
        assertThat(response.getChoices()).hasSize(1);
        assertThat(response.getChoices().get(0).getMessage().getRole()).isEqualTo("assistant");
        assertThat(response.getChoices().get(0).getMessage().getContentAsString())
                .isEqualTo("Hello! I am an AI assistant running behind the LLM Gateway.");
        assertThat(response.getUsage().getTotalTokens()).isEqualTo(27);

        // Verify upstream was called once
        wireMockServer.verify(postRequestedFor(urlEqualTo("/v1/chat/completions")));
    }

    @Test
    void testStreamingChatCompletionSse() {
        String ssePayload = """
                data: {"id":"chatcmpl-stream-1","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}

                data: {"id":"chatcmpl-stream-1","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":" streaming"},"finish_reason":null}]}

                data: {"id":"chatcmpl-stream-1","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":" world!"},"finish_reason":null}]}

                data: {"id":"chatcmpl-stream-1","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}

                data: [DONE]

                """;

        wireMockServer.stubFor(post(urlEqualTo("/v1/chat/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/event-stream")
                        .withBody(ssePayload)));

        ChatCompletionRequest request = ChatCompletionRequest.builder()
                .model("gpt-4o-mini")
                .messages(List.of(new ChatMessage("user", "Hello streaming")))
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
                .assertNext(sse -> assertThat(sse.data()).contains("\"content\":\"Hello\""))
                .assertNext(sse -> assertThat(sse.data()).contains("\"content\":\" streaming\""))
                .assertNext(sse -> assertThat(sse.data()).contains("\"content\":\" world!\""))
                .assertNext(sse -> assertThat(sse.data()).contains("\"total_tokens\":13"))
                .assertNext(sse -> assertThat(sse.data()).isEqualTo("[DONE]"))
                .verifyComplete();

        // Verify gateway injected stream_options: include_usage = true
        wireMockServer.verify(postRequestedFor(urlEqualTo("/v1/chat/completions"))
                .withRequestBody(matchingJsonPath("$.stream_options.include_usage", equalTo("true"))));
    }
}
