package com.llmgateway.provider;

import com.llmgateway.dto.ChatCompletionChunk;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatCompletionResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Universal contract for LLM provider adapters (Google Gemini, Groq, OpenAI, Mistral, WireMock, etc.).
 */
public interface LlmProvider {

    /**
     * Unique name identifying this provider instance (e.g. "gemini", "groq", "openai").
     */
    String getName();

    /**
     * Executes a non-streaming chat completion call.
     */
    Mono<ChatCompletionResponse> chat(ChatCompletionRequest request);

    /**
     * Executes a reactive streaming chat completion call, emitting typed chunks as they arrive.
     */
    Flux<ChatCompletionChunk> stream(ChatCompletionRequest request);
}
