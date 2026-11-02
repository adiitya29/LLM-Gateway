"""
Verification script using the official OpenAI Python SDK (openai >= 1.0)
Testing both non-streaming and streaming against the local LLM Gateway.
"""
import sys
from openai import OpenAI

def main():
    print("Initializing OpenAI client pointing to http://localhost:8080/v1 ...")
    client = OpenAI(
        base_url="http://localhost:8080/v1",
        api_key="sk-gw-dummy-key" # In Phase 1, any key is accepted by proxy
    )

    # 1. Test Non-Streaming
    print("\n--- 1. Testing Non-Streaming Completion ---")
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Say hello in one short sentence."}
            ],
            stream=False
        )
        print("Response ID:", response.id)
        print("Model:", response.model)
        print("Assistant Message:", response.choices[0].message.content)
        if response.usage:
            print(f"Usage: prompt={response.usage.prompt_tokens}, completion={response.usage.completion_tokens}, total={response.usage.total_tokens}")
        print(">> Non-Streaming Test: SUCCESS")
    except Exception as e:
        print(">> Non-Streaming Test: FAILED with error:", e)

    # 2. Test Streaming
    print("\n--- 2. Testing Streaming SSE Completion ---")
    try:
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": "Stream a short response."}
            ],
            stream=True
        )
        print("Stream chunks received: ", end="")
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                print(chunk.choices[0].delta.content, end="", flush=True)
            if chunk.usage:
                print(f"\n[Usage reported in stream: total={chunk.usage.total_tokens}]")
        print("\n>> Streaming Test: SUCCESS")
    except Exception as e:
        print("\n>> Streaming Test: FAILED with error:", e)

if __name__ == "__main__":
    main()

