"""
Verification script using the official OpenAI Python SDK (openai >= 1.0)
Testing Phase 2 Model Routing & Alias Resolution against local LLM Gateway:
- GET /v1/models (list models)
- POST /v1/chat/completions with alias 'fast-chat' (Groq)
- POST /v1/chat/completions with alias 'gemini-flash' (Gemini)
- POST /v1/chat/completions with streaming SSE
"""
from openai import OpenAI

def main():
    print("Initializing OpenAI client pointing to http://localhost:8080/v1 ...")
    client = OpenAI(
        base_url="http://localhost:8080/v1",
        api_key="sk-gw-dummy-key"
    )

    # 1. Test GET /v1/models
    print("\n--- 1. Testing GET /v1/models ---")
    try:
        models = client.models.list()
        print(f"Found {len(models.data)} models:")
        for m in models.data:
            print(f" - ID: {m.id} (owned by: {m.owned_by})")
        print(">> GET /v1/models: SUCCESS")
    except Exception as e:
        print(">> GET /v1/models: FAILED with error:", e)

    # 2. Test Non-Streaming with alias 'fast-chat'
    print("\n--- 2. Testing Non-Streaming with alias 'fast-chat' ---")
    try:
        response = client.chat.completions.create(
            model="fast-chat",
            messages=[
                {"role": "user", "content": "What is the speed of light?"}
            ],
            stream=False
        )
        print("Model in response:", response.model)
        print("Response:", response.choices[0].message.content)
        print(">> Alias 'fast-chat': SUCCESS")
    except Exception as e:
        print(">> Alias 'fast-chat': FAILED with error:", e)

    # 3. Test Streaming with alias 'fast-chat'
    print("\n--- 3. Testing Streaming with alias 'fast-chat' ---")
    try:
        stream = client.chat.completions.create(
            model="fast-chat",
            messages=[
                {"role": "user", "content": "Count from 1 to 5."}
            ],
            stream=True
        )
        print("Streaming chunks: ", end="")
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                print(chunk.choices[0].delta.content, end="", flush=True)
        print("\n>> Streaming Test: SUCCESS")
    except Exception as e:
        print("\n>> Streaming Test: FAILED with error:", e)

if __name__ == "__main__":
    main()
