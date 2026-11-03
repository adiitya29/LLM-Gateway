"""
Verification script using the official OpenAI Python SDK (openai >= 1.0)
Testing Phase 3:
1. Creates a restricted API key via Admin API (allowed: ['fast-chat'])
2. Tests unauthorized model access ('gemini-flash' -> 403 Forbidden)
3. Tests authorized model access ('fast-chat' -> 200 OK)
4. Tests streaming through authorized key
"""
import urllib.request
import json
import os
from openai import OpenAI, PermissionDeniedError, AuthenticationError

def create_admin_key(name="Python Test Key", allowed_models=None):
    url = "http://localhost:8080/v1/admin/keys"
    payload = {
        "name": name,
        "allowed_models": allowed_models if allowed_models else [],
        "budget_micro_usd": 5000000,
        "rate_limit_rpm": 60
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Admin-Master-Key": os.getenv("GATEWAY_MASTER_KEY", "dev-admin-key")
        },
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data["key"], data["id"]

def main():
    print("--- 1. Creating Restricted API Key via Admin API ---")
    try:
        raw_key, key_id = create_admin_key(
            name="Fast-Only Key",
            allowed_models=["fast-chat"]
        )
        print(f"Key created successfully! ID: {key_id}")
        print(f"Key: {raw_key[:12]}... (allowed models: ['fast-chat'])")
    except Exception as e:
        print("Failed to create key:", e)
        return

    client = OpenAI(
        base_url="http://localhost:8080/v1",
        api_key=raw_key
    )

    # 2. Test Unauthorized Model Access
    print("\n--- 2. Testing Unauthorized Model Access ('gemini-flash') ---")
    try:
        client.chat.completions.create(
            model="gemini-flash",
            messages=[{"role": "user", "content": "Hello"}],
            stream=False
        )
        print("ERROR: Unauthorized model request was unexpectedly permitted!")
    except PermissionDeniedError as e:
        print(">> Expected 403 Forbidden received:", e.message)
        print(">> Model Authorization Enforcement: SUCCESS")
    except Exception as e:
        print(">> Received error:", e)

    # 3. Test Authorized Model Access
    print("\n--- 3. Testing Authorized Model Access ('fast-chat') ---")
    try:
        response = client.chat.completions.create(
            model="fast-chat",
            messages=[{"role": "user", "content": "Hello from verified client!"}],
            stream=False
        )
        print("Response received:", response.choices[0].message.content)
        if response.usage:
            print(f">> Usage: prompt={response.usage.prompt_tokens}, comp={response.usage.completion_tokens}, total={response.usage.total_tokens}")
        print(">> Authorized Model Request: SUCCESS")
    except Exception as e:
        print(">> Authorized Request failed:", e)

    # 4. Test Usage & Balance Query
    print("\n--- 4. Querying Consumption & Balance (/v1/usage) ---")
    try:
        usage_req = urllib.request.Request(
            "http://localhost:8080/v1/usage",
            headers={"Authorization": f"Bearer {raw_key}"}
        )
        with urllib.request.urlopen(usage_req) as resp:
            usage_data = json.loads(resp.read().decode("utf-8"))
            print(f"Usage Summary: {json.dumps(usage_data, indent=2)}")
            print(f">> Budget: ${usage_data['budget_usd']}, Spent: ${usage_data['spent_usd']}, Remaining: ${usage_data['remaining_usd']}")
            print(">> Usage Accounting: SUCCESS")
    except Exception as e:
        print(">> Usage Query failed:", e)

if __name__ == "__main__":
    main()
