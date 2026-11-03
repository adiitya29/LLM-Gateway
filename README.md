# LLM Gateway

A high-performance, non-blocking LLM Gateway built with Java 21 and Spring Boot WebFlux.

## Features
- **OpenAI-Compatible API**: Transparent drop-in replacement (`POST /v1/chat/completions`, `GET /v1/models`).
- **Reactive Streaming Passthrough**: Zero-buffering SSE streaming with backpressure control.
- **Provider Abstraction & Dynamic Routing**: Multi-provider failover chains (Gemini, Groq, OpenAI).
- **Two-Tier Intelligent Caching**: Redis exact-match cache + pgvector HNSW semantic cache ($\ge 0.95$ cosine similarity).
- **Concurrent Budget Enforcement**: Atomic reserve-then-settle budgeting with micro-USD precision.
- **Resilience**: Resilience4j circuit breakers and strict pre-stream fallback guarantees.
- **Distributed Rate Limiting**: Bucket4j integrated with reactive Redis.

---

## Tech Stack
- **Language**: Java 21 (LTS)
- **Framework**: Spring Boot 4.1.x / WebFlux (Reactor Netty)
- **Database**: PostgreSQL with `pgvector`
- **Database Access**: Spring Data R2DBC + `DatabaseClient` (non-blocking)
- **Schema Migrations**: Flyway (synchronous JDBC startup runner)
- **Cache & Quota**: Redis (Reactive Lettuce)
- **Rate Limiting**: Bucket4j
- **Resilience**: Resilience4j Reactor

---

## Quickstart

### 1. Start Infrastructure (PostgreSQL + Redis)
```bash
docker compose up -d
```

### 2. Build the Application
```bash
mvn clean compile
```

### 3. Run Tests
```bash
mvn test
```

### 4. Start Gateway
```bash
mvn spring-boot:run
```

Health endpoint: `http://localhost:8080/actuator/health`

---

## Testing the Gateway

### 1. Key Management API (Phase 3)

#### Create a New Gateway API Key
```bash
curl -X POST http://localhost:8080/v1/admin/keys \
  -H "Content-Type: application/json" \
  -H "X-Admin-Master-Key: dev-admin-key" \
  -d '{
    "name": "Developer Key",
    "allowed_models": ["fast-chat", "smart-chat"],
    "budget_micro_usd": 10000000,
    "rate_limit_rpm": 60
  }'
```
*Returns the full raw key (`sk-gw-...`) **once** at creation.*

#### List All Registered Keys (Prefix Only)
```bash
curl http://localhost:8080/v1/admin/keys \
  -H "X-Admin-Master-Key: dev-admin-key"
```

#### Revoke a Key
```bash
curl -X DELETE http://localhost:8080/v1/admin/keys/{key_id} \
  -H "X-Admin-Master-Key: dev-admin-key"
```

---

### 2. List Available Models / Aliases (Phase 2)
```bash
curl http://localhost:8080/v1/models
```

### cURL (Routing via Alias `fast-chat` -> Groq)
```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-gw-test" \
  -d '{
    "model": "fast-chat",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": false
  }'
```

### cURL (Routing via Alias `gemini-flash` -> Gemini)
```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-gw-test" \
  -d '{
    "model": "gemini-flash",
    "messages": [{"role": "user", "content": "Explain quantum computing in one sentence."}],
    "stream": false
  }'
```

### cURL (Streaming SSE via `fast-chat`)
```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-gw-test" \
  -d '{
    "model": "fast-chat",
    "messages": [{"role": "user", "content": "Tell me a joke."}],
    "stream": true
  }'
```

### 3. Usage & Budget Consumption Query (Phase 4)
```bash
curl http://localhost:8080/v1/usage \
  -H "Authorization: Bearer <your-api-key>"
```
Response:
```json
{
  "api_key_id": "...",
  "key_prefix": "sk-gw-abcd...",
  "budget_micro_usd": 10000000,
  "spent_micro_usd": 1250,
  "remaining_micro_usd": 9998750,
  "budget_usd": 10.0,
  "spent_usd": 0.00125,
  "remaining_usd": 9.99875,
  "total_requests": 5,
  "total_prompt_tokens": 120,
  "total_completion_tokens": 180,
  "total_tokens": 300
}
```

---

### 4. Resilience & Fallback Chains (Phase 5)
- **Programmatic Circuit Breakers**: Powered by Resilience4j with reactive `CircuitBreakerOperator`. Configured per provider with automatic transitions between `CLOSED`, `OPEN`, and `HALF_OPEN`.
- **Pre-Stream Fallback Chains**: If the primary provider encounters an HTTP 5xx, 429, timeout, or circuit breaker open, the gateway transparently fails over to the next configured target before the first token is sent to the client.
- **Provider Locking**: Once the first token/byte is emitted to the client stream, the provider is locked to prevent stream corruption.
- **Usage Enrichment**: Each request log in `usage_logs` tracks `fallback_used` (boolean) and `provider_used` (actual provider that handled the request).

---

### Python SDK (`openai>=1.0`)
```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="sk-gw-test")

# Streaming
stream = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)
for chunk in stream:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
```
Or run the included test script:
```bash
python3 verify_gateway.py
```
