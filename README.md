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
