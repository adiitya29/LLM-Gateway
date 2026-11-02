# LLM Gateway

A high-performance, non-blocking LLM Gateway built with Java 21 and Spring Boot WebFlux.

Features:
- OpenAI-compatible API (`/v1/chat/completions`, `/v1/models`)
- Low-latency SSE streaming passthrough with backpressure
- Multi-provider fallback and Resilience4j circuit breakers
- Two-tier caching (Redis exact match + pgvector semantic match)
- Atomic reserve-then-settle budgeting with micro-USD accuracy
- Bucket4j rate limiting
