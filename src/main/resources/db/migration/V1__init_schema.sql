-- Enable pgvector and uuid extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- 1. API Keys Table
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    key_prefix VARCHAR(12) NOT NULL,
    key_hash VARCHAR(64) NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    allowed_models TEXT[] DEFAULT '{}',
    budget_micro_usd BIGINT NOT NULL DEFAULT 10000000, -- $10.00 default
    spent_micro_usd BIGINT NOT NULL DEFAULT 0,
    rate_limit_rpm INT NOT NULL DEFAULT 60,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);

-- 2. Model Pricing Table (prices in micro-USD per 1,000,000 tokens)
CREATE TABLE IF NOT EXISTS model_pricing (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider VARCHAR(50) NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    prompt_token_micro_usd INT NOT NULL,
    completion_token_micro_usd INT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_provider_model UNIQUE (provider, model_name)
);

-- Seed initial pricing data (micro-USD per 1M tokens)
INSERT INTO model_pricing (provider, model_name, prompt_token_micro_usd, completion_token_micro_usd)
VALUES
    ('gemini', 'gemini-1.5-flash', 75000, 300000),      -- $0.075 / $0.30 per 1M
    ('gemini', 'gemini-2.0-flash', 100000, 400000),     -- $0.10 / $0.40 per 1M
    ('groq', 'llama-3.3-70b-versatile', 590000, 790000),-- $0.59 / $0.79 per 1M
    ('groq', 'llama-3.1-8b-instant', 50000, 80000),     -- $0.05 / $0.08 per 1M
    ('openai', 'gpt-4o-mini', 150000, 600000),          -- $0.15 / $0.60 per 1M
    ('mock', 'mock-chat', 0, 0)
ON CONFLICT (provider, model_name) DO NOTHING;

-- 3. Usage Logs Table
CREATE TABLE IF NOT EXISTS usage_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_key_id UUID REFERENCES api_keys(id),
    request_id VARCHAR(64) NOT NULL,
    model_alias VARCHAR(100) NOT NULL,
    provider_used VARCHAR(50) NOT NULL,
    actual_model VARCHAR(100) NOT NULL,
    prompt_tokens INT NOT NULL DEFAULT 0,
    completion_tokens INT NOT NULL DEFAULT 0,
    total_tokens INT NOT NULL DEFAULT 0,
    cost_micro_usd BIGINT NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    time_to_first_token_ms INT,
    cache_hit VARCHAR(20) DEFAULT 'NONE',
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    status_code INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_usage_logs_key_time ON usage_logs(api_key_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_logs_request_id ON usage_logs(request_id);

-- 4. Semantic Cache Entries Table
CREATE TABLE IF NOT EXISTS semantic_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scope_hash VARCHAR(64) NOT NULL,
    prompt_text TEXT NOT NULL,
    prompt_embedding vector(768) NOT NULL,
    response_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_semantic_cache_scope ON semantic_cache(scope_hash);
CREATE INDEX IF NOT EXISTS idx_semantic_cache_embedding ON semantic_cache
USING hnsw (prompt_embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

