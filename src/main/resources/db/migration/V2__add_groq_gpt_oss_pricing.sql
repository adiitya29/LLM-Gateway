-- Add pricing for active Groq models
INSERT INTO model_pricing (provider, model_name, prompt_token_micro_usd, completion_token_micro_usd)
VALUES
    ('groq', 'openai/gpt-oss-120b', 150000, 600000),
    ('groq', 'openai/gpt-oss-20b', 75000, 300000)
ON CONFLICT (provider, model_name) DO NOTHING;

