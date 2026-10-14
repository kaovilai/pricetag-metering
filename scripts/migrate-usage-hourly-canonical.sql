-- Convert an older PriceTag usage_hourly table that included `tool` in its
-- key back to the canonical schema. Tool usage belongs in usage_tool_hourly.
--
-- Run only with Metering scaled down / ingestion drained. The legacy table is
-- retained until validation completes. This migration is idempotent: it exits
-- without changing anything when usage_hourly already has the canonical key.

DO $$
DECLARE
  has_tool BOOLEAN;
  has_tool_key BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'usage_hourly' AND column_name = 'tool'
  ) INTO has_tool;
  SELECT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'public' AND tablename = 'usage_hourly'
      AND indexname = 'usage_hourly_key'
      AND indexdef LIKE '%provider, tool%'
  ) INTO has_tool_key;
  IF NOT has_tool OR NOT has_tool_key THEN
    RAISE NOTICE 'usage_hourly already has the canonical schema; no migration needed';
    RETURN;
  END IF;

  CREATE TABLE usage_hourly_canonical (
    hour TIMESTAMPTZ NOT NULL,
    username TEXT NOT NULL,
    group_name TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT '',
    requests BIGINT NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    cached_input_tokens BIGINT NOT NULL DEFAULT 0,
    cache_creation_tokens BIGINT NOT NULL DEFAULT 0,
    cost_usd NUMERIC(20,8) NOT NULL DEFAULT 0
  );

  INSERT INTO usage_hourly_canonical
    (hour, username, group_name, model, provider, requests, prompt_tokens,
     completion_tokens, total_tokens, cached_input_tokens,
     cache_creation_tokens, cost_usd)
  SELECT hour, username, group_name, model, provider,
         SUM(requests), SUM(prompt_tokens), SUM(completion_tokens),
         SUM(total_tokens), SUM(cached_input_tokens), SUM(cache_creation_tokens),
         SUM(cost_usd)
  FROM usage_hourly
  GROUP BY hour, username, group_name, model, provider;

  ALTER TABLE usage_hourly RENAME TO usage_hourly_tool_legacy;
  ALTER INDEX usage_hourly_key RENAME TO usage_hourly_tool_legacy_key;
  ALTER INDEX IF EXISTS idx_usage_hourly_hour RENAME TO idx_usage_hourly_tool_legacy_hour;
  ALTER INDEX IF EXISTS idx_usage_hourly_user_hour RENAME TO idx_usage_hourly_tool_legacy_user_hour;
  ALTER TABLE usage_hourly_canonical RENAME TO usage_hourly;
  CREATE UNIQUE INDEX usage_hourly_key
    ON usage_hourly (hour, username, group_name, model, provider);
  CREATE INDEX idx_usage_hourly_hour ON usage_hourly (hour);
  CREATE INDEX idx_usage_hourly_user_hour ON usage_hourly (username, hour);
END $$;

-- Validate before dropping the legacy table:
-- SELECT SUM(requests) FROM usage_hourly;
-- Compare with usage_events and check /api/v1/admin/rollups parity.
-- After validation:
-- DROP TABLE usage_hourly_tool_legacy;
