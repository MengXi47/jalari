CREATE INDEX IF NOT EXISTS "{prefix}job_context_idx" ON {job} USING GIN (context jsonb_path_ops)
    WHERE context IS NOT NULL;

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
