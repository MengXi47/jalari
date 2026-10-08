ALTER TABLE {job} ADD COLUMN IF NOT EXISTS context JSONB;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS context JSONB;

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
