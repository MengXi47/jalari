ALTER TABLE {job} ADD COLUMN IF NOT EXISTS fingerprint TEXT;

ALTER TABLE {worker} ADD COLUMN IF NOT EXISTS tasks JSONB;

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
