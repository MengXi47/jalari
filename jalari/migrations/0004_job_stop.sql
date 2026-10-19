CREATE TABLE IF NOT EXISTS {job_control} (
    job_id BIGINT PRIMARY KEY,
    command TEXT,
    lease BIGINT NOT NULL DEFAULT 0,
    progress JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_control_command_check CHECK (command IN ('stop', 'cancel'))
);

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_state_check;

ALTER TABLE {job} ADD CONSTRAINT job_state_check
    CHECK (state IN ('scheduled', 'enqueued', 'succeeded', 'failed', 'deleted', 'stopped'));

ALTER TABLE {job_history} DROP CONSTRAINT IF EXISTS job_history_state_check;

ALTER TABLE {job_history} ADD CONSTRAINT job_history_state_check
    CHECK (state IN ('scheduled', 'enqueued', 'succeeded', 'failed', 'deleted', 'stopped'));

CREATE UNIQUE INDEX IF NOT EXISTS "{prefix}job_job_key_held_idx" ON {job} (job_key)
    WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued', 'stopped');

DROP INDEX IF EXISTS {schema}."{prefix}job_job_key_idx";

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
