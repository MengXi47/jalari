ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS missed TEXT NOT NULL DEFAULT 'once';

ALTER TABLE {recurring} DROP CONSTRAINT IF EXISTS recurring_missed_check;

ALTER TABLE {recurring} ADD CONSTRAINT recurring_missed_check CHECK (missed IN ('once', 'skip'));

CREATE TABLE IF NOT EXISTS {job_step} (
    job_id BIGINT NOT NULL REFERENCES {job} (id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    state TEXT NOT NULL,
    nonce UUID NOT NULL DEFAULT gen_random_uuid(),
    starts INTEGER NOT NULL DEFAULT 1,
    attempt INTEGER NOT NULL,
    lease BIGINT NOT NULL,
    output JSONB,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, key),
    CONSTRAINT job_step_state_check CHECK (state IN ('started', 'done')),
    CONSTRAINT job_step_finished_check CHECK ((state = 'done') = (finished_at IS NOT NULL)),
    CONSTRAINT job_step_key_check CHECK (octet_length(key) BETWEEN 1 AND 255)
);

ALTER TABLE {job_control} ADD COLUMN IF NOT EXISTS progress_seq BIGINT NOT NULL DEFAULT 0;

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
