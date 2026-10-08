CREATE TABLE IF NOT EXISTS {job} (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue TEXT NOT NULL DEFAULT 'default',
    task TEXT NOT NULL,
    codec TEXT NOT NULL DEFAULT 'json',
    payload BYTEA NOT NULL,
    state TEXT NOT NULL,
    job_key TEXT,
    queue_key TEXT,
    priority SMALLINT NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    timeout_ms INTEGER,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_state_check CHECK (state IN ('scheduled', 'enqueued', 'succeeded', 'failed', 'deleted'))
);

CREATE INDEX IF NOT EXISTS "{prefix}job_ready_idx" ON {job} (queue, priority DESC, run_at, id)
    WHERE state IN ('scheduled', 'enqueued');

CREATE UNIQUE INDEX IF NOT EXISTS "{prefix}job_job_key_idx" ON {job} (job_key)
    WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued');

CREATE INDEX IF NOT EXISTS "{prefix}job_finished_idx" ON {job} (state, updated_at)
    WHERE state IN ('succeeded', 'failed', 'deleted');

CREATE INDEX IF NOT EXISTS "{prefix}job_queue_key_idx" ON {job} (queue_key)
    WHERE queue_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS {job_history} (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES {job} (id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    error TEXT,
    duration_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_history_state_check CHECK (state IN ('scheduled', 'enqueued', 'succeeded', 'failed', 'deleted'))
);

CREATE INDEX IF NOT EXISTS "{prefix}job_history_job_id_idx" ON {job_history} (job_id);

CREATE INDEX IF NOT EXISTS "{prefix}job_history_created_at_idx" ON {job_history} (created_at);

CREATE TABLE IF NOT EXISTS {recurring} (
    name TEXT PRIMARY KEY,
    cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    task TEXT NOT NULL,
    codec TEXT NOT NULL DEFAULT 'json',
    payload BYTEA NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    max_attempts INTEGER NOT NULL,
    timeout_ms INTEGER,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    managed BOOLEAN NOT NULL DEFAULT FALSE,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "{prefix}recurring_due_idx" ON {recurring} (next_run_at)
    WHERE enabled;

CREATE TABLE IF NOT EXISTS {worker} (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname TEXT NOT NULL,
    pid INTEGER NOT NULL,
    queues JSONB NOT NULL,
    scheduler BOOLEAN NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {config} (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    housekeeping_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    housekeeping_interval INTERVAL NOT NULL DEFAULT '10 minutes',
    succeeded_retention INTERVAL NOT NULL DEFAULT '1 day',
    deleted_retention INTERVAL NOT NULL DEFAULT '7 days',
    failed_retention INTERVAL,
    worker_timeout INTERVAL NOT NULL DEFAULT '5 minutes',
    last_housekeeping_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT config_single_row CHECK (id),
    CONSTRAINT config_housekeeping_interval_check CHECK (housekeeping_interval >= INTERVAL '1 second'),
    CONSTRAINT config_succeeded_retention_check CHECK (succeeded_retention >= INTERVAL '0'),
    CONSTRAINT config_deleted_retention_check CHECK (deleted_retention >= INTERVAL '0'),
    CONSTRAINT config_failed_retention_check CHECK (failed_retention >= INTERVAL '0'),
    CONSTRAINT config_worker_timeout_check CHECK (worker_timeout >= INTERVAL '1 second')
);

INSERT INTO {config} (id) VALUES (TRUE) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS {schema_version} (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT schema_version_single_row CHECK (id)
);

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
