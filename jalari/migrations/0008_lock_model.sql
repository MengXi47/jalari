ALTER TABLE {job} ADD COLUMN IF NOT EXISTS locked_by UUID;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS lock_token BIGINT;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS queue_key_slot SMALLINT;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS command TEXT;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_state_check;

ALTER TABLE {job} ADD CONSTRAINT job_state_check
    CHECK (state IN ('scheduled', 'enqueued', 'running', 'succeeded', 'failed', 'discarded', 'stopped', 'deleted')) NOT VALID;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_command_check;

ALTER TABLE {job} ADD CONSTRAINT job_command_check CHECK (command IN ('stop', 'cancel')) NOT VALID;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_running_check;

ALTER TABLE {job} ADD CONSTRAINT job_running_check CHECK (
    (state = 'running') = (locked_by IS NOT NULL)
    AND (locked_by IS NULL) = (locked_at IS NULL)
    AND (locked_by IS NULL) = (lock_token IS NULL)
    AND (command IS NULL OR state = 'running')
    AND (queue_key_slot IS NOT NULL) = (state = 'running' AND queue_key IS NOT NULL)
) NOT VALID;

ALTER TABLE {job_history} DROP CONSTRAINT IF EXISTS job_history_state_check;

ALTER TABLE {job_history} ADD CONSTRAINT job_history_state_check
    CHECK (state IN ('scheduled', 'enqueued', 'running', 'succeeded', 'failed', 'discarded', 'stopped', 'deleted')) NOT VALID;

CREATE UNIQUE INDEX IF NOT EXISTS "{prefix}job_job_key_active_idx" ON {job} (job_key)
    WHERE job_key IS NOT NULL AND state IN ('scheduled', 'enqueued', 'running', 'stopped');

DROP INDEX IF EXISTS {schema}."{prefix}job_job_key_held_idx";

CREATE UNIQUE INDEX IF NOT EXISTS "{prefix}job_queue_key_running_idx" ON {job} (queue_key, queue_key_slot)
    WHERE queue_key IS NOT NULL AND state = 'running';

CREATE INDEX IF NOT EXISTS "{prefix}job_running_idx" ON {job} (locked_by)
    WHERE state = 'running';

DROP INDEX IF EXISTS {schema}."{prefix}job_finished_idx";

CREATE INDEX IF NOT EXISTS "{prefix}job_finished_idx" ON {job} (state, updated_at)
    WHERE state IN ('succeeded', 'failed', 'discarded', 'deleted');

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
