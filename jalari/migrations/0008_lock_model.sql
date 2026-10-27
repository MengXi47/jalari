SELECT job.id FROM {job} AS job
WHERE job.state IN ('scheduled', 'enqueued') AND job.attempts > 0
    AND NOT EXISTS (
        SELECT 1 FROM {job_history} AS history
        WHERE history.job_id = job.id AND history.attempt = job.attempts
            AND history.created_at >= job.updated_at
    )
FOR UPDATE OF job NOWAIT;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS locked_by UUID;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS lock_token BIGINT;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS queue_key_slot SMALLINT;

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS command TEXT;

ALTER TABLE {job_control} ADD COLUMN IF NOT EXISTS command TEXT;

WITH pending AS (
    SELECT job.id, control.command,
        job.state IN ('scheduled', 'enqueued') AND job.attempts > 0 AND NOT EXISTS (
            SELECT 1 FROM {job_history} AS history
            WHERE history.job_id = job.id AND history.attempt = job.attempts
                AND history.created_at >= job.updated_at
        ) AS unfinished
    FROM {job} AS job
    JOIN {job_control} AS control ON control.job_id = job.id
    WHERE control.command IS NOT NULL
        AND (job.state IN ('scheduled', 'enqueued')
            OR (job.state = 'stopped' AND control.command = 'cancel'))
),
settled AS (
    UPDATE {job} AS job
    SET state = CASE WHEN pending.command = 'cancel' THEN 'deleted' ELSE 'stopped' END,
        attempts = CASE WHEN pending.unfinished THEN job.attempts - 1 ELSE job.attempts END,
        updated_at = now()
    FROM pending
    WHERE job.id = pending.id
    RETURNING job.id, job.state, job.attempts
),
cleared AS (
    UPDATE {job_control} SET lease = lease + 1, progress = NULL, updated_at = now()
    WHERE job_id IN (SELECT id FROM settled WHERE state = 'deleted')
)
INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
SELECT id, state, attempts,
    CASE WHEN state = 'deleted' THEN 'the job was asked to cancel'
        ELSE 'the job was asked to stop' END,
    now()
FROM settled;

WITH interrupted AS (
    UPDATE {job} AS job
    SET state = CASE WHEN job.attempts >= job.max_attempts THEN 'failed' ELSE 'scheduled' END,
        last_error = 'the worker stopped during the last attempt',
        run_at = now(),
        updated_at = now()
    WHERE job.state IN ('scheduled', 'enqueued') AND job.attempts > 0
        AND NOT EXISTS (
            SELECT 1 FROM {job_history} AS history
            WHERE history.job_id = job.id AND history.attempt = job.attempts
                AND history.created_at >= job.updated_at
        )
    RETURNING job.id, job.state, job.attempts
),
cleared AS (
    UPDATE {job_control} SET lease = lease + 1, progress = NULL, updated_at = now()
    WHERE job_id IN (SELECT id FROM interrupted WHERE state = 'failed')
)
INSERT INTO {job_history} (job_id, state, attempt, error, created_at)
SELECT id, state, attempts, 'the worker stopped during the last attempt', now()
FROM interrupted;

ALTER TABLE {job_control} DROP COLUMN IF EXISTS command;

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

DROP INDEX IF EXISTS {schema}."{prefix}job_queue_key_started_idx";

DROP INDEX IF EXISTS {schema}."{prefix}job_queue_key_failed_idx";

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
