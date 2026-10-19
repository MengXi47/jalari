CREATE INDEX IF NOT EXISTS "{prefix}job_queue_key_started_idx" ON {job} (queue_key)
    WHERE queue_key IS NOT NULL AND state IN ('scheduled', 'enqueued')
        AND (attempts > 0 OR last_error IS NOT NULL);

CREATE INDEX IF NOT EXISTS "{prefix}job_queue_key_failed_idx" ON {job} (queue_key, updated_at)
    WHERE queue_key IS NOT NULL AND state = 'failed';

DROP INDEX IF EXISTS {schema}."{prefix}job_queue_key_idx";

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
