ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS priority SMALLINT NOT NULL DEFAULT 0;

ALTER TABLE {recurring} DROP CONSTRAINT IF EXISTS recurring_priority_check;

ALTER TABLE {recurring} ADD CONSTRAINT recurring_priority_check CHECK (priority BETWEEN -1 AND 2);

ALTER TABLE {job} ADD COLUMN IF NOT EXISTS parent_id BIGINT,
    ADD COLUMN IF NOT EXISTS root_id BIGINT,
    ADD COLUMN IF NOT EXISTS queue_key_limit SMALLINT,
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

UPDATE {job} SET priority = greatest(-1, least(2, priority)) WHERE priority NOT BETWEEN -1 AND 2;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_priority_check;

ALTER TABLE {job} ADD CONSTRAINT job_priority_check CHECK (priority BETWEEN -1 AND 2) NOT VALID;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_lineage_check;

ALTER TABLE {job} ADD CONSTRAINT job_lineage_check
    CHECK ((parent_id IS NULL) = (root_id IS NULL)) NOT VALID;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_queue_key_limit_check;

ALTER TABLE {job} ADD CONSTRAINT job_queue_key_limit_check
    CHECK (queue_key_limit IS NULL OR (queue_key_limit >= 2 AND queue_key IS NOT NULL)) NOT VALID;

ALTER TABLE {job} DROP CONSTRAINT IF EXISTS job_queue_key_slot_check;

ALTER TABLE {job} ADD CONSTRAINT job_queue_key_slot_check
    CHECK (queue_key_slot IS NULL OR (queue_key_slot >= 0 AND queue_key_slot < coalesce(queue_key_limit, 1))) NOT VALID;

CREATE INDEX IF NOT EXISTS "{prefix}job_parent_id_idx" ON {job} (parent_id)
    WHERE parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS "{prefix}job_root_id_idx" ON {job} (root_id)
    WHERE root_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS "{prefix}job_expires_at_idx" ON {job} (expires_at)
    WHERE expires_at IS NOT NULL AND state IN ('scheduled', 'enqueued');

CREATE TABLE IF NOT EXISTS {queue_pause} (
    queue TEXT PRIMARY KEY,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
