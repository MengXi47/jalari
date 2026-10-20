ALTER TABLE {recurring} ALTER COLUMN cron DROP NOT NULL;

ALTER TABLE {recurring} ALTER COLUMN next_run_at DROP NOT NULL;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS interval_count INTEGER;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS interval_unit TEXT;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS anchor TIMESTAMP;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS until_at TIMESTAMPTZ;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS max_runs INTEGER;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS run_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE {recurring} ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

ALTER TABLE {recurring} DROP CONSTRAINT IF EXISTS recurring_rule_check;

ALTER TABLE {recurring} ADD CONSTRAINT recurring_rule_check CHECK (
    (cron IS NOT NULL AND interval_count IS NULL AND interval_unit IS NULL AND anchor IS NULL)
    OR (cron IS NULL AND interval_count > 0 AND anchor IS NOT NULL
        AND interval_unit IN ('seconds', 'minutes', 'hours', 'days', 'weeks', 'months', 'years'))
);

ALTER TABLE {recurring} DROP CONSTRAINT IF EXISTS recurring_finished_check;

ALTER TABLE {recurring} ADD CONSTRAINT recurring_finished_check
    CHECK ((next_run_at IS NULL) = (finished_at IS NOT NULL));

ALTER TABLE {recurring} DROP CONSTRAINT IF EXISTS recurring_max_runs_check;

ALTER TABLE {recurring} ADD CONSTRAINT recurring_max_runs_check
    CHECK (max_runs IS NULL OR max_runs > 0);

INSERT INTO {schema_version} AS existing (id, version) VALUES (TRUE, {version})
    ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version, updated_at = now()
    WHERE existing.version < EXCLUDED.version;
