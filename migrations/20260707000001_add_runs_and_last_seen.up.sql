-- Run history: one row per harvest or index run, so mass-failure incidents
-- are visible as "run X: N failed" instead of requiring per-record forensics.
CREATE TABLE runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    source_repository TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    outcome TEXT NOT NULL DEFAULT 'running',
    processed INTEGER NOT NULL DEFAULT 0,
    imported INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    error_sample TEXT NOT NULL DEFAULT '',
    CONSTRAINT runs_kind_check CHECK (kind IN ('harvest', 'index')),
    CONSTRAINT runs_outcome_check CHECK (outcome IN ('running', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at);

-- "Last seen in the OAI feed" for orphan detection. Distinct from
-- last_checked_at, which only moves when a record actually changes: an
-- endpoint that silently drops a record (deletedRecord=no|transient) leaves
-- last_seen_at frozen while other records keep advancing.
ALTER TABLE oai_records
    ADD COLUMN last_seen_at TIMESTAMPTZ NULL;
