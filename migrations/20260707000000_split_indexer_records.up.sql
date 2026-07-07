-- Split the index lifecycle out of oai_records into indexer_records.
-- An indexer_records row exists only when there is index work or history:
-- created/reset to pending when a record becomes parsed (metadata success)
-- or deleted (import).

-- Surrogate key for FK references; the composite business key remains the PK.
ALTER TABLE oai_records
    ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE;

CREATE TABLE indexer_records (
    record_id BIGINT PRIMARY KEY REFERENCES oai_records(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    message TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    indexed_at TIMESTAMPTZ NULL,
    purged_at TIMESTAMPTZ NULL,
    last_checked_at TIMESTAMPTZ NULL,
    CONSTRAINT indexer_records_status_check
        CHECK (status IN ('pending', 'indexed', 'index_failed', 'purged', 'purge_failed'))
);

CREATE INDEX IF NOT EXISTS idx_indexer_records_status ON indexer_records(status);

-- Backfill. Beyond parsed/deleted records, the extra arms preserve index
-- history on records whose harvest later failed (e.g. indexed in Solr but the
-- latest re-harvest failed) -- such rows exist in production data.
INSERT INTO indexer_records (record_id, status, message, attempts, indexed_at, purged_at, last_checked_at)
SELECT id, index_status, index_message, index_attempts, indexed_at, purged_at, index_last_checked_at
FROM oai_records
WHERE status IN ('parsed', 'deleted')
   OR index_status != 'pending'
   OR index_attempts > 0
   OR index_message != ''
   OR indexed_at IS NOT NULL
   OR purged_at IS NOT NULL
   OR index_last_checked_at IS NOT NULL;

-- Move the index transition trigger to indexer_records.
DROP TRIGGER IF EXISTS trg_check_index_status_transition ON oai_records;
DROP FUNCTION IF EXISTS check_index_status_transition();

CREATE OR REPLACE FUNCTION check_indexer_status_transition() RETURNS trigger AS $$
BEGIN
    IF OLD.status = NEW.status THEN
        RETURN NEW;
    END IF;

    IF NOT (
        (OLD.status = 'pending'      AND NEW.status IN ('indexed', 'index_failed', 'purged', 'purge_failed'))
        OR (OLD.status = 'index_failed' AND NEW.status IN ('indexed', 'index_failed'))
        OR (OLD.status = 'purge_failed' AND NEW.status IN ('purged', 'purge_failed'))
        -- Reindex / metadata-success / import-deleted can reset to pending from any state
        OR (NEW.status = 'pending')
    ) THEN
        RAISE EXCEPTION 'illegal index_status transition: % -> %', OLD.status, NEW.status;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_indexer_status_transition
    BEFORE UPDATE OF status ON indexer_records
    FOR EACH ROW
    EXECUTE FUNCTION check_indexer_status_transition();

-- Drop the index lifecycle columns from oai_records.
ALTER TABLE oai_records
    DROP CONSTRAINT IF EXISTS oai_records_index_status_check;

DROP INDEX IF EXISTS idx_oai_records_status_index_status;

ALTER TABLE oai_records
    DROP COLUMN index_last_checked_at,
    DROP COLUMN purged_at,
    DROP COLUMN indexed_at,
    DROP COLUMN index_attempts,
    DROP COLUMN index_message,
    DROP COLUMN index_status;
