-- Restore the index lifecycle columns on oai_records from indexer_records.
ALTER TABLE oai_records
    ADD COLUMN index_status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN index_message TEXT NOT NULL DEFAULT '',
    ADD COLUMN index_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN indexed_at TIMESTAMPTZ NULL,
    ADD COLUMN purged_at TIMESTAMPTZ NULL,
    ADD COLUMN index_last_checked_at TIMESTAMPTZ NULL;

UPDATE oai_records r
SET index_status = i.status,
    index_message = i.message,
    index_attempts = i.attempts,
    indexed_at = i.indexed_at,
    purged_at = i.purged_at,
    index_last_checked_at = i.last_checked_at
FROM indexer_records i
WHERE i.record_id = r.id;

DROP TABLE indexer_records;
DROP FUNCTION IF EXISTS check_indexer_status_transition();

ALTER TABLE oai_records
    ADD CONSTRAINT oai_records_index_status_check
        CHECK (index_status IN ('pending', 'indexed', 'index_failed', 'purged', 'purge_failed'));

CREATE INDEX IF NOT EXISTS idx_oai_records_status_index_status
    ON oai_records(endpoint, metadata_prefix, status, index_status, identifier);

CREATE OR REPLACE FUNCTION check_index_status_transition() RETURNS trigger AS $$
BEGIN
    IF OLD.index_status = NEW.index_status THEN
        RETURN NEW;
    END IF;

    IF NOT (
        (OLD.index_status = 'pending'      AND NEW.index_status IN ('indexed', 'index_failed', 'purged', 'purge_failed'))
        OR (OLD.index_status = 'index_failed' AND NEW.index_status IN ('indexed', 'index_failed'))
        OR (OLD.index_status = 'purge_failed' AND NEW.index_status IN ('purged', 'purge_failed'))
        -- Reindex / metadata-success / import-deleted can reset to pending from any state
        OR (NEW.index_status = 'pending')
    ) THEN
        RAISE EXCEPTION 'illegal index_status transition: % -> %', OLD.index_status, NEW.index_status;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_index_status_transition
    BEFORE UPDATE OF index_status ON oai_records
    FOR EACH ROW
    EXECUTE FUNCTION check_index_status_transition();

ALTER TABLE oai_records
    DROP COLUMN id;
