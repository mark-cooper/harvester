ALTER TABLE oai_records
    ADD COLUMN index_status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN index_message TEXT NOT NULL DEFAULT '',
    ADD COLUMN index_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN indexed_at TIMESTAMPTZ NULL,
    ADD COLUMN purged_at TIMESTAMPTZ NULL,
    ADD COLUMN index_last_checked_at TIMESTAMPTZ NULL;

-- Legacy indexer runs wrote "indexed" into harvest status.
-- Normalize before adding strict allowed-value checks.
UPDATE oai_records
SET status = 'parsed'
WHERE status = 'indexed';

ALTER TABLE oai_records
    ADD CONSTRAINT oai_records_status_check
        CHECK (status IN ('pending', 'available', 'parsed', 'deleted', 'failed'));

ALTER TABLE oai_records
    ADD CONSTRAINT oai_records_index_status_check
        CHECK (index_status IN ('pending', 'indexed', 'index_failed', 'purged', 'purge_failed'));

CREATE INDEX IF NOT EXISTS idx_oai_records_status_index_status
    ON oai_records(endpoint, metadata_prefix, status, index_status, identifier);
