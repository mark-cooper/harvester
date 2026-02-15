DROP INDEX IF EXISTS idx_oai_records_status_index_status;

ALTER TABLE oai_records
    DROP CONSTRAINT IF EXISTS oai_records_index_status_check;

ALTER TABLE oai_records
    DROP CONSTRAINT IF EXISTS oai_records_status_check;

ALTER TABLE oai_records
    DROP COLUMN IF EXISTS index_last_checked_at,
    DROP COLUMN IF EXISTS purged_at,
    DROP COLUMN IF EXISTS indexed_at,
    DROP COLUMN IF EXISTS index_attempts,
    DROP COLUMN IF EXISTS index_message,
    DROP COLUMN IF EXISTS index_status;
