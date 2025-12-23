CREATE TABLE IF NOT EXISTS oai_records (
    endpoint TEXT NOT NULL,
    metadata_prefix TEXT NOT NULL,
    identifier TEXT NOT NULL,
    fingerprint TEXT GENERATED ALWAYS AS (
        left(encode(sha256((endpoint || ':' || metadata_prefix || ':' || identifier)::bytea), 'hex'), 24)
    ) STORED,
    datestamp TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}',
    summary TEXT NOT NULL DEFAULT '',
    summary_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', summary)) STORED,
    version INTEGER NOT NULL DEFAULT 1,
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (endpoint, metadata_prefix, identifier)
);

CREATE INDEX IF NOT EXISTS idx_oai_records_status ON oai_records(endpoint, metadata_prefix, status, identifier);
CREATE INDEX IF NOT EXISTS idx_oai_records_metadata ON oai_records USING GIN (metadata);
CREATE INDEX IF NOT EXISTS idx_oai_records_summary_tsv ON oai_records USING GIN (summary_tsv);
