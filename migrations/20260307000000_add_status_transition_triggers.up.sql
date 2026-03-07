-- Legal status transitions (harvest lifecycle).
-- This trigger fires on all UPDATEs, including ON CONFLICT DO UPDATE paths.
CREATE OR REPLACE FUNCTION check_status_transition() RETURNS trigger AS $$
BEGIN
    IF OLD.status = NEW.status THEN
        RETURN NEW;  -- no-op transitions are always allowed
    END IF;

    IF NOT (
        -- Harvest lifecycle transitions
        (OLD.status = 'pending'   AND NEW.status IN ('available', 'failed'))
        OR (OLD.status = 'available' AND NEW.status IN ('parsed', 'failed'))
        OR (OLD.status = 'failed'    AND NEW.status = 'pending')
        -- Import upsert / retry can reset to pending or deleted from any state
        OR (NEW.status IN ('pending', 'deleted'))
    ) THEN
        RAISE EXCEPTION 'illegal status transition: % -> %', OLD.status, NEW.status;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_status_transition
    BEFORE UPDATE OF status ON oai_records
    FOR EACH ROW
    EXECUTE FUNCTION check_status_transition();

-- Legal index_status transitions.
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
