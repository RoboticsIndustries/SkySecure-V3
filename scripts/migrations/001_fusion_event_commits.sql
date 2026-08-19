BEGIN;

-- Keep deployment fail-fast rather than waiting indefinitely on an unexpected DDL lock.
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS fusion_event_commits (
    event_id      TEXT PRIMARY KEY,
    event_time    TIMESTAMPTZ NOT NULL,
    icao24        CHAR(6) NOT NULL,
    source        VARCHAR(16) NOT NULL,
    raw_event     BYTEA NOT NULL,
    raw_event_sha256 TEXT NOT NULL,
    committed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Upgrade a table created by an earlier release candidate without rewriting
-- or deleting any existing ledger rows. Legacy rows remain explicitly NULL;
-- every new runtime claim supplies both fields.
ALTER TABLE fusion_event_commits
    ADD COLUMN IF NOT EXISTS raw_event BYTEA,
    ADD COLUMN IF NOT EXISTS raw_event_sha256 TEXT;

CREATE TABLE IF NOT EXISTS fusion_outbox (
    event_id     TEXT PRIMARY KEY REFERENCES fusion_event_commits(event_id),
    topic        TEXT NOT NULL,
    message_key  BYTEA NOT NULL,
    payload      BYTEA NOT NULL,
    delivered_at TIMESTAMPTZ,
    lease_owner  TEXT,
    lease_until  TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE fusion_outbox
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_fusion_outbox_pending
    ON fusion_outbox (event_id) WHERE delivered_at IS NULL;

COMMIT;
