-- Durable store for watch-monitor events (transponder shutoff, emergency
-- squawks, military concentrations, unusual military flight profiles).
-- Idempotent: safe to apply repeatedly to a running production database.
SET lock_timeout = '5s';
SET statement_timeout = '10min';

CREATE TABLE IF NOT EXISTS watch_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id    TEXT NOT NULL,
    time        TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind        VARCHAR(48) NOT NULL,
    icao24      CHAR(6),
    callsign    VARCHAR(16),
    severity    SMALLINT,
    summary     TEXT,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    meta        JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_watch_events_event_id
    ON watch_events (event_id);
CREATE INDEX IF NOT EXISTS idx_watch_events_time
    ON watch_events ("time" DESC);
CREATE INDEX IF NOT EXISTS idx_watch_events_kind
    ON watch_events (kind);
CREATE INDEX IF NOT EXISTS idx_watch_events_icao
    ON watch_events (icao24);
