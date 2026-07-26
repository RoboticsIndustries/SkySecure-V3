-- SkySecure V2 — PostgreSQL + PostGIS Schema

CREATE EXTENSION IF NOT EXISTS postgis;

-- ─── Aircraft tracks (time-series) ─────────────────────────────
CREATE TABLE IF NOT EXISTS track_points (
    time            TIMESTAMPTZ     NOT NULL,
    icao24          CHAR(6)         NOT NULL,
    callsign        VARCHAR(8),
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    altitude_baro   INTEGER,
    altitude_geo    INTEGER,
    velocity        REAL,
    heading         REAL,
    vertical_rate   REAL,
    source          VARCHAR(16),    -- ADSB | MLAT | ACARS | SATELLITE
    confidence      REAL,
    risk_score      SMALLINT,
    classification  VARCHAR(24),    -- CIVILIAN | LIKELY_MILITARY | etc.
    raw_icao        INTEGER,
    on_ground       BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_track_time ON track_points (time DESC);
CREATE INDEX IF NOT EXISTS idx_track_icao ON track_points (icao24, time DESC);
CREATE INDEX IF NOT EXISTS idx_track_geo ON track_points USING GIST (
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)
);

-- ─── Anomaly events ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_events (
    id              BIGSERIAL       PRIMARY KEY,
    time            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    icao24          CHAR(6),
    anomaly_type    VARCHAR(48)     NOT NULL,
    severity        SMALLINT,       -- 1–5
    description     TEXT,
    score_delta     SMALLINT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    resolved        BOOLEAN         DEFAULT FALSE,
    meta            JSONB
);

CREATE INDEX IF NOT EXISTS idx_anomaly_time ON anomaly_events (time DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_icao ON anomaly_events (icao24);
CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly_events (anomaly_type);

-- ─── Aircraft registry (known identities) ──────────────────────
CREATE TABLE IF NOT EXISTS aircraft_registry (
    icao24          CHAR(6)         PRIMARY KEY,
    registration    VARCHAR(16),
    manufacturer    VARCHAR(64),
    model           VARCHAR(64),
    operator        VARCHAR(128),
    owner           VARCHAR(128),
    country         CHAR(2),
    military        BOOLEAN         DEFAULT FALSE,
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    meta            JSONB
);

-- ─── Known military ICAO blocks ────────────────────────────────
CREATE TABLE IF NOT EXISTS military_icao_ranges (
    id              SERIAL          PRIMARY KEY,
    country         CHAR(2)         NOT NULL,
    range_start     INTEGER         NOT NULL,   -- hex as int
    range_end       INTEGER         NOT NULL,
    service         VARCHAR(64),                -- AF, NAVY, ARMY, etc.
    notes           TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_military_range_unique
ON military_icao_ranges (country, range_start, range_end);

-- Seed with known ranges (partial — from public aviation databases)
INSERT INTO military_icao_ranges (country, range_start, range_end, service, notes) VALUES
    ('US', 11399168, 11403263, 'USAF',  'USAF primary block'),
    ('US', 11403264, 11534335, 'DOD',   'US DOD general'),
    ('GB', 4440064,  4444159,  'RAF',   'Royal Air Force'),
    ('FR', 3948544,  3956735,  'FAF',   'French Air Force'),
    ('DE', 3932160,  3948543,  'GAF',   'German Air Force'),
    ('RU', 1048576,  2097151,  'RuAF',  'Russian Federation'),
    ('CN', 7864320,  8388607,  'PLAAF', 'Chinese PLA Air Force')
ON CONFLICT DO NOTHING;

-- ─── ACARS messages ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS acars_messages (
    id              BIGSERIAL       PRIMARY KEY,
    time            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    icao24          CHAR(6),
    registration    VARCHAR(16),
    flight          VARCHAR(8),
    label           VARCHAR(4),
    sublabel        VARCHAR(4),
    message_number  VARCHAR(8),
    content         TEXT,
    frequency       REAL,
    raw             TEXT
);

CREATE INDEX IF NOT EXISTS idx_acars_time ON acars_messages (time DESC);
CREATE INDEX IF NOT EXISTS idx_acars_icao ON acars_messages (icao24);
