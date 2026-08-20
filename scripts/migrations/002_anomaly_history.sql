-- Expand/backfill/validate migration for durable anomaly history.
-- This file intentionally avoids a single long transaction so populated tables
-- are upgraded in bounded batches and indexes can be built concurrently.
SET lock_timeout = '5s';
SET statement_timeout = '10min';

ALTER TABLE anomaly_events
    ADD COLUMN IF NOT EXISTS event_id TEXT,
    ADD COLUMN IF NOT EXISTS callsign VARCHAR(16),
    ADD COLUMN IF NOT EXISTS layer VARCHAR(4),
    ADD COLUMN IF NOT EXISTS detector VARCHAR(64),
    ADD COLUMN IF NOT EXISTS risk_score SMALLINT;

CREATE OR REPLACE PROCEDURE skysecure_backfill_anomaly_event_ids(batch_size INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    LOOP
        UPDATE anomaly_events target
        SET event_id = 'legacy-' || target.id::text
        WHERE target.ctid IN (
            SELECT source.ctid
            FROM anomaly_events source
            WHERE source.event_id IS NULL
            ORDER BY source.id
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        );
        GET DIAGNOSTICS updated_count = ROW_COUNT;
        COMMIT;
        EXIT WHEN updated_count = 0;
    END LOOP;
END;
$$;

CALL skysecure_backfill_anomaly_event_ids(10000);
DROP PROCEDURE skysecure_backfill_anomaly_event_ids(INTEGER);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'anomaly_events'::regclass
          AND conname = 'anomaly_events_event_id_nn'
    ) THEN
        ALTER TABLE anomaly_events
            ADD CONSTRAINT anomaly_events_event_id_nn
            CHECK (event_id IS NOT NULL) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE anomaly_events
    VALIDATE CONSTRAINT anomaly_events_event_id_nn;
ALTER TABLE anomaly_events
    ALTER COLUMN event_id SET NOT NULL;
ALTER TABLE anomaly_events
    DROP CONSTRAINT IF EXISTS anomaly_events_event_id_nn;

-- Remove debris left by interrupted concurrent builds so reruns can repair them.
SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I.%I', n.nspname, idx.relname)
FROM pg_index i
JOIN pg_class idx ON idx.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = idx.relnamespace
WHERE i.indrelid = 'public.anomaly_events'::regclass
  AND NOT i.indisvalid
  AND idx.relname IN ('idx_anomaly_event_id', 'idx_anomaly_event_id_replay',
                      'idx_anomaly_time', 'idx_anomaly_geo')
\gexec

-- Fresh databases already have a UNIQUE constraint from init.sql. Only build a
-- concurrent index when no valid single-column unique event_id index exists.
SELECT 'CREATE UNIQUE INDEX CONCURRENTLY idx_anomaly_event_id_replay ON public.anomaly_events (event_id)'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid
                       AND a.attname = 'event_id'
                       AND a.attnum = i.indkey[0]
    WHERE i.indrelid = 'public.anomaly_events'::regclass
      AND i.indisunique
      AND i.indisvalid
      AND i.indisready
      AND i.indpred IS NULL
      AND i.indnkeyatts = 1
      AND i.indnatts = 1
)
\gexec

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_anomaly_time
    ON anomaly_events (time DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_anomaly_geo
    ON anomaly_events USING GIST (
        ST_SetSRID(ST_MakePoint(lon, lat), 4326)
    )
    WHERE lat IS NOT NULL AND lon IS NOT NULL;
