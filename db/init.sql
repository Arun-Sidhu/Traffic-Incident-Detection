-- init.sql
-- History (append-only)
CREATE TABLE IF NOT EXISTS incident_events (
  id BIGSERIAL PRIMARY KEY,
  incident_id TEXT NOT NULL,
  segment_id  TEXT NOT NULL,
  status      TEXT NOT NULL CHECK (status IN ('open','closed')),
  start_ts    BIGINT NOT NULL,
  end_ts      BIGINT NOT NULL,
  severity            DOUBLE PRECISION,
  baseline_speed_mps  DOUBLE PRECISION,
  observed_speed_mps  DOUBLE PRECISION,
  observed_count      BIGINT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (incident_id, status, end_ts)
);

-- Current (one row per open incident)
CREATE TABLE IF NOT EXISTS current_incidents (
  incident_id TEXT PRIMARY KEY,
  segment_id  TEXT NOT NULL,
  start_ts    BIGINT NOT NULL,
  severity            DOUBLE PRECISION,
  baseline_speed_mps  DOUBLE PRECISION,
  observed_speed_mps  DOUBLE PRECISION,
  observed_count      BIGINT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_current_incidents_segment
  ON current_incidents(segment_id);