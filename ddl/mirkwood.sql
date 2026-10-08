-- Dataset: mirkwood    Table: mirkwood.emission_events
--
-- Origin: GlomarGadaffi/deanon-demo  (sources.py origin_repos name: wasatch-prospector)
-- Commit: 2b9c899281ff741131a7513a0af5e37785d0b20b
--   EmissionEvent_Schema.md    blob f2b45224198fac3cb130b53766c2db7cc6c6872e
--     column names, required flags, defaults, primary key, descriptions
--   adapters/database.py       blob 80c9826df06f7a86f446ff2d3eba5bf636d8a32e
--     the origin's own storage form: secondary_ids, tags, metadata and enrichment
--     are json.dumps() strings in TEXT columns; timestamps are ISO strings
--   adapters/base.py           blob 2fd544bc170fe6b7e5fb67beff27dccf1e3b0c95
--     observed_duration is Optional[str], "ISO 8601 or None"
--
-- The origin schema is written in Postgres types, so each type below is a
-- translation, not a copy:
--   UUID -> STRING          TIMESTAMPTZ -> TIMESTAMP     TEXT -> STRING
--   DOUBLE PRECISION, REAL -> FLOAT64 (BigQuery has one float type)
--   TEXT[], JSONB -> STRING holding JSON text (database.py does this)
--   INTERVAL -> STRING holding an ISO 8601 duration (base.py does this)
--   gen_random_uuid() -> GENERATE_UUID()    NOW() -> CURRENT_TIMESTAMP()
-- The STRING choices for the JSON columns also keep the JSON_VALUE and
-- JSON_VALUE_ARRAY queries documented in sources.py working.
--
-- No PARTITION BY / CLUSTER BY: the origin names none for BigQuery. Its only
-- physical hints are Postgres "recommended_indexes" (timestamp DESC,
-- (channel_type, timestamp), ...), which are not partition or cluster specs.
-- Add them once the owner decides.
--
-- Open divergences in the origin, resolved toward EmissionEvent_Schema.md:
--   * database.py makes ingest_timestamp and enrichment NOT NULL; the schema
--     file does not mark them required. Left nullable here (defaults still apply).
--   * The schema file lists the channel_type and source_tool enums; BigQuery has
--     no enum type and the origin sets no CHECK, so none is enforced here.
--
-- NOT created here: the origin's materialized views mv_device_tracks,
-- mv_proximity_pairs and mv_high_activity_zones (named in EmissionEvent_Schema.md
-- with a one-line description each, no definition).

CREATE SCHEMA IF NOT EXISTS mirkwood;

CREATE TABLE IF NOT EXISTS `mirkwood.emission_events` (
  event_id           STRING     NOT NULL DEFAULT GENERATE_UUID(),  -- UUID, primary key: unique event identifier
  `timestamp`        TIMESTAMP  NOT NULL,                          -- TIMESTAMPTZ, required: event occurrence time (UTC)
  ingest_timestamp   TIMESTAMP  DEFAULT CURRENT_TIMESTAMP(),       -- TIMESTAMPTZ, default NOW(): when the event was ingested
  latitude           FLOAT64,                                      -- DOUBLE PRECISION: decimal degrees
  longitude          FLOAT64,                                      -- DOUBLE PRECISION: decimal degrees
  accuracy_m         FLOAT64,                                      -- REAL: location accuracy in meters
  location_source    STRING,                                       -- TEXT: GPS, meshtastic, wardriver, etc.
  geohash            STRING,                                       -- TEXT: geohash for fast spatial grouping
  channel_type       STRING     NOT NULL,                          -- TEXT, required (enum in schema file)
  source_tool        STRING     NOT NULL,                          -- TEXT, required (enum in schema file)
  primary_id         STRING,                                       -- TEXT: UnitID, TGID, BLE MAC, BSSID, Mesh Node ID, SIP Extension, etc.
  secondary_ids      STRING,                                       -- TEXT[]: related identifiers, stored as a JSON array string
  device_fingerprint STRING,                                       -- TEXT: stable cross-channel device/person identifier
  metadata           STRING     NOT NULL DEFAULT '{}',             -- JSONB, required, default {}: protocol-specific data, JSON string
  observed_duration  STRING,                                       -- INTERVAL: duration of the observed activity, ISO 8601 string
  session_id         STRING,                                       -- TEXT: correlated session (call, conversation, movement track)
  tags               STRING,                                       -- TEXT[]: semantic tags, stored as a JSON array string
  enrichment         STRING     DEFAULT '{}',                      -- JSONB, default {}: WiGLE, OUI, agency data, JSON string
  PRIMARY KEY (event_id) NOT ENFORCED
)
OPTIONS (
  description = 'Unified EmissionEvent schema integrating all GlomarGadaffi repos for cross-channel RF fusion and correlation'
);
