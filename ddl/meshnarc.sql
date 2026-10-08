-- Dataset: meshnarc    Table: meshnarc.packets
--
-- Origin: GlomarGadaffi/meshtap  (sources.py origin_repos name: vandenberg-informant)
-- Path:   bq_schema.sql
-- Commit: 7433a60f1f3a6ee32e0dbac03ac8fe9a9c7aec4b  (blob f7b2d4b5ab28dcb2b9220e4e83234442384e1397)
--
-- The CREATE TABLE below is copied from the origin file unchanged: column names,
-- types, NOT NULL, the ingested_at default, PARTITION BY DATE(rx_timestamp),
-- CLUSTER BY source_protocol, port_num, from_id, and the table description.
-- Only CREATE SCHEMA IF NOT EXISTS is added; the origin file says
-- `bq mk --dataset meshnarc` for that step. No location is set because the
-- origin sets none.
--
-- NOT created here: the three views in the origin file (meshnarc.recent_nodes,
-- meshnarc.messages, meshnarc.positions). They are views, not tables.

CREATE SCHEMA IF NOT EXISTS meshnarc;

CREATE TABLE IF NOT EXISTS `meshnarc.packets` (
  packet_id       INT64       NOT NULL,   -- Meshtastic packet ID
  rx_timestamp    TIMESTAMP   NOT NULL,   -- When captured
  source_protocol STRING      NOT NULL,   -- 'meshtastic' | 'meshcore'

  -- Routing
  from_id         STRING,                 -- Source node (hex !aabbccdd)
  from_long_name  STRING,                 -- Node long name if in NodeInfo
  from_short_name STRING,                 -- Node short name
  to_id           STRING,                 -- Destination (!ffffffff = broadcast)
  channel_id      STRING,                 -- Channel name (e.g. LongFast)
  gateway_id      STRING,                 -- MQTT gateway node that uplinked this

  -- Mesh metadata
  hop_limit       INT64,
  hop_start       INT64,
  want_ack        BOOL,
  via_mqtt        BOOL,
  rx_snr          FLOAT64,
  rx_rssi         INT64,

  -- Payload
  port_num        STRING,                 -- TEXT_MESSAGE_APP, POSITION_APP, etc.
  payload_json    STRING,                 -- Decoded payload as JSON
  raw_payload_b64 STRING,                 -- Base64 protobuf bytes

  -- Extracted position (POSITION_APP)
  latitude        FLOAT64,
  longitude       FLOAT64,
  altitude        INT64,
  ground_speed    INT64,
  sats_in_view    INT64,
  precision_bits  INT64,

  -- Capture metadata
  capture_node_id STRING,                 -- Our meshnarc gateway node ID
  capture_lat     FLOAT64,
  capture_lon     FLOAT64,

  ingested_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(rx_timestamp)
CLUSTER BY source_protocol, port_num, from_id
OPTIONS (
  description = 'meshnarc: captured unauthenticated mesh radio packets'
);
