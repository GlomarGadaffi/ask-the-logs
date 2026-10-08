"""
GlomarGadaffi log source catalog for the BigQuery agent.

Each entry defines the BigQuery schema, domain vocabulary, and example
questions for one sensor pipeline. This catalog IS the schema specification —
it describes target tables that ingest pipelines should conform to.

Two tiers of tables exist in this ecosystem:

  RAW (per-source)
    Each sensor pipeline writes to its own dataset in the native format the
    tool emits. Query raw tables when you need full fidelity for one source.

  UNIFIED (mirkwood)
    wasatch-prospector normalizes all sources into a single emission_events
    table for cross-channel spatiotemporal correlation. Query mirkwood when
    you want cross-channel joins, device fingerprint tracking, or proximity
    analysis.

Pipeline origin for each source key:
  mirkwood      wasatch-prospector (Mirkwood normalizer — all channels fused)
  meshtastic    vandenberg-informant (MQTT → Python → BQ, schema in bq_schema.sql)
  scanner_rf    tysons-archivist / clarksburg-warden (BCD325P2 GLG → Splunk → BQ)
  p25_control   arlington-auditor (trunk-recorder CC → NDJSON → BQ)
  ble_scan      mockingbird-scrivener (ESP32 NimBLE → lawndale-courier NDJSON → Pi → BQ)
  wifi_wardrive ashburn-drifter / groton-tinkerer (GPS wardriver → WiGLE-format → BQ)
  wifi_attacks  mastic-scout (802.11 sniffer → LoRa alert_t → base station → BQ)
  lora_sweep    ashburn-sentry (SX1268 sweep → aspen NDJSON → BQ)
  sip_voip      pocket-dial / ashburn-messenger (SIP call records → BQ)
  lte_cellular  Rayhunter / LTESniffer (IMSI-catcher detect + LTE control plane → BQ)
  adsb          clarksburg-steward (ADS-B DF17 transponder captures → BQ)
"""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class SourceConfig:
    display_name: str
    origin_repos: list[str]
    description: str
    suggested_dataset: str
    domain_prompt: str
    example_questions: list[str]


SOURCE_CATALOG: dict[str, SourceConfig] = {

    # ─────────────────────────────────────────────────────────────────────────
    # MIRKWOOD — unified cross-channel fusion table
    # ─────────────────────────────────────────────────────────────────────────
    # MATERIALIZED VIEWS block: GlomarGadaffi/deanon-demo (wasatch-prospector),
    # EmissionEvent_Schema.md "materialized_views" @ 9a49d44. Names and one-line
    # purposes are verbatim; the repo defines no view SQL or columns.
    "mirkwood": SourceConfig(
        display_name="Mirkwood — All Sources (cross-channel)",
        origin_repos=["wasatch-prospector"],
        description=(
            "Unified emission_events table. Mirkwood normalizes every sensor "
            "pipeline into a single schema with a shared device fingerprint for "
            "cross-channel correlation. Start here for multi-source questions."
        ),
        suggested_dataset="mirkwood",
        domain_prompt="""You are querying the Mirkwood unified emission_events table.
All GlomarGadaffi sensor pipelines normalize into this single table for cross-channel
spatiotemporal correlation. It is the canonical cross-channel schema.

SCHEMA — dataset: mirkwood, table: emission_events
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  event_id          STRING      UUID — unique event identifier
  timestamp         TIMESTAMP   Event occurrence time (UTC)
  ingest_timestamp  TIMESTAMP   When the row was inserted into BQ
  latitude          FLOAT64     Decimal degrees (nullable)
  longitude         FLOAT64     Decimal degrees (nullable)
  accuracy_m        FLOAT64     Location accuracy in metres (nullable)
  location_source   STRING      GPS source label (e.g. 'meshtastic_gps', 'ble_scan',
                                'bcd325p2_gps', 'wardriver_gps', 'rayhunter_gps')
  geohash           STRING      Geohash string for spatial grouping (nullable)
  channel_type      STRING      ENUM: P25_TRUNK | EDACS | MESHTASTIC | BLE | WIFI | BT |
                                SIP | LTE_SNIFFER | IMSI_CATCHER_DETECT | ADS_B | OTHER
  source_tool       STRING      ENUM: BearSentinel | MeshNarc | rfparty | wardriver_rev3 |
                                pocket-dial | LTESniffer | Rayhunter | ADSBSynthetic
  primary_id        STRING      Main identifier for the emitting device or entity.
                                By channel_type:
                                  P25_TRUNK/EDACS  → unit_id (radio terminal, decimal)
                                                     or talkgroup_id
                                  MESHTASTIC       → Meshtastic node ID as "!hex8"
                                                     (e.g. "!a1b2c3d4")
                                  BLE              → Bluetooth MAC address
                                  WIFI/BT          → BSSID or Bluetooth MAC
                                  SIP              → SIP extension or source URI
                                  LTE_SNIFFER      → RNTI (per-cell, temporary) or cell_id
                                  IMSI_CATCHER_DETECT → cell_id of suspect tower
                                  ADS_B            → 24-bit ICAO aircraft address (hex)
  secondary_ids     STRING      JSON array of related identifiers (use JSON_VALUE_ARRAY
                                or UNNEST(JSON_VALUE_ARRAY(...)) to expand)
  device_fingerprint STRING     Stable SHA-256 cross-channel device/person identifier.
                                Enables JOIN across channel_types for the same physical device.
                                Uses clean MAC (BLE/WIFI/BT), hashed node ID (MESHTASTIC),
                                hashed unit_id (P25), hashed TMSI/IMSI (LTE).
  metadata          STRING      JSON object — protocol-specific rich data.
                                Access with JSON_VALUE(metadata, '$.key') or
                                JSON_QUERY(metadata, '$.nested.key').
                                Keys vary by source_tool (see per-source context).
  observed_duration STRING      ISO 8601 duration or NULL
  session_id        STRING      Correlated session — call_id for P25/SIP, NULL for beacons
  tags              STRING      JSON array of semantic tags. Common values:
                                  grant, telemetry, mesh, position, beacon,
                                  wifi, bluetooth, sip, voip, cellular, rogue,
                                  downgrade, surveillance, orbit, low_altitude,
                                  commercial, overpass
  enrichment        STRING      JSON object — WiGLE, OUI, agency enrichment data.
                                Keys: wigle (first_seen, last_seen, total_observations,
                                best_location), oui (manufacturer), agencydb (agency_name)

METADATA KEYS BY SOURCE_TOOL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BearSentinel   grant_type, unit_id, talkgroup_id, patch_id, lcn, system_type, agency_tag, raw_line
MeshNarc       node_id, long_name, short_name, hop_count, rssi, snr, payload_type, text, from_id, to_id, position
rfparty        mac, oui, name, rssi, service_uuids (array)
wardriver_rev3 type (WIFI/BT), ssid, bssid, mac, rssi, vendor, channel, encryption, wigle_data
pocket-dial    extension, call_id, contact, sdp, call_state
LTESniffer     rnti, cell_id, tmsi, imsi_fragment, band, earfcn
Rayhunter      suspicious_event, cell_id, imsi_request, downgrade_detected, anomaly_score
ADSBSynthetic  icao_hex, callsign, altitude_ft, velocity_kts, aircraft_lat, aircraft_lon,
               distance_km, orbit_pattern, icao_surveillance_flag, df, tc_position, tc_velocity

FINGERPRINT LOGIC
━━━━━━━━━━━━━━━━━
device_fingerprint enables cross-channel device correlation. Rows with the same
fingerprint represent observations of the same physical device across sensors.
Example cross-source JOIN:
  SELECT a.timestamp, a.channel_type, a.primary_id,
         b.timestamp AS b_ts, b.channel_type AS b_channel
  FROM mirkwood.emission_events a
  JOIN mirkwood.emission_events b USING (device_fingerprint)
  WHERE a.channel_type = 'BLE'
    AND b.channel_type = 'WIFI'
    AND ABS(TIMESTAMP_DIFF(a.timestamp, b.timestamp, SECOND)) < 300

JSON ACCESS IN BIGQUERY
━━━━━━━━━━━━━━━━━━━━━━━
metadata and secondary_ids are stored as JSON strings in BQ. Use:
  JSON_VALUE(metadata, '$.rssi')             → scalar string
  CAST(JSON_VALUE(metadata, '$.rssi') AS INT64) → scalar number
  JSON_QUERY(metadata, '$.service_uuids')    → JSON array string
  JSON_VALUE_ARRAY(secondary_ids)            → ARRAY<STRING>
  UNNEST(JSON_VALUE_ARRAY(tags)) AS tag      → expand tag array

COMMON QUERY PATTERNS
━━━━━━━━━━━━━━━━━━━━━
Time window:     WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
Single channel:  WHERE channel_type = 'MESHTASTIC'
By source tool:  WHERE source_tool = 'BearSentinel'
Has GPS:         WHERE latitude IS NOT NULL AND longitude IS NOT NULL
Proximity:       WHERE ST_DISTANCE(ST_GEOGPOINT(longitude, latitude),
                         ST_GEOGPOINT(-77.0369, 38.9072)) < 5000  -- within 5km
Device track:    GROUP BY device_fingerprint ORDER BY MIN(timestamp)
Has surveillance flag: WHERE 'surveillance' IN UNNEST(JSON_VALUE_ARRAY(tags))
Session activity: WHERE session_id IS NOT NULL GROUP BY session_id

MATERIALIZED VIEWS
━━━━━━━━━━━━━━━━━━
The Mirkwood schema design names three materialized views over emission_events:
  mv_device_tracks        Tracks per device_fingerprint with first/last seen and geometry
  mv_proximity_pairs      Events within 500m and 5 minutes of each other
  mv_high_activity_zones  Hotspots by channel and time window
The design gives only these one-line purposes. It defines no SQL, no column lists and no
dataset for them, so their columns are unknown here. Before querying one, confirm it
exists in BigQuery and read its columns. If it is missing, answer from emission_events
(device tracks: see "Device track" above; proximity: self-join emission_events with
ST_DISTANCE under 500 m and TIMESTAMP_DIFF under 300 s).
""",
        example_questions=[
            "What channel types have the most events in the last 24 hours?",
            "Show me all devices seen on both BLE and WiFi (same fingerprint)",
            "Find correlated events within 500m and 5 minutes of each other",
            "Which device fingerprints have the highest activity across all channels?",
            "Show me all events tagged 'surveillance' or 'rogue' this week",
            "What is the geographic distribution of events by channel_type?",
            "Find P25 trunked calls that occurred within 2 minutes of a BLE surveillance hit",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # MESHTASTIC — vandenberg-informant passive Meshtastic mesh capture
    # ─────────────────────────────────────────────────────────────────────────
    "meshtastic": SourceConfig(
        display_name="Meshtastic Mesh (vandenberg-informant)",
        origin_repos=["vandenberg-informant"],
        description=(
            "Passive Meshtastic LoRa mesh captures. CLIENT_MUTE node uplinks "
            "mesh traffic over Hologram cellular MQTT; Python subscriber decodes "
            "and streams to BigQuery. Schema is canonical (bq_schema.sql)."
        ),
        suggested_dataset="meshnarc",
        domain_prompt="""You are querying Meshtastic LoRa mesh network captures from the
vandenberg-informant subscriber pipeline (MQTT → Python subscriber → BigQuery).
The schema below is authoritative — defined in bq_schema.sql of the repo.

SCHEMA — dataset: meshnarc
━━━━━━━━━━━━━━━━━━━━━━━━━━
Table: meshnarc.packets
  Partitioned by DATE(rx_timestamp), clustered by (source_protocol, port_num, from_id)

  packet_id        INT64       Meshtastic packet ID (unique per from_id within a short window)
  rx_timestamp     TIMESTAMP   When the gateway received the packet (UTC)
  source_protocol  STRING      'meshtastic' or 'meshcore'
  from_id          STRING      Source node ID as "!aabbccdd" (8-char lowercase hex)
  from_long_name   STRING      Node's human display name from NodeInfo (nullable)
  from_short_name  STRING      4-char callsign (nullable)
  to_id            STRING      Destination node ID ("!ffffffff" = broadcast to all)
  channel_id       STRING      Mesh channel name (e.g. "LongFast", "MediumSlow")
  gateway_id       STRING      MQTT gateway node that uplinked this packet
  hop_limit        INT64       Remaining hop budget (0 = direct RF to gateway)
  hop_start        INT64       Hop budget at the source node (nullable)
  want_ack         BOOL        Source requested delivery acknowledgement
  via_mqtt         BOOL        True = packet traveled via internet before our gateway
  rx_snr           FLOAT64     Signal-to-noise ratio at the gateway (dB; >0 clean, <-10 noise floor)
  rx_rssi          INT64       RSSI at gateway (dBm; -40 strong, -85 typical, -130 noise floor)
  port_num         STRING      Application payload type:
                               TEXT_MESSAGE_APP     — text chat message
                               POSITION_APP         — GPS position report
                               TELEMETRY_APP        — device telemetry (battery, environment)
                               NODEINFO_APP         — node identity announcement
                               NEIGHBORINFO_APP     — neighbor list
                               TRACEROUTE_APP       — route discovery response
                               MAP_REPORT_APP       — map report
  payload_json     STRING      Decoded payload as JSON string (use JSON_VALUE to extract)
  raw_payload_b64  STRING      Base64-encoded raw protobuf bytes (for re-decoding)
  latitude         FLOAT64     GPS latitude in decimal degrees (POSITION_APP only, nullable)
  longitude        FLOAT64     GPS longitude in decimal degrees (nullable)
  altitude         INT64       Altitude in metres (nullable)
  ground_speed     INT64       Speed in m/s (nullable)
  sats_in_view     INT64       GPS satellites visible at source node (nullable)
  precision_bits   INT64       GPS precision descriptor (nullable)
  capture_node_id  STRING      Our receiving gateway node ID
  capture_lat      FLOAT64     Our gateway's fixed latitude (nullable)
  capture_lon      FLOAT64     Our gateway's fixed longitude (nullable)
  ingested_at      TIMESTAMP   BigQuery insertion time

Views (pre-built):
  meshnarc.recent_nodes   — 24h node summary:
      from_id, node_name, packet_count, port_nums[], first_seen, last_seen,
      avg_rssi, avg_snr, last_lat, last_lon

  meshnarc.messages       — TEXT_MESSAGE_APP only:
      rx_timestamp, from_id, from_long_name, to_id, channel_id, message_text,
      rx_rssi, rx_snr, via_mqtt, gateway_id

  meshnarc.positions      — POSITION_APP with non-null GPS:
      rx_timestamp, from_id, from_long_name, latitude, longitude, altitude,
      ground_speed, sats_in_view, rx_rssi, gateway_id

DOMAIN NOTES
━━━━━━━━━━━━
- Node IDs are always "!hex8" (e.g. "!a1b2c3d4"). The broadcast address is "!ffffffff".
- Hops traveled = hop_start - hop_limit (when both non-null). 0 = heard directly.
- rx_rssi is negative dBm. More negative = weaker signal. -85 is typical LoRa range.
- via_mqtt=false means the packet was captured over the air directly from the mesh.
- For TEXT_MESSAGE_APP: JSON_VALUE(payload_json, '$.text') extracts the message.
- For TELEMETRY_APP: battery_level, voltage, channel_utilization, air_util_tx are
  common nested keys under JSON_VALUE(payload_json, '$.device_metrics.battery_level') etc.
- LongFast channel = SF11, 250kHz BW — the default Meshtastic channel.
- channel_id="LongFast" is the main public channel; others are named by the operator.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Active nodes in last hour
SELECT from_id, from_long_name, COUNT(*) AS packets, MAX(rx_rssi) AS best_rssi
FROM meshnarc.packets
WHERE rx_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY from_id, from_long_name ORDER BY packets DESC;

-- Text messages
SELECT rx_timestamp, from_long_name, JSON_VALUE(payload_json, '$.text') AS text
FROM meshnarc.packets
WHERE port_num = 'TEXT_MESSAGE_APP' ORDER BY rx_timestamp DESC LIMIT 50;

-- Position track for a specific node
SELECT rx_timestamp, latitude, longitude, altitude
FROM meshnarc.positions WHERE from_id = '!a1b2c3d4' ORDER BY rx_timestamp;
""",
        example_questions=[
            "What nodes have been active in the last hour?",
            "Show me all text messages from the last 24 hours",
            "Which nodes are farthest from the gateway (weakest RSSI)?",
            "How many packets per port_num type today?",
            "Show me the position track for a specific node",
            "What is the typical hop count distribution on the LongFast channel?",
            "Find any packets that did NOT travel via MQTT (direct RF captures)",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # SCANNER RF — BCD325P2 trunked/conventional scanner (BearSentinel)
    # ─────────────────────────────────────────────────────────────────────────
    "scanner_rf": SourceConfig(
        display_name="BCD325P2 Scanner — RF Hits (tysons-archivist)",
        origin_repos=["tysons-archivist", "clarksburg-warden", "selah-cryptographer"],
        description=(
            "BCD325P2 police scanner telemetry. tysons-archivist streams GLG "
            "responses to Splunk; clarksburg-warden (BearSentinel) visualizes and "
            "archives the same data. Fields come directly from the scanner's remote "
            "protocol GLG command (13-field CSV)."
        ),
        suggested_dataset="scanner",
        domain_prompt="""You are querying BCD325P2 Uniden police scanner RF activity data.
tysons-archivist polls the scanner serial port via GLG (Get Last Global) and streams
JSON-formatted hits. Assumes a pipeline from Splunk or serial ingest into BigQuery.

SCHEMA — dataset: scanner, table: hits
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts                TIMESTAMP   Hit timestamp (UTC, assigned at ingest)
  frequency_or_tgid STRING      Frequency in MHz (e.g. "851.0875") for conventional,
                                or talkgroup ID (decimal) for trunked systems
  modulation        STRING      AM, FM, WFM, NFM, USB, LSB, P25, DMR, NXDN, MO
  attenuation       STRING      ATT on/off status
  ctcss_dcs         STRING      CTCSS tone (Hz, e.g. "127.3") or DCS code ("DCS023"),
                                empty if squelch is carrier-only
  system_name       STRING      System name as programmed in the scanner
  group_name        STRING      Channel group name within the system
  channel_name      STRING      Individual channel name
  squelch_status    STRING      Squelch open/closed indicator
  mute_status       STRING      Mute on/off
  system_tag        STRING      System tag (short label)
  channel_tag       STRING      Channel tag (short label)
  p25_nac           STRING      P25 Network Access Code as hex (e.g. "0x293"),
                                empty for non-P25 systems
  scanner_id        STRING      Which scanner/host (if multiple units in the field)

DOMAIN NOTES
━━━━━━━━━━━━
- GLG only produces output when the scanner is actively receiving. Silence = no rows.
  High activity periods will have dense row clusters; quiet periods have gaps.
- Trunked vs. conventional: on P25/EDACS trunked systems, frequency_or_tgid is the
  talkgroup ID (numeric). On conventional systems it is a frequency in MHz.
- P25 NAC (Network Access Code) is analogous to CTCSS for P25 digital systems.
  It gates access to a P25 site. Different sites on the same system may use different NACs.
- system_name is whatever the operator programmed — treat it as a label, not a canonical ID.
- BearSentinel (clarksburg-warden) adds ANALYST (archive) and SENTINEL (real-time) modes
  on top of this raw data. The same field names apply.
- Talkgroup IDs are 5-digit decimals for most P25 systems (0-65535 for conventional P25,
  up to 16777215 for ISSI-linked systems).

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Most active talkgroups (trunked mode)
SELECT frequency_or_tgid AS talkgroup, COUNT(*) AS hits
FROM scanner.hits
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND modulation IN ('P25', 'DMR', 'NXDN')
GROUP BY talkgroup ORDER BY hits DESC LIMIT 20;

-- P25 encrypted vs. clear traffic
SELECT p25_nac, COUNT(*) AS hits
FROM scanner.hits
WHERE p25_nac != '' GROUP BY p25_nac ORDER BY hits DESC;

-- Activity timeline by system
SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, system_name, COUNT(*) AS hits
FROM scanner.hits GROUP BY hour, system_name ORDER BY hour DESC;
""",
        example_questions=[
            "What are the most active talkgroups in the last 24 hours?",
            "Show me all P25 traffic with a specific NAC code",
            "Which system names have the most hits today?",
            "Show activity by hour for the past week",
            "What modulation types are most common?",
            "Find any emergency or priority channel activity",
            "Show me all hits on a specific frequency",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # P25 CONTROL — arlington-auditor P25 control channel logger
    # ─────────────────────────────────────────────────────────────────────────
    "p25_control": SourceConfig(
        display_name="P25 Control Channel (arlington-auditor)",
        origin_repos=["arlington-auditor", "selah-cryptographer"],
        description=(
            "P25 trunked radio control channel message stream. arlington-auditor "
            "builds trunk-recorder configs from RadioReference, then logs every "
            "TSBK and control channel message as NDJSON. Much richer than scanner "
            "hits — shows grants, registrations, patches, and system identity."
        ),
        suggested_dataset="p25_control",
        domain_prompt="""You are querying P25 trunked radio control channel message logs from
arlington-auditor. This tool monitors the P25 digital control channel (CC) directly,
decoding every TSBK/TDULC message. Much richer than scanner audio hits.

SCHEMA — dataset: p25_control, table: events
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts             TIMESTAMP    ISO 8601 message decode time (UTC)
  sys            STRING       System short name (from trunk-recorder config)
  nac            STRING       Network Access Code as "0x1234" hex string
  type           STRING       Control channel message type. Key types:
                                GRP_V_CH_GRANT       — group voice call grant (audio started)
                                GRP_V_CH_GRANT_UPDT  — grant update (call continuing)
                                UU_V_CH_GRANT        — unit-to-unit voice call
                                UNIT_REG_RSP         — radio registration (unit came online)
                                UNIT_DEREG_ACK       — radio deregistration (unit went offline)
                                GRP_AFF_RSP          — talkgroup affiliation
                                RFSS_STS_BCST        — RF sub-system status (site identity)
                                NET_STS_BCST         — network status (system/WACN identity)
                                CC_BCST              — control channel announcement
                                PATCH_ADD            — talkgroup patch created
                                PATCH_DELETE         — talkgroup patch removed
                                ACK_RSP              — acknowledgement
                                SNDCP_DATA_CH_GRANT  — data channel grant
  opcode         STRING       Raw TSBK opcode as "0x00" hex (nullable)
  opcode_name    STRING       Human-readable opcode name (nullable)
  source         INT64        Unit ID of the transmitting radio (decimal, nullable)
  talkgroup      INT64        Talkgroup ID (decimal, nullable)
  talkgroup_tag  STRING       Alpha tag from talkgroups.csv (human label, nullable)
  freq           FLOAT64      Voice/data channel frequency in MHz (nullable)
  emergency      BOOL         Emergency call flag — officer-down / priority traffic (nullable)
  encrypted      BOOL         Encrypted call flag (nullable)
  duplex         BOOL         Duplex/repeater channel flag (for grants, nullable)
  mode           STRING       Channel mode descriptor (nullable)
  priority       INT64        Call priority level (nullable)
  tdma           BOOL         P25 Phase 2 TDMA (vs. Phase 1 FDMA) (nullable)
  tdma_slot      INT64        TDMA slot number 0 or 1 (P25 Phase 2 only, nullable)
  sys_id         STRING       System ID as hex (nullable, present on RFSS_STS_BCST)
  rfss           INT64        RFSS (RF Sub-System) ID (nullable)
  site_id        INT64        Site ID within the RFSS (nullable)
  wacn           STRING       WACN (Wide Area Communications Network) ID hex (nullable)
  patch          STRING       JSON patch data for PATCH_ADD/DELETE:
                              {"supergroup": N, "ga1": N, "ga2": N, "ga3": N} (nullable)
  meta           STRING       Additional parser metadata string (nullable)

DOMAIN NOTES
━━━━━━━━━━━━
- GRP_V_CH_GRANT rows represent voice call starts. Emergency=true means officer-down
  or life-safety priority. Encrypted=true means the audio is DES/AES encrypted.
- UNIT_REG_RSP rows track radio "check-ins" — when a radio powers on or roams to a site.
  UNIT_DEREG_ACK tracks power-off or deliberate deregistration.
- RFSS_STS_BCST carries sys_id, rfss, and site_id — use these to filter to a specific
  physical P25 site when multiple sites are on the same WACN.
- NAC (Network Access Code) is the P25 equivalent of CTCSS — a 12-bit value (0x000–0xFFF)
  that gates access to a site. Common: 0x293, 0x001. The "all-call" NAC is 0xF7E.
- WACN is the Wide Area Communications Network ID — identifies the top-level P25 network.
  WACN 0xBEE00 is the US FirstNet national broadband network.
- Talkgroup IDs: public-safety systems often publish alpha tags. talkgroup_tag is enriched
  from RadioReference data via the rr_config_gen.py script in arlington-auditor.
- PATCH_ADD creates a temporary "supergroup" merging multiple talkgroups — indicates
  an inter-agency tactical operation.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Voice grants by talkgroup with alpha tags
SELECT talkgroup, talkgroup_tag, COUNT(*) AS calls,
       SUM(CAST(emergency AS INT64)) AS emergency_count,
       SUM(CAST(encrypted AS INT64)) AS encrypted_count
FROM p25_control.events
WHERE type = 'GRP_V_CH_GRANT'
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY talkgroup, talkgroup_tag ORDER BY calls DESC;

-- Emergency events
SELECT ts, source, talkgroup, talkgroup_tag, freq, sys
FROM p25_control.events
WHERE emergency = true ORDER BY ts DESC;

-- Unit registrations (who came online today)
SELECT DISTINCT source, ts, sys
FROM p25_control.events
WHERE type = 'UNIT_REG_RSP'
  AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
""",
        example_questions=[
            "How many voice calls per talkgroup in the last 24 hours?",
            "Show me all emergency calls this week",
            "Which unit IDs are most active (most voice grants)?",
            "Are there any encrypted talkgroups? How much traffic?",
            "Show me all talkgroup patches (PATCH_ADD) — inter-agency operations",
            "What are the site IDs and NACs seen on this system?",
            "Which frequencies are used most for voice grants?",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # BLE SCAN — mockingbird-scrivener BLE device sightings
    # ─────────────────────────────────────────────────────────────────────────
    "ble_scan": SourceConfig(
        display_name="BLE Device Sightings (mockingbird-scrivener)",
        origin_repos=["mockingbird-scrivener", "ashland-porter", "groton-tinkerer"],
        description=(
            "BLE advertisement captures from ESP32-S3 NimBLE scanner nodes. "
            "Devices identified via ashland-porter (BT-SIG company IDs, Apple "
            "Continuity, service UUIDs, surveillance vendor catalog from deflock). "
            "Streamed as NDJSON via lawndale-courier USB to a Pi 4 host for BQ ingest."
        ),
        suggested_dataset="ble_scan",
        domain_prompt="""You are querying BLE (Bluetooth Low Energy) device sighting logs from
mockingbird-scrivener ESP32-S3 scanner nodes. Each row is one BLE advertisement
event, identified using ashland-porter primitives.

SCHEMA — dataset: ble_scan, table: sightings
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts            TIMESTAMP   Wall clock (assigned by Pi 4 host at ingest, UTC)
  ts_mono_ms    INT64       Monotonic milliseconds since ESP32 boot (for ordering
                            within a single sensor session)
  seq           INT64       Sequence number from the lawndale-courier emitter
  mac           STRING      BLE MAC address in XX:XX:XX:XX:XX:XX format (uppercase)
  rand          BOOL        True = random/private address (rotates every ~15 min on
                            iOS/Android); False = public address (stable, device-unique)
  rssi          INT64       RSSI in dBm at the ESP32 scanner (-30 strong, -100 weak)
  name          STRING      Advertised local name (nullable — many devices don't broadcast)
  vendor        STRING      Resolved label: BT-SIG company name, Apple service type, or
                            service UUID name from the ashland-porter tables (nullable)
  surv          STRING      Matched surveillance vendor name from the deflock catalog.
                            Non-null only for devices flagged as likely surveillance
                            hardware (ALPR cameras, gunshot sensors, covert cameras, etc.).
                            NULL for ordinary consumer/commercial BLE devices.
  sensor_id     STRING      Which ESP32 scanner node captured this sighting
  scan_lat      FLOAT64     Scanner's GPS latitude at capture time (nullable)
  scan_lon      FLOAT64     Scanner's GPS longitude at capture time (nullable)

DOMAIN NOTES
━━━━━━━━━━━━
- MAC ADDRESS ROTATION: rand=true devices (iOS, Android, Windows) rotate their MAC
  address approximately every 15 minutes as a privacy measure. This means you CANNOT
  track a specific person over time using their MAC alone when rand=true. Use the
  mirkwood.emission_events device_fingerprint for cross-session correlation instead.
- rand=false devices (older IoT, some BLE beacons, Bluetooth Classic adapters) have
  a stable manufacturer-assigned MAC. The first 3 bytes (OUI) identify the vendor.
- RSSI interpretation: -40 dBm means the device is very close (<1m). -85 dBm is
  about 10-20m in open air. BLE range is typically 10-100m depending on tx power.
- The 'surv' field is from the deflock surveillance vendor catalog. It flags devices
  whose BT-SIG company code or vendor name string matches known ALPR (license plate
  reader), gunshot detection (ShotSpotter), or covert camera vendors. This is a
  heuristic — not all matches are true surveillance devices.
- Apple Continuity frames in vendor field: "AirPods", "HomePod", "iBeacon",
  "FindMy", "Nearby", "AirDrop" indicate Apple device proximity.
- Service UUIDs (16-bit) identify device type. Common: 0x180F (Battery), 0x1800 (Generic
  Access), 0x180A (Device Information), 0x1812 (HID — keyboard/mouse/gamepad).

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Surveillance-flagged devices
SELECT ts, mac, surv, rssi, scan_lat, scan_lon
FROM ble_scan.sightings
WHERE surv IS NOT NULL ORDER BY ts DESC;

-- Most seen vendors
SELECT vendor, COUNT(DISTINCT mac) AS unique_macs, COUNT(*) AS observations
FROM ble_scan.sightings WHERE vendor IS NOT NULL
GROUP BY vendor ORDER BY observations DESC LIMIT 20;

-- RSSI distribution (proximity heatmap proxy)
SELECT CASE WHEN rssi >= -50 THEN 'very_close'
            WHEN rssi >= -70 THEN 'close'
            WHEN rssi >= -85 THEN 'medium'
            ELSE 'far' END AS zone,
       COUNT(*) AS count
FROM ble_scan.sightings GROUP BY zone;
""",
        example_questions=[
            "How many unique devices were seen in the last hour?",
            "Show me all surveillance-flagged devices (surv is not null)",
            "What vendors appear most frequently?",
            "Show devices with very strong RSSI (rssi > -50) — close-range sightings",
            "How many devices use random vs public MAC addresses?",
            "Show me the device list from a specific sensor_id",
            "Plot sightings by hour over the last 48 hours",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # WIFI WARDRIVING — ashburn-drifter / groton-tinkerer
    # ─────────────────────────────────────────────────────────────────────────
    "wifi_wardrive": SourceConfig(
        display_name="WiFi Wardriving (ashburn-drifter / groton-tinkerer)",
        origin_repos=["ashburn-drifter", "groton-tinkerer"],
        description=(
            "GPS-tagged WiFi and Bluetooth scan logs from the ashburn-drifter "
            "wardriver (ESP32 dual-board with GPS on SD card) and groton-tinkerer "
            "(rfparty). WiGLE-compatible format with OUI vendor enrichment."
        ),
        suggested_dataset="wardrive",
        domain_prompt="""You are querying GPS-tagged WiFi access point and Bluetooth device
scan logs from the ashburn-drifter wardriver and groton-tinkerer (rfparty) tools.
Data is in WiGLE-compatible format.

SCHEMA — dataset: wardrive
━━━━━━━━━━━━━━━━━━━━━━━━━━
Table: wardrive.wifi_scans
  ts            TIMESTAMP   Scan timestamp (UTC)
  bssid         STRING      Access point MAC in XX:XX:XX:XX:XX:XX format
  ssid          STRING      Network name (empty string for hidden networks)
  channel       INT64       WiFi channel (1-14 for 2.4GHz, 36-165 for 5GHz, 1-233 for 6GHz)
  band          STRING      '2.4GHz', '5GHz', '6GHz'
  rssi          INT64       RSSI in dBm
  security      STRING      'OPEN', 'WEP', 'WPA', 'WPA2', 'WPA3', 'WPA2/WPA3'
  capabilities  STRING      Raw capability flags (JSON array or space-separated)
  vendor        STRING      OUI-resolved manufacturer name (nullable)
  latitude      FLOAT64     GPS latitude at scan time
  longitude     FLOAT64     GPS longitude at scan time
  altitude_m    FLOAT64     GPS altitude in metres (nullable)
  sensor_id     STRING      Which wardriver unit
  wigle_data    STRING      JSON: WiGLE enrichment (first_seen, last_seen,
                            total_observations, best_location) (nullable)

Table: wardrive.bt_scans
  ts            TIMESTAMP   Scan timestamp (UTC)
  mac           STRING      Bluetooth MAC in XX:XX:XX:XX:XX:XX
  name          STRING      Device name (nullable)
  vendor        STRING      OUI-resolved manufacturer (nullable)
  rssi          INT64       RSSI in dBm
  latitude      FLOAT64     GPS latitude at scan time
  longitude     FLOAT64     GPS longitude at scan time
  sensor_id     STRING      Which wardriver unit

DOMAIN NOTES
━━━━━━━━━━━━
- BSSID is the AP's hardware MAC. The OUI (first 3 bytes) identifies the manufacturer.
  Common OUIs: Ubiquiti (dc:9f:db), Cisco (00:0c:e7), TP-Link (98:da:c4).
- Hidden networks: ssid = '' (empty). They are still detectable by their BSSID beacon.
- WPA3 is the most modern and secure. WEP is deprecated and trivially crackable.
- Channels 1, 6, 11 are the non-overlapping 2.4GHz channels; dense AP deployments
  show contention on these.
- WiGLE enrichment (wigle_data) gives historical context: when this AP was first seen
  globally, how many independent observations exist, and best-quality location.
- groton-tinkerer (rfparty) also captures BLE alongside WiFi — see ble_scan source for BLE.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- WiFi density by channel
SELECT channel, COUNT(DISTINCT bssid) AS unique_aps
FROM wardrive.wifi_scans GROUP BY channel ORDER BY unique_aps DESC;

-- Open networks
SELECT bssid, ssid, vendor, latitude, longitude
FROM wardrive.wifi_scans WHERE security = 'OPEN' ORDER BY ts DESC;

-- WEP networks (legacy, vulnerable)
SELECT bssid, ssid, latitude, longitude
FROM wardrive.wifi_scans WHERE security = 'WEP';
""",
        example_questions=[
            "How many unique SSIDs and BSSIDs were scanned?",
            "Show me all open WiFi networks found",
            "What is the channel utilization distribution on 2.4GHz?",
            "Which vendors (by OUI) are most common in this dataset?",
            "Show me the scan coverage area (lat/lon bounds)",
            "How many WPA3 networks vs WPA2 vs legacy?",
            "Show me any hidden networks (empty SSID)",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # WIFI ATTACKS — mastic-scout 802.11 attack detector
    # ─────────────────────────────────────────────────────────────────────────
    "wifi_attacks": SourceConfig(
        display_name="802.11 Attack Alerts (mastic-scout)",
        origin_repos=["mastic-scout"],
        description=(
            "Distributed 802.11 management frame attack detector. Sensor nodes "
            "flag deauth/flood/evil-twin/EAPOL events; the base station receives "
            "alerts over a LoRa backhaul. Struct: alert_t (17 bytes on the air)."
        ),
        suggested_dataset="wifi_attacks",
        domain_prompt="""You are querying 802.11 wireless attack detection alerts from the
mastic-scout distributed sensor network. Sensor nodes monitor for management frame
attacks and transmit compact alert_t structs over a LoRa backhaul to a base station.

SCHEMA — dataset: wifi_attacks, table: alerts
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts          TIMESTAMP   Wall clock when the base station received the alert (UTC)
  sensor_id   INT64       Sensor node ID (1-byte, identifies which physical sensor)
  sig         STRING      Attack signature type:
                            deauth        — 802.11 deauthentication frame (client DoS)
                            deauth_flood  — sustained deauth burst (>N frames/sec)
                            beacon_flood  — fake beacon/SSID flood
                            auth_flood    — authentication request flood
                            evil_twin     — rogue AP with same SSID as a known network
                            eapol         — 802.1X EAPOL handshake capture attempt
                            jam_suspect   — sustained RX noise, no decodable management frames
  channel     INT64       2.4GHz channel (1–13) where the event was observed
  rssi        INT64       RSSI of the triggering frame at the sensor (dBm)
  src_mac     STRING      addr2 (sender MAC) of the suspect frame (XX:XX:XX:XX:XX:XX)
  count       INT64       Frame rate at trigger time (frames/second, for flood sigs)
  uptime_s    INT64       Sensor uptime in seconds at the time of the alert
  base_id     STRING      Which LoRa base station relayed this alert (nullable)
  sensor_lat  FLOAT64     Sensor's GPS latitude (nullable, if GPS-equipped)
  sensor_lon  FLOAT64     Sensor's GPS longitude (nullable)

DOMAIN NOTES
━━━━━━━━━━━━
- DEAUTH attacks: 802.11 deauthentication frames are unauthenticated — any device can
  send them. A flood (deauth_flood) disconnects clients from an AP (DoS). Classic
  attack vector to force a WPA2 handshake re-capture (PMKID/EAPOL) for cracking.
- EVIL_TWIN: a rogue AP broadcasting the same SSID as a legitimate network to perform
  MITM. The sensor detects this by comparing beacons to a known-good SSID/BSSID list.
- EAPOL: 802.1X authentication frames. Capturing the 4-way handshake allows offline
  WPA2 password attack. The sensor flags when it sees replay/injection of EAPOL.
- count field is meaningful for flood signatures. count < 10 = isolated event;
  count > 100/s = active attack in progress.
- Sensor nodes run on ESP32 with a passive WiFi monitor interface — they don't transmit
  on 2.4GHz (except the LoRa backhaul on 915MHz for the alert uplink).
- LoRa 17-byte payload (alert_t) is compact by design. Timestamps are assigned at the
  base station; uptime_s tracks sensor health.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Attack type frequency
SELECT sig, COUNT(*) AS count FROM wifi_attacks.alerts
GROUP BY sig ORDER BY count DESC;

-- High-rate floods (active attacks)
SELECT ts, sensor_id, sig, channel, src_mac, count
FROM wifi_attacks.alerts WHERE count > 50 ORDER BY ts DESC;

-- Evil twin and EAPOL events (credential-attack indicators)
SELECT ts, sensor_id, src_mac, channel, rssi
FROM wifi_attacks.alerts WHERE sig IN ('evil_twin', 'eapol') ORDER BY ts DESC;
""",
        example_questions=[
            "What attack types have been detected and how often?",
            "Show me all deauth floods with count > 50 frames/sec",
            "Are there any evil_twin or EAPOL (credential attack) events?",
            "Which sensor node sees the most attacks?",
            "Show attack frequency by hour over the past week",
            "Which channels (1-13) have the most attack activity?",
            "Show recent high-RSSI alerts (attacker close to sensor)",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # LORA SWEEP — ashburn-sentry LoRa RF parameter discovery
    # ─────────────────────────────────────────────────────────────────────────
    # I/Q ANALYSIS TIER block: GlomarGadaffi/lora-rf-toolkit (catalog name
    # ashburn-sentry), repo HEAD 7cd52f4:
    #   aspen_sniff.py  @ 8c9e01b  print()-only output. Line 56 `standard_bws =` has
    #                              no value (SyntaxError), so it does not run as published.
    #   aspen_trip.py   @ 57b482a  squelch-triggered complex64 .iq capture
    #   aspen_sweep.ino @ 3d9062f  probe settings behind sweeps.sf / sweeps.bw
    #   README.md       @ 7cd52f4  workflow, heuristic / high-SNR caveat
    # No iq_analysis table or emitter exists. Issue #10 field names are not emitted:
    # estimated_bw_khz appears nowhere; iq_file and estimated_sf exist only as
    # aspen_sniff.py parameters and locals, never as output.
    "lora_sweep": SourceConfig(
        display_name="LoRa RF Sweep (ashburn-sentry / aspen)",
        origin_repos=["ashburn-sentry", "roza-scavenger"],
        description=(
            "LoRa RF reverse-engineering toolkit. aspen_sweep sweeps 902-928 MHz "
            "US ISM with CAD (Channel Activity Detection), then brute-forces "
            "SF/BW/CR probe combinations. Emits JSON events per capture. "
            "Target: goTenna Aspen Grove and similar proprietary LoRa mesh radios."
        ),
        suggested_dataset="lora_rf",
        domain_prompt="""You are querying LoRa RF parameter discovery sweep logs from the
ashburn-sentry / aspen toolkit. The ESP32 + SX1268 radio sweeps the 902-928 MHz US ISM
band using Channel Activity Detection, then probes SF/BW/CR parameter combinations.

SCHEMA — dataset: lora_rf, table: sweeps
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts          TIMESTAMP   Wall clock (assigned at host ingestion, UTC)
  e           STRING      Event type:
                            cad_hit    — channel activity detected (signal present)
                            rx_ok      — clean decode (correct SF/BW/sync word)
                            rx_fail    — packet received but CRC failed (header decoded
                                         so SF/BW are confirmed, sync word may be wrong)
                            sweep_done — full 902-928 MHz sweep completed
                            ready      — sensor initialized
  t_ms        INT64       Millis since ESP32 boot at event time
  sw          INT64       Sweep iteration number (increments after each full sweep)
  freq        FLOAT64     Center frequency in MHz (e.g. 915.250)
  sf          INT64       Spreading Factor: 7–12. Higher = longer range, lower data rate.
                          goTenna hint: SF9 or SF10 per FCC filing 2AB7J-MESH01.
  bw          FLOAT64     Bandwidth in kHz: 125.0, 250.0, or 500.0.
  cr          STRING      Coding Rate: "4/5", "4/6", "4/7", "4/8". Lower = more redundancy.
  sync_word   STRING      Sync word tried: "0x12" (private LoRa), "0x34" (LoRaWAN public).
  rssi        INT64       RSSI of received/detected signal (dBm, nullable)
  snr         FLOAT64     SNR of received signal (dB, nullable)
  crc         BOOL        True = CRC passed (valid decode). False = CRC fail.
  payload_len INT64       Decoded payload length in bytes (nullable)
  payload_hex STRING      First 64 bytes of payload as hex string (nullable)
  sensor_id   STRING      Which sensor unit captured this

DOMAIN NOTES
━━━━━━━━━━━━
- SPREADING FACTOR (SF7-SF12): Higher SF = longer range but slower data rate.
  SF7 ~250 bytes/sec, SF12 ~290 bytes in 2+ seconds. Meshtastic uses SF11 (LongFast).
  goTenna Aspen Grove is suspected to use SF9 or SF10 based on FCC filings.
- BANDWIDTH (BW): 125kHz = standard; 250kHz = double data rate, less range;
  500kHz = maximum rate. Most proprietary LoRa uses 125kHz.
- SYNC WORD: 0x12 = private LoRa network (not LoRaWAN). 0x34 = public LoRaWAN.
  Most non-Meshtastic LoRa radios use 0x12 or a custom sync word.
- CAD (Channel Activity Detection) detects any LoRa preamble energy without decoding.
  A cad_hit at a frequency means SOMETHING is transmitting LoRa there.
- rx_fail with CRC=false but no header error means SF/BW are CONFIRMED (header
  decoded successfully) — only the payload sync/key is wrong. This narrows the search.
- rx_ok = full clean decode = we have all parameters correct including sync word.
- sweepN increments after every full 902-928 MHz pass. Multiple sweeps build up
  a frequency activity map.

I/Q ANALYSIS TIER (aspen_sniff.py) — NOT IN BIGQUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
There is NO BigQuery table and NO emitter for I/Q analysis. Do not query or invent
lora_rf.iq_analysis; lora_rf.sweeps is the only table in this source.
- aspen_trip.py (RTL-SDR) saves squelch-triggered bursts as complex64 .iq files.
- aspen_sniff.py reads one complex64 I/Q file (argument: file path; --rate sample rate
  in Hz, default 2.4e6), runs an STFT, and only PRINTS to stdout. It writes no JSON,
  file or table. Printed values: raw bandwidth (kHz, before snapping); bandwidth
  snapped to the nearest standard LoRa value (125, 250 or 500 kHz); symbol time Ts (ms);
  raw SF; SF rounded to an integer; and a final "TARGET ACQUIRED: SF<n> / BW<n>" line.
- The toolkit README calls the SF estimate heuristic and says it needs high SNR and clear,
  non-overlapping transmissions.
- sf and bw in lora_rf.sweeps are the probe settings aspen_sweep.ino tried, not
  I/Q estimates.
Workflow per the README: sweep for activity, capture raw I/Q on a detected frequency,
then run aspen_sniff.py on it. As published, aspen_sniff.py does not run (SyntaxError at
line 56), so the printed values above are the intended output, not what the file emits
today. Questions about I/Q-derived SF/BW cannot be answered from BigQuery.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- CAD hit frequency distribution
SELECT ROUND(freq, 3) AS frequency_mhz, COUNT(*) AS cad_hits
FROM lora_rf.sweeps WHERE e = 'cad_hit'
GROUP BY frequency_mhz ORDER BY cad_hits DESC LIMIT 20;

-- Successful decodes (rx_ok) — confirmed air interface parameters
SELECT ts, freq, sf, bw, cr, sync_word, payload_len, payload_hex
FROM lora_rf.sweeps WHERE e = 'rx_ok' ORDER BY ts DESC;

-- CRC failures (SF/BW confirmed) — narrowing sync word search
SELECT freq, sf, bw, cr, COUNT(*) AS rx_fail_count
FROM lora_rf.sweeps WHERE e = 'rx_fail'
GROUP BY freq, sf, bw, cr ORDER BY rx_fail_count DESC;
""",
        example_questions=[
            "Which frequencies have the most CAD (channel activity) hits?",
            "Show me all successful decodes (rx_ok) — confirmed LoRa parameters",
            "What SF and BW combinations appear in CRC failures (SF confirmed)?",
            "How many sweep iterations were completed?",
            "Show the frequency activity map across the 902-928 MHz band",
            "Are there any payloads that decoded cleanly? Show the hex",
            "What sync words produced successful decodes?",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # SIP / VOIP — pocket-dial / ashburn-messenger
    # ─────────────────────────────────────────────────────────────────────────
    "sip_voip": SourceConfig(
        display_name="SIP/VoIP Call Records (pocket-dial)",
        # UNVERIFIED (issue #5). Checked against pocket-dial @ 6c0eb17, tincan (was
        # ashburn-messenger) @ b28f41f, resiprocate (was reston-broker) @ 0ba595b and
        # deanon-demo (was wasatch-prospector) @ 2b9c899. None writes to BigQuery or
        # to sip_cdr.calls. The columns are the declared target only. The Mirkwood adapter
        # input (deanon-demo adapters/pocket_dial.py:11-41, tests/test_adapters.py:164-175)
        # reads some of these keys; no code in the repos checked emits the rest.
        # pocket-dial CDR: src/SIP/CallDetailRecord.hpp:6-7,23-40,68-73;
        # CdrRing.cpp:398-400,410-425; CdrArchive.cpp:37-42,81-83,92,106,269;
        # CdrArchive.hpp:22-28,37,101; Helpers/HttpServer.cpp:2927-2932;
        # Helpers/TimeSync.cpp:119; RequestsHandler.cpp:8661-8681; Session.cpp:94.
        # tincan: main/sip_uac.cpp:114,
        # main/poc_config.h:28-31. reConServer CDR: apps/reConServer/CDRFile.cxx:26,39-86.
        # cermak-magnate not read and dropped from origin_repos (issue #5, Drawbridge).
        origin_repos=["pocket-dial", "ashburn-messenger", "reston-broker"],
        description=(
            "UNVERIFIED schema. Intended: SIP call records from the pocket-dial "
            "ESP32-S3 SIP PBX (call result, caller/callee extensions, Call-ID, "
            "duration). No code in pocket-dial, ashburn-messenger or reston-broker "
            "writes to BigQuery or to sip_cdr.calls, so the table and its columns "
            "could not be confirmed."
        ),
        suggested_dataset="sip_cdr",
        domain_prompt="""You are querying SIP/VoIP call records meant to come from the pocket-dial
ESP32-S3 SIP PBX (registrar and call broker). The handset client is ashburn-messenger.

UNVERIFIED SCHEMA. No code in pocket-dial, ashburn-messenger or reston-broker writes
to BigQuery or to a sip_cdr.calls table. The columns below are this catalog's declared
target, not a confirmed table. Before filtering on call_state run
SELECT DISTINCT call_state to see the values that are really there. If the table or a
column does not exist, say so; do not guess other names.

SCHEMA — dataset: sip_cdr, table: calls   (target schema, unverified)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts             TIMESTAMP   Call time (UTC). pocket-dial emits a wall-clock time only
                             in its SD-card CSV (SD-capable eth build; RFC 3339 UTC),
                             and only once its clock has synced. For an answered call
                             it is the moment of answer, not the INVITE. Its /api/cdr
                             and NVS ring carry steady-clock ms since boot, not a date.
  call_id        STRING      SIP Call-ID of the dialog. pocket-dial: SD CSV only,
                             truncated to 64 chars.
  extension      STRING      pocket-dial has caller (From) and callee (To), bare
                             extension strings truncated to 48 chars (anchor/trunk legs
                             can carry other ids). No code defines which of the two
                             maps to extension and which to contact.
  contact        STRING      NOT EMITTED as such: pocket-dial records no Contact header
                             and no SIP URI, only the bare caller/callee strings.
  call_state     STRING      pocket-dial's field is "result", one of: answered, busy,
                             cancelled, unavailable, failed. It never emits RINGING,
                             OK or TERMINATED. One row per dialog, written once at
                             teardown; there are no state-transition rows.
  duration_s     INT64       Whole seconds of talk time (teardown minus answer, hold
                             time included). 0, not NULL, when never answered.
  sdp            STRING      NOT EMITTED: no pocket-dial CDR carries SDP or codec.
  server_id      STRING      NOT EMITTED: no server identifier in any CDR code read.
  latitude       FLOAT64     NOT EMITTED: pocket-dial has no location data.
  longitude      FLOAT64     NOT EMITTED: pocket-dial has no location data.

DOMAIN NOTES
━━━━━━━━━━━━
- pocket-dial records one row per torn-down dialog. call_id identifies that dialog;
  do not expect a chain of state events per call_id.
- pocket-dial's result: busy, cancelled and unavailable come from the Session states
  Busy, Cancel and Unavailable. answered is a final state of Connected, Held or Bye.
  Every other final state, including one that never left Invited, is failed.
- The pocket-dial SD CSV has two more columns not in this schema: reason (free text,
  48 chars max) and direction (internal, inbound or outbound).
- ashburn-messenger (GitHub now calls it tincan) is the handset. It offers only G.711
  mu-law (PCMU). In the files read (app_main.cpp, sip_uac.cpp) it writes no call
  records, only ESP_LOG output.
- reston-broker (GitHub now calls it resiprocate) is a fork of upstream reSIProcate.
  Its CDR writer in the tree is upstream reConServer's CDRFile: a comma-separated file
  with other columns (B2B call id, caller, callee, zones, start, connect, finish,
  durations, disposition ANSWERED/BUSY/NO ANSWER/FAILED, response code). Its codecs
  were not verified.

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Call volume by hour
SELECT TIMESTAMP_TRUNC(ts, HOUR) AS hour, COUNT(*) AS calls
FROM sip_cdr.calls WHERE call_state = 'answered' GROUP BY hour ORDER BY hour;

-- Longest calls
SELECT call_id, extension, duration_s
FROM sip_cdr.calls WHERE call_state = 'answered'
ORDER BY duration_s DESC LIMIT 20;

-- Failed/busy calls (connectivity issues)
SELECT ts, extension, call_state
FROM sip_cdr.calls WHERE call_state IN ('failed', 'busy') ORDER BY ts DESC;
""",
        example_questions=[
            "How many calls were made in the last 24 hours?",
            "What is the average call duration?",
            "Show me calls for one extension (caller/callee mapping unverified)",
            "How many calls failed or got BUSY responses?",
            "Show call volume by hour over the past week",
            "Which extensions are most active?",
            "Are there any very long calls (outliers)?",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # LTE / CELLULAR — Rayhunter IMSI catcher detect + LTESniffer
    # ─────────────────────────────────────────────────────────────────────────
    "lte_cellular": SourceConfig(
        display_name="LTE / Cellular (Rayhunter + LTESniffer)",
        origin_repos=["wasatch-prospector"],
        description=(
            "LTE control plane monitoring and IMSI-catcher detection. "
            "LTESniffer passively captures LTE DCI messages (RNTI, TMSI, cell ID). "
            "Rayhunter detects cell-site simulator anomalies (forced IMSI requests, "
            "downgrade attacks). SYNTHETIC data only in clarksburg-steward; real "
            "capture in wasatch-prospector with real hardware."
        ),
        suggested_dataset="lte_sigint",
        domain_prompt="""You are querying LTE cellular control plane and IMSI-catcher detection
data from LTESniffer (passive LTE DCI capture) and Rayhunter (IMSI catcher detector).

SCHEMA — dataset: lte_sigint
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Table: lte_sigint.lte_captures  (from LTESniffer)
  ts             TIMESTAMP   Capture timestamp (UTC)
  rnti           INT64       Radio Network Temporary Identifier (cell-specific,
                             temporary — not a stable device ID)
  cell_id        STRING      Serving cell E-UTRAN Cell Global ID
  tmsi           STRING      Temporary Mobile Subscriber Identity (nullable —
                             observed in control messages)
  imsi_fragment  STRING      Partial IMSI observed (nullable — indicates IMSI request)
  band           INT64       LTE band number (e.g. 12, 66, 71)
  earfcn         INT64       E-UTRA Absolute Radio Frequency Channel Number
  latitude       FLOAT64     Capture location (nullable)
  longitude      FLOAT64     Capture location (nullable)
  sensor_id      STRING      Which RTL-SDR/UHD capture device

Table: lte_sigint.rayhunter_alerts  (from Rayhunter)
  ts                  TIMESTAMP   Alert timestamp (UTC)
  cell_id             STRING      Suspect cell tower E-UTRAN CGI
  suspicious_event    STRING      Event class:
                                  IMSI_REQUEST       — tower requested subscriber identity
                                  DOWNGRADE_DETECTED — tower forced 2G/3G downgrade
                                  SILENT_CALL        — incoming call never rung (covert locate)
                                  LOCATION_UPDATE    — forced location update (tracking)
  imsi_request        BOOL        True = IMSI was explicitly requested
  downgrade_detected  BOOL        True = network downgrade attack detected
  anomaly_score       FLOAT64     0.0–1.0 confidence that this is a cell-site simulator
  latitude            FLOAT64     Approximate cell location (nullable)
  longitude           FLOAT64     Approximate cell location (nullable)
  sensor_id           STRING      Which Rayhunter device

DOMAIN NOTES
━━━━━━━━━━━━
- RNTI is a cell-specific temporary identifier (16-bit, reused). It identifies a device
  within a cell but changes when the device moves cells or reconnects. NOT stable across time.
- TMSI is also temporary but more persistent (changes at registration updates).
  If TMSI reuse patterns exist across rows, that may indicate tracking.
- imsi_fragment: a partial IMSI visible in certain authentication failure or paging
  messages. A cell-site simulator (IMSI catcher) explicitly requests the IMSI — this
  is the primary indicator in Rayhunter.
- anomaly_score > 0.7 suggests high confidence of IMSI catcher activity.
- EARFCN maps to a specific frequency and band. EARFCN lookup tables exist for
  LTE band → frequency conversion. Common US LTE: Band 12 (700 MHz), Band 66 (1700/2100),
  Band 71 (600 MHz). Band 12/71 are AT&T/T-Mobile rural bands.
- Downgrade attacks force a device from LTE to 2G (GPRS/EDGE) where encryption
  is weaker or absent, enabling call interception.
- All Rayhunter data in clarksburg-steward is SYNTHETIC (simulation only).
  LTESniffer captures in wasatch-prospector may be real (check DISCLAIMER.md).

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- High-anomaly IMSI catcher alerts
SELECT ts, cell_id, suspicious_event, anomaly_score, latitude, longitude
FROM lte_sigint.rayhunter_alerts
WHERE anomaly_score > 0.7 ORDER BY anomaly_score DESC;

-- IMSI request events
SELECT ts, cell_id, imsi_request, downgrade_detected, anomaly_score
FROM lte_sigint.rayhunter_alerts
WHERE imsi_request = true ORDER BY ts DESC;

-- LTE capture activity by band
SELECT band, earfcn, COUNT(DISTINCT rnti) AS unique_devices, COUNT(*) AS frames
FROM lte_sigint.lte_captures GROUP BY band, earfcn ORDER BY frames DESC;
""",
        example_questions=[
            "Show me all Rayhunter alerts with anomaly_score > 0.7",
            "How many IMSI request events have been detected?",
            "Are there any downgrade (2G fallback) attacks in the data?",
            "What LTE bands and cells are in the capture area?",
            "Show anomaly score distribution across all alerts",
            "Which cell IDs appear most frequently in alerts?",
            "How many unique RNTIs were observed per cell?",
        ],
    ),

    # ─────────────────────────────────────────────────────────────────────────
    # ADS-B — clarksburg-steward aviation transponder
    # ─────────────────────────────────────────────────────────────────────────
    "adsb": SourceConfig(
        display_name="ADS-B Aviation Transponders (clarksburg-steward)",
        origin_repos=["clarksburg-steward"],
        description=(
            "ADS-B DF17 Mode-S transponder captures for airspace awareness. "
            "clarksburg-steward's SENTINEL-NODE generates SYNTHETIC scenario data "
            "(commercial overpass, surveillance orbit patterns) for policy "
            "demonstration. NOTE: all data in this source is SYNTHETIC."
        ),
        suggested_dataset="adsb",
        domain_prompt="""You are querying ADS-B aviation transponder data from clarksburg-steward.
IMPORTANT: This dataset contains SYNTHETIC data generated for policy demonstration purposes.
No real ADS-B receiver, no 1090 MHz RF hardware, and no live airspace data are involved.

SCHEMA — dataset: adsb, table: transponders
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ts                     TIMESTAMP   ADS-B frame timestamp (UTC)
  icao_hex               STRING      24-bit ICAO aircraft address (6-char hex, globally unique)
  callsign               STRING      Flight callsign or registration (nullable)
  altitude_ft            INT64       Barometric altitude in feet (nullable)
  velocity_kts           INT64       Ground speed in knots (nullable)
  aircraft_lat           FLOAT64     Aircraft latitude at this frame
  aircraft_lon           FLOAT64     Aircraft longitude at this frame
  distance_km            FLOAT64     Computed distance from observation node in km
  orbit_pattern          BOOL        True = aircraft in a circular orbit pattern
                                     (loitering, not transiting)
  icao_surveillance_flag BOOL        True = ICAO address falls in blocks historically
                                     associated with federal/government aviation assets
                                     (ILLUSTRATIVE ONLY — not authoritative)
  df                     INT64       Downlink Format (17 = ADS-B extended squitter)
  tc_position            INT64       Type Code for position messages (11 = airborne)
  tc_velocity            INT64       Type Code for velocity messages (19)
  raw_weight             FLOAT64     0.0–1.0 salience weight from SENTINEL-NODE engine
  tags                   STRING      JSON array: surveillance | orbit | low_altitude |
                                     commercial | overpass | approach
  scenario               STRING      Scenario type that generated this row:
                                     surveillance_pattern | commercial_overpass | no_aircraft
  node_lat               FLOAT64     Observation node latitude
  node_lon               FLOAT64     Observation node longitude
  sensor_id              STRING      SENTINEL-NODE instance ID

DOMAIN NOTES
━━━━━━━━━━━━
- ADS-B DF17 frames are 112-bit Mode-S transmissions on 1090 MHz, broadcast by
  aircraft every ~1 second. They carry ICAO address, position (TC 9-18), and
  velocity (TC 19). All commercial aircraft in the US must transmit ADS-B Out.
- ICAO address blocks: US aircraft are 0xA00000–0xAFFFFF. The icao_surveillance_flag
  marks addresses in ranges illustratively associated with federal aviation (ISR,
  CBP, DEA, FBI surveillance planes). This is for demonstration only.
- Surveillance pattern indicators: orbit_pattern=true + low altitude (<3000 ft AGL) +
  icao_surveillance_flag=true + raw_weight > 0.7 = high-confidence surveillance orbit.
- Commercial overpass: altitude > 28000 ft, high speed, distance > 25 km, no orbit.
- raw_weight is the SENTINEL-NODE salience engine score (0=noise, 1=strong signal).

EXAMPLE QUERIES
━━━━━━━━━━━━━━━
-- Surveillance pattern detections
SELECT ts, icao_hex, callsign, altitude_ft, velocity_kts, distance_km, raw_weight
FROM adsb.transponders
WHERE orbit_pattern = true AND icao_surveillance_flag = true
ORDER BY raw_weight DESC;

-- Low-altitude aircraft near the node
SELECT ts, icao_hex, callsign, altitude_ft, distance_km, orbit_pattern
FROM adsb.transponders
WHERE altitude_ft < 3000 AND distance_km < 5
ORDER BY ts DESC;

-- Activity summary by scenario type
SELECT scenario, COUNT(DISTINCT icao_hex) AS unique_aircraft, COUNT(*) AS frames,
       AVG(raw_weight) AS avg_salience
FROM adsb.transponders GROUP BY scenario;
""",
        example_questions=[
            "Show me all orbit-pattern aircraft with surveillance ICAO flags",
            "What aircraft were below 3000 ft and within 5 km of the node?",
            "How many unique ICAO addresses appear in the dataset?",
            "What is the distribution of raw_weight (salience) scores?",
            "Show commercial overpass events vs surveillance patterns over time",
            "Which callsigns appear most frequently in orbiting patterns?",
            "Are there any high-salience (raw_weight > 0.8) events?",
        ],
    ),
}


def get_source(key: str) -> SourceConfig | None:
    """Return a SourceConfig by key, or None if not found."""
    return SOURCE_CATALOG.get(key)


def list_sources() -> list[dict]:
    """Return a list of source metadata dicts for the /api/sources endpoint."""
    return [
        {
            "key": k,
            "display_name": v.display_name,
            "description": v.description,
            "suggested_dataset": v.suggested_dataset,
            "origin_repos": v.origin_repos,
            "example_questions": v.example_questions,
        }
        for k, v in SOURCE_CATALOG.items()
    ]
