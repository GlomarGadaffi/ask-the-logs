# ask-the-logs

talk to the logs. AI agent that queries BigQuery datasets via natural language. uses Google Agents Development Kit (ADK) with Gemini LLM to translate user queries into SQL, execute, and explain results.

extended to support the full GlomarGadaffi sensor ecosystem: every capture pipeline (Meshtastic mesh, P25 trunked radio, BLE scanning, 802.11 attack detection, LoRa RF sweeps, SIP/VoIP, LTE cellular, ADS-B) has a domain prompt loaded into the agent at session start, giving Gemini precise schema knowledge so queries go straight to correct SQL.

## supported log sources

| key | display name | origin repo | dataset |
|---|---|---|---|
| `mirkwood` | Mirkwood — All Sources (cross-channel) | deanon-demo | `mirkwood` |
| `meshtastic` | Meshtastic Mesh | meshtap | `meshnarc` |
| `scanner_rf` | BCD325P2 Scanner RF Hits | bcd325-splunk-addon / bcd325-dashboard | `scanner` |
| `p25_control` | P25 Control Channel | p25-trunk-logger | `p25_control` |
| `ble_scan` | BLE Device Sightings | mockingbird-scrivener | `ble_scan` |
| `wifi_wardrive` | WiFi Wardriving | wigle-wardriver / rfparty | `wardrive` |
| `wifi_attacks` | 802.11 Attack Alerts | deauth-detector | `wifi_attacks` |
| `lora_sweep` | LoRa RF Sweep | lora-rf-toolkit | `lora_rf` |
| `sip_voip` | SIP/VoIP Call Records | pocket-dial / pocket-dial-handset | `sip_cdr` |
| `lte_cellular` | LTE / Cellular | Rayhunter / LTESniffer via deanon-demo | `lte_sigint` |
| `adsb` | ADS-B Aviation Transponders | sentinel-node-demo | `adsb` |

## bigquery schema design

### two-tier architecture

**tier 1 — native per-source tables**
each pipeline writes to its own dataset in the format its tool emits:
- `meshnarc.packets` — canonical schema in `meshtap/bq_schema.sql`
- `scanner.hits` — BCD325P2 GLG 13-field telemetry
- `p25_control.events` — TSBK/TDULC control channel messages
- `ble_scan.sightings` — NimBLE advertisement events (lawndale-courier NDJSON)
- `wardrive.wifi_scans` / `wardrive.bt_scans` — GPS-tagged scan logs
- `wifi_attacks.alerts` — alert_t structs relayed via LoRa backhaul
- `lora_rf.sweeps` — CAD hit + rx_ok/rx_fail parameter sweep events
- `sip_cdr.calls` — SIP call detail records
- `lte_sigint.lte_captures` / `lte_sigint.rayhunter_alerts` — LTE + IMSI detect
- `adsb.transponders` — ADS-B DF17 transponder frames (sentinel-node-demo: SYNTHETIC)

**tier 2 — mirkwood unified table**
deanon-demo normalizes all sources into `mirkwood.emission_events` via Python adapter classes. use this table for cross-channel questions:
- `device_fingerprint` — stable SHA-256 identifier enabling JOIN across channel types
- `channel_type` — `P25_TRUNK | EDACS | MESHTASTIC | BLE | WIFI | BT | SIP | LTE_SNIFFER | IMSI_CATCHER_DETECT | ADS_B`
- `metadata` — JSON object, keys vary by `source_tool`
- `tags` — JSON array for semantic filtering (`surveillance`, `emergency`, `orbit`, etc.)

`sources.py` in this repo is the normative schema specification — it encodes the full table structure, column types, domain vocabulary, and example queries for every source.

## integration

```python
from bigquery_agent.agent import create_bigquery_agent

# generic mode — all tables
agent = create_bigquery_agent(
    access_token=user_oauth_token,
    project_id="my-gcp-project",
)

# domain-specific mode — Meshtastic
agent = create_bigquery_agent(
    access_token=user_oauth_token,
    project_id="my-gcp-project",
    default_dataset="meshnarc",
    source_key="meshtastic",
)

# cross-channel Mirkwood
agent = create_bigquery_agent(
    access_token=user_oauth_token,
    project_id="my-gcp-project",
    source_key="mirkwood",
)
```

## api

| endpoint | method | description |
|---|---|---|
| `/api/sources` | GET | catalog of all log sources with schema metadata, descriptions, and example questions |
| `/api/projects` | GET | GCP projects accessible to the authenticated user |
| `/api/datasets` | GET | BigQuery datasets in a project |
| `/api/query` | POST | stream agent response (SSE) — rate limited, 10/minute per authenticated user |
| `/api/reset` | POST | clear session state |

### POST /api/query body

```json
{
  "message": "How many voice grants in the last hour?",
  "project_id": "my-gcp-project",
  "dataset": "p25_control",
  "source_key": "p25_control",
  "session_id": null
}
```

`source_key` is optional. when provided, the agent loads the matching domain prompt with full schema context. when omitted, the agent operates in generic mode and lists all available sources in its instruction.

### SSE event types

| type | payload |
|---|---|
| `session` | `{session_id}` |
| `text` | `{content}` |
| `tool` | `{name}` |
| `error` | `{content}` |
| `done` | `{duration, input_tokens, output_tokens}` |

## web ui

run the server and open `http://localhost:8000`. the config screen lets you pick GCP project, dataset, and log source. selecting a source loads its description and example questions into the welcome screen.

the ui passes `source_key` in the query body for every message in the session. the agent retains session context across messages — you can follow up naturally.

## setup

```bash
pip install -r requirements.txt

# set your Gemini API key
export GOOGLE_API_KEY=your-key-here

# run
uvicorn bigquery_agent.server:app --reload --port 8000
```

then open `http://localhost:8000`, set `CLIENT_ID` in `static/index.html` to your OAuth 2.0 client ID (Web application type, authorized origin `http://localhost:8000`).

## architecture

- **Google ADK** — `LlmAgent` with `BigQueryToolset`, `BigQueryCredentialsConfig`
- **Gemini** — default model `gemini-3-pro-preview` (configurable per `create_bigquery_agent`)
- **FastAPI + SSE** — `/api/query` streams `text/event-stream` events
- **GIS token client** — OAuth implicit flow in the browser; Bearer token passed to API
- **`sources.py`** — `SOURCE_CATALOG` dict; each entry has `domain_prompt` (schema + vocabulary), `suggested_dataset`, `example_questions`

## notes

- `adsb` source data is entirely SYNTHETIC — sentinel-node-demo generates scenario data for policy demonstration, not a real 1090 MHz receiver
- `lte_cellular` Rayhunter captures in sentinel-node-demo are also SYNTHETIC; real captures require physical Rayhunter hardware
- query cost scales with data volume scanned; always include a time filter on partitioned tables (e.g. `WHERE rx_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)`)
- `metadata`, `tags`, `secondary_ids` in `mirkwood.emission_events` are JSON strings; use `JSON_VALUE(col, '$.key')` in BigQuery Standard SQL
