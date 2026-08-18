# energyDataCapture — Implementation Plan (handoff spec)

**Status:** approved design, ready to implement.
**Author of this spec:** kickoff/research session (Claude Fable). **Implementer:** you (Claude Code / Opus).
**Review:** after implementation, an adversarial review pass happens in a separate session. Build to this spec; where the spec is silent, prefer the simplest thing that satisfies the tests in §15. If you find a genuine error in the spec, note it in a `DEVIATIONS.md` at repo root rather than silently diverging.

This document is self-contained: all API research (Leviton cloud, Carrier cloud, existing DynamoDB layout, blackstart inventory) is summarized here so you should not need to re-research it. Source repos are on this machine if you need to verify a detail:

- `~/code/bryantDataCollector` — existing daily Bryant/Carrier energy collector (Lambda + DynamoDB)
- `~/code/blackstart` — panel inventory web app; data file `data/montfort.json`
- Reference reading (already analyzed; clone again only if needed): `rwoldberg/ldata-ha` and `gtxaspec/leviton-load-center` + its library `gtxaspec/aioleviton` on GitHub

---

## 1. Mission

Collect household energy + HVAC time-series from cloud sources, buffer locally on a Mac Mini (Docker), land in S3 as partitioned Parquet. Queryable by DuckDB locally, Athena remotely, and readable by an LLM over MCP ("what did the heat pump draw during last Tuesday's hot afternoon"). Optimize for **query ergonomics and data trustworthiness**, not throughput — volume is tens of MB/year. Nothing is performance-sensitive.

Cardinal data rule: **never fabricate, interpolate, or zero-fill a missing sample. A gap must stay a gap.** Record what the API said, verbatim (including suspected-bogus values — see §6.6); distinguish "load was off" from "collector was down" via `sample_count` in rollups.

## 2. Locked decisions (do not re-litigate)

1. **No monthly compaction.** Daily compaction only. ~365 small files/year is fine for DuckDB and Athena.
2. **Bryant scope:** (a) new 30s-cadence *status* poller (temps/setpoints/stage/mode — net-new GraphQL query, see §7.3), (b) daily energy collection migrated into this pipeline landing in its own `energy/daily` dataset, (c) backfill of historical DynamoDB + legacy JSON into `energy/daily`. DynamoDB is dropped from the new stack (read-only access for backfill only; the old collector keeps running independently for a while as a safety net — don't touch it).
3. **Leviton spurious zeros (fw v2):** record verbatim into raw. No hold-last-value, no filtering. May be processed out downstream later.
4. **`ts_local`** is a timezone-naive wall-clock timestamp (America/Kentucky/Louisville), for human/LLM readability. Ambiguous during DST fall-back by design. `ts_utc` is canonical; **all bucketing, sorting, and dedupe keys use `ts_utc`.**
5. **kWh math:** `kwh = mean_watts × (sample_count × poll_interval_s) / 3.6e6` — energy over *observed* time only, never extrapolated across gaps.
6. **Confirmed values:** DynamoDB table `bryant-energy-data`; blackstart inventory `~/code/blackstart/data/montfort.json`; Bryant `device_id` = system serial (single system).
7. **LG&E Green Button** meter import is designed-for now (§13), built after everything else lands.
8. Leviton ingestion: **REST polling at 30s + bandwidth keepalive**, not WebSocket (§6.4). Depend on **`aioleviton`** (PyPI, `>=0.3.3`) rather than hand-writing the client; wrap it behind a thin adapter so it can be vendored/replaced if it goes stale.

## 3. Canonical schema — long format

One row per observation. All datasets share these columns:

| column | Arrow/Parquet type | notes |
|---|---|---|
| `ts_utc` | timestamp[us, tz=UTC] | canonical instant |
| `ts_local` | timestamp[us] (naive) | wall clock in America/Kentucky/Louisville |
| `source` | string | `leviton` \| `bryant` \| `lge` (future) |
| `device_id` | string | Leviton hub id (panel serial), Bryant system serial, LG&E meter id |
| `channel_id` | string | see per-source mapping (§6.5, §7.4) |
| `metric` | string | `watts`, `amps`, `volts`, `hz`, `indoor_temp_f`, `outdoor_temp_f`, `setpoint_cool_f`, `setpoint_heat_f`, `stage`, `mode`, `fan`, `humidity_pct`, `kwh_day`, `cost_day_usd`, … |
| `value` | double | numeric value; enums encoded per §7.4 |
| `unit` | string | `W`, `A`, `V`, `Hz`, `degF`, `kWh`, `USD`, `pct`, `enum` |

Enum-ish metrics (mode, stage, fan): store the numeric encoding in `value` and ALSO emit a parallel string form — see §7.4. Do not stuff strings into `value`.

Rows within every Parquet file are sorted by `(ts_utc, source, device_id, channel_id)`. ZSTD compression. Dedupe identity everywhere: `(ts_utc, source, device_id, channel_id, metric)`.

**Partitioning is on LOCAL date** (derived from `ts_local`), because every real query is a local-time question and DST must not misalign them.

## 4. S3 layout & datasets

```
s3://${S3_BUCKET}/energy/raw_30s/year=YYYY/month=MM/day=DD/     # 30s Leviton + Bryant-status observations
s3://${S3_BUCKET}/energy/daily/year=YYYY/                        # Bryant daily energy (day-grain; ts = local midnight)
s3://${S3_BUCKET}/energy/hourly/year=YYYY/month=MM/              # derived rollup — fully disposable/regenerable
s3://${S3_BUCKET}/energy/dim_channel/                            # semantic layer, single file, overwritten atomically
s3://${S3_BUCKET}/energy/meter/year=YYYY/                        # future: LG&E Green Button intervals (§13)
```

File naming (deterministic ⇒ idempotent re-runs overwrite instead of duplicating):

- raw_30s hourly parts: `part-{YYYYMMDD}T{HH}.parquet` (one per local hour, written by uploader)
- raw_30s compacted: `day-{YYYYMMDD}.parquet` (written by daily compactor)
- hourly rollup: `rollup-{YYYYMMDD}.parquet` (one file per local day, in its year/month partition)
- daily: `bryant-{YYYYMM}.parquet` (regenerated per month touched)
- dim_channel: `dim_channel.parquet`

Day-grain rows in `energy/daily` must NEVER be written into `raw_30s` (they would poison hourly rollups).

## 5. Architecture & runtime

One Docker container (compose, `restart: unless-stopped`), one long-running process (`energycap run`) hosting:

- an asyncio 30s poll loop (Leviton + Bryant status), writing to a SQLite spool
- a Leviton bandwidth-keepalive task (§6.4)
- an in-process scheduler firing: hourly upload (at ~HH:05 for the closed hour), daily compaction (~01:30 local for D-1), hourly rollup (~HH:20 for hour HH-1, and re-run yesterday's last hours at the 01:30 daily job to catch late uploads), daily Bryant energy fetch (~08:30 local, see §7.2), dim_channel rebuild (on demand only)
- a tiny HTTP health server (§11)

Every stage is ALSO a standalone CLI command runnable over an arbitrary date range, and all are idempotent (§10).

Mounted volume `/data` holds: `spool.db`, token caches (`/data/tokens/*.json`, mode 600), `status.json`, logs if any. Everything survives container restarts.

Python 3.12+, `pyproject.toml` (use `uv`). Suggested deps: `aioleviton`, `httpx`, `pyarrow`, `duckdb`, `boto3`, `typer`, `pydantic-settings`, `tenacity`. Tests: `pytest`. Keep the dependency list lean.

Module layout:

```
src/energy_capture/
  config.py        # pydantic-settings; every knob is an env var
  logging.py       # structured JSON logs to stdout; scrub credentials/tokens
  timeutil.py      # single home for UTC↔local conversions and local-date partition math
  model.py         # Observation dataclass; canonical Arrow schema; sort & dedupe key constants
  health.py        # status.json writer + HTTP /healthz
  sources/base.py  # Source protocol: async poll() -> list[Observation]
  sources/leviton.py
  sources/bryant.py
  spool/sqlite.py
  stages/poller.py
  stages/uploader.py
  stages/compactor.py
  stages/rollup.py
  stages/backfill.py
  stages/dim.py
  aws/s3io.py      # atomic write (temp key then copy/rename), list, parquet-metadata row-count verify
  aws/glue.py
  cli.py
config/channel_map.json
tests/
```

## 6. Source: Leviton LWHEM-2 (`my.leviton.com`)

Two 200A load centers → two `IotWhem` hubs under one residence. CT pairs on a subpanel feeder → `IotCt` objects. Smart breakers being added over time → `ResidentialBreaker` objects (currently few/none; discovery must handle empty lists).

### 6.1 Auth

- `POST https://my.leviton.com/api/Person/login?include=user`, JSON `{"email", "password"}` (+ `"code"` if 2FA; HTTP 406 means 2FA required, 408 invalid code — Leviton abuses status codes).
- Response `id` **is** the token; also capture `userId`, `ttl` (~60 days), `created`.
- Every request: bare header `authorization: <token>` (no `Bearer`), plus a browser-like `user-agent` and `Origin: https://myapp.leviton.com` (Leviton appears to fingerprint; aioleviton handles this).
- **No refresh endpoint.** Cache the full login response at `/data/tokens/leviton.json`. On startup, validate the cached token with `GET /Person/{userId}/residentialPermissions`; on 401, re-login. **Never log in more than once per 10 seconds** (Leviton punishes rapid logins) and never per-poll.

### 6.2 Object hierarchy / discovery

person → `GET /Person/{userId}/residentialPermissions` → residentialAccount → `GET /ResidentialAccounts/{id}/residences` → `GET /Residences/{id}/iotWhems` → per hub: `GET /IotWhems/{id}/residentialBreakers` and `GET /IotWhems/{id}/iotCts`. LoopBack `filter` goes in a literal `filter` HTTP header (usually `{}`). Run discovery at startup and every `LEVITON_DISCOVERY_INTERVAL_S` (default 3600) so newly added smart breakers appear without a restart.

### 6.3 Metric fields

- Breaker (`ResidentialBreaker`): `power`/`power2` (W, per pole), `rmsCurrent`/`rmsCurrent2` (A), `rmsVoltage`/`rmsVoltage2` (V), `position` (slot), `poles`, `currentState`, `branchType`, `serialNumber`, `connected`, `model` (`NONE`/`NONE-1`/`NONE-2` = dumb breaker placeholder — skip). The `2`-suffix is the **second pole**, not the second panel leg.
- CT (`IotCt`): `activePower`/`activePower2` (W), `rmsCurrent`/`rmsCurrent2` (A), `channel`, `usageType` (`NOT_USED` = skip). One `IotCt` object = one clamp **pair** (leg A / leg B). A null second-leg value means single-leg CT — emit nothing for that leg (gap, not zero).
- Hub (`IotWhem`): `rmsVoltageA`/`rmsVoltageB` (V), `frequencyA`/`frequencyB` (Hz), `connected`, `version` (firmware), `rssi`. No panel-level power field exists; do NOT synthesize a panel total in raw (that's a query-time concern).
- **Do not collect `energyConsumption`/`energyImport`**: firmware v2 turned them into period counters that reset on bandwidth toggles; both reference integrations abandoned them. We derive kWh in the rollup.

### 6.4 The bandwidth keepalive (critical)

The cloud serves stale cached readings unless the hub is in high-bandwidth mode. `PUT /IotWhems/{hub_id}` body `{"bandwidth": 1}` — which auto-decays back to 2 within seconds. The official app re-PUTs every 50s. Firmware 2.1.0: `bandwidth: 0` disconnects the hub for 10–20s → **never send 0** in normal operation.

⇒ Dedicated keepalive task: every 50s, `PUT {"bandwidth": 1}` to each connected hub. Skip hubs reporting `connected: false`. If keepalive PUTs fail repeatedly, back off exponentially (don't hammer a down API) and record the condition in `status.json`.

WebSocket (`wss://socket.cloud.leviton.com/`) exists but is NOT used: server hard-kills connections at exactly 60 min, fw≥2.0 needs per-breaker subscriptions, updates are partial deltas requiring state merging, and the bandwidth keepalive is needed anyway. REST at 30s is simpler and sufficient for an archival pipeline.

### 6.5 Row mapping

- `device_id` = hub id (the panel serial string).
- `channel_id`: breakers → `breaker_p{position}` (e.g. `breaker_p11`; 2-pole breaker = ONE channel; slot list lives in dim_channel). CTs → `ct_{channel}_a` / `ct_{channel}_b` (per leg). Hub-level → `panel_leg_a` / `panel_leg_b`.
- Breaker rows per poll: `watts` = `power + power2` (2-pole; single-pole: `power`), `amps` = per-pole mean for 2-pole (`(rmsCurrent + rmsCurrent2)/2`) or `rmsCurrent`, `volts` = leg sum for 2-pole (`rmsVoltage + rmsVoltage2`) or `rmsVoltage`. If a needed field is null → emit no row for that metric (gap).
- CT rows: per leg, `watts` = `activePower`(/2), `amps` = `rmsCurrent`(/2).
- Hub rows: `volts` for each leg, `hz` for each leg.
- Firmware ≥2.2.0 appends the panel serial to breaker ids (`4C45565275C6` → `4C45565275C6_A65E`). Never use raw breaker `id` in `channel_id`; `position` is the stable identity (and matches blackstart slots).
- `ts_utc` = one timestamp per source per poll cycle, taken when the response set is complete (µs precision). All rows from one Leviton poll share it.

### 6.6 Error handling & quirks

- Transient 502/504 from Leviton's gateway are NORMAL. Retry within the cycle (tenacity, e.g. 2 retries, 2s/5s); if the cycle still fails, log once at WARN, emit no rows, move on. Consecutive-failure count in `status.json`.
- fw v2 sends spurious zero power/current readings. **Record verbatim** (decision §2.3).
- Auth failure (401 after token refresh attempt) → re-login (respecting the 10s login floor); if login fails, back off 60s and keep trying; health status reflects it. Never crash the loop.
- Poll floor: never faster than 30s (hard-code a floor even if env var says lower).

## 7. Source: Bryant Evolution / Carrier Infinity cloud

### 7.1 Auth (shared by both Bryant paths)

OAuth2 **password grant** against Carrier's Okta: `POST https://sso.carrier.com/oauth2/default/v1/token`, form-encoded, `client_id=0oa1ce7hwjuZbfOMB4x7` (public SPA client, hardcoded is fine), `scope=openid offline_access`. Returns `access_token` + `refresh_token`.

The old collector re-authenticated with the password on every (daily) run and never used refresh tokens. **At 30s cadence that is unacceptable.** Requirements:

- Cache tokens at `/data/tokens/carrier.json` (mode 600). Track expiry from `expires_in` (fall back to decoding the JWT `exp` if absent).
- Use `grant_type=refresh_token` to renew; only fall back to password grant when refresh fails.
- GraphQL calls: `POST https://dataservice.infinity.iot.carrier.com/graphql` with `Authorization: Bearer <access_token>` and spoofed SPA headers `Origin: https://my.carrier.com`, `Referer: https://my.carrier.com/` (the old repo's CLAUDE.md says these matter — keep them).

Reusable code: `~/code/bryantDataCollector/carrier_auth.py` (auth) and the `graphql_query()` helper in `carrier_energy.py`. Port/adapt, don't import from the old repo.

### 7.2 Daily energy (migrated behavior) → `energy/daily`

Query `getInfinityEnergy($serial)` → `infinityEnergy.energyPeriods[]`, 8 components × {Kwh, Dollars}: `eHeat`, `cooling`, `fan`, `fanGas`, `hPHeat`, `loopPump`, `gas`, `reheat` (camelCase in period fields, lowercase in `energyConfig` — mind the casing). Once daily at ~08:30 **local**, fetch and land `day1` (yesterday) AND `day2` (day-before-yesterday, as a revision/catch-up — dedupe handles overlap).

Row mapping: `source=bryant`, `device_id=<serial>`, `channel_id` = component in lowercase (`eheat`, `cooling`, `fan`, `fangas`, `hpheat`, `looppump`, `gas`, `reheat`), metrics `kwh_day` (unit `kWh`) and `cost_day_usd` (unit `USD`), `ts_utc` = **local midnight of the measured day converted to UTC**, `ts_local` = local midnight. Skip components whose `energyConfig.<name>.enabled` is false (they're structurally absent, not zero) — fetch `energyConfig` in the same query.

`gasKwh` unit caveat: the field name says kWh but gas is probably not kWh. This system is a heat pump + electric strips, so gas should be zero/disabled; if `energyConfig.gas.enabled` is false it drops out naturally. If it's ever enabled and nonzero, keep `metric=kwh_day` but log a WARN — flagged for human review, don't guess a conversion.

### 7.3 Status poller (NET-NEW — the one research task left open)

Goal: at the same 30s cadence as Leviton (configurable `BRYANT_POLL_INTERVAL_S`, default 30, floor 30), capture what the unit is doing so energy can be correlated with state.

The old repo contains NO status query. The GraphQL schema at `dataservice.infinity.iot.carrier.com` exposes system status; the best open-source references are **`dahlb/carrier-api`** (PyPI: `carrier-api`) and its HA integration **`dahlb/ha_carrier`** — they use this same GraphQL endpoint + Okta auth. Read `carrier-api`'s source for the exact status query/fields before writing this module; verify against a live call. Consider depending on `carrier-api` directly if its surface fits — same adapter-wrapping rule as aioleviton.

Metrics to capture (map from whatever the status query actually provides; expected fields in parentheses):

| metric | expected source field | unit | channel_id |
|---|---|---|---|
| `indoor_temp_f` | zone `rt` | degF | `zone_{n}` |
| `humidity_pct` | zone `rh` | pct | `zone_{n}` |
| `setpoint_heat_f` / `setpoint_cool_f` | zone `htsp`/`clsp` | degF | `zone_{n}` |
| `outdoor_temp_f` | system `oat` | degF | `system` |
| `mode` | system `mode` (heat/cool/auto/off/…) | enum | `system` |
| `stage` | odu/idu operating stage | enum | `system` |
| `fan` | zone/idu fan state | enum | `zone_{n}` or `system` |
| `blower_rpm`, `cfm`, etc. | if available | native | `system` |

Enum encoding: `value` = a small stable integer from an explicit mapping table in `sources/bryant.py` (e.g. mode: off=0, heat=1, cool=2, auto=3, fanonly=4), `unit="enum"`, AND emit a companion metric `<name>_label`?  **No** — long schema has no string value column. Instead: the integer mapping table is duplicated into `dim_channel`-style docs? Also no. Resolution: put the enum decode into the **Glue column comment for `value`** and in the README, AND keep the mapping table as a constant that the rollup/README tests assert never changes meaning (append-only). Unknown enum string from the API → log WARN, emit no row (gap), never invent a number on the fly and never renumber.

If the API rate-limits or throttles at 30s (unknown), honor `Retry-After`/back off and record the effective cadence in `status.json`; make the interval configurable rather than fighting it.

### 7.4 device/channel ids

`device_id` = system serial (env `CARRIER_SERIAL`; old default `4022W200213`). Zones: the system reportedly has zoning support; enumerate zones from the status response, `channel_id=zone_1..n`.

## 8. Backfill (`energycap backfill`)

Source A — DynamoDB `bryant-energy-data` (us-east-1): one item per date, partition key `date` (`YYYY-MM-DD`), attributes `serial_number`, `period_type` (`day1`/`day2`), `collected_at`, and 16 `Decimal` metric attrs (`eHeatKwh`…`reheatDollars`). Scan (table is tiny), map exactly like §7.2 (ts = local midnight of `date`). Skip metrics that are `0` **only if** the component was structurally disabled — we can't know historically, so DO NOT skip; write zeros as recorded (they were recorded as zeros, that's what the API said). `Decimal("0")` coercion of nulls is a known source-side lossage; accept it.

Source B — `~/code/bryantDataCollector/energy_data/energy_2026_01.json`: object keyed `YYYY-MM-DD` → `{period_type, collected_at, data:{…16 camelCase fields…}}`, **no serial** (use `CARRIER_SERIAL`).

Rules: A and B overlap → dedupe on the standard key; where both exist for a date+metric prefer DynamoDB (has provenance). Backfill is idempotent over a date range and regenerates the affected `energy/daily/` monthly files completely. Read-only against DynamoDB; requires only `dynamodb:Scan` on that one table.

## 9. Semantic layer: `channel_map.json` + `dim_channel`

Nothing in Leviton's cloud ids can auto-join to the blackstart inventory (verified: no serials/channel ids exist in `montfort.json`; breaker `position` ↔ blackstart slot is the only linkage). So:

**`config/channel_map.json`** (committed, hand-maintained) — entries keyed `(source, device_id, channel_id)` →

```json
{
  "mappings": [
    {"source": "leviton", "device_id": "<hub-serial>", "channel_id": "breaker_p11",
     "blackstart_device_id": "A-11"},
    {"source": "leviton", "device_id": "<hub-serial-B>", "channel_id": "ct_1_a",
     "label": "HVAC subpanel feeder (leg A)", "panel": "B", "category": "hvac",
     "blackstart_device_id": "B-6-8"},
    {"source": "bryant", "device_id": "4022W200213", "channel_id": "hpheat",
     "label": "Heat pump — heating", "category": "hvac"}
  ]
}
```

Rules: if `blackstart_device_id` is set, label/panel/slots/category/priority/estimated_watts are pulled from `montfort.json` at build time (blackstart stays the source of truth for labels); explicit fields override; entries with neither are a build error. Path to montfort.json via env `BLACKSTART_INVENTORY_PATH` (the build runs on the Mac, not necessarily in the container).

**`energycap discover`** — enumerates the live Leviton hierarchy (hubs, breakers with position/name/branchType/model, CTs with channel/usageType) and Bryant zones, prints a table PLUS ready-to-paste JSON skeleton entries for anything not yet in `channel_map.json`. This is how new smart breakers get mapped in five minutes.

**`energycap build-dim`** — joins map + inventory → `dim_channel.parquet`, columns: `source`, `device_id`, `channel_id`, `label`, `short_label`, `panel`, `slots` (string, e.g. `"1,3"`), `category` (from blackstart `circuitType`/role, normalized), `room`, `priority`, `estimated_watts`, `blackstart_device_id`, `updated_at`. Unmapped live channels appear in `discover` output and as a WARN in `build-dim` — never silently absent. Written atomically (temp key → copy → delete old).

This layer is what makes LLM answers useful — every Glue/README example query joins through it.

## 10. Stage contracts (idempotency & safety)

Common rules: every stage takes `--start/--end` local dates (default: its scheduled window); deterministic output names (§4) so re-runs overwrite; dedupe on the standard key with **latest-write wins** at the file level (within a file, first occurrence after sorting — they're identical anyway); all S3 writes go to a temp key then copy+delete (no partial files at final keys); row counts logged per stage run and reflected in `status.json`.

- **Poller**: appends `Observation` rows to SQLite spool (`WAL` mode; table `observations` with the 8 schema columns + `uploaded_at NULL`). One transaction per poll cycle. Spool rows are deleted only after their hour is verified uploaded, plus a 7-day retention floor (`SPOOL_RETENTION_DAYS=7`) as a second safety net.
- **Uploader** (hourly, for each closed local hour with un-uploaded rows): read spool rows for that local hour, dedupe, sort, write `part-{YYYYMMDD}T{HH}.parquet` to the local-date partition, **verify** (read back S3 parquet metadata row count == written count), then mark spool rows uploaded. Handles multi-hour catch-up after downtime in one invocation.
- **Daily compactor** (for local day D, runs D+1): read all `part-*.parquet` for D **and** any existing `day-{D}.parquet`, dedupe, sort, write `day-{D}.parquet`, verify count == deduped count, and only then… keep the parts. Parts for day D are deleted by a later compactor run once D is ≥7 days old AND `day-{D}.parquet` exists and passes the count check again. Readers must therefore dedupe at query time OR the Glue table must point only at… **Resolution:** parts and day-file coexisting would double-count. Avoid by layout: compactor writes `day-{D}.parquet` and immediately moves the parts to a sibling **non-tabled** prefix `s3://…/energy/raw_30s_parts_archive/year=/month=/day=/` (copy+delete), where they live out their 7-day safety window before deletion. The `raw_30s` prefix therefore always contains exactly one authoritative set per day (parts for recent days, day-file for compacted days), and no query-time dedupe is needed.
- **Rollup** (hour N runs once N+1 is ≥20 min underway; the daily 01:30 job re-runs all of D-1): regenerate the **entire local day's** `rollup-{YYYYMMDD}.parquet` each time (cheap, and avoids intra-day merge logic). Buckets on local hour. Group `(local_hour_start_ts, source, device_id, channel_id, metric)` → `mean`, `min`, `max`, `p95` (DuckDB `quantile_cont(value, 0.95)`), `sample_count`, `first_ts_utc`, `last_ts_utc`, and `kwh` (only where `metric='watts'`, per §2.5, using `POLL_INTERVAL_S`). Day-grain metrics (`kwh_day` etc.) are excluded from rollup input. Implemented as DuckDB SQL over the S3 (or local) raw files — the SQL is the documentation of the math; keep it in one readable `.sql` file or module constant.
- **Backfill**: §8.

Late-data rule: raw uploads landing after a rollup ran are healed by the next rollup covering that day; the daily full-day re-run covers the common case. `energycap rollup --start … --end …` heals anything else (document in README: "if you fix a collector bug, re-run rollup over the range").

## 11. Health & logging

- Structured JSON logs to stdout (one object/line: `ts`, `level`, `stage`, `event`, counts, durations). A scrubbing filter guarantees passwords/tokens never appear (test this).
- `/data/status.json` rewritten atomically after every stage action:

```json
{
  "leviton": {"last_success_utc": "...", "consecutive_failures": 0, "channels_seen": 14},
  "bryant_status": {"last_success_utc": "...", "consecutive_failures": 0},
  "bryant_daily": {"last_success_utc": "..."},
  "uploader": {"last_success_utc": "...", "last_hour_uploaded": "2026-08-16T14", "rows": 4212},
  "compactor": {"last_day_compacted": "2026-08-15", "rows": 98304},
  "rollup": {"last_day_rolled": "2026-08-16", "rows": 1152},
  "spool": {"pending_rows": 1240, "oldest_pending_utc": "..."}
}
```

- `GET :${HEALTH_PORT}/healthz` serves it; non-200 if the poller's last success is older than 3× its interval.

## 12. Glue tables & query surfaces

Tables `energy_raw_30s`, `energy_hourly`, `energy_daily`, `dim_channel` in database `energy` (env `GLUE_DATABASE`, default `energy`). Created/updated by `energycap create-glue-tables` (boto3, idempotent create-or-update; no crawler, no CloudFormation needed).

- **Partition projection** on all partitioned tables: `projection.enabled=true`; `year` type `integer` range `2024,2035`; `month`/`day` type `integer` digits 2 range `1,12`/`1,31`; `storage.location.template` per layout. (`energy_daily`: year only.)
- **Comments are a first-class deliverable** — they're what an LLM reads to orient itself. Table comments must state: grain, partition-on-LOCAL-date semantics, the dedupe key, that gaps mean collector downtime, and for `energy_hourly` a blunt warning: *"sample_count < ~118 (watts@30s) means the hour has gaps; an absent row means the collector was down — do NOT read absence or low kwh as the load being off."* Column comments: `ts_local` naive-wall-clock semantics; `value` enum decodes for `mode`/`stage`/`fan` (§7.3); `kwh` observed-time-only formula; `channel_id` conventions. Write the actual comment strings during implementation with this care, not placeholders.
- README gets 4–6 real DuckDB `httpfs` examples, at minimum: (1) yesterday afternoon's heat pump draw joining `dim_channel` + `raw_30s`, (2) hourly kwh by label for a week from `energy_hourly` **including `sample_count`**, (3) gap-finding query (hours where sample_count < expected), (4) Bryant state vs Leviton watts correlation (join on time bucket), (5) daily Bryant history. Same queries expressed once for Athena where syntax differs.

## 13. Future source: LG&E Green Button (design now, build later)

LG&E (Louisville Gas & Electric / PPL) offers Green Button data export. Two possible paths, in preference order: **Green Button Connect My Data** (OAuth'd ESPI API, automated) if LG&E actually offers it, else **Download My Data** (manual XML/CSV export, imported via CLI). Assume manual import first; verify Connect availability when building.

Design accommodations already made: `source='lge'` in the schema enum; `energy/meter/year=YYYY/` dataset. Meter data is **interval** data (electric likely 15/30/60-min kWh; gas likely daily therms/CCF), not instantaneous — so the `meter` dataset adds ONE extra column to the canonical schema: `interval_s` (int32, duration the value covers; `ts_utc` = interval START). ESPI XML maps: `UsagePoint`→`device_id` (meter id), `MeterReading/ReadingType`→`metric`+`unit` (`kwh_interval`/`kWh`, `ccf_interval`/`CCF`…), `IntervalBlock/IntervalReading`→rows (`start`,`duration`,`value` with ESPI `powerOfTenMultiplier` applied). CLI: `energycap import-greenbutton <file.xml> [--source lge]`, idempotent (standard dedupe key). `channel_id`: `electric_main` / `gas_main`. Glue table `energy_meter` with the same projection treatment. **Do not build any of this yet** — just don't paint it into a corner (concretely: the Arrow schema module must make the extra-column dataset variant non-hacky, and `dim_channel` should happily hold lge channels).

## 14. Config (env vars, `.env` gitignored, `.env.example` committed)

`S3_BUCKET`, `AWS_REGION`, `AWS_PROFILE` (optional; container may use keys/instance creds instead), `GLUE_DATABASE=energy`, `LEVITON_USERNAME`, `LEVITON_PASSWORD`, `CARRIER_USERNAME`, `CARRIER_PASSWORD`, `CARRIER_SERIAL`, `DYNAMODB_TABLE=bryant-energy-data` (backfill only), `TZ_LOCAL=America/Kentucky/Louisville`, `POLL_INTERVAL_S=30`, `BRYANT_POLL_INTERVAL_S=30`, `LEVITON_DISCOVERY_INTERVAL_S=3600`, `SPOOL_DIR=/data`, `SPOOL_RETENTION_DAYS=7`, `HEALTH_PORT=8080`, `BLACKSTART_INVENTORY_PATH`, `LOG_LEVEL=INFO`.

No secrets in the repo. No credentials or tokens in logs (tested). Token caches only on the mounted volume.

## 15. Tests (pytest; all pure-logic tests run without network/AWS)

Required coverage — these encode the project's correctness contract:

1. **Rollup math**: mean/min/max/p95 against hand-computed fixtures; `kwh` = observed-time-only (a half-populated hour yields half the kwh of a full one at equal wattage); `sample_count` correct; day-grain metrics excluded.
2. **Dedupe**: overlapping parts with identical keys collapse to one row; `day2`-revision vs `day1` daily rows collapse correctly.
3. **DST boundaries** (America/Kentucky/Louisville): spring-forward day has 23 local hours and fall-back day has 25 in the rollup; partition date assignment around 02:00 transitions; local-midnight→UTC conversion for daily rows on both transition days; the fall-back ambiguous hour buckets by `ts_utc` without loss.
4. **Gap handling**: a poll cycle that fails emits zero rows; rollup over a gapped hour reports reduced `sample_count` and no interpolation; null Leviton fields produce absent rows, not zeros.
5. **Backfill**: both legacy formats parse to identical row shapes; DynamoDB-preferred-over-JSON on overlap; idempotent double-run byte-identical.
6. **Spool durability**: rows written before a simulated crash are uploaded after restart; uploaded-marking only after verify.
7. **Compactor safety**: parts never deleted/archived unless day-file verify passes; re-run after partial failure converges.
8. **Log scrubbing**: passwords/tokens injected into log records never reach output.
9. **Enum stability**: the mode/stage/fan mapping tables are append-only (test pins current values).
10. **channel_map/dim build**: montfort.json fixtures join correctly; unmapped channel → WARN; conflicting override wins.

Leviton/Carrier clients: unit-test the response→Observation mapping with recorded JSON fixtures (2-pole breaker, CT pair with null leg, spurious zero passes through verbatim, fw2.2 suffixed ids ignored in favor of position).

## 16. Suggested build order

1. Scaffold: pyproject, config, logging, timeutil, model, spool, CLI skeleton, Docker/compose. Tests for timeutil+model.
2. Leviton source + poller + keepalive (live-testable immediately with real creds in `.env`).
3. Uploader + daily compactor + verify logic.
4. Rollup (+ its tests — the heart of the correctness contract).
5. Bryant: auth hardening (refresh tokens), status poller (research `dahlb/carrier-api` first), daily energy job, backfill.
6. channel_map + discover + build-dim; Glue tables + comments; README with example queries.
7. `DEVIATIONS.md` if any; then hand back for adversarial review.

Definition of done: all §15 tests pass; `docker compose up` on the Mac Mini polls both sources and survives restart with no data loss; a full manual cycle (`poll`→`upload`→`compact-daily`→`rollup`→`build-dim`→`create-glue-tables`) succeeds against the real bucket; README queries return real data via DuckDB.
