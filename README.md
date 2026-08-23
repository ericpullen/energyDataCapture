# energyDataCapture (`energycap`)

Household energy and HVAC time series: two cloud APIs polled every 30 seconds on a Mac
Mini, spooled to SQLite, landed in S3 as partitioned Parquet, and queried with DuckDB
locally, Athena remotely, or by an LLM over MCP.

- **Sources:** two Leviton LWHEM-2 smart load centers (`my.leviton.com`) and a Bryant
  Evolution / Carrier Infinity heat pump (`dataservice.infinity.iot.carrier.com`).
- **Volume:** tens of MB per year. Nothing here is performance-sensitive; it is
  optimised for query ergonomics and for data you can trust.
- **Spec of record:** [`PLAN.md`](PLAN.md). Cardinal data rules: [`CLAUDE.md`](CLAUDE.md).
  Every place the implementation deviates from the spec, and why:
  [`DEVIATIONS.md`](DEVIATIONS.md).

> **The one rule that matters most: a gap stays a gap.** Nothing in this pipeline is
> ever interpolated, zero-filled, or held over from the last reading. A null field from
> an API emits no row; a failed poll cycle emits no rows at all. So a missing row means
> *"not observed"*, **never** *"the load was 0"*. `sample_count` in the hourly rollup is
> what lets you tell the two apart — see [Reading this data honestly](#reading-this-data-honestly).

---

## Contents

- [The data model in brief](#the-data-model-in-brief)
- [Setup](#setup)
- [How Leviton values are kept fresh](#how-leviton-values-are-kept-fresh)
- [Operating it](#operating-it)
- [Commands](#commands)
- [Querying with DuckDB](#querying-with-duckdb)
- [The same queries in Athena](#the-same-queries-in-athena)
- [Enum decodes: `mode`, `stage`, `fan`](#enum-decodes-mode-stage-fan)
- [Compressor stage: `stage` vs `stage_pct`](#compressor-stage-stage-vs-stage_pct)
- [Reading this data honestly](#reading-this-data-honestly)
- [Settled by the first live run](#settled-by-the-first-live-run-2026-08-17)
- [Known-unproven](#known-unproven)

Two other directories are worth knowing about. **`site/`** is a small public web site
(published to <https://energycap.ericpullen.com/> by `.github/workflows/pages.yml`) that exists
because LG&E's Green Button Connect registration requires public client, policy, logo, portal,
redirect and notification URLs — its redirect page is what hands an OAuth authorization code to
the collector running inside the house. **`docs/lge-greenbutton.md`** is the research behind
that registration and the application itself.

---

## The data model in brief

### Long format: one row per observation

Every dataset shares the same eight columns. A new sensor adds **rows, never columns**.

| column | type | meaning |
|---|---|---|
| `ts_utc` | `timestamp[us, tz=UTC]` | **The canonical instant.** All sorting, hourly bucketing and dedupe use this. |
| `ts_local` | `timestamp[us]`, naive | Wall clock in `America/Kentucky/Louisville`, for humans and LLMs. No offset attached; deliberately ambiguous for one hour each November. |
| `source` | string | `leviton`, `bryant`, or `lge` (designed for, not yet collected). |
| `device_id` | string | Leviton hub id (= panel serial), Bryant system serial, LG&E meter id. |
| `channel_id` | string | Leviton: `breaker_p{position}` (a 2-pole breaker is **one** channel), `ct_{channel}_{a,b}`, `panel_leg_{a,b}`. Bryant: `system`, `zone_{n}`, and the lowercase energy components (`cooling`, `hpheat`, `eheat`, `fan`, `fangas`, `looppump`, `gas`, `reheat`). |
| `metric` | string | `watts`, `amps`, `volts`, `hz`, `indoor_temp_f`, `outdoor_temp_f`, `setpoint_heat_f`, `setpoint_cool_f`, `humidity_pct`, `mode`, `stage`, `stage_pct`, `fan`, `blower_rpm`, `cfm`, `compressor_rpm`, `outdoor_coil_temp_f`, `static_pressure`, `idu_cfm`, `idu_iducfm`, `odu_iducfm`, `op_status`, `odu_mode`, `idu_status`, `kwh_day`, `cost_day_usd`, `kwh_interval`, `ccf_interval`. The Glue metric column comment no longer lists all of them (28 names overflow the 255-character limit); SELECT DISTINCT on this column is the authoritative enumeration. The last two live only in the meter table, and LG&E Connect has only ever served electric — so the gas one has a table and a unit but no rows yet. |
| `value` | double | The number. Enum metrics store a small integer code — see [the decodes](#enum-decodes-mode-stage-fan). |
| `unit` | string | `W`, `A`, `V`, `Hz`, `degF`, `rpm`, `CFM`, `inwc`, `pct`, `enum`, `kWh`, `USD`, `CCF`. Constant per metric. |

Those two lists are the **complete** vocabularies, not a sample: they are pinned
integer-for-integer to `UNIT_FOR_METRIC` in
[`src/energy_capture/model.py`](src/energy_capture/model.py) by a test, so filtering on
them cannot silently drop a real row. `compressor_rpm` is the compressor's own
speed — continuous where `stage_pct` is quantised, so it is the better variable
to correlate against a metered watts channel, and watts rising against a flat
rpm is how a failing compressor shows up. `outdoor_coil_temp_f` with
`outdoor_temp_f` gives the condenser approach temperature. `static_pressure` is
filter loading (unit `inwc`, a reference client's claim rather than a documented
one). **Three airflow numbers exist and disagree** — `idu_cfm` (500),
`idu_iducfm` (513) and `odu_iducfm` (1166) in one observed cycle — so each is
recorded under the field it came from; `cfm` is an older blended pick, kept for
archive continuity. `blower_rpm` / `cfm` are Bryant air-handler
telemetry on `channel_id = 'system'`; `kwh_interval` / `ccf_interval` and `CCF` belong to
the LG&E `energy/meter` dataset, built 2026-08-23 as the `energy_meter` table.
**Never sum `energy_meter.value` without pinning `interval_s`** — every meter
publishes the same energy as both a 900s and a 3600s series.

**`stage` and `stage_pct` are two mutually exclusive renderings of one field** — the
outdoor unit's `odu.opstat` — and which one a system emits is fixed by its hardware.
**This house emits `stage_pct` and will never emit a single `stage` row**, so a query
that filters on `stage` here returns nothing at all. That is the "absence is not zero"
trap in its sharpest form; read
[Compressor stage: `stage` vs `stage_pct`](#compressor-stage-stage-vs-stage_pct) before
writing either into a `WHERE`.

**Dedupe identity everywhere:** `(ts_utc, source, device_id, channel_id, metric)`. Every
writer dedupes on exactly that tuple, so a query over a settled partition never needs
`DISTINCT`.
**Sort order inside every file:** `(ts_utc, source, device_id, channel_id)`. ZSTD compressed.

### The four datasets

| S3 prefix | Glue table | grain | partitioned by | file name |
|---|---|---|---|---|
| `energy/raw_30s/` | `energy_raw_30s` | one 30s observation | local `year`/`month`/`day` | `part-{YYYYMMDD}T{HH}.parquet`, compacted to `day-{YYYYMMDD}.parquet` |
| `energy/hourly/` | `energy_hourly` | one hour × channel × metric (derived, disposable) | local `year`/`month` | `rollup-{YYYYMMDD}.parquet` |
| `energy/daily/` | `energy_daily` | one local day × HVAC component × metric | local `year` | `bryant-{YYYYMM}.parquet` |
| `energy/dim_channel/` | `dim_channel` | one channel — the semantic layer | not partitioned | `dim_channel.parquet` |

`energy/raw_30s_parts_archive/` also exists and is **deliberately not a table**: once a
day file is written and verified, the compactor moves that day's hourly parts there. So
`energy/raw_30s/` normally holds one authoritative copy of each day — parts for recent
days, a day file for compacted ones — and nothing is double counted.

**The one exception**, stated identically in the `energy_raw_30s` table comment: both are
present while a compaction is in flight, and stay so if the compactor died between writing
`day-{D}.parquet` and archiving the parts. Rows they share then count twice. Spot it as
`part-*.parquet` beside `day-*.parquet` in that day's prefix, or as totals ~2× the
neighbouring days'. Re-run `energycap compact-daily --start D --end D` to finish the
archive and resolve it; until then, dedupe that day on `(ts_utc, source, device_id,
channel_id, metric)` — which is exactly what the rollup does, unconditionally, for this
reason.

`energy/meter/` (`energy_meter`) is designed but not built: LG&E Green Button interval
data, adding an `interval_s` column where `ts_utc` is the interval *start* (PLAN.md §13).

### Partitioning is on the LOCAL date

`year=`/`month=`/`day=` come from `ts_local`, not from UTC, because every real question
here is a local-time question ("what did it draw yesterday afternoon?"). A UTC partition
would cut the local day at 19:00 or 20:00 depending on DST.

Consequences worth knowing: a local day is 23 or 25 hours long on DST transition days,
and the hourly rollup keys its buckets on `hour_start_utc` (with `local_hour_start` as
the readable label) precisely so the fall-back day keeps **25 distinct buckets** instead
of merging the two 01:00 hours.

### The hourly rollup

`energy_hourly` groups raw by `(hour_start_utc, source, device_id, channel_id, metric)`
and carries `mean`, `min`, `max`, `p95`, `sample_count`, `first_ts_utc`, `last_ts_utc`,
and `kwh`. The kWh math is **observed-time only**:

```
kwh = mean_watts * (sample_count * POLL_INTERVAL_S) / 3.6e6
```

A half-observed hour yields exactly half the kWh at the same wattage — energy is never
extrapolated across a gap. `kwh` is `NULL` (never `0`) for every metric except `watts`.
The SQL that does this is a single readable file: [`src/energy_capture/stages/rollup.sql`](src/energy_capture/stages/rollup.sql).

**Enum rows are rolled up here too.** The rollup excludes only the day-grain metrics, so
`mode`, `stage` and `fan` (`unit = 'enum'`) get a row per hour like everything else — and
`mean` and `p95` over them are **meaningless**, because they are arithmetic on integer
codes and there is no midpoint between `cool` and `auto`. For those rows only `min`, `max`
and `sample_count` carry meaning (`max` is genuinely useful *on a staged unit*: the top
stage the outdoor unit reached in the hour). The same warning and the same decode table
are carried in the `energy_hourly` **table** comment in Glue, because that table has no
`value` column to hang them on.

**`stage_pct` is not one of them.** It carries `unit = 'pct'`, not `'enum'` — it is a real
0–100 compressor capacity, so its hourly `mean` is a genuine *mean capacity for the hour*
and is the natural join partner for mean `watts`. The enum warning above applies to
`mode`, `stage` and `fan` only, and both documents say so in the same words.

### The semantic layer

`dim_channel` is what makes an answer useful: it turns `breaker_p10` on some hub serial
into "Heat pump — outdoor unit", panel B, slots `10,12`, category `hvac`, priority
`critical`. It is built by `energycap build-dim` from the hand-maintained
[`config/channel_map.json`](config/channel_map.json) joined to the blackstart panel
inventory (`BLACKSTART_INVENTORY_PATH`), and **every example query below joins through
it**. Coverage is not guaranteed — an unmapped live channel is simply absent from
`dim_channel` (`build-dim` WARNs about it), so use `LEFT JOIN` whenever dropping a real
measurement would be worse than seeing a null label.

---

## Setup

Prerequisites: [`uv`](https://astral.sh/uv), a container runtime, and an S3 bucket. Python
3.12+. Two runtimes are supported and the choice is made for you by the hardware — see
[Running it](#running-it-apple-container-or-docker) below.

```bash
git clone <this repo> && cd energyDataCapture
cp .env.example .env && $EDITOR .env     # credentials live here; .env is gitignored
uv sync
uv run pytest                            # the whole suite is offline: no network, no AWS
```

`.env` is the complete configuration surface — every knob is an environment variable
(PLAN.md §14). The ones you must set: `S3_BUCKET`, `AWS_REGION`, `LEVITON_USERNAME` /
`LEVITON_PASSWORD`, `CARRIER_USERNAME` / `CARRIER_PASSWORD` / `CARRIER_SERIAL`. See
[`.env.example`](.env.example) for the rest (poll intervals, `TZ_LOCAL`, `SPOOL_DIR`,
`HEALTH_PORT`, `GLUE_DATABASE`, `BLACKSTART_INVENTORY_PATH`). One knob is worth reading
about before you change it: `LEVITON_INGEST`, in
[How Leviton values are kept fresh](#how-leviton-values-are-kept-fresh).

No secrets ever go in the repo, in a log line, or in `status.json`; token caches live
only on the mounted `/data` volume, mode 600.

### First run, in order

```bash
uv run energycap discover                       # enumerate live channels, print map stubs
$EDITOR config/channel_map.json                 # paste the hub ids over the PLACEHOLDERs
uv run energycap build-dim --dry-run            # check the join before writing anything
uv run energycap build-dim                      # -> energy/dim_channel/dim_channel.parquet
uv run energycap create-glue-tables             # Athena tables + their comments
./scripts/energycap-container.sh build          # the image (Apple container; see below)
./scripts/energycap-container.sh run            # the collector itself, in the foreground
curl localhost:8080/healthz
```

`discover` is the five-minute path from a newly installed smart breaker to a labelled
row: it prints the live hierarchy plus ready-to-paste `channel_map.json` entries for
anything not yet mapped, and writes `config/live_channels.json` beside the map so
`build-dim` can warn about unmapped channels without a second live call.

> **Unproven, honestly:** nothing here has ever written to AWS. The collector itself has
> run against the live Leviton and Carrier clouds, both natively and **inside the
> container** (2026-08-17 — see
> [Settled by the first live run](#settled-by-the-first-live-run-2026-08-17)); every stage
> after the spool has not. `docker build` has still never run, and the LaunchAgent has
> never been loaded. The test suite itself stays fixture-driven, with an autouse guard
> that refuses any non-loopback socket. See [Known-unproven](#known-unproven).

### Running it: Apple `container` or Docker

One `Dockerfile`, shared. Two ways to run what it builds:

| | Apple [`container`](https://github.com/apple/container) + launchd | Docker + compose |
|---|---|---|
| when | **Preferred on this Apple-silicon Mac Mini** | Fallback here, and **required** on Intel Macs and Linux |
| build | `./scripts/energycap-container.sh build` | `docker compose build` |
| run | `./scripts/energycap-container.sh run` | `docker compose up -d` |
| supervision | launchd `KeepAlive` ([`deploy/com.duckbillhq.energycap.plist`](deploy/com.duckbillhq.energycap.plist)) | `restart: unless-stopped` |
| `/data` | host bind mount (`./data`) — spool readable from the Mac | named volume `energycap-data` |
| healthcheck | **none** — `container` has no such concept; poll `/healthz` yourself | `healthcheck:` in compose |
| log rotation | **none** — launchd appends forever; add a `newsyslog` stanza | `json-file`, 10 MB × 5 |

Apple's `container` has no compose, no restart policy, no healthcheck and no
`depends_on`, so the deployment is a wrapper script plus a LaunchAgent rather than a
compose file. **[`deploy/README.md`](deploy/README.md) is the full guide** — installation,
the launchd commands, the line-by-line table of what compose gave us and what replaces
it, the uid trap on the bind mount, and why an unattended reboot needs auto-login.

The two paths do **not** share state (bind mount vs. named volume) and must never run at
the same time: that is two collectors, two spools, and both hammering the same clouds.

`docker-compose.yml` is unchanged, still correct, and still the only option off Apple
silicon.

**Neither path has ever been built or run**, here or anywhere: there is no `container`
CLI and no Docker daemon on the development machine. See
[Known-unproven](#known-unproven) #2.

---

## How Leviton values are kept fresh

Read this before trusting a Leviton wattage, and before changing `LEVITON_INGEST`.

### The problem, as measured on this hardware

PLAN.md §2.8 and §6.4 locked "REST polling at 30s + bandwidth keepalive, not WebSocket".
On 2026-08-17 that decision was **overturned on measurement** against the two live
LWHEM-2 hubs (firmware 2.1.2) — see [`DEVIATIONS.md`](DEVIATIONS.md) #144. What the
measurements showed:

- Over a 5-minute production run (20 cycles) and a separate 12-minute probe (46 reads at
  15s), **10 of 12 channels never changed value at all**. A whole-panel `GRID_POWER` CT
  feed held **exactly 4086.05 W across 46 consecutive reads**; another held exactly
  505.17 W across the same 46. Changes arrived in bursts and then went flat for minutes,
  and *which* hub looked live changed between runs — so it was not one broken hub.
- An **A/B probe** ran four reads with no keepalive, then four each preceded by
  `PUT {"bandwidth": 1}`. The PUT demonstrably lands (the hub's `bandwidth` field reads 0
  at rest and 2 afterwards, i.e. 1 auto-decayed to 2 exactly as PLAN.md §6.4 describes) —
  and **both phases were identically frozen**. The keepalive changes nothing for a REST
  reader.
- The reference integration (`gtxaspec/leviton-load-center`) documents why: setting
  `bandwidth=1` triggers a full state flood from the server, and **that flood is pushed
  over the WebSocket**. Its REST path is documented as *"initial discovery, fallback
  polling (10-minute interval)"*. This pipeline was polling REST **20× faster than the
  reference's fallback rate and receiving a cache**.

The house is also about to go from 12 metered channels to ~40 as smart breakers are
installed. Per-breaker resolution is exactly where a stale cache hurts most.

### What changed — and, more importantly, what did not

A WebSocket subscriber (`wss://socket.cloud.leviton.com/`) now maintains an **in-memory
current-state store**, merging the partial deltas the cloud pushes. That is *all* it
does.

**The 30-second poll cycle still samples that store and emits exactly the rows it emitted
before, with one `ts_utc` per cycle.** There is no row per WebSocket delta. That was a
deliberate refusal: per-delta rows would make sampling irregular, which breaks the kWh
formula (`mean_watts × sample_count × poll_interval_s / 3.6e6` assumes a fixed cadence)
and destroys `sample_count`'s meaning as the gap detector. Everything downstream — the
spool, uploader, compactor, rollup, Glue tables, every query in this README — is
untouched. The only difference is that the sampled values are current instead of cached.
There is exactly one implementation of the row mapping and both ingestion paths run
through it; a test fails if a second one ever appears.

### The three modes (`LEVITON_INGEST`)

| mode | values come from | REST still does | on a socket problem |
|---|---|---|---|
| `hybrid` **(default)** | the push store | discovery, plus a periodic full re-read (`LEVITON_REST_RECONCILE_S`) that also **measures** how far behind the cache runs | reads REST for that cycle and records the fallback |
| `ws` | the push store | discovery only | **emits nothing** — a gap and a counted failure, exactly like a failed REST cycle |
| `rest` | REST, every cycle | everything | n/a — no socket is ever built |

`rest` is byte-for-byte the behaviour that shipped before this change, and it is the
instant revert: set it in `.env` and restart. No code change, no rebuild.

The knobs, all in [`.env.example`](.env.example) with the reasoning inline:

| variable | default | what it is |
|---|---|---|
| `LEVITON_INGEST` | `hybrid` | `hybrid` \| `ws` \| `rest`, as above |
| `LEVITON_WS_URL` | `wss://socket.cloud.leviton.com/` | so a moved endpoint is a config edit, not a release |
| `LEVITON_WS_RECONNECT_S` | `3300` | proactive reconnect. Leviton hard-kills a socket at exactly 60 minutes, so rotate at 55 and take the changeover on our own schedule. Must be < 3600 |
| `LEVITON_WS_STALL_TIMEOUT_S` | `90` | how long an **open** socket may go completely silent before it is treated as dead. Must exceed `POLL_INTERVAL_S` |
| `LEVITON_REST_RECONCILE_S` | `600` | `hybrid` only: the full REST re-read and drift measurement. 600s is the reference integration's own fallback cadence |

### The rule that keeps this honest

Sampling an in-memory store is structurally a hold-last-value, and holding the last value
is the one thing this pipeline must never do. The line that makes it legitimate:

> **Connection state gates emission. Field age never does.**

While the socket is connected *and* a full state sync has completed for that connection,
the store holds what the server currently believes is true, and sampling it is honest —
including a field that has not moved in ten minutes, because a resistive water-heater
element genuinely sits at 2462 W and gapping on "this has not changed recently" would
delete real data. While the socket is **disconnected, unsynced, auth-suspect, or open but
silent past `LEVITON_WS_STALL_TIMEOUT_S`**, we do not know the current value, and emitting
the last one under a current timestamp would be fabrication. So we emit nothing.

That last case — open but silent — is the dangerous one, because the TCP connection stays
healthy and the library reports `connected: true` throughout. Without the stall guard the
sampler would lift the same frozen numbers out of the store every 30s and stamp each with
a fresh `ts_utc`, producing an archive indistinguishable from a genuinely steady load.
Both gates are pinned by tests that were verified by breaking them and watching stale rows
appear (DEVIATIONS.md #144).

"Connection state" is meant literally, and in three places where reading it loosely would
let a hold-last-value back in (DEVIATIONS.md #159):

- **it is a property of each field.** The store is deliberately not cleared on reconnect,
  so at the instant the gate reopens, every field the new connection did not re-establish
  is dropped — `fields_evicted` counts them. That is a membership test, never an age
  threshold: a value pushed 55 minutes into a connection is current, a value pushed on the
  *previous* connection is not.
- **it is a property of each hub's feed.** Two hubs share one socket here, so an
  aggregate "some frame arrived" watchdog is kept happy by whichever hub is healthy while
  the other one's push feed is dead. Liveness is evaluated per hub — `hub_silence_s` and
  `stalled_hubs` in `status.json` — and a dead feed both shuts that hub's gate and forces
  the reconnect that is the only way to recover a subscription.
- **an explicit `null` from the REST seed clears the field**, rather than being skipped.
  A seed that can overwrite a value but never remove one would leave a stale number
  standing exactly where the API has just said "unknown".

Per-field last-update instants are recorded as **diagnostics** in `status.json` — the
`leviton_ws.objects` map, one entry per subscribed object, giving each field's age,
update count, provenance (`receipt`, `server`, or `rest_seed`) and last-update timestamp.
They are *reported and never consulted*: nothing in the gate reads them. They exist so the
real update distribution can be measured and this decision revisited on evidence, and
there is deliberately no max-age threshold anywhere in the code.

### Telling a WS-fresh row from a REST-cached one

**Not from the row.** The canonical schema (PLAN.md §3) has no provenance column and is
not getting one — adding one would change the dedupe key and every downstream table for a
property of the *collector*, not of the measurement. Rows are identical either way; that
is the point of having one mapper.

You tell them apart from **`status.json`** (served at `/healthz`), which gains two
sections:

```json
{
  "leviton_ingest": {
    "mode": "hybrid",
    "value_source": "ws",
    "ws_withheld_reason": null,
    "cycles_ws": 2841,
    "cycles_rest_fallback": 12,
    "cycles_withheld": 0,
    "last_reconcile_drift": {"compared": 46, "differing": 31}
  },
  "leviton_ws": {
    "connected": true,
    "synced": true,
    "sync_mode": "flood",
    "hub_silence_s": {"1000_0046_1D52": 0.4, "1000_0046_1D48": 2.1},
    "stalled_hubs": [],
    "fields_evicted": 7,
    "last_close_code": 1006,
    "reconnects": 14,
    "server_drops": 9,
    "messages_per_s": 3.7,
    "null_deltas_by_field": {}
  }
}
```

- `value_source` is the *current* cycle's answer: `ws`, `rest`, `rest_fallback`, or
  `withheld`. **Every transition of it is logged at INFO**, so a stretch of cached rows is
  reconstructable after the fact from the logs even though the rows themselves do not say
  so. The per-cycle line is DEBUG; only the transitions are INFO, so at `LOG_LEVEL=INFO`
  you get intervals rather than a line every 30 seconds.
- `cycles_ws` vs `cycles_rest_fallback` is the running score: how often the socket was
  actually usable.
- `ws_withheld_reason` names *why* the gate was shut — `disconnected`,
  `awaiting_initial_sync`, `stalled`, `auth_failed`, `not_started`, `ws_error`.
- `sync_mode` says how the current connection re-established state, and `flood` means one
  thing only: **every desired subscription target was touched by the flood on this
  connection**. `timeout` means it was not and the gate opened on the REST seed after
  `SYNC_FLOOD_TIMEOUT_S` (20s) — expect `timeout` at first. It is the number that decides
  whether the push feed is working, so it is never reported off the timeout path, not even
  for a connection with nothing to await.
- `hub_silence_s` is per hub, and `stalled_hubs` names the ones past
  `LEVITON_WS_STALL_TIMEOUT_S`. **Both hubs must be moving**; one number moving and the
  other frozen is the failure an aggregate watchdog cannot see.
- `fields_evicted` counts the values a reconnect did not re-prove and therefore dropped.
  Non-zero after a reconnect is expected and healthy — those are gaps rather than
  fabrications. `server_drops` separates server-side closes from our own deliberate
  55-minute rotation, both of which are counted in `reconnects`.
- `last_reconcile_drift` is `hybrid`'s cross-check, and the direct measurement of the
  problem this section opens with: both value sets are mapped through the same mapper and
  the number of metrics they disagree on is counted. A high `differing` is the frozen
  cache, quantified. Those comparison rows are counted and discarded — they never reach
  the spool.

In `ws` mode there is nothing to tell apart: every row came from the socket, and the
cycles that could not are simply absent, visible as a dip in `sample_count`. That dip is
honest, and it is what makes `ws` a measurement rather than a convenience.

### What is deliberately not done

- **`bandwidth: 0` is never sent**, not even as the middle of the `1 → 0 → 1` cycle the
  reference integration uses. Firmware 2.1.0 disconnects a hub for 10–20 seconds on
  receipt of a 0, which is a self-inflicted data gap, and the references also report that
  the 0 step makes the cloud emit transient zeros — which this pipeline would archive
  verbatim as real readings. The existing keepalive (`PUT {"bandwidth": 1}` every 50s)
  stays exactly as it was; with a subscriber attached it finally does what it was for.
- **`pollBreakers` is not sent.** It was investigated and rejected: a Poll request
  refreshes CT/breaker *lifetime* (cumulative energy) values only, and those stopped
  functioning at firmware 2.1.0. These hubs are 2.1.2, and PLAN.md §6.3 already excludes
  `energyConsumption`/`energyImport` for the same reason.
- **`GET /apiversion` every 10s** — a third keepalive the official app and one reference
  integration both use — is **not** implemented. It is a new outbound call pattern that
  has not been authorised. If the socket connects but the feed stays slow, it is the first
  thing to try.

---

## Operating it

Under Apple `container` + launchd on the Mac Mini
([`deploy/README.md`](deploy/README.md) has the rest):

```bash
./scripts/energycap-container.sh status         # subsystem, container, IP, /healthz, data dir
./scripts/energycap-container.sh logs -f        # structured JSON, one object per line
curl -s localhost:8080/healthz | jq
launchctl kickstart -k gui/$(id -u)/com.duckbillhq.energycap   # restart
launchctl bootout     gui/$(id -u)/com.duckbillhq.energycap    # stop for real
```

Under Docker (the fallback, and the only path off Apple silicon):

```bash
docker compose up -d
docker compose logs -f            # structured JSON, one object per line
curl -s localhost:8080/healthz | jq
docker compose restart            # the SQLite spool on /data survives; nothing is lost
```

Either way the SQLite spool on `/data` survives a restart, so nothing is lost. **Under
launchd, `energycap-container.sh stop` is not a stop** — `KeepAlive` starts it straight
back up, which is the whole point of it; `launchctl bootout` is the real one.

`GET /healthz` (also `/health`, `/`, `/status.json`) serves `/data/status.json` plus a
derived `health` block, and returns **503** when a poller's last success is older than
3× its poll interval. Under Docker that is also the container's healthcheck; **under
Apple `container` nothing polls it** — the runtime has no healthcheck concept, and
`KeepAlive` only ever sees the process die, so a wedged-but-alive collector goes unnoticed
until someone looks (`deploy/README.md` sketches the watchdog that would close this).
`status.json` is the
operational dashboard: per-source `last_success_utc` / `consecutive_failures`, the last
hour uploaded, the last day compacted and rolled, the spool's pending row count, and the
`leviton_ingest` / `leviton_ws` sections described in
[How Leviton values are kept fresh](#how-leviton-values-are-kept-fresh).

`/healthz` judges **pollers only**. A sick WebSocket does not turn the container
unhealthy, because in `hybrid` the REST fallback is still landing rows and an unhealthy
container would be a false alarm; read the `leviton_ws` section for the socket's own
state.

**`GET /ui` — the live dashboard**, on the same port (`http://localhost:8080/ui`). Open it
to watch the data arrive instead of waiting for it to reach S3: latest value per channel
with a 30-minute sparkline, the three biggest watt channels overlaid, the HVAC readings
with `mode` / `fan` / `stage` decoded to words, and the last six local hours aggregated —
mean, `sample_count` / expected, coverage and kWh, with **no row at all** for an hour that
collected nothing. Lines **break** across a gap and the gap is shaded; a reading of 0 (the
load was off) is drawn and worded differently from an absent sample (the collector was
down). `GET /ui/data` is the same snapshot as JSON if you would rather read it with `jq`.
It is one self-contained HTML file — no framework, no CDN, no external asset, so it works
with the network unplugged — it opens the spool **read-only**, and it adds no dependency
(DEVIATIONS.md #164). Like `/healthz`, it is unauthenticated: expose the port only to a
network you trust.

**Scrolling back in time.** The watts chart has a movable window: pick **30m / 1h / 6h /
24h**, pan back and forward half a window at a time (the ◀ ▶ buttons, dragging the plot,
or ← / →), and click **Live** (or press Home) to follow *now* again. Panning freezes the
chart's window while the rest of the page keeps refreshing, and it stops at the oldest row
the spool still holds — which the page names in words, because "the spool never had it" is
not an outage. Over the wire this is `GET /ui/data?window_s=<60..86400>&end=<ISO-8601>`;
both are optional, `end` omitted means *live*, and a malformed value is a **400** rather
than a quietly different chart (DEVIATIONS.md #165). An hour or less is drawn from raw 30s
samples; longer windows are **bucketed on the server** (24h → 2.5-minute buckets), and a
bucket with no samples arrives as an explicit hole — the line breaks there exactly as it
does at a raw gap — while a bucket with fewer samples than expected keeps the mean of the
samples it actually has and says how many that was. The axis always states what one mark
is, so a day of data never pretends to be 30-second resolution.

Inside `energycap run` there is one asyncio process hosting the poll loops, the Leviton
bandwidth keepalive (`PUT {"bandwidth": 1}` every 50s, never 0), the Leviton WebSocket
subscriber and its watchdog, a small scheduler, and the health server. The keepalive is
still mandatory, but note what it is mandatory *for*: it triggers the cloud's full state
flood, and that flood is delivered over the socket — the keepalive on its own does **not**
unfreeze a REST reader, which was measured directly. See
[How Leviton values are kept fresh](#how-leviton-values-are-kept-fresh).

| job | fires | does |
|---|---|---|
| `upload_hourly` | every hour at **:05** | spool → `part-{YYYYMMDD}T{HH}.parquet` for closed hours |
| `rollup_hourly` | every hour at **:20** | regenerate `rollup-{YYYYMMDD}.parquet` for the day(s) of hour HH-1 |
| `daily_maintenance` | **01:30** local | upload catch-up, then compact and re-roll D-3..D-1 (late data heals itself), then purge the spool |
| `bryant_daily_energy` | **08:30** local | Carrier daily energy for day2..day1 → `energy/daily` |

Everything on that list is also a standalone CLI command over an arbitrary local date
range, and every one is idempotent — output filenames are deterministic, so a re-run
overwrites instead of duplicating.

**If you fix a collector bug, re-run the rollup over the affected range.** That is the
documented heal for late or corrected data:

```bash
uv run energycap rollup --start 2026-08-01 --end 2026-08-16
```

---

## Commands

All dates are **local** (`YYYY-MM-DD`). `--start` alone means that one day. Global
options: `--log-level`, `--traceback`, `--version`.

| command | what it does | default window |
|---|---|---|
| `energycap run` | The long-lived collector: poll loops, keepalive, scheduler, `/healthz`. What the container runs. | — (blocks until SIGTERM) |
| `energycap poll [--once] [--source ...]` | One poll cycle (or the loop) → SQLite spool. A failed cycle writes zero rows. | — |
| `energycap upload` | Closed local hours from the spool → `part-*.parquet`, verified by S3 Parquet row count before the spool rows are marked uploaded. | yesterday → today |
| `energycap compact-daily` | A day's parts → `day-{YYYYMMDD}.parquet`, then (only after the row count verifies) moves the parts to the archive prefix. | yesterday |
| `energycap rollup` | Rebuild the whole local day's hourly rollup. The heal for late data. | yesterday → today |
| `energycap fetch-daily` | Carrier daily energy → `energy/daily` (day1 + day2 revision). | D-2 → D-1 |
| `energycap backfill` | Historical Bryant energy from the legacy DynamoDB table + the old collector's JSON, DynamoDB preferred on overlap. Read-only against DynamoDB. | D-2 → D-1 |
| `energycap discover [--dump FILE]` | Enumerate the live Leviton/Bryant hierarchy; print a `channel_map.json` skeleton for unmapped channels and write `live_channels.json` beside the map. `--dump` records every raw upstream response. | — |
| `energycap build-dim [--dry-run]` | `channel_map.json` + blackstart inventory → `dim_channel.parquet` (single object, atomically overwritten). Reads `live_channels.json` to WARN about unmapped channels. | — |
| `energycap create-glue-tables [--dry-run]` | Idempotent create-or-update of the four Glue tables and their comments. No crawler. | — |
| `energycap import-greenbutton FILE` | An LG&E **Download My Data** export (Green Button ESPI XML, or MyMeter's `Usage.csv`) → `energy/meter` interval rows. Local Parquet by default; `--bucket` to also mirror to S3. Refuses to guess at units — see below. | — |
| `energycap greenbutton-authorize [--code …]` | Authorize against Green Button Connect. No `--code` prints the URL to open; `--code` exchanges it and caches tokens at `SPOOL_DIR/tokens/lge.json`, mode 600. | — |
| `energycap fetch-greenbutton` | The same meter intervals over the authorized Connect API instead of a downloaded file — same parser, same writer. Scheduled daily at 09:15 local. | D-3 → today |
| `energycap compare-meter` | The utility meter vs. the summed service-feed CTs, hour by hour, with sample coverage. | yesterday → today |

### Meter vs. panels

The point of the sub-metering is that it should add up to the bill. These two
commands check that it does, and they need no AWS:

```bash
# 1a. Automated — authorize once, then it fetches itself daily at 09:15 local.
container exec energycap energycap greenbutton-authorize     # prints a URL; open it
container exec energycap energycap fetch-greenbutton --start 2026-08-14

# 1b. Or manual — MyMeter → Download My Data → Green Button XML, dropped in ./data.
container exec energycap energycap import-greenbutton /data/GreenButton.xml

# 2. Compare against ct_1_a + ct_1_b on both hubs — the two service feeds.
container exec energycap energycap compare-meter --start 2026-08-14 --end 2026-08-17
```

Authorizing needs a MyMeter **local** account — one whose email differs from your
My Account login, created with a registration code LG&E emails on request. The
browser sends you back to the published callback page, which hands the code to
the collector on `localhost:8080`; the code never reaches a host we run.

Both run **inside the container**, because the collector owns the spool and
opening it from the macOS host while the container writes it corrupts the
database.

Two things the importer will not do, both for the same reason — a meter
comparison exists to catch errors, so it must not introduce any:

- **Guess at units.** ESPI states them in a `ReadingType` (`uom` +
  `powerOfTenMultiplier`). If the export omits it the import *fails* rather than
  assuming watt-hours; `--assume-uom Wh` is the deliberate override.
- **Invent an interval length.** The XML states it. A CSV does not, so it comes
  from an end-time column, or `--interval-s`, or is inferred from the reading
  spacing and logged as inferred.

**LG&E's export carries the same series under three meter ids.** A real download
(2026-08-18) held `1308468`, `944401` and `944006`, identical to the watt-hour for
every interval of ten days — the same service through meter changes. `compare-meter`
collapses identical series to one and says so; if several meters genuinely differ it
refuses and makes you pick with `--meter`. Summing them would have trebled the meter
reading and made the panels look like they measure a third of the house.

`compare-meter` reads `coverage` — `sample_count` over a full hour's samples —
and keeps hours below `--min-coverage` (default 0.9) out of the totals while
still printing them. An hour the collector only half observed shows ~half the
panel energy; that is the collector being down, not the CTs being wrong, and
conflating the two would be the easiest way to misread this whole exercise.

---

## Querying with DuckDB

Replace `my-energy-bucket` with your `S3_BUCKET`. One-time session setup — the same
credential chain the pipeline itself uses:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET energy (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
SET TimeZone = 'UTC';   -- so ts_utc displays as UTC; ts_local is naive either way
```

Two things about paths, both of which will bite otherwise:

- **Point the glob at the partitions you want.** `energy/raw_30s/year=2026/month=08/day=15/*.parquet`
  reads one local day; `.../month=08/day=*/*.parquet` reads a month. The queries below do
  this, which is why they never mention partition columns.
- **If you do want the `year`/`month`/`day` columns**, pass `hive_partitioning = true` —
  but also pass `hive_types`, because DuckDB reads `month=08` back as the **string**
  `'08'` (a leading zero does not auto-cast) while `year=2026` and `day=15` come back as
  integers:

  ```sql
  SELECT r.year, r.month, r.day, count(*) AS rows_scanned
  FROM read_parquet(
          's3://my-energy-bucket/energy/raw_30s/*/*/*/*.parquet',
          hive_partitioning = true,
          hive_types = {'year': INTEGER, 'month': INTEGER, 'day': INTEGER}
       ) AS r
  WHERE r.year = 2026 AND r.month = 8 AND r.day BETWEEN 14 AND 15
    AND r.metric = 'watts'
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3;
  ```

If you query interactively a lot, define views once
(`CREATE VIEW dim AS SELECT * FROM read_parquet('s3://…/dim_channel.parquet');`) and drop
the repetition. The queries below are written self-contained so they can be pasted
anywhere.

### Three rules the queries below obey — break them and you get a wrong number

**1. A channel is `(source, device_id, channel_id)`, never `channel_id` alone.**
`channel_id` is unique only *within* a device. This house has **two** Leviton hubs, so
`panel_leg_a`, `panel_leg_b`, breaker positions and CT channel numbers all repeat across
the two panels — `breaker_p10` on panel A and `breaker_p10` on panel B are different
circuits carrying different loads. Group and label on the whole triple. Grouping on
`coalesce(short_label, channel_id)` instead silently adds two circuits together: the tell
is a doubled `sample_count` (5760 where a day can only hold 2880), and the kWh is the sum
of two unrelated loads. This bites hardest on unmapped channels, where the label falls
back to the bare `channel_id` — and per [Known-unproven](#known-unproven), a channel stays
unmapped until someone re-runs `energycap discover` and edits `channel_map.json`.

**2. The literal `30` in these queries is `POLL_INTERVAL_S`.** It appears in every
`samples_expected`, every `pct_of_hour_observed`, and in the observed-time kWh arithmetic.
If you change that setting, change it here too — and remember that rows collected before
the change were sampled at the old interval, so a range spanning it has no single correct
literal.

**3. Expected sample counts come from real elapsed time, not from `24`.** An hourly rollup
bucket is always exactly one real hour (it is keyed on `hour_start_utc`), so `3600 / 30`
is always right per hour. A local **day** is not: it is 23 hours on the March
spring-forward Sunday and 25 on the November fall-back Sunday, so the honest daily
expectation is 2760 / 2880 / 3000 and never a hardcoded 2880. The daily query below
derives it from the actual UTC extent of the local day; copy that idiom rather than the
number.

### 0. Orient yourself: what channels exist?

Start here. This is the table an LLM should read before writing anything else.

```sql
SELECT source, device_id, channel_id, label, category, panel, slots, room,
       priority, estimated_watts
FROM read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet')
ORDER BY source, device_id, channel_id;
```

### 1. What did the heat pump draw yesterday afternoon?

The 30s detail, joined to the semantic layer so the answer has a name in it:

```sql
SELECT r.ts_local, r.device_id, r.channel_id, d.short_label, r.value AS watts
FROM read_parquet('s3://my-energy-bucket/energy/raw_30s/year=2026/month=08/day=15/*.parquet') AS r
JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
  ON d.source = r.source AND d.device_id = r.device_id AND d.channel_id = r.channel_id
WHERE r.metric = 'watts'
  AND d.category = 'hvac'
  AND r.ts_local >= TIMESTAMP '2026-08-15 12:00:00'
  AND r.ts_local <  TIMESTAMP '2026-08-15 18:00:00'
ORDER BY r.ts_local, r.device_id, r.channel_id;
```

`device_id` and `channel_id` ride along because two hubs can carry the same `channel_id`
and a human can give two circuits the same `short_label` — rule 1 above.

The same afternoon summarised, **one row per physical channel** — note `samples` next to
`samples_expected`, and that the kWh is computed the same observed-time way the rollup
does it:

```sql
SELECT
    r.source, r.device_id, r.channel_id,
    d.short_label,
    count(*)                                       AS samples,
    -- Real elapsed seconds of the requested LOCAL window / POLL_INTERVAL_S.
    -- Written this way it stays true if you move the window across a DST change.
    CAST((epoch(TIMESTAMP '2026-08-15 18:00:00' AT TIME ZONE 'America/Kentucky/Louisville')
        - epoch(TIMESTAMP '2026-08-15 12:00:00' AT TIME ZONE 'America/Kentucky/Louisville'))
         / 30 AS BIGINT)                           AS samples_expected,
    round(avg(r.value), 1)                         AS mean_watts,
    round(max(r.value), 1)                         AS peak_watts,
    round(avg(r.value) * count(*) * 30 / 3.6e6, 3) AS kwh_observed
FROM read_parquet('s3://my-energy-bucket/energy/raw_30s/year=2026/month=08/day=15/*.parquet') AS r
JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
  ON d.source = r.source AND d.device_id = r.device_id AND d.channel_id = r.channel_id
WHERE r.metric = 'watts'
  AND d.category = 'hvac'
  AND r.ts_local >= TIMESTAMP '2026-08-15 12:00:00'
  AND r.ts_local <  TIMESTAMP '2026-08-15 18:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY kwh_observed DESC;
```

If `samples` is well below `samples_expected`, the answer covers less time than you asked
for. Say so out loud rather than reporting the kWh as if the afternoon were complete. If
`samples` is a clean multiple of `samples_expected`, you have merged channels rather than
found extra data — check the `GROUP BY`.

### 2. Hourly kWh by label for a week — **always with `sample_count`**

```sql
SELECT
    h.local_hour_start,
    h.source, h.device_id, h.channel_id,
    coalesce(d.short_label, h.channel_id) AS channel,
    round(h.kwh, 4)  AS kwh,
    round(h.mean, 1) AS mean_watts,
    h.sample_count,
    -- An hourly bucket is keyed on hour_start_utc, so it is always exactly one
    -- real hour — 3600/30 is correct even on the DST transition days.
    round(100.0 * h.sample_count / (3600 / 30)) AS pct_of_hour_observed
FROM read_parquet('s3://my-energy-bucket/energy/hourly/year=2026/month=08/rollup-*.parquet') AS h
LEFT JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
       ON d.source = h.source AND d.device_id = h.device_id AND d.channel_id = h.channel_id
WHERE h.metric = 'watts'
  AND h.local_hour_start >= TIMESTAMP '2026-08-10 00:00:00'
  AND h.local_hour_start <  TIMESTAMP '2026-08-17 00:00:00'
ORDER BY h.local_hour_start, h.source, h.device_id, h.channel_id;
```

Rolled up to days, still carrying the coverage. The `GROUP BY` carries the whole channel
identity, and `samples_expected` is **derived from the local day's real length** instead
of assuming 24 hours:

```sql
WITH hourly AS (
    SELECT *
    FROM read_parquet('s3://my-energy-bucket/energy/hourly/year=2026/month=08/rollup-*.parquet')
    WHERE metric = 'watts'
      AND local_hour_start >= TIMESTAMP '2026-08-10 00:00:00'
      AND local_hour_start <  TIMESTAMP '2026-08-17 00:00:00'
),
day_length AS (
    -- 86400 normally; 82800 on the spring-forward Sunday, 90000 on the fall-back one.
    SELECT local_day,
           epoch((local_day + INTERVAL 1 DAY)::TIMESTAMP
                 AT TIME ZONE 'America/Kentucky/Louisville')
         - epoch(local_day::TIMESTAMP AT TIME ZONE 'America/Kentucky/Louisville')
               AS seconds
    FROM (SELECT DISTINCT CAST(local_hour_start AS DATE) AS local_day FROM hourly)
)
SELECT
    CAST(h.local_hour_start AS DATE)      AS local_day,
    h.source, h.device_id, h.channel_id,
    coalesce(d.short_label, h.channel_id) AS channel,
    round(sum(h.kwh), 3)                  AS kwh,
    sum(h.sample_count)                   AS samples,
    CAST(any_value(l.seconds) / 30 AS BIGINT) AS samples_expected,   -- 30 = POLL_INTERVAL_S
    count(*)                              AS hours_present
FROM hourly AS h
JOIN day_length AS l ON l.local_day = CAST(h.local_hour_start AS DATE)
LEFT JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
       ON d.source = h.source AND d.device_id = h.device_id AND d.channel_id = h.channel_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, kwh DESC;
```

`hours_present` is 24 on a normal day, **23 on the March spring-forward day and 25 on the
November fall-back day** — that is correct, not a bug, and `samples_expected` now moves
with it (2760 / 2880 / 3000). If `hours_present` comes out at 48 you have merged two
devices' channels; see rule 1 above.

### 3. Find the gaps

Two different kinds of gap, two queries. First, hours that exist but are short — a full
hour of `watts` at 30s is 120 samples (~118 in practice):

```sql
SELECT
    h.local_hour_start,
    h.source, h.device_id, h.channel_id,
    coalesce(d.short_label, h.channel_id) AS channel,
    h.sample_count,
    (3600 / 30) - h.sample_count AS samples_missing,   -- 30 = POLL_INTERVAL_S
    h.first_ts_utc,
    h.last_ts_utc,
    round(h.kwh, 4) AS kwh_of_observed_time
FROM read_parquet('s3://my-energy-bucket/energy/hourly/year=2026/month=08/rollup-*.parquet') AS h
LEFT JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
       ON d.source = h.source AND d.device_id = h.device_id AND d.channel_id = h.channel_id
WHERE h.metric = 'watts'
  AND h.sample_count < 118
ORDER BY h.sample_count, h.local_hour_start;
```

`first_ts_utc` well after the hour start means the hour *begins* with a gap;
`last_ts_utc` well before the end means it ends with one.

Second — and this is the one people forget — hours with **no row at all**, which is what
a full outage looks like. The hour spine is generated in UTC (uniform hours), so it is
correct across DST:

```sql
WITH observed AS (
    SELECT source, device_id, channel_id, hour_start_utc
    FROM read_parquet('s3://my-energy-bucket/energy/hourly/year=2026/month=08/rollup-*.parquet')
    WHERE metric = 'watts'
      AND local_hour_start >= TIMESTAMP '2026-08-10 00:00:00'
      AND local_hour_start <  TIMESTAMP '2026-08-17 00:00:00'
),
spine AS (
    SELECT unnest(generate_series(min(hour_start_utc), max(hour_start_utc), INTERVAL 1 HOUR))
               AS hour_start_utc
    FROM observed
),
channels AS (SELECT DISTINCT source, device_id, channel_id FROM observed)
SELECT
    s.hour_start_utc AT TIME ZONE 'America/Kentucky/Louisville' AS local_hour_missing,
    c.source, c.device_id, c.channel_id,
    coalesce(d.short_label, c.channel_id) AS channel
FROM spine AS s
CROSS JOIN channels AS c
LEFT JOIN observed AS o
       ON o.hour_start_utc = s.hour_start_utc
      AND o.source = c.source AND o.device_id = c.device_id AND o.channel_id = c.channel_id
LEFT JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
       ON d.source = c.source AND d.device_id = c.device_id AND d.channel_id = c.channel_id
WHERE o.hour_start_utc IS NULL
ORDER BY 1, 2, 3, 4;
```

This spine exists **only in the query**. Nothing gap-filled is ever written to S3.

### 4. Bryant state vs Leviton watts, on a common time bucket

What the HVAC was *doing* against what it was *drawing*, in five-minute buckets, with the
enum codes decoded inline:

```sql
WITH raw AS (
    SELECT *
    FROM read_parquet('s3://my-energy-bucket/energy/raw_30s/year=2026/month=08/day=15/*.parquet')
    WHERE ts_local >= TIMESTAMP '2026-08-15 13:00:00'
      AND ts_local <  TIMESTAMP '2026-08-15 19:00:00'
),
hvac_instants AS (
    -- Sum ACROSS channels within one instant, then average across the bucket.
    -- avg() straight over every (channel, instant) row would report the mean of
    -- the HVAC channels instead of the HVAC total — half the truth on two CT legs.
    SELECT time_bucket(INTERVAL 5 MINUTE, r.ts_utc) AS bucket,
           r.ts_utc,
           sum(r.value) AS watts,
           count(*)     AS channels_reporting
    FROM raw AS r
    JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
      ON d.source = r.source AND d.device_id = r.device_id AND d.channel_id = r.channel_id
    WHERE r.source = 'leviton' AND r.metric = 'watts' AND d.category = 'hvac'
    GROUP BY 1, 2
),
hvac AS (
    SELECT bucket,
           round(avg(watts))         AS hvac_watts,
           sum(channels_reporting)   AS watt_samples,
           max(channels_reporting)   AS hvac_channels
    FROM hvac_instants
    GROUP BY 1
),
sys_state AS (
    -- BOTH renderings of odu.opstat are selected on purpose. A staged outdoor
    -- unit fills stage_code and leaves stage_pct NULL; a variable-capacity one
    -- (this house — odu.type gs3ngiphp) fills stage_pct and NEVER writes a
    -- 'stage' row at all. Ask for only one and you may get a column of nulls
    -- that looks like "the compressor was off".
    SELECT time_bucket(INTERVAL 5 MINUTE, ts_utc) AS bucket,
           max(CASE WHEN metric = 'mode'  THEN value END) AS mode_code,
           max(CASE WHEN metric = 'stage' THEN value END) AS stage_code,
           round(avg(CASE WHEN metric = 'stage_pct' THEN value END), 1) AS stage_pct,
           round(avg(CASE WHEN metric = 'outdoor_temp_f' THEN value END), 1) AS outdoor_f
    FROM raw WHERE source = 'bryant' AND channel_id = 'system'
    GROUP BY 1
),
zone_state AS (
    SELECT time_bucket(INTERVAL 5 MINUTE, ts_utc) AS bucket,
           round(avg(CASE WHEN metric = 'indoor_temp_f'   THEN value END), 1) AS indoor_f,
           round(avg(CASE WHEN metric = 'setpoint_cool_f' THEN value END), 1) AS setpoint_cool_f
    FROM raw WHERE source = 'bryant' AND channel_id = 'zone_1'
    GROUP BY 1
)
SELECT
    s.bucket AT TIME ZONE 'America/Kentucky/Louisville' AS local_time,
    m.name AS mode,
    st.name AS stage,        -- NULL here: this outdoor unit is variable-capacity
    s.stage_pct,             -- 0-100 compressor capacity: the signal this house emits
    s.outdoor_f, z.indoor_f, z.setpoint_cool_f,
    h.hvac_watts, h.watt_samples, h.hvac_channels
FROM sys_state AS s
LEFT JOIN zone_state AS z ON z.bucket = s.bucket
LEFT JOIN hvac AS h ON h.bucket = s.bucket
LEFT JOIN (VALUES (0,'off'),(1,'heat'),(2,'cool'),(3,'auto'),(4,'fanonly'),
                  (5,'hpheat'),(6,'electric'),(7,'gasheat'),(8,'dehumidify'))
       AS m(code, name) ON m.code = s.mode_code
LEFT JOIN (VALUES (0,'off'),(1,'low'),(2,'high'),(3,'idle'),(4,'dehumidify'))
       AS st(code, name) ON st.code = s.stage_code
ORDER BY s.bucket;
```

Enum metrics are averaged at your peril: the mean of `mode` codes is meaningless. Use
`max`/`min` within a bucket (as above) to see the state, and if they disagree the state
changed inside the bucket. Never take `avg()` of `mode`, `stage` or `fan`. `stage_pct` is
the exception and is aggregated with `avg()` above deliberately — it is a real percentage,
so its bucket mean is the mean compressor capacity, which is exactly what you want beside
`hvac_watts`. On this system `stage` comes back NULL and `stage_pct` carries the signal;
on a staged unit it is the reverse. See
[Compressor stage](#compressor-stage-stage-vs-stage_pct).

The bucket key is `ts_utc`, deliberately — `ts_local` would merge the two 01:00 local
hours of the November fall-back Sunday into one set of buckets, averaging watts across two
physically different hours and doubling every sample count. The local time in the output
is a **label derived from the bucket**, never the bucket itself. `hvac_channels` says how
many HVAC channels were actually summed, so an instant where one CT went missing shows up
instead of quietly lowering the total.

### 5. Daily Bryant energy history

Day-grain rows from the Carrier cloud, stamped at **local midnight** of the measured day:

```sql
SELECT
    CAST(e.ts_local AS DATE)              AS local_day,
    e.device_id,
    e.channel_id,
    coalesce(d.short_label, e.channel_id) AS component,
    round(max(CASE WHEN e.metric = 'kwh_day'      THEN e.value END), 2) AS kwh,
    round(max(CASE WHEN e.metric = 'cost_day_usd' THEN e.value END), 2) AS usd
FROM read_parquet('s3://my-energy-bucket/energy/daily/year=*/bryant-*.parquet') AS e
LEFT JOIN read_parquet('s3://my-energy-bucket/energy/dim_channel/dim_channel.parquet') AS d
       ON d.source = e.source AND d.device_id = e.device_id AND d.channel_id = e.channel_id
WHERE e.ts_local >= TIMESTAMP '2026-08-01 00:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, kwh DESC;
```

`device_id` is the Carrier system serial. There is only one system today, so it never
splits a group — it is in the `GROUP BY` because rule 1 has no exceptions, and a replaced
outdoor unit means a new serial.

```sql
SELECT CAST(ts_local AS DATE) AS local_day,
       round(sum(value), 2)   AS hvac_kwh,
       count(*)               AS components_reported
FROM read_parquet('s3://my-energy-bucket/energy/daily/year=*/bryant-*.parquet')
WHERE metric = 'kwh_day'
GROUP BY 1
ORDER BY 1 DESC;
```

`components_reported` matters: a component whose `energyConfig.<name>.enabled` is false is
**structurally absent**, not zero — it emits no row at all. On this system the live
`energyConfig` (2026-08-17) disables exactly one, `looppump`; the other seven are enabled
and do report, several of them `0.0`, which is a **real measured zero** and a different
thing from absence. A day missing entirely means the 08:30 fetch did not land — not that
the HVAC used nothing.

**Do not add these day-grain rows to anything from `raw_30s` or `energy_hourly`.** They
are a different grain, they are excluded from the rollup by design, and mixing them
double-counts.

---

## The same queries in Athena

The tables (`energy_raw_30s`, `energy_hourly`, `energy_daily`, `dim_channel` in the
`energy` database) are created by `energycap create-glue-tables`, with partition
projection — so there is **no crawler, no `MSCK REPAIR TABLE`, and no partition to
register, ever**. Their table and column comments carry the same warnings this README
does; `SHOW CREATE TABLE energy.energy_hourly` is a good first thing for an LLM to read.

What changes from the DuckDB versions:

| DuckDB | Athena (Trino) |
|---|---|
| `read_parquet('s3://…/year=2026/month=08/day=15/*.parquet')` | `energy.energy_raw_30s` + `WHERE year = 2026 AND month = 8 AND day = 15` |
| partition columns typed with `hive_types` | already `int` in the catalog — **`WHERE month = 8`, never `month = '08'`** (the path is zero-padded, the column is not) |
| `time_bucket(INTERVAL 5 MINUTE, ts_utc)` | `date_trunc('hour', ts_utc) + (minute(ts_utc) / 5) * INTERVAL '5' MINUTE` — still **`ts_utc`**, never `ts_local` |
| `generate_series(a, b, INTERVAL 1 HOUR)` + `unnest` | `CROSS JOIN UNNEST(sequence(a, b, INTERVAL '1' HOUR))` |
| `ts AT TIME ZONE 'America/Kentucky/Louisville'` | `with_timezone(ts, 'UTC') AT TIME ZONE 'America/Kentucky/Louisville'` (Athena columns are plain `timestamp`) — needed to *label* a UTC bucket in local time; unnecessary for `ts_local` / `local_hour_start`, which are already local |
| `epoch(TIMESTAMP '…' AT TIME ZONE tz)` | `to_unixtime(with_timezone(TIMESTAMP '…', tz))` — how a local window's real elapsed seconds are measured |
| `x::DATE` | `CAST(x AS DATE)` |

Everything else — the joins, `coalesce`, `CASE`, `VALUES` decode tables, `round`,
timestamp literals — is identical.

**The bucket key stays `ts_utc` in Athena too.** Rewriting `time_bucket(…, ts_utc)` as a
`date_trunc` over `ts_local` swaps the canonical instant for a naive wall clock; on the
November fall-back Sunday the two 01:00–01:59 local hours then land in the same buckets,
so watts are averaged across two physically different hours and every sample count
doubles. Bucket on `ts_utc` and derive a local **label** from the bucket afterwards. The
three rules above the DuckDB queries — channel identity, the `POLL_INTERVAL_S` literal,
and real-elapsed-time sample expectations — apply here unchanged.

**0. Orientation**

```sql
SELECT source, device_id, channel_id, label, category, panel, slots, room,
       priority, estimated_watts
FROM energy.dim_channel
ORDER BY source, device_id, channel_id;
```

**1. Yesterday afternoon's heat pump draw**

```sql
SELECT
    r.source, r.device_id, r.channel_id,
    d.short_label,
    count(*)                                       AS samples,
    -- Real elapsed seconds of the requested LOCAL window / POLL_INTERVAL_S.
    CAST((to_unixtime(with_timezone(TIMESTAMP '2026-08-15 18:00:00',
                                    'America/Kentucky/Louisville'))
        - to_unixtime(with_timezone(TIMESTAMP '2026-08-15 12:00:00',
                                    'America/Kentucky/Louisville')))
         / 30 AS bigint)                           AS samples_expected,
    round(avg(r.value), 1)                         AS mean_watts,
    round(max(r.value), 1)                         AS peak_watts,
    round(avg(r.value) * count(*) * 30 / 3.6e6, 3) AS kwh_observed
FROM energy.energy_raw_30s AS r
JOIN energy.dim_channel AS d
  ON d.source = r.source AND d.device_id = r.device_id AND d.channel_id = r.channel_id
WHERE r.year = 2026 AND r.month = 8 AND r.day = 15     -- integers: prunes the scan
  AND r.metric = 'watts'
  AND d.category = 'hvac'
  AND r.ts_local >= TIMESTAMP '2026-08-15 12:00:00'
  AND r.ts_local <  TIMESTAMP '2026-08-15 18:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY kwh_observed DESC;
```

**2. Hourly kWh by label for a week, with `sample_count`**

```sql
SELECT
    h.local_hour_start,
    h.source, h.device_id, h.channel_id,
    coalesce(d.short_label, h.channel_id) AS channel,
    round(h.kwh, 4)  AS kwh,
    round(h.mean, 1) AS mean_watts,
    h.sample_count,
    -- One bucket is always exactly one real hour (it is keyed on hour_start_utc).
    round(100.0 * h.sample_count / (3600 / 30)) AS pct_of_hour_observed
FROM energy.energy_hourly AS h
LEFT JOIN energy.dim_channel AS d
       ON d.source = h.source AND d.device_id = h.device_id AND d.channel_id = h.channel_id
WHERE h.year = 2026 AND h.month = 8
  AND h.metric = 'watts'
  AND h.local_hour_start >= TIMESTAMP '2026-08-10 00:00:00'
  AND h.local_hour_start <  TIMESTAMP '2026-08-17 00:00:00'
ORDER BY h.local_hour_start, h.source, h.device_id, h.channel_id;
```

**3. Gaps — short hours, then entirely missing hours**

```sql
SELECT
    h.local_hour_start,
    h.source, h.device_id, h.channel_id,
    coalesce(d.short_label, h.channel_id) AS channel,
    h.sample_count,
    (3600 / 30) - h.sample_count AS samples_missing,   -- 30 = POLL_INTERVAL_S
    h.first_ts_utc,
    h.last_ts_utc,
    round(h.kwh, 4) AS kwh_of_observed_time
FROM energy.energy_hourly AS h
LEFT JOIN energy.dim_channel AS d
       ON d.source = h.source AND d.device_id = h.device_id AND d.channel_id = h.channel_id
WHERE h.year = 2026 AND h.month = 8
  AND h.metric = 'watts'
  AND h.sample_count < 118
ORDER BY h.sample_count, h.local_hour_start;
```

```sql
WITH observed AS (
    SELECT source, device_id, channel_id, hour_start_utc
    FROM energy.energy_hourly
    WHERE year = 2026 AND month = 8
      AND metric = 'watts'
      AND local_hour_start >= TIMESTAMP '2026-08-10 00:00:00'
      AND local_hour_start <  TIMESTAMP '2026-08-17 00:00:00'
),
bounds AS (SELECT min(hour_start_utc) AS lo, max(hour_start_utc) AS hi FROM observed),
spine AS (
    SELECT hour_start_utc
    FROM bounds
    CROSS JOIN UNNEST(sequence(lo, hi, INTERVAL '1' HOUR)) AS t (hour_start_utc)
),
channels AS (SELECT DISTINCT source, device_id, channel_id FROM observed)
SELECT s.hour_start_utc AS utc_hour_missing,
       c.source, c.device_id, c.channel_id,
       coalesce(d.short_label, c.channel_id) AS channel
FROM spine AS s
CROSS JOIN channels AS c
LEFT JOIN observed AS o
       ON o.hour_start_utc = s.hour_start_utc
      AND o.source = c.source AND o.device_id = c.device_id AND o.channel_id = c.channel_id
LEFT JOIN energy.dim_channel AS d
       ON d.source = c.source AND d.device_id = c.device_id AND d.channel_id = c.channel_id
WHERE o.hour_start_utc IS NULL
ORDER BY 1, 2, 3, 4;
```

**4. Bryant state vs Leviton watts on a common bucket**

```sql
WITH raw AS (
    -- The bucket key is ts_utc, the canonical instant. Bucketing ts_local here
    -- would merge the two 01:00 local hours of the November fall-back Sunday.
    SELECT *,
           date_trunc('hour', ts_utc) + (minute(ts_utc) / 5) * INTERVAL '5' MINUTE AS bucket
    FROM energy.energy_raw_30s
    WHERE year = 2026 AND month = 8 AND day = 15
      AND ts_local >= TIMESTAMP '2026-08-15 13:00:00'
      AND ts_local <  TIMESTAMP '2026-08-15 19:00:00'
),
hvac_instants AS (
    -- Sum ACROSS channels within one instant, then average across the bucket.
    SELECT r.bucket, r.ts_utc,
           sum(r.value) AS watts,
           count(*)     AS channels_reporting
    FROM raw AS r
    JOIN energy.dim_channel AS d
      ON d.source = r.source AND d.device_id = r.device_id AND d.channel_id = r.channel_id
    WHERE r.source = 'leviton' AND r.metric = 'watts' AND d.category = 'hvac'
    GROUP BY r.bucket, r.ts_utc
),
hvac AS (
    SELECT bucket,
           round(avg(watts))       AS hvac_watts,
           sum(channels_reporting) AS watt_samples,
           max(channels_reporting) AS hvac_channels
    FROM hvac_instants
    GROUP BY bucket
),
sys_state AS (
    -- Both renderings of odu.opstat, for the reason the DuckDB version gives:
    -- this outdoor unit is variable-capacity, so 'stage' rows do not exist and
    -- stage_pct is the compressor signal.
    SELECT bucket,
           max(CASE WHEN metric = 'mode'  THEN value END) AS mode_code,
           max(CASE WHEN metric = 'stage' THEN value END) AS stage_code,
           round(avg(CASE WHEN metric = 'stage_pct' THEN value END), 1) AS stage_pct,
           round(avg(CASE WHEN metric = 'outdoor_temp_f' THEN value END), 1) AS outdoor_f
    FROM raw WHERE source = 'bryant' AND channel_id = 'system'
    GROUP BY bucket
),
zone_state AS (
    SELECT bucket,
           round(avg(CASE WHEN metric = 'indoor_temp_f'   THEN value END), 1) AS indoor_f,
           round(avg(CASE WHEN metric = 'setpoint_cool_f' THEN value END), 1) AS setpoint_cool_f
    FROM raw WHERE source = 'bryant' AND channel_id = 'zone_1'
    GROUP BY bucket
)
SELECT s.bucket AS bucket_utc,
       -- a LABEL derived from the bucket, never the bucket key itself
       with_timezone(s.bucket, 'UTC') AT TIME ZONE 'America/Kentucky/Louisville'
           AS local_time,
       m.name AS mode,
       st.name AS stage,     -- NULL here: this outdoor unit is variable-capacity
       s.stage_pct,          -- 0-100 compressor capacity: what this house emits
       s.outdoor_f, z.indoor_f, z.setpoint_cool_f,
       h.hvac_watts, h.watt_samples, h.hvac_channels
FROM sys_state AS s
LEFT JOIN zone_state AS z ON z.bucket = s.bucket
LEFT JOIN hvac AS h ON h.bucket = s.bucket
LEFT JOIN (VALUES (0,'off'),(1,'heat'),(2,'cool'),(3,'auto'),(4,'fanonly'),
                  (5,'hpheat'),(6,'electric'),(7,'gasheat'),(8,'dehumidify'))
       AS m (code, name) ON m.code = CAST(s.mode_code AS integer)
LEFT JOIN (VALUES (0,'off'),(1,'low'),(2,'high'),(3,'idle'),(4,'dehumidify'))
       AS st (code, name) ON st.code = CAST(s.stage_code AS integer)
ORDER BY s.bucket;
```

If your engine rejects the interval multiplication in `bucket`, replace it with
`date_trunc('minute', ts_utc)` (one-minute buckets, two samples each at 30s polling) or
`date_trunc('hour', ts_utc)` — **still on `ts_utc`**. A coarser bucket is harmless; a
bucket keyed on `ts_local` is not, because it silently folds the fall-back Sunday's two
01:00 hours together — and losing five-minute resolution is a much smaller loss than
averaging two different hours into one number.

**5. Daily Bryant energy history**

```sql
SELECT
    CAST(e.ts_local AS DATE)              AS local_day,
    e.device_id,
    e.channel_id,
    coalesce(d.short_label, e.channel_id) AS component,
    round(max(CASE WHEN e.metric = 'kwh_day'      THEN e.value END), 2) AS kwh,
    round(max(CASE WHEN e.metric = 'cost_day_usd' THEN e.value END), 2) AS usd
FROM energy.energy_daily AS e
LEFT JOIN energy.dim_channel AS d
       ON d.source = e.source AND d.device_id = e.device_id AND d.channel_id = e.channel_id
WHERE e.year = 2026
  AND e.ts_local >= TIMESTAMP '2026-08-01 00:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, kwh DESC;
```

---

## Enum decodes: `mode`, `stage`, `fan`

Three metrics store a small integer in `value` with `unit = 'enum'` (the long schema has
no string column). These tables live in
[`src/energy_capture/sources/bryant.py`](src/energy_capture/sources/bryant.py) and are
quoted verbatim into the Glue comment on `value` (on `energy_raw_30s`) and into the
`energy_hourly` **table** comment, which has no `value` column but does aggregate enum
rows. Both quotations, and this table, are pinned to that source by tests.

| metric | `channel_id` | `value` → meaning |
|---|---|---|
| `mode` | `system` | `0` = off, `1` = heat, `2` = cool, `3` = auto, `4` = fanonly, `5` = hpheat, `6` = electric, `7` = gasheat, `8` = dehumidify |
| `stage` | `system` | `0` = off, `1` = low, `2` = high, `3` = idle, `4` = dehumidify — the **outdoor unit's** operating stage (`odu.opstat`). **Not emitted on this system** — see the next section |
| `fan` | `zone_{n}` | `0` = off, `1` = low, `2` = med, `3` = high |
| `op_status` | `system` | `0` = idle, `1` = cooling, `2` = heating, `3` = fanonly, `4` = defrost, `5` = dehumidify, `6` = off — the system's own one-word account (`oprstsmsg`) |
| `odu_mode` | `system` | `0` = off, `1` = cooling, `2` = heating, `3` = defrost, `4` = dehumidify, `5` = idle, `6` = cool, `7` = heat — the **outdoor** unit's `opmode`, distinct from its stage. Two spellings of cooling are live in the wild (`1` and `6`) and both are kept: the table is append-only |
| `idu_status` | `system` | `0` = off, `1` = on, `2` = low, `3` = high, `4` = idle — the **indoor** unit's `opstat` |

Note that `fan`'s `"auto"` is *not* an API value — it is a Home Assistant display label
that some clients substitute for `"off"`. It is deliberately absent here.

The rules, which are not negotiable:

- **These tables are append-only, forever.** Renumbering an existing entry silently
  rewrites the meaning of every row ever archived — years of history would change meaning
  with no diff in the data. A new API string is appended with the next unused integer; a
  retired number is never reused.
- **An API string that is not in the table logs a WARN and emits no row** — a gap. Never
  a fallback bucket, never an invented number, never `unknown = 99`.
- Because of that, an absent `mode`/`stage`/`fan` row can mean the API sent something we
  have never seen, not that the system was off. Check the logs for `bryant_enum_unknown`.
- `stage` is **permanently empty on this system** — settled by a live run, not a
  prediction. The next section is the whole story.

---

## Compressor stage: `stage` vs `stage_pct`

`odu.opstat` — what the *outdoor unit* is doing — is the single most useful HVAC signal
here: it is the one that correlates with watts. It arrives in **one of two shapes, decided
by the hardware, and a system only ever produces one of them**:

| the outdoor unit is… | `odu.opstat` reads | metric emitted | `unit` | the other metric |
|---|---|---|---|---|
| single-, two- or multi-stage | a word: `off`, `low`, `high`, `idle`, `dehumidify` | `stage` (integer code, decoded above) | `enum` | no `stage_pct` row, ever |
| **variable-capacity** (Greenspeed / inverter) | a number: `"0"`–`"100"`, the compressor's capacity percentage | `stage_pct` (the number as reported) | `pct` | **no `stage` row, ever** |

**This house is the second kind.** The first live call (`energycap discover`, 2026-08-17)
returned `odu.type = "gs3ngiphp"` with `odu.opstat = "35"` — a Greenspeed variable-capacity
heat pump reporting 35 % capacity. Every poll cycle since has emitted `stage_pct` and
**zero `stage` rows**, and that will not change unless the outdoor unit is replaced.

So, concretely:

```sql
-- Returns NOTHING on this system. Not "the compressor was off" — nothing.
WHERE source = 'bryant' AND metric = 'stage'

-- This is the compressor signal here.
WHERE source = 'bryant' AND metric = 'stage_pct'    -- value is 0-100, unit 'pct'
```

That is the [cardinal rule](#reading-this-data-honestly) at its most dangerous: an empty
result is *absence*, and absence is never zero. If you do not know which rendering a
system uses, ask it — `energycap discover` prints `odu_type`, `odu_opstat` and
`stage_metric` per system, without waiting for a poll cycle — or select both metrics and
let the one that comes back NULL tell you.

Both paths stay live forever, so a replaced or reflashed outdoor unit can switch a system
from one to the other mid-archive. The two never collide: they are different metric names,
so they never share a dedupe key `(ts_utc, source, device_id, channel_id, metric)` and
never average together. The source logs `bryant_stage_representation` at INFO the first
time it sees a rendering and again on any change, and `status.json` carries
`stage_representation`, `stage_pct_rows` and `stage_enum_rows`.

Two things `stage_pct` is **not**:

- **Not an enum.** `unit = 'pct'`, so `avg()` over it is meaningful — mean compressor
  capacity for the bucket, the natural partner for mean `watts`. The
  "never `avg()` an enum" rule covers `mode`/`stage`/`fan` only.
- **Not clamped or rounded.** The value is written exactly as reported (`"35"` → `35.0`).
  A number outside 0–100 is not a percentage, so it emits **no row** and WARNs
  (`bryant_stage_pct_out_of_range`) rather than being clamped: a clamped `100` would be
  indistinguishable from an observed `100` once archived. `0` is a real reading — the
  compressor idling at 0 % — not a missing one.

Background: PLAN.md §7.3 and DEVIATIONS.md #59 (which predicted this exact outcome and
pre-authorised the `stage_pct` metric) and #75.1 (the live question it answered).

---

## Reading this data honestly

This is the section to read before turning a number into a sentence.

**A low `kwh` means low observed energy, not a quiet appliance.** `kwh` covers only the
time we actually watched: `mean_watts × sample_count × poll_interval_s / 3.6e6`. An hour
with 60 of 120 samples reports half the energy of a complete hour at the same wattage.
Always look at `sample_count` beside it. `sample_count ≈ 118–120` is a full hour at 30s;
anything materially lower is a gap, and the honest phrasing is "over the 30 minutes we
observed, it drew X."

**An absent row means "not observed".** It never means zero. A row can be absent because
the collector was down, the container was restarting, the cloud API was failing, a field
came back null, an enum string was unrecognised, or the channel is not wired at all.
Query 3 above distinguishes the two shapes of gap. When summing across a range, check
whether the hours you expected are actually present before reporting a total.

**A metric that this hardware never emits also returns nothing.** The sharpest case is
`stage` versus `stage_pct`: they are two renderings of one field and this outdoor unit is
variable-capacity, so `WHERE metric = 'stage'` matches **zero rows for all time** while
`stage_pct` carries the compressor signal. An empty result set is a statement about the
query, not about the compressor — check
[Compressor stage](#compressor-stage-stage-vs-stage_pct) before reporting that the heat
pump never ran.

**A recorded `0.0` is a real, different thing.** Leviton firmware v2 emits genuine
spurious zero power readings, and they are archived verbatim, unfiltered — recording what
the API said is a cardinal rule, and filtering is a query-time choice, not a collection-time
one. So `min = 0` in an hourly row may be a firmware artifact rather than an idle circuit.
A `0.0` in `energy_daily` for an *enabled* component is a genuine measured zero; a
component that does not exist on this house emits no row at all.

**A Leviton row does not say how fresh its value was.** Every row is one 30-second
sample stamped with the instant the cycle completed, and that has not changed — but the
*value* in it came either from the live push store or, when the socket was unusable and
the mode is `hybrid`, from a REST read that Leviton is known to serve from a server-side
cache. The row cannot tell you which. `status.json`'s `leviton_ingest.value_source`, its
per-source cycle counters, and the INFO line logged at every transition can; see
[How Leviton values are kept fresh](#how-leviton-values-are-kept-fresh). What the row
*never* is, in any mode, is a value held over from a connection we had lost — while the
socket is down, unsynced or silently stalled the cycle emits nothing at all, so a
`sample_count` dip in those hours is the honest record of a reconnection, not a
measurement of a quiet circuit. This distinction matters most for short, sharp events: a
cached stretch can miss a two-minute compressor start entirely and show a flat line
across it.

**`ts_local` is ambiguous for one hour every November.** It is a naive wall clock with no
offset attached, kept for readability. On the fall-back Sunday, 01:00–02:00 local happens
twice and both are labelled `01:xx`. `ts_utc` is canonical and unambiguous: sort, bucket,
dedupe and join on it. In the hourly rollup that day has 25 buckets per series, two of
which share `local_hour_start` and differ in `hour_start_utc`. The spring-forward day has
23. If you group a fall-back day by `local_hour_start` you will merge two real hours and
silently lose one; group by `hour_start_utc` and label with `local_hour_start`. The same
rule governs raw 30s data: bucket on `ts_utc` (5-minute, 15-minute, whatever) and derive
the local time from the bucket. A bucket key built out of `ts_local` merges the two 01:00
hours, averages watts across them, and doubles every sample count in them.

**Nothing is ever interpolated.** There is no gap filling anywhere in the pipeline: no
zero-fill, no carry-forward, no hour spine, no `COALESCE` in the rollup. If you need a
continuous series for a chart, build the spine **in your query** (as query 3 does) and
keep the fabricated points visibly distinct from the observed ones.

**`estimated_watts` in `dim_channel` is a planning estimate from the panel inventory, not
a measurement.** It can legitimately be 0 or null where nobody ever measured the circuit.
For what actually happened, use `energy_hourly`.

**A channel is `(source, device_id, channel_id)`.** `channel_id` is unique only within a
device, and this house has two Leviton hubs, so `panel_leg_a`, `panel_leg_b`, breaker
positions and CT channel numbers all repeat across the two panels. Grouping or labelling
on `channel_id` alone — or on `coalesce(short_label, channel_id)`, which falls back to the
bare `channel_id` for every unmapped channel — adds two physically different circuits
together and reports the sum as one. The tell is a `sample_count` that exceeds what the
window can physically hold: 5760 samples in a 2880-sample day means two channels, not a
long day.

**Sample expectations follow real elapsed time.** An hourly rollup bucket is always
exactly one real hour, so `3600 / 30` is right per hour even on a DST Sunday. A local
*day* is 23 hours in March and 25 in November, so the expected daily sample count is 2760
or 3000, not 2880. Derive it from the local day's UTC extent rather than hardcoding 24.
The literal `30` in all of this is `POLL_INTERVAL_S`; if that setting ever changes, every
expectation and every kWh literal in a query changes with it, and a range that spans the
change has no single right answer.

**`dim_channel` coverage is not guaranteed.** A live channel nobody has mapped yet is
simply missing from it, so a `JOIN` will silently drop real measurements. Use `LEFT JOIN`
plus `coalesce(d.short_label, r.channel_id)` unless you specifically want mapped channels
only (as query 1 does, deliberately, to select the HVAC category). That `coalesce` is a
**display label, not an identity** — keep `(source, device_id, channel_id)` in the
`GROUP BY` and in the output beside it, or two unmapped channels sharing a `channel_id`
across the two panels will be added together under one name.

**Day-grain and 30s rows never mix.** `kwh_day` / `cost_day_usd` live only in
`energy_daily`, are barred from `raw_30s`, and are excluded from the rollup input. Do not
sum them with hourly `kwh`; they are different measurements of overlapping things, from
different sources, and adding them double-counts.

**Backfilled history is written exactly as it was recorded, zeros included.** For
historical days we cannot know which components were structurally disabled at the time, so
a zero in old `energy_daily` rows may mean "disabled" rather than "measured zero".

---

## Settled by the first live run (2026-08-17)

`energycap discover` and `energycap poll --once` have now run against the real Leviton and
Carrier clouds. What that settled, so nobody re-litigates it from the list below:

- **`odu.opstat` is numeric on this system, and `stage` will never be emitted here.**
  `odu.type = "gs3ngiphp"` with `odu.opstat = "35"`: a Greenspeed **variable-capacity**
  heat pump reporting a 35 % capacity percentage, not one of the stage words. The
  compressor signal is therefore the `stage_pct` metric (`unit = 'pct'`), and `stage` is
  permanently absent — see
  [Compressor stage](#compressor-stage-stage-vs-stage_pct). This was DEVIATIONS.md #75.1,
  the highest-risk open question in the Bryant work; #59 pre-authorised exactly this
  metric for exactly this outcome.
- **`infinityStatus(serial:)` resolves**, so the cheap per-serial query is the one in use
  (`status.json` reports `operation: getInfinityStatus`); the fallback was never needed.
- **`cfgem = "F"`**, so `outdoor_temp_f` is emitted and no Celsius conversion is in play.
- **One zone is enabled** of the eight the payload reports, so `zone_1` is the only zone
  channel that produces rows.
- **The Leviton hub ids are real** in `config/channel_map.json`, pasted from that run;
  the only placeholder left there is the future LG&E meter (PLAN.md §13).
- **`getInfinityEnergy` still resolves** with the field set this pipeline asks for:
  `energyPeriods` values arrive as JSON numbers and `energyConfig.<name>.enabled` as a
  JSON boolean. Only `looppump` is disabled on this system — `gas`, `fangas` and `reheat`
  are *enabled* and report `0`, so expect seven components a day, not four.

## Known-unproven

Everything below is a real gap in confidence, not a formality. The test suite is green and
entirely offline; beyond the first live poll above, most of the *world* is still untouched.

1. **Nothing downstream of the spool has ever touched AWS**, and no stage has run in
   anger: the first live exercise was `discover` + `poll --once` into the local SQLite
   spool. `upload`, `compact-daily`, `rollup`, `build-dim`, `create-glue-tables` and
   `backfill` have never been run against the real bucket, so PLAN.md §16's "full manual
   cycle" is still outstanding. The suite itself remains fixture-driven and offline —
   `tests/conftest.py` installs an autouse guard that refuses any non-loopback socket, so
   no test has ever seen a live response.
2. **The image is built and runs under Apple `container`; Docker is now the untested
   path.** On 2026-08-17 the image was built with `container build` (1.2.2, macOS 27,
   arm64) and run via `./scripts/energycap-container.sh run`: 12 poll cycles, both
   sources, zero errors, `/healthz` answering 200 on the published port, the WebSocket
   connecting from inside the container, rows landing in the host's `data/spool.db`
   through the bind mount, and a clean SIGTERM drain. The build-time steps that bake in
   DuckDB's `httpfs` and assert `America/Kentucky/Louisville` both passed in-image, and
   `.dockerignore` is honoured (no `.env` in the image). The uid-over-virtiofs question —
   previously flagged as the most likely thing to break — is **answered**: virtiofs does
   not enforce guest ownership, so uid 10001 writes a host-owned mount unchanged.
   What is still unproven here: **`docker build` has never run** (no daemon on this
   machine), and **the LaunchAgent has never been loaded**, so KeepAlive, the throttle
   and reboot survival — the entire supervision story that replaces compose's
   `restart:` — remain untested. Details and the re-check triggers are in
   [`deploy/README.md`](deploy/README.md).
3. **The Carrier status field map still has UNVERIFIED entries.** Fields observed in a
   real captured response are distinguished in the source from fields that merely exist in
   the introspected schema (`damperposition`, `occupancy`, `zones[].name`, parts of the
   `odu` compressor telemetry, and others). `odu.opstat` — the one that used to head this
   item — is now settled and answered above. The units behind `statpress`, `blwrpm` and
   `oat` are still taken from a reference client's code comments rather than
   documentation (DEVIATIONS.md #60, #75.10). The real domain of `status.mode` also needs
   a season: only a few of its words have ever been observed, so expect
   `bryant_enum_unknown` and **append, never renumber**.
4. **Whether the Carrier cloud tolerates 30-second polling is unknown.** Nothing in the
   ecosystem polls it faster than every 30 *minutes*, and neither reference client handles
   429 at all. The pipeline honours `Retry-After` and backs off, and the interval is
   configurable (`BRYANT_POLL_INTERVAL_S`); watch the throttle counters for the first 24
   hours and raise it if needed. Related: it is not yet known whether the payload even
   *changes* every 30s — the golden capture suggests server-side caching.
5. **`config/channel_map.json` is real but not complete.** The hub ids, CT channel
   numbers and breaker positions came from the 2026-08-17 `discover` run, so the Leviton
   joins in the queries above do resolve — but coverage is a hand-maintained list, a
   newly installed breaker is unmapped until someone re-runs `discover`, and
   `dim_channel` is only ever as good as that file. `LEFT JOIN`, always.
6. **Athena/Glue has never been exercised against real AWS.** The table definitions, the
   partition projection templates and the comments are all unit-tested offline (against an
   in-process Glue stand-in, because `moto`'s Glue backend is unusable in this
   environment). Every DuckDB query above is extracted and **executed by the test suite**
   ([`tests/test_docs.py`](tests/test_docs.py)) against local Parquet written by the
   pipeline's own writers, over a corpus that deliberately contains two hubs sharing
   `channel_id` values and both DST transition days — so the answers above are real
   answers, not plausible-looking SQL. **The Athena translations are desk-checked, not
   executed**: nothing here can reach Athena, so their syntax and their `with_timezone` /
   `to_unixtime` idioms are the part of the query surface still taken on faith. What *is*
   enforced for them is the part that was wrong before — a test fails if any example, in
   either dialect, cuts a bucket key out of the naive `ts_local` wall clock or hardcodes a
   24-hour day.
7. **The Leviton WebSocket connects and helps, but only partly.** Connected on the first
   attempt on 2026-08-17 (natively and from inside the container): handshake accepted, 7
   objects subscribed, ~0.35 messages/sec, both hubs chattering, and the fallback ladder
   behaved as designed (first cycle `rest_fallback` / `awaiting_initial_sync`, then
   `value_source: ws`). One channel went from frozen to genuinely live — Panel A's grid
   CT produced **11 distinct values across 11 polls** where REST had returned one value
   for 46 consecutive reads, at finer precision than REST reports.
   **But 10 of 12 channels were still frozen**, with `sync_mode: timeout` and
   `awaiting_sync` never reaching 0 — i.e. the bandwidth-1 flood does not establish every
   subscribed object. Two consequences worth knowing before trusting the data: rows for
   those channels are labelled `value_source: ws` while actually carrying REST-seeded
   values, because the seed legitimately establishes state (`fields_evicted: 0`), so the
   row label is coarser than reality — only `leviton_ws.objects` per-field ages can tell
   them apart; and the next thing to try is the reference client's third keepalive,
   `GET /apiversion` every 10s (DEVIATIONS.md #155), which is documented but not shipped.
   Still unproven: the 55-minute proactive reconnect (runs so far have been minutes, not
   hours), what close code the 60-minute hard kill delivers, whether explicit nulls ever
   arrive in a delta, and the real message volume at ~40 channels. `hybrid` remains the
   default precisely because its worst case is exactly the old behaviour. Read
   `leviton_ws.objects`, `leviton_ws.sync_mode`, `leviton_ws.hub_silence_s` (**both** hubs
   must be moving) and `leviton_ingest.last_reconcile_drift` before publishing any
   freshness claim, and see [`DEVIATIONS.md`](DEVIATIONS.md) #144 and its status section.
8. Other open questions the rest of the first live run should settle — Okta token
   lifetimes and whether the refresh token rotates, whether the spoofed
   `Origin`/`Referer` headers are load-bearing, and the contents of the legacy DynamoDB
   table (run the first `backfill` with `--dry-run`) — are enumerated in
   [`DEVIATIONS.md`](DEVIATIONS.md) #75.

---

## License

MIT — see [LICENSE](LICENSE).
