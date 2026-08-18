# Where this project is — handoff, 2026-08-18

A snapshot for picking the work back up. `PLAN.md` is still the spec of record and
`DEVIATIONS.md` (163+ entries) is still the record of every departure from it; this file
just says what is done, what is proven, and what is next.

**Branch:** merged to `main` via [PR #1](https://github.com/ericpullen/energyDataCapture/pull/1)
on 2026-08-18. `energycap-implementation` is merged and kept, not deleted.
**Tests:** 1478 passing, 0 skipped, entirely offline (an autouse guard in
`tests/conftest.py` refuses any non-loopback socket).
**Public site:** <https://energycap.ericpullen.com/> is live — see `docs/lge-greenbutton.md` §3a.

---

## What exists

All seven build-order steps of `PLAN.md` §16, plus two things the spec did not ask for:

| | |
|---|---|
| Collector | 30s pollers (Leviton + Bryant) → SQLite spool → hourly Parquet parts → daily compaction → DuckDB hourly rollup |
| Sources | Leviton LWHEM-2 (REST + **WebSocket**), Bryant/Carrier (status + daily energy), legacy DynamoDB/JSON backfill |
| Semantic layer | `config/channel_map.json` joined to the blackstart inventory → `dim_channel` |
| Query surfaces | Glue tables with partition projection and real comments; README with executable DuckDB examples |
| **Dashboard** (not in PLAN) | `GET /ui` — live values, sparklines, HVAC, hourly kWh math, 24h scrollback |
| **Apple `container`** (not in PLAN) | `scripts/energycap-container.sh` + launchd, alongside Docker |

## What has actually been proven against reality

A ~19-hour continuous run in the container (2026-08-17 19:22Z → 2026-08-18 14:31Z,
22.5k rows) plus earlier native runs:

- **Both clouds authenticate**, tokens cache and are reused. 1,716 WS cycles vs 11 REST
  fallbacks — 99.4% of samples came from the WebSocket.
- **The WebSocket fixed the staleness.** This was the whole reason it was built, and it
  worked: `awaiting_sync` is now **0** (it was 2–4 initially), and all eight watt channels
  show distinct hourly means — Water heater cycling 0 → 4963 W, Panel A feed 548 → 4719 W,
  Panel B feed 0 → 4468 W. Before the WebSocket, a whole-panel feed held **exactly 4086.05 W
  for 46 consecutive REST reads**.
- **15 reconnects, 0 stalls** — the 55-minute proactive reconnect works unattended.
- **REST reconcile drift** is down to 4 differing of 24 compared (was 8).
- The container survived overnight: hourly upload and rollup jobs failed cleanly all night
  with `S3_BUCKET is not configured`, logged as `job_failed`, contained, polling never
  interrupted.
- `energycap discover` settled several §7.3 unknowns: `odu.opstat` is **numeric** (46 →
  variable-capacity, so `stage_pct` not `stage`), `infinityStatus(serial:)` resolves,
  `cfgem` is `F`, 1 of 8 zones enabled.
- The strip-heat CT (Panel B `ct_2`) reads **1.5–11.7 W**, not a flat 0 — so the clamps are
  live; the near-zero is real for cooling season, consistent with them being on the HVAC
  subpanel feeder (blackstart `B-6-8`) rather than the compressor.

## What is still NOT proven

1. **Nothing has ever touched AWS.** No `S3_BUCKET` is set. `upload`, `compact-daily`,
   `rollup`, `build-dim`, `create-glue-tables` and `backfill` have never run against a real
   bucket, so `PLAN.md` §16's "full manual cycle" is outstanding, and the Athena side of the
   README is desk-checked rather than executed.
2. **The LaunchAgent has never been loaded** — KeepAlive, ThrottleInterval and reboot
   survival are untested. The container has only ever been started by hand.
3. **`docker build` has never run** (no daemon on this machine). Docker is now the untested
   path; Apple `container` is the proven one.
4. `sync_mode` is still `timeout` rather than `flood`. Everything works, but the bandwidth-1
   flood is not what is establishing the subscription set — `GET /apiversion` every 10s
   (DEVIATIONS #155) is the untried next lever if it ever matters.
5. Carrier field units (`statpress`, `blwrpm`, `oat`) still come from a reference client's
   comments, and `status.mode`'s full domain needs a heating season.

## Operating it

```bash
export PATH="$HOME/.local/bin:$PATH"

# native
uv run energycap run                      # Ctrl-C drains cleanly
# container (preferred on this Mac; currently running)
./scripts/energycap-container.sh run      # foreground; `stop` / `logs` / `status` too

open http://localhost:8080/ui             # dashboard
curl -s localhost:8080/healthz | jq       # health
```

### The one operational trap

**Never open `data/spool.db` from the host while the container is running.** A host reader
plus the container writer on one SQLite WAL database across the virtiofs boundary
**corrupted the database** on 2026-08-17 (`integrity_check` reported `btreeInitPage` errors
across ten pages). A container-only run over the same bind mount is fine — re-tested clean.
The dashboard exists partly so nobody needs host access to the spool; it runs inside the
process that owns the database. If you must query it, stop the container first, or copy all
three `spool.db*` files and query the copy.

Also: the WAL is where the data lives. `spool.db` alone is ~4 KB; copying it without
`-wal` and `-shm` gets you an empty database.

Spool grows ~35 MB/day and nothing purges without S3 (purge requires uploaded **and** aged),
so no data is at risk, but watch the disk on a long capture.

---

## Next up: LG&E Green Button Connect

The goal is automated meter data from LG&E's **MyMeter** site via **Green Button Connect My
Data** — the OAuth'd ESPI API — rather than the manual "Download My Data" export.

`PLAN.md` §13 designed for this and deliberately stopped short of building it, so the
groundwork is already in place and should not be redesigned:

- `source='lge'` is already in the source vocabulary (`model.SOURCES`), not a two-source enum.
- `model.METER_SCHEMA` already exists: the canonical schema **plus `interval_s` (int32)**,
  because meter data is *interval* data — `ts_utc` is the interval START, not an instant.
  `MeterObservation` is its row type. This variant was built deliberately non-hacky.
- `aws/s3io.py` already has `meter_key` → `energy/meter/year=YYYY/{source}-{YYYYMM}.parquet`
  (filename convention invented in DEVIATIONS #3 — change it there if the real data wants
  something else).
- `config/channel_map.json` carries a **placeholder** `lge` / `electric_main` entry proving
  `dim_channel` holds an lge channel with no code change. A `gas_main` joins it if the gas
  meter is exported too.
- `energycap import-greenbutton` exists as a CLI command that exits 3 with "deliberately
  deferred per PLAN.md §13".

What §13 specifies for the mapping, once real data is in hand: ESPI `UsagePoint` →
`device_id` (meter id); `MeterReading/ReadingType` → `metric` + `unit` (`kwh_interval`/`kWh`,
`ccf_interval`/`CCF`); `IntervalBlock/IntervalReading` → rows, with the ESPI
`powerOfTenMultiplier` applied. Glue table `energy_meter` with the same partition-projection
treatment. Idempotent on the standard dedupe key.

**Researched 2026-08-18 — see `docs/lge-greenbutton.md`**, which answers §13's "verify
Connect availability when building" and drafts the registration form field by field. In short:

- **Connect exists** and is real OAuth2/ESPI. §13's "assume manual import first" hedge is
  resolved in Connect's favour.
- **Registration is a one-shot, human-reviewed form** on the MyMeter site — one per vendor,
  ever, covering all customers. There is **no developer portal and no published API base
  URI**: the OAuth endpoints and the 3PV credentials arrive in the approval email. So no
  client code should be written until it lands.
- Granularity is **900 or 3600 seconds only**; a **daily subscription** is available, so the
  fetch cadence can mirror the Bryant daily-energy stage.
- **Connect is electric-only.** §13 assumed gas would come along; it does not, so
  `import-greenbutton` becomes the permanent gas and bulk-history path rather than a stopgap
  (DEVIATIONS #166).

**The registration is ready to submit.** `docs/lge-greenbutton.md` §3 is the completed
application; the public site every URI points at is live and verified. The only field left is
the phone number, which is deliberately not committed to a public repo.

Two things outside the code still gate real data:

1. **Submitting the form**, on the MyMeter site, and waiting for a human at LG&E to approve it.
   The approval email is what carries the OAuth endpoints, so **no client code should be written
   before it arrives.**
2. **The MyMeter *local* account** — needs a registration code requested by email from
   `MyMeter@lge-ku.com`, and its address **cannot match the My Account primary email**. It is
   the long-latency item and it is independent of vendor approval, so it is worth starting
   first.

Still unknown, and to be asked in the approval correspondence: the endpoints and any sandbox,
the maximum accepted `HistoryLength` (the draft guesses 730 days), token lifetimes and
re-consent schedule, publication lag and whether readings get revised, and whether raw and
VEE readings arrive as separate `MeterReading`s — if so they need distinct `metric` values
rather than colliding on the dedupe key.

Worth knowing that the old collector's frontend already parsed LG&E exports client-side:
`~/code/bryantDataCollector/frontend/index.html` handles an uploaded `Usage.csv` **and**
Green Button ESPI XML. That parsing is a useful reference for the real shape of LG&E's
export, even though the new pipeline will not reuse the code.
