# Where this project is — handoff, updated 2026-08-19

A snapshot for picking the work back up. `PLAN.md` is still the spec of record and
`DEVIATIONS.md` (163+ entries) is still the record of every departure from it; this file
just says what is done, what is proven, and what is next.

**Branch:** merged to `main` via [PR #1](https://github.com/ericpullen/energyDataCapture/pull/1)
on 2026-08-18. `energycap-implementation` is merged and kept, not deleted.
**Tests:** 1595 passing, 0 skipped, entirely offline (an autouse guard in
`tests/conftest.py` refuses any non-loopback socket).
**Public site:** <https://energycap.ericpullen.com/> is live — see `docs/lge-greenbutton.md` §3a.

---

## What exists

All seven build-order steps of `PLAN.md` §16, plus two things the spec did not ask for:

| | |
|---|---|
| Collector | 30s pollers (Leviton + Bryant) → SQLite spool → hourly Parquet parts → daily compaction → DuckDB hourly rollup |
| Sources | Leviton LWHEM-2 (REST + **WebSocket**), Bryant/Carrier (status + daily energy), **LG&E Green Button Connect** (OAuth2/ESPI meter intervals), legacy DynamoDB/JSON backfill |
| Semantic layer | `config/channel_map.json` joined to the blackstart inventory → `dim_channel` |
| Query surfaces | Glue tables with partition projection and real comments; README with executable DuckDB examples |
| **Dashboard** (not in PLAN) | `GET /ui` — live values, sparklines, HVAC, hourly kWh math, 24h scrollback |
| **Apple `container`** (not in PLAN) | `scripts/energycap-container.sh` + launchd, alongside Docker |
| **`compare-meter`** (not in PLAN) | the utility meter against the summed feed CTs, hour by hour, with sample coverage |
| **Public site** (not in PLAN) | `site/` → <https://energycap.ericpullen.com/>, the six URIs Green Button registration requires |

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
   README is desk-checked rather than executed. The EC2 move forces this one.
   (LG&E is the exception: `fetch-greenbutton` and `compare-meter` are local-only by design
   and have both run against real data.)
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

## LG&E Green Button — DONE

Registered, approved, authorised and fetching, all on 2026-08-18. `docs/lge-greenbutton.md` is
the full record (registration, the live credential probe, and what the real API turned out to
do); `DEVIATIONS.md` #166–#170 is the reasoning.

| | |
|---|---|
| `energycap greenbutton-authorize` | one browser round trip; tokens at `{SPOOL_DIR}/tokens/lge.json`, mode 600 |
| `energycap fetch-greenbutton` | the Connect API → `energy/meter`, scheduled daily 09:15 local |
| `energycap import-greenbutton` | a downloaded file → the **same** parser and writer |
| `energycap compare-meter` | meter vs. summed feed CTs, hour by hour, with coverage |
| `/ui` meter card | house, barn, freshness, and panels-vs-meter for the last complete day |

**Three things the live API did that no document predicted** (#169). `published-min` wants
**ISO-8601 with a `Z`**, not the spec's epoch seconds — and camelCase `publishedMin` returns
**200 while ignoring the filter**, handing back 49 MB instead of 415 KB. Every UsagePoint
publishes the same energy as **both a 900s and a 3600s series**, which the canonical dedupe key
silently collapsed until `model.METER_DEDUPE_KEY` gained `interval_s`; nothing may ever sum two
interval series. And Connect exposes a meter the download does not.

**Two meters, and they must never be summed.** `1308468` is the house (74–99 kWh/day);
`1326254` is the **barn** — a separate service that is ~100% EV charging (Ford Charge Station
Pro + Tesla Universal), ~150 W baseline with one large load late afternoon into evening peaking
at 14.7 kW, 3.6–40 kWh/day. `channel_map.json` marks the house `primary: true`, which is how
`compare-meter` and the meter card know which to compare; without it they refuse rather than
guess. The download also republishes the house under two retired ids (944006, 944401) which are
mapped and collapsed on sight.

**Result: the feed CTs read ~3.4% high against the meter** over fully covered hours. That is
within the combined tolerance of the clamps and the meter, and it is the first evidence the
sub-metering is trustworthy.

---

## The spool was corrupted a second time — read this before touching it

**2026-08-18: I corrupted the spool by running `dashboard.build_snapshot(spool_path=…)` from
the host** while the container was writing. Same signature as 2026-08-17
(`Tree 2 page 2: btreeInitPage() returns error code 11`). The rule in "Operating it" above is
not about the `sqlite3` shell — **any SQLite open from the host counts**, including a read-only
one, because it still creates and mutates the `-shm` file. Read-only is not an exemption.

Fully recovered, and the procedure works:

```bash
./scripts/energycap-container.sh stop                     # clean, checkpoints the WAL
sqlite3 spool.db ".recover" | sqlite3 spool.recovered.db  # rows land in lost_and_found
# lost_and_found: `id` is the rowid, c1..c11 are ts_utc … uploaded_at
# then INSERT them into a database created by open_spool(), so the real schema and
# indexes are rebuilt; verify integrity_check, row count and span before swapping in.
```

94,637 of ~94,670 rows recovered — about one 30-second cycle lost. The corrupt file is kept
as `data/spool.db.corrupt-<timestamp>` (gitignored).

---

## Next up: move the collector to EC2

Agreed 2026-08-18, **after the new breakers are installed and reporting**. The analysis behind
it is worth not re-deriving:

**Redundant collectors were considered and rejected.** Running two collectors and merging in S3
looks attractive, and most of the design supports it — every stage is idempotent on the dedupe
key. But `new_cycle(ts_utc=now_utc())` stamps each cycle with *that collector's* clock, so two
collectors produce different `ts_utc` for the same reading, the dedupe key does not collapse
them, `sample_count` doubles and **every kWh figure doubles, silently**. Fixing that needs
either timestamp quantisation to the poll grid (which redefines `ts_utc`) or hour-granularity
failover with a collector id (never stitch *within* an hour — that is where `sample_count` goes
wrong). Part filenames are deterministic, so they would also collide until the key carries a
collector id.

**And it would not buy much.** Both collectors poll the same two clouds, so a Leviton or
Carrier outage takes out all of them; redundancy only covers local failure. Meanwhile it
doubles load on the fragile part — two poll loops, two WebSockets, two keepalive loops sending
`bandwidth: 1` to the same hub every 50s — and nothing else in the ecosystem polls Carrier
faster than every 30 minutes.

**So: one collector, somewhere reliable.** It fits well because this is a *cloud-to-cloud*
poller — Leviton and Carrier are internet APIs, so the collector has no reason to be on the
home LAN. Practicalities already established:

- The image is **arm64**, so a **t4g.nano/micro** (Graviton) runs it unchanged, ~$3–8/month.
- An **instance role instead of AWS keys on disk** — a real improvement over the Mac.
- Storage is trivial: the spool grows ~35 MB/day at 7-day retention, so 8 GB is plenty.
- Two loose ends: the blackstart inventory is a local file
  (`~/code/blackstart/data/montfort.json`) that must be copied or baked in, and the dashboard
  needs a reachability plan (Tailscale, or a security group locked to one IP).
- The spool already absorbs S3/network outages — it purges only rows that are both uploaded
  **and** aged — so the only gap redundancy would have covered is the process being down.

**Cheaper experiment worth doing first:** the launchd `KeepAlive` path has still never been
loaded or tested. If the real enemy is the host rather than home internet, that fixes it for
nothing.

---

## Also open

- **Nothing has ever touched AWS.** This is now the biggest gap, and the EC2 move forces it:
  `upload`, `compact-daily`, `rollup`, `build-dim`, `create-glue-tables` and `backfill` have
  never run against a real bucket. `PLAN.md` §16's "full manual cycle" is outstanding.
- **New Leviton breakers** arriving ~2026-08-21. `energycap discover` prints a
  `channel_map.json` skeleton for anything unmapped; `channel_id` is `breaker_p{position}`,
  never the API's breaker id. Priority circuits, from the first real load analysis: **A-1-3
  (dryer)** — a noon spike of ~5.4 kW cycling on a thermostat was identified as the dryer purely
  from the feed legs, because it was balanced across both — then **A-10-12** (kitchen
  counter/dishwasher MWBC), which straddles both legs and will otherwise keep masquerading as a
  240 V load.
- The chart shows at most three series, so a derived "Panel A total (A+B)" series was offered
  and not yet built. Leg-level series read as though a 2-pole load exceeds its own panel feed.
