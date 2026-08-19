# Where this project is — handoff, updated 2026-08-19 (afternoon)

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
| **Lightsail host** (not in PLAN) | `deploy/lightsail.md` + `deploy/lightsail-userdata.sh` — the collector's cloud home, $7/mo |
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
   README is desk-checked rather than executed. Splitting the poller from the batch stages
   forces this one, since S3 becomes the handoff between the two hosts.
   (LG&E is the exception: `fetch-greenbutton` and `compare-meter` are local-only by design
   and have both run against real data.)
2. **The LaunchAgent has never been loaded** — KeepAlive, ThrottleInterval and reboot
   survival are untested. The container has only ever been started by hand.
3. ~~`docker build` has never run~~ — **resolved 2026-08-19**: it built clean on the
   Lightsail host, first try, unmodified, on x86_64. Both runtimes now work.
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
# container (Apple `container` on this Mac; currently running)
./scripts/energycap-container.sh run      # foreground; `stop` / `logs` / `status` too

open http://localhost:8080/ui             # dashboard
curl -s localhost:8080/healthz | jq       # health

# the Lightsail host — see deploy/lightsail.md
ssh -i ~/.ssh/energycap-lightsail.pem ubuntu@13.219.164.226
open http://13.219.164.226:8080/ui        # firewalled to the house IP; no auth on it
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

## The collector now runs on AWS Lightsail — phase 1 done

Deployed 2026-08-19. `deploy/lightsail.md` is the full record; the short version:
`energycap`, `us-east-1a`, Ubuntu 24.04, `micro_3_0` (1 GB / 2 vCPU / 40 GB SSD / 2 TB
transfer) at **$7/month all in**, static IP `13.219.164.226`, with SSH and 8080 both open
only to the house IP. It polls and spools exactly as the Mac does; **nothing touches S3
yet**, so
`upload_hourly`, `rollup_hourly` and `daily_maintenance` fail cleanly as `job_failed`
exactly as they did overnight at home.

**Lightsail over EC2** because it is ~$4.40/month cheaper at the same RAM once EC2's
public-IPv4 charge (~$3.65/mo) and EBS are counted, and it bundles a bigger disk. The
cost is that Lightsail takes no IAM instance profile, so the S3 phase needs a scoped
access key on the box rather than a role.

**Two things got proven that had never run:**

1. **`docker build` finally executed — and passed first try**, on x86_64, unmodified.
   Both build-time assertions held: the tz database resolves inside `python:3.12-slim`
   (so LOCAL-date partitioning is safe) and DuckDB httpfs 1.5.5 baked in. Docker is no
   longer the untested path; only Apple `container` and Docker now both work.
2. **Local-time scheduling on a UTC host.** Every job resolved to the right instant —
   `upload_hourly` next at 16:05Z = 12:05 local. `timeutil` genuinely does not depend on
   the process timezone.

Measured: the whole process is **231 MB RSS** (pollers, WebSocket, scheduler, dashboard,
pyarrow, DuckDB), which is what sized the 1 GB instance.

**The cutover is done — the Mac collector is stopped and there is one collector again.**
The full history moved across: **181,371 rows**, `2026-08-17T19:22:55Z .. 15:55:06Z`,
14 channels, `integrity_check` ok, and the instance has been polling on top of it since.

The ~25 minutes where both collectors ran needed care, and the rule is worth remembering:
**the overlap was dropped, not merged.** Both hosts sampled the same readings on different
clocks, so a union would have given each channel ~240 samples in that hour instead of
~120, doubling every kWh figure. The dedupe key does not protect you — `ts_utc` differs,
so the rows are not duplicates *by the key* even though they are duplicates *in fact*.
Only the instance's 102 rows strictly after the Mac's last timestamp were carried over;
the boundary step was 22 seconds, under one poll interval, so there is no gap either.
`deploy/spool-splice.py` and `deploy/lightsail.md` have the procedure.

The Mac's `data/spool.db` is untouched and is the pre-migration backup — keep it until the
instance has a few clean days behind it.

The token caches were deliberately **not** copied: Okta rotates the Carrier refresh token
on every refresh (`carrier_auth.py:1291`), so a shared chain would have the two hosts
invalidating each other. Each host bootstraps its own.

**LG&E was re-authorised on the instance** rather than copied, for the same reason, and
that reverses the earlier plan to keep Green Button on the Mac: `tokens/lge.json` holds a
rotating refresh token, so whichever host has the file is the only one that can refresh
it. When the job split happens, `greenbutton_daily` should stay **wherever that token
lives** — which is now the instance — not move to the Mac.

First fetch there returned 4,398 rows over 2026-08-01..19: both meters, both interval
series (3,519 at 900s, 879 at 3600s), 4,198 reverse-flow readings skipped. Connect still
does not serve the two retired house ids the Download export republishes.

**The migration validated itself.** With the spool history in place the meter card ran a
full-day comparison on **24 of 24 hours with zero exclusions** — meter 77.614 kWh vs
panels 75.186, **−3.1%**, consistent with the ~3.4% measured on the Mac. Nothing was lost
in the move.

## Still ahead: split the poller from the batch stages

Agreed 2026-08-19, and it supersedes "one box, somewhere reliable" as the end state. Keep
the cheap always-on box doing only what must be live — Leviton poll + keepalive, Carrier
status, `bryant_daily_energy`, `upload_hourly`, the spool purge, the health/UI server —
and move `rollup_hourly`, compaction, `build-dim`, `create-glue-tables` and `backfill` to
the Mac Mini, which is already paid for.

**Split by credential locality, not just by CPU.** That is the non-obvious part: two of
the five jobs are pinned by which token cache they need, not by how heavy they are. Both
`bryant_daily_energy` and `greenbutton_daily` stay on the instance despite being daily
batch, because Carrier's and LG&E's refresh tokens both rotate and each must live on
exactly one host — and as of 2026-08-19 that host is the instance for both.

**The real justification is durability, not money** — the spread between bundles is only
~$7/month. In the split, S3 becomes the archive within an hour of collection and the Mac
becomes *disposable*: if it is down for a week you lose nothing, you re-run the rollups
over the missed range afterwards. That is what idempotent-over-a-date-range was for. It
also shrinks the always-on box's AWS permissions to `PutObject` on one prefix.

**What it costs to build:** `default_jobs()` (`runtime.py:557`) returns a hardcoded
5-tuple with no filtering, so it needs a config knob selecting which jobs a process runs.
The fiddly part is `daily_maintenance`, which bundles four steps that now belong on
opposite sides — upload catch-up and the spool purge stay with the spool, compaction and
the re-roll move to the Mac. Clean to separate (the purge only reads the spool's own
`uploaded_at`), but it is a genuine change to the one component that is currently proven.

**It cannot be step one:** S3 *is* the handoff between the halves, so PLAN.md §16's manual
cycle has to come first. Good news is it changes nothing about what is deployed today.

---

## The EC2 analysis that led here

Kept because the reasoning is worth not re-deriving, even though the answer became
Lightsail:

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
home LAN. The spool already absorbs S3/network outages — it purges only rows that are both
uploaded **and** aged — so the only gap redundancy would have covered is the process itself
being down.

Two things from that analysis turned out differently in practice, both recorded above: the
image did **not** have to be arm64 (Lightsail is x86 and the Dockerfile is arch-agnostic by
design), and the instance role is **not** available on Lightsail, which was the one real
concession made for the price. **Lightsail Container Service was evaluated and rejected
outright**: it has no persistent storage — the deployment spec has no volume parameter at
all — so every deploy would drop the spool and the token caches, manufacturing exactly the
gaps cardinal rule 1 exists to prevent.

The launchd `KeepAlive` experiment was overtaken by events — the Mac is no longer intended
to be the always-on host, so it stays untested and no longer matters much.

---

## Also open

- **Nothing has ever written to S3.** Still the biggest gap and now the blocker for the job
  split above: `upload`, `compact-daily`, `rollup`, `build-dim`, `create-glue-tables` and
  `backfill` have never run against a real bucket. `PLAN.md` §16's "full manual cycle" is
  the next piece of work. (The account is `603071433332`; credentials via
  `source ~/code/bryantDeployerRole.sh`, IAM user `bryantDataCollectorDeployer`, whose
  Lightsail permissions are confirmed but whose S3/Glue permissions are not yet.)
- **The Lightsail spool is now the only live copy of the history** (the Mac's stopped
  `data/spool.db` is a static backup as of 2026-08-19 15:55Z). Nothing is uploaded, so
  nothing purges and it just grows — ~35 MB/day against 40 GB. Getting S3 working is what
  turns this from "one disk" into a real archive.
- **New Leviton breakers** arriving ~2026-08-21. `energycap discover` prints a
  `channel_map.json` skeleton for anything unmapped; `channel_id` is `breaker_p{position}`,
  never the API's breaker id. Priority circuits, from the first real load analysis: **A-1-3
  (dryer)** — a noon spike of ~5.4 kW cycling on a thermostat was identified as the dryer purely
  from the feed legs, because it was balanced across both — then **A-10-12** (kitchen
  counter/dishwasher MWBC), which straddles both legs and will otherwise keep masquerading as a
  240 V load.
- The chart shows at most three series, so a derived "Panel A total (A+B)" series was offered
  and not yet built. Leg-level series read as though a 2-pole load exceeds its own panel feed.
