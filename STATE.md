# Where this project is — handoff, updated 2026-08-23

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

1. ~~**Nothing has ever touched AWS.**~~ — **partly resolved 2026-08-23.** `S3_BUCKET` is
   set, `s3://ericpullen-energycap` exists, and **`upload` has run for real**: 140 hours,
   694,557 rows, 0 failed. `docs/s3-storage.md` is the plan and the log. What is *still*
   unproven is the rest of `PLAN.md` §16's cycle — `compact-daily`, `rollup`, `build-dim`,
   `create-glue-tables` and `backfill` have not yet run against the bucket, and the Athena
   side of the README is still desk-checked rather than executed.
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

---

## The panel went almost fully smart — 2026-08-22

20 more Leviton smart breakers went in. `energycap discover` found **32 live channels, all
32 now mapped** (`unmapped_count: 0`), up from 12 mapped of 32. The collector needed **no
restart and no code change**: it re-reads the hierarchy hourly, so
`/healthz` was already reporting `channels_seen: 32` and 65 rows a cycle before anything
was edited. What was missing was only the semantic layer.

| | before | after |
|---|---|---|
| Leviton entries in `channel_map.json` | 12 | 32 |
| `dim_channel` rows | 26 | 46 |
| Panel A devices on a smart breaker | 2 of 22 | 18 of 22 |
| Panel B | 0 of 10 | 4 of 10 |

Both repos changed, and blackstart is still the source of truth for labels — every new
entry carries `blackstart_device_id` and no label of its own:

- `config/channel_map.json` — 20 new entries, plus the Leviton block reordered per hub
  (breakers by position, then CTs, then legs) so it reads like a panel schedule.
- `~/code/blackstart/data/montfort.json` — 22 devices retrofitted to the real `-0ST`
  catalog numbers with a `monitoring.meteredBy` block naming hub, position and
  `channel_id`. Every one of the 22 agreed with the amps and poles already recorded there,
  which is a genuine cross-check of both files: **no rating changed.**

**The catalog suffix is `-0ST`, not `-ST`.** blackstart's `smartUpgradePath` predicted
`LB120-ST`; the hardware reports `LB120-0ST`. Seven ratings confirmed (15/20 A 1-pole,
15/20/30/40/50 A 2-pole).

### What the new channels are worth

- **`breaker_p10` on hub `1000_0046_1D48` is the one to know about.** It is the Bryant
  284ANV060000 5-ton outdoor unit — blackstart B-10-12, the **last untraced load in the
  house** and the reason the Panel B load meter reads LOW. 683 W + 667 W = 1,350 W observed
  at part capacity. It is also the 30 s counterpart to the Bryant `cooling`/`hpheat` daily
  kWh channels: the same unit through two clouds at two grains. **Correlate, never sum.**
  Note this is *not* `ct_2_a`/`ct_2_b`, which are on the strip-heat feeder.
- **`breaker_p26` on the same hub** measures the Anker trickle-charge rate that blackstart
  records as a 1,000 W placeholder.
- **The three Panel A MWBCs (p5, p10, p13) carry a volts trap.** `sources/leviton.py` sums
  both poles for every metric. `watts` is right — the legs are independent circuits — but
  `volts` reads ~240 for what are two 120 V legs. The map notes say so on each of them.
- Leviton's `branchType` is a dropdown somebody picked, exactly like the CT `usageType`:
  it calls the heat pump "Air Conditioner". Not authoritative, ever.

### Two contradictions the same data turned up — both resolved same day

The owner settled both, and the answer moved more than the questions asked. **Each panel's
LWHEM-2 energy monitor sits on its own 2-pole dumb breaker**, which is what those two
"placeholder" objects are:

- **Panel B 17/19** — a 2-pole 15 A dumb breaker feeding Panel B's monitor. The slots were
  *not* empty; the photo reading was wrong. Panel B reconciles again at 18 occupied + 12
  empty.
- **Panel A 27/29** — the same thing for Panel A's monitor, and **not** the Siemens SPD it
  was recorded as. Leviton's "Smart Home" label was right and the earlier identification
  was wrong.
- **Panel A 22/24 is now a Leviton LSPD1-T** plug-on surge protective device. That is where
  the SPD went; the external Siemens unit is gone from Panel A (Panel B keeps its own at
  28/30).

Two things worth carrying forward:

1. **Neither monitor breaker may be shed, and the reason is not what it looks like.**
   Switching one off de-energises nothing — the smart breakers are ordinary mechanical
   breakers and keep carrying current — but it takes out all metering, the app and remote
   control for that panel, i.e. exactly the visibility you want in an outage. It also means
   **the one load in the house that can never appear in the data is the metering system
   itself**: each monitor's supply comes through a dumb breaker.
2. **The LSPD1-T is a combination device, which is easy to model wrong.** Leviton's spec is
   "two 15A single-pole standard thermal magnetic circuit breakers" plus a Type 1 SPD in one
   plug-on body — so blackstart A-22 and A-24 keep their identities, their ten circuits and
   their ~770 W unchanged; only the hardware record moved. I first modelled it as a bare SPD
   and stranded those ten circuits; the owner corrected it. Two facts fell out of the spec
   that are worth knowing: the SPD is fed **line-side**, so it keeps protecting with those
   breakers off or tripped, and **there is no smart variant**, which makes A-22 and A-24 the
   only Panel A branch circuits that can never report usage.

One consequence on this side of the fence: **Panel A can produce no further Leviton
channels.** 18 of its 22 devices are smart; the other four are the generator inlet, the
monitor's own supply, and the two LSPD1-T circuits — none of which can ever be one. So
`test_dim.py`'s "newly installed breaker" example moved to **`breaker_p9` on hub
`1000_0046_1D48`** (blackstart B-9, full bath fan/light/heater), a real candidate on the
panel that still has six dumb branch breakers. `channel_map.json` needed no change for any
of this: every affected position is a dumb breaker and was never a channel.

A third, happier corroboration stands unchanged: Leviton's own label for Panel B 6/8 is
**"Heat Strip Subpanel"** — independent confirmation of the feed-through finding and of
what `ct_2_a`/`ct_2_b` are clamped on.

### The install also exposed a real bug: `breaker_p0`

Both hubs briefly produced a channel that does not exist. `breaker_p0` carried genuine
readings (10 W, 0.174 A) and went silent at 16:47Z — the 37 minutes between plugging the
breakers in and finishing the positioning wizard in the Leviton app.

Cause, in three steps: Leviton omits `position` while a breaker is enrolled but **not yet
located** ("Un-Positioned Breaker" in its UI, a normal part of every install);
`aioleviton` defaults the missing key to `0` (`position=data.get("position", 0)` against an
`int` field); and §6.5's `breaker_p{position}` turns that default into an identity. Real
watts from a real circuit, filed under a slot no Leviton panel has — they number from 1.

Fixed: `BreakerReading.is_unpositioned` is checked in `_map_snapshot`, which is the
package's only mapper, so one guard covers REST and WebSocket both. Such a breaker produces
no rows and is WARNed once per process (`leviton_breaker_unpositioned`), with the count on
`/healthz` as `leviton_ingest.unpositioned_breakers`; `discover` lists it marked SKIP and
non-mappable so nobody is invited to map a fiction. Same treatment §7.3 gives an
unrecognised enum string. DEVIATIONS #171 has the reasoning.

**Left alone deliberately:** the ~74 rows already written under `breaker_p0` stay (rule 2 —
they are what the API said, they are flagged unmapped by `build-dim` and `/ui`, and nothing
sums by breaker yet), and `aioleviton` is not patched or vendored — the guard is correct
either way, and `from_model`'s `or 0` catches a future upstream `None` too.

Worth knowing: **neither public integration handles this.** `rwoldberg/ldata-ha` (66★)
filters the same placeholder models, then trusts `position` verbatim — an un-positioned
breaker falls through its `_LEG1_POSITIONS` check and is silently attributed to leg 2, and
its panel card renders from position 1 up, so the entity exists but never appears.
`gtxaspec/leviton-load-center` calls it "Breaker 0". There was no prior art to copy.

### Tests

`uv run pytest` — 1599 passing (7 new: the un-positioned guard, both payload spellings, the once-only WARN, the upstream coercion it defends against, and the discover SKIP row). `tests/fixtures/blackstart/montfort_trimmed.json` was
regenerated as a faithful trim of the corrected inventory (11 devices → 24). **3
pre-existing failures** are environmental, not code: real `LGE_*` credentials are exported
in the shell, so the three tests asserting that no LG&E credential has a default value
fail. They fail identically on unmodified `main`.

blackstart: `npm test` — 107 + 105 checks pass, 0 errors, and `sw.js` `CACHE` is bumped to
`blackstart-v5`. The restored survey assertions now also pin *why* Panel B 17/19 reconcile
— they hold the monitor breaker — so a future edit that "fixes" the count by declaring them
empty again fails the test.

---

## The HVAC cross-check — `/ui/hvac`, and what it found

Asked on 2026-08-22: does the Bryant data match what the breakers see? The answer is in
three parts, and the screen at **`http://13.219.164.226:8080/ui/hvac`** shows all of them.

**1. The kWh comparison is impossible today, and not for a subtle reason.** `fetch-daily`
writes Bryant's day-grain energy to `energy/daily` in **S3**, never to the spool (rule 6),
and no bucket is configured — so `bryant_daily.last_success_utc` has been `null` since the
instance was built and **zero Bryant energy rows exist anywhere**. This is the S3 gap again,
now with a user-visible cost. The screen states it instead of drawing an empty chart.

**2. What can be compared — Bryant's 30s state against the panel's 30s watts — agrees, well.**

| | |
|---|---|
| capacity % vs compressor watts | **r = 0.976** (1-min buckets, n=289) |
| slope | **29.9 W per capacity point**, sd 1.4 (4.7%) |
| implied full-load draw | **2.99 kW** — sane for a 5-ton variable-speed unit |
| Bryant says off (no `stage_pct`) | breaker reads **exactly 0.0 W**, 72 of 72 buckets |
| Bryant reports a capacity | breaker never below **1,175 W**, 289 of 289 |
| disagreements | **0** |

Two clouds that share no identifier, describing one machine, and they agree to within a few
percent. That is the first independent confirmation that the HVAC side of this data is
trustworthy.

**3. `ct_2_a`/`ct_2_b` are not (mostly) the strip heat.** Over the full five-day Bryant
overlap the feeder's watts correlate **r = 0.959 with blower RPM cubed** — the fan affinity
law — versus **0.73 with compressor capacity**, running 0 W at 400 rpm to 822 W at 1,200 rpm.
The clamps are on the whole HVAC subpanel feeder, and in cooling season what flows through
them is the **air handler blower**. The earlier "flat 0 W through 17 minutes of cooling"
reading is not contradicted — it was taken at low blower speed, where the feeder still reads
0.0 W today. This is also partial evidence for blackstart's `hvac-blower-circuit`: the blower
is behind the feed-through lug, which is what that safety note assumed.

### The trap the screen is built around

**Bryant and Leviton never share a `ts_utc`.** Each source stamps its own cycle, so joining
on the canonical dedupe key returns **0 rows of 722** — not few, zero. Every comparison on
this screen is bucket-aligned, and the bucket width is printed next to every number. Same
lesson as the two-collector overlap: `ts_utc` identifies a cycle, not an instant two pollers
agree on.

### Caveats the screen keeps visible

The compressor breaker went in at **16:59Z on 2026-08-22**, so its baseline is *hours*, all
in cooling, capacity 45–85%, one outdoor-temperature band. Sample counts and coverage are on
every figure for that reason — a 24h window currently reports ~26% coverage, which is the
honest number. Heating season, defrost and the strips are all unobserved.

Selection is by `category == "hvac"` in `channel_map.json` (the compressor's entry gained an
explicit override), so a future HVAC circuit joins the screen by being mapped, not by a code
change. DEVIATIONS #172 has the reasoning.

---

## Bryant energy is recorded at last — and the Carrier API audit

### The blocker was not subtle

`fetch-daily` and `backfill` were S3-only. No bucket, so `bryant_daily_energy` had failed
**every night since the instance was built** and zero Bryant energy rows existed anywhere.
`stages/dailystore` now owns the destination for both: local
`{SPOOL_DIR}/daily/bryant-YYYYMM.parquet` always, S3 as a mirror when a bucket appears. Rule 6
holds — day-grain rows still never touch the spool; this is a sibling dataset to `meter/`,
exactly the `fetch-greenbutton` precedent.

**Backfill result: 3,712 rows, 232 consecutive days, 2026-01-02..08-21**, every day the old
collector's Lambda ever wrote, no gaps. The component split over that span is why it was worth
doing:

| component | kWh | panel side |
|---|---|---|
| hpheat | 2,954 | compressor breaker |
| cooling | 2,445 | compressor breaker |
| fan | 1,722 | **shares the feeder** |
| eheat (strips) | 1,277 | **shares the feeder** |

`fan` and `eheat` are one conductor as far as `ct_2_a`/`ct_2_b` are concerned. **Bryant is the
only source that can separate the blower from the strips**, which is the whole case for keeping
a day-grain number now that the compressor has its own 30s channel. 1,277 kWh of strip heat is
not a rounding error, and it is entirely invisible on the panel side.

### Two bugs this shook out, both about silent loss

1. **A failed local Parquet write was deleting the month.** `pq.write_table` unlinks the target
   before opening it, so a write that then fails leaves *nothing* — 336 rows read fine, the
   write hit EACCES, the file vanished, and the next run merged over an empty month and wrote
   28 rows looking perfectly healthy. Now temp-file + fsync + `os.replace`, the same guarantee
   `s3io.write_table_atomic` always gave the mirror.
2. **A blended coverage number turned a missing channel into a -99% disagreement.** For
   2026-08-18..21 the feeder covered 100% while the compressor breaker did not yet exist, so
   the panel total was feeder-only against Bryant's whole-system total. Coverage is now per
   group, the worse of the two governs, and every mapped channel must have reported before any
   delta is shown. DEVIATIONS #173a/b.

The first genuinely comparable day will be **2026-08-23**, which Bryant reports on the 24th.

### The Carrier API audit — what we request and do not map

This had already been done and written down: `sources/bryant.py`'s docstring says the query
asks for everything and maps only what a *real captured response* had verified, listing
`damperposition`, `occupancy`, `zones[].name`, `oprstsmsg`, `odu.opmode` and "the `odu`
compressor telemetry" as requested-but-unmapped. **What has changed is that the live capture it
was waiting for has happened** (2026-08-17 dump) and every one of those fields came back
populated. Nobody has acted on it since.

Highest value first, given we now measure compressor watts:

| field | live value | why it matters now |
|---|---|---|
| `odu.comprpm` | `1190` | **The best one.** Compressor RPM is continuous where `opstat` is quantised to 45/60/75/85, so it is a far better x-axis for the watts calibration — and watts rising against flat RPM is how a failing compressor shows up. |
| `odu.oducoiltmp` | `74` | With `oat`, the condenser approach temperature — the standard charge/fouling indicator, and it pairs with watts to catch degradation. |
| `idu.statpress` | `0.14` | Static pressure. Now that the feeder is known to track blower power, rising static at constant CFM is a clogged filter, cross-confirmed from two sources. |
| `odu.iducfm` / `idu.iducfm` | `1166` / `513` | Three airflow numbers exist (`idu.cfm` 500, `idu.iducfm` 513, `odu.iducfm` 1166) and we record one. Worth knowing which is which before trusting CFM. |
| `filtrlvl` | `10` | Consumable *used* percent, correctly not a metric — but actionable maintenance, so a `status.json` field rather than a row. |
| `oprstsmsg`, `odu.opmode`, `idu.opstat` | `idle`, `cooling`, `off` | Per-unit state strings; we take only `odu.opstat`. Would need enum tables (append-only, never renumbered). |
| zone `zoneconditioning`, `damperposition`, `hold`, `occupancy` | `active_cool`, `15`, `on`, `unoccupied` | Real state, low value here: one zone is enabled. |

Not available at any grain: **energy finer than a day**. `energyPeriods` serves `day1`, `day2`,
`month1`, `year1` — the 30s status feed carries no energy field at all. So daily is the floor,
and the panel is the only source of sub-day HVAC energy.

Deliberately not done: **synthesising 30s Bryant power from `capacity% x 29.9 W`**. The
calibration exists now, which makes it tempting, but it is a model and not a measurement —
rules 1 and 2. Fine as a derived comparison on a screen; never a stored row.

Beyond `getInfinityStatus` and `getInfinityEnergy`, what else the endpoint offers is unknown:
PLAN.md documents only those two, and a schema introspection would be a new outbound call
pattern against Carrier — the kind of thing #155 says needs sign-off first.

---

## Nine more Bryant metrics, a modelled power series, and the schema introspected

All three asked for on 2026-08-22, after the day-grain work.

### The nine metrics

Every one had been *requested but unmapped* pending a live response — and the capture that
would settle them arrived on 2026-08-17, five days before anyone acted on it.

| API field | metric | unit |
|---|---|---|
| `odu.comprpm` | `compressor_rpm` | rpm |
| `odu.oducoiltmp` | `outdoor_coil_temp_f` | degF (via `cfgem`, like any temperature) |
| `idu.statpress` | `static_pressure` | inwc |
| `idu.cfm` / `idu.iducfm` / `odu.iducfm` | `idu_cfm` / `idu_iducfm` / `odu_iducfm` | CFM |
| `oprstsmsg` / `odu.opmode` / `idu.opstat` | `op_status` / `odu_mode` / `idu_status` | enum |

**19 rows a cycle now, up from 10**, zero unknown enums. `compressor_rpm` is the one that
earns its place: continuous where `stage_pct` is quantised to 45/60/75/85, so it is a better
independent variable for the watts comparison, and watts rising against flat rpm is how a
failing compressor announces itself.

**The three airflow numbers disagree** — 500, 513 and 1166 in one cycle — so each is recorded
under the field it came from. `cfm` keeps the old blended pick for archive continuity.

The fixtures earned their keep immediately: `odu.opmode` is `"cool"` in the 2026-08-16
capture and `"cooling"` in the 2026-08-17 one. Both are in the table at **different codes**
(6 and 1), because it is append-only and a synonym is cheaper than renumbering an archived
code.

Two real problems fell out, both recorded in DEVIATIONS #174: `_ENUM_DECODE` was built from a
typed list of three metrics, so the three new enum metrics briefly shipped with **no published
decode at all**; and the Glue catalog outgrew its own comment fields — the raw_30s metric
names alone are **251 of 255 allowed characters**. The decode now lives once in
`dim_channel`'s description with every enum table pointing at it, and the metric catalog lives
in the README with the comment naming the traps plus `SELECT DISTINCT`. Nothing was
truncated; both moved to fields that can hold them.

### The modelled power series

Bryant publishes no instantaneous power anywhere, so `/ui/hvac` now carries
`capacity_pct x watts_per_point`, drawn dashed beside the measured watts.

**It is never stored.** Rules 1 and 2 govern the archive, not the screen: a modelled watt
sitting beside a measured one is indistinguishable a year later. So it is computed on read,
labelled `derived: true`, ships its coefficient and provenance, and a test asserts that
rendering the screen writes no rows and that no `modelled_w` metric exists.

The coefficient is **fitted from the window on screen** (falling back to the measured 29.9
W/point when the window is too thin, and saying which it used). Live right now: **29.34 W/pt
fitted over 720 buckets, residual mean +9.5 W** — about 0.5% of a ~1.9 kW draw, worst single
bucket 249 W. The residual is the point: it is the number that says whether the machine is
drifting from its own control's account of itself.

### The introspection

**662 types, 106 root query fields**; PLAN.md documents two. Full table in DEVIATIONS #175.
The four worth remembering:

- **`runtimeUsageInfinity`** — `{deviceId, startDate, endDate, period}` → runtime buckets with
  cool/heat runtime split, outside temperature and `isSystemOn`. Nothing collects this today.
- **`deviceHistory`** — a generic point-history endpoint taking an arbitrary interval. The one
  remaining candidate for sub-daily telemetry history, and untested: it needs a
  `username`/`locationId` the probe did not have.
- **`infinityProfile`** — equipment identity: indoor/outdoor model and serial, capacities,
  stage count, firmware. Would settle several "which unit is this" questions permanently.
- **`infinityNotifications`** — faults and alerts.

**The API settled the granularity question in words.** `runtimeUsageInfinity` with
`period: "hour"` replies:

> `Invalid period specified. Available periods are: day, week, month`

So there is no sub-daily energy or runtime on this endpoint at all. The earlier inference was
right and is now a quotation. **The panel is the only source of sub-day HVAC energy**, which
is exactly why the compressor breaker matters.

Two loose ends, both honest: `period: "day"` returns a repeatable **HTTP 504 from Carrier's
own gateway** (the operation exists and validates input, but times out server-side — retry
another day), and `deviceHistory`'s `point` vocabulary is unexplored. Also worth knowing for
next time: these input types are **non-null** (`GetRuntimeUsageInput!`), and a nullable
declaration is a 400 with a validation message.

## The archive exists — 2026-08-23

The oldest gap in this project is closed. `docs/s3-storage.md` is the plan and the running
record; this is the short version.

**`s3://ericpullen-energycap`** (`us-east-1`), versioned, SSE-S3, all public access blocked,
five lifecycle rules. **Two scoped IAM users** — `energycap-collector` on the instance
(`PutObject`/`GetObject`/`DeleteObject` on `raw_30s/`, `daily/`, `meter/`, `_tmp/` and nothing
else) and `energycap-batch` on the Mac (whole bucket + Glue + Athena). Creds at
`~/code/energycap{Collector,Batch}Role.sh`, mode 600, outside the repo. The collector's
denials on `energy/hourly/` and `glue:GetDatabases` are confirmed against the real API, so the
always-on internet-facing box cannot touch derived data or the catalog. An `energycap` Athena
workgroup, not `primary`, with a 1 GB per-query scan cap.

**The first upload: 140 hours, 694,557 rows, 0 failed, 65.6 seconds**, in one scheduler firing
with no manual intervention. `spool.pending_rows` went 687,585 → 1,008 (just the open hour).
Verified by reading S3 back with DuckDB rather than trusting the stage's own counters:
694,557 rows and **694,557 distinct dedupe tuples** across 140 separately-written files;
**0 rows** where `ts_local`'s date disagrees with its file's `part-{YYYYMMDD}` stamp; no
day-grain metric anywhere in `raw_30s`. Nothing stranded in `energy/_tmp/`.

**The poll loops never noticed the 65-second upload** — keepalive `consecutive_failures: 0`,
`connected_hubs: 2` throughout. `runtime._call`'s `asyncio.to_thread` is load-bearing and now
measured rather than argued.

### Three things worth carrying forward

1. **`AWS_PROFILE=` is not "no profile".** It is a profile *named* empty string, and botocore
   raises `ProfileNotFound` on every client build with perfectly valid static keys present —
   `get_settings()` even reports `aws_profile = None`, because pydantic coerces it away.
   Passing no `profile_name` does not help; botocore re-reads the variable itself. Caught in
   pre-flight, or the first firing would have failed 140 hours. Fixed in `.env.example` and
   defensively in `s3io._drop_empty_aws_profile()`. DEVIATIONS #176.
2. **Never group Leviton channels by `channel_id` alone.** `count(DISTINCT channel_id)` gives
   **24**; the truth is **32**. Eight ids exist on *both* hubs — `breaker_p1`, `breaker_p10`,
   `breaker_p14`, `breaker_p26`, `ct_1_a`, `ct_1_b`, `panel_leg_a`, `panel_leg_b` — because
   both panels have a position 1. The dedupe key carries `device_id` so the archive is right,
   but any chart grouping on `channel_id` silently merges two circuits on different panels.
   **This one matters most for the UI work next.**
3. **The volume estimate was wrong by 10× and PLAN.md was right.** zstd achieves
   **1.1–1.5 bytes/row** on long-format data, not the ~10 assumed mid-plan: 694,557 rows in
   1,251,413 bytes. Post-retrofit that is ~270 KB/day ≈ **100 MB/year** for `raw_30s`. The
   whole year's archive is smaller than one Athena scan cap. The spool disk needed S3 because
   unuploaded rows never purge, not because of volume.

**Next:** `compact-daily` → `rollup` → `build-dim` → `create-glue-tables` over
2026-08-17..22, then `energy_meter` (the fifth Glue table, never built) and the mirror of the
nine local day-grain month files. `docs/s3-storage.md` §6–§8.

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

- ~~**Nothing has ever written to S3.**~~ — **the archive is live as of 2026-08-23.** See
  "The archive exists" below and `docs/s3-storage.md`. The remaining batch stages
  (`compact-daily`, `rollup`, `build-dim`, `create-glue-tables`, `backfill`) are the next
  piece of work, and they no longer block on infrastructure.
- ~~**The Lightsail spool is the only live copy of the history**~~ — no longer true. Every
  closed hour is in S3 within the hour. The purge is now *unblocked* but has not yet run:
  it needs uploaded **and** older than `SPOOL_RETENTION_DAYS=7`, and the oldest rows are from
  2026-08-17, so the first rows become eligible on **2026-08-24**. Watch the 01:30 job then —
  a shrinking spool is the end-to-end proof. The Mac's stopped `data/spool.db` remains the
  pre-migration backup from 2026-08-19 15:55Z.
- ~~**New Leviton breakers** arriving ~2026-08-21~~ — **installed and mapped 2026-08-22.**
  See "The panel went almost fully smart" below. Both priority circuits from the first load
  analysis now have their own channel: **A-1-3 (dryer)** and **A-10-12** (kitchen
  counter/dishwasher MWBC).
- The chart shows at most three series, so a derived "Panel A total (A+B)" series was offered
  and not yet built. Leg-level series read as though a 2-pole load exceeds its own panel feed.
