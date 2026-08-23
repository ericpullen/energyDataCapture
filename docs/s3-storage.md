# S3 long-term storage — the plan, and the record of turning it on

Written **2026-08-23**. `PLAN.md` §4/§10/§12 is the spec of record for the layout, the stage
contracts and the Glue tables; this file is the plan for actually landing data in a real
bucket for the first time, plus the record of what happened when it did.

**Bucket: `ericpullen-energycap`, `us-east-1`.** Account `603071433332`.

---

## 1. Why now

The S3 layer has been fully implemented since 2026-08-18 and **has never executed against a
real bucket**. Measured on the Lightsail instance on 2026-08-23:

| | |
|---|---|
| Spool | `spool.db` **204 MB** + 4 MB WAL, **678,326 rows, every one `uploaded_at IS NULL`**, oldest `2026-08-17T19:22:55Z` |
| Disk | 11 GB used of 38 GB, growing ~35 MB/day, and **nothing purges** — `SpoolDB.purge` needs uploaded **and** aged, so an unuploaded row is immortal |
| `uploader` / `compactor` / `rollup` in `status.json` | `last_success_utc: null`. Not "stale" — never once succeeded |
| Already on local Parquet | `daily/bryant-2026{01..08}.parquet` (3,712 rows, the whole Lambda history) and `meter/lge-202608.parquet` (4,398 rows) |
| `S3_BUCKET` | empty, in `.env` and in the container env |

Sixty-five rows a cycle of trustworthy data, six days deep, on one 40 GB disk with no
archive. That is the whole case.

It is also the blocker for the poller/batch **job split** in `STATE.md`: S3 *is* the handoff
between the two halves, so this has to work first.

## 2. What the account looked like beforehand

- Creds: `source ~/code/bryantDeployerRole.sh` → IAM user `bryantDataCollectorDeployer`,
  which holds **`AdministratorAccess`**. (`STATE.md` recorded its S3/Glue permissions as
  unconfirmed; they are confirmed now, by way of being unlimited.)
- **No bucket existed** for this project. Glue had **zero databases**.
- Lightsail takes no instance profile — the one concession made for the price — so the
  collector needs a **scoped access key** on the box.

One gotcha worth writing down: `.env` sets `AWS_PROFILE=` (empty), and an exported empty
`AWS_PROFILE` makes every `aws` CLI call fail with `The config profile () could not be
found`. `unset AWS_PROFILE` after sourcing the creds script.

## 3. Four code gaps

Everything else in this plan is ops. These four are real changes.

1. **No `energy_meter` Glue table.** `aws/glue.py:1195` already documents the exact fifth
   `table_specs` entry to add (`prefix_builder=s3io.meter_year_prefix`,
   `partition_keys=("year",)`), and `s3io.meter_key` / `meter_year_prefix` exist. Without it
   LG&E data lands in S3 unqueryable.
2. **`greenbutton_daily` never mirrors to S3.** `runtime.py:554` calls
   `fetch.run(start=…, end=…)` with no `bucket`, and `greenbutton_fetch.run` only uploads
   `if bucket`. `bryant_daily_energy` meanwhile auto-mirrors through `stages/dailystore`.
   Fix the asymmetry the way `dailystore` did: the **scheduled** path defaults to
   `S3_BUCKET`; `import-greenbutton` stays opt-in, because an import is a manual act on a
   file a human just downloaded.
3. **No Athena query-results location.** Not a code change — a workgroup setting — but
   nothing in the README can run until it exists.
4. **Nine existing local month files need a one-shot mirror.** `backfill --bucket` and
   `fetch-greenbutton --bucket`, both idempotent.

## 4. Phase 0 — bucket and IAM

`us-east-1`, the same region as the instance, so transfer is free.

- Block all public access; SSE-S3; **versioning ON**.
  Versioning is the durability argument, not a reflex: every stage overwrites a
  *deterministically named* object (that is what makes re-runs idempotent), so a bad code
  change silently replaces good data with bad. Versions make that recoverable.
- Lifecycle:
  - expire noncurrent versions after **30 days**;
  - abort incomplete multipart uploads after **7 days**;
  - expire `energy/raw_30s_parts_archive/` after **14 days** (the safety window PLAN.md §10
    wants is 7);
  - expire `energy/_tmp/` after **1 day** — sweeps a temp object stranded by a crash between
    stage and copy;
  - expire `athena-results/` after **7 days**.
  - **No storage-class transitions.** At this volume the per-object transition charge
    exceeds the storage saved.
- **Two IAM principals, not one:**

  | | |
  |---|---|
  | *collector* (on the instance) | `PutObject`/`GetObject`/`DeleteObject` on `energy/raw_30s/*`, `energy/daily/*`, `energy/meter/*`, `energy/_tmp/*` + `ListBucket` conditioned on those prefixes. **No Glue, no Athena, nothing on `hourly/` or `dim_channel/`.** |
  | *batch/admin* (the Mac) | whole bucket + Glue + Athena |

  All three object verbs on each prefix, because `s3io.write_table_atomic` is temp-key →
  copy → delete and `verify_row_count` does ranged GETs.

  **`energy/_tmp/` is the correction this phase forced.** `s3io.temp_key` stages *every*
  atomic write under one flat `energy/_tmp/{uuid}-{basename}` prefix — not under the
  dataset's own prefix — so it cannot be scoped per dataset and both principals need it.
  The uuid is what keeps concurrent writers from clobbering each other; the cost is that
  prefix isolation stops at the staging area. A stranded temp object is swept by lifecycle
  after a day.

  This split is otherwise exactly the permission boundary the job split wants — "it shrinks
  the always-on box's AWS permissions to `PutObject` on one prefix" — so it is worth getting
  right once, here.

## 5. Phase 1 — drain the backlog

The one step with real risk, and the one with a hard placement constraint: **it must run
inside the container.** Never open the live spool from the host — that corrupted the database
twice (2026-08-17 and 2026-08-18), and read-only is not an exemption because SQLite still
mutates `-shm`.

The design already handles this, deliberately:

- `_job_hourly_upload` calls `uploader.run()` with **no range** — its docstring calls that
  "the true catch-up window, so an outage of any length drains in one firing".
- `runtime._call` runs every stage on `asyncio.to_thread`, precisely so a long upload cannot
  stall the 30s pollers or the **50s Leviton keepalive** (a stalled keepalive would drop the
  hub).
- `default_jobs` hands the job the process's own `SpoolDB`, so no second connection opens.

So: set `S3_BUCKET` and the collector key on the instance, recreate the container, and wait
for `HH:05`. ~144 hours drain sequentially, each hour verified and marked on its own.

```bash
docker logs -f energycap | grep -E 'upload_(start|hour_ok|done)'
curl -s localhost:8080/healthz | jq '.uploader, .spool'
```

Watch for per-hour `verify_row_count` failures — that is the interlock doing its job, not a
bug — and for `spool.pending_rows` falling to roughly one hour's worth.

## 6. Phase 2 — compact, roll up, dim

From the Mac. These stages read S3 and never touch the spool, which is what makes the Mac
safe for them.

The scheduler only compacts D-1, so the six days of history need explicit ranges:

```bash
uv run energycap compact-daily --start 2026-08-17 --end 2026-08-22
uv run energycap rollup       --start 2026-08-17 --end 2026-08-22
uv run energycap build-dim
```

Verify what PLAN.md §10 promises rather than assuming it: parts **moved** to
`energy/raw_30s_parts_archive/`, `energy/raw_30s/` holding exactly one authoritative set per
day (no part+day-file double count), day-file row count equal to the deduped count.

## 7. Phase 3 — query surface

Add the `energy_meter` spec (§3.1), then:

```bash
uv run energycap create-glue-tables    # creates the `energy` database too
```

Then run the README's DuckDB examples against real S3, and the same queries in Athena. The
gap-finding query — hours where `sample_count` is below expected — is the one that proves the
table comments are not lying.

## 8. Phase 4 — the day-grain datasets

```bash
uv run energycap backfill --start 2026-01-02 --end 2026-08-22 \
    --bucket ericpullen-energycap
uv run energycap fetch-greenbutton --start 2026-08-01 --end 2026-08-23 \
    --bucket ericpullen-energycap
```

Plus the §3.2 fix, so the meter mirror stops being a manual act.

## 9. Phase 5 — confirm the archive is real

Let `daily_maintenance` run once at 01:30, then check that the spool **shrinks**. That is the
end-to-end proof: a row is deleted only when it is both uploaded and aged, so a shrinking
spool means the whole chain held. After a few clean days the job split unblocks.

## 10. Two things worth knowing before committing to this

**PLAN.md's "tens of MB/year" is right, and the estimate that replaced it was wrong.**
This section originally projected 0.7–1.5 GB/year for `raw_30s`, from ~10 bytes/row. Phase 1
measured the real thing and it is **1.1–1.5 bytes/row** — zstd over long-format data is
extraordinarily effective, because every column except `value` and `ts_utc` is a small
repeating vocabulary that dictionary-encodes almost to nothing.

The measured numbers, from 140 real hourly parts: **694,557 rows in 1,251,413 bytes.** At the
post-retrofit rate (~242k rows/day across 32 Leviton + 2 Bryant channel pairs) that is
**~270 KB/day ≈ 100 MB/year**. So the spec's original order of magnitude stands and the
correction was the error — worth stating plainly, because it is the number everything
downstream gets sized against.

Cost is therefore even less than "trivial": storage is cents per year, and the whole archive
is smaller than the 1 GB per-query Athena scan cap. The reason the 40 GB spool disk needed S3
was never volume — it was that nothing purges until rows are uploaded, so an unuploaded row
is immortal.

**Rollup rewrites the whole local day's file every hour** — by design, to avoid intra-day
merge logic. With versioning on that is ~24 versions/day of a ~20 KB object. Harmless, and
the reason the noncurrent-version expiry rule above is not optional.

---

## 11. Execution log

Appended as each phase actually runs. Nothing here is a plan; it is what happened.

### Phase 0 — bucket and IAM · **done 2026-08-23**

Everything in §4, created and verified. Nothing in the bucket yet; that is Phase 1.

**Bucket `ericpullen-energycap`** (`us-east-1`, so `get-bucket-location` correctly returns
null — there is no `LocationConstraint` for that region):

| | |
|---|---|
| Versioning | `Enabled` |
| Default encryption | `AES256` (SSE-S3) with `BucketKeyEnabled` |
| Public access | all four blocks `true` |
| Object ownership | `BucketOwnerEnforced` (ACLs off entirely) |
| Lifecycle | `expire-noncurrent-versions` (30d), `abort-incomplete-multipart` (7d), `expire-parts-archive` (14d), `sweep-stranded-temp-objects` (1d), `expire-athena-results` (7d) |

**Two IAM users, each with a scoped managed policy** — `energycap-collector` and
`energycap-batch`. Credentials are `export`-style shell fragments at mode 600, following the
existing `~/code/bryantDeployerRole.sh` convention:

- `~/code/energycapCollectorRole.sh` — goes on the instance in Phase 1
- `~/code/energycapBatchRole.sh` — the Mac's creds for Phases 2–4

Both are outside the repo. The Mac no longer needs `AdministratorAccess` for this work;
`bryantDeployerRole.sh` stays for account administration only.

**Athena workgroup `energycap`**, created rather than reusing `primary` so this project's
results and cost stay separate. Output `s3://ericpullen-energycap/athena-results/`, SSE-S3,
`EnforceWorkGroupConfiguration=true`, and a **1 GB `BytesScannedCutoffPerQuery`** — the whole
dataset is ~1.5 GB/year, so a query that scans more than that has lost its partition filter,
and failing is more useful than billing.

#### The permission split was tested by asserting the denials, not just the grants

The collector key can put/get/delete under `energy/raw_30s/` and `energy/_tmp/`, and is
**denied** on `energy/hourly/` (`PutObject`) and on `glue:GetDatabases` — both confirmed by
the real API, not by `simulate-principal-policy`. That is the point of the split: the
always-on internet-facing box cannot touch derived data or the catalog.

Smoke-test objects were then hard-deleted **including their versions and delete markers** —
with versioning on, `delete-object` only writes a marker, so the bucket is not actually empty
until the versions go too. Confirmed: 0 versions, 0 delete markers.

#### Two traps worth not re-deriving

1. **`athena:ListWorkGroups` and `athena:ListDataCatalogs` take no resource ARN.** They were
   in the workgroup-scoped statement and silently failed while `GetWorkGroup` on the same
   statement worked. They now live in a separate `Resource: "*"` statement (policy `v2`).
2. **IAM propagation on a new policy *version* took over four minutes.** Long enough that an
   inline probe policy attached mid-diagnosis looked like the fix and wasn't — removing the
   probe left the call still working, because `v2` had simply become live in the meantime.
   `simulate-principal-policy` said `allowed` the whole time and was right. If a fresh grant
   is denied, wait considerably longer than feels reasonable before believing the policy is
   wrong.

Also, for anyone driving the CLI here: `.env` exports `AWS_PROFILE=` (empty), which makes
every `aws` call fail with `The config profile () could not be found`. `unset AWS_PROFILE`
after sourcing any creds file.

### Phase 1 — drain the backlog · **done 2026-08-23**

**140 hours, 0 failed, 0 skipped, 694,557 rows, 65.6 seconds.** One scheduler firing at
15:05Z, no manual intervention, no second spool writer.

```
upload_start  bucket=ericpullen-energycap hours=140 force=false
upload_done   hours_uploaded=140 hours_failed=0 hours_skipped=0
              rows=694557 marked=694557 last_hour_uploaded=2026-08-23T10 duration_s=65.59
```

| | before | after |
|---|---|---|
| `spool.pending_rows` | 687,585 | **1,008** (only the still-open hour, oldest `15:00:13Z`) |
| `uploader.last_success_utc` | `null` since the instance was built | `2026-08-23T15:06:05Z` |
| objects under `energy/raw_30s/` | 0 | **140** parts, 1,251,413 bytes total |
| objects stranded in `energy/_tmp/` | — | **0** |

#### What was verified, independently of the stage's own logging

Read back from S3 with DuckDB `httpfs` under the `energycap-batch` key — which also
pre-validates Phase 2's read path:

- **694,557 rows in S3, and 694,557 distinct `(ts_utc, source, device_id, channel_id, metric)`
  tuples.** Zero duplicates across 140 separately-written files.
- **Local-date partitioning holds exactly**: 0 rows where `ts_local`'s date disagrees with the
  `part-{YYYYMMDD}T{HH}` stamp of the file containing it. CLAUDE.md rule 4, confirmed on real
  data rather than in a test.
- **Rule 6 holds**: no day-grain metric and no `kWh` unit anywhere in `raw_30s`.
- Partitions run `day=17` through `day=23`, and day 17 begins at `part-20260817T15.parquet` —
  15:00 *local*, which is the 19:22Z first spool row. The partition boundary is the local one.
- Split by source: leviton 511,279 + bryant 183,278 = 694,557.

#### The poll loops never noticed

This was the one real risk and it is now measured, not reasoned about. Across the 65-second
upload the Leviton keepalive held `consecutive_failures: 0` with `connected_hubs: 2`, and both
pollers kept their success stamps current. `runtime._call`'s `asyncio.to_thread` is doing
exactly what its docstring claims, and a 140-hour catch-up is no threat to the 50-second
keepalive that would drop the hub.

`docker compose stop` also proved the clean-shutdown contract on the way in: the 4 MB `-wal`
and the `-shm` were checkpointed fully into `spool.db` and removed, so nothing was recovered
on restart.

#### A query trap the verification turned up — for the UI work next

**Never count or group Leviton channels by `channel_id` alone.** A naive
`count(DISTINCT channel_id)` returns **24** where `/healthz` says 32, because eight
`channel_id` values exist on *both* hubs — `breaker_p1`, `breaker_p10`, `breaker_p14`,
`breaker_p26`, `ct_1_a`, `ct_1_b`, `panel_leg_a`, `panel_leg_b`. Both panels have a position 1.

`count(DISTINCT (device_id, channel_id))` returns **32**, matching `channels_seen` and the 32
mapped entries in `channel_map.json`. The canonical dedupe key includes `device_id` precisely
for this reason, so the archive is correct — but any query, chart or dashboard that groups by
`channel_id` will silently merge two circuits on different panels. Join through `dim_channel`,
or group on the pair.

Also visible in the first real archive: the retrofit is legible in the data. 10 channel pairs
a day through 2026-08-21, 27 on the 22nd, 32 from the 23rd — ~97,900 rows/day before, ~242,000
after.

### Phase 2 — compact, roll up, dim · **done 2026-08-23**

Run from the Mac under `energycap-batch`. These stages read S3 and never touch the spool,
which is what makes the Mac safe for them.

| stage | result |
|---|---|
| `compact-daily 2026-08-17..22` | 6 days, **583,677 rows**, 129 parts archived, **0 duplicates dropped**, 0 failed |
| `rollup 2026-08-17..22` | 6 days, **4,979 rows**, hours 9/24/24/24/24/24, every write verified |
| `build-dim` | **46 rows**, 0 placeholders, 24 from blackstart, `live_channels: 32`, `unmapped_count: 0` |

Bucket after: `raw_30s/` 17 objects (6 day files + 11 parts for the open day),
`raw_30s_parts_archive/` 129, `hourly/` 6, `dim_channel/` 1, `_tmp/` **0**.

#### The no-double-count invariant holds

PLAN.md §10's whole reason for the archive prefix. Verified structurally, not assumed:
days 17–22 have **exactly one day file each and zero parts**, day 23 has **11 parts and no day
file**, and **no day has both**. The archive prefix holds the 129 moved parts — 583,677 rows,
a strict duplicate of the day files rather than extra data, and it is not covered by any Glue
table location.

Row count through compaction: **694,557 before, 694,557 after, 694,557 distinct dedupe
tuples.** Nothing lost, nothing duplicated.

#### The kWh math, re-derived from outside the SQL

- `kwh` recomputed independently as `mean * sample_count * 30 / 3.6e6` over all 1,274 watt
  rows: **max absolute error 4.4e-16**. The rollup SQL is doing exactly what §2.5 specifies.
- **0** non-`watts` rows carry a `kwh` — observed-time-only, watts only.
- `sum(sample_count)` over the rolled-up hours = **583,677** = the raw row count for those
  days, **difference 0**. This is the gap-accounting contract: `sample_count` is what
  distinguishes "the load was off" from "the collector was down", and it reconciles exactly.
- 24 physical hours per full local day (no DST in this range), 9 for the partial first day.

**And it reproduces a number measured independently, before S3 existed.** The two panel feed
CT pairs sum to **75.19 kWh for 2026-08-18**; the live dashboard, computing from the spool at
the time, recorded **75.186** against a meter reading of 77.614 (−3.1%). The full chain —
spool → hourly part → day file → rollup — returns the same answer to three decimal places.

#### `dim_channel` joins, and the two rows that do not

34 of the rollup's 36 `(source, device_id, channel_id)` series join. The two that do not are
**`breaker_p0` on both hubs**, one hour each, 38 samples apiece — the phantom un-positioned
breaker of DEVIATIONS #171. That is the designed outcome, not a defect: the rows stay in raw
because rule 2 says record what the API said, and they are absent from `dim_channel` because
the channel is a fiction. A UI must choose deliberately between a LEFT JOIN (two unlabelled
series) and an INNER JOIN (two silently dropped).

#### Two things for the UI work

- **`panel_leg_a`/`panel_leg_b` carry only `hz` and `volts`** — no `watts`, so no `kwh`. They
  are a voltage/frequency reference, not an energy series. The house total is the **feed CT
  pairs** (`ct_1_a`/`ct_1_b` across both hubs), which is what matches the meter.
- **Never `sum(kwh)` across all channels.** The hierarchy nests — a breaker's watts are also
  inside its panel's feed CT — so an unqualified total double-counts. Pick a level.

#### A redaction false positive, found by reading the real logs

`compact_day_verified` logged `pass_=1` and it came out as `"pass_": "***REDACTED***"`.
`logging._normalise_key` strips non-alphanumerics, so `pass_` normalises to `pass`, which is in
`SECRET_KEY_NAMES`. The scrubber is right and was left alone — over-redacting costs a
diagnostic, under-redacting costs a credential, and that trade should not be reversed. The
field was renamed to `compaction_pass`, which logs as a number.

Two tests were added: one pinning that `pass_` *still* redacts (so nobody "fixes" this by
weakening the scrubber), and one that walks the AST of every `log.*()` call in
`src/energy_capture/` and fails if any keyword normalises onto `SECRET_KEY_NAMES`. `pass_` was
the only offender; `auth=`, `token=` and `credentials=` are equally easy to reach for.

#### Idempotency, proven rather than assumed

`compact-daily` re-run over the identical range: every day `rewrote: false`, `parts: 0`,
`parts_archived: 0`, `duplicates_dropped: 0`, same 583,677 rows. A second run is a no-op that
still verifies.

### Phase 3 — the query surface · **done 2026-08-23**

`energy_meter` is built — PLAN.md §13's fifth table, designed 2026-08-18 and never
implemented — and `create-glue-tables` has run against the real catalog for the first time.

```
glue_tables_done database=energy database_created=true tables=5 created=5 updated=0
created_tables=[energy_raw_30s, energy_hourly, energy_daily, energy_meter, dim_channel]
```

- **Idempotent**: a second run reports `unchanged: 5`, `created: 0`, and issues no writes.
- **Update-in-place works too** (a different code path): correcting one comment and re-running
  reported `updated: 5`, `created: 0` — five tables amended, none duplicated.
- **Partition projection landed**: `projection.enabled=true`, `year` 2024–2035, `month`/`day`
  2-digit, and `storage.location.template` pointing at the real bucket. No crawler, no
  `MSCK REPAIR`, no partition ever registered by hand.

`energy_daily` and `energy_meter` are created over prefixes that are still **empty** — Phase 4
mirrors the nine local month files. The tables are correct; they just have no rows yet.

#### Athena, executed rather than desk-checked

A dedicated workgroup (`energycap`, not `primary`), results to `athena-results/` with a 7-day
lifecycle, 1 GB per-query scan cap.

| query | result |
|---|---|
| `count(*)` for one projected partition | **97,920** — identical to DuckDB, 0 bytes scanned (Parquet metadata) |
| `SHOW TABLES IN energy` | all five |
| hourly kWh by label, joined to `dim_channel`, with `sample_count` (README ex. 2) | Panel A feed A 164.88 kWh … Water heater 40.15 — **19,856 bytes scanned** |
| gap finder, `sample_count < 118` (README ex. 3) | **38 hours**, worst 1 sample, best 115 |

The gap finder returning 38 is the honest answer, not a fault: the partial first day, the
retrofit day where new channels began mid-hour, and `breaker_p0`'s single hour. That is exactly
what the column comment exists to make legible.

#### Three things the fifth table forced

1. **A latent comment bug, surfaced by a test rather than by a reader.**
   `_CATALOG_METRICS` is built from metric groups *that have a table*. Giving `ccf_interval` a
   table put **CCF into the shared vocabulary**, and `energy_raw_30s` — the one table with no
   `unit` override — started advertising a unit it can never hold.
   `test_a_unit_comment_names_no_unit_that_cannot_appear_there` caught it immediately. Both
   `energy_raw_30s` and `energy_meter` now carry `unit` comments scoped to their own metrics,
   the pattern `energy_daily` already used. Left alone, this would have told an LLM that a
   30-second watt-sample table might contain cubic feet of gas.

2. **Rule 1's intent is universal; its wording is not.** Every table description must warn that
   a gap is not zero load, and the test demanded the literal phrase "gaps mean collector
   downtime, never zero load". For `energy_meter` that is simply false — we do not collect this
   series; LG&E publishes it days late and revises it, so a gap means the utility has not
   published, and blaming collector downtime would send a reader to check our own uptime. The
   table now gets its own branch of the test, asserting the meter-appropriate form: publication
   lag, *not* collector downtime, and **an absent interval is not zero consumption**, nothing
   interpolated or zero-filled.

3. **The description hit the 2048-character ceiling twice** and `_fit` refused both times
   rather than truncating — which is the whole point of that guard, since a warning cut off
   mid-sentence is worse than no warning. Trimmed to 2035 by cutting prose, never a warning.

Test counts are now derived from `ALL_TABLES` rather than hardcoded, so a sixth table cannot
leave assertions quietly checking the old number. `test_the_meter_table_is_not_created_yet`
became `test_the_meter_table_exists_and_carries_interval_s`, pinning that the `900`/`3600`
double-publication warning is actually in the shipped comment.

The README's metric catalog was updated in the same breath: `kwh_interval` and `ccf_interval`
are no longer "designed but not yet collected", and the stale "the dataset PLAN.md §13 designs
and does not build" line now names the built table and repeats the `interval_s` warning.
`tests/test_docs.py` enforces that seam, and caught both the omission and — when the fix
mentioned `energy_meter` in backticks inside the metric cell — a table name being parsed as a
metric name.

### Phase 4 — the day-grain datasets · **done 2026-08-23, with one thing blocked on you**

All five datasets now hold data. **174 objects, 2.15 MB** total.

| prefix | objects | bytes |
|---|---|---|
| `energy/raw_30s/` | 17 | 934,790 |
| `energy/raw_30s_parts_archive/` | 129 | 1,127,722 |
| `energy/hourly/` | 6 | 115,019 |
| `energy/daily/` | **8** | 27,937 |
| `energy/meter/` | **1** | 38,959 |
| `energy/dim_channel/` | 1 | 6,345 |
| `energy/_tmp/` | **0** | 0 |

#### Bryant day-grain: `backfill` executed against real DynamoDB and S3

**3,728 rows, 233 days, 8 months, 0 failed** — `2026-01-02..2026-08-22`, the whole history the
old Lambda ever wrote. This is another never-executed stage now proven end to end.

`records: 235` = 233 from DynamoDB + 2 legacy JSON, and `rows_from_legacy_json: 0` because the
DynamoDB rows superseded them on the dedupe key.

STATE.md recorded 3,712 rows from the instance's earlier local-only run; this is **3,728**
because that run ended at `08-21` and this one includes `08-22` — one more day at 16
component-metrics. Not a discrepancy.

Athena agrees with the numbers recorded before S3 existed: hpheat 2,954 kWh, cooling 2,464,
fan 1,727, eheat 1,277 over 233 days. `reheat`, `fangas`, `gas` and `looppump` are 0.0 on all
233 days — the components this house does not have, written as zeros by `backfill` because,
as the table comment says, we cannot know retroactively which components were disabled.

The batch key gained **read-only DynamoDB on the single table** `bryant-energy-data`
(`Scan`/`Query`/`DescribeTable`, policy v3). `backfill` is a batch stage and belongs on the
Mac; the collector key still has no DynamoDB access at all.

Worth noting against DEVIATIONS #75, which says to run the first backfill with `--dry-run` and
read the diagnostic counters first: **`backfill` has no `--dry-run` option.** The advice cannot
be followed as written. It was safe to skip — the stage writes local-first, regenerates whole
months, and is byte-identical on a re-run — but the spec and the CLI disagree.

#### LG&E meter: mirrored, and the authorisation has lapsed

`energy/meter/` now holds the **4,598 rows** already on the instance's disk — both meters, both
interval series, `2026-08-01..08-20`, no retired ids. Pushed through the production writer
(`greenbutton._upload_months` → `s3io.write_table_atomic`), verified at 4,598 rows in S3.

**But `fetch-greenbutton` cannot fetch anything new: `{SPOOL_DIR}/tokens/lge.json` is gone.**
LG&E rejected the refresh grant at some point after 2026-08-20, and `lge_auth.py:488` cleared
the cache on purpose — "continuing to present a credential the custodian has rejected is how a
registration gets disabled". The data stopping at 08-20 dates it.

**What hid it:** `_job_greenbutton_daily` returns `{"skipped": "not_authorized"}` when the token
file is absent — quietly, by design, so an unauthorised deployment does not accumulate a
failing job every morning. That is the right default and it is also why nobody noticed for
three days. STATE.md's "fetching, all on 2026-08-18" was true when written and is now stale.

**This needs you**: `energycap greenbutton-authorize` is a browser round trip nobody can do on
your behalf.

```bash
ssh -i ~/.ssh/energycap-lightsail.pem ubuntu@13.219.164.226
cd ~/energyDataCapture
docker compose exec energycap energycap greenbutton-authorize
# open the URL it prints, consent, then:
docker compose exec energycap energycap greenbutton-authorize --code "<code>" --state "<state>"
docker compose exec energycap energycap fetch-greenbutton --start 2026-08-15 --end 2026-08-23
```

Worth considering afterwards: a `/healthz` field or an alert for "meter data is more than N
days stale", since a quiet skip is indistinguishable from a working system from the outside.

#### The nightly meter mirror is fixed in code

`greenbutton_fetch.run` now defaults its bucket to `s3io.configured_bucket()`, the same line
`stages/daily.py:838` has always had. Before this, `_job_greenbutton_daily` passed **no bucket
at all**, so `energy/meter/` would have stayed empty forever even with `S3_BUCKET` set — the
exact failure DEVIATIONS #173 records for Bryant, in the one dataset that had not yet hit it.

The asymmetry is deliberate and now pinned by two tests: a **scheduled fetch** mirrors when a
bucket exists; `import-greenbutton` does **not**, because an import is a manual act on a file a
human just downloaded and must not fan out to the archive by surprise.

Making a fetch mirror by default also broke three tests that had been quietly relying on
`run()` never touching S3 — the outbound-network guard caught them attempting a real upload to
the conftest bucket. They now say `bucket=""` explicitly, which is more honest than the
implicit local-only they depended on.

**The instance is still running the old code** (`f786dfb`), so the nightly job will not mirror
until this branch is deployed. The mirror above was done with an explicit `--bucket`.

#### The meter trap, demonstrated rather than asserted

The warning written into `energy_meter`'s comment in Phase 3, now measured in Athena:

| device_id | interval_s | kWh | rows |
|---|---|---|---|
| 1308468 (house) | 900 | **1578.69** | 1,843 |
| 1308468 (house) | 3600 | **1577.74** | 460 |
| 1326254 (barn) | 900 | 478.35 | 1,836 |
| 1326254 (barn) | 3600 | 479.10 | 459 |

The same energy, 0.06% apart, published twice. `SELECT sum(value) FROM energy_meter` returns
**3,113 kWh for 2,056 kWh of actual consumption** — and it looks perfectly reasonable. This is
the single easiest way to be badly wrong with this data, and it is why `interval_s` is in this
table's dedupe key and in its column comment.

One more thing not summed: house + barn. They are separate services.

### The LG&E lapse, hardened · 2026-08-23

Re-authorised by the owner; the fetch caught up (**2,018 new rows → 5,358** in the month file,
mirrored and verified, current through `2026-08-23T14:45Z`). The new grant reports a 24-hour
access token, `refreshable: yes`, `HistoryLength=63072000` — normal, not a short grant.

Three changes so this cannot hide again. Full reasoning in DEVIATIONS #177.

1. **A revocation leaves a breadcrumb.** `LgeTokenCache.clear()` writes `lge-revoked.json`
   (mode 600, `revoked_at` + `reason`, never credential material), only when a token existed;
   `save()` retires it. `_job_greenbutton_daily` then **raises** instead of skipping, reaching
   `job_failed` / `consecutive_failures` / `/healthz` with the fix named. "Never authorised"
   still skips quietly — that was always correct.
2. **`health.meter`** reports the age of the **newest interval held**, not the last successful
   run — a fetch can succeed and return nothing new, which is what a revoked feed looks like
   from the job's side. `METER_STALE_AFTER_DAYS` (default 3). It **reports and never 503s**: the
   lag is the utility's, and compose runs a healthcheck, so a 503 here would be actively
   harmful.
3. **Proactive refresh.** `REFRESH_MARGIN_S` was a flat 300s against a 24-hour token with a
   once-daily job — the job never landed in that window, so it always refreshed *reactively* and
   the refresh token went unexercised for nearly two days. Now
   `max(300s, lifetime × 1/3)` from the token's own issued lifetime: 8h for a 24h token, so a
   daily job refreshes daily. The 300s floor still governs short-lived tokens.

Change 3 is the likely cause and is a **hypothesis, not proof** — the logs that carried the
rejection had rotated. It is the only mechanism on our side that fits, it is harmless if the real
cause was different, and change 1 means the next occurrence arrives with its reason attached.

14 new tests, including that a stale meter still returns HTTP 200, that freshness is measured on
the data rather than the job, and that a re-authorised deployment stops shouting.
