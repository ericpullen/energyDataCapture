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

**PLAN.md's "tens of MB/year" is stale.** It was written for 14 channels. The panel retrofit
took the collector to ~65 rows/cycle ≈ **187k rows/day**. At the ~10 bytes/row the existing
Parquet files actually achieve (`meter/lge-202608.parquet` is 51 KB for 4,398 rows;
`daily/bryant-202608.parquet` is 3.1 KB for ~460), that is **~2 MB/day ≈ 0.7–1.5 GB/year**
for `raw_30s` — about 30× the spec's assumption.

It is still trivial money: **under $1/month** all in (storage in pennies, ~12k
requests/month ≈ $0.06, and Athena scans fractions of a cent per query with partition
projection). But it is why the 40 GB spool disk *needed* S3 rather than merely wanting it,
and it is the number to use when sizing anything downstream.

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
