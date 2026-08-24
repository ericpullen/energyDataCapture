# DEVIATIONS

Places where the implementation departs from, or resolves an ambiguity in, `PLAN.md`.
Required by PLAN.md §5 ("note it in a `DEVIATIONS.md` rather than silently diverging")
and §16.7. Each entry states the spec text, what was built, and why.

Status: **all seven build-order steps of PLAN.md §16 are implemented.** Built:
scaffold — config, logging, timeutil, model, spool, health, s3io, CLI, Docker
(§16.1, #1–#12); the Leviton source, poller and keepalive (§16.2), uploader and
compactor (§16.3), rollup (§16.4) and the `energycap run` process host (#13–#48);
Carrier auth, the Bryant status poller, the daily energy stage and backfill
(§16.5, #49–#75); `config/channel_map.json`, `discover`, `build-dim`, the Glue
tables and comments, and the README with example queries (§16.6, #84–#110); and
this file (§16.7). Every command in `energycap --help` resolves to a real
implementation except `import-greenbutton`, which PLAN.md §13 designs and
deliberately defers and which exits 3 with a message saying so.

**§16's definition of done is only partly met, and the outstanding half is the
half that needs credentials.** All §15 tests pass (see the suite), but nothing
here has ever run against the live Leviton cloud, the live Carrier cloud or real
AWS — this environment has no credentials of any kind, and `tests/conftest.py`
refuses any non-loopback socket. So `docker compose up` on the Mac Mini, the full
manual cycle (`poll`→`upload`→`compact-daily`→`rollup`→`build-dim`→
`create-glue-tables`) against the real bucket, and "README queries return real
data" are all still ahead. There is also no Docker daemon here, so the image has
never been built. §7.3's "verify against a live call" is outstanding; #75 lists
the questions only a live run can settle, and `energycap discover --dump FILE`
(#92) exists to capture the evidence on the first one.

**Read the [Status section at the end of this file](#status--what-is-done-and-what-has-never-been-executed)
before treating anything here as proven.** It is the full, unsoftened list of
what has never been executed and the ordered checks a first live run should
perform. Entries #113–#131 record the final reconciliation pass (two read-only
audits, three parallel fix sessions, and the README/Glue seam between them).

**One locked decision has been overturned.** PLAN.md §2 item 8 and §6.4 locked
"REST polling at 30s + bandwidth keepalive, **not** WebSocket". Live measurement
against the real hubs showed the REST endpoints serve a frozen cache, the owner
authorised building the WebSocket ingester on 2026-08-17, and **#144** records
the deviation with its evidence. Sampling is unchanged — one row set per 30s
cycle, one `ts_utc`, one mapper — and `LEVITON_INGEST=rest` still selects the
original path. The socket itself has **never been connected from this
environment**; it is listed in the status section among the things a first live
run has to prove.

---

## 1. Hourly rollup groups by `hour_start_utc`, not by the naive local hour

**Spec tension.** §10 says the rollup groups by
`(local_hour_start_ts, source, device_id, channel_id, metric)`. §15.3 requires the DST
fall-back day to produce **25** hourly rows and states the ambiguous hour must "bucket by
`ts_utc` without loss". These conflict: on 2026-11-01 the two 01:00 local hours share one
naive wall-clock start, so grouping on a naive local hour start collapses them and yields
24 buckets, losing an hour of data.

**Resolution.** `HOURLY_SCHEMA` carries **both**:

- `hour_start_utc` (aware UTC, first column) — the actual `GROUP BY` key and the lead of
  `HOURLY_SORT_KEY` / `HOURLY_DEDUPE_KEY`. Derived via `timeutil.utc_hour_start`.
- `local_hour_start` (naive local) — human/LLM readable, and deliberately ambiguous on the
  fall-back day, consistent with §2.4's treatment of `ts_local`.

This satisfies §15.3 and CLAUDE.md rule 3 while keeping §10's readable column. **The rollup
stage must group on `hour_start_utc`.** Pinned by `test_model.py::test_hourly_keys_lead_with_the_utc_bucket`
and `test_timeutil.py::test_fall_back_day_sweep_has_25_utc_buckets_but_24_local_hour_labels`.

## 2. `HOURLY_SCHEMA` adds `unit`; `kwh` is nullable

§10's column list omits `unit`. Added so hourly rows are self-describing for Athena and for
an LLM without joining back to raw.

`kwh` is the **only** nullable column in the schema and must be `NULL` — never `0` — for
metrics other than `watts` (§2.5 derives kWh only from watts). A `0` would read as "no
energy used", which is exactly the "load was off" vs "collector was down" confusion
CLAUDE.md rule 5 forbids.

## 3. `energy/meter` filename convention invented

§4 names the `energy/meter/year=YYYY/` prefix but gives no filename, and §13 is explicitly
design-only. `s3io.meter_key` mirrors the `energy/daily` convention:
`{source}-{YYYYMM}.parquet` (e.g. `lge-202601.parquet`) — one regenerable file per month so
an idempotent Green Button import overwrites rather than duplicating. Pinned in
`tests/test_s3io.py`; change it there if the LG&E work wants something else.

## 4. `status.json` carries two extra top-level keys

Beyond §11's example: `started_utc` and `updated_utc`.

`started_utc` is required by the staleness grace period (a watched poller that has never
succeeded is measured from process start, so a fresh container is healthy for its first
3 intervals rather than instantly red). `updated_utc` tells an operator the status writer
itself is alive, not merely that some poller is. All seven §11 sections and all their
example keys are present verbatim.

## 5. `consecutive_failures` / `last_failure_utc` / `last_error` appear on more sections

§11's example shows failure counters only on `leviton` and `bryant_status`. `record_failure`
is generic, so they may also appear on `uploader`, `compactor`, `rollup` and `bryant_daily`.
§6.4 (keepalive backoff) and §7.3 (throttled cadence) both require non-poller conditions to
be recorded in this file, and a per-section counter is the only sane way to express that.
`last_error` is scrubbed, whitespace-collapsed and truncated to 500 chars.

## 6. `/healthz` returns a derived `health` block

The response body is the status document **plus** a computed top-level `health` object
(`ok`, `now_utc`, `started_utc`, `stale_after_intervals`, `checks[]`). It is computed per
request and never persisted to `status.json` (pinned by test). §11 only specifies that
`/healthz` "serves it" and returns non-200 when stale; the block makes *which* check failed
visible instead of leaving an operator to guess.

Boundary semantics: §11 says "older than 3×", so the comparison is strict — age exactly
`3 × interval` is 200, greater is 503.

## 7. Spool table carries columns beyond the eight schema fields

§10 specifies "the 8 schema columns + `uploaded_at NULL`". The table also has
`id INTEGER PRIMARY KEY AUTOINCREMENT`, `local_date TEXT` and `local_hour INTEGER`, plus a
UNIQUE index over the canonical dedupe key used with `INSERT OR IGNORE`.

- `local_date`/`local_hour` are computed **once at insert via `timeutil`** and never
  re-derived in SQL. Deriving them in SQL would mean re-implementing DST inside SQLite's
  `strftime` — precisely the bug class CLAUDE.md's "timeutil is the only place" rule exists
  to prevent.
- The UNIQUE index makes `append()` idempotent, so a repeated `energycap poll --once`
  cannot double-insert. First occurrence wins, matching `dedupe_observations`.

## 8. `SpoolDB.append()` rejects rows the spool must not hold

Raises `ValueError` before any insert (so a bad row never leaves a half-written cycle) for:
day-grain metrics (`kwh_day`/`cost_day_usd` — CLAUDE.md rule 6; the spool feeds `raw_30s`),
`MeterObservation` rows (no `interval_s` column; storing one would silently drop it), a
tz-aware `ts_local`, and a `ts_local` that is not the local wall clock of `ts_utc`.

Not specified by §10, which only says the poller "appends `Observation` rows". Failing loudly
at the boundary beats poisoning a rollup silently.

## 9. `purge()` measures retention on `ts_utc`, not `uploaded_at`

§10 says spool rows are deleted after their hour is verified uploaded "plus a 7-day
retention floor". Age is measured on observation time, so `SPOOL_RETENTION_DAYS=7` means
"keep 7 days of data" rather than "keep rows for 7 days after upload" — the latter would
make retention depend on collector downtime.

## 10. CLI command `fetch-daily` added

§5 says every stage is also a standalone CLI command over an arbitrary date range, and the
~08:30 Bryant daily energy fetch (§7.2) is a scheduled stage — but §5's module list has no
home for it. Added as `fetch-daily` → `stages/daily.py:run`, default window D-2..D-1 to match
the `day1`/`day2` pair in §7.2. If the Bryant work puts it elsewhere, change the one line in
`cli.STAGE_ENTRYPOINTS`.

## 11. Poll-interval floor is applied silently

§6.6 requires a hard 30s floor "even if env var says lower". It is applied in a
`field_validator` as `max(v, 30)` with no warning, because logging inside `Settings`
construction would recurse into `get_settings()`. The running process should log the
effective interval once at startup instead.

`LEVITON_DISCOVERY_INTERVAL_S` (`ge=60`) and `SPOOL_RETENTION_DAYS` (`ge=1`) **raise** rather
than clamp — only the poll intervals clamp, because only they are specified as a floor.

## 12. Test fix: `zip(..., strict=True)` on a list and its own tail

`test_timeutil.py::test_iter_local_hours_is_contiguous_and_indexed` paired `hours` with
`hours[1:]` under `strict=True`, which can never be equal-length and always raised
`ValueError`. Dropped `strict` (with a comment); every adjacent pair is still asserted. A
test-authoring bug, not a spec change.

---

# Collection pipeline (Leviton source, poller, uploader, compactor, rollup, runtime)

## 13. §6.1's "aioleviton handles this" is wrong about the `Origin` header

§6.1 says every request needs `Origin: https://myapp.leviton.com` because "Leviton
appears to fingerprint", and parenthesises "(aioleviton handles this)". It does not:
`aioleviton/base_client.py::_request` sets only `content-type`, `accept`,
`user-agent` and the bare `authorization` header.

**Built.** `LevitonAdapter._ensure_client` creates the `aiohttp.ClientSession` with
default headers `origin: https://myapp.leviton.com` and
`referer: https://myapp.leviton.com/`; aiohttp merges session defaults with
aioleviton's per-request headers, so both survive.

**Why.** Losing the fingerprint header would be an invisible auth failure in
production that no offline test can catch.

## 14. "Cache the full login response" — the library throws the raw body away

§6.1 says to cache the full login response (`id`, `userId`, `ttl`, `created`).
`BaseLevitonClient.login()` returns an `AuthToken` dataclass and discards the
response dict.

**Built.** `_login_payload(auth)` reconstructs `{"id", "userId", "ttl", "created",
"user"}` from the `AuthToken`; that is what is written to
`{SPOOL_DIR}/tokens/leviton.json` at mode 0600. Every field §6.1 names is present.

**Why.** It is the closest thing to "the full response" obtainable through the
library's public surface. **Consequence:** a field Leviton adds later is lost; a
future vendoring of the client should capture the raw body instead. Pinned by
`test_login_caches_the_full_response_at_mode_0600`.

## 15. §6.3 names camelCase API fields; aioleviton exposes snake_case attributes

`power2`→`power_2`, `rmsCurrent2`→`rms_current_2`, `rmsVoltageA`→`rms_voltage_a`,
`frequencyA`→`frequency_a`, `activePower2`→`active_power_2`, `usageType`→`usage_type`,
`branchType`→`branch_type`, `serialNumber`→`serial_number`.

**Built.** `HubReading` / `BreakerReading` / `CtReading.from_model()` read the
snake_case attributes via `getattr` with defaults; the emitted rows follow §6.5's
arithmetic exactly. `getattr` rather than attribute access so the readings stay
constructible from any duck-typed object if aioleviton is vendored or replaced.

**Why.** The spec is authoritative for *what* to emit; the installed library is
authoritative for *how* to read it.

## 16. Breaker pole count and the skip list

§6.5 specifies "2-pole" and "single-pole" arithmetic and is silent on any other
pole count. §6.3 names `NONE`/`NONE-1`/`NONE-2` as the skippable placeholder models.

**Built.** `BreakerReading.is_multi_pole` is `poles >= 2`, not `poles == 2`.
`PLACEHOLDER_BREAKER_MODELS` is exactly `{"NONE", "NONE-1", "NONE-2"}`.

**Why.** The API exposes exactly two poles' worth of fields, so `>= 2` is the
closest available truth; treating a hypothetical 3-pole breaker as single-pole
would silently drop half its measured load. **Flagged:** aioleviton's own
`Breaker.is_smart` *also* excludes `model == "LSBMA"` (a physical CT accessory).
PLAN.md's list is explicit and LSBMA accessories meter real load, so the spec was
followed — but the reference integration disagrees. If a review prefers
aioleviton's behaviour, add `"LSBMA"` to the frozenset and pin it in
`test_every_placeholder_model_spelling_is_recognised`.

## 17. A failed Leviton cycle **raises**; it does not return an empty list

§6.6 says a failed cycle should "log once at WARN, emit no rows, move on".
`sources/base.py` documents `poll()` as *raising* `SourceTransientError` /
`SourceAuthError` so the loop can tell "polled fine, everything was null" from
"the poll failed".

**Built to the base.py contract.** `poll()` raises, having produced exactly zero
rows — no `PollCycle` is created until the response set is complete, so a partial
cycle is structurally impossible. `LevitonSource.consecutive_failures` is exposed
as a property and one WARN is logged per failed *cycle*, not per retry.

**Why.** base.py is the shared contract the generic poller codes against; a source
that swallowed failures would keep the poller's failure counter permanently zero.
Pinned by `test_a_failed_poll_cycle_emits_exactly_zero_rows` and
`test_a_partially_failed_cycle_emits_zero_rows_not_partial_ones`.

## 18. Discovery gets the same retry/re-auth policy as the poll cycle

§6.6 mandates the retry-then-give-up policy only for the poll cycle.

**Built.** `LevitonSource.discover()` runs its fetches through the same `_guarded()`
helper as `poll()`: two retries at 2s/5s, then one re-login on a surviving 401.

**Why.** Discovery hits the same 502-prone gateway, and it runs both at startup
(where a transient 502 would abort `energycap run`) and hourly from a background
task. Not a spec conflict — a place the spec was silent.

## 19. `LevitonAdapter.close()` deliberately never logs out

aioleviton offers `logout()`. Calling it would invalidate the cached token and
force a fresh login on the next container start — exactly the rapid-login
behaviour §6.1 warns against, with no refresh endpoint to soften it. `close()`
releases the aiohttp session only, and is idempotent.

## 20. `status.json` section ownership, and sections created on demand

§11 lists seven sections and does not say who writes them.

**Built.** The generic poller (`stages/poller.py`) owns each source's section
(`leviton`, `bryant_status`): `last_success_utc`, `consecutive_failures`,
`channels_seen`. A **source** never writes its own poller section — both writing it
would double-count `consecutive_failures`. Sources and the scheduler instead create
extra sections on demand: `leviton_keepalive` (§6.4 requires the backoff condition
to be recorded and §11 has no home for it), `leviton_auth`, and `scheduler` (a job
that has been failing hourly has no other home).

**Why.** Consistent with #5, which already sanctions extra sections and keys. None
of the extra sections affect `/healthz`, which only judges `leviton` and
`bryant_status` (see #47).

## 21. Stage entry points take extra keyword-only parameters with defaults

§5/§10 and `cli.STAGE_SIGNATURES` give the stage signatures as
`run(*, start: date, end: date)` and friends. Every stage additionally accepts
injection points — `bucket`, `client`, `spool`, `status`/`store`, `now`,
`poll_interval_s`, `dry_run`, `settings`, `stop`, `sleep`, `monotonic` — all
keyword-only with defaults, and `uploader.run` makes `start`/`end` optional.

**Why.** Explicitly permitted by `cli.py`'s documented contract ("may add further
keyword parameters *with defaults*"); the CLI's calls are unchanged. These are what
let the whole pipeline — including its failure containment, its `status.json`
writes and its idempotency guarantees — be tested offline with no AWS and no
network. `uploader.run()` with no range is additionally the *true* catch-up window
(see #22), which is what the scheduler fires.

Return values are typed beyond §10's silence: `UploadSummary` is a
`collections.abc.Mapping` of loggable fields that also exposes per-hour results,
and the other stages return plain mappings. `cli._run_stage` folds a `Mapping` into
its `stage_ok` line, which is where §10's "row counts logged per stage run" lands.

## 22. An explicit `--start/--end` **narrows** the uploader's work; `--force` widens it

§10 implies `--start/--end` selects what a stage processes.

**Built.** The uploader's default set is every closed local hour that still holds
un-uploaded spool rows. A range filters that set; it does **not** cause
already-uploaded hours to be rewritten unless `force=True` (which requires an
explicit range).

**Why.** The CLI's default upload window is yesterday..today and the compactor
publishes `day-{D}.parquet` at ~01:30. An uploader that rewrote every hour in its
range would re-create parts beside the day file inside the Glue-tabled `raw_30s`
prefix and double-count. `force` is the documented, deliberate repair path for an
uncompacted day.

## 23. The verify gate is an explicit call in the uploader, not a writer default

§10: the uploader must "verify (read back S3 parquet metadata row count == written
count), then mark". `s3io.write_table_atomic` already does that internally when
`verify=True`.

**Built.** The uploader calls `write_table_atomic(..., verify=False)` and then makes
its own `s3io.verify_row_count()` call as the explicit gate before
`mark_uploaded`.

**Why.** The gate that guards marking must be visible in the uploader, not a side
effect of a default argument — and it avoids reading the footer twice.
`write_table_atomic` still verifies the *staged* object before publishing, so
nothing bad reaches the final key either way.

## 24. An idle uploader run does not blank `last_hour_uploaded` / `rows`

§11 shows `uploader: {last_success_utc, last_hour_uploaded, rows}`.

**Built.** A run that uploaded nothing stamps `last_success_utc` only and leaves
`last_hour_uploaded`/`rows` at their previous values. A partial failure records the
failure **and** whatever did land. The uploader also refreshes §11's `spool` block
(`pending_rows`, `oldest_pending_utc`) in the same pass.

**Why.** Zeroing `rows` on an idle run would erase the last real number and make
"nothing to upload" indistinguishable from "uploaded an empty hour" — CLAUDE.md
rule 1 in spirit. §11's `spool` block has no other natural writer besides the
poller (#43), and the uploader is what changes it.

## 25. An hour with no spool rows writes no object; day-grain rows are rejected again

§10/§4 assume a part exists for every hour in the window, and CLAUDE.md rule 6 bans
day-grain rows from `raw_30s`.

**Built.** An hour with zero spool rows writes **no** object rather than an empty
part that would overwrite a good one (a gap stays a gap). And `_assert_not_day_grain()`
raises, naming the offending metrics, before anything is written — in addition to
the spool's append-time rejection (#8) and `model.observations_to_table`'s.

**Why.** The uploader is the boundary where a `kwh_day` row would actually reach
`raw_30s` and poison a rollup, so it checks rather than assumes.

## 26. Every range stage attempts every unit of work, then raises once

§10 does not say what a stage does when one unit of work in its range fails.

**Built, uniformly across the three range stages.** The uploader attempts every
hour, the compactor every day, the rollup every day; each failure is logged and
recorded in `status.json` per unit, and a single exception —
`uploader.UploadFailed`, `compactor.CompactionError`, `rollup.RollupError`, each a
`RuntimeError` carrying the per-unit detail — is raised at the end if any failed.

**Why.** Aborting on the first bad unit would strand the rest of a multi-hour
catch-up or leave yesterday unrolled because a day three days back is unreadable;
returning quietly would hide the failure from the CLI's exit code and from
`status.json`. The failed unit's inputs are left untouched and retried next run.
(The rollup originally aborted at the first bad day; it was aligned with the other
two during integration, and `test_one_bad_day_does_not_strand_the_rest_of_the_range`
pins it.)

## 27. Archived parts are deleted by a sweep, not by the `--start/--end` range

§10: "Parts for day D are deleted by a later compactor run once D is ≥7 days old".

**Built.** Deletion is a **sweep** over every day found under
`energy/raw_30s_parts_archive/`, run at the end of every `compactor.run()`
regardless of the range (one LIST call; disable with `sweep=False`). Days whose
parts were archived during *this* run are excluded from the same run's sweep.

**Why.** The scheduled job only ever asks for D-1, which is never ≥7 days old, so a
range-driven deletion would never fire and the archive would grow forever. The
same-run exclusion means a day first compacted 30 days late still gets a full
safety window rather than zero.

## 28. `ARCHIVE_RETENTION_DAYS` is its own constant, not `SPOOL_RETENTION_DAYS`

§10's 7-day archive window and §14's `SPOOL_RETENTION_DAYS=7` share a number.
They are deliberately **not** wired together: `ARCHIVE_RETENTION_DAYS = 7` is a
module constant in `stages/compactor.py` (overridable per call). Coupling two
unrelated safety windows to one env var makes either one surprising to change.

## 29. The archive count check is the strong form

§10 says archived parts may be deleted once `day-{D}.parquet` "exists and passes the
count check again", without saying what that check is.

**Built.** Read the day file, fold every archived part back in, dedupe, and require
the row count to be **unchanged** — plus a Parquet-footer `verify_row_count` on the
day file. If the count grows, the archive is kept and an ERROR is logged.

**Why.** Comparing the day file's count to itself would prove nothing; this proves
the day file is a superset of exactly what is about to be deleted.

## 30. A byte-equal re-compaction skips the write but still runs the verify gate

§10 relies on deterministic names so "re-runs overwrite".

**Built.** If the computed table equals the existing day file, no S3 PUT/COPY
happens (the ETag is unchanged) — but the footer verify still runs before any part
is touched.

**Why.** Re-running compaction on an already-compacted day must be a true no-op that
nevertheless proves the day file is intact before archiving anything.

## 31. A part that reappears beside the day file triggers another compaction pass

PLAN.md is silent on an uploader writing a part while the compactor runs.

**Built.** After archiving, the partition is re-listed; a part that reappeared is
folded in and the day is compacted again, up to `MAX_PASSES = 3`, then
`CompactionError`.

**Why.** Leaving it in place would double-count its rows — the exact failure §10's
layout resolution exists to prevent. The invariant is checked, not assumed.

## 32. Parts are concatenated **before** the existing day file

§10 says "dedupe … with latest-write wins at the file level" without fixing the
order of parts versus the existing day file. Parts go first, so
first-occurrence-wins dedupe makes a re-uploaded (corrected) part beat the day file
built from its predecessor. In the normal case the rows are identical and the order
is irrelevant.

## 33. A foreign object in a `raw_30s` partition is a hard error

PLAN.md is silent on it. A `part-*.parquet` lacking the canonical 8 columns raises
`CompactionError` for that day — nothing archived, nothing deleted — rather than
being skipped. An unexplained object inside a Glue-tabled prefix is already a query
correctness problem; failing loudly beats compacting around it.

## 34. The rollup carries `unit` through with `min()`, not as a GROUP BY column

§10's column list has no `unit`; #2 added it to `HOURLY_SCHEMA`. It is aggregated as
`min(unit)` rather than grouped on, because grouping would let a (pathological)
mid-hour unit change split one metric into two rows and violate the uniqueness of
`model.HOURLY_DEDUPE_KEY`. `min()` is deterministic, and units are constant per
metric via `model.UNIT_FOR_METRIC` anyway.

## 35. Rollup input rows are deduped before aggregation

§10 says the rollup reads "the S3 (or local) raw files" and is silent on duplicate
input rows. The SQL applies `DISTINCT ON` over the canonical dedupe key
`(ts_utc, source, device_id, channel_id, metric)` before aggregating, ties broken on
`(value, unit)` for determinism.

**Why.** A rollup running while the compactor is mid-flight can see both the hourly
parts and the day file. Counting a sample twice would inflate `sample_count` and
therefore `kwh` — precisely the number the correctness contract is built on.

## 36. A local day with no raw input gets **no** rollup file

§10/§4 assume every day in the range has raw data. A day whose `raw_30s` partition
holds no Parquet logs a WARN (`rollup_no_raw_input`) and writes/deletes nothing.

**Why.** An empty rollup file would assert "this day was measured and had nothing in
it" — the "load was off" vs "collector was down" confusion CLAUDE.md rule 5 forbids.
An absent file is the truthful representation. (A day that *has* raw input but
yields zero rows is still written, so the file always reflects the raw.)

## 37. DuckDB runs single-threaded, in UTC

PLAN.md is silent on DuckDB settings; CLAUDE.md rule 7 requires byte-identical
re-runs. The connection is created with `threads=1` and `TimeZone='UTC'`.

**Why.** Multi-threaded hash aggregation combines partial floating-point sums in a
non-deterministic order, so `avg()` — and hence `kwh` — could differ in the last bit
between runs, and the re-written Parquet would not be byte-identical. Nothing here
is performance-sensitive (tens of MB/year), so serial execution costs nothing.

## 38. The hour spine and the excluded-metric list are registered relations, not SQL literals

§10 specifies the day-grain exclusion; §2.4/CLAUDE.md put all UTC↔local math in
`timeutil`. Neither the metric list nor the hour boundaries appear as literals in
`rollup.sql`: `rollup_excluded_metrics` is registered from `model.DAY_GRAIN_METRICS`
and `rollup_hours` from `timeutil.iter_local_hours` (23 rows on spring-forward, 25
on fall-back), and the query INNER-joins raw samples to the spine on a UTC
half-open range.

**Why.** It keeps the SQL free of timezone arithmetic and keeps the exclusion list
from drifting from the model. The join is INNER, never LEFT, so an unobserved hour
cannot produce a row — a test greps the SQL for `coalesce`/`ifnull`/`generate_series`/
`left join`/`full join`/`fill(` and fails if any appears. The SQL is still one
readable, verbatim-executed file.

## 39. `energycap run` is `energy_capture.runtime`, not `stages/runner`

§5's module list has no home for the process host and the CLI skeleton pointed
`run` at `energy_capture.stages.runner`.

**Built.** The host is `energy_capture.runtime`, and
`cli.STAGE_ENTRYPOINTS["run"] = ("energy_capture.runtime", "run")`.

**Why.** `run` is not a pipeline stage: it *drives* the stages, is not idempotent
over a date range, and takes no `--start/--end`. `tests/test_cli.py` reads the table
rather than hardcoding a module, so it needed no change. `stages/__init__.py`'s
docstring table now says so explicitly.

## 40. The 01:30 firing is one composite job, and it looks back 3 days

§5 lists "daily compaction (~01:30 local for D-1)" and the rollup re-run as separate
scheduled items, both for D-1.

**Built.** One job, `daily_maintenance`, running **upload catch-up → compact →
rollup → spool purge** in that order over D-3..D-1 (`DAILY_LOOKBACK_DAYS = 3`), with
each step's failure caught and the job raising `JobStepError` at the end if any
failed.

**Why.** Ordering is load-bearing: the compactor must run after the uploader has
drained D-1 (a late part otherwise forces a second compaction pass) and the rollup
must see the compacted day file. Separate jobs at 01:30 would have no defined order.
The extra two days are idempotent no-ops (#30 makes re-compaction a true no-op, and
the rollup regenerates whole days) that automatically heal a night the container
spent down — without them, a missed 01:30 means D-1 is never compacted by *any*
later run, because the schedule only ever asks for yesterday.

## 41. The hourly rollup job rebuilds whole local days, not "hour HH-1"

§5 says the hourly rollup runs "~HH:20 for hour HH-1". The rollup stage regenerates
an entire local day per invocation (there is no per-hour output file), so at HH:20
the job rolls `local_date_of(now - 1h)` through `local_date_of(now)`. Just after
local midnight those are two different days, so the window spans both — one extra
idempotent whole-day rebuild rather than silently skipping yesterday's last hour.

## 42. The spool retention purge is wired into the 01:30 job

§10: "Spool rows are deleted only after their hour is verified uploaded, plus a
7-day retention floor (`SPOOL_RETENTION_DAYS=7`) as a second safety net."
`SpoolDB.purge()` implements both interlocks (see #9), but **nothing called it** —
each module was built in parallel and none of them owned the schedule, so
`spool.db` would have grown for the life of the container.

**Built.** `daily_maintenance` runs `purge` as its final step (#40), after the
uploader has marked the hours it landed. Relatedly, `default_jobs(spool=…)` now
threads the running process's `SpoolDB` into both spool-touching jobs, so exactly
one `SpoolDB` exists per process instead of the uploader opening a second
connection set every hour.

**Why.** Both interlocks live in `purge()` itself — an old but un-uploaded row is
un-landed data and is kept — so the schedule only has to call it. Pinned by
`test_the_scheduled_purge_deletes_only_uploaded_rows_past_the_floor`.

## 43. The poller refreshes the `spool` gauge every 300s, not every cycle

§11 shows `spool: {pending_rows, oldest_pending_utc}` with no writer specified.
`SpoolDB.stats()` scans the table, and doing that every 30s is the one place in the
poll loop where "nothing is performance-sensitive" would actually be abused. The
poller refreshes it every `SPOOL_STATUS_INTERVAL_S = 300` and once more during
shutdown; the uploader refreshes it hourly (#24). Five-minute freshness makes "the
uploader has stopped draining the spool" visible long before the hourly uploader
would.

## 44. `channels_seen` is never overwritten with zero

§11 has `leviton: {…, channels_seen: 14}` and does not say what a cycle with no
readable fields should write. `channels_seen` is updated only when the cycle
produced at least one row; a zero-row cycle stamps `last_success_utc` and leaves the
previous value intact. Overwriting 14 with 0 would read as "the panel has no
channels" when the truth is "nothing was readable this cycle". The zero-row fact is
still visible as `rows: 0`.

## 45. Two cycle-failure classes beyond transient and auth

§6.6 covers transient and auth failures. `CycleResult.failure` also carries
`"unexpected"` (a bug *inside* a source — logged with a traceback) and `"spool"`
(the spool write itself failed — logged with the row count that was lost). Both bump
the same consecutive-failure counter and both leave the loop running.

**Why.** §6.6's "never crash the loop" has to hold for a `KeyError` in a mapper too,
and a spool write failure is a different operational problem from a cloud failure
and must be distinguishable in the logs. Neither invents data: a lost cycle is a gap.

## 46. A source that cannot start is kept in the loop

PLAN.md is silent on a source that cannot authenticate at container start.
`Poller.start()` catches a failing `source.start()`, records the failure, logs
`source_start_failed` and **keeps** the source. Both sources re-authenticate inside
`poll()`, and a container that refuses to boot because a third-party cloud is having
a bad minute is worse than one that boots degraded, reports it on `/healthz`, and
heals on the next cycle.

## 47. Pollers that are not running are dropped from the `/healthz` staleness set

PLAN.md §11 does not say what `/healthz` should do about a source that is not
running. `health.py` ships `bryant_status` in its default staleness set, so with only
Leviton built `/healthz` would return 503 forever and the Docker healthcheck would
never pass. `Poller.start()` calls `StatusStore.forget_poller` for every watched
section with no running source — the remedy `health.py`'s own docstring prescribes.

## 48. The image bakes in DuckDB's `httpfs` extension and pins `HOME`

PLAN.md §5 says the container runs one process; it does not discuss DuckDB's
extension bootstrap. `stages/rollup.py` runs `INSTALL httpfs` before reading S3, and
`INSTALL` **fetches over the network on first use** — which would happen at 01:30,
inside a non-root container, on a night the extension repository might be
unreachable.

**Built.** The runtime stage runs `INSTALL httpfs; LOAD httpfs` as the `energycap`
user at build time (extensions are cached under `$HOME/.duckdb`, so it must not run
as root), and sets `HOME=/home/energycap` explicitly rather than relying on Docker's
default — which is also the path `docker-compose.yml`'s optional `~/.aws` mount
already assumed.

**Why.** A build that cannot reach the extension repository fails loudly and
immediately; a 01:30 rollup that cannot fails silently for a night. Same reasoning as
the existing build-time timezone assertion.

---

# Bryant / Carrier (#49–#75)

## 49. The auth ladder has one more rung than §7.1 describes

§7.1: "Use `grant_type=refresh_token` to renew; only fall back to password grant
when refresh fails."

**Built.** A fixed, bounded ladder in `sources/carrier_auth.py`: in-memory token →
on-disk cache → refresh grant → password grant. On a 401/403 the refresh grant runs
(itself falling back to the password grant once if the refresh is rejected); if the
*refreshed* token is also rejected, one further password grant runs and then the call
gives up. At most 3 GraphQL attempts and 2–3 token requests per `query()`.

**Why.** §7.1 describes two rungs but the 30s cadence makes "what happens when the
renewed token is also dead" a real case, not a hypothetical. The ladder is written as
a fixed sequence rather than a retry loop specifically so it cannot spin: the failure
mode to avoid is a poller that turns a Carrier outage into a token-endpoint flood.

## 50. 403 is treated as an auth failure, not a transient error

§7.1 and §6.6 speak only of 401.

**Built.** `AUTH_HTTP_STATUSES = {401, 403}`.

**Why.** Mirrors `dahlb/carrier-api`'s own `_AUTH_HTTP_STATUSES`; Carrier's gateway
returns 403 for a dead token. Treating it as transient would retry the same dead
token three times and then emit a gap, instead of renewing and succeeding.

## 51. An HTTP 200 carrying a GraphQL auth error is routed into the auth ladder

PLAN.md does not anticipate 200-with-error, which GraphQL APIs routinely do.

**Built.** A 200 whose `errors[].extensions.code` is `UNAUTHENTICATED` /
`UNAUTHORIZED` / `FORBIDDEN` (or whose message reads that way) raises through the
auth path. Every other `errors` payload becomes `CarrierGraphQLError`, which is a
`SourceTransientError` and deliberately **not** a `SourceAuthError`.

**Why.** Otherwise an expired token surfaces as a permanent data error and the
collector never re-authenticates. The converse matters just as much: a malformed
query must not trigger a re-login ladder, so the two are separated by the error code
rather than by "did the request fail".

## 52. Anti-spin floors PLAN.md is silent about

**Built.** (a) `MIN_RENEW_INTERVAL_S = 30` — a floor between *expiry-driven*
renewals; a 401 bypasses it, because a 401 is evidence rather than a guess. (b) The
300s refresh margin is capped at half the token's own lifetime.

**Why.** Both exist so that a skewed container clock, or a surprisingly short
`expires_in`, cannot make every token look expired on arrival and turn a 30s poller
into a token-endpoint client. Neither changes behaviour in the normal case.

## 53. A token with no `expires_in` and no decodable JWT `exp` is assumed to live 900s

§7.1: "Track expiry from `expires_in` (fall back to decoding the JWT `exp` if
absent)." It stops there.

**Built.** A third fallback: assume 900s and log a WARN naming the condition. The
JWT decode is a dependency-free, deliberately *unverified* base64url read of the
payload segment, defensive about padding, non-UTF-8 bytes, non-object payloads and
non-numeric/bool/negative `exp` — it returns `None` rather than raising.

**Why.** Refusing to use a token we were just handed would be strictly worse than
using it and re-authenticating early. Signature verification is pointless here: we
are reading our own token's expiry hint, not trusting a third party's assertion.

## 54. 429 policy: short waits are slept off, long ones open a fail-fast window

§7.3 says only "honor `Retry-After`/back off".

**Built.** A `Retry-After` of ≤5s is slept inline and the call retried. Anything
longer opens a backoff window, raises `CarrierRateLimitError` **immediately**, and
subsequent calls fail fast without touching the network until the window closes.
`Retry-After` is parsed in both delta-seconds and HTTP-date forms, capped at 1h; a
missing header assumes 60s.

**Why.** Blocking a poll cycle for two minutes would stall the whole asyncio loop and
delay Leviton too. Failing fast produces a gap, which is the correct outcome under
cardinal rule 1 — and the effective cadence is recorded in `status.json` exactly as
§7.3 asks.

## 55. The `Mobile-App-Brand: carrier` header is sent alongside §7.1's Origin/Referer

§7.1 names `Origin` and `Referer` only.

**Built.** All three, matching `~/code/bryantDataCollector`'s working code
(`MOBILE_APP_BRAND`). `dahlb/carrier-api` sends none of the three and works for many
users, so none of them is likely load-bearing — but the old collector's exact header
set is the one empirically known to work on *this* account.

## 56. The token cache stores more than the tokens, and a foreign cache is ignored

§7.1 requires the cache at `/data/tokens/carrier.json` mode 600 but does not specify
its contents.

**Built.** Also stores `username`, `token_type`, `scope` and the expiry source; a
cache belonging to a different `CARRIER_USERNAME` is ignored rather than used. The
file is rewritten on **every** renewal because Okta rotates the refresh token. A
cache *write* failure (read-only `/data`) logs a WARN and keeps the in-memory token
rather than failing the poll.

**Why.** Rotating tokens mean a stale cache file is a dead credential, so "rewrite
always" is the only safe rule. Ignoring a foreign cache prevents an account switch
from producing a confusing 400 loop.

## 57. Exception messages are scrubbed, not just log records

CLAUDE.md rule 8 covers logs.

**Built.** Every exception this module raises is built from status codes and OAuth
`error` codes only — never a response body — and is passed through
`logging.scrub_text()` at construction. `CarrierToken.__repr__` redacts both tokens.

**Why.** Stricter than the literal rule because exception strings reach
`status.json` via `StatusStore.record_failure`, and an upstream that echoes a token
back in `error_description` would otherwise write it to disk. Pinned by
`test_carrier_auth.py::test_secrets_do_not_reach_exception_messages_or_status_fields`.

## 58. `stage` is taken from `odu.opstat` only, with no fallback to `idu.opstat`

§7.3's table says `stage` ← "odu/idu operating stage" without choosing.

**Built.** The outdoor unit, always. `idu.opstat` is parsed onto `SystemStatus` and
left unmapped. Swappable via the `STAGE_SOURCE` module constant.

**Why.** The outdoor unit is the compressor — the thing drawing the power the Leviton
CTs measure — so `stage` correlates with watts only if it means the ODU. A fallback
would silently make one metric mean two different physical units depending on which
field happened to be populated, which is worse than a gap.

## 59. A numeric `odu.opstat` emits NO row — SETTLED 2026-08-17, superseded by #132

§7.3 specifies an enum encoding for `stage`.

**Originally built.** If `opstat` parsed as a number it was dropped, counted
(`numeric_stage_samples`) and WARNed as `bryant_enum_numeric`.

**Why.** On a variable-capacity outdoor unit `opstat` is a 0–100 capacity percentage
string, not a word (proven by `ha_carrier`'s own `.isdigit()` and `float()` branches).
`45` is not a stage, and emitting it under `metric="stage"` would put two
incompatible units in one column. This entry called that the highest-risk open item
in the Bryant work: *if* this house turned out to be variable-capacity, `stage` would
be permanently empty and a new metric (`stage_pct`, unit `pct`) would have to be added
to `model.UNIT_FOR_METRIC`.

**SETTLED — the risk landed.** `energycap discover` and `energycap poll --once` ran
against the real Carrier cloud on **2026-08-17** (serial `4022W200213`,
`getInfinityStatus`, `isDisconnected: false`). The response says
`odu.type = "gs3ngiphp"` — Greenspeed, i.e. **variable capacity** — and
`odu.opstat = "35"`, a capacity percentage. Every poll cycle logged the predicted
`bryant_enum_numeric` WARNING and emitted no compressor row at all, so the single most
useful HVAC signal (the one that correlates with watts, PLAN.md §1's marquee query)
was a permanent gap on the only system this pipeline collects.

The pre-authorised change was therefore made, verbatim as this entry specified it:
`stage_pct` (unit `pct`, channel `system`) is now in `model.UNIT_FOR_METRIC` and a
numeric `odu.opstat` emits it. **`STAGE_CODES` was not renumbered, retired or frozen**
— the enum path is untouched and both renderings stay possible for the life of the
archive, because the archive outlives the hardware. The live payload is committed as
`tests/fixtures/bryant/status_live_capture.json` (verbatim) and
`status_varcap.json` (trimmed), and is replayed through the source in
`tests/test_bryant_live_capture.py`. Details in **#132**–**#138**; the log-level and
counter consequences in **#134**–**#136**. #75.1 is answered.

## 60. `outdoor_temp_f` is emitted only when `cfgem == "F"`

§7.3's table gives `outdoor_temp_f` ← system `oat`, unit degF, unconditionally.

**Built.** Emitted verbatim when `cfgem == "F"`; otherwise dropped with
`bryant_outdoor_temp_unit_unverified`.

**Why.** The claim that `oat` is always Fahrenheit regardless of `cfgem` is an
`ha_carrier` maintainer's code comment, not documentation. This system reports `F`,
so no data is lost today; building logic on an undocumented claim would be a silent
lie the day it stopped holding.

## 61. Celsius zone readings are converted to degF rather than emitted as degC

§7.3 names the metrics `indoor_temp_f` / `setpoint_*_f` and §3 fixes their unit.

**Built.** If `cfgem == "C"`, zone temperatures are converted to °F. If `cfgem` is
absent or unrecognised, **no temperature row is emitted at all** — humidity, fan,
mode and stage still are.

**Why.** The temperature unit is data, not a constant, and the two honest options are
"convert" or "emit nothing". Emitting a Celsius number under `unit="degF"` would be a
silent lie; emitting `unit="degC"` under a metric named `_f` would break the
metric→unit invariant `model.py` enforces.

## 62. An unknown enum string warns once per distinct value, not once per cycle

§7.3: "Unknown enum string from the API → log WARN, emit no row."

**Built.** No row, always — but the WARN fires once per distinct `(metric, value)`
per process. The running total stays visible in `status.json`
(`unknown_enum_values`) and on `source.unknown_enum_counts`.

**Why.** The condition is permanent until a human edits an append-only table, so a
30s loop would write 2,880 identical lines a day and bury everything else. The
gap-emitting behaviour §7.3 actually cares about is unchanged.

## 63. `isDisconnected: true` emits zero rows and counts as a SUCCESSFUL cycle

PLAN.md does not mention the flag.

**Built.** Zero rows, not a failure. Logged once per transition
(`bryant_system_disconnected` / `bryant_system_reconnected`) and exposed as
`disconnected` in `status.json`.

**Why.** The payload is the cloud's stale cache of a thermostat that is not talking to
it; archiving it at this cycle's `ts_utc` would be fabrication (cardinal rule 1).
Equally, it is not a *poll* failure — the poll worked — so counting it as one would
make `/healthz` red for a thermostat outage the collector is handling correctly.

## 64. The GraphQL operation self-switches once, at runtime

§7.3 left the status query as the open research task and names no operation.

**Built.** Two queries generated from one shared selection set (so they cannot
drift): the cheap per-serial `infinityStatus(serial:)`, and the reference-proven
`getInfinitySystems($userName)` filtered client-side on `profile.serial`. The source
falls back **once**, automatically, only on a GraphQL-level rejection or a null
result — never on 5xx/429/401 — and pins the fallback only after it actually
succeeds. Disable with `allow_fallback=False`.

**Why.** `infinityStatus(serial:)` is present in the maintainer-committed
introspection dump but is executed by nobody in the wild; it may be unimplemented or
permission-gated. It avoids dragging the enormous `config` blob over the wire every
30s, so it is worth trying — but not worth a broken collector, hence the automatic
fallback. Restricting the trigger to GraphQL-level errors stops a transport blip from
permanently downgrading the query.

## 65. No periodic re-discovery task for Bryant

`LevitonSource` re-discovers every `LEVITON_DISCOVERY_INTERVAL_S` (§6.2).

**Built.** `background_tasks()` returns `()`. Zones are re-read from every poll
response for free; a changed zone set is logged as `bryant_zone_set_changed`.

**Why.** §6.2's interval exists because Leviton breakers are added over time and
appear only in a separate discovery call. Bryant's zones ride along in the status
payload, so a discovery task would only double the request rate against a cloud whose
tolerance for 30s polling is entirely unproven.

## 66. Zone rows require `enabled == "on"`, exactly

§7.4: "enumerate zones from the status response, `channel_id=zone_1..n`."

**Built.** A zone emits rows only when `zone["enabled"] == "on"` (case-insensitive on
the word, but a strict positive test). `channel_id` comes from `zone["id"]` coerced
with `str()`, never from list position. `fan` is emitted on `zone_{n}` only — §7.3
allowed "zone_{n} or system", and the API has no system-level fan state.

**Why.** A disabled zone still returns plausible-looking garbage: in the golden
capture zone 2 reports `rh: "34"`, `htsp: "60.0"`, `clsp: "80.0"` and even
`zoneconditioning: "active_heat"`, with only `rt` honestly `"None"`. Filtering on
nullness alone would fabricate 7 phantom zones × 4 metrics × 2,880 polls ≈ **80,000
invented rows a day** — precisely the failure cardinal rule 1 forbids. Position is
avoided because the websocket path delivers `id` as an int while REST delivers a
string.

## 67. Fields are requested but deliberately left unmapped

**Built.** The query asks for `statpress`, `zoneconditioning`, `damperposition`,
`occupancy`, `humid`, `hold`, `currentActivity`, `odu.opmode` and the ODU compressor
telemetry, and maps none of them to metrics. `humlvl` / `filtrlvl` / `uvlvl` are
requested and unmapped on purpose — they are consumable *used* percentages, not
humidity/filter/UV readings; room humidity is zone `rh` and nothing else.

**Why.** Each would need a new entry in `model.UNIT_FOR_METRIC`, and several
(`damperposition`, `occupancy`, the ODU telemetry) are schema-present but never
requested by any reference, so there is zero evidence they are populated rather than
`"None"`. Requesting them means the first live response is a complete artefact to
inspect before anyone commits a schema — cheap now, impossible later.

## 68. A missing Carrier credential surfaces as `SourceAuthError`, not `RuntimeError`

**Built.** `BryantStatusSource` construction never raises; configuration is resolved
inside `poll()`, and a missing `CARRIER_USERNAME`/`CARRIER_PASSWORD` becomes a
`SourceAuthError` rather than the bare `RuntimeError` `Settings.require` raises.

**Why.** A `RuntimeError` would hit the poll loop's "a bug inside a source" branch and
log a traceback every 30 seconds, when a missing credential is an ordinary auth
condition `status.json` and `/healthz` already render properly. Consistent with #46:
the container boots degraded and heals.

## 69. A daily-energy component emits rows only when `enabled` is explicitly `true`

§7.2: "Skip components whose `energyConfig.<name>.enabled` is false."

**Built.** The inverse test. A component absent from `energyConfig`, or one whose
`enabled` value cannot be interpreted, is treated as **not** enabled and logged
(`daily_component_absent_from_config` / `daily_component_enabled_unreadable`). `true`,
`"true"`/`"false"` and `1`/`0` are all accepted; anything else is not-enabled + WARN.

**Why.** The period object carries a tempting `0` for every disabled component, so
"emit unless explicitly false" is how a phantom component's zeros get into the
archive. An *enabled* component reporting 0 does emit the zero — that is a
measurement, not a gap. Both halves are pinned: a test asserts the fixture really
does offer those zeros, so a mapper that dropped the config check would be caught.

## 70. A missing `energyConfig` (or `energyPeriods`) fails the whole run

§7.2 is silent on the response being malformed.

**Built.** `DailyFetchError`; nothing is written.

**Why.** Without the config, a structurally disabled component is indistinguishable
from a measured zero, and writing the periods anyway would fabricate rows for
hardware this house does not have. Failing loudly and writing nothing leaves the
existing monthly file untouched and the day a gap, which is recoverable; a half-real
month is not.

## 71. `--start/--end` filters the daily fetch, it does not query a range

§10: every stage takes `--start/--end` over an arbitrary local date range.

**Built.** The Carrier cloud serves only `day1`/`day2` — there is no date input and no
historical period — so the range **filters** those two dates. Their local dates come
from `PERIOD_OFFSET_DAYS = {day1: 1, day2: 2}` against the local date of `now`
(injectable), matching the old collector. Aggregate `month1`/`year1` periods are
ignored: they are spans, not days. Any date in the range that neither period covers
logs `daily_range_unavailable` at WARN naming `energycap backfill`, and emits
nothing.

**Why.** §10's contract cannot mean "query that range" against an API with no range
parameter. Idempotency and determinism over the range are unaffected, and the
CLI's default window (D-2..D-1) and `BRYANT_DAILY_LOOKBACK_DAYS=2` line up exactly
with day2..day1, so the scheduled 08:30 firing never trips the warning.

## 72. `stages/daily.py` is synchronous and does not share the poller's Carrier stack

`carrier_auth`'s design assumes one shared `CarrierAuth` + client per process.

**Built.** `run()` is sync (runtime hands it to `asyncio.to_thread`, so a coroutine
entry point would break the scheduler contract) and builds a short-lived stack per
invocation, closed in a `finally`.

**Why.** An `asyncio.Lock` and an `httpx` pool are bound to the loop that created
them, and this stage runs on its own. The cost is bounded: once a day, and the 0600
token cache at `{SPOOL_DIR}/tokens/carrier.json` means a warm cache costs zero Okta
round trips — one extra token request only if the cached token has already expired.

## 73. Both `energy/daily` writers regenerate the whole month, and the last writer wins the contested cell

§8: backfill "regenerates the affected `energy/daily/` monthly files completely".
§10: "dedupe on the standard key with **latest-write wins** at the file level".

**Built.** One convention shared by `stages/daily.py` (§7.2) and `stages/backfill.py`
(§8), because they write the same object:

1. the key comes from `s3io.daily_key(month_start, source=SOURCE_BRYANT)` — neither
   module builds a key by hand;
2. read the existing monthly object back;
3. concatenate **this stage's own rows first**, then every row the existing object
   held;
4. dedupe first-occurrence-wins on `model.DEDUPE_KEY`, sort, `write_table_atomic`.

Backfill's internal precedence is therefore DynamoDB → legacy JSON → existing object,
satisfying §8's "prefer DynamoDB". Across stages, whichever ran last owns any cell
both claim — which is exactly §10's file-level latest-write-wins.

**Why.** A literal reading of "regenerate completely" — write only what this range
produced — would mean `--start 2026-01-03 --end 2026-01-03` **deletes the rest of
January**, including everything the live daily fetch had written. Because neither
stage ever writes a subset of a month, neither can half-overwrite the other. Pinned
end-to-end by `test_integration.py`'s `test_backfill_then_daily_leaves_the_month_complete`
and `test_daily_then_backfill_leaves_the_month_complete`, which run both stages
against one moto bucket in both orders over a month containing backfill-only days,
fetch-only days and one contested day.

**Known limit:** neither stage defends against the two running *concurrently* against
the same month — the second read-modify-write would clobber the first. They are a
manual command and an 08:30 job, so this is not a real schedule, but it is not
enforced either.

## 74. Backfill details PLAN.md §8 does not settle

All of the following emit **no row** (a gap) rather than a zero: an absent attribute,
`None`, a non-numeric or non-finite value, `True`/`False`. Each logs its own WARN.
A numeric *string* is accepted via `Decimal` but logged as `backfill_string_number`,
since production values are `Decimal`.

- **Recorded zeros are written as recorded.** §8 says so explicitly, and it is the
  deliberate inversion of §7.2's skip-disabled rule: historically we cannot know
  whether a component was disabled, and `0` is what the API said.
- **`Decimal` → `float` precision loss logs `backfill_precision_loss` and still emits
  the row.** §8 says the conversion must not lose precision *silently*; dropping the
  row would manufacture a gap where a real measurement exists (cardinal rule 1). Can
  only fire above ~17 significant digits, which this API does not produce.
- **An unmapped attribute logs `backfill_unknown_attribute`.** Not required by §8.
  It is the tripwire that makes a future Carrier field rename visible instead of
  silently dropping a component.
- **A missing `serial_number` falls back to `CARRIER_SERIAL` with a WARN**; a missing
  or unparseable `date` **skips** the item with a WARN. The measurement is real even
  when provenance is missing, but a row cannot be placed in time without its date.
- **The `Scan` uses `ConsistentRead=True`.** Still only `dynamodb:Scan` — the whole
  IAM requirement, pinned by a botocore spy asserting the operation set is exactly
  `{"Scan"}`. The table is tiny and a backfill that quietly read a stale replica
  would be a bad thing to discover months later.
- **The DynamoDB client is pinned to `us-east-1`** (`DYNAMODB_REGION`, a §2.6/§8
  confirmed value) rather than inheriting `AWS_REGION`. `AWS_REGION` governs where
  the bucket lives; if it changed, the table would not move, and a wrong-region
  client returns `ResourceNotFoundException` — which reads like "the table is empty".
- **A nonzero `gasKwh` logs a WARN in backfill too.** §7.2 mandates it for the live
  fetch and §8 does not repeat it, but the caveat (the field says kWh and gas
  probably is not kWh) applies identically to historical rows. The value is recorded
  verbatim as `metric=kwh_day`; no conversion is guessed.
- **`BRYANT_LEGACY_JSON_PATH` is a new env var, absent from §14's table.** It is a
  real `Settings` field (`bryant_legacy_json_path`) and is in `.env.example`, like
  every other knob. Unset it defaults to `~/code/bryantDataCollector/energy_data`
  (a directory → `energy_*.json`, or a single file); an explicit `legacy_path=`
  argument always wins. The default lives in the stage, not in `Settings`, because
  the path is meaningless inside the container — this import runs on the Mac.
- **`status.json` gains `backfill` and `bryant_auth` sections**, which §11 does not
  list, and `bryant_daily` carries more than §11's `last_success_utc`. Consistent
  with #5 and #20 (sections are created on demand); `/healthz` ignores them. The
  Bryant *status* source writes `bryant_auth` and never `bryant_status` — §11 gives
  that section to `stages/poller.py`, and two writers would double-count
  `consecutive_failures`. It is written only when a counter changes, not on every
  cycle, because a 30s unconditional write would rewrite the file 2,880×/day.

## 75. §7.3's "verify against a live call" — PARTLY DONE (first live call 2026-08-17)

§7.3: "Read `carrier-api`'s source for the exact status query/fields before writing
this module; **verify against a live call**." §16's definition of done requires a full
manual cycle against the real bucket.

**Status update, 2026-08-17.** `energycap discover` and `energycap poll --once` have
now run against the real Leviton and Carrier clouds, and the `getInfinityStatus`
response is committed (`tests/fixtures/bryant/status_live_capture.json`, verbatim and
untrimmed; the serial appears only in the request variables and is redacted). Items
**1, 2, 6 and 10** below are answered by it and are struck through with what was
observed. Nothing downstream of the spool has touched AWS: items 3, 4, 5, 7, 8 and 9
are still entirely open, and one response cannot answer a question about behaviour over
24 hours or a season.

**Built.** Everything Bryant-related is tested exclusively against recorded fixtures,
`httpx.MockTransport` and moto. There are no Carrier credentials and no AWS
credentials in the build environment, so no live call was possible. The suite is now
*structurally* offline: `tests/conftest.py`'s autouse `no_outbound_network` fixture
replaces `socket.connect`/`connect_ex`/`create_connection` with versions that refuse
any non-loopback address. That guard was not decoration — it immediately caught a
scheduler test that had begun driving the real daily stage and was opening a live TLS
connection to `sso.carrier.com` (it looked like it passed offline only because Okta
answered `invalid_grant`).

**Outstanding, in priority order — a first live run must answer these:**

1. ~~**What does `odu.opstat` return on this system?**~~ **ANSWERED — a number.**
   `odu.type = "gs3ngiphp"` (Greenspeed = variable capacity) and `odu.opstat = "35"`,
   a 0–100 capacity percentage. `stage` emitted nothing and `bryant_enum_numeric`
   fired on every cycle exactly as predicted, so the pre-authorised `stage_pct` metric
   was built (#59, #132). The first response is committed as a fixture and replayed in
   `tests/test_bryant_live_capture.py`; the `stage` encoding was **not** frozen and
   **not** renumbered. `numeric_stage_samples` keeps its meaning, but the ongoing
   signal to watch is now `stage_pct_rows` / `stage_pct_out_of_range` (#134, #135).
2. ~~**Does `infinityStatus(serial:)` resolve at all?**~~ **ANSWERED — yes.** The
   per-serial query resolved on the first attempt; `status.json` reports
   `operation: getInfinityStatus` and no `bryant_status_query_fallback` was logged, so
   the #64 fallback has still never run against the real cloud.
3. **Does the cloud tolerate 30s polling?** Nothing in the ecosystem polls faster
   than 30 *minutes* (`ha_carrier`'s `DEFAULT_UPDATE_INTERVAL_MINUTES = 30`), and
   neither reference handles 429 at all. Watch `throttle_events` / `retry_after_s`
   for 24h and raise `BRYANT_POLL_INTERVAL_S` if needed.
4. **Does the payload even change every 30s?** The golden capture shows `localTime`
   lagging `utcTime` by ~4 minutes, suggesting server-side caching. `server_utc_time`
   is parsed and logged at DEBUG on every `bryant_poll_ok` precisely so this can be
   diffed across cycles.
5. **The real domain of `status.mode`.** Only `"heat"` is confirmed-observed
   anywhere; the reference's own list (`gasheat`/`electric`/`hpheat`/`dehumidify`) is
   dead code contradicted by its own golden capture. Expect `bryant_enum_unknown`
   across a season and **append, never renumber**.
6. ~~**How many zones are actually enabled**, before writing `channel_map.json`.~~
   **ANSWERED — one of eight.** The payload reports eight zones and only `zones[0]`
   (`id: "1"`) is `enabled: "on"`; the other seven are phantoms that still carry a
   plausible `rh`, `htsp` and `clsp` with a null `rt`. So `zone_1` is the only zone
   channel that produces rows, and the strict `enabled == "on"` test of #66 is what
   stops seven phantom zones' worth of fabricated readings a cycle. Pinned by
   `test_the_seven_phantom_zones_contribute_nothing`.
7. **Auth specifics:** that the ROPC password grant still works for client
   `0oa1ce7hwjuZbfOMB4x7`; the real `expires_in` and refresh-token lifetime (what the
   300s margin should be tuned against); whether Okta really rotates the refresh
   token; whether `Origin`/`Referer`/`Mobile-App-Brand` are load-bearing.
8. **Daily-energy shape:** that `getInfinityEnergy` still resolves with this field
   set; whether `energyPeriods` values are JSON numbers or strings (both accepted);
   whether `energyConfig.<name>.enabled` is a boolean or a string; and whether this
   system's `energyConfig.gas` is really disabled.
9. **Backfill against the real account:** the table's actual contents and attribute
   types, that the real items' 16 attributes are exactly these 16 spellings, and that
   `dynamodb:Scan` alone suffices. Run the first backfill with `--dry-run` and read
   `backfill_dynamodb_scanned`, `backfill_unknown_attribute`,
   `backfill_precision_loss` and `backfill_gas_kwh_nonzero` before writing anything.
   Only `energy_2026_01.json` exists on this machine; whether other legacy files
   exist elsewhere is unconfirmed.
10. **Units to confirm before they reach a Glue comment:** ~~`cfgem == "F"`~~
    **ANSWERED — `cfgem` is `"F"`.** The temperature-unit question is closed for this
    house: zone temperatures are Fahrenheit as received, the Celsius conversion path
    (#61) is dead code here, and `outdoor_temp_f` is emitted (#60) without relying on
    the unverified "`oat` is always Fahrenheit" claim, which is therefore **still
    unconfirmed** — as are `statpress` in inH₂O (`ha_carrier`'s authority only) and
    `blwrpm` in RPM (inferred from the field name). A system reporting `C` would still
    exercise untested-against-reality behaviour.

---

# Step 6–7: the semantic layer, Glue, the README, and four defect fixes

Entries #76–#118 come from the final workflow: `channel_map.json` + `discover` +
`build-dim`, the Glue tables, the README, the fixes for five confirmed defects, and the
integration pass that wired them together.

## 76. The credential-rejection backoff guards *every* grant, not only rejection-driven ones

`GRANT_FAILURE_BACKOFF_S = 60.0` (PLAN.md §6.6's "back off 60s and keep trying") is
checked at the top of `CarrierAuth._renew_locked`, so it covers both entry points —
`token()`'s expiry path and `reauthenticate()`'s rejection path.

The narrower reading ("floor the rejection-driven grant") would not have fixed the defect
it exists for. A failed grant leaves `self._token = None`, so the *next* call arrives
through `token()` with `reason="expiry"` — the expiry path is precisely the one that
spins. Guarding both leaves one place to reason about, and the window can only ever be
armed by an Okta rejection anyway. Pinned by
`test_a_rejected_password_grant_backs_off_instead_of_retrying_every_poll`.

## 77. A third anti-spin constant PLAN.md does not name: `AUTH_LADDER_BACKOFF_S`

`MIN_PASSWORD_GRANT_INTERVAL_S = 900.0` alone still leaves one *refresh* grant on every
poll cycle forever (2,880/day), because `CarrierGraphQLClient` re-climbs its ladder from
scratch on every call. So exhausting the ladder opens an `AUTH_LADDER_BACKOFF_S = 900.0`
window during which `query()` makes exactly one attempt with the token it already holds
and then fails, buying no tokens at all.

It is armed **only** when the full ladder was actually climbed and exhausted, cleared by
any success, and never re-armed by a fail-fast failure — so a persistent rejection still
gets a genuine full retry every 900s rather than being permanently disarmed. Pinned by
`test_a_persistent_403_does_not_re_climb_the_ladder_every_poll`,
`test_inside_the_auth_backoff_a_call_costs_one_attempt_and_no_grant` and
`test_a_success_clears_the_auth_backoff`. Extends #52.

## 78. A field-level denial with no fallback is reported as a *data* error

`CarrierAuthError` gained a keyword-only `errors` sequence: populated for a
200-with-`errors` body, empty for a transport 401/403. `bryant._fetch_status` treats a
`CarrierAuthError` carrying a non-empty `errors` array as the gateway refusing the
**field** (the likeliest way `infinityStatus(serial:)` fails) and falls back to
`getInfinitySystems`; a bare 401/403 is re-raised untouched into the auth ladder.

Consequence, on one path: with `allow_fallback=False`, a field-level denial now raises
`CarrierGraphQLError` (a `SourceTransientError`, chained via `from`) rather than the
`CarrierAuthError` it arrived as. That is the logical end of the distinction — the field
was refused, our token was not — but it does change the exception class a caller sees.
PLAN.md and #64 are silent on it. Pinned by
`test_a_field_level_denial_with_no_fallback_is_a_data_error`.

## 79. None of the new backoffs add a key to `status.json`

Deliberate. A remaining-seconds field changes on every cycle, which would defeat
`bryant._publish_status`'s comparable-counters short-circuit (#74's "not written on every
cycle") and reintroduce 2,880 file rewrites a day — the exact failure mode #80 sits next
to. The conditions are visible instead as structured log events
(`carrier_grant_backoff`, `carrier_password_grant_rejected`,
`carrier_password_grant_floored`, `carrier_auth_ladder_backoff`) and through the existing
`password_grants` / `refresh_grants` counters, which already reach `status.json`.

What *does* change in `status.json` on a floored cycle is `last_error`'s text: instead of
the Okta code it reads "carrier <reason>: credentials were rejected, Ns of backoff
remaining". The real cause stays greppable in the logs. Both strings are built from
status/OAuth codes only and pass through `scrub_text`.

## 80. `bryant._publish_status` needs an explicit "the last write was a failure" flag

A failed cycle stamps exactly the same comparable counters as a successful one, so the
first success after any blip compared equal and was skipped: `record_success` never ran
and `status.json` reported the Carrier transport as permanently failing, with a frozen
`last_success_utc` and a stale `last_error`, until some unrelated counter moved.
`_status_failed` (set from `error is not None` *after* the comparison) forces that first
success through. `_consecutive_failures` cannot stand in for it — `poll()` zeroes it
before calling `_publish_status` on the success path.

## 81. `GRANT_FAILURE_BACKOFF_S` is armed only by `CarrierAuthError`, unlike Leviton's login backoff

`sources/leviton.py` arms `LOGIN_FAILURE_BACKOFF_S` on `SourceTransientError` too. The two
modules now read slightly differently on purpose: Carrier's transient and 429 paths
already have their own retry and `ThrottleState` policies (#54), Leviton's login does not.

## 82. The Bryant daily job takes **one** clock read, at fetch time; the scheduler's slot only detects skew

§7.2 / #71 date day1/day2 from "the local date of `now`". `runtime._job_bryant_daily` used
the scheduler's firing slot to build its `--start/--end` window while `stages/daily.py`
re-read the wall clock to date day1/day2 — two independent reads. Straddling local
midnight (a suspended host, an NTP step), the freshest day of energy was fetched, dated by
one read, and filtered straight back out by a window built from the other: the job
returned `{"rows": 1}` with a green `job_ok`. Silent loss that looks healthy.

Now the job takes exactly one fresh read (`fetch_at`) after the import guards and
immediately before the fetch, uses it for both the window and the stage's dating (passed
down as `now=fetch_at`), and keeps the slot only to WARN `bryant_daily_clock_skew` when
the two disagree on the local date. The window deliberately follows the **fetch** clock:
Carrier's day1/day2 are relative to the instant of the fetch, so slot-dating would
mislabel the response.

The WARN does not fail the job or touch any failure counter — a late firing still fetches
valid, correctly dated data, so failing it would turn a recoverable suspend into a missed
day of energy. `stages/daily.py::run` must keep accepting `now=`; the runtime now passes it
on every firing, so removing it raises `TypeError` loudly rather than regressing silently.

Residual and unavoidable: `_call` hands the stage to `asyncio.to_thread`, so a few
milliseconds pass between the read and the request. If the Carrier cloud's own notion of
"today" rolls over inside that window the response is dated by Carrier's clock, not ours.
Only a live run can characterise that (#75).

## 83. `default_jobs` gains a `clock` injection point

`Runtime.serve` threads its own clock into `default_jobs(clock=self._clock)` so the Bryant
job's fresh read comes from the same clock the scheduler fires on, rather than a second
source of time. Extends the keyword-only, defaulted injection convention of #21 from
stages to the job factory.

## 84. `dim_channel.updated_at` is derived from the INPUTS, never from the wall clock

§9 lists the column without defining it. It is the blackstart inventory's
`metadata.lastUpdated` (local midnight → UTC), overridable per entry with an `updated_at`
key or wholesale with `build(updated_at=…)`. A `now()` stamp would make every `build-dim`
rewrite the object with different bytes, breaking CLAUDE.md rule 7's byte-identical re-run
(pinned by `test_a_rerun_is_byte_identical`).

Consequence, stated plainly: editing `channel_map.json` without touching the inventory does
not move `updated_at`. Fallback when no inventory metadata is available: the channel_map
file's mtime, with a WARN — deterministic across re-runs on one machine, not across
checkouts.

## 85. The PLACEHOLDER convention (PLAN.md is silent on it)

Leviton hub ids are panel serial numbers and are unknowable until `discover` runs against
the live panels; §9's own example uses a `<hub-serial>` sentinel. An entry is documentation
**iff** it carries `"placeholder": true` **and** the literal token `PLACEHOLDER` in
`source`/`device_id`/`channel_id`. Placeholders are excluded from `dim_channel.parquet` and
WARNed with the remedy. Either half without the other is a build error that spells out the
fix, because a flag without a token looks like a real channel being silently dropped.

## 86. A build that would write zero rows raises instead of overwriting a good object

`DimBuildError`, naming the placeholders and telling the operator to run
`energycap discover`. Same spirit as the compactor/rollup refusing to publish emptiness
(#36).

## 87. Placeholder entries are still validated against the inventory

A placeholder naming a nonexistent blackstart id fails the build. Consequence: because the
shipped map's placeholders reference real ids, `energycap build-dim` requires
`BLACKSTART_INVENTORY_PATH` even though today's output is Bryant-only, and it runs on the
Mac rather than in the container. Deliberate — stale documentation that only fails on the
day someone makes it real is worse — but it is a real constraint.

## 88. Three §9 columns whose rules PLAN.md does not give

* **`short_label`** falls back to `label` when the inventory has no `shortLabel` (the real
  file only records one where the long label is unwieldy), so the column is never null for
  a row that has a label.
* **`room`** is a comma-joined, de-duplicated list of the rooms the device's `circuits[]`
  reach, with top-level `roomAliases` applied ("Second Bedroom on Left" → "Office"). §9 has
  one singular `room` column, but the real inventory routinely has one breaker feeding four
  rooms (A-11); picking one would be a guess.
* **`category`** is normalized from `circuitType` on its **leading token**
  (`branch_120v`, `appliance_240v`, `device_240v`, `mwbc`, `backup_feed`, `feed_through`)
  so the inventory's parenthesised prose tail can be reworded upstream without emptying the
  column, falling back to `role`. An explicit category outside `KNOWN_CATEGORIES` is kept
  and WARNed rather than rejected.

## 89. `channel_map.json`'s entry keys are a closed set, and the whole file is validated in one pass

An unknown key is a build error: a typo like `"labell"` would otherwise be dropped silently
and leave a channel unlabelled. Two keys exist beyond §9's example: `placeholder` (#85) and
`notes` — free-form human documentation that is deliberately **never** written to the
Parquet file (JSON has no comments). The top level is fixed to `mappings` alone.

Validation collects **every** problem and raises once with a numbered report rather than
failing on the first; the file is hand-maintained, and fixing it one error per run would be
miserable. An entry that resolves to no label at all is a build error even when it has
other explicit fields — §9 only requires "neither blackstart_device_id nor explicit fields"
to fail, but a dim row with a category and no name reads as documented when it is not.

## 90. Only the inventory's top-level `devices[]` is indexed

`subpanels[].devices[]` (HVAC-A/B/C, `position: null`) are excluded on purpose: the
inventory itself records that no Leviton smart breaker can ever meter them — they sit
behind the feed-through lug at the air handler — so allowing a mapping to one would create
a channel that can never produce data.

## 91. `dim_channel`'s Arrow schema lives with its writer, and Glue aliases it

`stages/dim.py::DIM_SCHEMA` is the single declaration; `aws/glue.py::DIM_CHANNEL_SCHEMA` is
an alias of it, not a second spelling. The two were built independently and had already
drifted on nullability (`label`/`short_label` non-null in the writer, nullable in Glue),
which is exactly how a table ends up describing something the writer does not produce.
This follows the rule the other three tables already obey — the module that writes the rows
owns the schema (`model.py` for raw/hourly/daily, `stages/dim.py` for dim_channel).

Aliasing makes a paper comparison vacuous, so `tests/test_glue.py` now proves the real
thing instead: it builds rows through `dim.build_table` (including an `int`
`estimated_watts`, as blackstart records it, and an all-nulls row), writes a real Parquet
object through `s3io.write_table_atomic`, reads the schema back off the bytes and compares
it to the declared Glue types.

## 92. `energycap discover` writes two LOCAL files; `cli.py` grew the flags to control them

The command's help used to say "Read-only: it touches no S3 object and writes no data".
It touches no S3 object, no spool row and no Parquet file — but it writes
`config/live_channels.json` beside the map (always, so `build-dim` can WARN without a live
call, PLAN.md §9) and, on request, a raw dump. The help text now says so, and the CLI
exposes `--out`, `--no-write-live-channels`, `--dump FILE` and `--raw` (previously reachable
only through `ENERGYCAP_DISCOVER_OUT` / `ENERGYCAP_DISCOVER_NO_WRITE` /
`ENERGYCAP_DISCOVER_DUMP`, which still work). The dump is written mode 0600 and is the
sensitive one; a test asserts it holds no password and no Leviton session token.

## 93. `build-dim` reads the sidecar `discover` wrote, without being told to

PLAN.md §9 promises an unmapped live channel is WARNed by `build-dim`. The operator runs
two commands and does not hand one's output to the other, so when neither `live_channels=`
nor `live_channels_path=` is passed, `dim.build` picks up `live_channels.json` beside the
map if it exists (`dim.default_live_channels_path`, which asks
`stages/discover.py` for the path so the two cannot drift). Without this the promise was
dead code that no test of either stage alone would have caught; `cli.py` also grew
`--live-channels FILE` for an explicit path. A first build before anything has been
discovered is not an error — there are simply no WARNs. Pinned by a real round trip in
`tests/test_discover.py`.

## 94. The sidecar is not written when no source could be enumerated

A file claiming zero live channels would tell `build-dim` the panel is empty, which is the
opposite of the truth. A warning naming the path is printed instead and any previous file
is left alone.

## 95. Skipped objects are shown in the report but excluded from the sidecar's `channels[]`

`NONE`/`NONE-1`/`NONE-2` placeholder breakers, `NOT_USED` CTs and zones with `enabled != on`
are printed in the table marked `SKIP:` with a reason, and live in the sidecar's
`skipped_channels[]`. `dim.load_live_channels` treats every `channels[]` entry as live, so
including them would make `build-dim` WARN that a dumb breaker needs a label.

## 96. Three mechanical costs of reusing the sources rather than re-implementing them

* **Leviton is fetched twice per run**: `source.discover()` for the authoritative channel
  set (the skip rules stay in `sources/leviton.py`) and `adapter.fetch_snapshot()` for the
  full hierarchy the table needs, including the skipped objects. ~14 requests on a manual
  command; nothing here runs on the 30s loop.
* **Raw Leviton capture subclasses `LevitonAdapter`**, re-expressing its three fetchers via
  the protected `_ensure_client`/`_call`, because the adapter converts `aioleviton` models
  to readings and drops `model.raw` with no hook to observe them. Auth, the Origin spoof,
  the token cache, the retry policy and the exception translation are all still the
  parent's. If `sources/leviton.py` ever grows a public hook, collapse the subclass onto it.
* **In dump mode only**, the Bryant pass issues one extra recorded `getInfinityEnergy`
  query (through the public `stages/daily.py` fetch) so #75.8's open questions about the
  daily-energy shape are captured by the same run. A failure becomes a recorded fact
  (`energy_probe=<error>`), never a failed discover.

## 97. `--json` keeps stdout a single parseable document, but `cli.py` still logs there

`json_only=True` prints only `{"mappings": […]}` on stdout and sends the table, warnings and
file notices to stderr. `cli._run_stage`'s own `stage_start`/`stage_ok` JSON *log* lines
still share stdout, so `energycap discover --json | json.load` needs line filtering. The
reliable machine path is the sidecar file.

## 98. A PLACEHOLDER entry does not count as "mapped" in `discover`

The channel it describes is still unmapped until a real `device_id` is pasted in, so it
gets a skeleton entry and a nudge printed with the real hub id right above it.
`PLACEHOLDER_TOKEN` is duplicated as a literal in `discover.py` rather than imported from
`dim.py`, so `discover` never needs `build-dim` to be importable.

## 99. `DiscoveryFailed` is raised only after the report has printed

Every requested source is attempted; one cloud being unavailable prints a single scrubbed
`UNAVAILABLE:` line and the other still prints its table and contributes skeleton entries.
Only when *every* requested source fails does the stage raise — after the report — so the
operator sees the errors in context and the CLI still exits non-zero. Consistent with #26.

## 100. moto's Glue backend is unusable here, so the stateful Glue tests drive a faithful fake

`pyproject.toml` installs `moto[s3]`, and moto 5.2.2's Glue backend imports `pyparsing`,
which that extra does not pull in (`ModuleNotFoundError` on the first Glue call); adding a
dependency was out of scope for the build. The stateful tests therefore drive `FakeGlue`, a
small in-process stand-in that raises real botocore `EntityNotFoundException` /
`AlreadyExistsException` errors and adds the fields the real service adds server-side
(`CreateTime`, `UpdateTime`, `CreatedBy`, `CatalogId`, `transient_lastDdlTime`,
StorageDescriptor defaults). `test_moto_glue_backend_behaves_the_same` runs the identical
flow against real moto behind `pytest.importorskip("pyparsing")`, so it starts passing the
day `moto[glue]` is added. **This is the suite's one skipped test** — an environment
limitation, not a disabled assertion.

## 101. Glue's length limits forced a comment-placement decision

`Column.Comment` is capped at 255 characters and `Table.Description` at 2048. The full
mode/stage/fan decode is 190 characters on its own, so the **full** decode lives on
`energy_raw_30s.value` — the only table where enum rows exist — while `energy_daily.value`
gets its own precise comment stating there are no enum metrics there. The decode string is
generated from `bryant.enum_decode_text()`, so it cannot drift; tests parse the integers
back out and compare them to `ENUM_TABLES` entry for entry, and re-check the length after
appending a new enum value. `_fit()` **raises** rather than truncating: a truncated "do NOT
read absence as the load being off" would be a disaster, so an overflow fails loudly at
build time with a message saying to shorten the prose, never to drop a decode entry.

## 102. Partition columns are typed `int`, not `string`

§12 does not give the Hive type. With `projection.<key>.digits=2` Athena zero-pads the S3
**path** (`month=08`) but the column value stays numeric, so `WHERE month = 8` is correct
and `WHERE month = '08'` returns nothing. That trap is spelled out verbatim in the
`month`/`day` column comments and pinned by a test. `year` keeps §12's `2024,2035` range
with no `digits`; the ceiling is a ceiling, not a promise — a projected partition with no
objects behind it simply returns no rows, so the only cost is re-running this in 2036.

## 103. Two table parameters §12 does not mention are set on every table

`EXTERNAL=TRUE` and `classification=parquet`, alongside `TableType=EXTERNAL_TABLE` and the
explicit Parquet InputFormat/OutputFormat/SerDe. Necessary because there is no crawler to
infer them (§12 forbids one).

## 104. "Idempotent create-or-update", built one step stronger

A table whose definition already matches is left **completely** untouched — no
`update_table` call, so no pointless Glue table version is minted. Pinned by asserting the
second run issues zero write operations. Parameters AWS/Athena/a crawler add on their own
(`transient_lastDdlTime`, `UPDATED_BY_CRAWLER`, the crawler size/count keys) are ignored
when comparing, or every run after an Athena query would look like a change.

An **existing** database is never modified, not even its description: §12 only says to
create the database if absent, the database may hold tables this pipeline does not own, and
its description is not ours to overwrite. A lost `create_database` race
(`AlreadyExistsException`) is treated as success.

## 105. `energy_hourly`'s comment states its dedupe key as `hour_start_utc`, not `ts_utc`

Following #1, which resolved the §10/§15.3 conflict in favour of bucketing on
`hour_start_utc` so the DST fall-back day keeps 25 buckets. The hourly table has no
`ts_utc` column at all, so quoting §12's canonical key there would name a column that does
not exist.

## 106. `aws/glue.py` imports from `stages/` (twice, deliberately)

`COMPONENTS` from `stages/daily.py` so the `channel_id` comment's eight lowercase Bryant
energy components are derived from the module that writes those rows, and `DIM_SCHEMA` from
`stages/dim.py` (#91). Both are small layering inversions (aws → stages) and neither
creates a cycle, since those stages import `aws/s3io.py`, not `aws/glue.py`. The
alternative — re-typing the values — is what drift is made of.

## 107. `energy_meter` (§13) is deliberately not created

Adding it is one `TableSpec` entry: `schema=model.METER_SCHEMA`,
`prefix_builder=s3io.meter_year_prefix`, `partition_keys=("year",)`. The `interval_s`
column comment already exists, and a test builds that spec and checks its location,
template and columns render correctly without registering it.

## 108. `create-glue-tables --dry-run` still calls AWS

It reads (`get_database`, `get_table`) to report what *would* change, so it needs
credentials and `S3_BUCKET` even though it writes nothing. That is a more useful dry run
than an offline render of the table definitions, but it does mean `--dry-run` is not an
offline command.

## 109. The README is pinned to the code by `tests/test_docs.py`

§16.6 asks for a README with example queries; nothing pinned it to the constants it quotes.
A wrong enum decode in a document an LLM reads to orient itself silently rewrites the
meaning of every archived row for whoever trusts it. So the enum decodes (prose table *and*
the SQL `VALUES` blocks a reader copies), the presence of every CLI command, the
observed-time kWh formula and the canonical dedupe key are all now asserted against
`sources/bryant.py`, `cli.app` and `model.py`. Prose remains free to change.

## 110. What the README's queries actually prove

All 11 DuckDB statements were **executed** against a miniature copy of the S3 layout built
with the repo's own code (`model.observations_to_table`, `stages.rollup.rollup_day`,
`stages.dim.build_table`), including a deliberately half-missing hour and one hour deleted
entirely; the gap queries return the short hour with `sample_count 60` and exactly half the
kWh, and the spine query returns the deleted hour. The 7 Athena/Trino blocks are
**desk-checked only** — there is no Trino here and `sqlglot` is not installed. The README
says so in its Known-unproven list.

One trap found by executing rather than assuming: with `hive_partitioning = true` DuckDB
reads `month=08` back as the **string** `'08'` (the leading zero blocks the auto-cast) while
`year=2026` and `day=15` come back as integers. The README's queries therefore point their
globs at the partitions they want and never reference partition columns; where they do, they
use the `hive_types = {'year': INTEGER, …}` form. This is also the one place the DuckDB and
Athena idioms genuinely diverge, since Glue declares those columns `int` (#102).

## 111. The five defect fixes were re-verified by reverting them

Each fix was commented out, its regression test watched to fail, and then restored:
#76 (2 tests), #77 (2), #78 (2), #80 (2) and #82 (4). One gap was found doing this and
closed: `MIN_PASSWORD_GRANT_INTERVAL_S` (#77's first half) passed with **and** without its
fix, because `AUTH_LADDER_BACKOFF_S` masked it in every existing scenario.
`test_a_rejection_driven_password_grant_is_floored_even_when_okta_says_yes` now pins it
through `reauthenticate()` — the case where Okta happily mints a token every time and the
*gateway* is what rejects it, so no failure-based backoff ever arms — and that test does
fail without the floor.

## 112. `tests/test_integration.py`'s CLI-table test now asserts the opposite of what it did

It used to assert that `discover`, `build-dim` and `create-glue-tables` were *absent*
(`ModuleNotFoundError` naming exactly the module the table points at). They have landed, so
it now asserts that every entry in `STAGE_ENTRYPOINTS` imports and is callable, that each
one **binds** the exact keyword arguments its CLI command passes, that every other
parameter has a default, and that `import-greenbutton` is still the one command with no
entry point (§13). Strengthened, not relaxed: binding the real signature is what catches a
stage wired to the right module but the wrong function.

---

# Final reconciliation: the audit fixes and the README/Glue seam (#113–#131)

Two read-only audits of the finished build found defects in the log scrubber, in the Glue
comments and in the README's example queries. Three sessions fixed them in parallel and a
fourth reconciled the seam between them. The entries below record where those fixes went
beyond — or resolved an ambiguity in — what `PLAN.md` and `CLAUDE.md` actually say. Suite
after reconciliation: **1221 passed, 1 skipped** (the skip is the pre-existing `moto[glue]`
one, #100).

## 113. A log record whose own `%`-formatting is broken is emitted, not dropped

PLAN.md §11 and CLAUDE.md rule 8 require that credentials never reach the output; neither
says what to do with a record whose *caller* got its own `%`-formatting wrong (wrong arity,
wrong conversion). Stdlib logging drops such a record.

**The defect being fixed.** `ScrubbingFilter` scrubbed `record.msg` as free text while
`record.args` was still populated. `log.warning("password=%s", "hunter2")` had its `%s`
eaten by `_KV_RE`, so `record.getMessage()` raised inside the handler: **no stdout line at
all**, and logging's own error handler printed `Arguments: ('hunter2',)` to **stderr** — so
the value that was never registered as a secret leaked, unredacted, out of the process. It
was a leak as well as a dropped line.

**Built.** `_scrubbed_message(msg, args)` redacts the *arguments* structurally first (key-
name redaction only works on the real objects, before `%s` flattens a dict into a string),
expands them against the still-unscrubbed format string (so the placeholder count cannot
have changed), then scrubs the expanded text. Where the caller's own formatting is broken,
the line is kept as `"<template> [unformattable log args: <scrubbed args>]"` rather than
dropped, on the same reasoning as the defect itself: a silently missing log line is how an
incident hides, and the args are already redacted by that point. Pinned by
`test_a_caller_arity_bug_keeps_the_line_instead_of_dropping_it` and
`test_the_arity_fallback_still_redacts`. Matching stdlib and dropping instead would be one
`except` branch.

## 114. Every record that passes the scrubber now carries `args = None`

A consequence of #113 worth stating, because it is a contract other code could break. The
filter expands the message itself and returns `args=None` so nothing downstream can re-
format it (including `JsonFormatter.format`'s belt-and-braces `scrub_text` pass). Anything
that read `record.args` after filtering would now see `None`. Nothing in the repo does:
the only `getMessage()` call is `tests/test_discover.py:692`, on an args-less record, and
`StageLogger` takes `event` + fields and never uses `%`-args at all — the fix matters for
stdlib-logger call sites and for third-party libraries logging under `energy_capture.*`.
Whoever edits this must keep the order scrub-args → expand → scrub-text; scrubbing
`record.msg` as text while `args` is populated *is* the defect.

## 115. Layer 3 (text patterns) is deliberately conservative, and stays that way

`_KV_RE`'s value class excludes whitespace, quotes and backslashes, so
`password="quoted value"` redacts only as far as the space
(`password="***REDACTED*** value"`), and a value sitting against an already-escaped `\"`
inside a serialised JSON line is left alone entirely. That is the documented fix for the
quotes/backslashes bug: widening the class would swallow JSON escapes and break the one-
object-per-line guarantee of PLAN.md §11. Such values are caught by layer 1 (registered
literal) or layer 2 (key name) instead.
`test_text_scrubbing_inside_an_already_serialised_json_line_keeps_it_parseable` states the
trade explicitly so nobody "improves" the regex later.

## 116. A field named `auth` is redacted wholesale, siblings and all

`auth` is in `SECRET_KEY_NAMES`, so `scrub()` replaces the whole value — including any
innocent keys nested under it — with `***REDACTED***`. This is existing, intended
behaviour (over-redaction is the safe direction under CLAUDE.md rule 8), but it means a
debug payload nested under an `auth` key disappears entirely rather than being partially
redacted. Found while writing a test; the fix there was to name the field `login`.

## 117. The shared partition clause in the table *descriptions* had the same defect

Audit defect 1 was reported against the `ts_utc`/`ts_local` column comments. Fixing it
exposed an unlisted instance one level up: `_LOCAL_PARTITION_CLAUSE`, shared by every
table description, told `energy_hourly` readers that its partition values "come from
`ts_local`" — a column that table does not have. It is now generated per table,
`_local_partition_clause(keys, source_column)`, so `energy_hourly` says
`local_hour_start` and `energy_daily` says "the year partition — the only one — comes from
ts_local". `TableSpec.partition_source_column()` reads the source column off the schema and
**raises** for a partitioned table carrying neither `ts_local` nor `local_hour_start`.

## 118. `energy_daily` inherited a unit comment describing units it cannot hold

A second unlisted instance, of audit defect 3: `energy_daily` fell through to the canonical
`unit` comment, which offered `W`/`A`/`V`/`Hz`/`degF`/`pct`/`'enum'` — none of which occur
there — and pointed at "the value column's decode" for enum rows, on a table whose own
`value` comment says no enum metrics exist. It now has a generated comment listing `kWh`,
`USD` only. Caught by the new `test_a_unit_comment_names_no_unit_that_cannot_appear_there`.

## 119. `energy_hourly` gets its own gap clause, not the shared one

The shared `_GAP_CLAUSE` describes a gap as "a null field from the API emits no row and a
failed poll cycle emits no rows at all" — the *raw* grain's failure shape. At the hourly
grain an unobserved hour is NO ROW and a partly observed one is a low `sample_count`, so
`_GAP_CLAUSE_HOURLY` states it that way. All of CLAUDE.md rule 1's content is preserved:
the existing assertions on "gaps mean collector downtime, never zero load", "interpolated"
and "zero-filled" still hold on that table. The second reason was budget — the 2048-char
description had to absorb the enum decode (#120).

## 120. Prose in two table descriptions was tightened to fit Glue's 2048-char ceiling

`energy_raw_30s` and `energy_hourly` are close to the limit `_fit()` refuses to truncate
past (#101). Surrounding prose was shortened — "30-second poll" → "30s poll", "Leviton
firmware v2" → "Leviton fw v2", shorter joins of existing clauses — to make room for the
enum decode and the compaction caveat. **No warning, decode, formula, dedupe key or
cardinal-rule statement was dropped**, `_fit` still raises rather than truncating, and
every existing content assertion still passes. Current lengths, of 2048: raw_30s 2034,
hourly 2015, daily 2005, dim_channel 952. Anything appended to `bryant.ENUM_TABLES` grows
the hourly description, so that headroom is the real budget.

## 121. `_PARTITION_COLUMN_COMMENTS` was replaced by a factory, and a test rewritten

The private constant no longer exists; `_partition_column_comments(source_column)` renders
the same three comments against whichever local timestamp column the table actually has.
`test_the_partition_column_comments_warn_that_the_column_is_an_integer` (which read the
constant) was rewritten to call the factory and parametrized over `ts_local` and
`local_hour_start`. Strictly stronger — it now checks both renderings — and a companion
test asserts the same properties on the comments rendered into every real table.

## 122. Two new build-time invariants in `aws/glue.py`

Both are deliberate, and both fail where somebody is watching rather than in a comment an
LLM later trusts (the same policy as `arrow_to_glue_type`, #100-adjacent):

1. **Adding a metric to `model.UNIT_FOR_METRIC` now requires adding it to
   `_METRIC_GROUPS`**, or importing `aws.glue` raises `ValueError` naming the metric. This
   is the defect-3 fix: the metric and unit lists in the comments are generated from
   `model`, and `_METRIC_GROUPS` supplies only the grouping prose and which tables a group
   reaches. Whoever adds `stage_pct` (#59) will hit this.
2. **A `TableSpec` with `partition_keys` must carry `ts_local` or `local_hour_start`**
   (#117). PLAN.md §13's `energy_meter` satisfies it via `METER_SCHEMA.ts_local`, and gets
   correct year-only partition prose for free.

## 123. The Glue fixes could not be diffed against the original — they were reconstructed

Nothing is committed on this branch beyond the initial commit (which contains only
`.gitignore`, `LICENSE` and `README.md`), so "this test fails without the fix" could not be
shown with `git stash`. The pre-fix comment strings were instead reconstructed into a
scratchpad copy of the module and the new tests run against it; every new test failed there
for the intended reason. Two reload-based tests failed only through a harness artefact
(`importlib.reload` of a module loaded by path) and are failing-by-construction against the
old code regardless, since the strings they look for did not exist in it.

## 124. README query 4's `hvac` CTE was reporting the mean of the HVAC channels

Not one of the four assigned defects, but the same failure class as defect 2 (an aggregate
silently merging channels). `round(avg(r.value))` over every (channel, instant) row
reported the *average* of the HVAC channels rather than the HVAC *total* — 775 W where the
truth on this house's two CT legs is 1550 W. It now sums ACROSS channels within one instant
and then averages across the bucket, and carries a new `hvac_channels` column so an instant
where one CT went missing is visible instead of quietly lowering the total. Contained to
that one CTE and its output columns, in both dialects; the DuckDB form is executed by the
suite.

## 125. The README's vocabularies were completed past the assigned defect

The brief named `blower_rpm`/`cfm` and `rpm`/`CFM`. Also added: `kwh_interval`,
`ccf_interval` and `CCF` — defined in `model.py` for the designed-but-unbuilt
`energy/meter` dataset (PLAN.md §13) and marked as such in the README. Without them the
lists could only be pinned as a *subset*; with them `tests/test_docs.py` asserts exact
equality with `model.METRICS` / `model.UNITS`, which is a far stronger guard against the
same defect recurring.

## 126. `(source, device_id, channel_id)` was added to non-aggregating queries too

Queries 1 (30s detail), 2 (hourly listing), 3 (short-hours and missing-hours output) and 5
now carry the full channel identity even where no aggregate could produce a wrong number.
Two hubs' rows were previously indistinguishable in the output, and an LLM copying the
pattern would have learned the wrong habit. Query 5 (Bryant daily) also groups by
`device_id`/`channel_id`; there is one Carrier serial today so it splits no group, and the
README says so beneath the query. Slightly wider output than before.

## 127. Two absolute claims about `raw_30s` were softened, in both documents, identically

The README said `energy/raw_30s/` "always holds exactly one authoritative copy of each day"
and that "no query ever needs `DISTINCT`"; the Glue `energy_raw_30s` description said
"never both … no de-duplication is needed". #35 records a real window where a day file and
its parts coexist, so both were wrong in the same direction — the direction that
double-counts a day.

**Built, and reconciled in the final pass**, because the two documents had been fixed
independently and did not agree: the README described "the few seconds a compaction is in
flight" while the Glue comment described only "a compactor that crashed". Both windows are
real (the in-flight one is seconds; the crash one persists until the next run), so both
documents now name both, plus the same tell (`part-*.parquet` beside `day-*.parquet`, or
totals ~2× the neighbouring days'), the same remedy
(`energycap compact-daily --start D --end D`) and the same interim workaround (dedupe that
day). `_DEDUPE_CLAUSE` — which reaches the database description, `energy_raw_30s` and
`energy_daily` — now says "no query **over a settled partition** needs DISTINCT", the same
qualified form the README uses, and the module docstring was corrected to match so nobody
re-types the absolute from it.

## 128. The enum rollup warning and decode reach `energy_hourly`, and the README says so

`rollup.sql` excludes only `model.DAY_GRAIN_METRICS`, so `mode`/`stage`/`fan` **are** rolled
up: `energy_hourly` carries `mean`/`min`/`max`/`p95` over integer codes, and two of those
four are arithmetic on a label. That table has no `value` column to hang a decode on, so
`ENUM_ROLLUP_WARNING` and the generated `_ENUM_DECODE` live in its **table** comment, and
`unit`/`mean`/`p95` say MEANINGLESS for `unit='enum'` while `min`/`max` say they are
meaningful. The README stated none of this (it warned against `avg(mode)` only in the
context of a raw_30s query); its hourly-rollup section now carries the same warning and
points at the table comment, and its enum-decodes section names both quotation sites.

## 129. The README↔Glue seam is now pinned by tests, not by care

PLAN.md §12 makes both documents deliverables but nothing made them agree, and they had
drifted on all four of the audited facts. `tests/test_docs.py` gained five tests that pin
the *seam* rather than either document: the metric and unit vocabularies (every metric or
unit named in a Glue comment must appear in the README's closed list; the only permitted
difference is a metric no table can hold, which the README must flag as
designed-but-not-collected, and the difference is derived from `glue`'s own per-table
reachability so building `energy_meter` shrinks it automatically); the enum decode reaching
the `energy_hourly` table comment integer-for-integer; the enum warning appearing in both
documents; and the compaction caveat, where both documents must qualify the `DISTINCT`
claim, name the window, the consequence, the remedy command and the workaround. Each was
verified to fail by mutating the README or the Glue source and watching exactly the
intended test go red.

## 130. What the Athena translations still are not

Unchanged from #110 and honest: nothing in this environment can reach Athena or Trino, so
the 7 Athena blocks remain **desk-checked and unexecuted**. A drafted
`from_unixtime(floor(to_unixtime(...)/300)*300)` bucket idiom was deliberately *removed*
rather than published as a Trino form nobody could verify. What is enforced for them is
static and is the part that was wrong before: a test fails if any example in either dialect
cuts a bucket key out of `ts_local` or hardcodes a 24-hour day.

## 131. How the DST claim in the README was actually validated

`test_the_daily_coverage_query_expects_a_real_number_of_samples_across_dst` runs the
README's daily-coverage block verbatim with only its partition glob and its two window
literals substituted (`month=08` → `month=03`/`month=11`, and the two date literals). The
arithmetic under test is untouched and returns 2760 / 2880 / 3000 against hours_present
23 / 24 / 25 — but the executed text is not byte-identical to the published text for the
two DST cases, and that is worth knowing before treating it as a full end-to-end proof.

---

# Step 8: `stage_pct` — what the first live Carrier call forced

Entries #132–#143 come from the change made after `energycap discover` and
`energycap poll --once` finally ran against the real Carrier cloud on **2026-08-17**. The
capture settled the highest-risk open question in the Bryant work (#59, #75.1) the way the
risk was written: this house's outdoor unit is variable-capacity, so `odu.opstat` is a
capacity percentage and the `stage` enum could never hold it.

## 132. `stage_pct` — a numeric `odu.opstat` is now a first-class metric

**Evidence.** The live `getInfinityStatus` response for serial `4022W200213`:
`odu.type = "gs3ngiphp"`, `odu.opstat = "35"`, `idu.opstat = "off"`, `cfgem = "F"`,
`mode = "cool"`, `isDisconnected: false`, eight zones reported and one enabled. Every poll
cycle logged `bryant_enum_numeric` and emitted **no** compressor row.

**Built.** `model.UNIT_FOR_METRIC` gains `"stage_pct": UNIT_PCT` — DEVIATIONS #59's exact
wording — and `sources/bryant.py` grows `BryantStatusSource._add_stage`, which moves the
stage decision out of the generic `_encode`:

- a **number** in 0–100 → one `stage_pct` row, `unit="pct"`, `channel_id="system"`, value
  **as reported** (`"35"` → `35.0`; nothing rounded, scaled or bucketed);
- a **known word** → `stage`, an enum code from the untouched, still append-only
  `STAGE_CODES` — byte for byte the old behaviour;
- an **unknown word** → WARN `bryant_enum_unknown`, no row. A string never becomes a
  number;
- a number **outside** 0–100 → no row (#133);
- missing / `"None"` / empty → a silent gap, as before.

At most one of the two metrics emits per cycle, both paths stay live forever, and because
they are different metric *names* they never collide in the dedupe key
`(ts_utc, source, device_id, channel_id, metric)` and never average together in a rollup.

**Why a new metric rather than a new enum code.** `35` is not a stage. Adding `"35": 5` to
`STAGE_CODES` would put a measurement and a category in one column under one unit; rounding
it into `low`/`high` would fabricate a reading the API never sent. The metric *name* is the
representation tag, which is the only honest way to carry two mutually exclusive renderings
of one field in a long-format table. `stage_pct` is deliberately **not** in
`model.ENUM_METRICS`: it is a measurement, and its mean is meaningful (#139).

## 133. A `stage_pct` outside 0–100 is a gap, and is never clamped — NEW POLICY

PLAN.md is silent on out-of-range percentages.

**Built.** A numeric `odu.opstat` that parses outside `STAGE_PCT_MIN`/`STAGE_PCT_MAX`
(0–100) emits **no row**, WARNs once per distinct value (`bryant_stage_pct_out_of_range`)
and increments `stage_pct_out_of_range` in `status.json`.

**Why not clamp.** A clamped `100` is indistinguishable from an observed `100` once
archived — that is fabrication under cardinal rule 1, and it is worse than a gap because it
is invisible. A gap says "we do not know", which is the truth. `_as_float` also rejects
NaN/inf, which `float()` — and therefore `_looks_numeric` — happily accept from the strings
`"nan"`/`"inf"`, so those land in the same branch instead of becoming a row that no
comparison can order. **`0` is treated as a real reading** (a compressor idling at 0 %
capacity is a fact about the compressor), not as a gap.

## 134. `bryant_enum_numeric` no longer fires for `stage`; the numeric case is INFO, once

**The defect, observed live.** A variable-capacity system is in the numeric branch on
*every* cycle, so the WARNING fired every 30 s — **2,880 identical lines a day** — which is
precisely how the one warning that matters gets missed. This is an operational defect, not
a cosmetic one.

**Built.** `stage` never reaches `_encode`'s numeric branch now. The numeric case is
reported by `bryant_stage_representation` at **INFO, once**: on first observation, and
again only on a *transition* between representations (both directions), carrying
`previous` and `changed`. `bryant_enum_numeric` still exists for `mode` and `fan`, where a
number really is unexplained. The ongoing volume lives in `status.json`, not in the log:
`stage_representation`, `stage_representation_changes`, `stage_pct_rows`,
`stage_enum_rows`, `stage_pct_out_of_range`, `distinct_warnings` (plus the pre-existing
`numeric_stage_samples`). Pinned by
`test_twenty_consecutive_live_cycles_log_at_most_one_warning`, which replays the live
capture twenty times and asserts ≤ 1 WARNING and exactly one INFO line — the defect stated
as an assertion rather than as prose.

## 135. Counter semantics: a numeric `stage` is no longer an "unknown enum value"

A numeric `odu.opstat` no longer increments `unknown_enum_values` or
`source.unknown_enum_counts['stage']`. It is a *known representation* of a known field, not
an unmapped value, and leaving it in the unknown-enum counters would make that counter
useless as an alarm on this system. `numeric_stage_samples` keeps its old meaning — every
numeric `opstat` sample, in range or not — so #75.1's instruction to "watch
`numeric_stage_samples`" still works exactly as written.

## 136. `status.json` was being rewritten every 30 s (pre-existing defect, fixed)

Found while adding the counters above, and it is the same 2,880-times-a-day defect the
module explicitly set out to avoid.

`_publish_status` compared **monotonic per-cycle counters** to decide whether anything had
changed, so on any variable-capacity system — and on any system with a single *persistent*
unknown enum word — the "has anything changed?" test was always true and the file was
rewritten on every cycle.

**Built.** The per-cycle counters are listed in `_VOLATILE_STATUS_FIELDS` and excluded from
the change comparison. The write trigger is now `distinct_warnings` (a count of distinct
warned conditions) and `stage_representation` — state fields that move only when something
new actually happens. The counters are still *written* whenever a write happens, so nothing
is lost from the file; only the pointless rewrites are.

## 137. Diagnostics: `odu.type`, `stage_metric_for()`, and three new discovery details

`SystemStatus.odu_type` parses `odu.type`, and the `system` channel's discovery details
carry `odu_type`, `odu_opstat` and `stage_metric` (from the new `stage_metric_for()`
classifier). `energycap discover` therefore answers "does this house produce `stage` or
`stage_pct`?" from one response, before any row exists — which is what #75.1 asked for and
what previously required a poll cycle and a log dive.

**Nothing branches on `odu.type`.** It is a hint, never a decision: the `opstat` value
itself is the evidence, and a system whose type string is unfamiliar must still be read
correctly. These are diagnostics, not metrics — no schema change, no rows.

## 138. The committed Bryant fixtures ARE the live capture, and are replayed whole

`data/discover-raw.json` (mode 0600) is **gitignored and not committed** — it describes one
house. Two derivations of it are:

- `tests/fixtures/bryant/status_live_capture.json` — the `getInfinityStatus` envelope
  **verbatim and untrimmed**: all eight reported zones, every field, every value as the API
  spelled it. The serial appears only in the request *variables* and is recorded as
  `<CARRIER_SERIAL>`; every zone `name` arrived as JSON `null`, so no room labels exist to
  redact.
- `tests/fixtures/bryant/status_varcap.json` — the same capture trimmed to two zones (one
  enabled, one phantom) for readable per-field assertions. It **replaced** an earlier
  hypothetical (`varcaphp`, `opstat "45"`), and it is what `tests/test_glue.py` and
  `tests/test_docs.py` pin their documentation claims to.

`tests/test_bryant_live_capture.py` replays the untrimmed capture through the production
path and pins the exact row set it produces: one `stage_pct` = 35.0 `pct` on `system`,
**zero** `stage` rows, ten rows sharing one `ts_utc`, and nothing at all from the seven
phantom zones. It also asserts the trimmed fixture still describes the same house, so the
Glue/README pins stay answerable to the real payload, and — on the machine that holds
`data/discover-raw.json` — re-derives the fixture from it so a hand-edit cannot go
unnoticed. Off that machine the raw file is simply absent and the remaining assertions
carry the test; nothing is skipped or xfailed.

## 139. `stage_pct` in the Glue catalog, and the trap stated where a reader will meet it

- `_METRIC_GROUPS` gains `"stage_pct"` beside `"stage"` in the `bryant status` group
  (tables unchanged: `raw_30s` + `hourly`). That alone clears the import-time `ValueError`
  that #122.1's "every metric must be grouped" invariant would otherwise raise — predicted
  verbatim by that entry.
- New `STAGE_REPRESENTATION_NOTE`, built from `bryant.STAGE_METRIC`,
  `bryant.STAGE_PCT_METRIC`, `model.unit_for_metric(...)` and a new
  `ODU_TYPE_OBSERVED = "gs3ngiphp"` — never re-typed as prose: *`stage` and `stage_pct`
  render ONE field (`odu.opstat`), MUTUALLY EXCLUSIVE; this unit is VARIABLE-CAPACITY, so
  `stage` never appears — absence, not zero.* It is carried by `DATABASE_DESCRIPTION`,
  `energy_raw_30s` and `energy_hourly`: every string a reader of either metric can meet.
  Putting it in the database description was not requested and was done deliberately — it
  is the first string an LLM reads over MCP, and the trap is invisible from a metric list.
- New `STAGE_MEAN_NOTE` on `energy_hourly`, so `ENUM_ROLLUP_WARNING` cannot be read as
  covering `stage_pct`: its `mean` is a real mean capacity, the natural partner for mean
  watts.
- `ENUM_ROLLUP_WARNING` and the hourly `unit`/`mean` comments now **generate** their enum
  roster from `model.ENUM_METRICS` instead of the literal "mode/stage/fan", so a metric
  that starts or stops being an enum cannot leave those warnings naming yesterday's set.
  Prose order changed ("stage/mode/fan"); content did not.

## 140. What was cut from the Glue descriptions to fit the note under 2048 characters

`energy_raw_30s` and `energy_hourly` were already at ~2034/2015 characters against Glue's
2048 ceiling (#120), so a ~290-character note required trimming. Final lengths: 2025 /
2016 / 1972 / 1151 (database). Nothing on the protected list — the gap warnings, the enum
decode, the kWh formula, the dedupe keys, the compaction exception, any cardinal-rule
sentence — was dropped. Connective prose was compressed throughout, including the shared
`_GAP_CLAUSE`/`_DEDUPE_CLAUSE`/`_local_partition_clause` (which is why `energy_daily` also
shrank). What was actually **removed**:

1. `energy_raw_30s`'s "Long format: the number is in `value`, what it measures is in
   `metric`/`unit`" and the "LWHEM-2 load centers / Infinity HVAC" model names — both
   still present verbatim in the `source`, `metric` and `value` column comments and in the
   database description;
2. `energy_hourly`'s trailing "Join dim_channel on (source, device_id, channel_id)." —
   still in the database description and in that table's `channel_id` comment;
3. `energy_hourly`'s "an unobserved hour produces NO ROW at all…" sentence, which
   duplicated PLAN.md §12's verbatim `HOURLY_GAP_WARNING` one paragraph earlier in the
   same string.

If any of those three is considered load-bearing where it was, the budget has to come from
somewhere else — the ceiling is not negotiable.

## 141. README changes the live evidence forced, beyond the assigned scope

The README is a deliverable and must not lie, so the capture required corrections outside
the `stage_pct` brief:

- **New section "Compressor stage: `stage` vs `stage_pct`"** — a two-row hardware table,
  the live evidence (`odu.type = "gs3ngiphp"`, `odu.opstat = "35"`), the two `WHERE`
  clauses side by side with a note that one returns nothing *forever* here, how to ask a
  system which rendering it uses (`discover` prints `odu_type`/`odu_opstat`/`stage_metric`),
  and why `stage_pct` is neither an enum nor ever clamped. `stage_pct` joins the `metric`
  vocabulary; the enum table's `stage` row is flagged "not emitted on this system"; query 4
  selects both renderings in **both** dialects.
- **A new "Settled by the first live run (2026-08-17)" section**, and the `odu.opstat`
  question moved out of Known-unproven into it.
- The Setup blockquote and Known-unproven #1 claimed "no live call has ever been made to
  Leviton, Carrier, or AWS". Narrowed to "nothing downstream of the spool has touched AWS",
  with `discover`/`poll --once` recorded as done.
- Known-unproven #5 claimed the Leviton half of `config/channel_map.json` is placeholders.
  It now holds **real hub ids**; only the future LG&E meter entry is a placeholder, so the
  item was rewritten as a coverage caveat.
- Known-unproven #3 and #7 were rewritten to drop the questions the capture answered while
  keeping the ones it did not.
- The daily-energy paragraph claimed `gas`/`reheat`/`fangas`/`looppump` are structurally
  absent and "a day normally reports only the four that exist". The live `energyConfig`
  disables only `looppump`, so the text now says **seven** components report, several as a
  real measured `0.0`.

## 142. `tests/test_docs.py`'s corpus now emits `stage_pct` and no `stage`

The executed-README corpus was rebuilt so the Bryant `system` channel emits
`stage_pct` = 35.0 and no `stage` row, exactly as the real house does. Without that, the
new query-4 assertion would be vacuous and an example query naming only `stage` would keep
looking correct offline while returning an empty column against the real bucket. The tests
were extended, never loosened: no example query may name one rendering without the other,
and query 4 is *run* against the corpus asserting the `stage` column comes back NULL in
every row while `stage_pct` is 35.0 — the trap reproduced rather than assumed.

## 143. What the first live run did and did not touch

For the record, because #75 and the status section below are read as a checklist:
`energycap discover` and `energycap poll --once` reached the real Leviton and Carrier
clouds and wrote `data/discover-raw.json` plus local spool/status files. **No AWS call of
any kind was made** — no S3 write, no Glue `CreateTable`, no Athena query, no DynamoDB
scan — and no upload, compaction, rollup or `build-dim` has ever run against the real
bucket. The test suite remains structurally offline: `tests/conftest.py`'s autouse guard
still refuses every non-loopback socket, and every assertion in this step was made against
the committed capture, never against the network.

---

# Step 9: the Leviton WebSocket freshness engine (#144–#158)

Everything in this step exists because a LOCKED decision was measured and found wrong.
#144 is the deviation; #145–#158 are the choices building it forced. The whole step is
**untested against the live cloud** — see the status section, which now lists the socket
among the things a first live run has to prove.

## 144. The Leviton WebSocket, against LOCKED decisions §2.8 and §6.4

**This is a deviation from a locked decision, not an ambiguity.** It is recorded here
because CLAUDE.md forbids re-litigating §2, and because the only thing that justifies
overturning one of those is evidence.

**The spec text.** PLAN.md §2, item 8: *"Leviton ingestion: **REST polling at 30s +
bandwidth keepalive**, not WebSocket (§6.4)."* And §6.4: *"WebSocket
(`wss://socket.cloud.leviton.com/`) exists but is NOT used: server hard-kills connections
at exactly 60 min, fw≥2.0 needs per-breaker subscriptions, updates are partial deltas
requiring state merging, and the bandwidth keepalive is needed anyway. **REST at 30s is
simpler and sufficient for an archival pipeline.**"*

**Authorisation.** The owner authorised building the WebSocket ingester explicitly on
2026-08-17, after the measurements below. Every technical objection §6.4 raises is real
and none of them was waved away — the 60-minute kill, the per-breaker subscriptions, and
the partial-delta merge are all handled, and the keepalive is unchanged. The clause that
turned out to be false is the last one: for these hubs, REST at 30s is **not** sufficient,
because it is not returning current values at all.

**The evidence, measured 2026-08-17 against the real hardware** — two LWHEM-2 hubs on
firmware 2.1.2, `1000_0046_1D52` ("Panel A", 2 smart breakers) and `1000_0046_1D48`
("Panel B", CT pairs only):

1. Over a 5-minute production run (20 cycles) and a separate 12-minute probe (46 reads at
   15s), **10 of 12 channels never changed value at all**. A whole-panel `GRID_POWER` feed
   held **exactly 4086.05 W across 46 consecutive reads**; another held exactly 505.17 W
   across the same 46. Changes arrived in bursts and then went flat for minutes, and which
   hub looked live changed between runs — so this is not one broken hub.
2. An **A/B probe** — four reads with no keepalive, then four each preceded by
   `PUT {"bandwidth": 1}` — showed the PUT demonstrably lands (the hub's `bandwidth` field
   reads 0 at rest and 2 afterwards, i.e. 1 auto-decayed to 2 exactly as §6.4 describes)
   and that **both phases were identically frozen**. The keepalive changes nothing for a
   REST reader.
3. `gtxaspec/leviton-load-center` documents why: setting `bandwidth=1` *"triggers a full
   state flood from the server"* — and that flood is pushed over the **WebSocket**. The
   same integration documents its REST path as *"initial discovery, fallback polling
   (10-minute interval)"*. This pipeline was polling REST **20× faster than the
   reference's own fallback rate and receiving a cache**.
4. Scale is about to change: ~25 additional smart breakers take this house from 12
   channels to ~40. Per-breaker resolution is where a stale cache hurts most.

**What was built.** `src/energy_capture/sources/leviton_ws.py`: a WebSocket subscriber
(behind the §2.8 adapter seam, over `aioleviton` 0.3.3 — no hand-rolled client) that
maintains an **in-memory current-state store**, merging the partial deltas the cloud
pushes. It maps no rows and never touches the spool.

**What was deliberately NOT built, and this is the load-bearing part.** The socket changes
how values are kept **fresh**; it does not change how rows are **sampled**. There is no
`Observation` per WebSocket delta. The existing 30-second poll cycle samples the store and
emits exactly the rows it emitted before, with one `ts_utc` per cycle (§6.5). A
row-per-delta design would have made sampling irregular, which breaks §2.5's kWh formula
(`mean_watts × sample_count × poll_interval_s / 3.6e6` assumes a fixed cadence) and
destroys `sample_count`'s meaning as the gap detector (§12's "`sample_count` < ~118 means
the hour has gaps"). Spool, uploader, compactor, rollup, Glue, the README's queries and
the dedupe key are all untouched.

**The cardinal-rule trap, and the line drawn through it.** Sampling an in-memory store is
structurally a hold-last-value, which CLAUDE.md rule 1 forbids. What makes it legitimate:

> **Connection state gates emission. Field age never does.**

While the socket is connected *and* a full state sync has completed for the current
connection, the store holds what the server currently believes; sampling it is honest,
including a field that has not moved in ten minutes, because the live capture shows a
resistive water-heater element genuinely holding 2462 W and gapping on "this has not
changed recently" would delete real data. While the socket is disconnected, unsynced,
auth-suspect, or **open but silent** past `LEVITON_WS_STALL_TIMEOUT_S`, we do not know the
current value and emit nothing — the same gap a failed REST cycle leaves. Per-field
last-update instants are recorded as **diagnostics** in `status.json`; no max-age
threshold was invented.

**The REST path remains selectable and fully supported.** `LEVITON_INGEST=rest` takes
byte-for-byte the code path that shipped before this change — `leviton_ws` is not even
imported, no socket is built, and no background task is added. Every pre-existing Leviton
test passes against it unmodified (they were written before the socket existed and none of
them was touched), and `test_rest_mode_never_builds_a_socket` pins it. It is the instant
revert: an `.env` edit and a restart, no code change and no rebuild.
`hybrid` (the default) prefers the store and falls back to REST on a shut gate, so its
worst case is exactly the old behaviour; `ws` gaps instead of falling back, which is what
makes it a measurement.

**`pollBreakers` was investigated and REJECTED.** `rwoldberg/ldata-ha` documents that a
Poll request refreshes CT/breaker **lifetime** (cumulative energy) values only, and that
*"as of WHEM firmware v2.1.0 these lifetime values no longer function"*. These hubs are
2.1.2, and §6.3 already excludes `energyConsumption`/`energyImport` for exactly that
reason. Nothing in this pipeline sends `pollBreakers`; a test greps for it.

**`bandwidth: 0` is still never sent**, even though the reference integration sends it as
the middle of a `1 → 0 → 1` cycle and attributes faster CT cadence to it. §6.4's hard
constraint stands: firmware 2.1.0 disconnects a hub for 10–20 seconds on receipt, these
hubs are 2.1.2, and the owner has not authorised it. The references also report that the 0
step makes the cloud emit transient zeros — which this pipeline would archive **verbatim
as real readings** (§2.3), turning a keepalive optimisation into fabricated data. The §6.4
keepalive is unchanged (`PUT {"bandwidth": 1}` every 50s, connected hubs only, exponential
backoff); it is merely also fired once immediately before each connect, because that PUT
is what triggers the flood that seeds the store. `sources/leviton.py` still has exactly
one bandwidth call site and the AST test that pins it still passes.

## 145. `LEVITON_WS_URL` cannot be honoured, and says so loudly rather than being inert

`aioleviton` 0.3.3's `LevitonWebSocket.connect()` passes `const.WEBSOCKET_URL` to
`ws_connect` as a literal; neither `create_websocket()` nor the constructor accepts a URL.
Honouring the setting would mean mutating a third-party module global process-wide, which
nobody authorised. So `LevitonAdapter.ws_transport_factory(url)` compares the setting to
the library constant and, when they differ, logs **ERROR** `leviton_ws_url_not_honoured`
naming the endpoint actually in use. The default value matches and says nothing. Pinned by
a test. A silently ignored setting is worse than an absent one; if the endpoint ever
really moves, vendoring `connect()` at the adapter seam is the fix.

## 146. A build-time capability gap degrades to REST; a runtime freshness failure does not

A client with no `create_websocket()` degrades **both** `hybrid` and `ws` to REST-only,
with one ERROR line (`leviton_ws_unavailable`) and `ws_available: false` in `status.json`.
Strictly, `ws` mode should then gap forever. Permanent, self-inflicted data loss over a
*build capability* gap is the worse failure, so the line is drawn explicitly:

- a **capability** gap (the library cannot make a socket at all) degrades and shouts;
- a **runtime** freshness failure (drop, stall, unsynced, auth-suspect) honours the mode
  and gaps in `ws`.

This is also what lets every pre-existing REST test pass unmodified — the fake client has
no `create_websocket` — but it is a designed rule, not a test convenience.

## 147. The `ws`-mode gap **raises** rather than returning zero rows

Returning `[]` would have the poller record a SUCCESS with 0 rows, keeping `/healthz`
green while nothing was collected. Raising `SourceTransientError` makes it a counted
failure in `consecutive_failures` and in `status.json` — the same accounting a failed REST
cycle gets. "We do not know the value" is the same fact either way, and it should be
reported the same way.

## 148. Hybrid's REST fallback knowingly re-introduces the stale cache

This is the honest cost of `hybrid` and it is stated rather than hidden: on a shut gate,
`hybrid` reads REST — the very cache this change exists to escape. It is never silent. A
per-cycle counter (`cycles_rest_fallback`), the reason (`ws_withheld_reason`) and an INFO
log line on **every transition** of `value_source` make a fallback stretch reconstructable
afterwards. The per-cycle line is DEBUG and only transitions are INFO, so at production
`LOG_LEVEL=INFO` a reader gets intervals rather than a line every 30 seconds.

**Rows themselves carry no provenance.** The canonical schema (§3) is unchanged and gains
no column: provenance is a property of the collector, not of the measurement, and adding a
column would change the dedupe key and every downstream table. The README says so where a
reader will meet it.

## 149. New `status.json` section `leviton_ingest`

A sibling of `leviton`, alongside the existing `leviton_keepalive` / `leviton_auth` and
the new `leviton_ws`. §11 permits sections created on demand (#20). Cost: a second
whole-document rewrite per poll cycle. `status.json` is rewritten whole and atomically; at
30s that is negligible, but it is a change in write volume and #136 fixed a defect in this
exact area, so it is recorded.

## 150. A defect in the freshness layer is a shut gate, not a failed cycle

An exception raised by the ingester during sampling is caught in `_sample_snapshots` and
treated as a shut gate with reason `ws_error` (REST fallback in `hybrid`, a gap in `ws`)
rather than failing the cycle. The socket is an optimisation over a REST path that already
works; a bug in it is a reason to stop trusting its **values**, never a reason to stop
collecting.

## 151. `reconcile_round` measures drift through the real mapper

`hybrid`'s periodic full REST re-read does double duty: it keeps the structural skeleton
current between hourly discoveries, and it **measures the premise of this whole change**.
Both value sets are mapped through the same `_map_snapshot` into a throwaway `PollCycle`
and the number of disagreeing metrics is recorded as `last_reconcile_drift` — a direct
per-round reading of how far behind the REST cache runs. The rows are counted and
discarded; they never reach the spool. Building a second, private view of the mapping to
do the comparison is exactly the drift the module refuses to have, hence the reuse.

## 152. `WS_TICK_INTERVAL_S` duplicates `leviton_ws.WATCHDOG_INTERVAL_S`

`leviton_ws` imports the reading dataclasses from `leviton`, so the constant cannot be
imported back without a cycle. A tick slower than the watchdog would delay every reconnect
and every stall detection by the difference, silently — so a test pins the two equal.

## 153. An explicit `null` in a delta CLEARS the field (both references do the opposite)

`{"activePower": null}` is treated as *"the API said unknown"*, so the field's value is
cleared and the sampler emits no row for it. **Both** reference integrations do the
opposite — they keep the cached value — which is a hold-last-value at *field* granularity,
i.e. CLAUDE.md rule 1 territory one level below the connection gate. Our REST path already
emits no row for a null field (§6.5), and this keeps the two ingestion paths identical,
which #144's single-mapper claim depends on.

The risk is the mirror image: if the cloud sprays nulls, this gaps real data. That is
**unmeasured** — no evidence exists either way about how often nulls arrive on the socket.
So null deltas are counted per field (`null_deltas_by_field` in `status.json`) and the
alternative policy is implemented and one constructor argument away
(`NULL_POLICY_IGNORE`), to be chosen later on evidence rather than taste.

## 154. Post-reconnect seeding: REST **and** a flood-or-timeout hold

The gate requires a full state sync, and the flood cannot establish one on its own: it is
a burst of ordinary *partial* notifications whose union is only hopefully complete, and no
reference implementation depends on it being complete. So every (re)connect runs
keepalive → connect → subscribe → **seed the store from one REST snapshot** → hold
emission until either every subscribed object has been touched by the flood or
`SYNC_FLOOD_TIMEOUT_S` (20s) expires, recording which happened as `sync_mode`.

The honesty cost is explicit rather than accidental: **the REST snapshot is itself the
stale cache this change exists to escape.** Seeding from it means the first samples after
each reconnect can be minutes old while the collector believes it is fresh. At a 55-minute
reconnect cadence that is a small fraction of samples, and it is bounded and measurable —
seeded fields carry `ts_source="rest_seed"`, so the exposure is visible per field in the
diagnostics rather than invisible. A delta that arrives before the seed is never
overwritten by it (`only_if_older_than`).

## 155. `GET /apiversion` every 10s is NOT implemented

Both `rwoldberg/ldata-ha` and the official app (per its HAR) issue this third keepalive
while a socket is up, and ldata attributes it to keeping the cloud *"honoring the
bandwidth:1 setting for v2 firmware"* — which makes it the most plausible cheap
explanation for the measured "the PUT lands and nothing changes" symptom. It is a new
outbound call pattern not in §6.4 and was not authorised, so it is not built. It is the
first thing to try if the socket connects but the feed stays slow, and it needs owner
sign-off.

## 156. Runtime and health wiring stayed deliberately small

The socket reaches the event loop through the **existing** `BackgroundTask` seam that
already carries the keepalive (`leviton_ws` at 15s, plus `leviton_rest_reconcile` in
`hybrid`), so it inherits `run_background_task`'s log-and-absorb guarantee and
`Poller.close()`'s teardown without new supervision machinery. `tick()` never raises.
`runtime.py`'s only changes are its docstring and `leviton_ingest` on the
`runtime_starting` log line. A bespoke WS task path in `runtime.py` would have been a
second, weaker copy of a seam that already works.

`/healthz` still judges **pollers only**, so a dead socket cannot turn the container
unhealthy while REST is landing rows. In `ws` mode a sustained socket failure *does* reach
`/healthz`, but through the honest route: withheld cycles are counted failures, so the
poller itself goes stale.

## 157. Per-field diagnostics are written **into** `status.json`, not left behind a method

Found during reconciliation: `field_diagnostics()` existed and was correct, but
`_publish_status` called `status_snapshot()` without `include_fields`, so the per-field
last-update instants never reached `status.json`. The design brief for this step says
those instants are to be recorded *in `status.json`* precisely so the update distribution
can be measured before anyone claims the socket improved freshness — and a diagnostic that
lives only behind a Python method call is a diagnostic nobody reads at 3am. The watchdog
now publishes `include_fields=True`, so the section carries an `objects` map: per object,
per field, the age, update count, provenance (`receipt` / `server` / `rest_seed`) and
last-update timestamp.

Cost: at ~40 channels the map is tens of kilobytes in a file that is rewritten whole and
atomically on every watchdog tick. That is nothing on a machine CLAUDE.md describes as not
performance-sensitive, and it buys the only evidence that can settle whether this entire
step worked. The rule it must not break is pinned by the test alongside it: these values
are **reported, never consulted** — `freshness()` does not read them, because field age is
not a gate (#144).

## 158. The two gates were verified by REVERTING them

A gate with no failing test behind it is not a gate, so both were broken on purpose and
the suite was watched.

**The emission gate.** `can_sample()` was edited to `return True` unconditionally.
**17 tests failed.** The two that matter are end-to-end, at the rows:

- `test_hybrid_falls_back_to_rest_on_a_dead_socket_and_records_it` failed
  `assert 240.5 == 121 ± 1.2e-04`. 121 V is the REST fixture; **240.5 V was the last value
  the socket pushed before it dropped**, re-emitted with a current `ts_utc` — a
  hold-last-value across a known-dead connection, which is precisely CLAUDE.md rule 1.
- `test_ws_mode_emits_a_gap_rather_than_falling_back` failed `DID NOT RAISE
  SourceTransientError`: with the gate off, a disconnected socket produced a full set of
  rows instead of a gap.

**The silent-stall guard.** The stall branch in `freshness()` was disabled on its own,
leaving every other check intact. Before this reconciliation pass that broke exactly
**one** test — a unit-level assertion in `tests/test_leviton_ws.py` — which is thin cover
for the most dangerous failure mode in the design, since `connected` stays `True`
throughout and nothing else notices. Two end-to-end tests were added
(`test_hybrid_will_not_sample_a_socket_that_is_open_but_silent` and
`test_ws_mode_gaps_rather_than_sampling_a_stalled_socket`), and with the guard disabled
all three now fail. A probe run with the guard off recorded what the archive would have
received: `panel_leg_a volts=240.5 value_source=ws connected=True can_sample=True` — the
frozen store's last word, re-emitted every 30 seconds and **labelled fresh**.

Both experiments were re-run against the final code, and both files were restored
byte-for-byte afterwards — verified by MD5 against a copy taken before each edit, not by
eye — with the full suite green after each restore.

The **single-mapper** claim was verified the same way.
`test_exactly_one_function_in_the_package_maps_a_leviton_row` walks the AST of both
modules and asserts that exactly one function anywhere — `LevitonSource._map_snapshot` —
calls `PollCycle.add` / `PollCycle.add_metrics`.
A decoy second mapper was added to `sources/leviton.py` to confirm the test bites; it did,
naming the decoy. That is the structural half; the behavioural half is
`test_a_ws_sourced_cycle_and_a_rest_sourced_cycle_map_identically`, which asserts a
WS-sourced cycle and a REST-sourced cycle produce identical rows in identical order,
including every gap.

## 159. "Connection state" was sloppy in three places, and each one was a hold-last-value

An adversarial pass against the module found six defects, five of which are the same
mistake in different clothes. #144's rule — *connection state gates emission, field age
never does* — was enforced on the **socket** as a whole, when it is really a property of
each **field** and of each **hub's feed**. Every one of these produced a value we do not
currently know, stamped with a fresh `ts_utc` and labelled `value_source="ws"`.

**(1) A field must have been established on the connection we are sampling from.** The
store is deliberately not cleared on reconnect (`status.json` shows the last known state),
and the timeout path opens the gate for objects the flood never touched. So a reconnect
whose REST seed failed (a 502, which §6.6 documents as routine) published the *previous*
connection's numbers as current. `StateStore.evict_before(mark)` now runs from
`_mark_synced` with `mark = self._connected_at_monotonic`, dropping every field the new
connection did not re-establish at the instant the gate opens, and counting them as
`fields_evicted`. This is **membership, never age** — a value pushed 55 minutes into a
connection is welcome to stay, and
`test_the_eviction_is_connection_membership_and_never_a_max_age` pins that with 55 minutes
of hub-only chatter and `fields_evicted == 0`.

**(2) An explicit `null` in the REST seed clears, rather than being dropped.** #153 already
says a null delta clears the field; the *seed* path was dropping nulls on the way in, so it
could overwrite a carried-over value but never remove one — meaning a channel whose current
REST value is null kept the previous connection's number where the REST path emits no row
at all. `_seed_deltas` now keeps an explicit `None` for the measurement keys
(`_SEED_MEASUREMENT_FIELDS`, the union of the three overlay maps) and still drops it for
`WS_STATE_FIELDS` (`connected`/`currentState`), because those are control state — clearing
`connected` would silently stop the keepalive for that hub. `StateStore.apply` gained
`count_nulls=False`, used only by the seed, so #153's `null_deltas_by_field` keeps
measuring how often the **socket** sprays nulls rather than conflating REST's.

**(3) Liveness is per hub, not per socket.** Two hubs share one socket in this house, so
the aggregate "any frame from anyone" stall watchdog is satisfied by whichever hub is
healthy while the other one's push feed is dead — and that hub's channels were being lifted
out of a frozen store. `SubscriptionTarget` now carries `hub_id` (`compare=False`, so
`.key`, equality and hashing are unchanged), `freshness()`/`can_sample()`/`is_fresh()` take
an optional `hub_id`, `overlay_snapshot` gates on the snapshot's hub and `sample_object` on
the object's owner. The zero-argument call is now the **strict aggregate** — every tracked
hub must be alive — because `overlay_snapshots` maps all hubs into one cycle and must not
publish a dead one's last words. `tick()`'s stall branch fires on the worst hub, so a dead
feed forces the reconnect that is the only way to recover a subscription. `status.json`
gained `hub_silence_s` and `stalled_hubs`.

## 160. The wait set, the backoff ladder and `sync_mode` were all describing what *happened*

The other three defects, which are about the module lying to itself and then to the owner.

**(4) The flood wait set is derived from what we WANT, not from what subscribed.** A
per-target subscribe failure removed that target from `_awaiting`, so the connection could
declare the strong `flood` sync while that object was never subscribed and would never be
pushed — a silent, whole-connection data outage for one channel that nothing above noticed.
`_connect_once` now sets `self._awaiting = set(self._targets)` **before** subscribing, and
`_retry_pending_subscribes()` (desired-but-not-subscribed) runs every watchdog tick and from
`set_targets` — which now drives subscription off "desired but not subscribed" rather than
the added/removed diff, so the hourly discovery pass repairs a failure even when the target
set is identical. New counters: `subscriptions_pending`, `subscribe_failures`.

**(5) A handshake that completes has proved nothing.** `_attempt_connect` reset the backoff
ladder whenever `_connect_once` returned, and the server-drop path did not back off at all,
so a server that accepts every connection and drops it a second later was retried by the
next tick with no delay — a hot reconnect loop against Leviton, one layer below §6.1's
"never log in more than once per 10 seconds". The ladder is now cleared only in
`_mark_synced` (a connection that re-established state on a live socket), and
`_handle_disconnect` counts `server_drops`/`reconnects` and calls `_back_off()`. Measured
with a probe: against a server that drops every connection, 20 seconds of ticks built **21
connections** before this fix and **4** after it.

**(6) `sync_mode` must not say `flood` unless a flood happened.** The flood branch read
`not self._awaiting`, which is trivially true when there was nothing to await, so a
connection that subscribed to nothing and received nothing — sampling a pure REST cache —
reported the strongest possible answer. `sync_mode` is exactly the signal the owner has been
told to read to decide whether the socket works, so the branch now reads `if self._targets
and not self._awaiting`, the timeout branch reports `SYNC_MODE_TIMEOUT` unconditionally, and
`_seed_from_rest` sets `_seeded = (fields applied > 0)` so an empty or all-null seed is not
state.

**BEHAVIOUR CHANGE OUTSIDE THE MODULE:** `reconnects` in `status.json` now also counts
server-side drops, not only deliberate `_cycle()` calls. A drop *is* a reconnect and that is
the number the operator reads for churn; `server_drops` separates the two causes.

## 161. The WebSocket handshake now carries the `Origin`, and nothing was vendored

Preventative, not a found defect: `aioleviton` 0.3.3's `LevitonWebSocket.connect()` calls
`self._session.ws_connect(WEBSOCKET_URL, heartbeat=..., headers={"user-agent": USER_AGENT})`
— a hardcoded literal with **no** `Origin` and no hook — while both other implementations of
this protocol send one and §6.1 records that Leviton appears to fingerprint callers (our REST
adapter already injects it). It was listed as one of the two suspects if the first live
handshake fails.

Copying `connect()` would have meant owning the auth frame, the ready-status handshake and
the listen-task lifecycle forever, so instead the single attribute that call reads is
wrapped: `_HandshakeHeaderSession` forwards everything to the real `aiohttp` session and only
rewrites `ws_connect`'s headers (ours win the merge, and an `origin=` kwarg is dropped
because aiohttp applies it *after* the merge). `apply_ws_handshake_headers` is idempotent and
degrades to a WARNING plus an unmodified object if `_session` ever disappears — a missing
`Origin` may well work, and refusing to connect over a suspicion would be the worse failure.
The default is literally `sources.leviton.LEVITON_ORIGIN` (one source of truth, pinned by a
test), plus `referer` and `aioleviton`'s own `USER_AGENT` read from the installed package.

**The vendoring note is the `_HandshakeHeaderSession` docstring, and it is executable**:
`test_the_aioleviton_handshake_seam_is_still_where_we_reach_into_it` asserts upstream's
`connect()` still builds the handshake from `self._session` and still sends no `Origin` of
its own, naming what to re-check if either changes. This also adds a new import edge
(`leviton_ws` → `sources.leviton.LEVITON_ORIGIN`); that direction already existed for the
reading dataclasses, so there is no cycle.

## 162. #159–#161 were verified by reverting them, and by measuring how they compose

The #158 precedent, repeated: a fix with no failing test behind it is not a fix. Each fix was
broken in isolation by a scripted anchor replacement, the suite re-run, the failing set and
its message recorded, and `leviton_ws.py` restored byte-for-byte — verified by MD5
(`22e085aed89e727f91b850cb326ccb52`, identical before and after every experiment), not by
eye.

- **(1) per-field membership** → **3 failures** in the full suite. The end-to-end one is at
  the rows: `AssertionError: 240.5 was established on the PREVIOUS connection; re-emitting
  it with a current ts_utc is a hold-last-value across a disconnect` / `assert 'volts' not
  in {'volts': 240.5, 'hz': 60.0}`.
- **(2) a null clears** → **3 failures**, the end-to-end one reading `REST said the leg is
  unknown; a seed that can overwrite a value but never remove one leaves 118.0 standing for
  the sampler to publish` / `assert 'rmsVoltageA' not in {'rmsVoltageA': 118.0, ...}`.
- **(3) per-hub liveness** → **6 failures**. A probe run with the fix reverted recorded what
  the archive would have received: `1000_BBBB_2222 panel_leg_a volts=250.0 value_source=ws
  can_sample=True`, four minutes after that hub's feed went silent, while its sibling's
  chatter kept the aggregate watchdog happy. With the fix in place the same probe reports
  `can_sample(B)=False value_source=rest_fallback rows_carrying_250.0=[]`.
- **(5) backoff** → **2 failures**, the first being `AssertionError: the next attempt waits
  for the backoff / assert 2 == 1` — a second transport built immediately after a server
  drop. The probe numbers are in #160.

**How they compose**, which is the thing separate fixes get wrong.
`test_a_dead_hub_a_reconnect_and_a_null_seed_compose_into_gaps` runs all three in one story
at the rows: Panel B's feed dies while Panel A chatters, the reconnect that its silence
forces lands while REST is 502ing, and only the one CT the flood reaches is re-established;
then REST returns reporting Panel A's leg as unknown and another reconnect seeds from that
null. It fails under each of the three reverts, with a different message each time.

Two honest findings from that exercise, neither of which changed the code:

1. **Fix (2) is subsumed by fix (1) at the rows, and is defence in depth.** When the seed
   succeeds it covers every measurement field of every object REST reports, so a null clears
   what the eviction would have dropped anyway; when the seed fails or omits an object, that
   object also leaves the structural skeleton (`_rest_snapshot` updates `self._snapshots`), so
   it emits no rows either way. The place fix (2) is independently observable is the **store**
   between connect and sync — which is what `status.json` publishes — so it is pinned there
   (`peek_object` shows the field gone, `fields_cleared` went up) rather than with a
   row-level assertion that would pass for the wrong reason.
2. **A hub whose feed is permanently dead still gets a fresh grace period on every
   connection.** `_hub_activity` is reset at connect for every tracked hub, so after each
   reconnect a dead hub reads as silent-for-0s and its gate is open again until
   `LEVITON_WS_STALL_TIMEOUT_S` elapses. What it publishes during that window is the REST
   seed, i.e. exactly #154's documented and accepted exposure — the *previous* connection's
   pushed values are gone, evicted by fix (1), so this is a cached value and never a
   fabricated one. It is left as is because the alternative (carrying silence across a
   reconnect) would mean a hub that was quiet for an unrelated reason could never re-open
   its own gate. In `hybrid` the cost is nil (the fallback is that same REST read); in `ws`
   it is ~90s of REST-cached values per reconnect for a hub that is not pushing, which
   `hub_silence_s` makes visible. Pinned by the composition test, which asserts
   `stalled_hubs == []` immediately after the reconnect *and* that no row carries the dead
   hub's last pushed value.
3. **The eviction boundary is `<`, so a frame stamped at *exactly* the connect mark counts
   as belonging to the new connection.** This is deliberate — the REST seed is stamped at or
   after the mark and must survive — and on a real ns-resolution `time.monotonic()` a frame
   received before `connect()` returned cannot tie with it. It is visible only with a frozen
   test clock, where pushing a frame and reconnecting at the same instant kept the frame; the
   composition test advances one second before the watchdog fires, which is what actually
   happens. Recorded because the alternative (`<=`) would evict the seed and is the more
   attractive-looking of the two.

`status.json`'s truthfulness for the first live run is pinned through the **real**
`StatusStore`, written to a file and re-read as JSON, by
`test_the_status_file_the_owner_reads_tells_the_truth_about_both_hubs`: a connection whose
flood covered only Panel A reports `sync_mode: "timeout"` with `awaiting_sync: 3`; ten
minutes of Panel-A-only traffic produces `stalls: 1` and a reconnect; and only a flood that
covered both hubs' objects earns `sync_mode: "flood"` with `hub_silence_s` showing both hubs
at 0.0.

---

# Step 10: deployment on Apple's `container` (#163)

## 163. PLAN.md §5 says "One Docker container (compose, `restart: unless-stopped`)" — there is now a second, non-compose path

**The spec.** PLAN.md §5 opens: *"One Docker container (compose, `restart:
unless-stopped`), one long-running process (`energycap run`)"*, and §16's definition of
done names `docker compose up` on the Mac Mini. This is not a §2 locked decision, but it
is a §5 statement about the runtime, so it is recorded here rather than quietly diverged
from.

**What changed, and why.** The deployment target is an Apple-silicon Mac Mini, and Apple
shipped [`container`](https://github.com/apple/container) v1.0.0 on 2026-06-09: a native,
lightweight, per-container-VM runtime that is the better fit for this machine than Docker
Desktop. It builds this repo's `Dockerfile` unchanged. So there are now **two runtimes,
one image**:

- `scripts/energycap-container.sh` + `deploy/com.duckbillhq.energycap.plist` — Apple
  `container` supervised by launchd. **Preferred on this Mac.**
- `docker-compose.yml` — **unchanged, still correct, still supported**, and still the
  **only** option on an Intel Mac or a Linux box. Nothing was removed or deprecated.

`Dockerfile` was not modified either. `init: true` has no equivalent under `container`
and needs none (`runtime.py` installs its own SIGTERM/SIGINT handlers and spawns no
children to reap); `HEALTHCHECK`, `EXPOSE` and `STOPSIGNAL` are inert under a runtime with
no healthcheck concept rather than wrong; and the health server already binds `0.0.0.0`,
so `-p` reaches it.

**Why compose could not simply be carried over.** Apple's `container` has **no compose,
no `restart:` policy of any kind, no healthcheck, and no `depends_on`.** Each of those is
a real loss, and each one is either replaced or admitted:

| `docker-compose.yml` | Replacement under `container` |
|---|---|
| `restart: unless-stopped` | **launchd `KeepAlive`** (plain `<true/>`: restart on any exit) with `ThrottleInterval 30`. Semantics differ: compose's `unless-stopped` remembers a manual stop; KeepAlive does not, so `launchctl bootout`/`disable` is the manual stop. |
| `healthcheck:` | **Nothing automatic — an admitted regression.** `/healthz` still returns 503 when a poller's last success is older than 3× its interval (§11), but nothing polls it and nothing acts on it. KeepAlive only ever sees the process *die*, so a wedged-but-alive collector goes unnoticed until someone looks. `deploy/README.md` sketches the `StartInterval` watchdog that would close this; it is not built. |
| `stop_grace_period: 30s` | **`container stop --time 30`**, issued from a TERM/INT trap in the wrapper, plus **`ExitTimeOut 45`** in the plist so launchd's default 20s does not SIGKILL the wrapper mid-shutdown. |
| `init: true` | Nothing needed — see above. |
| `depends_on` | Not used; there is one service by design (§5: no database, no queue, no sidecar). |
| named volume `energycap-data` | **A host bind mount** (`-v <repo>/data:/data`). Upside: `spool.db` is directly readable from the Mac. Downside: the uid trap below. |
| `env_file:` / `ports:` | `--env-file` / `-p`, both supported. |
| `logging: json-file, max-size 10m, max-file 5` | **Not replaced — a real regression.** launchd appends to `StandardOutPath` forever. `deploy/README.md` gives a `/etc/newsyslog.d/energycap.conf` stanza, but installing it needs `sudo` and is an operator action, not something the wrapper can do. |

**The supervised process runs the container in the FOREGROUND, and that is
load-bearing.** launchd supervises a *process*, not a container. `--detach` in
`ProgramArguments` would make `container run` return instantly, the wrapper exit 0,
KeepAlive restart it, and the result is a throttled restart loop with several collectors
racing over one SQLite spool — not a supervisor. It is written down in the script, the
plist and `deploy/README.md`.

**It is a LaunchAgent, not a LaunchDaemon,** because the `container` subsystem is itself a
per-user launchd service (`com.apple.container.*`): root would see a different subsystem
that knows nothing about the image. The consequence is that the collector only starts
once the owning user has logged in, so an unattended Mac Mini needs auto-login (**which
FileVault makes impossible**), `pmset autorestart 1` and `pmset sleep 0`. That trade —
at-rest disk encryption against unattended restart — is the operator's to make
consciously; `.env` and the token caches live on that disk.

**Three failure modes this deployment has that compose did not**, all handled in the
wrapper and documented:

1. **The name-collision wedge.** `--rm` should drop the container record, but an
   ungraceful shutdown (launchd's `ExitTimeOut` expiring, a power cut) can leave the name
   taken. Compose restarted the *same* container; KeepAlive issues a fresh
   `container run` every time, so one stale record would block **every** restart, once per
   `ThrottleInterval`, forever — a permanent data outage from a power cut. `run` now
   tries `container stop`, then whichever of `delete`/`rm`/`remove` **`container --help`
   actually advertises**. That subcommand is not in the reference this was built against,
   so it is probed, never assumed; nothing invented is ever on an executable path.
2. **The uid trap.** The image runs as uid 10001. Docker's named volume sidestepped
   ownership entirely; a virtiofs bind mount does not, and how it maps uids is
   **unverified**. Worse, `data/status.json` and `data/tokens/` are mode 600 owned by the
   host user after a host-side `discover`/`poll --once` (cardinal rule 8), which uid 10001
   cannot touch. The wrapper checks and **warns loudly rather than refusing to start** —
   the mapping may well be permissive, and hard-failing on a guess would be worse.
3. **`--env-file` promises less than Docker's.** The whole documented contract is
   "key=value format, ignores `#` comments and blank lines": no quote stripping, no
   trailing-whitespace trimming, no `${VAR}` interpolation. A `TZ_LOCAL` with an inline
   comment would silently mis-assign every LOCAL-date partition (cardinal rule 4). The
   committed `.env.example` is already clean; the wrapper lints a hand-edited `.env` on
   every run and reports **key names and line numbers only, never values**.

**Preflight severity is deliberately split.** A non-zero exit from
`container system status` is fatal; output that merely *looks* stopped only warns. We have
never seen what that command prints, and a wrong guess about its wording, made fatal, would
turn a healthy machine into a permanent outage under KeepAlive. The real `container`
command a moment later gets to be the authority.

**`plutil -lint` is not an XML check, and it hid a real defect.** The template passed
`plutil -lint` cleanly while its comments still contained `--`, which the XML
specification forbids inside a comment. Apple's parser is lenient; Python's `plistlib` and
`xmllint` are not, and either rejects the whole file. The comments now spell the flags out
in words, and `tests/test_deploy.py` parses the template with `plistlib` precisely so a
lenient linter cannot certify a malformed file again. That file is the only automated
check on any of this: `bash -n` on the wrapper, the strict plist parse, and the two
mistakes that would otherwise be silent — a detach flag reaching `ProgramArguments`, and an
`ExitTimeOut` that does not outlive the wrapper's 30-second graceful stop.

**None of it has been executed.** There is no `container` CLI and no Docker daemon on this
machine, so **the image has never been built by either runtime** and the LaunchAgent has
never been loaded. What was verified is only what could be: `bash -n`, `plutil -lint`, and
every host-side code path driven end to end against a stub `container` on `PATH` —
preflight failures, the uid warning, the `.env` lint (including a decoy file with a quoted
secret, which was never printed), `HEALTH_PORT` parsing, argv boundaries through a data
directory path containing a space, all three name-collision recovery paths, and
foreground SIGTERM → `container stop --time 30` → exit 0. `deploy/README.md` leads with a
table of every assumption that is still a guess.

## 164. PLAN.md §11's health server serves `/healthz` only — it now also serves `/ui` and `/ui/data`

**The spec.** PLAN.md §11 specifies one HTTP surface: *"a small HTTP health server"* serving
`GET /healthz` (plus the aliases `/health`, `/`, `/status.json`) with the status document
and a derived `health` block. No other route is contemplated.

**What was built.** The same `HealthServer`, on the same port, now also answers two
**read-only** dashboard routes owned by `energy_capture/dashboard.py`:

| route | serves |
|---|---|
| `GET /ui` | `static/dashboard.html` — one self-contained file: no framework, no build step, no CDN, no external asset of any kind. It renders with the network unplugged. |
| `GET /ui/data` | a JSON snapshot of the live spool plus `status.json`, which the page polls every 5s. |

**Why.** Between the spool and S3 this pipeline is invisible. `status.json` says whether a
poller succeeded; it says nothing about *what the house is doing*, and the measurements
themselves only become queryable once they are Parquet in a bucket — hours later, through
DuckDB or Athena. There was no way to stand in the kitchen and watch the numbers move, and
no way to see the one thing this project is built around: that a gap is visible as a gap.
The page shows the live values, the last 30 minutes per channel, the `<=3` biggest watt
channels overlaid, the HVAC readings with the enums decoded to words, and the last six
local hours aggregated with `sample_count` / expected / coverage / kWh beside every mean.

**Why on the health port rather than a second one.** A second port means a second
`-p`/`ports:` mapping in two deployment paths, a second thing to firewall and a second
listener to supervise, for a page the same process is already able to serve. PLAN.md §5's
"one container, one process" argues for one socket.

**What it costs.** Nothing measurable, and deliberately so:

- **Zero new dependencies.** `pyproject.toml` and `uv.lock` are untouched. The module
  imports only the standard library (`sqlite3`, `json`, `threading`, `importlib.resources`)
  plus `energy_capture` itself. The page is vanilla JS and inline SVG.
- **Read-only, on its own connection.** The spool is opened `mode=ro` **and**
  `PRAGMA query_only`, with a 2s busy timeout, and closed per request. A test asserts a
  write through that handle fails. Nothing in `dashboard.py` writes anything, anywhere.
- **Off the event loop.** `build_snapshot` runs in a worker thread
  (`asyncio.to_thread`), because the loop serving this page is the loop running the poll
  loops, the keepalive and the WebSocket reader. A browser refreshing every 5s must not be
  able to stall collection — least of all when the uploader is failing and the spool is
  growing, which is exactly when someone is watching.
- **`/healthz` is unchanged, byte for byte.** The UI paths are matched before the status
  paths and share no code with them; a test pins the existing behaviour of every path in
  `DEFAULT_HEALTH_PATHS`, and every failure inside the dashboard becomes a JSON 500 on the
  UI route rather than anything the probe can see.
- **The kWh math is not reinvented.** `dashboard.hourly_rollup` mirrors `stages/rollup.sql`
  step for step — bucket on `hour_start_utc`, exclude `DAY_GRAIN_METRICS`, no row for an
  empty hour, `kwh` for watts only and `NULL` otherwise — and `dashboard.KWH_FORMULA`
  quotes the formula verbatim so a future editor changing one notices the other. It is a
  documented mirror, not a second definition: the SQL is DuckDB over Parquet with two
  registered relations, this is live SQLite rows.

**The route is unauthenticated, like `/healthz`.** It exposes the same status document
plus measurements, and nothing else: no `.env` value, no token, no cache from
`/data/tokens`. That was checked by diffing the served JSON against every value in `.env`
and every field of both token caches — only the HVAC equipment serial (already the
`device_id` of every Bryant row) and the timezone name appear. Bind the port to the LAN
you trust, exactly as for `/healthz`.

**One operational warning learned the hard way.** SQLite's shared-memory locking is not
coherent across a VM or network-filesystem boundary. `deploy/`'s bind mount makes
`data/spool.db` visible on the Mac, but reading it **from the host while the container is
writing it** — including with `mode=ro`, which still writes the `-shm` WAL index — can
report `database disk image is malformed`, and can cause it. Read the spool from inside
the container, or use `/ui/data`, which is served by the process that owns the file. The
snapshot builder now says so in the error it puts on the page.

## 165. `/ui/data` was a fixed 30-minute live window — the watts chart's window now moves, and long windows are bucketed on the server

**The spec.** #164 introduced `/ui` and `/ui/data` with one hard-coded window: the last 30
minutes, always live, always raw 30s samples. PLAN.md contemplates neither the route nor a
window, so this is an extension of #164 rather than a divergence from §11.

**What was asked for.** Verbatim: *"I want to be able to scroll back in time in the Watts
graph you have running. Like, let me look back 24 hours at most (I know we don't have that
much data yet, but just something to start with)."*

**What was built.** Presets, panning, and a way back to live — and nothing else. No brush,
no minimap, no zoom gesture, no date-picker: the owner has asked twice for restraint.

- **Page.** `30m / 1h / 6h / 24h`; ◀ ▶ pan half a window; drag the plot; ← / → pan and
  `Home` goes live (the pre-existing keyboard crosshair moved to `Shift` + ← / →, and the
  hint line under the chart says so). Panning pins the right edge, which stops the chart
  following *now* while the rest of the page keeps refreshing every 5s.
- **Route.** `GET /ui/data` takes two optional parameters: `window_s` (integer seconds,
  clamped to `[60, 86400]`, default 1800) and `end` (ISO-8601 instant at the right edge;
  omitted **is** what live means). Unknown parameters are still ignored — `/ui/data?since=now`
  has always been a 200 and stays one — but a malformed value is a **400** with the
  parameter named, never a silent default: a chart quietly showing a different window than
  the one asked for is a chart that lies about itself. `handle_ui_data(store, target)`
  returns `(status, document)` and is the only thing that can answer 400; `build_snapshot`
  still takes an already-validated request and has no opinion about query strings.
- **`health.py` had to change**, in one place: `_respond_ui` called
  `build_snapshot(self._store)` and threw the request target away, so no parameter could
  ever arrive. It now calls `handle_ui_data(self._store, target)` and returns the status it
  gets back. `_send_bytes` already knew `400 Bad Request`; every pre-existing health and UI
  route test passes as written.

**Where a chart fabricates data, and what happens instead.** 24h at 30s is ~2,880 points
per channel, so the server buckets above an hour (`CHART_RAW_MAX_WINDOW_S = 3600` — the
live view stays exactly as raw and as responsive as it was). Bucketing is the place
CLAUDE.md rule 1 dies quietly, so:

- **an empty bucket is an explicit hole** — `mean`/`min`/`max` `null`, `sample_count` 0.
  Not 0 W, not the previous bucket's value, not an interpolation between neighbours. It
  reaches the client as a `null` and the page breaks the line there into a **new SVG
  subpath**, exactly as it already did for a raw gap. Verified on real data: a 6h window
  over the container restart at 19:29–19:31 UTC leaves the 19:30 bucket empty, and each of
  the three drawn paths comes out with two subpaths, not one;
- **a partial bucket is not a full one.** Every bucket carries its own `sample_count`
  *and* the `expected` count for its span, and the page draws it with a hollow mark in the
  series colour (no new hue) saying "*n* of 5 samples";
- **the mean is `sum(values) / len(values)`** — the mean of the samples that EXIST.
  Dividing by `expected` would drag every partial bucket toward zero, which is
  PLAN.md §2.5's "extrapolate across a gap" wearing a different hat. Hand-checked against
  the raw rows: the last bucket of the live spool held 4 of 5 samples all reading
  801.915 W and came back as 801.915, not 801.915 × 4/5 = 641.5;
- **boundaries are computed on `ts_utc`** (CLAUDE.md rule 3) as epoch multiples of the
  bucket width, so they do not move when the window slides — a refresh only ever changes
  the last bucket — and the DST fall-back day's two 01:00 local hours land in *different*
  buckets. Bucketing on the naive local label would merge an hour of EDT with an hour of
  EST; a test walks both and asserts 24 repeated local labels as 24 pairs of distinct rows.

The width is always a whole number of poll intervals and at least two of them, so
"expected per bucket" is an exact integer and "partial" means something: 24h → 150s
(2.5-minute buckets, 577 of them, 5 samples each), 6h → 60s. One bucket per poll interval
would leave real buckets empty on ordinary jitter and draw holes that are not holes — the
opposite failure to fabricating data, and just as much a lie.

**The axis is drawn from the answer, never from the question.** Every label — window,
resolution wording, bucket width, tick times — comes from the document the server returned.
If it disagrees with what the page asked for, the range bar says *"Window not applied —
this server answered with its own window"* instead of drawing the server's data under the
page's labels. A 24h window can straddle a DST change, so each x-tick borrows the
`utc`/`local` offset of the nearest point (which came from `timeutil`) rather than one
page-wide offset.

**Panning stops at the data, and says so.** `spool.extent` reports the oldest and newest
`ts_utc` the spool still holds. The stretch a window covers that the spool never held is
greyed with `--mark-muted` and named in words ("the spool holds nothing before 15:22
local") — deliberately *not* washed like an outage, because a purged or never-collected
stretch is not a collector failure. The spool is not an archive: rows are purged once
uploaded, so 24h of window will usually be more window than data.

**What it costs.**

- **Zero new dependencies**; `pyproject.toml` and `uv.lock` untouched. Still three
  categorical colour slots and no fourth series; no new CSS custom property; still no
  external asset of any kind, so the page still renders with the network unplugged.
- **No table scan.** The window is ranked with a `GROUP BY` in SQLite so only the three
  drawn channels' rows reach Python. `EXPLAIN QUERY PLAN` against the live spool copy
  reports `SEARCH observations USING INDEX ux_observations_dedupe (ts_utc>? AND ts_utc<?)`
  for both the ranking and the point query — an index seek on the leading `ts_utc` column,
  not a scan.
- **Measured on the running container**: a 24h snapshot answers in ~110 ms and adds
  ~130 kB to the document (~400 kB total); the no-parameter request is ~95 ms, unchanged.
- **The no-parameter document is exactly what it was.** Diffed field by field against
  `HEAD`'s `build_snapshot` output for the same clock and spool: the only differences are
  *added* keys (`spool.chart_rows`, `spool.extent`, and the window fields under `overlay`).
  Nothing pre-existing changed value or position.

**One thing this deliberately does not do.** At 24h a sub-bucket outage — the 133-second
gap in the live spool — does not break the line, because both buckets either side of it
still hold real samples. It is reported as two *partial* buckets (4/5 and 3/5) with hollow
marks and the counts in the tooltip. Widening a hole to cover any gap would be inventing an
absence; narrowing the buckets to catch it would be the 6h view, which is one click away
and does show it as a break.

---

## 166. PLAN.md §13 assumed LG&E Green Button would carry gas — Connect is electric-only

§13 designs the `meter` dataset for both fuels: "gas likely daily therms/CCF", `channel_id`
`gas_main`, unit mapping `ccf_interval`/`CCF`. Researched against the utility on 2026-08-18
(`docs/lge-greenbutton.md`), and the gas half cannot come from Connect:

- LG&E's own [3PV registration PDF][gbpdf] says "Green Button Connect (GBC) allows customers
  to share **electric** usage data with 3rd Party Vendors."
- The registration form's required function blocks are exactly **1** (Common), **3** (Connect
  My Data), **4** (Interval Metering) and **5** (Interval **Electric** Metering). ESPI has no
  gas block in that set, and padding the `FB=` list with blocks the custodian does not
  support is a way to fail scope validation for nothing.

[gbpdf]: https://lge-ku.com/sites/default/files/media/files/downloads/LGE-KU-Green-Button-Connect-Third-Party%20Vendor-Registration-Process.pdf

**Nothing in the schema changes** — `METER_SCHEMA`, `meter_key` and the `dim_channel`
placeholder are fuel-agnostic and never knew about fuels in the first place.

And in this deployment it costs nothing: **the property has no gas service** (owner, 2026-08-18).
So the `gas_main` half of §13 is *moot*, not blocked — there is no meter to export. The
`channel_map.json` note that promised "a 'gas_main' entry joins it if the gas meter is exported
too" has been removed, because a note describing work that will never happen reads as a backlog
item to the next person.

`import-greenbutton` therefore keeps a narrower reason to exist than "the gas path": **bulk
history** beyond whatever `HistoryLength` Connect grants, and the fallback if the vendor
registration is not approved. Both commands coexist; the tempting simplification of "build
Connect, delete the importer" still loses something, just less than it would have.

Also settled by the same research, and *not* deviations, just answers to questions §13 left
open: Connect exists (§13 hedged "if LG&E actually offers it", and told us to assume manual
import first — that hedge is now resolved in Connect's favour); supported granularity is
`IntervalDuration` 900 or 3600 only; a **daily** subscription is available, so the fetch
cadence can match the Bryant daily-energy stage; and registration is a one-shot,
human-reviewed form whose approval email is the only source of the OAuth endpoints — there is
no developer portal and no published base URI, so no client code should be written until it
arrives.

---

## 167. PLAN.md §13 says "**Do not build any of this yet**" — `import-greenbutton` is now built

§13 designs the Green Button source and ends with an explicit instruction not to build it,
only to avoid painting the schema into a corner. That instruction was written when Green
Button meant *Connect*, the OAuth'd API, whose registration is still with the utility awaiting
a human's approval (`docs/lge-greenbutton.md`).

**Download My Data needs no OAuth and no approval.** It is the same ESPI data, exported by
hand from MyMeter today. So the reason for deferring — "there is nothing to import until the
API exists" — turned out not to hold, and the owner asked for the backfill while the
registration is pending. Waiting would have meant sitting on the one measurement that says
whether the sub-metering is trustworthy.

Nothing about §13's *design* was changed to do it: `source='lge'`, `METER_SCHEMA` with
`interval_s`, `MeterObservation`, `meter_key`'s `{source}-{YYYYMM}.parquet` naming and the
`dim_channel` placeholder are all used exactly as specified. The build is an import, as §13
promised it would be.

Three decisions the spec did not cover:

1. **Local Parquet is the default output; S3 is opt-in** via `--bucket`. §13 assumed
   `energy/meter/` in S3, but this deployment has never had a bucket, and an import is a
   manual act on a file a human just downloaded — fanning it out to S3 because an env var
   happened to be set is a surprise. The month files are named exactly as `meter_key` names
   them, so mirroring later is a copy.
2. **The importer refuses to guess at units.** ESPI carries `uom` and `powerOfTenMultiplier`
   in a `ReadingType`; if the export omits it, the import *fails* rather than assuming
   watt-hours. A silent factor of 1000 is precisely the error a meter comparison exists to
   detect, so the comparison must not be capable of introducing one. `--assume-uom Wh` is the
   deliberate override, and it records itself in the run's notes.
3. **Only forward flow is imported.** ESPI `flowDirection` 19 (generation sent back) has no
   metric in `model.METRIC_UNITS` and this house has no solar; those readings are counted and
   reported rather than silently folded into consumption.

## 168. `compare-meter` is new — PLAN.md has no stage that reads two datasets

Every other stage moves data one hop along the pipeline. This one answers a question:
do the two service-feed CT pairs, summed, equal what the utility meter recorded? That is the
whole justification for the sub-metering, and until it is checked, every number this project
produces is unverified.

It is a stage rather than a documented SQL query for one reason: **the panel side must not
re-implement the kWh math.** It hands spool rows to `rollup.rollup_day`, the same function and
the same `rollup.sql` that produce `energy/hourly`, so the comparison cannot drift from what
the warehouse would say — including that `kwh` is observed-time-only.

Two things it deliberately does that a naive comparison would not:

- **It sums `ct_1_a`/`ct_1_b` only.** `panel_leg_*` is *voltage* — summing it in would add
  hundreds of "kWh" that are really volts — and branch breakers are *inside* the feed, so
  adding them would double-count the house. Both are pinned by tests, both verified by
  reverting the filter.
- **It reports `coverage` and excludes low-coverage hours from the totals while still
  printing them.** An hour the collector half observed shows half the panel energy. That
  number is correct — it is what was observed — and reading it as a CT error would be the
  easiest way to draw exactly the wrong conclusion from this exercise. Silently dropping
  those hours would be worse still, so the count of excluded hours is printed.

It reads the SQLite spool directly, so it must run inside the container
(`container exec energycap …`): opening the spool from the host while the collector writes it
corrupts the database.

### What the real export turned out to contain

A live Download My Data export (2026-08-18, 10 days, 2,649 forward readings) settled several
things the design had to guess at:

- **15-minute intervals** (`intervalLength` 900), `uom` 72 (Wh), `powerOfTenMultiplier` 0,
  and every UsagePoint paired with a `flowDirection` 19 (reverse) MeterReading — 2,373
  readings skipped, since there is no generation here.
- **The ReadingType link runs the other way.** LG&E's ReadingType carries `related` pointing
  *down* at its MeterReading; the MeterReading's own `related` points *up* at the UsagePoint.
  The obvious implementation — follow the MeterReading's links to find a ReadingType — finds
  nothing at all. Both directions are now indexed, and both are fixtured.
- **Three UsagePoints carry an identical series.** `1308468`, `944401` and `944006`, equal to
  the watt-hour for every interval of ten days: the same service through meter changes.
  Summing them would treble the meter reading and make the panels look like they measure a
  third of the house. `resolve_meter` collapses an identical series to one and says so in the
  report; genuinely different meters raise rather than being guessed between.
- **`device_id` is the UsagePoint's `name`** (`1308468`) rather than its id (`00121847`) —
  the number on the bill, and the same identity the CSV export prints, so the two formats
  agree on the dedupe key.

First result, over the 13 hours with full sample coverage: meter 46.295 kWh against panels
47.878 kWh — **the feed CTs read 3.4% high**. That is within the combined tolerance of the
clamps and the meter, and it is the first evidence that the sub-metering is trustworthy.

## 169. The live Connect API disagrees with the ESPI spec, and with the download

Three things the first real fetch (2026-08-18) settled, none of which could have been read
off a document.

**1. `published-min`/`published-max` are NOT epoch seconds.** The ESPI spec says they are.
Measured against `Batch/Subscription/00000074`:

| sent | result |
|---|---|
| `published-min=1755230400` (spec) | **400** Bad Request |
| `published-min=2026-08-15` | **400** |
| `published-min=2026-08-15T00:00:00` (no `Z`) | **400** |
| `publishedMin=…` (camelCase) | **200, filter silently ignored** |
| `published-min=2026-08-15T00:00:00Z` | **200, filtered** |

The camelCase row is the trap: it succeeds, and returns the *entire* authorised history — **49
MB against 415 KB** for four days — so a daily job would quietly download fifty megabytes with
a fetch window that means nothing. `_espi_instant()` formats `%Y-%m-%dT%H:%M:%SZ`, no
microseconds, and a test pins every part of that.

**2. Every UsagePoint publishes the same energy twice**, as a 900-second series *and* a
3600-second one — 167 colliding timestamps in four days. The canonical dedupe key
`(ts_utc, source, device_id, channel_id, metric)` has no `interval_s`, so one series silently
replaced the other and an hour boundary held either fifteen minutes of energy or a whole hour
of it, unpredictably. Any `SUM` over the result was wrong, and nothing said so.

`model.METER_DEDUPE_KEY` is therefore `DEDUPE_KEY + ("interval_s",)` for the meter dataset
only. Two readings covering different durations are not duplicates — the duration is part of
what identifies them — and discarding one at ingest would be filtering the custodian's data
(CLAUDE.md rule 2). Choosing between them is a *query-time* decision, so `compare-meter`
`resolve_interval()` takes the finest series and says which, because adding both would report
roughly twice the household's consumption. Confirmed redundant by the data itself: meter
1326254 totals 76.0 kWh over the 900s series and 76.1 kWh over the 3600s one for the same four
days.

**3. Connect exposes a meter the download does not.** The manual export carried three
UsagePoints with an identical series (#168); the API returns `1308468` **and `1326254`**, a
genuinely different meter running 3.6–40 kWh/day against the house's 74–99. `resolve_meter`
already refuses to guess between meters that differ, so this surfaces as a prompt for
`--meter` rather than a silently trebled total — which is what that guard was written for.

Also measured: the Connect feed runs ~6 hours fresher than the download, and the resource the
token points at is a `Batch/Subscription/{id}` URI rather than the configured resource base,
which is why `LgeToken.resource_uri` is preferred over `LGE_RESOURCE_URI`.

## 170. `.env` discovery walked into a working-directory bug

`Settings.model_config` had `env_file=".env"`, which pydantic-settings resolves against the
**process working directory**. Running any command from `data/` — where the Green Button
exports live, so exactly where an operator stands — produced a `Settings` with every
credential blank, and the error said only "LGE_CLIENT_ID / LGE_CLIENT_SECRET are not
configured". It cost a real authorization attempt, with a code that expires in minutes.

`_discover_env_file()` now walks up from the working directory to the nearest `.env`, the way
`git` and `direnv` do, so anywhere inside the repository works and nothing outside it changes.
The container is unaffected: it takes real environment variables from `--env-file` and
deliberately has no `.env` in the image at all.

The message was the other half of the defect and is fixed too — it now names *which* variable
is missing and where settings were read from (`config.describe_env_source()`). "Not
configured" with no location gave an operator nothing to act on.

## 171. PLAN.md §6.3's skip list is one item short — an un-positioned breaker is not a channel

§6.3 lists exactly one reason to skip a breaker: the `NONE`/`NONE-1`/`NONE-2` placeholder
models that stand in for dumb breakers. Live installation on 2026-08-22 produced a second
reason, and it wrote rows before it was noticed.

A smart breaker that has been enrolled with the hub but **not yet located in the app**
reports full electrical data with no `position`. Leviton's own UI calls this an
"Un-Positioned Breaker" and clears it with the positioning wizard; it is a normal, transient
part of every installation. `aioleviton` then defaults the missing key —
`position=data.get("position", 0)` against an `int`-typed field — so it reaches this pipeline
as slot **0**, and §6.5's `breaker_p{position}` turns that default into an identity.

The result: for the 37 minutes between plugging in 20 breakers and finishing the wizard,
both hubs produced rows under `breaker_p0`, a slot no Leviton panel has (they number from
1). Real watts from a real circuit, attributed to a fiction. Nothing downstream can tell an
invented slot from a surveyed one, which is precisely the confusion the `channel_id`
convention exists to prevent.

`BreakerReading.is_unpositioned` (`position < MIN_BREAKER_POSITION`) is now checked in
`_map_snapshot` — the package's only mapper, so it covers the REST and WebSocket paths at
once — and such a breaker produces **no rows**, WARNed once per breaker per process
(`leviton_breaker_unpositioned`) with the count exposed as
`leviton_ingest.unpositioned_breakers`. `discover` lists it marked SKIP and non-mappable, so
nobody is invited to write a `channel_map` entry for `breaker_p0`.

This is the treatment §7.3 already prescribes for an unrecognised enum string — WARN, emit
no row — applied to an unrecognised *position*, and it is what cardinal rule 1 requires: an
unknown stays unknown rather than becoming a plausible-looking value. Once-per-process,
because only an operator can fix it and a 30s loop would otherwise write the same line 2,880
times a day.

**Two things deliberately not done.** The ~74 rows already written under `breaker_p0` are
left in place: rule 2 says record what the API said, they are honestly flagged as unmapped
by `build-dim` and `/ui`, and nothing sums by breaker yet. And the `aioleviton` default is
not patched or vendored — the guard at our own boundary is correct whether or not upstream
ever changes, and `from_model`'s `or 0` means a future upstream `None` lands on the same
skip.

Worth knowing for context: neither public integration handles this. `rwoldberg/ldata-ha`
filters the same placeholder models, then trusts `position` verbatim — an un-positioned
breaker falls through `if breaker["position"] in _LEG1_POSITIONS` and is silently attributed
to leg 2, while its panel card renders from position 1 upward so the entity never appears.
`gtxaspec/leviton-load-center` names it `Breaker 0`. There was no prior art to copy.

## 172. `/ui/hvac` is new, and the comparison it was built for turned out to be impossible

PLAN.md has no HVAC cross-check screen, the same way it had no dashboard (#164).
This one was asked for as "does the Bryant data match what the breakers see", and answering
it turned up three things worth recording.

**The comparison as posed cannot be made.** Bryant's own energy is day-grain and
`fetch-daily` writes it to `energy/daily` in **S3** — never to the spool, because rule 6
forbids day-grain rows there. No bucket is configured, so `bryant_daily.last_success_utc`
has been `null` since the instance was built and **not one Bryant energy row exists
anywhere**. Comparing Bryant kWh against breaker kWh is therefore blocked by the same S3
gap as everything else, and the screen says so in words (`bryant_energy.available: false`)
rather than drawing an empty chart that would read as zero.

**What can be compared is better anyway**, and it validated: Bryant's 30s *state* against
the panel's 30s *watts*. Measured over the first six hours of the compressor breaker's
life, `stage_pct` (the outdoor unit's capacity percentage) and `breaker_p10`'s watts track
at **r = 0.976 on 1-minute buckets, 29.9 W per capacity point (sd 1.4), implying 2.99 kW at
100%** — a sane figure for a 5-ton variable-speed unit, from two clouds that share no
identifier. And the on/off test is perfect: in 72 buckets where Bryant omitted `stage_pct`
the breaker read **exactly 0.0 W**, and in 289 where it reported a capacity the breaker
never dropped below 1,175 W. Zero disagreements either way.

**The two sources never share a `ts_utc`,** and this is the trap the screen is built around.
Each source stamps its own cycle (`new_cycle(ts_utc=now_utc())`), so a join on the canonical
key returns **nothing** — measured, 0 rows of 722, not a small number but zero. Every
comparison here is bucket-aligned and the bucket width is reported with the numbers. It is
the same lesson as the two-collector overlap (`deploy/spool-splice.py`): `ts_utc` identifies
a *cycle*, not an instant two pollers agree on.

**A fourth finding revises an earlier inference.** `channel_map.json`'s `ct_2_a`/`ct_2_b`
notes record `ct_2_a`/`ct_2_b` as the electric strip heat, on the strength of a flat 0 W across 17
minutes of cooling. Over five days of Bryant status the feeder's watts correlate **r = 0.959
with blower RPM *cubed*** — the fan affinity law — against **0.73 with compressor capacity**,
and its rpm bands run 0 W at 400 rpm to 822 W at 1,200 rpm. So the clamps are on the whole
subpanel feeder and what they mostly carry in cooling season is the **air handler blower**,
not the strips. The earlier 17-minute observation is not contradicted, it was taken at low
blower speed (400 rpm bands still mean 0.0 W today). The screen reports those four
correlations side by side precisely so the question stays open to evidence rather than being
settled by a label.

Two design choices worth defending. The panel side is selected by `category == "hvac"` in
`channel_map.json`, not by hub id in code, so a second HVAC circuit reaches the screen by
being mapped — and the compressor's entry gains an explicit `category` override for it.
And `breaker_*` vs `ct_*` is the only thing the module infers, because §6.5 already makes
that distinction mean "equipment circuit" vs "clamp on a feeder", which is exactly the
difference between measuring the compressor and measuring whatever else shares its subpanel.

## 173. `fetch-daily` and `backfill` were S3-only, so Bryant's energy was never recorded

PLAN.md §4 puts Bryant day-grain energy in `s3://.../energy/daily/`, and both stages that
write it were built to do exactly that and nothing else. No bucket has ever been configured,
so the effect was not a degraded feature but an absent one: `bryant_daily_energy` failed every
night since the instance was built, and **not one Bryant energy row existed anywhere**. It
took building `/ui/hvac` to notice, because that screen wanted to compare kWh and found none.

New `stages/dailystore` owns the destination for both stages: the local month
`{SPOOL_DIR}/daily/bryant-YYYYMM.parquet` always, and the S3 key §4 specifies as a **mirror**
when a bucket exists. Rule 6 is untouched — day-grain rows still never enter the spool or
`raw_30s`; `{SPOOL_DIR}` is simply the writable volume, and `daily/` is its own dataset the
way `meter/` already is. The precedent is exact: `stages/greenbutton.py` has defaulted to
`{SPOOL_DIR}/meter` with S3 opt-in since #167, for the same reason and with the same comment.
The one deliberate difference is that a *scheduled* stage mirrors automatically when a bucket
exists, where a manual import should not fan out by surprise.

**Result, the same evening:** `fetch-daily` wrote its first 28 rows, and `backfill` pulled
**3,712 rows over 232 consecutive days (2026-01-02..08-21)** out of the legacy DynamoDB table
— every day the old collector's Lambda has ever recorded, with no gaps. The component split
over that history is the thing worth having: 2,954 kWh heat pump, 2,445 cooling, 1,722 blower,
**1,277 electric strips**. The blower and the strips are the same conductor as far as
`ct_2_a`/`ct_2_b` are concerned, so Bryant is the only source that can separate them — which
is the whole argument for continuing to collect a day-grain number now that the compressor has
its own 30s breaker channel.

### 173a. And a failed local write was deleting the month

Found by losing August for real, minutes after the above went in.
`pyarrow.parquet.write_table` straight to the destination removes the existing file before it
opens the new one. So: 336 backfilled rows were read successfully, the write failed with
EACCES (the file had arrived via `docker compose cp` owned by uid 1000, not the container's
10001), **the file was gone afterwards**, and the next run reported `existing_rows: 0` and
wrote 28 rows over the empty month. Nothing errored on that second run — it looked healthy.

`dailystore.write_table_atomic` now writes a temp file in the same directory, fsyncs, and
`os.replace`s it. Either the old month survives intact or the new one replaces it whole, never
neither — the guarantee `aws/s3io.write_table_atomic` already gave the mirror, which is
precisely why only the local path had the hole. The existing "an unreadable month raises
rather than being treated as empty" test could not have caught this: there the *read* fails,
here the read succeeded and the write did the damage.

### 173b. A blended coverage number turned a missing channel into a -99% disagreement

The first live render of the kWh comparison reported -99% for 2026-08-18..21. Those were not
disagreements: the compressor breaker did not exist on those days, so the panel total was
feeder-only against a Bryant total containing 21 kWh of cooling that nothing on the panel was
measuring. One coverage figure across both channel groups called the days 100% covered,
because the feeder genuinely had covered them.

Coverage is now per group, the comparable figure is the **worse** of the two, and every mapped
channel must actually have reported before a delta is offered at all. When one is withheld the
payload names which reason applied. This is the same discipline `sample_count` exists for,
applied one level up: a partial *channel set* is as misleading as a partial hour, and it does
not announce itself.

## 174. The Glue catalog outgrew its own comment fields

PLAN.md §12 and this project's conventions treat the Glue comments as a complete data
dictionary: the `metric` comment enumerates every metric, the `value` comment carries the
whole enum decode, and tests pin both against the code so neither can rot. That worked at 19
metrics and three enum tables. Mapping nine more Bryant fields broke it arithmetically, not
stylistically:

* the `energy_raw_30s` metric names **alone** are **251 of the 255 characters** the Glue API
  allows in a column comment — no room for one word of prose, let alone the day-grain
  cross-reference the tests also require;
* the decode across six enum metrics is **~370 characters**, so it cannot live in a column
  comment at all;
* and both table descriptions were already within ~20 characters of the 2048 limit, because
  they were written to fill it.

Neither enumeration was truncated. Both moved to a field that can hold them.

**The decode now lives once, in `dim_channel`'s table description** — the semantic-layer
table, which is where a dictionary belongs, with ~600 characters of headroom for future
appends. Every table that carries enum rows points at it by name, and the pointer is tested
to resolve. It is still generated from `bryant.ENUM_TABLES`, still integer-for-integer, still
covers every metric in `model.ENUM_METRICS`, and the "an appended value reaches the comment
without anyone editing it" guarantee is intact — it just reaches a different comment.

**The metric catalog moved to the README**, which has room to be the closed list, and the
`metric` comment now names the metrics a reader gets *wrong* — `watts`, the mutually
exclusive `stage`/`stage_pct` pair, and the day-grain pair barred from these tables — plus
`SELECT DISTINCT metric`. That last part is the honest improvement: the original test's
rationale was "a reader filtering on an incomplete list drops rows", and a comment that
tells you to enumerate cannot cause that failure at all, where a generated list that no
longer fits eventually would.

Six tests were repointed rather than deleted, each keeping its guarantee and stating in its
docstring what changed and why. `_ENUM_DECODE` also stopped being built from a typed list of
three metrics — which is how the three new enum metrics shipped, briefly, with no published
decode at all.

One incidental rename: static pressure's unit is `inwc`, not `inH2O`. Every vocabulary
parser in `tests/test_docs.py` matches letters only, so a digit in a unit name would have
hidden it from all of them — a unit that no test can see is a unit that will drift.

## 175. The Carrier schema, introspected — and the granularity question settled by the API

DEVIATIONS #155's rule (a new outbound call pattern against Carrier needs sign-off) was
honoured: the owner asked for this on 2026-08-22. Introspection ran through
`graphql_client_from_settings`, so the Origin spoof, the Bearer token, the 401 ladder and the
throttle handling were the ones already proven; no new credential path exists.

**662 types, 106 root query fields.** PLAN.md documents two of them. The ones worth knowing:

| operation | what it is |
|---|---|
| `infinityStatus(serial)` | in use — the 30s status feed |
| `infinityEnergy(serial)` | in use — day-grain energy, `energyPeriods` |
| **`runtimeUsageInfinity(input)`** | `{deviceId, startDate, endDate, period}` → runtime buckets with `totalRuntime`, `totalCoolRuntime`, `totalHeatRuntime`, `outsideTemperature`, `isSystemOn` |
| **`deviceHistory(input)`** | `{deviceId, point, periodValue/Unit, intervalValue/Unit}` — a generic point-history endpoint with an arbitrary interval |
| `infinityProfile(serial)` | equipment identity: indoor/outdoor model and serial, `iducapacity`, `oducapacity`, `idustages`, firmware |
| `infinityConfig(serial)` | full system configuration |
| `infinityNotifications(serial)` | faults/alerts |
| `locationEnergyUsage(input)` | `{username, locationId}` → `EnergyUsage{energyPeriodType, dateTime, kwh, cost}` |

**The API settled the granularity question itself.** `runtimeUsageInfinity` with
`period: "hour"` answers, in words:

> `Invalid period specified. Available periods are: day, week, month`

So there is **no sub-daily energy or runtime anywhere on this endpoint** — the earlier
inference from `energyPeriods` was right, and is now a quoted fact rather than an inference.
The panel remains the only source of sub-day HVAC energy, which is precisely why
`breaker_p10` matters.

Two live findings that are not conclusions yet: `runtimeUsageInfinity` with `period: "day"`
returns **HTTP 504 from Carrier's own gateway**, repeatably, while an invalid period returns
200 with a message — so the operation exists and validates its input but times out
server-side, and it is worth retrying another day rather than writing off. And
`deviceHistory` needs a `username`/`locationId` this probe did not have, so its `point`
vocabulary — the one thing that could still yield sub-daily *telemetry* history — is
untested.

Also corrected: the input types are non-null (`GetRuntimeUsageInput!`). A plain
`GetRuntimeUsageInput` declaration is a 400 with a validation message, which is what the
first three probes were.

---

# Status — what is done, and what has never been executed

**This is the honest final state. PLAN.md §16's definition of done is roughly half met.**

## Done, and tested

Every module of PLAN.md §5 is implemented and every command in `energycap --help` resolves
to a real implementation except `import-greenbutton`, which §13 designs, deliberately
defers, and which exits 3 with a message saying so. All of §15's required coverage exists:
rollup math including observed-time kWh, dedupe, the 23- and 25-hour DST days, gap
handling, backfill from both legacy formats, spool durability, compactor safety, log
scrubbing, enum-table stability, and the `channel_map`/`dim` build. The Glue table and
column comments and the README are pinned to the code — and, since #129, to each other.
Every DuckDB example in the README is extracted from the file and **executed** against
local Parquet written by this package's own writers.

Step 9 added the Leviton WebSocket freshness engine (#144–#158): the in-memory
current-state store, the emission gate, the silent-stall watchdog, the 55-minute proactive
reconnect, the three `LEVITON_INGEST` modes and the `leviton_ingest` / `leviton_ws`
sections of `status.json`. It is fully covered offline against a fake transport, both
gates were verified by reverting them (#158), and sampling is provably unchanged — one
`ts_utc` per cycle in every mode, and exactly one row mapper, pinned structurally and
behaviourally. **Covered offline is all it is**: see the socket's entry below.

An adversarial pass then found six defects in it and they are fixed (#159–#161): the gate
now reads connection state as a property of each **field** and each **hub's feed** rather
than of the socket, an explicit REST null clears rather than being dropped, the flood wait
set and the backoff ladder describe what we want rather than what happened, and
`sync_mode` can no longer say `flood` for a connection that never saw one. All three of
the load-bearing ones were verified by reverting them and are pinned end-to-end at the
rows; how they compose is measured rather than assumed (#162).

`uv run pytest -q` → **1394 passed, 0 skipped**, in ~25 s, with no network and no AWS.
(It was 1382 before step 10; `tests/test_deploy.py` adds the 12 — the only automated check
on the deployment assets, and the file that caught the malformed XML comment in #163.)
(It was 1221 passed / 1 skipped at the end of step 7, 1222 / 0 before step 8, 1280 / 0
after it, 1359 / 0 at the end of step 9's first pass and 1382 / 0 after #159–#162; the
WebSocket ingester, its reconciliation tests and those regression pins account for the
rest.)

## Never executed — the outer half, minus one live poll

There are Leviton and Carrier credentials on this machine now (`.env`, gitignored) and the
first `discover` / `poll --once` has run (#143). There are still **no AWS credentials in
use and no Docker daemon**, `tests/conftest.py` still installs an autouse guard that
refuses any non-loopback socket, and everything below has still never happened. None of it
is "probably fine".

- **One live Leviton call, and no more.** `discover` enumerated the real hubs, CTs and
  breakers. The token cache over time, the 30s poll loop over hours, and the 50-second
  `bandwidth: 1` keepalive have still only met recorded fixtures. The short REST-freshness
  probes that motivated #144 are the exception: they are real reads from the real hubs,
  and they are the *only* live evidence in this file about Leviton data quality.
- **THE WEBSOCKET HAS NEVER BEEN CONNECTED.** Not once, from this environment. Every test
  in `tests/test_leviton_ws.py` and every WebSocket test in `tests/test_leviton.py` runs
  against a fake transport; `tests/conftest.py`'s autouse guard would refuse a real socket.
  So #144's **diagnosis is measured and its cure is not**: the frozen REST reads and the
  A/B keepalive probe are real numbers from the real hardware, but nothing has ever
  verified that a socket fixes them. What is unproven is listed as its own first-live-run
  item below. This is the largest untested surface added since the original build, it
  gates emission, and `LEVITON_INGEST=rest` exists so it can be switched off without a
  code change.
- **One live Carrier call, and no more.** Okta ROPC succeeded and `getInfinityStatus` /
  `getInfinityEnergy` both resolved and are now committed as fixtures (#138), which settled
  #75.1/.2/.6/.10. Refresh-token rotation and lifetimes, 30s cadence over 24 hours, 429
  handling, and the real seasonal domain of `status.mode` are **still fixture-driven and
  outstanding**.
- **No AWS call of any kind.** No S3 write, no atomic temp-key→copy→delete, no read-back
  row-count verify, no Glue `CreateTable`, no Athena query, no DynamoDB `Scan`. The Glue
  tests drive an in-process fake (#100); `moto`'s Glue backend is unusable here.
- **THE IMAGE HAS NEVER BEEN BUILT UNDER EITHER RUNTIME.** Not by `docker build`, not by
  `container build`: there is no Docker daemon on this machine and no Apple `container`
  CLI either (#163). So `docker compose up` has never run, `scripts/energycap-container.sh`
  has never run, and `deploy/com.duckbillhq.energycap.plist` has never been loaded —
  including the build steps that bake in DuckDB's `httpfs` extension and assert that
  `America/Kentucky/Louisville` resolves inside the image (#48). The `container` path adds
  its own unverified surface on top: the wrapper's flags come from Apple's published
  command reference rather than a successful run, and whether uid 10001 can write a host
  bind mount over virtiofs is the single likeliest thing to break (#163). `bash -n`,
  `plutil -lint` and a stub-`container` exercise of every host-side path are all that has
  actually been proven.
- **No end-to-end cycle against a real bucket.** §16's
  `poll`→`upload`→`compact-daily`→`rollup`→`build-dim`→`create-glue-tables` has never been
  run against anything but local temp directories.
- **`config/channel_map.json` now holds the real Leviton hub ids**, pasted from the live
  `discover` run; the only placeholder left is the future LG&E meter (§13). What remains
  unproven is coverage — that every breaker/CT the house actually meters has an entry and a
  label — not the ids themselves.
- **The Athena translations of the example queries have never been executed** (#130).

## The first live run: what to check, in order

Do these deliberately, and capture evidence before freezing anything.

1. **Build the image and start the collector. THE IMAGE HAS NEVER BEEN BUILT UNDER EITHER
   RUNTIME** — no `docker build`, no `container build`, ever (#163). This is the first
   thing that can fail, and nothing downstream matters until it passes. Whichever runtime
   you use, what is on trial here is the image, the `httpfs` bake, the timezone inside the
   container, the `/data` mount, and `curl localhost:8080/healthz`.

   On this Apple-silicon Mac Mini, the preferred path (`deploy/README.md`):

   ```bash
   container system start
   ./scripts/energycap-container.sh build
   ./scripts/energycap-container.sh run          # foreground, by hand, before launchd
   ```

   Then, in order: confirm `.dockerignore` was honoured and **`/app/.env` is not in the
   image**; expect the uid trap (#163) on the bind mount (`permission denied` or
   `unable to open database file` on `/data/spool.db`) and fix ownership before concluding
   anything else is broken; only then install the LaunchAgent and prove it survives a real
   reboot with `container system status` running and `/healthz` answering 200. Every flag
   in that wrapper came from Apple's published command reference, not from a run — read
   its output, do not trust it.

   Off Apple silicon, or as the fallback here: `docker compose build && docker compose up -d`.
   Do **not** run both paths at once: they use different storage (named volume vs. bind
   mount), so that is two collectors, two spools, and both polling the same clouds.
2. ~~**`energycap discover --dump dump.json` (mode 0600).**~~ **DONE 2026-08-17.** It
   answered the Leviton questions at once — the **real hub ids** (now in
   `config/channel_map.json`), the CT `channel` numbers and the breaker `position`s — and
   the Carrier ones in items 3–6. The raw capture is `data/discover-raw.json` (gitignored,
   mode 0600); the Bryant half is committed as a fixture (#138). Still open from this call:
   whether any **LSBMA accessories** are present (#16) and whether firmware ≥2.2.0 is
   suffixing breaker ids — both are Leviton-side and were not re-read here.
3. ~~**Does `infinityStatus(serial:)` resolve at all?**~~ **ANSWERED — yes**, first try;
   `status.json` reports `operation: getInfinityStatus` and the #64 fallback never fired.
4. ~~**Is `odu.opstat` a word or a numeric percentage?**~~ **ANSWERED — numeric.**
   `odu.type = "gs3ngiphp"` (Greenspeed, variable capacity), `odu.opstat = "35"`, and
   `bryant_enum_numeric` fired on every cycle with no compressor row emitted. The
   pre-authorised `stage_pct` metric was built (#59, #132) and `STAGE_CODES` was **not**
   renumbered. The counters to watch from here are `stage_pct_rows` and
   `stage_pct_out_of_range` (#134).
5. ~~**How many zones are actually enabled?**~~ **ANSWERED — one of eight reported.** Only
   `zone_1` is `enabled: "on"`, so `channel_map.json` needs exactly one `zone_{n}` entry;
   the seven phantoms report humidity and setpoints and must keep producing nothing (#66).
6. ~~**Is `cfgem == "F"`?**~~ **ANSWERED — yes.** `outdoor_temp_f` is emitted (#60) and the
   Celsius conversion path (#61) is dead code on this house — which also means it stays
   unexercised against reality.
6a. **The WebSocket — the whole of step 9, first contact with the cloud.** Start on
   `LEVITON_INGEST=hybrid` (the default): its worst case is exactly the pre-step-9
   behaviour, so this check costs no data. Then read `status.json`, in this order, and
   **publish no freshness claim until you have**:

   1. **Does the handshake even work?** `leviton_ws.connected`, `reconnects` and
      `server_drops` (the latter separates the cloud closing on us from our own 55-minute
      rotation; `reconnects` counts both — #160). We now send the `Origin`/`Referer` pair
      the REST adapter sends (#161), so the first suspect for a refused handshake is
      handled; the one that remains is a `{"type":"challenge"}` frame, which `aioleviton`
      does not handle at all and which would surface as
      `LevitonConnectionError("WebSocket did not reach ready state")`. If the handshake
      fails repeatedly, note that `connect_attempts` should be **climbing** — a flat 0 with
      a rising `server_drops` would mean the ladder regressed (#160).
   2. **Does the flood actually arrive, and does it cover everything?** `sync_mode` says
      `flood` (every *desired* subscription target was touched on this connection) or
      `timeout` (it was not, and we opened the gate on the REST seed after 20s — #154).
      **Expect `timeout` at first.** Since #160 `flood` can no longer be reported off the
      timeout path or for a connection with nothing to await, so it now means what it
      says; `awaiting_sync` says how much is missing and `subscriptions_pending` /
      `subscribe_failures` say whether the gap is a subscribe that never landed.
   2a. **Are BOTH hubs' feeds alive?** `hub_silence_s` — one entry per hub — and
      `stalled_hubs`. This is the check an aggregate watchdog cannot make: Panel A's line
      voltage jitters constantly, and before #159 that was enough to certify Panel B's
      dead feed as live and publish its frozen store. If one hub's silence keeps climbing
      while the other's sits near zero, that hub is not pushing and the whole per-breaker
      story for it is fiction. Also expect `fields_evicted` to be **non-zero after every
      reconnect** — it counts the values the new connection did not re-prove, which are
      now gaps rather than fabrications.
   3. **Do the frozen channels finally move?** This is the question the whole step exists
      for. `leviton_ws.objects` gives per-field last-update ages and provenance;
      `leviton_ingest.last_reconcile_drift` gives `{compared, differing}` — how many
      metrics the REST cache and the live store disagree on, both mapped through the same
      mapper. A high `differing` is the frozen cache quantified, and a `differing` of 0
      over a busy period would mean the socket has changed nothing. Watch the two feeds
      that held 4086.05 W and 505.17 W across 46 reads specifically.
   4. **At what cadence?** Unmeasured and contested in the ecosystem: gtxaspec claims CTs
      degrade to a 2–12 minute cadence once `bandwidth` decays from 1 to 2 (their remedy
      is the `1 → 0 → 1` toggle we refuse — #144), while ldata implies the official app's
      plain `bandwidth: 1` is enough for real time. `leviton_ws.objects` answers it
      directly. Even the pessimistic case beats a value frozen across 46 reads, but do not
      claim a number before reading it.
   5. **Are per-breaker subscriptions actually required on 2.1.2?** Both are subscribed
      today (hub *and* each CT/breaker), deliberately, because the cost is a few frames
      and the downside of being wrong is silent loss of the whole-panel `GRID_POWER`
      feeds. Compare which objects deliver data to decide whether the belt-and-braces CT
      subscription is load-bearing here.
   6. **What close code does the 60-minute kill deliver?** `last_close_code`. Nobody in
      the ecosystem has recorded it; one day of running settles it. Nothing branches on
      it — do not add logic that does.
   7. **Do explicit nulls arrive?** `null_deltas_by_field`. If they are common, #153's
      "null clears the field" policy is gapping real data and `NULL_POLICY_IGNORE` is one
      argument away. If the dict stays empty, the question is closed.
   8. **What is the message volume?** `messages_per_s`. Unmeasured; at ~40 channels the
      theoretical worst case is ~3.4M frames/day. The merge callback runs inline in the
      read loop, so this is also the backpressure number.
   9. **Only then**, if `cycles_rest_fallback` is near zero over a sustained period,
      consider `LEVITON_INGEST=ws` — which gaps instead of falling back, and will drop
      `sample_count` in any hour it gaps. That is the point of it: it turns socket
      reliability into a number in the archive. `LEVITON_INGEST=rest` is the instant
      revert at any time.

   If the socket connects but the feed stays slow, the first thing to try is the
   unimplemented `GET /apiversion` 10-second keepalive (#155) — and it needs owner
   sign-off, because it is a new outbound call pattern.

7. **Does the Carrier cloud tolerate 30-second polling?** Nothing in the ecosystem polls it
   faster than every 30 *minutes* and neither reference client handles 429 at all. Watch
   `throttle_events` / `retry_after_s` for the first 24 hours and raise
   `BRYANT_POLL_INTERVAL_S` if needed (#54).
8. **Does the payload even change every 30 seconds?** The golden capture shows `localTime`
   lagging `utcTime` by ~4 minutes, which suggests server-side caching. `server_utc_time` is
   parsed and logged at DEBUG on every `bryant_poll_ok` precisely so consecutive cycles can
   be diffed. If it is cached, 30s polling is storing duplicates and the interval should go
   up.
9. **Then the full cycle against the real bucket**, one stage at a time, reading the row
   counts: `poll --once` → `upload` → `compact-daily` → `rollup` → `build-dim` →
   `create-glue-tables`, then the README's queries through Athena as well as DuckDB. Run
   the first `backfill` with `--dry-run` and read `backfill_dynamodb_scanned`,
   `backfill_unknown_attribute`, `backfill_precision_loss` and `backfill_gas_kwh_nonzero`
   before writing anything.

## 176. `AWS_PROFILE=` is not "no profile" — it is a profile named empty string

`.env.example` shipped a bare `AWS_PROFILE=` under the comment "Optional. Leave unset in the
container if it uses keys / instance credentials." The comment's advice is right and the line
below it does the opposite: an empty assignment *is* set, compose's `env_file` forwards it into
the container verbatim, and botocore then resolves a profile whose name is the empty string and
raises `ProfileNotFound: The config profile () could not be found` on **every** client build —
with `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` sitting right there, valid.

This cost the first S3 deployment a cycle. `S3_BUCKET` was set, the collector key was in place,
`get_settings()` reported `aws_profile = None` (pydantic coerces the empty string away), and
`s3io.get_client()` still died. Passing no `profile_name` to `boto3.session.Session` does not
help, which is the counter-intuitive part: botocore re-reads `AWS_PROFILE` from the environment
itself when it builds its config store, so the variable's *presence* is what matters, not what
the application chooses to pass. Had it not been caught in pre-flight, `upload_hourly` would
have swapped one silent failure mode for another — `ProfileNotFound` instead of
`S3_BUCKET is not configured` — with the 678k-row backlog still stranded.

Two changes, because either alone is insufficient:

- `.env.example` comments the line out and states the trap, so a fresh `cp .env.example .env`
  cannot reintroduce it.
- `s3io._drop_empty_aws_profile()` deletes a set-but-blank `AWS_PROFILE` from `os.environ`
  before the session is built, WARNing `aws_profile_empty_ignored` once. Mutating the
  environment is heavy-handed and it is the only thing that works, since botocore reads the
  variable directly. It guards on *blank* rather than empty so a stray space is caught too, and
  a deliberately named profile is left strictly alone (four tests pin all of it).

Worth knowing for anyone driving the AWS CLI in this repo: the same empty variable breaks every
`aws` invocation the same way, which is why `docs/s3-storage.md` tells you to `unset
AWS_PROFILE` after sourcing a credentials file.


## 177. The LG&E authorisation lapsed, and three things made it invisible

Observed 2026-08-23, while filling `energy/meter/` in the S3 rollout
(`docs/s3-storage.md` §8). `{SPOOL_DIR}/tokens/lge.json` was **gone**, meter data stopped at
**2026-08-20**, and nothing anywhere said so. Re-authorising needs a browser, so this cost the
owner a manual round trip. Not one of the three causes is a bug on its own; together they made a
dead data feed indistinguishable from a healthy one.

**1. The deletion was correct and silent.** `lge_auth.py` clears the token cache when LG&E
rejects the refresh grant, and the comment is right: "continuing to present a credential the
custodian has rejected is how a registration gets disabled." But `clear()` just unlinked the
file, and a missing token file is *also* the normal state of a deployment that never clicked
through consent.

**2. So the job reported an ordinary skip.** `_job_greenbutton_daily` returns
`{"skipped": "not_authorized"}` when the file is absent — quietly, deliberately, because "a
scheduled job that errors daily on a perfectly normal configuration is noise, and noise is what
teaches an operator to stop reading the log" (its own test says so). Exactly the right default,
and it swallowed a revocation for three days.

`clear()` now leaves a breadcrumb — `lge-revoked.json`, mode 600, holding *only*
`revoked_at` and `reason`, never credential material — and only when a token actually existed, so
a fresh install stays quiet. A successful `save()` retires it, because re-authorising is the cure
and one old revocation must not shout forever. With a breadcrumb present the job raises
`GreenbuttonAuthorizationRevoked` instead of skipping, so it lands in `job_failed`,
`consecutive_failures` and `/healthz`, and the message names the fix.

**3. `/healthz` had no way to see it, and `last_success_utc` would have lied.** The natural
check is the wrong one: a fetch can *succeed* and return nothing new, which is precisely what a
revoked feed looks like from the job's side. So the new `health.meter` block measures the age of
the **newest interval actually held** (`newest_interval_utc`, recorded by the fetch), not the age
of the last successful run.

It reports and **never 503s**, which is a deliberate asymmetry: LG&E publishes days late and
revises, so staleness here is the *utility's* lag, not this container's fault, and marking the
container unhealthy for it would be wrong — and with a healthcheck in `docker-compose.yml`,
actively harmful. `METER_STALE_AFTER_DAYS` (default 3) sets the reporting threshold. The block is
absent entirely until Green Button has been fetched once, so a deployment that does not use it
grows no permanently-stale field.

### And the likely reason it lapsed at all

`REFRESH_MARGIN_S` was a flat **300 seconds** while LG&E issues a **24-hour** access token, and
`greenbutton_daily` fires **once a day**. Those three numbers do not fit: the job essentially
never lands inside a 5-minute window at the end of 24 hours, so it always found the token already
dead and refreshed *reactively* — leaving the refresh token unexercised for nearly two days at a
stretch. Refresh tokens go stale when they are not used.

The threshold is now `max(REFRESH_MARGIN_S, lifetime × 1/3)`, computed from the token's own
issued lifetime (`obtained_at`..`expires_at`). For a 24h token that is 8h, so the daily job
refreshes every day: at 09:15 local on a token issued 15:50Z the previous day, 2.6h remain
against an 8h threshold. The flat 300s floor still governs anything short-lived, so a
15-minute token is not churned on every call. Refreshing early is cheap, and the rotation was
already persisted before use.

This is a hypothesis about LG&E's side, not a proven cause — the container logs that would have
carried the rejection had already rotated. It is the only mechanism on our side that fits, the
fix is harmless if the real cause was different, and change (1) means the next occurrence
arrives with its reason attached instead of as another silence.


## 178. `dim_channel` gains a 14th column, `is_primary` — PLAN.md §9 lists thirteen

§9 enumerates `dim_channel.parquet`'s columns and this is not among them. It is added anyway,
because a comment shipped in Phase 3 promises it.

`energy_meter`'s table description tells a reader: *"dim_channel marks the house primary — join
it rather than hardcoding an id."* That sentence was written to stop the single most dangerous
mistake available in that dataset — summing the house meter and the barn's EV charging as if they
were one service. When it was written, `dim_channel` **did not have the column**, so the advice
pointed at nothing. Either the comment was wrong or the schema was incomplete; the schema was.

`primary` was already in `ENTRY_KEYS` and on `ChannelEntry`, already set in
`config/channel_map.json` (`1308468`, the house), and already read by `meterview.primary_meter`
— which loads `channel_map.json` directly for exactly this one fact. So the value existed and was
in use; only the *published* semantic layer omitted it, which meant any consumer reading S3 alone
— Athena, an LLM, the new history UI — had to hardcode a meter id or invent a heuristic ("the
bigger one is the house"). That is precisely the knowledge §9 says belongs in the map rather than
in code.

Resolved to `bool`, non-nullable, defaulting `False`, taken from the entry and deliberately
**not** inheritable from the blackstart inventory: a panel device knows nothing about which
utility meter is the house. It sits beside `blackstart_device_id`, before `updated_at`, so the
timestamp stays last.

**Named `is_primary`, not `primary`, and that is not a style preference.** `PRIMARY` is a
reserved SQL word — DuckDB refuses `SELECT ... primary FROM ...` outright with a parser error, so
the column would have needed `"primary"` quoting in every query forever. The whole purpose of
this layer is that a human or an LLM reads the comments and writes working SQL; a column you
cannot name without quoting defeats that. The first build shipped as `primary` and the very first
real query against it failed, which is how this was caught. The hand-edited `channel_map.json`
key stays `primary` — it is JSON, never SQL — so the map reads naturally and only the published
column carries the `is_` prefix.

Consequences accepted: `DIM_SCHEMA` is a column wider, `build-dim` must be re-run before any
consumer sees it (done, 46 rows), `create-glue-tables` must re-publish the table (done, updated
in place), and the tests pinning "exactly §9's list" now pin fourteen columns with a pointer
here. `meterview.primary_meter` is left reading `channel_map.json` — it runs beside the map on
the Mac and does not need S3 — but it is no longer the *only* way to learn the fact.


## 179. `stage` is not permanently empty — `odu.opstat` picks its shape per reading

**#59 predicted the mechanism correctly and then the documentation drew the wrong conclusion
from it.** #59 said `odu.opstat` arrives either as a word or as a capacity percentage, that the
two are different measurements, and that they must therefore become two metrics — `stage`
(`unit='enum'`) and `stage_pct` (`unit='pct'`). All of that is right, and
`sources/bryant.stage_metric_for` implements it as a **per-value** classifier: `_looks_numeric`
on the string that just arrived.

What #59, #75.1, the README and every Glue table comment then asserted was that the *choice*
is made once, by the hardware — "two mutually exclusive renderings decided by the hardware, and
a system only ever produces one of them" — and that this house, being a Greenspeed
variable-capacity unit (`odu.type = "gs3ngiphp"`, `opstat` observed as `"35"` on 2026-08-17),
would emit `stage_pct` and **"no `stage` row will ever exist here"**. The README went further and
told a reader that `WHERE metric = 'stage'` "Returns NOTHING on this system."

**Six days of archive say otherwise.** `energy/raw_30s` for 2026-08-17→23, one serial
(`4022W200213`), one outdoor unit, nothing reconfigured or reflashed:

| local day | `stage` rows | `stage_pct` rows |
|---|---|---|
| 08-17 (partial) | 17 | 1,013 |
| 08-18 | 847 | 2,025 |
| 08-19 | 1,945 | 934 |
| 08-20 | 813 | 2,067 |
| 08-21 | 1,518 | 1,361 |
| 08-22 | 1,430 | 1,451 |
| 08-23 (to 14:00) | 1,521 | 159 |
| **total** | **8,091** | **9,010** |

47% / 53%. Not a handful of stragglers around a firmware event — an even split, every day, and
the flip happens several times a day. The `stage` values observed are `off` (6,837 rows) and
`dehumidify` (1,254); the `stage_pct` values run 13–86.

**The mechanism, now that there is enough data to see it:** a variable-capacity unit reports a
*percentage while the compressor is modulating* and falls back to a *word* when it is not. The
field's shape tracks compressor state, so it changes as often as the compressor does. 2026-08-23
is the clean illustration — the compressor stopped at 05:31:03 local and from 06:00 onward every
single cycle is a `stage` row, 120 an hour, while the whole of the previous afternoon's cooling
block (08-22 15:00–22:00) is `stage_pct` at 120 an hour.

The code needed no change. The classifier was already per-value, `STAGE_CODES` was never
renumbered, the two metrics never share a dedupe key, and the archive is correct throughout:
every row ever written records what the API actually said. **What was wrong was every sentence
that told a reader to pick one metric**, which is the worst possible flavour of wrong here,
because the documents exist precisely so that a human or an LLM can write a correct `WHERE`
clause without reading the source. A reader who followed the README lost half the day and was
told, in bold, that the empty half was impossible.

Corrected in four places, all of which had independently retyped the claim:

1. **README** — the data-model summary, the enum-decode bullet ("`stage` is **permanently empty
   on this system**"), and the whole "Compressor stage: `stage` vs `stage_pct`" section, whose
   SQL example block now shows the `max(CASE WHEN metric = …)` pivot that selects both instead of
   two `WHERE` clauses labelled right and wrong.
2. **`aws/glue.py`** — `STAGE_REPRESENTATION_NOTE`, the string published into the Glue database
   description and into every table comment either metric can reach. This is the one that
   mattered most: it is what an LLM reads to orient itself, and it said
   "`stage` NEVER appears: absence, not zero." It now says the rendering is chosen PER READING,
   that this unit emits BOTH, and SELECT BOTH. Kept to 267 characters — 35 more than the text it
   replaced, which fit under the 2048-character table-comment budget only after trimming; #174
   is still the reason that budget is tight.
3. **`sources/bryant.py`** — trap 6 in the module docstring, which said "the hardware picks".
4. **The tests that pinned the wrong words.** `test_glue.py` asserted `"MUTUALLY EXCLUSIVE"` and
   `"VARIABLE-CAPACITY"` appeared in every stage-bearing table comment, and `test_docs.py`
   asserted `"mutually exclusive"` appeared in the README. Those tests were doing their job —
   they held the two documents to the same story — they were just holding them to a false one.
   They now pin `PER READING`, `SELECT BOTH` and `absence is not zero`, which are the parts that
   are true and the parts a reader acts on.

**Two smaller consequences worth writing down.**

`energycap discover` prints `stage_metric` per system, and #59 offered that as the way to answer
"which rendering does this system emit" without waiting for a poll cycle. It cannot answer that
question. It reports the rendering of the **single reading it just took** — which is exactly how
the wrong conclusion got made on 2026-08-17, from one `opstat = "35"`. The README now says so.
Only the archive can answer it, and the honest answer for any variable-capacity unit is "both".

`bryant_stage_representation` logs at INFO on first sight of a rendering "and again on any
change". It was written to catch a replaced or reflashed outdoor unit — a once-in-years event —
and it is instead firing several times a day, forever, on a healthy system. Left as-is rather
than demoted: the log is not lying, and the row counts in `status.json`
(`stage_pct_rows` / `stage_enum_rows`) are the numbers to read. But `stage_representation`, the
single-valued tag beside them, describes only the most recent cycle and should not be read as a
property of the system.


## 180. Panel B's feed CT reports 0 A while its own breakers report 1.9 kW

Not a deviation from the spec — a deviation from what the data appears to say. Recorded here
because CLAUDE.md rule 2 ("record what the API said, verbatim; filtering is a query-time
concern") means the archive is *correct* and every naive query over it is *wrong*, and the only
place that can be written down is here.

**What the archive shows.** Over 2026-08-17 → 08-23, exact `0.0` on the Leviton CT `watts`
channels:

| hub | panel | CT channels | samples | exact zeros |
|---|---|---|---|---|
| `1000_0046_1D48` | B | `ct_1_a/b`, `ct_2_a/b` | 68,420 | **29–60% every single day** |
| `1000_0046_1D52` | A | `ct_1_a/b` | 34,210 | **0. Not one, in six days.** |

**Five things that are ruled out, each by a query rather than an argument.**

1. **Not a gap, and not a failed poll.** `volts = 121.0` and `hz = 60.0` are present and correct
   on the *same hub, same `ts_utc`* in every one of the 2,881 zero cycles checked. The hub was
   connected and answering.
2. **Not the watts arithmetic.** `amps` is `0.0` in 29,325 of 29,325 rows where `watts` is `0.0`,
   and there is **not one** row with `watts = 0` and `amps > 0`. The current measurement itself is
   zero, so this is not a power-factor or `V × A` bug.
3. **Not the hub, and not the transport.** Panel B's smart breakers report normally straight
   through the zero runs, down to `0.228 A` (`breaker_p10`) and `0.17 A` (`breaker_p26`). Whatever
   fails, fails per CT object.
4. **Not rounding or a small-value floor.** No CT sample on either hub has ever landed between
   `0` and `0.562 A`. The clamp reports `0` or a real number, with nothing in between.
5. **Not per-sample noise.** 143 runs on `ct_1_a` alone: median **7 minutes**, mean 10, maximum
   **63.5 minutes**.

**And the observation that settles it.** 2026-08-23, local, `1000_0046_1D48`:

```
03:20:03   ct_1_a: 0.0 W / 0.000 A     breaker_p10 (heat pump): 834 W
03:21:03   ct_1_a: 0.0 W / 0.000 A     breaker_p10 (heat pump): 834 W
   … eleven consecutive cycles, 5m30s …
03:25:33   ct_1_a: 0.0 W / 0.000 A     breaker_p10:              12 W
```

834 W across two legs is ~3.5 A per leg, an order of magnitude above any plausible dead-band,
flowing through a panel whose feed clamp reports exactly zero amps. Across all latched cycles
80.5% carry under 120 W of breaker load — which is why this looks like a simple under-range at
first — but **19.5% carry 300 W to 1,960 W**.

So the reading does not merely drop out at low current: **it latches, and does not recover when
load returns.** That also explains every secondary pattern, all of which are load-shaped rather
than clock-shaped:

- Concentrated 22:00–07:00 local, nearly absent 11:00–13:00. Panel B's only loads are the heat
  pump, the cooktop and the double oven, all of which are off overnight.
- Longer runs the deeper into the night (mean 1,159 s in the 05:00 hour, 270 s at 13:00).
- Strictly nested: the `ct_1` pair is **never** zero unless the `ct_2` pair is also zero — 3,165
  cycles both, **0 cycles `ct_1` alone**, over 19,505. This looked like a shared-subsystem clue
  and it is not; see the blower cross-check below.
- No alignment to any clock minute, so nothing periodic in our own code is doing it.
- Panel A is exempt because its feed never goes below **2.455 A**. It never enters the dropout
  that starts the latch. This is not a healthier panel; it is a busier one.

**Cross-checked against an independent instrument.** `energy_meter` (LG&E house meter `1308468`,
900 s series) versus the summed feed CTs for 2026-08-23, hour by hour: `−22.2%`, `−12.5%`,
`−18.7%`, `−13.0%` for 03:00–06:00, the four hours containing zeros, against `+4.6%`, `−3.6%`,
`−2.8%` for the three that contain none. The clean hours reproduce the `~3.4% high` figure
`compare-meter` established in STATE.md, so the sub-metering is sound and the deficit is
entirely these latched runs.

**The first hypothesis was ours, and the experiment killed it.** The obvious suspect was the
WebSocket current-state store: `LEVITON_INGEST=hybrid` gates freshness on the *connection*, never
on how old a field is, so the store cannot distinguish *"the hub has told me nothing because the
value is genuinely unchanged"* from *"the hub has stopped telling me anything about this
channel"*. A CT that drops out at low current and is then never republished would leave the store
handing out `0` forever — the same shape as the REST staleness `.env.example` documents (a feed
frozen at `4086.05 W` for 46 consecutive reads), with a different frozen value. Three live
readings seemed to agree: reconcile drift at `15 differing of 65` against the `4 of 24` STATE.md
records, `awaiting_sync` at **7** rather than the `0` STATE.md reports as the WebSocket's headline
achievement, and `/ui/data` serving `breaker_p0` at `age_s = 98787` for a channel REST no longer
returns at all.

**It is not the store. It is the hub, and both transports read the same wrong number.**

The A/B ran 2026-08-23 20:18Z → 2026-08-24 06:23Z, 900 paired cycles, 16:18 → 02:23 local, so it
spanned the whole dense part of the latch window. 11 cycles lost to fetch errors. Over 3,556
comparable CT observations:

| | count | share |
|---|---|---|
| WebSocket store **and** REST both exactly `0` | 1,273 | 35.8% |
| WS `0` while REST reports load | 17 | 0.5% |
| REST `0` while WS reports load | 23 | 0.6% |

A latching store would make the second row large and the third row near-zero. They are the same
size, and the 17 are almost all transition samples — `rest prev = 0.0, next = <the value>` — which
is what two readers a few seconds apart look like at the edge of a run, not a stale cache. The
`12–15 differing of 65` reconcile drift is the same jitter measured across mostly fast-changing
breaker channels, and is not evidence of anything.

**And REST reproduces the impossible reading directly, with no WebSocket anywhere in the path:**

```
time_utc   feed_W  feed_A  brk_sum_W     hp_W    hp_A     <- one REST response, one hub
04:57:00      0.0   0.000       1467   1457.0   6.085
04:57:41      0.0   0.000       1467   1457.0   6.085
05:52:26      0.0   0.000       1764   1754.0   7.330
```

Of 114 REST observations with the feed at `0`, **32 carry 300 W or more** of breaker load on the
same hub in the same response and 7 carry over 1 kW. REST zero-run lengths match the archive's
WS-fed runs in shape (8 runs, median 360 s, mean 428 s, max 960 s, against median 420 s / mean
604 s / max 3810 s). Same phenomenon, same hub, different transport.

So the fault is inside the load centre: **the hub reports `0 A` on its CT channels while
simultaneously reporting real current on its breaker channels, over both REST and the
WebSocket.** Our pipeline is recording that faithfully, which is cardinal rule 2 working exactly
as intended and is the reason the evidence existed to find this at all.

The `breaker_p0`-at-27-hours observation stands on its own and is unrelated: the store retains
channels REST has stopped returning. The poller drops them before the spool (19 rows ever
archived), so it is a dashboard cosmetic, not a data defect. Worth a separate look, not this one.

**A second cross-check narrowed the defect, and it exonerated `ct_2`.** `ct_2` clamps the HVAC
subpanel feeder, whose only real load is the air-handler blower — and Bryant reports that blower's
speed independently, 30 s for 30 s. Matched by the minute over eight days:

| blower state | minutes | `ct_2` at zero | mean W when `ct_2` reports |
|---|---|---|---|
| stopped | 1,331 | 98.1% | 603 |
| low (<600 rpm) | 5,021 | 95.1% | 172 |
| running (≥600 rpm) | 3,402 | **6.8%** | 694 |

`ct_2` reads zero when there is genuinely nothing to read and reports correctly the moment the
blower spins up. **So the "`ct_2` fails 70% of the time" figure above is wrong as a fault rate** —
most of those zeros are honest, and this entry originally implied otherwise. `ct_2` shares the
low-current floor and simply spends most of its life below it.

The same split on `ct_1` is what isolates the real defect — 62.1% zero with the blower stopped,
15.5% low, 1.9% running. `ct_1` clamps the whole-panel service feed, which is *never*
legitimately at zero (the panel always carries at least heat-pump standby), and 172 of its 750
zero cycles coincide with ≥300 W measured on that hub's own breakers.

That also kills the nesting clue: `ct_1` never drops out without `ct_2` because `ct_2` always
carries less current and always crosses the threshold first. **Load ordering, not a shared
failure path.** Worth stating because the reverse reading is the intuitive one and it is wrong.

So the defect is narrow and stateable: **the channel-1 `GRID_POWER` CT pair on
`1000_0046_1D48` enters a low-current dropout and does not exit it when current returns** — up to
1,960 W, up to 63.5 minutes, over both transports, on the same firmware (2.1.2) as the hub that
has never done it once. `connected: false` is not the tell: all six CT channels on both hubs
report it, including the pair that never fails.

### 2026-08-24: the restart failed, and it exposed a second failure mode this entry had missed

The hub was power-cycled at 10:40 local. **It did not fix anything, and the investigation above
was measuring only half the fault.**

Everything before this looked for `value = 0`. The other mode is **a frozen non-zero value**, and
it is both more common and easier to prove. On 2026-08-24 the feed CT returned `489.3 W` for
**600 consecutive samples, 05:00 → 10:41 local, one distinct value across five hours**, while
`breaker_p10` on the same hub cycled 0 → 165 W with 5–13 distinct values per hour. Summing that
panel's metered children exceeds the frozen feed reading in **459 of those 600 cycles**, by up to
116 W — physically impossible, since all of it passes through the feed clamp.

Across eight days, distinct values per channel-hour on the feed CT pairs: the healthy hub
averages **55.0** (2 of 326 channel-hours pinned to one value); `1D48` averages **8.2** (**53 of
326** pinned). The independent REST-only reader saw the same thing — 42 distinct non-zero values
in 900 samples with a longest identical run of **141**, against 131 values and a longest run of
36 on the healthy hub. So the freeze is not a WebSocket-store artefact either.

**Why this matters beyond the hardware:** "a gap stays a gap" (rule 1) and "record what the API
said" (rule 2) are both being honoured, and the archive is still correct — but a frozen non-zero
reading is invisible to every check this project has. `sample_count` is full, no row is missing,
no value is null, and the number is plausible. Only comparing a channel against its own children,
or counting distinct values per hour, catches it. **That is a query-surface gap worth closing**:
a `distinct_values` column in the hourly rollup would have surfaced this on day one, and it is
cheap — one `count(DISTINCT value)` in `rollup.sql`. Not implemented; noted as the concrete
follow-up.

The restart's one useful gift is the mechanism. At 10:41:51, about a minute after the reset, the
feed CT updated **exactly once** (`489.3 → 494.76`) and then froze again at the new value for the
next 90 minutes, while the healthy hub's feed swung 2,755 → 920 W in 57 seconds. The hub appears
to take a single CT reading at startup and then stop refreshing the channel. That is now the
headline symptom in the support report.

Corrected consequence for the earlier text: recovery is **not** always "clean and complete". The
zero runs recover; the frozen runs are not recoveries at all, just a different stuck value.

Still open, and not answerable from here: clamp hardware, that hub's CT inputs, or firmware.
**This is a Leviton bug to report**, not one we can fix — drafted in
`docs/leviton-ct-zero-report.md`. A power cycle of the affected hub is being tried first, since
a stuck firmware state is cheap to eliminate and both hubs run identical firmware.

**Why S3 cannot settle it, which is the actionable gap.** Nothing in the archive records *which
path supplied a value*. A row that came from the WebSocket store and a row that came from a REST
reconcile are byte-identical, so no query over `energy/raw_30s` can distinguish a stale store
from an honest hub reading. That is worth fixing independently of this bug — a per-cycle
`value_source` is already in the log line and in `status.json`, and only the archive lacks it.

**How the experiment was run, and why it had to be overnight.** An A/B against the same channels
at the same moments: `LEVITON_INGEST=rest` with its own `SPOOL_DIR` on the Mac, paired every 30 s
against the production process's `GET /ui/data`. Safe to run beside production for two reasons
worth reusing: `poll --once` sets `run_background = (not once)`, so **no bandwidth keepalive is
ever sent** — which is the one Leviton call that can disconnect a hub — and `rest` mode opens no
socket at all. One login, token cached in the probe's own `tokens/` and reused for all 900 cycles.

It had to span 22:00–02:00. The first paired sample, at 16:15 local, had Panel B drawing 7.8 A
with the heat pump at 1902 W, and WS and REST agreed to three decimals on every one of 30
channels. **The fault cannot be observed while the panel is busy** — which is exactly why six
days of otherwise clean-looking data hid it, and why any future probe of this must be scheduled
against the load, not against the clock.

**The fix this entry originally proposed would have been wrong, and that is worth recording.**
The plan was to give **zero-valued** WS entries a staleness timeout and emit nothing once it
expired, turning a latched `0` into an honest gap. It would not have worked: the hub reports the
zero over REST too, so the timeout would fire on values that are being actively republished, and
`rest` mode — which has no store at all — would keep the defect untouched. It would also
manufacture gaps over genuine zeros, which is its own violation of cardinal rule 1 in the
opposite direction. **Do not add a staleness timeout for this.** The defect is upstream of every
line of our code, and the honest handling is to keep recording verbatim and correct at query
time.

**Until then, at query time.** Panel B's feed CT under-reports and must not be used for kWh in
any hour containing zeros. `sum(breakers) + ct_2` on that hub reconciles to the feed at 100–102%
in clean hours and is the better substitute — with the caveat that `ct_2` carries the same
defect. `energy_meter` is the arbiter wherever it overlaps. And a `WHERE value > 0` filter is
**not** a fix: dropping the zeros and rescaling the surviving mean to a full cadence overshot the
meter by 55% (23.49 kWh against 15.16) for 02:00–08:30 on 08-23, because the latched samples are
not a random subset of the hour.


## 181. The collector's `energy/hourly/` denial is lifted — the boundary was guarding a split that has not happened

`docs/s3-storage.md` Phase 0 built two scoped IAM users and then did the thing most IAM work
skips: it **asserted the denials against the real API**, not `simulate-principal-policy`. The
collector key could write `energy/raw_30s/` and `energy/_tmp/` and was confirmed denied on
`energy/hourly/` (`PutObject`) and `glue:GetDatabases`, "because the always-on internet-facing
box cannot touch derived data or the catalog." That is a good boundary, correctly built, and
correctly verified.

**It was guarding an architecture that does not exist yet.** `PLAN.md` §5's design is one
container, one process, and `energycap run` hosts the poll loops *and* the in-process scheduler:
hourly upload, 01:30 daily compaction, hourly rollup. The poller/batch split that would move the
last two off the instance is listed in `STATE.md` as *next*, not done — and there is **no
configuration knob to disable a scheduled job**. `grep -nE "enabl|disabl" config.py` returns
`leviton_ws_enabled` and `leviton_rest_reconcile_enabled` and nothing else.

So the boundary did not prevent the collector from writing derived data. It made the collector
**fail hourly, forever, in a way nothing was watching**:

```
"rollup": { "consecutive_failures": 6,
  "last_error": "AccessDenied: ... user/energycap-collector is not authorized to perform:
                 s3:PutObject on resource:
                 \"arn:aws:s3:::ericpullen-energycap/energy/hourly/year=2026/month=08/
                 rollup-20260823.parquet\" because no identity-based policy allows the
                 s3:PutObject action",
  "last_day_attempted": "2026-08-23" }
```

`energy/hourly/` was stale at `rollup-20260822.parquet` while `raw_30s` uploaded cleanly every
hour, which is exactly the shape of failure that hides: the dataset a human looks at was fine,
the derived one nobody lists was frozen, and `/healthz` was reporting it accurately the whole
time to nobody.

**`v2` also omitted `energy/raw_30s_parts_archive/*` entirely** — no write, no `ListBucket`
prefix. The 01:30 compaction moves a day's hourly parts there after verifying the day file, so
it would have failed on its first run on this host too. It had not failed yet only because the
process restarted at 12:53 local and 01:30 had not come round; `compactor` read
`consecutive_failures: 0, last_success_utc: null`, which is "never due", not "never broken", and
is easy to misread as the latter.

**Resolved by widening the collector policy (`v3`), not by fixing the split.** A new
`WriteDerivedDataAndThePartsArchive` statement grants `PutObject`/`GetObject`/`DeleteObject` on
`energy/hourly/*` and `energy/raw_30s_parts_archive/*`, and the archive prefix joins the
`ListBucket` condition. The old read-only-on-`hourly` statement collapses to `dim_channel` only,
since `hourly` reads are now covered by the write statement. **`glue:GetDatabases` stays
denied** — the catalog half of the boundary is untouched, and `create-glue-tables` remains a
batch/admin operation.

This was a deliberate choice between three options, taken with the trade-off stated rather than
discovered later:

| | |
|---|---|
| Widen the policy (**chosen**) | No code change; rollup and compaction work tonight. Costs the boundary. |
| Grant the parts archive only, disable the rollup job | Keeps the boundary; needs a new config knob and a batch runner for `rollup`. |
| Do the job split now | What the policy was written for; largest change, and `hourly` stays stale until the batch side is scheduled and running. |

**What is given up, stated plainly.** The internet-facing box can now overwrite and delete
derived data. The blast radius is bounded by the fact that `energy/hourly/` is disposable by
design — `rollup` rebuilds any local date range from `raw_30s`, idempotently, which is cardinal
rule 7 — and the parts archive is already covered by the `expire-parts-archive` lifecycle rule.
So the realistic worst case is "re-run a stage", not "lose data". It is still strictly less
containment than `v2` had, and reinstating `v2` is not possible without breaking the collector
until the split exists. If the split is ever done, this entry is the thing to revisit first.

**Verified against the real API as `energycap-collector`**, per the standard this document set
for itself: `PutObject` to `energy/hourly/`, `PutObject` to `energy/raw_30s_parts_archive/`, and
the `CopyObject` into `energy/hourly/` that was the failing call — all three succeeded. Smoke-test
objects hard-deleted with their versions (versioning is on, so a plain delete only writes a
marker); 0 versions and 0 delete markers confirmed on both prefixes afterwards.

One footnote against #174's neighbour trap: **`v3` propagated instantly.** Phase 0 recorded that
a new policy *version* took over four minutes to go live, and warned against believing a fresh
grant is wrong too early. This time the first call after `create-policy-version --set-as-default`
already succeeded. Both observations are real; the honest rule is that version propagation is
unpredictable, so retry before re-diagnosing, and never trust `simulate-principal-policy` in
either direction.


The remaining open questions — Okta token lifetimes and whether the refresh token rotates,
whether the spoofed `Origin`/`Referer`/`Mobile-App-Brand` headers are load-bearing, the
real domain of `status.mode`, the exact shape of `getInfinityEnergy`, the units behind
`statpress`/`blwrpm`/`oat`, and the contents of the legacy DynamoDB table — are enumerated
in **#75**, which remains the checklist for the first live run.

## 182. Green Button carries no prices, so the tariff is a committed config file

PLAN.md §13 designs the meter dataset as kWh and stops there; `docs/review-2026-08-23.md`
B5 asks for the dollars. The first question was whether the utility already sends them.

**It does not, and this is settled rather than assumed.** Against a real LG&E export
(`data/20260809_20260918_GreenButton.xml`, 5,022 `IntervalReading`s):

| Checked | Result |
|---|---|
| `cost` element on any `IntervalReading` | **0 occurrences** |
| `UsageSummary` / `ElectricPowerUsageSummary` | **absent** |
| `TariffProfile`, `billingPeriod`, rate-plan elements | **absent** |
| `ReadingType/currency` | **`0`** on all six — ESPI's "not applicable" |
| Resources reachable from `link rel` | UsagePoint, MeterReading, ReadingType, IntervalBlock, LocalTimeParameters. Nothing billing-shaped to walk to. |

Structurally consistent with the registration in `docs/lge-greenbutton.md`: the granted
scope is `FB=1_3_4_5` (Common, Connect My Data, Interval Metering, Interval Electric
Metering) and LG&E's required function-block set tops out at interval metering. There is
no billing block to have asked for.

So dollars can only come from a transcribed tariff, which is **`config/tariff.json`** —
hand-maintained and committed beside `channel_map.json`, and the same kind of artefact: a
semantic layer the data cannot supply about itself. `tests/test_tariff.py` replays every
cycle in it and requires the printed total back to the cent, so it cannot rot silently.

**The bills are deliberately not committed.** They are account documents with names,
addresses and account numbers on them; `data/` is gitignored. Only the derived rate
parameters are in the repo.

One trap recorded because it looks exactly like billing data and is not: `cost_day_usd` in
`energy/daily` is **Carrier's own HVAC-only estimate** at whatever rate is typed into the
thermostat. It is the only USD in the archive and it must never be treated as a bill.

## 183. Two services, two rate schedules — and the riders do not apply to the same base

Every meter-side surface in this project treats the barn as "the other meter". For pricing
it is more than that: **a different rate schedule on a different account**, and the
difference is not a scale factor.

| | House `1308468` | Barn `1326254` |
|---|---|---|
| Account | *(separate accounts; numbers not committed — public repo)* | *(ditto)* |
| Schedule | Residential Electric Service | **General Service Single Phase** |
| Basic service | $0.47/day | $1.29/day |
| Energy charge | $0.11362/kWh | $0.13248/kWh |
| Fuel adjustment inside the rider base | **yes** | **no** |
| Flat deduction from the rider base | none | **$0.0286/kWh** |
| Home energy assistance charge | $0.30/mo | not charged |
| KY sales tax | exempt (primary residence) | **6%**, on top of the school tax |
| Demand register | none | kW, printed *Information Only*, **not billed** |

And the three percentage riders each apply to a **different** base — on one July house bill
the printed bases are $330.69, $332.38 and $320.68:

```
env_base  = basic + energy + DSM + FAC        (Residential)
          = basic + energy + DSM              (General Service; FAC excluded)
          less $0.0286/kWh                    (General Service only)
rar_base  = env_base + environmental_surcharge
pgr_base  = basic + energy                    (excludes DSM and FAC)
school_base = total_electric - home_energy_assistance   (the one untaxed line)
sales_base  = total_electric + school_tax     (levied ON TOP of it, not alongside)
```

All of it derived from, and re-verified against, ten bills per meter — twenty of twenty
reproduce to the cent. Money is `Decimal` with `ROUND_HALF_UP` **per printed line**, because
LG&E rounds each line and sums the rounded lines; summing exact products and rounding once
gives a different answer, and float banker's rounding gives a third.

Two further facts the bills gave up, recorded so nobody re-derives them:

- **The fuel adjustment and the three rider percentages are re-set monthly and cannot be
  predicted.** Ten observed months span $0.00048 to $0.01063/kWh on the fuel adjustment
  alone — a 22x range — and riders flip between charge and credit (`0.380% CR`). So
  `billing_cycles` in the tariff file is *history*, not a forecast, and a cycle with no bill
  on file is reported as `no_verdict_estimated_riders` rather than being quietly totalled.
- **A mid-cycle rate change is split by actual metered usage, not by day count.** The
  2026-01 house bill put 355 kWh at the old rate where day-proration gives 335. `price_cycle`
  reproduces the real split when handed `kwh_by_date` — which this project has, at 15-minute
  grain — and says `allocation="day_proration"` when it does not. The residual on the three
  affected cycles is bounded at $0.25 by a test.

## 184. The billing cycle is `(read_start, read_end]` — measured, because the day count cannot catch it

A bill prints two meter read dates and a day count: read 6/26 and again 7/28, "32 Days
Billed". Both obvious readings of that give 32 days, so **the day count cannot distinguish
them** — only the kWh move. `verify-bill` initially treated the end read date as exclusive,
which is wrong.

Summing the meter series over each cycle both ways, against the billed kWh:

| Convention | Barn, 8 full cycles | House, 3 full cycles |
|---|---|---|
| `[read_start, read_end)` — end date excluded | mean abs error **3.67%** | **0.56%** |
| `(read_start, read_end]` — end date included | mean abs error **0.16%** | **0.05%** |

A 23x improvement, and the residual loses its alternating sign — the alternation was energy
being shifted between adjacent cycles. Physically the reads land late in the day, so usage on
the read date has already accrued to the cycle being closed.

Corroborated independently: solving for the 2026-01 interim-rate boundary from the barn's
15-minute series (find the day where cumulative kWh reaches the 64 kWh the bill put at the
old rate) lands on **2026-01-01** under this convention — a clean administrative date — where
the exclusive reading gives 2025-12-31.

This lives in one function, `tariff.billing_days`, with the numbers above in its docstring,
and every entry point speaks the bill's language (the two printed read dates) so only that
function knows the offset. The rate-period `effective_from` dates in `config/tariff.json` are
in the *billed-day* frame accordingly: the first day charged at that rate.

Residual after the fix: every covered cycle reads **0.03%–0.65% low** against the billed kWh,
consistently negative. That is the interval series' per-reading rounding against the meter's
integer register — ~0.5 Wh on each of ~3,000 intervals — not a billing error, and it is well
inside the ±1% verdict tolerance.

## 185. The meter dataset gains 2.6 years of history — and a THIRD interval series

`import-greenbutton` was built for bulk history and had never been used for it. Three
Download My Data exports for the house plus one for the barn (2026-08-23) took
`energy/meter` from 23 days to **2024-01-01 .. 2026-08-23, 183,711 rows**.

**The exports do not all reach equally far back, and that is the point.** LG&E's download
offers 15-minute, hourly and daily granularity as separate files, and they have *different*
retention:

| Granularity | Span | Why it matters |
|---|---|---|
| 900s | 2025-07-24 .. 2026-08-23 | ~13 months. Fine enough to verify a bill cycle. |
| 3600s | 2025-07-24 .. 2026-08-23 | Same span; redundant with 900s. |
| **86400s (daily)** | **2024-01-01** .. 2026-08-22 | **2.6 years — the only history before 2025-07-24.** |

So "how far back does Green Button go" has no single answer: the coarser the grain, the
further back it reaches. Worth knowing before anyone concludes the archive starts in 2025.

**This makes the never-sum trap worse, so the catalog comment was rewritten.** There are now
up to three series carrying the same energy for one meter; summing all three is roughly
triple, not double. `_METER_DESCRIPTION` now says so and names the differing spans. Every
consumer in the tree was checked and already pins `interval_s` — `compare.resolve_interval`
(finest wins), `verify_bill.meter_cycle`, `meterview` (per-device finest),
`historyview.METER_INTERVAL_S = 900`. Nothing needed a code change; the risk was purely that
a human or an LLM would query it by hand.

**Cross-validated before trusting any of it.** Over the 395 days where all three grains
overlap they agree to within **0.05%** (39,580.5 daily vs 39,559.7 from 900s vs 39,573.8 from
3600s), so the daily series is a sound answer to "what did last winter use" even though no
finer data exists behind it.

**The S3 mirror had to merge DOWN before pushing up.** The local month files were built from
the downloads; S3's `lge-202608.parquet` held **1,488 barn rows the local copy did not** —
the instance's own Connect fetches, which only ever existed there. Pushing the local files
directly would have deleted them. The fix was to pull S3's month files, run them through the
same `merge_into_month` every other path uses, verify the result was a strict superset of
both sides (0 S3 rows missing, 0 duplicate dedupe tuples), and only then upload.

*This is a standing hazard, not a one-off:* `data/meter/` on the Mac and `energy/meter/` in
S3 are two independently-written copies of one dataset, and the instance writes the one in
S3 daily. Any future bulk import from the Mac must merge down first. Verified after upload by
reading the archive back with DuckDB — 183,711 rows, 183,711 distinct dedupe tuples — and by
one Athena query across `year IN (2024, 2025, 2026)`, which also confirms partition
projection (`2024,2035`) already covered the new years.

## 186. `SCHEDULED_JOBS` — the knob #181 needed and did not have

`docs/review-2026-08-23.md` A2 reported the collector failing `rollup_hourly` hourly and
forever on `AccessDenied`, with `compactor.last_success_utc: null`, and recommended either
moving the derived-data jobs to a host with credentials or adding a job-selection knob.

**The live symptom was already gone by the time the work started.** #181's `v3` policy took
effect between the review's probe and 2026-08-23 19:20 local, and the rollup succeeded:
`consecutive_failures: 0`, `last_day_rolled: 2026-08-23`, 1,603 rows over 19 hours, with
`energy/hourly/year=2026/month=08/rollup-20260823.parquet` present in S3 at that timestamp.
So A2's rollup half closed itself.

**The compactor half was still unproven, and it moves data.** It had never run on the
instance — `last_success_utc: null` is "never due", not "never broken" — and its first
unattended run was due at 01:30. Rather than wait, all six S3 calls the archive move makes
were exercised against the real API as `energycap-collector`, the standard #181 set:

| Call | |
|---|---|
| `PutObject` to `energy/raw_30s/` (write a part) | OK |
| `ListBucket` on the archive prefix | OK |
| `CopyObject` part -> archive | OK |
| `HeadObject` on the archive copy (the size check before deleting the source) | OK |
| `DeleteObject` the source part | OK |
| `DeleteObject` from the archive (the 7-day sweep) | OK |

So tonight's compaction will work. **A caveat on the cleanup, because #181 set a standard
this could not fully meet:** `energycap-batch` lacks `s3:ListBucketVersions` and
`s3:GetLifecycleConfiguration`, so the probe objects could not be hard-deleted with their
versions the way #181 describes. Both prefixes are clean to every normal listing — no stage,
query or table location can see anything — and the two 6-byte noncurrent versions are covered
by the `expire-noncurrent-versions` (30d) rule, the archive one sooner by
`expire-parts-archive` (14d). **The runbook is nonetheless not reproducible with the
credentials it tells you to use**, which is worth fixing the next time that policy is edited.

**What was actually built: the knob.** `SCHEDULED_JOBS` (empty = all, which stays the
default and is PLAN.md §5's one-container design) selects a subset of the schedule by name.
`runtime.SCHEDULED_JOB_NAMES` is the valid set, with a test asserting it matches what
`default_jobs()` really returns so the two cannot drift. An unknown name **raises at boot**
rather than being ignored — a typo that silently disabled `upload_hourly` would present as a
healthy collector whose archive stopped growing, which is #181's failure mode approached from
the other side.

Its justification is no longer "stop attempting what it cannot do". It is that **#181 paid
for the missing knob with the IAM boundary**, choosing to widen the collector policy over
`energy/hourly/` and `energy/raw_30s_parts_archive/` and recording that "if the split is ever
done, this entry is the thing to revisit first." The knob is what makes revisiting possible:
a collector started with `SCHEDULED_JOBS=upload_hourly,bryant_daily_energy,greenbutton_daily`
writes no derived data at all, and the policy can go back to `v2`.

**Not done, and deliberately left as a decision:** actually splitting. That needs the batch
stages given a home with a scheduler (launchd on the Mac), and moving them is a change to
where the work runs, not a config edit. The knob is the prerequisite, landed and tested; the
move is the user's call. Note also that `greenbutton_daily` must stay wherever
`tokens/lge.json` lives — the instance — because the refresh token rotates (STATE.md).

## 187. Delivery — `watch-health`, and the counter that could only go up

`docs/review-2026-08-23.md` A1: "every failure signal this system carefully
produces terminates in `/healthz` and `status.json`, which nothing reads — the
system has detection everywhere and delivery nowhere." Both recent incidents (the
three-day LG&E lapse, the six-day CT zero-latch) ran under a green healthcheck and
were found by a human reading a chart.

`energycap watch-health` is the delivery half: it reads a collector's status
document **from another machine** and pushes to Pushover. `deploy/watchdog.md` is
the runbook, `deploy/com.duckbillhq.energycap-watch.plist` the launchd job.

### The design rule: absence is a failure, not a pass

The obvious implementation of this command is a `jq` expression, and the obvious
`jq` expression is wrong. `/healthz` on the live instance carries **no
`greenbutton` section at all** until the daily fetch has run once, so
`health.meter.stale` is *absent* — and `jq '.health.meter.stale == true'` reads
absence as health, which is the exact disguise #177 wore.

So every rule either finds its evidence or alarms saying it could not. An empty
`{}` document raises five alarms, not zero; an unreachable host is the loudest
alarm there is, because that is what a dead collector looks like. There is a test
for each case, and **the first draft failed one of them**: the `pollers` and
`meter` rules were nested under `if isinstance(health, Mapping)`, so a document
with no `health` block skipped them silently. Caught by
`test_an_empty_document_alarms_on_everything_it_cannot_check` before it shipped —
the bug this command exists to prevent, found inside the command itself.

`HEALTHZ_URL` has no default for the same reason: a watcher that quietly defaults
to localhost is a watcher installed beside the thing it watches, and a box that
has died cannot report that it died.

### The counter that could only go up

Found while deciding what to key the rules on. `Scheduler` wrote the shared
`scheduler` status section **on failure and never on success**, so on the live
instance it read:

```
"scheduler": { "consecutive_failures": 203,
               "last_error": "RollupError: 1 of 1 day(s) failed to roll up: 2026-08-23",
               "job": "rollup_hourly" }
```

while every job was in fact succeeding and the rollup had completed three hours
earlier. A counter that cannot go down is not a signal: anything watching it
alarms forever, gets muted, and then misses the real event.

The reset is **job-aware**, which matters more than it looks. One section is
shared by every job, so clearing it on any success would let the hourly uploader
wipe a rollup streak thirty seconds later — the counter would oscillate 1, 0, 1,
0 and never reach a threshold. So a success clears the section only when the
failure recorded there belongs to the *same* job. The meaning is now exact: the
job named in `job` has failed `consecutive_failures` times in a row and has not
succeeded since. Both halves are pinned by tests; the existing
`test_a_throwing_scheduled_job_does_not_stop_the_scheduler` is what caught the
naive version, since its scenario is precisely a failing job interleaved with a
succeeding sibling.

The watcher still does not consult that aggregate. Each stage's own section
(`rollup`, `uploader`, `compactor`) tracks one stage and has always reset
correctly, and that is the precise signal.

### Not every failing run gets a push

At the 15-minute cadence, alarming on every failing run is ~96 identical
notifications a day for one fault. The owner mutes the channel, and a muted
channel is the silent-failure hole rebuilt one level up. So: push on **state
change**, push an **all-clear** on recovery (a resolved page should be visibly
resolved, not merely quiet), and re-push a persistent fault every 6 hours so one
reported at 03:00 is not forgotten. State in `{SPOOL_DIR}/watch-state.json`; a
missing or corrupt file means "no history", which **sends** — failing open is the
only safe direction for an alarm.

### Two rules `/healthz` will not give us

`uploader` and `spool` (review A3). `/healthz` judges pollers only, so rotated S3
credentials leave it green indefinitely while the archive stops growing. Both
rules read the raw sections and never consult `health.ok`, so no change to
`health.py` was needed.

### Verified live, 2026-08-23

Against the real instance and the real Pushover account: seven checks run, one
alarm raised (`meter` — the `greenbutton` section genuinely is absent since the
process restarted at 16:53 and the fetch fires at 09:15), delivered to the phone,
and on the two following runs `send=false reason=unchanged`. Exit code 1 on a
failing check, on an unreachable host, and on a missing URL.

### What it still cannot tell you

**That the watcher itself stopped.** A launchd job on a sleeping Mac is silent,
and launchd *skips* missed `StartInterval` firings rather than catching up.
Silence is indistinguishable from health — the same class of bug, one level up.
Closing it needs a dead-man's switch (an external service that alerts when pings
STOP); `watch-health` exits 0 on a clean run specifically so one can be appended:
`energycap watch-health && curl -fsS https://hc-ping.com/<uuid>`. Not done here
because it needs an account this project does not have, and it is recorded as the
highest-value remaining item in the alerting story rather than quietly omitted.

## 188. Both coverage gates were half-built — the panel side and the meter side

`docs/review-2026-08-23.md` B1 and B2. Two independent ways the ±5% answer went
silently wrong, with the same shape: a completeness check that could not see the
thing that was missing.

### B1 — `sample_count` cannot see an absent hub

`sample_count` on a compared hour is the **minimum** across the feed channels
that produced rows, which is right for a channel that reported *partially*. It is
blind to a channel that reported *nothing*: an absent hub contributes nothing to
a minimum, so the surviving hub's full count is published as **100% coverage**
while the summed panel energy is short by a whole panel. One hub offline for a
day reads as "the panels are ~50% below the meter", at full coverage, on both the
CLI and the `/ui` meter card.

The fix is to count distinct `(device_id, channel_id)` series per hour and
compare against what SHOULD report. Keyed on the pair, not the channel id: both
hubs publish a `ct_1_a`, so `channel_id` alone cannot tell four reporting series
from two — which is precisely why the original code could not see it.

**Where the expectation comes from matters more than the count.** It is read
from `channel_map.json`, never derived from the measurements, because deriving it
is circular: a hub that stopped reporting for the whole range would simply not be
"expected", so its absence could never make anything incomplete. `historyview`
reached the same conclusion from `dim_channel` (#173b); `compare-meter` uses the
map because it is designed to run with nothing but the collector and the map
needs no build step.

`compare_range` takes the expected count as an argument rather than reading the
map itself — it is the measurement, and where the expectation comes from is
policy belonging to the caller. Left unset it is 0, meaning **UNKNOWN**, and the
report says so out loud rather than quietly excluding nothing. (The first draft
read the map inside `compare_range`, which coupled every test to the repo's real
config and correctly failed them.)

### B2 — `/ui/history` gated only the panel side

`intervals` was already carried on every row of the meter block and never
checked. LG&E publishes late and revises, so a day whose panel side is complete
and whose meter side is half written compares a full day against a partial one
and manufactures a delta the size of the missing part — a measured **+65%** on
one such day, marked `complete: true`, which then polluted `mean_delta_pct`, the
block's headline number.

The gate is the DST-aware interval count: **92, 96 or 100** at 900s. Hard-coding
96 would call the short day incomplete every March and — worse — call the long
day complete every November while an hour of energy went unmeasured.

### B6, folded in because it is the same command

The README's billing recipe fetched both meters and then ran `compare-meter`
with no `--meter`, which is a guaranteed `AmbiguousMeterError`. `meterview`
already consulted the map's `primary` flag; `compare.run` did not, so the CLI and
the `/ui` card disagreed about whether the question was answerable at all.
`compare.run` now defaults to the map's primary. It reads `primary is True`, not
`bool(primary)` — review B7's point that `bool("no")` is `True` is exactly how a
typo becomes a silent wrong answer — and still refuses rather than guessing when
no primary is marked and the meters genuinely differ.

Each of the three gates has a test that fails without it, verified by reverting
the fix and watching it go red.

## 189. The rollup refuses to price energy with an interval the data contradicts

`docs/review-2026-08-23.md` C1, and the only finding in that review that could
silently rewrite history rather than merely report it wrongly.

`kwh = mean * sample_count * poll_interval_s / 3.6e6`, and `poll_interval_s` is
read from the **current environment** — not from the data, not from the file, not
from anything the row carries. Nothing recorded what cadence a given day was
actually collected at, and nothing checked.

So: set `POLL_INTERVAL_S=60` at some future point, then follow this project's own
documented repair path — *"if you fix a collector bug, re-run rollup over the
affected range"* — across 2026, and **every historical kWh doubles**.
Deterministically. Idempotently. With no error raised, no warning logged, and
nothing in the output that looks wrong. The bill comparison, the digest, the
dashboards and `verify-bill` would all agree with each other on the new, wrong
numbers.

**Demonstrated on the real archive** (2026-08-21, one raw input, 826 hourly rows):

| | |
|---|---|
| priced at 30s, the cadence it was collected at | **90.48 kWh** |
| priced at 60s | **180.97 kWh** — exactly 2.00x |

**The guard.** `rollup_day` now measures the data's own cadence and refuses when
it disagrees with the interval about to be used, beyond `INTERVAL_TOLERANCE`
(0.25 — loose on purpose: the failure is a FACTOR, not a few percent of jitter,
and an alarm that fires on jitter gets turned off).

The statistic is the **median gap between consecutive samples of one channel**,
computed in SQL with a window function over the day's `watts` rows. Three
choices, each of which a simpler version got wrong first:

* **Median, not mean.** One collector outage leaves a single enormous delta that
  drags a mean upward.
* **Consecutive deltas, not `(last_ts - first_ts) / (sample_count - 1)`.** That
  per-row average is inflated by any gap *inside* the hour, so the first draft
  refused to roll up a deliberately gapped test hour — the guard firing hardest
  exactly where the data most needs rolling up.
* **`watts` rows only.** They are the only rows whose kWh uses the interval, and
  Bryant has its own `BRYANT_POLL_INTERVAL_S`.

Under `MIN_DELTAS_FOR_SPACING` (20) the answer is `None` and the guard stays
silent: a handful of rows is not evidence of a cadence, and refusing there would
break the rollup of a day the collector had only just started.

The message names the factor the energy would be wrong **by**, which is
`configured / observed` — an earlier draft printed `observed / configured`, the
same number upside down, which would have sent a reader hunting for missing
energy instead of invented energy. A test pins the exact string.

`--poll-interval-s` states the cadence old rows were collected at, and
`--allow-interval-mismatch` downgrades the refusal to a WARN for a cadence that
genuinely changed mid-range. Two existing DST tests were sampling hourly while
claiming 30s; they now declare `poll_interval_s=3600`, which is what their
fixtures actually contain.

**Not done: `observed_seconds` as an HOURLY_SCHEMA column.** The review offers it
as the alternative fix, and it is the better *record* — it would make each row
self-describing, so a reader could verify `kwh == mean * observed_seconds / 3.6e6`
without knowing what `POLL_INTERVAL_S` was in force. It is deferred because
nothing in this codebase reads Parquet with `union_by_name`, so adding a column
splits `energy/hourly` into two incompatible schemas and breaks
`read_parquet('…/rollup-*.parquet')` until **every** existing day is re-rolled.
That re-roll is the sanctioned, idempotent path and is cheap today (7 days) and
never cheaper — but it rewrites production data, so it is a decision rather than
a side effect. The guard above closes the actual hazard without it.

## 190. `energy/hourly` gains `observed_seconds` — kWh made auditable

The other half of #189, and the part that puts the fact in the *data* rather than
only in a guard. `PLAN.md` §10 lists the hourly columns; this is a fifteenth.

`kwh = mean * sample_count * poll_interval_s / 3.6e6`, and that interval came
from the collector's environment at rollup time. Nothing in the file recorded it,
so a reader could not distinguish energy computed at a 30s cadence from the same
samples re-priced at 60s — the two are byte-identical apart from the number.
`observed_seconds` is that denominator written down, which makes every row
self-checking:

```
kwh = mean * observed_seconds / 3.6e6
```

**NULL exactly where `kwh` is NULL.** Tempting to populate it everywhere — it
reads like a property of the sampling, not of the metric — but the rollup uses
ONE interval for the whole day while Bryant polls on its own
`BRYANT_POLL_INTERVAL_S`. Writing Leviton's cadence onto Bryant's rows would put
a quiet falsehood in the one column whose entire purpose is to be trustworthy.
A test pins the null-alignment.

### The migration, and why it did not need a deploy first

Nothing here read Parquet with `union_by_name`, so adding a column splits
`energy/hourly` into two incompatible schemas: `read_parquet('…/rollup-*.parquet')`
fails on the first mismatch and takes `/ui/history` down until every file is
rewritten. Worse, the collector on the instance keeps writing the OLD shape every
hour until it is rebuilt, so "re-roll everything" is not even a stable end state
— the next hourly job re-splits the archive.

So the four hourly reads in `historyview` now use `union_by_name := true`. An
older file reads NULL for a column it predates, which is the truth about that
file. Deliberately NOT applied to the other datasets: their schemas are stable and
blanket tolerance would hide a genuine drift that ought to fail loudly. A test
builds a genuinely mixed archive — one file with the column, one without — and
requires the glob to read.

That makes the re-roll a tidy-up rather than a prerequisite, and the deploy
ordering irrelevant.

### Verified, 2026-08-24

Re-rolled 2026-08-17..24 (8 days, 7,848 rows) and read the result back
independently rather than trusting the stage's counters:

| check | result |
|---|---|
| `kwh = mean * observed_seconds / 3.6e6` | **0 violations** in 2,226 energy rows |
| `(kwh IS NULL) <> (observed_seconds IS NULL)` | **0 rows** |
| 2026-08-21 total, a closed day measured before the change | **90.48 kWh**, unchanged |
| Athena over the updated Glue table | 7,848 rows, 0 violations |

The archive's overall total rose 724.07 -> 732.92 kWh, which is **live data
arriving between the two reads** (rows 7,764 -> 7,848), not the migration: the
closed days are identical.

**One thing the new column immediately exposed**, which is the point of having
it: 56 rows carry `observed_seconds = 3630` — 121 samples in a clock hour where
120 fit. All on 2026-08-22, the breaker-retrofit day. A poll cycle drifting
across an hour boundary lands two samples in the same hour; the energy is
overstated by ~0.8% for those rows. Pre-existing and honest (it really did take
121 samples), but until now there was nothing in the data that could show it.

## 191. The nightly digest — the first thing that says whether a day was UNUSUAL

`docs/review-2026-08-23.md` E: anomaly detection "does not exist yet; the data is
ready". Everything else in this project reports what happened; nothing said
whether it was normal. That role was filled by a human noticing a shape on a
chart, which is how six days of latched CT zeros (#180) and a three-day LG&E
lapse (#177) were both found, days late.

`energycap digest` reviews a local day and pushes what is worth looking at, on
the Pushover channel #187 built. Scheduled `digest_daily` at **06:00 local** —
after the 01:30 compaction and re-roll, so D-1 is finished rather than half
written, and early enough to be waiting at breakfast.

### Two kinds of check, deliberately separate

**A trailing 21-day band per circuit**, needing no knowledge of what a circuit
is. Median and MAD, **not** mean and standard deviation: one genuinely anomalous
day inflates a standard deviation enough to swallow the next one — the failure
mode where a fault that starts today makes tomorrow's identical fault look
normal. The MAD does not move. A test pins that with a 100x outlier.

**Five hard rules**, one per scenario the review named: strip heat in mild
weather, a load that stopped cycling, a circuit that went quiet, the barn outside
its 3.6–40 kWh envelope, and a rising overnight floor.

The strip-heat rule is the one worth the most: `eheat` kWh/day and
`outdoor_temp_f` have both been collected all along — 234 days of the former —
and **nothing ever joined them**. Resistance heat above 45°F outdoor is the most
expensive silent fault this house can have.

### Not crying wolf is the whole design

Every comparison is **coverage-gated on `observed_seconds`** (#190) — the column
added the same day, doing real work immediately. A day the collector only half
watched has roughly half the kWh; reporting that as "usage halved" would train
the owner to ignore the digest, which is exactly how #180 stayed hidden. Such a
day is *named as skipped*, so "quiet" and "not looked at" never look alike. The
gate applies to the **baseline too**: a band built from half-watched days sits
low, and then the first complete day reads as a spike.

Too little history is silence, not a pass: below `MIN_BASELINE_DAYS` a circuit is
reported un-baselined rather than judged.

**One rule had to be rewritten because a test caught it crying wolf.**
`stuck_load` originally fired on any circuit drawing in all 24 hours — which is
every fridge and network rack, every night, forever. It now fires only when the
circuit's own history shows it *normally cycles*, so the finding is the CHANGE.
Two tests hold that line from both sides.

### What it cannot do, measured rather than assumed

**It would not have caught #180.** Panel B's daily feed total is 23.5, 24.0,
21.4, 24.6, 24.1, 24.7 kWh straight through the latched days — flat. A
sub-hourly intermittent fault averages out at day grain, and no daily band will
ever see it. That belongs to the meter comparison (whose gates #188 fixed) or to
a future zero-run rule over `raw_30s`. Saying so here is better than letting the
digest imply a coverage it does not have.

**It is mostly un-baselined today**, and honestly reports that: on the real
archive for 2026-08-23 it compared 8 circuits and named 20 as not yet baselined —
the breakers installed on 08-22 have no history, and `energy/hourly` only reaches
back to 08-17. It becomes useful as the archive grows; it does not pretend to be
useful now.

Verified end to end against the real archive and the real Pushover account:
`digest_done findings=0 compared=0 skipped_unbaselined=28`, delivered.

---

## 192. The documentation sweep — what the catalog said that the archive did not

*Review `docs/review-2026-08-23.md` block F. Six findings, all documentation, all
in the two places an LLM actually reads: the Glue table/column comments and the
README. None of them changes a byte of data; all of them change what a reader
concludes from it.*

### F1 — the nesting hierarchy was published nowhere (HIGH)

**`sum(kwh)` across every channel is 2–3× the house.** A smart breaker is
physically *inside* its panel's feed CT, and the HVAC subpanel feeder (`ct_2_*`)
carries a blower that some branch breakers also see. Add them all up and the same
electrons are counted two or three times — `historyview`'s own first draft
returned about 3× the house total, and the number looked entirely plausible.

This was known and written down. It was written down in `config/channel_map.json`
notes, which are **stripped out of the Parquet**, and in `historyview`'s module
docstring, which is source code. Neither reaches a catalog reader. `SHOW CREATE
TABLE energy.energy_hourly` — the thing this project's own README tells an LLM to
read first — said nothing about it.

Now stated in five places, all of them queryable:

| where | form |
|---|---|
| `DATABASE_DESCRIPTION` | `NESTING_WARNING`, full — it is the first document read |
| `energy_raw_30s`, `energy_hourly` | `NESTING_WARNING_SHORT` (the budget is 2048 chars) |
| `dim_channel` | `NESTING_WARNING`, full — this is the table readers are told to start from |
| the canonical `channel_id` **column** comment | leads with it, because that is the column a reader `GROUP BY`s |
| README "Reading this data honestly" + query rule 2 | with the level table |

The README's level table is pinned by a test to `historyview.LEVELS`, so prose
and the code that enforces it cannot drift apart. A first draft of that table
said the feed level is `ct_1_*` and `ct_3_*`; `LEVEL_SQL` classifies `ct_1_*` on
both hubs as feed and every other `ct_*` as subfeed. The test caught it.

### F2 — the README denied a dataset that has 183,711 rows in it

`energy_meter` was described as "designed but not built" in one place, `lge` as
"designed for, not yet collected" in another, and the Athena section listed four
tables. `DATABASE_DESCRIPTION` omitted the dataset entirely. An LLM following any
of them concludes that checking a bill against the utility's own measurement is
impossible — which is goal 3 of this project.

Fixed in all four, and the database description now carries the `interval_s`
trap, because naming the dataset without it is worse than not naming it.

### F3 — #179's correction missed seven spots

DEVIATIONS #179 established that `stage` and `stage_pct` are one field rendered
two ways, chosen **per reading**, and that this house emits both interleaved —
8,091 and 9,010 rows over six days. The correction reached
`STAGE_REPRESENTATION_NOTE` and the README's own `## Compressor stage` section
and stopped there. Still asserting the pre-#179 claim:

- the README's **enum-decode table** — "**Not emitted on this system**" — the
  single most likely lookup target in the document;
- the README's "Reading this data honestly" — "`WHERE metric = 'stage'` matches
  **zero rows for all time**";
- both worked query examples (DuckDB and Athena), in three comments each;
- `glue.py`'s `ODU_TYPE_OBSERVED` docstring, the `metric` column comment's
  "mutually exclusive stage pair", and the `energy_raw_30s` inline comment
  claiming one metric "can never appear at all";
- `config/channel_map.json`'s `hvac_status` note — "may be permanently empty".

**And the test fixtures.** `tests/test_docs.py`'s corpus emitted `stage_pct` and
no `stage` row, with a docstring explaining that this is "exactly as the live
system does", and a test *asserted* that query 4's `stage` column is NULL in
every row. So the documentation, the fixtures and the tests all agreed with each
other and with nothing else. The corpus now flips rendering at 16:00 local —
inside query 4's 13:00–19:00 window, which the first attempt got wrong and the
test caught — and the assertion is now that **both** columns carry signal in
different buckets, which is the trap a reader actually needs reproduced.

### F4 — `dim_channel.category`'s examples were invented

The published comment offered `kitchen`, `lighting` and `backup-feed`. Not one is
a value this pipeline can produce; the real spelling is `backup_feed`, with an
underscore. `WHERE category = 'lighting'` returns zero rows and reads as "this
house has no lighting circuits". The examples are generated from
`dim.KNOWN_CATEGORIES` now, and a test asserts every member appears.

### F5 — two traps published nowhere queryable

**The enum warning was scoped to three of six metrics.** `op_status`, `odu_mode`
and `idu_status` all carry `unit = 'enum'`, and the README said the "mean and p95
are meaningless" warning "applies to `mode`, `stage` and `fan` **only**". Stating
it as three-of-six is worse than stating it vaguely: it actively licenses
`avg(value)` over the other three.

**The MWBC volts trap.** `breaker_p5`, `breaker_p10` and `breaker_p13` are
multi-wire branch circuits, and `sources/leviton` sums both poles for *every*
metric. Right for `watts` — two independent 120 V legs, so the sum is the real
load — wrong for `volts`, which comes back ~240. This lived only in
`channel_map.json` notes. `dim_channel.category = 'mwbc'` is the only queryable
way to find these channels, so the warning now lives on that column's comment and
in the `dim_channel` table description.

### F6 — four smaller ones

- `compare.py`'s "Because there is no S3 yet" — there has been since 2026-08-19.
  The real reason it reads the spool is better than the stale one: the uploader
  is hourly, so the last hour exists only in the spool and an S3-backed
  comparison would silently compare an incomplete final hour.
- `energycap-container.sh`'s "NOTHING IN THIS FILE HAS EVER BEEN EXECUTED",
  sitting directly above its own measurement log. Narrowed to what is still true:
  the *development Mac* cannot exercise it.
- `.env.example`'s four `LGE_*` lines carried trailing `# from the approval
  email`. python-dotenv strips that; **Apple `container run --env-file` and
  Docker's `--env-file` do not**, so the client id would have been read with the
  comment attached and the token exchange would have failed with an opaque 401.
- README's "the only placeholder left in `channel_map.json` is the future LG&E
  meter" — the meters became real entries on 2026-08-23.

### Budget note

The Glue description limit is 2048 characters and three of the five tables are now
within 25 of it (`energy_raw_30s` has **9** characters of headroom, `energy_hourly`
10, `dim_channel` 23). Several of
these strings are *generated* — the enum decode grows with `bryant.ENUM_TABLES`,
the category list with `dim.KNOWN_CATEGORIES`, the metric list with the schema —
so the next appended enum code will overflow `energy_raw_30s`. That fails loudly
(`_fit` raises, `create-glue-tables` stops) rather than silently truncating, which
is the designed behaviour, but the next person to add a metric should expect to
tighten prose in the same commit.

---

## 193. Test isolation was an allowlist, and it covered 16 of 48 settings

*Review block G. Three tests had been failing on this machine for two sessions;
the failures were the visible tenth of it.*

`tests/conftest.py` opens by promising that "a developer's real `.env` can never
reach a test". It blanks `Settings.model_config["env_file"]`, which does stop
dotenv loading — and **pydantic-settings still reads `os.environ`**. The actual
defence was `_TEST_ENV`, a hand-maintained dict of fake values, and it named 16
of `Settings`' 48 fields.

The other 32 came from whatever the developer had exported. On this machine that
included a real `LGE_CLIENT_ID=gbc_18`, which made three tests fail outright:

- `test_no_lge_credential_has_a_default_value` — asserting `lge_client_id == ""`
  against a shell that had set it;
- `test_no_client_id_is_a_clear_error_not_a_broken_url` — the error it exists to
  prove could not be raised, because the id was present;
- `test_greenbutton_daily_skips_quietly_when_connect_is_not_configured` — got
  `not_authorized` instead of `not_configured`, because from the suite's point of
  view Connect *was* configured.

**The loud failures were the good case.** `LEVITON_INGEST`, `SCHEDULED_JOBS`,
both `PUSHOVER_*`, `HEALTHZ_URL`, `BLACKSTART_INVENTORY_PATH` and the five
integrity thresholds were inherited the same way and simply passed — the suite
was running against one machine's configuration and reporting green. A test that
passes for a reason that is not in the repository is not a test.

**Fixed by inverting the rule.** `_clear_settings_environment` deletes every
variable `Settings` reads before `_TEST_ENV` puts back what the suite chose. The
field list comes from `Settings.model_fields`, so a new setting is isolated the
day it is added rather than the day someone remembers this file — which is the
failure mode that produced this in the first place. `Settings` is
`case_sensitive=False`, so every case variant present in `os.environ` is removed,
not just the upper-case spelling.

`test_no_setting_can_be_inherited_from_the_developers_shell` asserts the
consequence directly: any setting the suite did not ask for holds its declared
default. Verified by sabotage — stubbing the clear out makes it fail on
`LGE_CLIENT_ID` — and the suite is green for the first time in three sessions:
**1,845 passed, 0 failed.**

---

## 194. The safety layer reviewed against itself — `docs/review-2026-08-24.md`

*Items N2–N9, N12 and the long-deferred D2. Everything here is a fault in code
written to catch faults, which is the only category of bug that gets quieter the
worse it is.*

### The digest fired before the data it reviews (N2)

`digest_daily` ran at **06:00** reviewing D-1. D-1's Bryant `eheat` lands at
08:30 and D-1's LG&E intervals at 09:15. The digest never re-checks a day once
its data arrives, so on every scheduled night:

- `strip_heat_in_mild_weather` read an absent `eheat` row,
- `barn_envelope` saw no meter rows,
- `meter_disagreement` — **the only check that can see a mis-scaled clamp** —
  skipped itself for "no overlap".

The three most valuable rules in the digest had never once executed on the
schedule. Manual runs worked perfectly, which is exactly why nobody noticed.

Moved to **10:00**, and the *ordering* is what a test now pins — not the
literal — so moving a fetch later fails with this explanation instead of
silently re-opening the hole.

### The digest violated rule 1 inside itself (N2, second half)

`heat.get("eheat", 0.0)` turned "the Bryant day has not been fetched" into
"eheat used 0 kWh", and the rule returned silently. That is **absence read as
zero**, in the tool written to enforce that it never is. The rules now
distinguish an absent dataset from a measured zero and put the skip in
`report.notes` where the digest body prints it. An `eheat` key missing while
*other* components reported is still a real zero — that is Carrier omitting a
structurally disabled component, which is a different fact.

### `_job_digest` disguised every possible breakage as success (N3)

The import guard was `except Exception`. A `SyntaxError` in `digest.py`, a
missing dependency, a typo in a module constant — all became
`{"skipped": "not_implemented"}`, which the scheduler records as a **success**.
A genuinely broken digest was indistinguishable from a deployment that simply
lacks the stage, forever, on `/healthz` and on the phone alike. Narrowed to
`ModuleNotFoundError` naming the module itself, matching `_job_bryant_daily`,
which had the correct form all along and documents why.

### Neither digest nor integrity could be watched (N3)

A digest that threw landed in the shared `scheduler` section, which
`watch-health` deliberately ignores (it is shared by six jobs — #187). So a
digest crashing every morning for a fortnight looked exactly like a fortnight of
quiet nights. Both stages now write their own `status.json` section **on failure
as well as success**, both joined the watcher's per-stage streak roster, and
there is a new rule: *the last successful digest must be under 26 hours old*,
with a missing section reading as unknown rather than fine.

And because a quiet night and a dead digest are the same silence on a phone, one
firing a week (**Monday**) pushes even with nothing to report.

### A Pushover outage suppressed the alarm it failed to carry (N4)

`_write_state` advanced `firing` — the set the change-detector compares against,
i.e. the record of "this has been reported" — whether or not the push landed. So
after a failed delivery the next run compared the new alarm set against itself,
found no change, and went quiet under the six-hour repeat timer. A brand-new
CRITICAL raised during a Pushover outage stayed silent for up to six hours: the
notifier's own outage suppressing the notification, in a command whose entire
purpose is delivery. An undelivered push now leaves `firing` untouched (the set
is kept under `undelivered` for diagnosis) so the next run re-detects and
retries.

### The `greenbutton` watch rule was unreachable (N5)

`GreenbuttonAuthorizationRevoked` raises *before* the stage runs, so the stage
never records anything and the failure existed only in the ignored `scheduler`
section. Meanwhile `watch.py` **has** a `greenbutton` streak rule and nothing in
the codebase ever called `record_failure("greenbutton")`. Two correct-looking
halves that never met: a repeat of #177 would have fallen through to meter
staleness — three days, WARNING — which is the lapse #177 was written to end.
The revocation now writes greenbutton's own section before raising.

### `BARN_MIN_KWH` was dead code (N6, partial)

Only the high end of the barn envelope was implemented, leaving "the EV silently
stopped charging" — the failure a homeowner actually wants to hear about — with
no check. The low end cannot be a bare threshold: one quiet day is a day nobody
drove, and paging on that is how a channel gets muted. It now needs two things
at once — `BARN_QUIET_DAYS` (4) consecutive days at the floor, **and** a
baseline showing this meter normally charges (`BARN_ACTIVE_FRACTION`). Same
discipline that stopped `stuck_load` alarming on every refrigerator: the finding
is the CHANGE, and only the channel's own past can say what changed.

### The recommended `SCHEDULED_JOBS` split disabled the spool purge (N7)

The knob selects whole jobs, and `daily_maintenance` is a bundle: upload
catch-up, compact, re-roll, **and the retention purge** — the only caller of
`SpoolDB.purge` anywhere. The poller config the docstring and `.env.example`
both showcase omits `daily_maintenance`, so that collector never purges and its
spool grows without bound. Job granularity is not responsibility granularity;
that was A2's lesson arriving one layer down. It cannot be a hard failure — a
real split deployment does want the purge on the batch host — so a process that
spools without a purge job now emits the loudest warning the boot has.

### At most one primary, and `primary` must be a boolean (N8)

`bool(raw.get("primary"))` accepted every string a hand-editor is likely to
type — `"no"`, `"false"`, `"0"` — and made all three **True**, silently
promoting the barn to the house. And *nothing* enforced at-most-one primary per
source, while every consumer assumes it: `compare-meter` checks a bill against
the primary, `meterview` labels it "the house", `historyview` sums what it is
handed. Two primaries means house + barn added together and reported as the
house — and because the completeness gate uses `>=`, the doubled interval count
passes as "complete", so the wrong number arrives wearing a coverage guarantee.

The stakes rose with the 2.6-year import (#185): the house is now in the table
under **three** device ids, so one slipped flag trebles it. Both are build
errors now, and `meterview` tests `is True` rather than truthiness.

### `verify-bill`'s help described a default it did not have, and the convention backwards (N9)

`--meter`'s help promised "Default: the map's primary" while a bare run raised
`AmbiguousMeterError` — and since the import, "more than one meter" is always.
It now defaults to the primary, matching `compare-meter` (B6).

Worse, the command docstring said "`--end` is the meter READ date and is
**EXCLUSIVE**". The code, `tariff.billing_days`, and every other document treat
the days billed as `(start, end]` — start excluded, end included. That is the
exact off-by-one the module was written to kill, printed in its own `--help`.
Corrected, with the measurement (0.16% vs 3.67% MAE) that settled it.

### Freshness could move backwards (N12)

A manual backfill of 2024 is a successful fetch whose `last_ts_utc` is two years
old, and writing it verbatim **rewound** `newest_interval_utc` — a false
staleness alarm raised by a run that added data. Now monotonic. A zero-row fetch
also overwrote the stored `meters` roster with `[]`, the same class as the D1 bug
one field over.

The comparison **parses** rather than comparing strings, and that is not
academic: the fetch summary carries microseconds and a stored stamp may not, and
`'.' < 'Z'`, so `2026-08-16T16:00:00.000000Z` sorts *before* the identical
second-precision stamp. The same instant, ordered wrongly, in the one comparison
whose whole job is to move only forwards.

**Found on the way:** the process-wide `StatusStore` is a module-level singleton
and was never reset between tests, so one test's `status.json` was read back by
the next. It went unnoticed while every user of the store only ever *wrote* to
it; it surfaced the moment a stage started merging its previous value forward.
Same shape as #193's environment leak — state from outside the test, invisible
until something finally looks at it. `conftest` now resets it per test.

### D2 at last — not every rejection is a revocation

Every 400/401/403 on a refresh grant cleared the token cache. Classified by the
RFC 6749 §5.2 `error` code now:

| code | cache | why |
|---|---|---|
| `invalid_grant` | **cleared** | the refresh token is expired, revoked or replayed |
| `invalid_scope` | **cleared** | 2026-08-24's incident: "the requested scope does not match the scope granted by the resource owner" — the consent no longer covers what we ask |
| `invalid_client`, `unauthorized_client`, `unsupported_grant_type`, `invalid_request` | **kept** | about OUR credentials or OUR request; the customer's authorisation is untouched |
| unrecognised / no JSON | **cleared** | an unknown rejection is likelier a dead grant, and re-authorising is a recovery that exists where a silently dead feed is not |

`invalid_client` mattering is the point: a mistyped `LGE_CLIENT_SECRET` used to
destroy a working refresh token, turning a ten-second config fix into a trip to
a browser to re-consent — and nothing about that trip would have fixed the
secret.

### Deliberately not addressed here

**N1 is not a code problem and cannot be fixed from this repository.** The
watchdog's launchd job has never been loaded, the instance runs a torn pre-merge
image, and LG&E needs a browser re-authorisation. Until those three happen the
operational value of everything above — and of everything in #187 and #191 — is
still zero. That is stated plainly rather than counted as progress.

Also carried forward: N10 (store the measured median spacing beside
`observed_seconds`, which records the pricing *assumption*), N11 (one sentence
in #181 about versioning's 30-day box), N9's `intervals > expected` acceptance
and the CWD-relative channel-map degradation, and the older A4/A5/B3/B4/C2/C3/
D3/D4/D5/D6.

---

## 195. One process was silently erasing another's `status.json` sections

*Found by verifying the #194 deploy rather than trusting it: the digest ran
successfully on the instance, and `/healthz` showed no `digest` section at all.*

`StatusStore._flush` writes **the whole in-memory document** atomically, and the
long-running collector re-flushes on every poll cycle. So a stage run as a
one-shot — `docker compose run --rm energycap digest`, which is how every
non-scheduled stage is invoked on the instance — wrote its section, and the
collector overwrote the file from its own copy seconds later. The section
reappeared only if the container happened to restart, because `_merge_existing`
reads the file at startup; that is why `integrity` was present (left over from
before the 23:38 restart) while a digest run two minutes old was not.

**This mattered immediately.** #194 added a `watch-health` rule that alarms when
the digest has not succeeded in 26 hours, keyed on that exact section. A
watchdog reading a field another process quietly deletes is worse than no rule:
it is a rule that alarms forever, gets muted, and takes the rest of the page
with it.

The scheduled path was never affected — `digest_daily` runs inside the collector
process, so it writes to the collector's own store — which is precisely why this
could have sat undiscovered until the first time someone re-ran a stage by hand
and believed the result.

**Fixed by tracking ownership.** A store now records which sections *it* has
written; on every flush it re-reads the file and adopts any section it does not
own. Our sections win over the file, so a stale file cannot resurrect a section
this process has already moved past — the opposite bug, and a subtler one. Two
processes writing the *same* section can still lose an update, which is inherent
to a single-file status document and is why every section has exactly one writer
in practice.

---

## 196. The LG&E refresh grant needs `scope`, and never worked without it

*The answer to "why don't the tokens auto-update". Measured against the live
endpoint on 2026-08-24, not reasoned about.*

**Every refresh this integration has ever attempted failed.** The only thing
that has ever produced a working LG&E token is a human in a browser, and the
three-day #177 lapse plus the 08-24 outage are both this, surfacing.

RFC 6749 §6 makes `scope` **optional** on a refresh grant and says that omitting
it means "the originally granted scope". So `refresh()` sent
`grant_type`+`refresh_token` and nothing else, which is correct against the
specification. This custodian does not implement it that way — it appears to
compare the **client registration's** scope against the grant's and answers:

```
400 {"type": "...rfc9110#section-15.5.1", "title": "invalid_scope", "status": 400,
     "detail": "The requested scope does not match the scope granted by the resource owner."}
```

Probed on a token **four minutes old**, whose granted scope was string-identical
to the configured `LGE_SCOPE`:

| request | result |
|---|---|
| `grant_type`, `refresh_token` | **400 `invalid_scope`** |
| the same, plus `scope=<the token's own granted scope>` | **200**, new 24 h token, refresh token rotated |

So the fix is one form field. The token's **own** granted scope is sent rather
than the configured one: they are identical today, and if they ever diverge the
grant is the truth while `LGE_SCOPE` is a stale local guess.

This was invisible for a specific reason. The access token lasts 24 h and
`greenbutton_daily` runs once a day, so a refresh is attempted roughly once per
day, fails, clears the cache, and the next morning reports `not_authorized` —
which is a *quiet skip*, the correct behaviour for a deployment that was never
authorised. #177 fixed the reporting of that state and the underlying grant was
never exercised in a test, because a unit test with a mock transport passes
whatever the form body contains.

### And a second bug, in yesterday's fix

`_oauth_error` (DEVIATIONS #194) read the code from `payload["error"]`, per RFC
6749 §5.2. **LG&E answers in RFC 9457 problem+json and puts it in `title`.** So
the classification added specifically to stop destroying good tokens never saw a
code from this custodian at all: every rejection fell through to the
unrecognised branch and cleared the cache — including the `invalid_client` case
the classification exists to protect. Both keys are read now.

That is a fix shipped and disproved within the hour, and the only reason it was
caught is that the deploy was *verified against the live endpoint* rather than
against its own tests. Mock transports answer whatever you tell them to.

### Cleanup

The probe left `lge-refresh-probe*.json` on the instance volume; removed. The
successful `scope`-bearing refresh rotated the live refresh token, and the new
pair was written into the real cache rather than discarded, so no browser trip
was spent on the diagnosis.
