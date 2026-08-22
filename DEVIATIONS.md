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

The remaining open questions — Okta token lifetimes and whether the refresh token rotates,
whether the spoofed `Origin`/`Referer`/`Mobile-App-Brand` headers are load-bearing, the
real domain of `status.mode`, the exact shape of `getInfinityEnergy`, the units behind
`statpress`/`blwrpm`/`oat`, and the contents of the legacy DynamoDB table — are enumerated
in **#75**, which remains the checklist for the first live run.
