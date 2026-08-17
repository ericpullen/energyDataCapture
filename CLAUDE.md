# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`energycap` — a household energy/HVAC time-series pipeline. One long-running Python process in Docker on a Mac Mini polls two cloud APIs (Leviton LWHEM-2 load centers, Bryant/Carrier Infinity HVAC) every 30s, spools to SQLite, and lands partitioned Parquet in S3. Queried by DuckDB locally, Athena remotely, and by an LLM over MCP.

Volume is tens of MB/year. **Nothing here is performance-sensitive.** Optimize for query ergonomics, data trustworthiness, and readable code — never for throughput.

## The spec

**`PLAN.md` is the spec of record.** Read the relevant section before implementing a module; it contains all the API research (Leviton endpoints, Carrier GraphQL, DynamoDB layout) so re-researching is usually unnecessary.

- §2 lists **locked decisions** — do not re-litigate them in code or in conversation.
- Where the spec is silent, pick the simplest thing that satisfies the tests in §15.
- If you find a genuine error in the spec, record it in `DEVIATIONS.md` at repo root and proceed — never silently diverge.
- After implementation, a separate session reviews this code adversarially against `PLAN.md`. Write code that survives that.

## Cardinal data rules

These are the correctness contract. Violating one is a bug even if every test passes.

1. **Never fabricate, interpolate, or zero-fill a missing sample. A gap stays a gap.** A null API field emits no row — not a zero. A failed poll cycle emits zero rows — it does not repeat the last value.
2. **Record what the API said, verbatim.** Leviton fw v2 emits spurious zero power readings; they go into raw unfiltered. Filtering is a downstream/query-time concern.
3. **`ts_utc` is canonical.** All bucketing, sorting, and dedupe use `ts_utc`. `ts_local` is a timezone-naive wall clock for humans and LLMs, deliberately ambiguous during DST fall-back.
4. **Partitioning is on LOCAL date**, derived from `ts_local`. Every real query is a local-time question.
5. **kWh is observed-time-only**: `kwh = mean_watts * (sample_count * poll_interval_s) / 3.6e6`. Never extrapolate across a gap. `sample_count` is what distinguishes "the load was off" from "the collector was down" — it must reach every rollup row and every table comment.
6. **Day-grain rows never enter `raw_30s`.** `energy/daily` (Bryant kWh/day) would poison hourly rollups.
7. **Every stage is idempotent** over an arbitrary `--start/--end` local date range. Deterministic output filenames so a re-run overwrites instead of duplicating. Dedupe key everywhere: `(ts_utc, source, device_id, channel_id, metric)`.
8. **No secrets in the repo, no credentials or tokens in logs.** Token caches live only on the mounted `/data` volume, mode 600. The log scrubber is tested (§15.8).

## Commands

`uv` is not yet installed on this machine (`curl -LsSf https://astral.sh/uv/install.sh | sh`). Docker is at `/usr/local/bin/docker`. Python 3.12.7 via pyenv.

```bash
uv sync                          # install deps from pyproject.toml
uv run pytest                    # full suite; all pure-logic tests run with no network/AWS
uv run pytest tests/test_rollup.py -k kwh

uv run energycap run             # the long-running process (poll loops + scheduler + health server)
uv run energycap discover        # enumerate live Leviton/Bryant channels, print channel_map skeleton
uv run energycap poll --once
uv run energycap upload --start 2026-08-15 --end 2026-08-16
uv run energycap compact-daily --start ... --end ...
uv run energycap rollup --start ... --end ...        # re-run after fixing a collector bug
uv run energycap backfill --start ... --end ...
uv run energycap build-dim
uv run energycap create-glue-tables

docker compose up -d && docker compose logs -f
curl localhost:8080/healthz
```

Every scheduled stage is also a standalone CLI command over an arbitrary date range — keep it that way when adding stages.

## Architecture

One container, one process (`energycap run`) hosting: an asyncio 30s poll loop → SQLite spool; a Leviton bandwidth keepalive task; an in-process scheduler (hourly upload, 01:30 daily compaction, hourly rollup, ~08:30 Bryant daily energy fetch); a small HTTP health server.

Data flows: `sources/*` → `Observation` rows → SQLite spool → hourly `part-*.parquet` → daily `day-*.parquet` (parts moved to a non-tabled archive prefix, never left alongside the day file) → DuckDB-SQL hourly rollup. See `PLAN.md` §4–5 for the S3 layout and §10 for stage contracts.

Module layout is fixed in `PLAN.md` §5 (`src/energy_capture/{config,logging,timeutil,model,health}.py`, `sources/`, `spool/`, `stages/`, `aws/`, `cli.py`). Some invariants about it:

- **`timeutil.py` is the only place UTC↔local conversion and partition-date math happens.** No `zoneinfo` calls scattered through stages.
- **`model.py` owns the canonical Arrow schema** plus the sort and dedupe key constants. The future `meter` dataset adds an `interval_s` column (§13) — the schema module must express that variant without a hack.
- **External clients live behind thin adapters.** `aioleviton` and (likely) `carrier-api` are wrapped in `sources/*` so a stale upstream can be vendored or replaced without touching the pipeline.
- **The rollup SQL is the documentation of the kWh math.** Keep it in one readable `.sql` file or module constant, not assembled from string fragments.
- **`aws/s3io.py` does atomic writes only**: temp key → copy → delete, then verify by reading back Parquet metadata row count. No partial files at final keys.

## Conventions

- Structured JSON logs to stdout, one object per line (`ts`, `level`, `stage`, `event`, counts, durations). No print debugging left behind.
- Every knob is an env var via `pydantic-settings` (`PLAN.md` §14). `.env` is gitignored; keep `.env.example` current whenever you add a setting.
- Poll intervals have hard floors in code (30s), regardless of what the env var says.
- Enum metrics (`mode`, `stage`, `fan`) store a small integer in `value` with `unit="enum"`. The mapping tables in `sources/bryant.py` are **append-only** — a test pins the current values. Never renumber; an unknown API string logs WARN and emits no row.
- `channel_id` conventions: Leviton breakers `breaker_p{position}` (position, never the API's breaker `id` — fw ≥2.2.0 mutates ids), CTs `ct_{channel}_{a,b}`, hub `panel_leg_{a,b}`; Bryant `zone_{n}` / `system` for status, lowercase component name for daily energy.
- Glue table and column comments are a **first-class deliverable** — they are what an LLM reads to orient itself. Write the real strings (grain, local-date partitioning, dedupe key, the `sample_count` gap warning, enum decodes, the kWh formula), never placeholders.

## API gotchas that will bite

- **Leviton keepalive is mandatory.** The cloud serves stale cached readings unless the hub is in high-bandwidth mode: `PUT /IotWhems/{id}` `{"bandwidth": 1}` every 50s per connected hub. **Never send `bandwidth: 0`** — fw 2.1.0 disconnects the hub for 10–20s.
- Leviton auth: the login response `id` *is* the token, sent as a bare `authorization` header (no `Bearer`). No refresh endpoint — cache and re-login on 401. **Never log in more than once per 10 seconds**, and never per-poll.
- Leviton returns transient 502/504 routinely. Retry within the cycle, then give up quietly (WARN once, no rows, bump the failure counter). The poll loop never crashes.
- Carrier: OAuth2 password grant against Okta, but **use `refresh_token` for renewal** — the old collector re-authenticated with the password every run, which is unacceptable at 30s. GraphQL calls need the spoofed `Origin: https://my.carrier.com` / `Referer` headers.
- Carrier energy field casing differs between `energyPeriods` (camelCase) and `energyConfig` (lowercase). Components with `enabled: false` are structurally absent, not zero — skip them.

## Related repos on this machine

- `~/code/bryantDataCollector` — the existing daily Bryant collector (Lambda + DynamoDB). Source for the backfill and reusable auth/GraphQL logic. **Port and adapt; never import from it.** It keeps running independently as a safety net — do not modify it.
- `~/code/blackstart` — panel inventory web app; `data/montfort.json` is the source of truth for circuit labels/panels/slots, joined via breaker `position` ↔ blackstart slot. Path comes from `BLACKSTART_INVENTORY_PATH`.

## Testing

`PLAN.md` §15 enumerates the required coverage; it is the correctness contract, not a wish list. The ones most likely to catch a real regression: rollup math (observed-time kWh), DST boundaries in America/Kentucky/Louisville (23- and 25-hour local days), gap handling, compactor safety (parts never archived unless the day-file verify passes), and log scrubbing.

Client code is tested against recorded JSON fixtures — 2-pole breaker, CT pair with a null leg, a spurious zero passing through verbatim, fw2.2 suffixed ids ignored in favor of `position`. Never hit a live API from a test.
