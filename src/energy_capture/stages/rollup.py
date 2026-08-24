"""Hourly rollup: ``energy/raw_30s`` -> ``energy/hourly`` (PLAN.md §10).

The heart of the correctness contract. Everything here exists to make one
sentence true: **an hourly row describes only the time we actually observed.**

What it does
------------
For each LOCAL date in ``--start/--end`` it regenerates the *entire* day's
``rollup-{YYYYMMDD}.parquet`` from the day's raw Parquet — cheap, and it avoids
intra-day merge logic entirely (PLAN.md §10). Re-running is therefore both the
idempotency story and the late-data story: raw that lands after a rollup ran is
healed by the next run covering that day, and "re-run rollup over the range" is
the documented fix after a collector bug.

How it does it
--------------
One DuckDB query, kept whole in :download:`rollup.sql` next to this module.
That file is the documentation of the math (CLAUDE.md); this module only binds
parameters to it. Nothing here builds SQL out of strings.

Two Python-side inputs are handed to the query as registered relations rather
than as inline SQL, because both are things Python already owns:

``rollup_hours``
    One row per physical hour of the local day — ``(hour_start_utc,
    hour_end_utc, local_hour_start)`` straight out of
    :func:`energy_capture.timeutil.iter_local_hours`. 23 rows on a
    spring-forward day, 25 on a fall-back day. This keeps **all** UTC<->local
    conversion inside ``timeutil`` (CLAUDE.md) — there is not one timezone
    expression in the SQL — and it is what makes the bucket key
    ``hour_start_utc`` rather than the naive local hour (DEVIATIONS.md #1).
    Grouping on the wall-clock label would merge the fall-back day's two 01:00
    hours and silently lose an hour of data.

``rollup_excluded_metrics``
    :data:`energy_capture.model.DAY_GRAIN_METRICS`, so the exclusion of
    ``kwh_day``/``cost_day_usd`` (CLAUDE.md rule 6) cannot drift from the model.

What it will never do
---------------------
Fill a gap. There is no hour-spine LEFT JOIN, no ``COALESCE``, no zero-fill and
no interpolation anywhere in this stage. An hour with no samples produces **no
row**; a partly observed hour produces one row with a smaller ``sample_count``.
``sample_count`` is the only thing that lets a reader distinguish "the load was
off" from "the collector was down", so it is on every row, always.

``kwh`` is observed-time-only — ``mean_watts * (sample_count *
POLL_INTERVAL_S) / 3.6e6`` — and is ``NULL``, never ``0``, for every metric
other than ``watts`` (PLAN.md §2.5, DEVIATIONS.md #2).

Entry points
------------
``run(*, start, end)``
    What ``energycap rollup`` calls: reads S3 via DuckDB ``httpfs``, writes each
    day's rollup with :func:`energy_capture.aws.s3io.write_table_atomic`, and
    updates the ``rollup`` section of ``status.json``.
:func:`rollup_day`
    The pure core: local date + a list of Parquet paths (local files or ``s3://``
    URIs) -> an Arrow table in :data:`energy_capture.model.HOURLY_SCHEMA`. This
    is what the tests drive; it touches no network and no S3.
:func:`rollup_range`
    ``run`` with the S3 wiring injected, so the day loop, the status updates and
    the idempotency guarantees are testable offline.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

import duckdb
import pyarrow as pa

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import get_settings
from energy_capture.logging import get_logger

__all__ = [
    "EXCLUDED_METRICS_RELATION",
    "HOURS_RELATION",
    "INTERVAL_TOLERANCE",
    "MIN_DELTAS_FOR_SPACING",
    "SQL_PATH",
    "DayRollup",
    "PollIntervalMismatch",
    "RollupError",
    "observed_interval_s",
    "connect",
    "excluded_metrics_table",
    "hours_table",
    "load_sql",
    "rollup_day",
    "rollup_range",
    "run",
]

log = get_logger("rollup")


class RollupError(RuntimeError):
    """At least one local day in the range failed to roll up.

    Raised only after every day has been attempted, so one bad day cannot stop
    the rest of the range — the same policy as ``uploader.UploadFailed`` and
    ``compactor.CompactionError``. Chained to the first underlying exception.
    """

#: The query. One file, executed verbatim (CLAUDE.md: the rollup SQL is the
#: documentation of the kWh math — it is never assembled from fragments).
SQL_PATH: Path = Path(__file__).with_name("rollup.sql")

#: Relation names the SQL expects to find registered on the connection.
HOURS_RELATION = "rollup_hours"
EXCLUDED_METRICS_RELATION = "rollup_excluded_metrics"

#: ``status.json`` section this stage owns (PLAN.md §11).
STATUS_SECTION = "rollup"

#: DuckDB thread count. One, deliberately. Multi-threaded hash aggregation
#: combines partial sums in a non-deterministic order, so ``avg()`` — and
#: therefore ``kwh`` — could differ in the last bit between runs, and a
#: re-run would no longer be byte-identical. The whole dataset is tens of MB
#: per year; nothing here is performance-sensitive (CLAUDE.md).
DUCKDB_THREADS = 1


# --------------------------------------------------------------------- inputs


@lru_cache(maxsize=1)
def load_sql() -> str:
    """The contents of ``rollup.sql`` (read once)."""
    return SQL_PATH.read_text(encoding="utf-8")


def hours_table(local_day: date, *, tz: str | None = None) -> pa.Table:
    """The local day's hours as a relation the SQL can join against.

    Columns ``(hour_start_utc, hour_end_utc, local_hour_start)``, one row per
    *physical* hour: 23 on the spring-forward day, 25 on the fall-back day,
    where the two 01:00 rows share a ``local_hour_start`` but have distinct
    ``hour_start_utc`` values — which is exactly why the rollup groups on the
    latter (DEVIATIONS.md #1).

    Built from :func:`energy_capture.timeutil.iter_local_hours` so that no
    timezone logic exists in the SQL.
    """
    hours = list(timeutil.iter_local_hours(local_day, tz=tz))
    return pa.table(
        {
            "hour_start_utc": pa.array(
                [h.start_utc for h in hours], type=pa.timestamp("us", tz="UTC")
            ),
            "hour_end_utc": pa.array(
                [h.end_utc for h in hours], type=pa.timestamp("us", tz="UTC")
            ),
            "local_hour_start": pa.array(
                [h.local_start for h in hours], type=pa.timestamp("us")
            ),
        }
    )


def excluded_metrics_table() -> pa.Table:
    """Day-grain metrics, as a relation — the SQL's exclusion list.

    Sourced from :data:`energy_capture.model.DAY_GRAIN_METRICS` so the rule
    "day-grain rows never enter an hourly mean" (CLAUDE.md rule 6) is stated in
    exactly one place.
    """
    return pa.table(
        {"metric": pa.array(sorted(model.DAY_GRAIN_METRICS), type=pa.string())}
    )


#: Deltas below this are treated as noise, not cadence: a duplicate that
#: survived dedupe under a different key, or two sources writing the same
#: instant. Never a real 30s poll.
MIN_DELTA_S: float = 1.0

#: How far the configured interval may sit from the observed cadence before the
#: rollup refuses. Deliberately loose: the failure this guards against is a
#: FACTOR (30s priced as 60s doubles every kWh), not a few percent of jitter,
#: and an alarm that fires on jitter gets turned off.
INTERVAL_TOLERANCE: float = 0.25

#: Fewer deltas than this is not evidence of a cadence.
MIN_DELTAS_FOR_SPACING: int = 20

#: The data's own cadence: the MEDIAN gap between consecutive samples of one
#: channel. Median, not mean, because a collector outage leaves one enormous
#: delta that would drag a mean upward — the median is unmoved by a minority of
#: gaps, which is exactly the property needed here (a day full of gaps must not
#: be mistaken for a day sampled slowly).
_INTERVAL_SQL: Final[str] = """
WITH samples AS (
    SELECT DISTINCT ts_utc, source, device_id, channel_id
    FROM read_parquet($input_files)
    WHERE metric = 'watts'
      AND ts_utc >= $day_start_utc
      AND ts_utc <  $day_end_utc
),
deltas AS (
    SELECT date_diff('second', lag(ts_utc) OVER w, ts_utc) AS delta_s
    FROM samples
    WINDOW w AS (PARTITION BY source, device_id, channel_id ORDER BY ts_utc)
)
SELECT median(delta_s) AS observed_s, count(*) AS n
FROM deltas
WHERE delta_s IS NOT NULL AND delta_s >= $min_delta_s
"""


class PollIntervalMismatch(RollupError):
    """The interval used for kWh disagrees with the data's actual cadence.

    The one deterministic way this project can silently rewrite history. ``kwh``
    is ``mean * sample_count * poll_interval_s / 3.6e6``, and
    ``poll_interval_s`` is read from the CURRENT environment rather than from
    the data. Change ``POLL_INTERVAL_S`` to 60 next year, then follow the
    documented repair path — "re-run rollup over the range" — across 2026, and
    every historical kWh doubles. Deterministically, idempotently, with no
    error raised and nothing in the output that looks wrong.
    """


def observed_interval_s(
    con: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
) -> float | None:
    """The sample cadence the DATA shows, in seconds, or ``None`` if unknowable.

    Only ``watts`` rows are measured: they are the only ones whose kWh uses the
    interval at all, and another source may legitimately sample at its own
    cadence (``BRYANT_POLL_INTERVAL_S`` is a separate knob).
    """
    row = con.execute(
        _INTERVAL_SQL,
        {
            "input_files": list(files),
            "day_start_utc": day_start_utc,
            "day_end_utc": day_end_utc,
            "min_delta_s": MIN_DELTA_S,
        },
    ).fetchone()
    if not row or row[0] is None or int(row[1] or 0) < MIN_DELTAS_FOR_SPACING:
        return None
    return float(row[0])


def _check_interval(
    observed: float | None, interval: int, local_day: date, *, allow_mismatch: bool
) -> None:
    """Refuse to price energy with an interval the data contradicts."""
    if observed is None or observed <= 0:
        # Not enough samples to tell. Silence is correct: a handful of rows is
        # not evidence, and refusing on it would make the guard fire hardest
        # exactly where there is least data.
        return
    if abs(observed / interval - 1.0) <= INTERVAL_TOLERANCE:
        return
    # kwh = mean * sample_count * interval / 3.6e6, so pricing 30s data at 60s
    # DOUBLES it. The factor the energy is wrong by is configured/observed --
    # not observed/configured, which is the same number upside down and was
    # what an earlier draft of this message printed.
    error_factor = interval / observed
    message = (
        f"{local_day}: kWh would be computed with poll_interval_s={interval}s, "
        f"but the data's own median sample spacing is {observed:.1f}s. "
        f"Rolling up anyway would multiply every kWh for this day by "
        f"{error_factor:.2f}. Pass the interval these rows were COLLECTED at "
        "(`rollup --poll-interval-s`), not the one configured now."
    )
    if not allow_mismatch:
        raise PollIntervalMismatch(message)
    log.warning(
        "rollup_interval_mismatch_allowed",
        local_day=local_day.isoformat(),
        configured_s=interval,
        observed_s=round(observed, 2),
        kwh_error_factor=round(interval / observed, 3),
        detail=message,
    )


def _poll_interval_s(explicit: int | None = None) -> int:
    """``POLL_INTERVAL_S`` — the observed seconds each sample stands for."""
    if explicit is not None:
        if int(explicit) <= 0:
            raise ValueError(f"poll_interval_s must be positive, got {explicit!r}")
        return int(explicit)
    return int(get_settings().poll_interval_s)


# ---------------------------------------------------------------- connection


def connect(
    *, threads: int = DUCKDB_THREADS, s3: bool = False
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection configured for this stage.

    ``threads=1`` for reproducible floating-point aggregation (see
    :data:`DUCKDB_THREADS`) and ``TimeZone='UTC'`` so ``TIMESTAMPTZ`` values
    round-trip to Arrow as ``timestamp[us, tz=UTC]`` regardless of the host's
    zone. Pass ``s3=True`` to load ``httpfs`` and the AWS credential chain —
    only needed when an input is an ``s3://`` URI, so tests never touch it.
    """
    con = duckdb.connect(config={"threads": int(threads)})
    con.execute("SET TimeZone='UTC'")
    if s3:
        _configure_s3(con)
    return con


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    """Load ``httpfs`` and hand DuckDB the same credentials boto3 would use.

    ``credential_chain`` walks the standard AWS chain (env vars, profile,
    instance role), which is what the container has. Credentials never reach a
    log line: nothing here is logged but the region.
    """
    settings = get_settings()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    clauses = ["TYPE s3", "PROVIDER credential_chain"]
    if settings.aws_region:
        clauses.append(f"REGION '{settings.aws_region}'")
    if settings.aws_profile:
        clauses.append(f"PROFILE '{settings.aws_profile}'")
    con.execute(f"CREATE OR REPLACE SECRET energycap_s3 ({', '.join(clauses)})")
    log.debug("duckdb_s3_configured", region=settings.aws_region or None)


def _to_arrow(result: Any) -> pa.Table:
    """Materialise a DuckDB result as an Arrow table across duckdb versions."""
    fetch = getattr(result, "to_arrow_table", None) or result.fetch_arrow_table
    return fetch()


# ------------------------------------------------------------------- the core


def rollup_day(
    local_day: date,
    inputs: Sequence[str] | Iterable[str],
    *,
    poll_interval_s: int | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
    tz: str | None = None,
    allow_interval_mismatch: bool = False,
) -> pa.Table:
    """Roll one LOCAL day's raw Parquet up to hourly rows.

    Args:
        local_day: the local calendar date to rebuild (partitioning is on the
            local date — CLAUDE.md rule 4).
        inputs: Parquet paths for that day's partition — local file paths in
            tests, ``s3://…`` URIs in production. Hourly parts and a compacted
            day file may both be present; duplicates collapse on the canonical
            dedupe key.
        poll_interval_s: seconds each sample stands for (defaults to
            ``POLL_INTERVAL_S``). Only ``watts`` uses it, via ``kwh``.
            Cross-checked against the data's own sample spacing; a disagreement
            beyond :data:`INTERVAL_TOLERANCE` raises
            :class:`PollIntervalMismatch` rather than silently rescaling
            history.
        allow_interval_mismatch: downgrade that refusal to a WARN. For the
            genuine case where the cadence really did change mid-day; never
            for making an error go away.
        connection: reuse an existing DuckDB connection (one is created and
            closed otherwise).
        tz: override the local zone (tests).

    Returns:
        A table in :data:`energy_capture.model.HOURLY_SCHEMA`, sorted by
        :data:`energy_capture.model.HOURLY_DEDUPE_KEY`. Hours with no samples
        are **absent** — never zero-filled. An empty ``inputs`` yields an empty
        table (a day with no raw data has no hourly rows; that is a gap, and a
        gap stays a gap).
    """
    files = [str(path) for path in inputs]
    if not files:
        return model.HOURLY_SCHEMA.empty_table()

    interval = _poll_interval_s(poll_interval_s)
    day_start_utc, day_end_utc = timeutil.local_day_bounds_utc(local_day, tz=tz)

    owned = connection is None
    con = connection if connection is not None else connect(
        s3=any(f.startswith("s3://") for f in files)
    )
    # Held in locals for the duration of the query: DuckDB's registered
    # relations are views over these Arrow tables, not copies of them.
    hours = hours_table(local_day, tz=tz)
    excluded = excluded_metrics_table()
    try:
        con.register(HOURS_RELATION, hours)
        con.register(EXCLUDED_METRICS_RELATION, excluded)
        result = con.execute(
            load_sql(),
            {
                "input_files": files,
                "day_start_utc": day_start_utc,
                "day_end_utc": day_end_utc,
                "poll_interval_s": interval,
            },
        )
        table = _to_arrow(result)
        observed = observed_interval_s(
            con, files, day_start_utc=day_start_utc, day_end_utc=day_end_utc
        )
    finally:
        if owned:
            con.close()

    # The query already emits HOURLY_SCHEMA's columns in order; the cast pins
    # the types (and the non-nullability of everything except `kwh`) so a
    # DuckDB change can never quietly alter the on-disk schema.
    _check_interval(
        observed, interval, local_day, allow_mismatch=allow_interval_mismatch
    )
    return table.cast(model.HOURLY_SCHEMA)


# --------------------------------------------------------------- the day loop


@dataclass(frozen=True, slots=True)
class DayRollup:
    """Outcome of rolling up one local day."""

    local_day: date
    rows: int
    #: Number of raw Parquet inputs read.
    inputs: int
    #: Where it was written (``None`` when nothing was written).
    key: str | None = None
    #: Distinct local hours that produced at least one row (never gap-filled).
    hours: int = 0
    table: pa.Table | None = field(default=None, repr=False, compare=False)


InputResolver = Callable[[date], Sequence[str]]
DayWriter = Callable[[pa.Table, date], str | None]


def rollup_range(
    *,
    start: date,
    end: date,
    resolve_inputs: InputResolver,
    write: DayWriter | None,
    poll_interval_s: int | None = None,
    connection: duckdb.DuckDBPyConnection | None = None,
    status: Any | None = None,
    tz: str | None = None,
    s3: bool | None = None,
    allow_interval_mismatch: bool = False,
) -> list[DayRollup]:
    """Regenerate every local day in ``[start, end]``; return one result per day.

    Each day is independent and complete — the whole day's rollup file is
    rebuilt, so a re-run overwrites rather than merging (PLAN.md §10). Days
    whose partition holds no raw Parquet are skipped (``write`` is not called):
    an absent rollup file means "no raw data for that day", which is a truthful
    gap, and writing an empty file would not make it less of one.

    ``write`` returns the key/path it wrote (or ``None``); pass ``write=None``
    for a dry run. ``s3`` says whether the inputs are ``s3://`` URIs (so DuckDB
    needs ``httpfs``); leave it ``None`` to detect it from the first day's
    inputs.

    Every day is attempted even if an earlier one failed, and :class:`RollupError`
    is raised at the end if any did — the same policy the uploader and compactor
    follow. The 01:30 job asks for D-3..D-1, and one unreadable old day must not
    stop yesterday (the day anyone actually queries) from being rebuilt.
    """
    store = _status_store(status)
    results: list[DayRollup] = []
    failures: list[tuple[date, BaseException]] = []

    owned = connection is None
    con = connection if connection is not None else connect(
        s3=_needs_s3(start, end, resolve_inputs) if s3 is None else s3
    )
    try:
        for day in timeutil.iter_local_dates(start, end):
            try:
                result = _rollup_one_day(
                    day,
                    resolve_inputs=resolve_inputs,
                    write=write,
                    poll_interval_s=poll_interval_s,
                    connection=con,
                    tz=tz,
                    allow_interval_mismatch=allow_interval_mismatch,
                )
            except Exception as exc:
                log.error(
                    "rollup_day_failed",
                    local_date=day.isoformat(),
                    error=f"{type(exc).__name__}: {exc}",
                )
                if store is not None:
                    store.record_failure(STATUS_SECTION, exc, last_day_attempted=day)
                failures.append((day, exc))
                continue

            results.append(result)
            if store is not None and result.inputs:
                store.record_success(
                    STATUS_SECTION,
                    last_day_rolled=day.isoformat(),
                    rows=result.rows,
                    hours=result.hours,
                )
    finally:
        if owned:
            con.close()

    if failures:
        days = ", ".join(day.isoformat() for day, _ in failures)
        raise RollupError(
            f"{len(failures)} of {len(list(timeutil.iter_local_dates(start, end)))} "
            f"day(s) failed to roll up: {days}"
        ) from failures[0][1]

    return results


def _rollup_one_day(
    day: date,
    *,
    resolve_inputs: InputResolver,
    write: DayWriter | None,
    poll_interval_s: int | None,
    connection: duckdb.DuckDBPyConnection,
    tz: str | None,
    allow_interval_mismatch: bool = False,
) -> DayRollup:
    """One local day of :func:`rollup_range`. Raises on any failure."""
    inputs = list(resolve_inputs(day))
    if not inputs:
        log.warning("rollup_no_raw_input", local_date=day.isoformat())
        return DayRollup(local_day=day, rows=0, inputs=0)

    table = rollup_day(
        day,
        inputs,
        poll_interval_s=poll_interval_s,
        connection=connection,
        tz=tz,
        allow_interval_mismatch=allow_interval_mismatch,
    )
    hours = _distinct_hours(table)
    key = write(table, day) if write is not None else None
    log.info(
        "rollup_day_ok",
        local_date=day.isoformat(),
        rows=table.num_rows,
        hours=hours,
        inputs=len(inputs),
        key=key,
        dry_run=write is None,
    )
    return DayRollup(
        local_day=day,
        rows=table.num_rows,
        inputs=len(inputs),
        key=key,
        hours=hours,
        table=table,
    )


def _distinct_hours(table: pa.Table) -> int:
    """Hours that produced at least one row. Not "hours in the day" — the
    difference between the two is the gap, and this stage never fills it."""
    if table.num_rows == 0:
        return 0
    return len(set(table.column("hour_start_utc").to_pylist()))


def _needs_s3(start: date, end: date, resolve_inputs: InputResolver) -> bool:
    """True when any day's inputs are ``s3://`` URIs (so ``httpfs`` is needed)."""
    for day in timeutil.iter_local_dates(start, end):
        for path in resolve_inputs(day):
            return str(path).startswith("s3://")
    return False


def _status_store(status: Any | None) -> Any | None:
    """The status store to update: the caller's, the process default, or none."""
    if status is not None:
        return status
    try:
        from energy_capture.health import get_status_store

        return get_status_store()
    except Exception:  # pragma: no cover - telemetry must never break a stage
        log.warning("status_store_unavailable")
        return None


# ---------------------------------------------------------------- CLI entry


def run(
    *,
    start: date,
    end: date,
    bucket: str | None = None,
    client: Any | None = None,
    poll_interval_s: int | None = None,
    dry_run: bool = False,
    status: Any | None = None,
    allow_interval_mismatch: bool = False,
) -> dict[str, Any]:
    """``energycap rollup --start … --end …`` (PLAN.md §10).

    Rebuilds ``energy/hourly/year=YYYY/month=MM/rollup-{YYYYMMDD}.parquet`` for
    every LOCAL date in the range, reading that day's ``energy/raw_30s``
    partition (hourly parts and/or the compacted day file) over DuckDB
    ``httpfs``. Deterministic filenames plus a whole-day rebuild make this
    idempotent: running it twice leaves byte-identical objects.
    """
    target_bucket = bucket or s3io.default_bucket()
    s3_client = client if client is not None else s3io.get_client("s3")

    def resolve_inputs(day: date) -> list[str]:
        keys = s3io.list_keys(
            target_bucket,
            s3io.raw_30s_day_prefix(day),
            suffix=".parquet",
            client=s3_client,
        )
        return [s3io.s3_uri(target_bucket, key) for key in keys]

    def write(table: pa.Table, day: date) -> str:
        key = s3io.hourly_key(day)
        s3io.write_table_atomic(table, target_bucket, key, client=s3_client)
        return key

    results = rollup_range(
        start=start,
        end=end,
        resolve_inputs=resolve_inputs,
        write=None if dry_run else write,
        poll_interval_s=poll_interval_s,
        allow_interval_mismatch=allow_interval_mismatch,
        status=status,
        s3=True,
    )

    rows = sum(r.rows for r in results)
    written = [r for r in results if r.key is not None]
    summary: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(results),
        "days_written": len(written),
        "days_without_raw": sum(1 for r in results if r.inputs == 0),
        "rows": rows,
        "dry_run": dry_run,
    }
    log.info("rollup_ok", **summary)
    return summary
