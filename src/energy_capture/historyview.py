"""The ``/ui/history`` data layer: the S3 archive, not the spool.

``/ui`` and ``/ui/hvac`` read the live SQLite spool, which is a **7-day window** by
construction — ``SpoolDB.purge`` deletes rows that are uploaded *and* older than
``SPOOL_RETENTION_DAYS``. So no amount of work on those screens can ever answer
"how did January compare to July". That is this module's whole reason to exist:
it reads the Parquet datasets in S3 (``energy/hourly``, ``energy/daily``,
``energy/meter``, ``energy/dim_channel``) through DuckDB ``httpfs``.

Four blocks, one per question worth asking of an archive:

``circuits``
    Per-circuit kWh over the range, ranked, joined to ``dim_channel`` for labels.
``meter``
    The LG&E revenue meter against the summed panel feed CTs, day by day.
``hvac``
    Bryant day-grain energy by component, month by month — the seasonal arc.
``coverage``
    ``sample_count`` per day per series: where the gaps are, and when each
    channel came online.

Every block is independent and wrapped: one failing query lands a string in
``errors`` and the rest of the page still renders, the same contract
``/ui/data`` and ``/ui/hvac/data`` already keep.

The traps this module is written around
---------------------------------------

These are not hypothetical. Each one produced a wrong number during the S3
rollout before it was caught, and each is now structural here rather than a
comment somebody has to remember.

**Never group by ``channel_id`` alone.** Eight ``channel_id`` values exist on
*both* Leviton hubs — ``breaker_p1``, ``breaker_p10``, ``breaker_p14``,
``breaker_p26``, ``ct_1_a``, ``ct_1_b``, ``panel_leg_a``, ``panel_leg_b`` —
because both panels have a position 1. ``count(DISTINCT channel_id)`` returns 24
where the truth is 32. Every grouping here carries the full
``(source, device_id, channel_id)`` key, and :data:`SERIES_KEY_SQL` is the only
way a series is named.

**Never sum kWh across levels.** The measurement hierarchy nests: a breaker's
watts are also inside its panel's feed CT, and the HVAC subpanel feeder
(``ct_2_*``) contains the blower that some branch breakers also see. An
unqualified ``sum(kwh)`` over every channel is meaningless — it was 3x the house
total in the first draft of this file. :data:`LEVEL_SQL` classifies every series
as ``feed`` / ``subfeed`` / ``branch`` / ``reference`` and totals are only ever
reported *within* a level. The house total is the ``feed`` level, which is what
the meter is comparable to.

**``panel_leg_*`` carries no energy at all.** Those series report only ``hz`` and
``volts``, never ``watts``, so they have no kWh and are classified ``reference``.
A UI that expected them to sum would silently show zero.

**Pin ``interval_s`` on the meter.** Every LG&E meter publishes the *same energy*
as both a 900s and a 3600s series. Summing without pinning double counts — over
this archive ``sum(value)`` returns 3,113 kWh for 2,056 kWh of real consumption,
and it looks entirely plausible. :data:`METER_INTERVAL_S` pins it and the choice
is reported in the document.

**One meter is not the house.** ``1308468`` is the house; ``1326254`` is a
separately metered barn that is ~100% EV charging. They are different services
and must never be summed. The house is found by joining
``dim_channel.is_primary`` (DEVIATIONS #178), never by hardcoding an id here.

**Coverage is per series, and the worst one governs.** The first draft of the
meter block divided ``sum(sample_count)`` by the hours *present* rather than the
hours *expected*, which reported 95.5% coverage for 2026-08-17 — a day that is
actually 36% covered — and turned an incomplete day into a −60% disagreement.
That is DEVIATIONS #173b's blended-coverage bug, rediscovered. Coverage here is
computed per series against the expected hours of that **local** day (23, 24 or
25 across DST, from :mod:`~energy_capture.timeutil`), and a day only gets a
meter delta when every expected series reported and the worst of them clears
:data:`COMPLETE_COVERAGE`.

**A gap stays a gap.** Nothing here interpolates, zero-fills, or carries a value
forward. A day with no rows is absent from the series, not a zero, and the
coverage block exists precisely so absence is visible rather than inferred.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Final

from energy_capture import timeutil
from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger

log = get_logger("historyview")

__all__ = [
    "COMPLETE_COVERAGE",
    "DEFAULT_RANGE_DAYS",
    "LEVELS",
    "MAX_RANGE_DAYS",
    "METER_INTERVAL_S",
    "SAMPLES_PER_HOUR",
    "HistoryRange",
    "build_document",
    "parse_range",
    "reset_cache",
]

#: Watt samples an hour holds at the 30-second poll interval. The rollup's
#: ``sample_count`` is what makes a gap visible, so the denominator matters:
#: 120 per hour per series, never "however many rows happened to be there".
SAMPLES_PER_HOUR: Final[int] = 120

#: Fraction of a day's expected samples a series must reach before the day is
#: treated as complete enough to compare against the utility meter. Slightly
#: below 1.0 because a single dropped poll cycle should not disqualify a day —
#: and above 1.0 is possible (the WebSocket sometimes delivers extra cycles).
COMPLETE_COVERAGE: Final[float] = 0.98

#: Which of the meter's two interval series to use. Both carry the SAME energy
#: (see the module docstring); 900s is the finer of the two and is what
#: ``compare-meter`` uses, so the two agree.
METER_INTERVAL_S: Final[int] = 900

DEFAULT_RANGE_DAYS: Final[int] = 90
MAX_RANGE_DAYS: Final[int] = 400
MIN_RANGE_DAYS: Final[int] = 1

#: How long a built document is reused. S3 + DuckDB is far slower than the
#: spool reads ``/ui/data`` does, and the archive only changes when the hourly
#: rollup lands, so re-querying per page-poll would be pure waste.
CACHE_TTL_S: Final[float] = 300.0

#: The measurement levels, outermost first. Totals are only ever summed WITHIN
#: one of these — see the module docstring on the nesting hierarchy.
LEVELS: Final[tuple[str, ...]] = ("feed", "subfeed", "branch", "reference", "other")

_LEVEL_NOTES: Final[dict[str, str]] = {
    "feed": (
        "Panel feed CTs (ct_1_*) on both hubs — the whole-house total, and the "
        "only level comparable to the utility meter."
    ),
    "subfeed": (
        "Sub-panel feeders (ct_2_*) — the HVAC subpanel. INSIDE the feed level; "
        "never add these to it."
    ),
    "branch": (
        "Individual smart-breaker branch circuits. INSIDE the feed level, and "
        "partly inside the subfeed too."
    ),
    "reference": (
        "panel_leg_* — voltage and frequency reference only. These report no "
        "watts, so they have no energy at all."
    ),
    "other": "Non-Leviton 30s series (Bryant status). Not energy.",
}

#: The only way a series is named. Grouping on channel_id alone under-counts 32
#: series as 24 — see the module docstring.
SERIES_KEY_SQL: Final[str] = "h.source || '/' || h.device_id || '/' || h.channel_id"

#: Classify a series into the measurement hierarchy. Derived from the channel_id
#: convention (PLAN.md §6.5) rather than from dim_channel.category, because
#: category describes what a circuit FEEDS ("hvac"), not where it sits in the
#: nesting — ct_2_* and breaker_p10 are both category 'hvac' and are at
#: different levels.
LEVEL_SQL: Final[str] = """CASE
        WHEN h.source <> 'leviton'         THEN 'other'
        WHEN h.channel_id LIKE 'panel_leg%' THEN 'reference'
        WHEN h.channel_id LIKE 'ct_1_%'     THEN 'feed'
        WHEN h.channel_id LIKE 'ct_%'       THEN 'subfeed'
        ELSE 'branch'
    END"""


# --------------------------------------------------------------------- range


@dataclass(frozen=True, slots=True)
class HistoryRange:
    """An inclusive range of LOCAL dates, plus the expected-hours math.

    ``expected_hours`` is per local day and DST-aware — 23 on the spring-forward
    day, 25 on the fall-back day — because it is the denominator of every
    coverage figure on the page and a flat 24 would quietly mis-state two days a
    year.
    """

    start: date
    end: date
    preset: str | None = None

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    def local_days(self) -> list[date]:
        return [self.start + timedelta(days=n) for n in range(self.days)]

    def expected_hours(self, local_day: date) -> int:
        """Physical hours in ``local_day`` — 23, 24 or 25."""
        return len(list(timeutil.iter_local_hours(local_day)))

    def expected_samples(self, local_day: date) -> int:
        return SAMPLES_PER_HOUR * self.expected_hours(local_day)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "days": self.days,
            "preset": self.preset,
        }


class RangeError(ValueError):
    """A ``?start=``/``?end=``/``?days=`` that must be answered with 400."""


def _parse_date(raw: str, what: str) -> date:
    try:
        return date.fromisoformat(raw.strip())
    except (TypeError, ValueError) as exc:
        raise RangeError(f"{what} must be an ISO local date (YYYY-MM-DD), got {raw!r}") from exc


def parse_range(
    params: dict[str, str] | None = None, *, today: date | None = None
) -> HistoryRange:
    """Turn query parameters into a :class:`HistoryRange`.

    ``days=N`` (a preset) or an explicit ``start``/``end`` pair. Anything
    unparseable is a :class:`RangeError` — a **400**, never a silent default,
    because a chart that quietly answers a different question than the one the
    URL asked is worse than an error.
    """
    params = params or {}
    reference = today or timeutil.local_date_of(timeutil.now_utc())

    raw_start, raw_end, raw_days = (
        params.get("start"), params.get("end"), params.get("days")
    )

    if raw_start or raw_end:
        if not (raw_start and raw_end):
            raise RangeError("start and end must be given together")
        start, end = _parse_date(raw_start, "start"), _parse_date(raw_end, "end")
        preset = None
    else:
        if raw_days is None:
            days = DEFAULT_RANGE_DAYS
        else:
            try:
                days = int(str(raw_days).strip())
            except (TypeError, ValueError) as exc:
                raise RangeError(f"days must be a positive integer, got {raw_days!r}") from exc
        if days < MIN_RANGE_DAYS:
            raise RangeError(f"days must be at least {MIN_RANGE_DAYS}, got {days}")
        if days > MAX_RANGE_DAYS:
            raise RangeError(f"days must be at most {MAX_RANGE_DAYS}, got {days}")
        end, start = reference, reference - timedelta(days=days - 1)
        preset = f"{days}d"

    if end < start:
        raise RangeError(f"end {end.isoformat()} is before start {start.isoformat()}")
    span = (end - start).days + 1
    if span > MAX_RANGE_DAYS:
        raise RangeError(f"range spans {span} days; the maximum is {MAX_RANGE_DAYS}")
    return HistoryRange(start=start, end=end, preset=preset)


# ------------------------------------------------------------------- sources


@dataclass(frozen=True, slots=True)
class Sources:
    """The S3 URIs each block reads. Built from ``s3io``, never re-typed."""

    hourly: str
    daily: str
    meter: str
    dim: str
    bucket: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "hourly": self.hourly,
            "daily": self.daily,
            "meter": self.meter,
            "dim_channel": self.dim,
        }


def _sources(bucket: str) -> Sources:
    from energy_capture.aws import s3io

    return Sources(
        # Globs rather than per-partition keys: partition pruning is not worth
        # the complexity at ~100 MB/year, and a missing month must read as
        # "no rows", not as a failure.
        hourly=f"s3://{bucket}/{s3io.HOURLY_PREFIX}/*/*/rollup-*.parquet",
        daily=f"s3://{bucket}/{s3io.DAILY_PREFIX}/year=*/*.parquet",
        meter=f"s3://{bucket}/{s3io.METER_PREFIX}/year=*/*.parquet",
        dim=f"s3://{bucket}/{s3io.dim_channel_key()}",
        bucket=bucket,
    )


def _rows(con: Any, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    """Run ``sql`` and return plain dicts, so the JSON layer stays dumb."""
    result = con.execute(sql, params)
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _f(value: Any, digits: int = 3) -> float | None:
    """Round for transport; ``None`` stays ``None`` (a gap is not a zero)."""
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


# -------------------------------------------------------------------- blocks


_CIRCUITS_SQL: Final[str] = f"""
SELECT
    h.source,
    h.device_id,
    h.channel_id,
    {SERIES_KEY_SQL}                       AS series_key,
    {LEVEL_SQL}                            AS level,
    d.label,
    d.short_label,
    d.category,
    d.panel,
    d.room,
    d.priority,
    sum(h.kwh)                             AS kwh,
    max(h.max)                             AS peak_w,
    sum(h.sample_count)                    AS samples,
    count(*)                               AS hours,
    count(DISTINCT h.local_hour_start::DATE) AS days_seen,
    min(h.local_hour_start)::DATE          AS first_day,
    max(h.local_hour_start)::DATE          AS last_day
FROM read_parquet(?) h
LEFT JOIN read_parquet(?) d
       ON d.source = h.source
      AND d.device_id = h.device_id
      AND d.channel_id = h.channel_id
WHERE h.metric = 'watts'
  AND h.local_hour_start >= ?
  AND h.local_hour_start < ?
GROUP BY ALL
ORDER BY kwh DESC NULLS LAST
"""

_CIRCUIT_DAILY_SQL: Final[str] = f"""
SELECT
    h.local_hour_start::DATE               AS local_day,
    {SERIES_KEY_SQL}                       AS series_key,
    {LEVEL_SQL}                            AS level,
    sum(h.kwh)                             AS kwh,
    sum(h.sample_count)                    AS samples
FROM read_parquet(?) h
WHERE h.metric = 'watts'
  AND h.local_hour_start >= ?
  AND h.local_hour_start < ?
GROUP BY ALL
ORDER BY local_day, series_key
"""


def circuits_block(con: Any, src: Sources, rng: HistoryRange) -> dict[str, Any]:
    """Per-circuit energy over the range, ranked, with per-level totals.

    Levels are reported separately and never added together — see the module
    docstring. ``coverage_pct`` is against the expected hours of the days the
    range actually spans, so a channel installed mid-range reads as partial
    rather than as a low consumer.
    """
    lo, hi = _window(rng)
    rows = _rows(con, _CIRCUITS_SQL, [src.hourly, src.dim, lo, hi])
    daily = _rows(con, _CIRCUIT_DAILY_SQL, [src.hourly, lo, hi])

    expected_total = sum(rng.expected_samples(day) for day in rng.local_days())

    out_rows: list[dict[str, Any]] = []
    for row in rows:
        samples = int(row.get("samples") or 0)
        out_rows.append(
            {
                "series_key": row["series_key"],
                "source": row["source"],
                "device_id": row["device_id"],
                "channel_id": row["channel_id"],
                "level": row["level"],
                # An unmapped channel keeps its identity rather than vanishing:
                # breaker_p0 (DEVIATIONS #171) is real recorded data with no
                # dim_channel row, and an INNER JOIN would silently drop it.
                "label": row.get("label") or _unmapped_label(row),
                "short_label": row.get("short_label") or _unmapped_label(row),
                "category": row.get("category"),
                "panel": row.get("panel"),
                "room": row.get("room"),
                "priority": row.get("priority"),
                "mapped": row.get("label") is not None,
                "kwh": _f(row.get("kwh")),
                "peak_w": _f(row.get("peak_w"), 1),
                "samples": samples,
                "hours": int(row.get("hours") or 0),
                "days_seen": int(row.get("days_seen") or 0),
                "first_day": _isoday(row.get("first_day")),
                "last_day": _isoday(row.get("last_day")),
                "coverage_pct": _f(
                    100.0 * samples / expected_total, 1
                ) if expected_total else None,
            }
        )

    levels = []
    for level in LEVELS:
        members = [r for r in out_rows if r["level"] == level]
        if not members:
            continue
        kwh_values = [r["kwh"] for r in members if r["kwh"] is not None]
        levels.append(
            {
                "level": level,
                "note": _LEVEL_NOTES[level],
                "series": len(members),
                # Summed only within the level. Across levels this would
                # double-count the hierarchy.
                "kwh": _f(sum(kwh_values)) if kwh_values else None,
                "samples": sum(r["samples"] for r in members),
            }
        )

    return {
        "rows": out_rows,
        "levels": levels,
        "daily": [
            {
                "local_day": _isoday(r["local_day"]),
                "series_key": r["series_key"],
                "level": r["level"],
                "kwh": _f(r.get("kwh")),
                "samples": int(r.get("samples") or 0),
            }
            for r in daily
        ],
        "expected_samples_total": expected_total,
    }


_METER_SQL: Final[str] = """
WITH panels AS (
    SELECT
        h.local_hour_start::DATE                       AS local_day,
        h.device_id || '/' || h.channel_id             AS series_key,
        sum(h.kwh)                                     AS kwh,
        sum(h.sample_count)                            AS samples
    FROM read_parquet(?) h
    WHERE h.metric = 'watts'
      AND h.channel_id LIKE 'ct_1_%'
      AND h.local_hour_start >= ?
      AND h.local_hour_start < ?
    GROUP BY ALL
), panel_day AS (
    SELECT
        local_day,
        sum(kwh)      AS panel_kwh,
        count(*)      AS series_seen,
        min(samples)  AS worst_samples
    FROM panels GROUP BY local_day
), meter_day AS (
    SELECT
        m.ts_local::DATE     AS local_day,
        sum(m.value)         AS meter_kwh,
        count(*)             AS intervals
    FROM read_parquet(?) m
    JOIN read_parquet(?) d
      ON d.source = m.source
     AND d.device_id = m.device_id
     AND d.channel_id = m.channel_id
     AND d.is_primary
    WHERE m.metric = 'kwh_interval'
      AND m.interval_s = ?
      AND m.ts_local >= ?
      AND m.ts_local < ?
    GROUP BY ALL
)
SELECT
    COALESCE(p.local_day, m.local_day) AS local_day,
    p.panel_kwh,
    p.series_seen,
    p.worst_samples,
    m.meter_kwh,
    m.intervals
FROM panel_day p
FULL OUTER JOIN meter_day m ON m.local_day = p.local_day
ORDER BY local_day
"""

#: How many panel feed series SHOULD exist, read from the semantic layer rather
#: than from the measurements.
#:
#: Deriving it from the hourly data would be circular: a feed that stopped
#: reporting for the whole range would simply not be "expected", so its absence
#: could never make a day incomplete — and the panel total would silently miss a
#: whole panel while still publishing a delta. DEVIATIONS #173b's rule is that
#: every MAPPED channel must have reported, and `dim_channel` is what knows the
#: mapping. A test pins this (three of four feeds present is not comparable).
_METER_SERIES_SQL: Final[str] = """
SELECT DISTINCT d.device_id || '/' || d.channel_id AS series_key
FROM read_parquet(?) d
WHERE d.source = 'leviton' AND d.channel_id LIKE 'ct_1_%'
"""


def meter_block(con: Any, src: Sources, rng: HistoryRange) -> dict[str, Any]:
    """The utility meter against the summed panel feed CTs, day by day.

    A delta is published **only** for a day where every feed series reported and
    the worst of them cleared :data:`COMPLETE_COVERAGE`. Anything else carries
    its coverage and a null delta: an incomplete day compared against a complete
    meter reading manufactures a disagreement that is not real (DEVIATIONS
    #173b, and it recurred while this module was being written).
    """
    lo, hi = _window(rng)
    expected_series = len(_rows(con, _METER_SERIES_SQL, [src.dim]))
    rows = _rows(
        con,
        _METER_SQL,
        [src.hourly, lo, hi, src.meter, src.dim, METER_INTERVAL_S, lo, hi],
    )

    out: list[dict[str, Any]] = []
    for row in rows:
        local_day = row["local_day"]
        expected = rng.expected_samples(local_day) if isinstance(local_day, date) else None
        worst = row.get("worst_samples")
        seen = int(row.get("series_seen") or 0)
        coverage = (
            _f(100.0 * float(worst) / expected, 1)
            if worst is not None and expected else None
        )
        complete = bool(
            expected_series
            and seen == expected_series
            and worst is not None
            and expected
            and float(worst) >= COMPLETE_COVERAGE * expected
        )
        panel_kwh, meter_kwh = _f(row.get("panel_kwh")), _f(row.get("meter_kwh"))
        delta_pct = None
        if complete and panel_kwh is not None and meter_kwh:
            delta_pct = _f(100.0 * (panel_kwh - meter_kwh) / meter_kwh, 2)
        out.append(
            {
                "local_day": _isoday(local_day),
                "meter_kwh": meter_kwh,
                "panel_kwh": panel_kwh,
                "intervals": int(row["intervals"]) if row.get("intervals") else None,
                "series_seen": seen or None,
                "series_expected": expected_series or None,
                "coverage_pct": coverage,
                "complete": complete,
                "delta_pct": delta_pct,
            }
        )

    deltas = [r["delta_pct"] for r in out if r["delta_pct"] is not None]
    return {
        "rows": out,
        "interval_s": METER_INTERVAL_S,
        "series_expected": expected_series or None,
        "complete_days": len(deltas),
        "mean_delta_pct": _f(sum(deltas) / len(deltas), 2) if deltas else None,
        "notes": [
            f"Meter energy is the {METER_INTERVAL_S}s interval series only. Every "
            "meter publishes the same energy as both a 900s and a 3600s series; "
            "summing both double counts.",
            "The house meter is found through dim_channel.is_primary. The barn is "
            "a separate service and is never included.",
            "A delta is shown only for a day where every panel feed series "
            f"reported and the worst cleared {COMPLETE_COVERAGE:.0%} coverage.",
        ],
    }


_HVAC_SQL: Final[str] = """
SELECT
    strftime(y.ts_local, '%Y-%m')  AS month,
    y.channel_id                   AS component,
    d.short_label                  AS label,
    sum(y.value)                   AS kwh,
    count(*)                       AS days_reported
FROM read_parquet(?) y
LEFT JOIN read_parquet(?) d
       ON d.source = y.source
      AND d.device_id = y.device_id
      AND d.channel_id = y.channel_id
WHERE y.metric = 'kwh_day'
  AND y.ts_local >= ?
  AND y.ts_local < ?
GROUP BY ALL
ORDER BY month, kwh DESC
"""


def hvac_block(con: Any, src: Sources, rng: HistoryRange) -> dict[str, Any]:
    """Bryant day-grain energy by component, per month — the seasonal arc.

    Day-grain rows live only in ``energy_daily`` (cardinal rule 6) and are NOT
    comparable to the 30s watt series: this is the only source that can separate
    the blower from the strip heat, because on the panel side they share one
    conductor.

    Components this system does not have (``reheat``, ``fangas``, ``gas``,
    ``looppump``) are reported with their real 0.0 rather than dropped — a zero
    the API actually sent is data, and `backfill` cannot know retroactively which
    components were disabled.
    """
    lo, hi = _window(rng)
    rows = _rows(con, _HVAC_SQL, [src.daily, src.dim, lo, hi])

    months: list[str] = []
    components: dict[str, float] = {}
    out: list[dict[str, Any]] = []
    for row in rows:
        month = row["month"]
        if month not in months:
            months.append(month)
        kwh = _f(row.get("kwh"), 1) or 0.0
        components[row["component"]] = components.get(row["component"], 0.0) + kwh
        out.append(
            {
                "month": month,
                "component": row["component"],
                "label": row.get("label") or row["component"],
                "kwh": kwh,
                "days_reported": int(row.get("days_reported") or 0),
            }
        )

    # Only components that ever moved get a colour slot; the always-zero ones
    # would spend four of eight categorical hues saying nothing.
    active = sorted(
        (name for name, total in components.items() if total > 0),
        key=lambda name: components[name],
        reverse=True,
    )
    inactive = sorted(name for name, total in components.items() if total <= 0)
    return {
        "rows": out,
        "months": months,
        "components": active,
        "components_always_zero": inactive,
        "totals": {name: _f(components[name], 1) for name in components},
        "note": (
            "Day-grain HVAC energy from the Carrier cloud — the only source that "
            "separates the air-handler blower from the electric strip heat, which "
            "share one conductor on the panel side. Never add these to a 30s watt "
            "series or to energy_hourly."
        ),
    }


_COVERAGE_SQL: Final[str] = f"""
SELECT
    h.local_hour_start::DATE  AS local_day,
    {SERIES_KEY_SQL}          AS series_key,
    {LEVEL_SQL}               AS level,
    d.short_label             AS label,
    sum(h.sample_count)       AS samples,
    count(*)                  AS hours_present
FROM read_parquet(?) h
LEFT JOIN read_parquet(?) d
       ON d.source = h.source
      AND d.device_id = h.device_id
      AND d.channel_id = h.channel_id
WHERE h.metric = 'watts'
  AND h.local_hour_start >= ?
  AND h.local_hour_start < ?
GROUP BY ALL
ORDER BY local_day, series_key
"""


def coverage_block(con: Any, src: Sources, rng: HistoryRange) -> dict[str, Any]:
    """``sample_count`` per series per local day, against what the day expects.

    This is the block that makes cardinal rule 1 legible: a gap stays a gap, and
    the only way to tell "the load was off" from "the collector was down" is
    ``sample_count``. It is also how the panel retrofit reads as history — a
    series simply has no cells before the day its breaker went in.

    The denominator is the **expected** hours of each local day (23/24/25 across
    DST), never the hours that happen to be present.
    """
    lo, hi = _window(rng)
    rows = _rows(con, _COVERAGE_SQL, [src.hourly, src.dim, lo, hi])

    days = [day.isoformat() for day in rng.local_days()]
    series: dict[str, dict[str, Any]] = {}
    cells: list[dict[str, Any]] = []
    for row in rows:
        key = row["series_key"]
        if key not in series:
            series[key] = {
                "series_key": key,
                "label": row.get("label") or _unmapped_label(
                    {"channel_id": key.rsplit("/", 1)[-1], "device_id": key.split("/")[1]}
                ),
                "level": row["level"],
            }
        local_day = row["local_day"]
        expected = rng.expected_samples(local_day) if isinstance(local_day, date) else 0
        samples = int(row.get("samples") or 0)
        cells.append(
            {
                "local_day": _isoday(local_day),
                "series_key": key,
                "samples": samples,
                "expected": expected,
                "coverage_pct": _f(100.0 * samples / expected, 1) if expected else None,
                "hours_present": int(row.get("hours_present") or 0),
                "hours_expected": rng.expected_hours(local_day)
                if isinstance(local_day, date) else None,
            }
        )

    complete = sum(1 for c in cells if c["coverage_pct"] and c["coverage_pct"] >= 98.0)
    return {
        "days": days,
        "series": sorted(series.values(), key=lambda s: (s["level"], s["label"])),
        "cells": cells,
        "samples_per_hour": SAMPLES_PER_HOUR,
        "cells_total": len(cells),
        "cells_complete": complete,
        "note": (
            "An absent cell means the collector recorded nothing for that series "
            "that day — not that the load was off. Nothing here is interpolated "
            "or zero-filled. Expected samples per day is 120/hour x the physical "
            "hours of that LOCAL day (23, 24 or 25 across DST)."
        ),
    }


# ------------------------------------------------------------------ assembly


def _window(rng: HistoryRange) -> tuple[datetime, datetime]:
    """Half-open naive local bounds — ``[start 00:00, end+1 00:00)``.

    Naive on purpose: ``local_hour_start`` and ``ts_local`` are both
    timezone-naive wall clocks (CLAUDE.md rule 3), so comparing them against a
    naive local bound is the whole point. Partitioning is on the local date, so
    this is also the boundary the files were written on.
    """
    lo = datetime.combine(rng.start, datetime.min.time())
    hi = datetime.combine(rng.end + timedelta(days=1), datetime.min.time())
    return lo, hi


def _unmapped_label(row: dict[str, Any]) -> str:
    """A name for a channel with no ``dim_channel`` row.

    It MUST carry the device: ``breaker_p0`` exists on both hubs (DEVIATIONS
    #171), so labelling it by ``channel_id`` alone puts two different circuits on
    the screen under one name — the same trap this module exists to avoid, showing
    up in the UI's own labels. The hub id's last group is enough to tell them
    apart without turning the axis into serial numbers.
    """
    device = str(row.get("device_id") or "")
    tail = device.rsplit("_", 1)[-1] if device else ""
    suffix = f" · {tail}" if tail else ""
    return f"{row['channel_id']}{suffix} (unmapped)"


def _isoday(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return None


def _kpi(circuits: dict[str, Any], meter: dict[str, Any], rng: HistoryRange) -> dict[str, Any]:
    """The four headline numbers. Each names the level it came from."""
    feed = next((lvl for lvl in circuits.get("levels", []) if lvl["level"] == "feed"), None)
    branches = [
        r for r in circuits.get("rows", [])
        if r["level"] == "branch" and r["kwh"] is not None
    ]
    top = max(branches, key=lambda r: r["kwh"], default=None)
    days_with_data = len({c["local_day"] for c in circuits.get("daily", [])})
    return {
        "house_kwh": {
            "value": feed["kwh"] if feed else None,
            "label": "House energy",
            "detail": "Sum of the panel feed CTs — the whole-house total.",
        },
        "top_circuit": {
            "value": top["kwh"] if top else None,
            "label": top["short_label"] if top else "Top circuit",
            "detail": "Largest single branch circuit over the range.",
        },
        "meter_delta_pct": {
            "value": meter.get("mean_delta_pct"),
            "label": "Panels vs meter",
            "detail": (
                f"Mean over {meter.get('complete_days') or 0} fully covered "
                "day(s). Negative means the panels read low."
            ),
        },
        "days_with_data": {
            "value": days_with_data,
            "label": "Days with data",
            "detail": f"Of {rng.days} day(s) in the range.",
        },
    }


@dataclass
class _CacheEntry:
    key: tuple[Any, ...]
    document: dict[str, Any]
    built_at: float = field(default_factory=time.monotonic)


_cache: _CacheEntry | None = None
_cache_lock = threading.Lock()


def reset_cache() -> None:
    """Drop the cached document (tests, and a manual refresh)."""
    global _cache
    with _cache_lock:
        _cache = None


def build_document(
    rng: HistoryRange,
    *,
    settings: Settings | None = None,
    bucket: str | None = None,
    con: Any = None,
    use_cache: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The whole ``/ui/history/data`` document. Never raises for data.

    A block that fails contributes its error string to ``errors`` and is omitted;
    the page renders what did work. Only a missing bucket is fatal enough to
    return an otherwise-empty document, because then there is no archive at all.
    """
    global _cache

    resolved = settings or get_settings()
    target = bucket if bucket is not None else (getattr(resolved, "s3_bucket", "") or "")
    reference = timeutil.ensure_utc(now) if now else timeutil.now_utc()

    cache_key = (rng.start, rng.end, target)
    if use_cache and con is None:
        with _cache_lock:
            entry = _cache
        if entry and entry.key == cache_key and (
            time.monotonic() - entry.built_at
        ) < CACHE_TTL_S:
            cached = dict(entry.document)
            cached["cached"] = True
            return cached

    document: dict[str, Any] = {
        "generated_utc": timeutil.format_utc(reference),
        "range": rng.to_dict(),
        "cached": False,
        "errors": [],
        "levels_note": (
            "Energy is only ever summed WITHIN a level. The hierarchy nests — a "
            "branch breaker's watts are also inside its panel's feed CT — so an "
            "unqualified total across levels double counts."
        ),
    }

    if not target:
        document["errors"].append(
            "S3_BUCKET is not configured, so there is no archive to read. "
            "The live spool views (/ui, /ui/hvac) are unaffected."
        )
        document["source"] = None
        return document

    src = _sources(target)
    document["source"] = src.to_dict()

    owns_con = con is None
    if owns_con:
        from energy_capture.stages import rollup

        # Reused rather than re-implemented: this is the one place in the
        # codebase that hands DuckDB the AWS credential chain, and a second
        # copy would drift.
        con = rollup.connect(s3=True)

    try:
        blocks = (
            ("circuits", circuits_block),
            ("meter", meter_block),
            ("hvac", hvac_block),
            ("coverage", coverage_block),
        )
        for name, builder in blocks:
            started = time.monotonic()
            try:
                document[name] = builder(con, src, rng)
                log.debug(
                    "history_block_ok",
                    block=name,
                    duration_s=round(time.monotonic() - started, 3),
                )
            except Exception as exc:  # one bad block must not blank the page
                message = f"{name}: {type(exc).__name__}: {exc}"
                document["errors"].append(message)
                log.warning("history_block_failed", block=name, error=message)
        document["kpi"] = _kpi(
            document.get("circuits") or {}, document.get("meter") or {}, rng
        )
    finally:
        if owns_con:
            con.close()

    if use_cache and owns_con:
        with _cache_lock:
            _cache = _CacheEntry(key=cache_key, document=document)

    log.info(
        "history_document_built",
        start=rng.start.isoformat(),
        end=rng.end.isoformat(),
        days=rng.days,
        errors=len(document["errors"]),
    )
    return document
