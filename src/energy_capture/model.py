"""The canonical row, the Arrow schemas, and the sort/dedupe keys (PLAN.md §3).

Everything downstream — spool, uploader, compactor, rollup, backfill — sorts and
dedupes with :data:`SORT_KEY` / :data:`DEDUPE_KEY` from this module. Do not
re-spell those tuples anywhere else.

Long format, one row per observation::

    ts_utc | ts_local | source | device_id | channel_id | metric | value | unit

Dataset variants are expressed by composing the canonical field list rather than
by patching schemas:

* :data:`RAW_30S_SCHEMA` / :data:`DAILY_SCHEMA` — the canonical 8 columns.
* :data:`METER_SCHEMA` — the canonical columns **plus** ``interval_s`` (int32)
  for the future LG&E Green Button dataset, where ``ts_utc`` is the interval
  *start* (PLAN.md §13). Its row type is :class:`MeterObservation`.
* :data:`HOURLY_SCHEMA` — the derived rollup (PLAN.md §10), a different grain
  entirely: one row per (hour, source, device, channel, metric).

Cardinal rules enforced here:

* Gaps stay gaps: an :class:`Observation` always carries a real number. A null
  API field means *emit no row* — never construct one with ``value=0``.
* Day-grain metrics (``kwh_day``, ``cost_day_usd``) may never enter ``raw_30s``;
  :func:`observations_to_table` rejects them for that dataset (CLAUDE.md rule 6).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, fields as dataclass_fields
from datetime import datetime
from enum import StrEnum

import pyarrow as pa

from energy_capture import timeutil

__all__ = [
    "CANONICAL_COLUMNS",
    "DAILY_SCHEMA",
    "DAY_GRAIN_METRICS",
    "DEDUPE_KEY",
    "METER_DEDUPE_KEY",
    "DIM_KEY",
    "ENUM_METRICS",
    "HOURLY_DEDUPE_KEY",
    "HOURLY_SCHEMA",
    "HOURLY_SORT_KEY",
    "METER_SCHEMA",
    "METRICS",
    "RAW_30S_SCHEMA",
    "SORT_KEY",
    "SOURCES",
    "SOURCE_BRYANT",
    "SOURCE_LEVITON",
    "SOURCE_LGE",
    "UNITS",
    "UNIT_FOR_METRIC",
    "Dataset",
    "MeterObservation",
    "Observation",
    "dedupe_key_for",
    "dedupe_observations",
    "dedupe_table",
    "empty_table",
    "is_day_grain",
    "make_observation",
    "observations_to_table",
    "row_type_for",
    "schema_for",
    "sort_observations",
    "sort_table",
    "table_to_observations",
    "unit_for_metric",
]

# --------------------------------------------------------------------- keys

#: Row order inside every Parquet file (PLAN.md §3). ``ts_utc`` first: it is the
#: canonical instant, and query engines get a monotonic time column for free.
SORT_KEY: tuple[str, ...] = ("ts_utc", "source", "device_id", "channel_id")

#: Row identity everywhere — spool, parts, day files, backfill (CLAUDE.md rule 7).
DEDUPE_KEY: tuple[str, ...] = ("ts_utc", "source", "device_id", "channel_id", "metric")

#: The ``meter`` dataset's identity: :data:`DEDUPE_KEY` **plus ``interval_s``**.
#:
#: Meter data is *interval* data, and a custodian may publish the same energy at
#: more than one resolution. LG&E does: measured 2026-08-18, every UsagePoint
#: carries both a 15-minute and an hourly series, colliding at 167 timestamps in
#: four days. Under the canonical key those are "duplicates" and one silently
#: replaces the other — so an hour boundary randomly holds either 15 minutes of
#: energy or a whole hour of it, and any SUM over the result is wrong.
#:
#: They are not duplicates. A reading covering 900 seconds and one covering 3600
#: seconds are different observations that happen to start together, so the
#: duration is part of what identifies them. Choosing *between* the series is a
#: query-time decision (``compare-meter`` takes the finest), not a reason to
#: throw one away at ingest — CLAUDE.md rule 2.
METER_DEDUPE_KEY: tuple[str, ...] = DEDUPE_KEY + ("interval_s",)

#: Identity of a channel in ``channel_map.json`` / ``dim_channel`` (PLAN.md §9).
DIM_KEY: tuple[str, ...] = ("source", "device_id", "channel_id")

#: Hourly rollup grain. Bucketing is on ``hour_start_utc`` so the two 01:00 local
#: hours of a DST fall-back day stay distinct (25 buckets, PLAN.md §15.3).
HOURLY_SORT_KEY: tuple[str, ...] = (
    "hour_start_utc",
    "source",
    "device_id",
    "channel_id",
)
HOURLY_DEDUPE_KEY: tuple[str, ...] = HOURLY_SORT_KEY + ("metric",)

# -------------------------------------------------------------- vocabularies

SOURCE_LEVITON = "leviton"
SOURCE_BRYANT = "bryant"
SOURCE_LGE = "lge"

#: Valid ``source`` values (PLAN.md §3; ``lge`` is designed-for, not yet built).
SOURCES: frozenset[str] = frozenset({SOURCE_LEVITON, SOURCE_BRYANT, SOURCE_LGE})

UNIT_WATTS = "W"
UNIT_AMPS = "A"
UNIT_VOLTS = "V"
UNIT_HERTZ = "Hz"
UNIT_DEGF = "degF"
UNIT_KWH = "kWh"
UNIT_USD = "USD"
UNIT_PCT = "pct"
UNIT_ENUM = "enum"
UNIT_CCF = "CCF"
UNIT_RPM = "rpm"
UNIT_CFM = "CFM"

#: Canonical unit vocabulary (PLAN.md §3, plus §13's CCF and §7.3's native units).
UNITS: frozenset[str] = frozenset(
    {
        UNIT_WATTS,
        UNIT_AMPS,
        UNIT_VOLTS,
        UNIT_HERTZ,
        UNIT_DEGF,
        UNIT_KWH,
        UNIT_USD,
        UNIT_PCT,
        UNIT_ENUM,
        UNIT_CCF,
        UNIT_RPM,
        UNIT_CFM,
    }
)

#: metric -> unit (PLAN.md §3). Adding a metric means adding it here; nothing may
#: invent a unit at the call site.
UNIT_FOR_METRIC: dict[str, str] = {
    # Leviton electrical (30s)
    "watts": UNIT_WATTS,
    "amps": UNIT_AMPS,
    "volts": UNIT_VOLTS,
    "hz": UNIT_HERTZ,
    # Bryant status (30s)
    "indoor_temp_f": UNIT_DEGF,
    "outdoor_temp_f": UNIT_DEGF,
    "setpoint_cool_f": UNIT_DEGF,
    "setpoint_heat_f": UNIT_DEGF,
    "humidity_pct": UNIT_PCT,
    # `stage` and `stage_pct` are the two mutually-exclusive renderings of the
    # SAME field, the outdoor unit's `odu.opstat` (PLAN.md §7.3, DEVIATIONS.md
    # #59). A single-/two-stage compressor reports a word ("off"/"low"/"high")
    # and becomes `stage`, an enum code; a variable-capacity compressor reports a
    # 0-100 capacity percentage and becomes `stage_pct`. One cycle produces at
    # most one of them, and the metric NAME is how a reader knows which
    # representation a row used — never mix them in one comparison.
    "stage": UNIT_ENUM,
    "stage_pct": UNIT_PCT,
    "mode": UNIT_ENUM,
    "fan": UNIT_ENUM,
    "blower_rpm": UNIT_RPM,
    "cfm": UNIT_CFM,
    # Bryant daily energy (day grain)
    "kwh_day": UNIT_KWH,
    "cost_day_usd": UNIT_USD,
    # LG&E Green Button meter intervals (future, PLAN.md §13)
    "kwh_interval": UNIT_KWH,
    "ccf_interval": UNIT_CCF,
}

#: Known metric names.
METRICS: frozenset[str] = frozenset(UNIT_FOR_METRIC)

#: Metrics whose ``value`` is a small integer code with ``unit='enum'``; the
#: mapping tables live in ``sources/bryant.py`` and are append-only (PLAN.md §7.3).
#:
#: ``stage_pct`` is deliberately **not** here: it is an ordinary percentage
#: measurement, not a code, even though it renders the same API field as
#: ``stage``. Averaging it is meaningful; averaging an enum code is not.
ENUM_METRICS: frozenset[str] = frozenset({"mode", "stage", "fan"})

#: Day-grain metrics. These live only in ``energy/daily``: they must never be
#: written to ``raw_30s`` and are excluded from rollup input (CLAUDE.md rule 6).
DAY_GRAIN_METRICS: frozenset[str] = frozenset({"kwh_day", "cost_day_usd"})

#: Metrics the hourly rollup computes ``kwh`` for (PLAN.md §2.5).
POWER_METRIC = "watts"


def unit_for_metric(metric: str) -> str:
    """Canonical unit for ``metric``; raises for an unknown metric."""
    try:
        return UNIT_FOR_METRIC[metric]
    except KeyError:
        raise ValueError(
            f"unknown metric {metric!r}; add it to UNIT_FOR_METRIC in model.py"
        ) from None


def is_day_grain(metric: str) -> bool:
    """True for ``kwh_day`` / ``cost_day_usd`` — the ``energy/daily`` metrics."""
    return metric in DAY_GRAIN_METRICS


# ------------------------------------------------------------------- rows


@dataclass(frozen=True, slots=True)
class Observation:
    """One observation: the canonical row of PLAN.md §3.

    ``ts_utc`` must be timezone-aware UTC; ``ts_local`` must be the naive local
    wall clock for the same instant. Build with :func:`make_observation` so the
    two can never disagree.
    """

    ts_utc: datetime
    ts_local: datetime
    source: str
    device_id: str
    channel_id: str
    metric: str
    value: float
    unit: str


@dataclass(frozen=True, slots=True)
class MeterObservation(Observation):
    """Interval-metered row: adds the duration the value covers (PLAN.md §13).

    ``ts_utc`` is the interval **start**; ``interval_s`` is its length in seconds.
    """

    interval_s: int = 0


# ------------------------------------------------------------------ schemas


class Dataset(StrEnum):
    """The S3 datasets of PLAN.md §4 that this module has a schema for."""

    RAW_30S = "raw_30s"
    DAILY = "daily"
    HOURLY = "hourly"
    METER = "meter"


def _canonical_fields() -> list[pa.Field]:
    return [
        pa.field("ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ts_local", pa.timestamp("us"), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("channel_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
    ]


def observation_schema(*extra: pa.Field) -> pa.Schema:
    """The canonical 8-column schema, optionally extended with ``extra`` fields.

    This is how dataset variants are expressed: ``METER_SCHEMA`` is
    ``observation_schema(pa.field("interval_s", pa.int32()))``. No dataset ever
    reorders or retypes the canonical columns.
    """
    return pa.schema(_canonical_fields() + list(extra))


#: Column order of the canonical row.
CANONICAL_COLUMNS: tuple[str, ...] = tuple(f.name for f in _canonical_fields())

#: 30s observations: Leviton + Bryant status (``energy/raw_30s``).
RAW_30S_SCHEMA: pa.Schema = observation_schema()

#: Bryant daily energy, day grain, ``ts_utc`` = local midnight (``energy/daily``).
DAILY_SCHEMA: pa.Schema = observation_schema()

#: LG&E Green Button intervals (``energy/meter``), ``ts_utc`` = interval start.
METER_SCHEMA: pa.Schema = observation_schema(
    pa.field("interval_s", pa.int32(), nullable=False)
)

#: Derived hourly rollup (``energy/hourly``) — fully regenerable (PLAN.md §10).
#:
#: ``hour_start_utc`` is the bucket key: local hours are grouped by UTC so the
#: repeated 01:00 hour of a fall-back day is two rows, not one merged row.
#: ``local_hour_start`` is its naive local rendering (ambiguous on that day, by
#: design). ``kwh`` is populated only for ``metric='watts'`` and is
#: observed-time-only: ``mean * sample_count * poll_interval_s / 3.6e6``.
HOURLY_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("hour_start_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("local_hour_start", pa.timestamp("us"), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("channel_id", pa.string(), nullable=False),
        pa.field("metric", pa.string(), nullable=False),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("mean", pa.float64(), nullable=False),
        pa.field("min", pa.float64(), nullable=False),
        pa.field("max", pa.float64(), nullable=False),
        pa.field("p95", pa.float64(), nullable=False),
        pa.field("sample_count", pa.int64(), nullable=False),
        pa.field("first_ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        # NULL for every metric except `watts` — never 0, which would read as
        # "no energy used" instead of "not applicable".
        pa.field("kwh", pa.float64(), nullable=True),
    ]
)

_SCHEMAS: dict[Dataset, pa.Schema] = {
    Dataset.RAW_30S: RAW_30S_SCHEMA,
    Dataset.DAILY: DAILY_SCHEMA,
    Dataset.HOURLY: HOURLY_SCHEMA,
    Dataset.METER: METER_SCHEMA,
}

_ROW_TYPES: dict[Dataset, type[Observation]] = {
    Dataset.RAW_30S: Observation,
    Dataset.DAILY: Observation,
    Dataset.METER: MeterObservation,
}


def schema_for(dataset: Dataset | str) -> pa.Schema:
    """Arrow schema of a dataset (``"raw_30s"``, ``"daily"``, ``"hourly"``, ``"meter"``)."""
    return _SCHEMAS[Dataset(dataset)]


def row_type_for(dataset: Dataset | str) -> type[Observation]:
    """Dataclass used for rows of an observation dataset (not ``hourly``)."""
    try:
        return _ROW_TYPES[Dataset(dataset)]
    except KeyError:
        raise ValueError(f"{dataset} is not an observation dataset") from None


def empty_table(dataset: Dataset | str = Dataset.RAW_30S) -> pa.Table:
    """A zero-row table with the dataset's schema (a gap is still a real answer)."""
    return schema_for(dataset).empty_table()


# ---------------------------------------------------------------- builders


def make_observation(
    *,
    ts_utc: datetime,
    source: str,
    device_id: str,
    channel_id: str,
    metric: str,
    value: float,
    unit: str | None = None,
    interval_s: int | None = None,
) -> Observation:
    """Build an :class:`Observation`, deriving ``ts_local`` and ``unit``.

    The sanctioned constructor: it guarantees ``ts_local`` is the local wall
    clock of ``ts_utc`` (via :mod:`energy_capture.timeutil`, the only place that
    conversion happens) and that ``unit`` matches the metric vocabulary.

    ``value`` must be a real finite number. Passing ``None`` or NaN raises —
    a missing sample is a *missing row*, never a fabricated one (CLAUDE.md rule 1).
    Pass ``interval_s`` to get a :class:`MeterObservation`.
    """
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {sorted(SOURCES)}")
    if value is None:
        raise ValueError(
            f"value is None for {source}/{device_id}/{channel_id}/{metric}: "
            "emit no row for a missing sample (gaps stay gaps)"
        )
    numeric = float(value)
    if math.isnan(numeric) or math.isinf(numeric):
        raise ValueError(
            f"non-finite value {value!r} for {source}/{device_id}/{channel_id}/{metric}"
        )
    resolved_unit = unit if unit is not None else unit_for_metric(metric)
    if resolved_unit not in UNITS:
        raise ValueError(f"unknown unit {resolved_unit!r}; expected one of {sorted(UNITS)}")

    instant = timeutil.ensure_utc(ts_utc)
    ts_local = timeutil.to_local_naive(instant)
    common = dict(
        ts_utc=instant,
        ts_local=ts_local,
        source=source,
        device_id=device_id,
        channel_id=channel_id,
        metric=metric,
        value=numeric,
        unit=resolved_unit,
    )
    if interval_s is None:
        return Observation(**common)
    if int(interval_s) <= 0:
        raise ValueError(f"interval_s must be positive, got {interval_s!r}")
    return MeterObservation(**common, interval_s=int(interval_s))


def _as_row(obs: Observation, columns: Sequence[str]) -> dict[str, object]:
    row: dict[str, object] = {}
    for name in columns:
        value = getattr(obs, name)
        if name == "ts_utc":
            value = timeutil.ensure_utc(value)
        elif name == "ts_local" and isinstance(value, datetime) and value.tzinfo is not None:
            raise ValueError("ts_local must be a naive local wall clock, not tz-aware")
        row[name] = value
    return row


def sort_observations(
    observations: Iterable[Observation], keys: Sequence[str] = SORT_KEY
) -> list[Observation]:
    """Stable sort by ``keys`` (default :data:`SORT_KEY`)."""
    return sorted(observations, key=lambda o: tuple(getattr(o, k) for k in keys))


def dedupe_observations(
    observations: Iterable[Observation], keys: Sequence[str] = DEDUPE_KEY
) -> list[Observation]:
    """Drop later rows sharing a :data:`DEDUPE_KEY` — **first occurrence wins**.

    Callers express precedence through input order: backfill puts DynamoDB rows
    before the legacy JSON rows so DynamoDB wins on overlap (PLAN.md §8).
    """
    seen: set[tuple[object, ...]] = set()
    out: list[Observation] = []
    for obs in observations:
        key = tuple(getattr(obs, k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(obs)
    return out


def dedupe_key_for(dataset: Dataset | str) -> tuple[str, ...]:
    """The identity columns for a dataset — ``meter`` adds ``interval_s``."""
    return METER_DEDUPE_KEY if Dataset(dataset) is Dataset.METER else DEDUPE_KEY


def dedupe_table(table: pa.Table, keys: Sequence[str] = DEDUPE_KEY) -> pa.Table:
    """Drop later rows sharing ``keys`` — first occurrence in table order wins."""
    if table.num_rows == 0:
        return table
    columns = [table.column(k).to_pylist() for k in keys]
    seen: set[tuple[object, ...]] = set()
    keep: list[int] = []
    for index, key in enumerate(zip(*columns, strict=True)):
        if key in seen:
            continue
        seen.add(key)
        keep.append(index)
    if len(keep) == table.num_rows:
        return table
    return table.take(pa.array(keep, type=pa.int64()))


def sort_table(table: pa.Table, keys: Sequence[str] = SORT_KEY) -> pa.Table:
    """Sort by ``keys``, breaking ties by original row order.

    The tiebreak makes output byte-identical across re-runs of the same input,
    which is what "idempotent" means for the compactor and backfill (§15.5).
    """
    if table.num_rows <= 1:
        return table
    ordinal = pa.array(range(table.num_rows), type=pa.int64())
    with_ordinal = table.append_column("__row_ordinal", ordinal)
    sort_spec = [(key, "ascending") for key in keys] + [("__row_ordinal", "ascending")]
    return with_ordinal.sort_by(sort_spec).drop_columns(["__row_ordinal"])


def observations_to_table(
    observations: Iterable[Observation],
    *,
    dataset: Dataset | str = Dataset.RAW_30S,
    schema: pa.Schema | None = None,
    sort: bool = True,
    dedupe: bool = True,
    validate: bool = True,
) -> pa.Table:
    """Convert observations to an Arrow table: deduped, then sorted.

    Dedupe runs before the sort so that input order decides precedence, and the
    sort is then deterministic — the exact contract the uploader, compactor and
    backfill rely on for idempotent re-runs.

    ``validate`` enforces that day-grain metrics never enter ``raw_30s``
    (CLAUDE.md rule 6) and that ``energy/daily`` holds only day-grain metrics.
    """
    resolved = Dataset(dataset)
    target_schema = schema if schema is not None else schema_for(resolved)
    rows = list(observations)

    if validate:
        _validate_dataset_metrics(rows, resolved)
    if dedupe:
        rows = dedupe_observations(rows, dedupe_key_for(resolved))
    if sort:
        rows = sort_observations(rows)
    if not rows:
        return target_schema.empty_table()

    columns = [field.name for field in target_schema]
    return pa.Table.from_pylist([_as_row(o, columns) for o in rows], schema=target_schema)


def _validate_dataset_metrics(rows: Sequence[Observation], dataset: Dataset) -> None:
    if dataset is Dataset.RAW_30S:
        offenders = sorted({o.metric for o in rows if is_day_grain(o.metric)})
        if offenders:
            raise ValueError(
                f"day-grain metrics {offenders} may not be written to raw_30s; "
                "they belong in energy/daily (they would poison hourly rollups)"
            )
    elif dataset is Dataset.DAILY:
        offenders = sorted({o.metric for o in rows if not is_day_grain(o.metric)})
        if offenders:
            raise ValueError(
                f"non-day-grain metrics {offenders} may not be written to energy/daily"
            )


def table_to_observations(
    table: pa.Table, *, dataset: Dataset | str | None = None
) -> list[Observation]:
    """Convert an observation table back to dataclass rows.

    Returns :class:`MeterObservation` when the table carries ``interval_s``
    (or when ``dataset='meter'``).
    """
    if dataset is not None:
        row_type = row_type_for(dataset)
    else:
        row_type = MeterObservation if "interval_s" in table.column_names else Observation
    names = {f.name for f in dataclass_fields(row_type)}
    out: list[Observation] = []
    for row in table.to_pylist():
        kwargs = {k: v for k, v in row.items() if k in names}
        kwargs["ts_utc"] = timeutil.ensure_utc(kwargs["ts_utc"])
        if row_type is MeterObservation:
            kwargs["interval_s"] = int(kwargs.get("interval_s") or 0)
        out.append(row_type(**kwargs))
    return out
