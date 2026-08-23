"""Glue Data Catalog tables, and their comments (PLAN.md §12).

``energycap create-glue-tables`` creates or updates four external tables in the
``GLUE_DATABASE`` database (default ``energy``) so Athena — and an LLM reading
the catalog over MCP — can query what the pipeline lands in S3:

===================  ====================================  ==================
table                dataset (PLAN.md §4)                  partitioned by
===================  ====================================  ==================
``energy_raw_30s``   ``energy/raw_30s/``                   year / month / day
``energy_hourly``    ``energy/hourly/``                    year / month
``energy_daily``     ``energy/daily/``                     year
``dim_channel``      ``energy/dim_channel/``               (single file)
===================  ====================================  ==================

boto3 only: no crawler, no CloudFormation. A crawler would *infer* the schema
and would never write a comment, and the comments are the point (see below).

Four properties this module is built around.

**1. Idempotent create-or-update.** Every table definition is a pure function of
:func:`table_specs`; the run reads what is in the catalog, creates what is
missing, updates what differs, and leaves an already-correct table completely
untouched (no pointless table version). Running it twice therefore leaves byte
-identical definitions and issues zero writes the second time.

**2. Locations are derived, never re-typed.** Both the table ``Location`` and the
``storage.location.template`` come from the key builders in
:mod:`energy_capture.aws.s3io` — the same functions the uploader, compactor and
rollup write through. If a prefix ever changes there, the Glue tables follow
automatically instead of silently pointing at an empty prefix.

**3. Column types are derived from the writers' Arrow schemas.**
:func:`arrow_to_glue_type` maps :mod:`energy_capture.model`'s schemas — the exact
schemas ``pyarrow`` writes to Parquet — into Hive types. A Glue schema that
disagrees with the data is not a loud error; it is a silent Athena failure (a
mistyped column reads back as NULL), so ``tests/test_glue.py`` writes a real
Parquet file and checks the mapping against the file's own schema.

**4. Comments are a first-class deliverable, not decoration.** They are what an
LLM reads to orient itself before writing a query, and the three things it most
needs to be told are things no schema can express:

* partitioning is on the **LOCAL** date, so ``year/month/day`` are not UTC;
* the dedupe key is ``(ts_utc, source, device_id, channel_id, metric)``, so no
  query over a *settled* partition needs ``DISTINCT`` — the one qualification
  being a day that is mid-compaction, which ``_RAW_30S_DESCRIPTION`` and the
  README both spell out identically;
* **a gap means the collector was down, never that the load was off.** A low
  ``sample_count`` or an absent hour must never be read as zero consumption.

One vocabulary fact needs prose rather than a list, and it is
:data:`STAGE_REPRESENTATION_NOTE`: ``stage`` and ``stage_pct`` are two mutually
exclusive renderings of ONE field (the outdoor unit's ``odu.opstat``), chosen by
the hardware. A staged compressor reports a word and lands as ``stage``, an enum
code; a variable-capacity one reports a 0-100 capacity percentage and lands as
``stage_pct``, a real measurement. The live system is the second kind
(``odu.type = gs3ngiphp``, ``opstat`` observed as ``"35"``), so it emits
``stage_pct`` and no ``stage`` row will ever exist here. A reader who filters on
``stage`` therefore gets an empty result — and an empty result is absence, not
zero (CLAUDE.md rule 1). The note is carried by the database description and by
every table comment either metric can reach, because the trap is invisible from
the metric list alone — both names are on it, and one of them never lands.

The enum decode for ``mode``/``stage``/``fan`` is quoted straight out of
:func:`energy_capture.sources.bryant.enum_decode_text`, so the comment cannot
drift from the append-only mapping tables it documents; a test asserts the two
agree integer for integer. It appears wherever enum rows can be met: on the
``value`` column of the tables that have one, and in the ``energy_hourly`` table
comment, which has no ``value`` column but *does* aggregate enum rows (the
rollup excludes only the day-grain metrics), together with the warning that
``mean``/``p95`` over an integer code are meaningless.

Two kinds of drift this module refuses to allow, because both produce a comment
that is *wrong* rather than merely thin — and a wrong comment is worse than a
missing one, since an LLM will act on it:

* **Per-table facts are generated per table.** ``ts_utc``/``ts_local`` name the
  partition columns the table actually has (``energy_daily`` is partitioned by
  year ONLY, so telling anyone to prune on ``month``/``day`` there produces a
  ``COLUMN_NOT_FOUND``), and each partition column comment names the column its
  values are derived from — ``ts_local`` where there is one, ``local_hour_start``
  on ``energy_hourly``, which has no ``ts_local`` at all.
* **The metric and unit vocabularies are generated from**
  :mod:`energy_capture.model`. A reader treats those lists as closed and writes
  ``WHERE metric IN (…)`` from them, so a list that has fallen behind
  ``model.UNIT_FOR_METRIC`` silently drops real rows. ``_METRIC_GROUPS`` only
  supplies the grouping prose, and importing this module fails if it does not
  cover ``model.METRICS`` exactly.

**Adding PLAN.md §13's ``energy_meter`` table** is deliberately one entry, not a
restructuring: build a :class:`TableSpec` with ``schema=model.METER_SCHEMA``,
``prefix_builder=s3io.meter_year_prefix`` and ``partition_keys=("year",)`` and
add it to :func:`table_specs`. The projection parameters, the location template,
the type mapping and the ``interval_s`` column comment are already generic.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Final

import pyarrow as pa
from botocore.exceptions import ClientError

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.sources import bryant
from energy_capture.stages.daily import COMPONENTS as DAILY_COMPONENTS
from energy_capture.stages.dim import DIM_SCHEMA

__all__ = [
    "CANONICAL_COLUMN_COMMENTS",
    "DATABASE_DESCRIPTION",
    "DIM_CHANNEL_SCHEMA",
    "ENUM_ROLLUP_WARNING",
    "GLUE_COMMENT_MAX_LEN",
    "GLUE_DESCRIPTION_MAX_LEN",
    "HOURLY_GAP_WARNING",
    "ODU_TYPE_OBSERVED",
    "PARQUET_INPUT_FORMAT",
    "PARQUET_OUTPUT_FORMAT",
    "PARQUET_SERDE",
    "PARTITION_COLUMN_TYPE",
    "PARTITION_KEY_NAMES",
    "PROJECTION_DAY_RANGE",
    "PROJECTION_MONTH_RANGE",
    "PROJECTION_YEAR_RANGE",
    "STAGE_MEAN_NOTE",
    "STAGE_REPRESENTATION_NOTE",
    "TABLE_DIM_CHANNEL",
    "TABLE_ENERGY_DAILY",
    "TABLE_ENERGY_HOURLY",
    "TABLE_ENERGY_RAW_30S",
    "TableSpec",
    "arrow_to_glue_type",
    "create_or_update_tables",
    "partition_projection_parameters",
    "table_input",
    "table_inputs",
    "table_specs",
]

log = get_logger("create_glue_tables")


# ============================================================================
# Catalog constants
# ============================================================================

TABLE_ENERGY_RAW_30S: Final[str] = "energy_raw_30s"
TABLE_ENERGY_HOURLY: Final[str] = "energy_hourly"
TABLE_ENERGY_DAILY: Final[str] = "energy_daily"
TABLE_DIM_CHANNEL: Final[str] = "dim_channel"

#: Hive/Athena plumbing for Parquet. Spelled out rather than crawler-inferred.
PARQUET_INPUT_FORMAT: Final[str] = (
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
)
PARQUET_OUTPUT_FORMAT: Final[str] = (
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
)
PARQUET_SERDE: Final[str] = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

#: AWS Glue API limits. Exceeding either is a hard 400 from the service, so they
#: are enforced here (and in the tests) rather than discovered in production.
GLUE_COMMENT_MAX_LEN: Final[int] = 255
GLUE_DESCRIPTION_MAX_LEN: Final[int] = 2048

#: Partition projection ranges (PLAN.md §12). The upper year bound is a ceiling,
#: not a promise: projection only tells Athena which paths *may* exist, and a
#: projected partition with no objects behind it simply returns no rows.
PROJECTION_YEAR_RANGE: Final[str] = "2024,2035"
PROJECTION_MONTH_RANGE: Final[str] = "1,12"
PROJECTION_DAY_RANGE: Final[str] = "1,31"

#: ``month``/``day`` paths are zero-padded (``month=08``), hence ``digits=2``.
#: ``year`` is naturally four digits, so it needs none.
PROJECTION_MONTH_DIGITS: Final[str] = "2"
PROJECTION_DAY_DIGITS: Final[str] = "2"

#: Partition columns are integers, not strings: the projection ``digits`` setting
#: pads the *path*, but the column value stays numeric, so ``WHERE month = 8``
#: is correct and ``WHERE month = '08'`` is not.
PARTITION_COLUMN_TYPE: Final[str] = "int"

#: The Hive partition key names this layout uses, outermost first.
PARTITION_KEY_NAMES: Final[tuple[str, ...]] = ("year", "month", "day")

_PROJECTION_BY_KEY: Final[Mapping[str, Mapping[str, str]]] = {
    "year": {"type": "integer", "range": PROJECTION_YEAR_RANGE},
    "month": {
        "type": "integer",
        "range": PROJECTION_MONTH_RANGE,
        "digits": PROJECTION_MONTH_DIGITS,
    },
    "day": {
        "type": "integer",
        "range": PROJECTION_DAY_RANGE,
        "digits": PROJECTION_DAY_DIGITS,
    },
}

#: A date whose partition segments are all distinct and all zero-padded, used to
#: probe an ``s3io`` prefix builder for its layout. Never written anywhere.
_TEMPLATE_PROBE_DATE: Final[date] = date(2024, 1, 2)

#: Table parameters AWS or a crawler may add on its own. Ignored when deciding
#: whether a table needs updating, so a foreign key never causes a rewrite loop.
_UNMANAGED_TABLE_PARAMETERS: Final[frozenset[str]] = frozenset(
    {
        "transient_lastDdlTime",
        "last_modified_by",
        "last_modified_time",
        "UPDATED_BY_CRAWLER",
        "CrawlerSchemaDeserializerVersion",
        "CrawlerSchemaSerializerVersion",
        "averageRecordSize",
        "objectCount",
        "recordCount",
        "sizeKey",
        "typeOfData",
    }
)

_ENTITY_NOT_FOUND: Final[str] = "EntityNotFoundException"
_ALREADY_EXISTS: Final[str] = "AlreadyExistsException"


# ============================================================================
# dim_channel's Arrow schema (PLAN.md §9)
# ============================================================================
#
# Every other table's columns come from `model.py`, which owns the schema its
# writers actually produce. `dim_channel` follows the same rule, one module over:
# `stages/dim.py` is the writer, so `DIM_SCHEMA` is the truth and this is an
# alias, not a second declaration. Two independent spellings of the same thing
# is how a table ends up declaring `label` NOT NULL while the writer emits nulls
# (or worse, a type Athena reads back as all-NULL). What the file actually
# contains is verified end to end in tests/test_glue.py, which writes a real
# Parquet object through `dim.build_table` and reads its schema back.
#
# Two types here are load-bearing and easy to get wrong:
#   * `slots` is a STRING ("1,3"), not a list — PLAN.md §9 says so explicitly.
#   * `priority` is a STRING ("critical"), not a number — that is what the
#     blackstart inventory holds.
# Only the (source, device_id, channel_id) identity plus the label are
# non-nullable; a channel with no inventory entry still gets a row, with nulls
# (never invented values). Hive has no NOT NULL, so nullability never reaches
# Glue — it is the writer's contract, which is exactly why it lives with it.
DIM_CHANNEL_SCHEMA: Final[pa.Schema] = DIM_SCHEMA


# ============================================================================
# Type mapping: what the writers produce -> what Athena must be told
# ============================================================================

_ARROW_TO_GLUE: Final[tuple[tuple[Callable[[pa.DataType], bool], str], ...]] = (
    (pa.types.is_timestamp, "timestamp"),
    (pa.types.is_date, "date"),
    (pa.types.is_boolean, "boolean"),
    (pa.types.is_int64, "bigint"),
    (pa.types.is_int32, "int"),
    (pa.types.is_int16, "smallint"),
    (pa.types.is_int8, "tinyint"),
    (pa.types.is_float64, "double"),
    (pa.types.is_float32, "float"),
    (pa.types.is_string, "string"),
    (pa.types.is_large_string, "string"),
    (pa.types.is_binary, "binary"),
)


def arrow_to_glue_type(arrow_type: pa.DataType) -> str:
    """Hive/Athena type name for an Arrow type.

    Deliberately total-or-loud: an unmapped Arrow type raises instead of
    defaulting to ``string``. A wrong Glue type does not fail a query — Athena
    returns NULLs or garbage for that column — so the failure has to happen
    here, at ``create-glue-tables`` time, where somebody is watching.

    Note that a timezone-aware and a naive Arrow timestamp both become Hive
    ``timestamp``: Hive has no zone-aware type. That is exactly why ``ts_utc``
    and ``ts_local`` carry the comments they do — the catalog cannot tell them
    apart, so the prose must.
    """
    for predicate, glue_type in _ARROW_TO_GLUE:
        if predicate(arrow_type):
            return glue_type
    raise ValueError(
        f"no Glue type mapping for Arrow type {arrow_type!r}; add one to "
        "energy_capture.aws.glue._ARROW_TO_GLUE rather than guessing at the "
        "call site — a wrong Glue type is a silent Athena failure"
    )


# ============================================================================
# The comments
# ============================================================================


def _fit(text: str, limit: int, what: str) -> str:
    """Return ``text``, or raise if it would be rejected by the Glue API.

    Truncating silently would be worse than failing: a comment cut off mid
    -sentence is how "do NOT read absence as the load being off" becomes "do".
    """
    collapsed = " ".join(text.split())
    if len(collapsed) > limit:
        raise ValueError(
            f"{what} is {len(collapsed)} characters; the Glue API allows {limit}. "
            "Shorten the prose — do not drop the warning or the decode."
        )
    return collapsed


_TZ = timeutil.tz_name()

#: The Bryant daily-energy ``channel_id`` vocabulary, taken from the stage that
#: writes those rows so the comment cannot drift from the mapping.
_DAILY_CHANNELS = ", ".join(spec.channel_id for spec in DAILY_COMPONENTS)

#: The enum decode, quoted from the append-only tables in ``sources/bryant.py``.
#: Built, never typed: a renumber or an appended value shows up here on the next
#: ``create-glue-tables`` run, and ``tests/test_glue.py`` pins the agreement.
#: Built from every table in ``bryant.ENUM_TABLES`` rather than a typed list of
#: three, because a typed list silently omits a new enum metric — which is
#: exactly what happened when ``op_status``/``odu_mode``/``idu_status``
#: were added on 2026-08-22 and shipped with no published decode at all.
_ENUM_DECODE = "; ".join(
    f"{metric} {bryant.enum_decode_text(metric).replace(', ', ',')}"
    for metric in model.UNIT_FOR_METRIC
    if metric in bryant.ENUM_TABLES
)

#: The decode no longer fits in a 255-character column comment (six enum metrics,
#: ~350 characters), so it lives in the table DESCRIPTION, whose budget is 2048,
#: and the columns point at it. That is the only way to obey both the Glue limit
#: and the rule that a published decode may never drop an entry: move it to the
#: field that can hold it, never truncate it.
_DECODE_POINTER: Final[str] = "full enum decode in dim_channel's table description"

#: The enum metrics, named from ``model.ENUM_METRICS`` rather than typed, so a
#: metric that stops (or starts) being an enum cannot leave the warnings below
#: naming the wrong set. ``stage_pct`` is deliberately not one of them: it renders
#: the same API field as ``stage`` and is a genuine percentage, so its mean is
#: meaningful where an enum code's is not.
_ENUM_METRIC_NAMES: Final[str] = "/".join(
    metric for metric in model.UNIT_FOR_METRIC if metric in model.ENUM_METRICS
)

#: The blunt warning that has to travel with the decode wherever enum rows are
#: *aggregated*. ``energy_hourly`` rolls them up — ``rollup.sql`` excludes only
#: ``model.DAY_GRAIN_METRICS`` — so it carries mean/min/max/p95 computed over
#: integer codes, and two of those four are arithmetic on a label.
ENUM_ROLLUP_WARNING: Final[str] = (
    f"ENUM ROWS ARE ROLLED UP HERE: {_ENUM_METRIC_NAMES} carry unit='enum', so "
    "mean/min/max/p95 aggregate integer CODES; mean and p95 over codes are "
    "MEANINGLESS (no midpoint between 'cool' and 'auto'). For those rows only "
    "min, max and sample_count carry meaning."
)

#: The outdoor unit this pipeline actually polls, read off the first live
#: ``getInfinityStatus`` response (``odu.type``). ``gs3ngiphp`` is a Greenspeed
#: variable-capacity heat pump, which is what settled DEVIATIONS.md #59/#75.1:
#: its ``opstat`` is a capacity percentage, not one of the words in the enum
#: table. Named here because "which rendering does THIS system emit" is the one
#: question the vocabulary lists cannot answer.
ODU_TYPE_OBSERVED: Final[str] = "gs3ngiphp"

#: ``odu.opstat`` — the compressor's operating state, and the HVAC signal that
#: correlates with watts — has two mutually exclusive renderings decided by the
#: hardware, and a reader who does not know that writes ``WHERE metric='stage'``
#: and concludes the compressor never ran. The metric names are quoted from
#: ``sources/bryant.py`` and the unit from ``model.UNIT_FOR_METRIC``, so a rename
#: there cannot leave this sentence describing metrics that no longer exist.
# Compressed 2026-08-22 when nine new metrics pushed both descriptions past the
# 2048-character budget. The load-bearing half is the last clause — that on THIS
# unit one of the two metrics can never appear — so that is what survived intact.
STAGE_REPRESENTATION_NOTE: Final[str] = " ".join(
    (
        f"{bryant.STAGE_METRIC}/{bryant.STAGE_PCT_METRIC} are MUTUALLY EXCLUSIVE",
        "renderings of one field (odu.opstat): staged units emit the enum,",
        "VARIABLE-CAPACITY ones the pct.",
        f"THIS unit is variable-capacity (odu.type {ODU_TYPE_OBSERVED}), so",
        f"{bryant.STAGE_METRIC} NEVER appears: absence, not zero.",
    )
)

#: The other half of the same fact, for the table that aggregates: the enum
#: warning above must not be read as covering ``stage_pct``, which is an ordinary
#: percentage and the natural join partner for mean watts.
STAGE_MEAN_NOTE: Final[str] = (
    f"{bryant.STAGE_PCT_METRIC} is no enum: its mean is real capacity, the "
    "partner for mean watts."
)


# ------------------------------------------------- metric / unit vocabularies
#
# A reader treats the metric and unit lists in a comment as a CLOSED vocabulary
# and writes `WHERE metric IN (...)` straight out of them, so a list that has
# fallen behind `model.py` silently drops real rows — which is exactly what
# happened to `blower_rpm` and `cfm`. The lists are therefore generated from
# `model.UNIT_FOR_METRIC`; all this module supplies is the grouping prose and
# which tables a group can reach, and `_check_metric_groups()` fails at import
# if `model.py` grows a metric nobody assigned to a group.


@dataclass(frozen=True, slots=True)
class _MetricGroup:
    """One "(leviton)"-style bracket in the ``metric`` comment."""

    label: str
    metrics: frozenset[str]
    #: Tables whose rows can carry these metrics. Empty = designed, not landed.
    tables: frozenset[str]


_METRIC_GROUPS: Final[tuple[_MetricGroup, ...]] = (
    _MetricGroup(
        label="leviton",
        metrics=frozenset({"watts", "amps", "volts", "hz"}),
        tables=frozenset({TABLE_ENERGY_RAW_30S, TABLE_ENERGY_HOURLY}),
    ),
    _MetricGroup(
        label="bryant status",
        metrics=frozenset(
            {
                "indoor_temp_f",
                "outdoor_temp_f",
                "setpoint_cool_f",
                "setpoint_heat_f",
                "humidity_pct",
                "mode",
                # Two mutually exclusive renderings of ONE field, odu.opstat
                # (model.UNIT_FOR_METRIC, DEVIATIONS.md #59): a staged outdoor
                # unit emits `stage` (enum) and never `stage_pct`; a
                # variable-capacity one emits `stage_pct` (0-100 percent) and
                # never `stage`. Both are named everywhere either can appear,
                # because a reader who filters on the one this system does not
                # emit gets an empty result, not a zero.
                "stage",
                "stage_pct",
                "fan",
                "blower_rpm",
                "cfm",
                # Added 2026-08-22 (see model.UNIT_FOR_METRIC). `compressor_rpm`
                # is the one worth naming to a reader: it is the compressor's own
                # speed, continuous where `stage_pct` is quantised, and it is the
                # better variable to compare against a metered watts channel.
                "compressor_rpm",
                "outdoor_coil_temp_f",
                "static_pressure",
                # THREE airflow numbers, deliberately unblended because they
                # disagree: `idu_cfm` and `idu_iducfm` are the indoor unit's, and
                # `odu_iducfm` is the outdoor unit's view of the same air. `cfm`
                # above is the older blended pick, kept for archive continuity.
                "idu_cfm",
                "idu_iducfm",
                "odu_iducfm",
                # Per-unit state, distinct from `mode` (the system's intent) and
                # from `stage` (the outdoor unit's capacity).
                "op_status",
                "odu_mode",
                "idu_status",
            }
        ),
        tables=frozenset({TABLE_ENERGY_RAW_30S, TABLE_ENERGY_HOURLY}),
    ),
    _MetricGroup(
        label="bryant daily, energy_daily only",
        metrics=frozenset(model.DAY_GRAIN_METRICS),
        tables=frozenset({TABLE_ENERGY_DAILY}),
    ),
    _MetricGroup(
        # PLAN.md §13: designed, no dataset and no table yet, so these are named
        # here purely so the coverage check below can account for them.
        label="lge meter, designed but not collected",
        metrics=frozenset({"kwh_interval", "ccf_interval"}),
        tables=frozenset(),
    ),
)


def _check_metric_groups() -> None:
    """Fail at import if the groups and ``model.METRICS`` disagree.

    Loud here, at ``create-glue-tables`` time, rather than quiet in a comment an
    LLM then trusts. A comment that lists 14 of 16 metrics does not look broken.
    """
    grouped = [metric for group in _METRIC_GROUPS for metric in sorted(group.metrics)]
    duplicated = sorted({m for m in grouped if grouped.count(m) > 1})
    missing = sorted(model.METRICS - set(grouped))
    unknown = sorted(set(grouped) - model.METRICS)
    if missing or unknown or duplicated:
        raise ValueError(
            "energy_capture.aws.glue._METRIC_GROUPS disagrees with "
            f"model.UNIT_FOR_METRIC: missing={missing} unknown={unknown} "
            f"duplicated={duplicated}. Put every metric in exactly one group — "
            "the metric/unit comments are generated from these, and a metric "
            "left out of them is a row a query silently drops."
        )


_check_metric_groups()


def _in_model_order(metrics: frozenset[str]) -> tuple[str, ...]:
    """``metrics``, ordered as ``model.UNIT_FOR_METRIC`` declares them."""
    return tuple(metric for metric in model.UNIT_FOR_METRIC if metric in metrics)


def _groups_for_table(table: str) -> tuple[_MetricGroup, ...]:
    return tuple(group for group in _METRIC_GROUPS if table in group.tables)


def _metrics_for_table(table: str) -> tuple[str, ...]:
    """Every metric that can actually appear in ``table``, in model order."""
    reachable: frozenset[str] = frozenset()
    for group in _groups_for_table(table):
        reachable |= group.metrics
    return _in_model_order(reachable)


def _metric_list_text(groups: Sequence[_MetricGroup]) -> str:
    """``"watts, amps, volts, hz (leviton); indoor_temp_f, … (bryant status)"``."""
    return "; ".join(
        f"{', '.join(_in_model_order(group.metrics))} ({group.label})" for group in groups
    )


def _unit_list_text(metrics: Sequence[str]) -> str:
    """The units those metrics use, first-appearance order, ``'enum'`` quoted."""
    rendered: list[str] = []
    for metric in metrics:
        unit = model.unit_for_metric(metric)
        text = f"'{unit}'" if unit == model.UNIT_ENUM else unit
        if text not in rendered:
            rendered.append(text)
    return ", ".join(rendered)


#: The groups a reader of the canonical (observation-grain) tables should know
#: about: everything that has landed anywhere in the catalog. The day-grain
#: group carries its own "energy_daily only" label, so naming it here tells a
#: raw_30s reader where those two metrics live instead of hiding them.
_CATALOG_GROUPS: Final[tuple[_MetricGroup, ...]] = tuple(
    group for group in _METRIC_GROUPS if group.tables
)
_CATALOG_METRICS: Final[tuple[str, ...]] = _in_model_order(
    frozenset().union(*(group.metrics for group in _CATALOG_GROUPS))
)

#: The blunt warning PLAN.md §12 requires on ``energy_hourly``, verbatim.
HOURLY_GAP_WARNING: Final[str] = (
    "sample_count < ~118 (watts@30s) means the hour has gaps; an absent row "
    "means the collector was down — do NOT read absence or low kwh as the load "
    "being off."
)

#: The partitioning fact a query author (human or LLM) has to know before
#: writing any WHERE — generated per table, because *which* partition columns
#: exist and *which* column their values come from are both per table.
def _local_partition_clause(partition_keys: tuple[str, ...], source_column: str) -> str:
    names = "/".join(partition_keys)
    which = (
        f"the {names} partitions come"
        if len(partition_keys) > 1
        else f"the {names} partition — the only one — comes"
    )
    return (
        f"PARTITIONED ON LOCAL DATE ({_TZ}), not UTC: {which} from "
        f"{source_column} — a UTC partition would cut the local day at 19:00 or "
        "20:00, per DST."
    )


#: Deliberately NOT the absolute "no query ever needs DISTINCT". The writers do
#: dedupe on this tuple, but a partition being compacted right now can hold a day
#: file and its parts at the same time (see _RAW_30S_DESCRIPTION), and the README
#: states the same qualified form — the two documents have to agree.
_DEDUPE_CLAUSE = (
    "Dedupe key: (ts_utc, source, device_id, channel_id, metric). Every writer "
    "dedupes on it: at most one row per key, so no query over a settled "
    "partition needs DISTINCT."
)
_GAP_CLAUSE = (
    "GAPS MEAN COLLECTOR DOWNTIME, NEVER ZERO LOAD. Nothing is interpolated, "
    "zero-filled or held over: a null API field emits no row, a failed poll "
    "cycle emits none at all. An absent row means 'not observed', NOT 'the load "
    "was 0' — never read one as an appliance being off, and never SUM across one "
    "as if continuous. A recorded 0.0 is different and real: Leviton fw v2 emits "
    "genuine spurious zeros, archived verbatim, unfiltered."
)
#: The same rule stated for the derived hourly grain, where a gap shows up as an
#: absent bucket or a low sample_count rather than as a dropped poll cycle. The
#: "do not read it as zero" half is HOURLY_GAP_WARNING, one paragraph up in the
#: same comment, so it is not repeated here.
_GAP_CLAUSE_HOURLY = (
    "GAPS MEAN COLLECTOR DOWNTIME, NEVER ZERO LOAD: nothing is interpolated, "
    "zero-filled or held over. A recorded 0.0 is real: Leviton fw v2 spurious "
    "zeros are archived verbatim."
)
#: The same rule for the day-grain table, where "a failed poll cycle" is not the
#: shape the gap takes and the 2048-character description budget is tighter.
_GAP_CLAUSE_DAY = (
    "GAPS MEAN COLLECTOR DOWNTIME, NEVER ZERO LOAD. Nothing here is "
    "interpolated or zero-filled: a day the collector or the Carrier cloud did "
    "not deliver is simply absent, and an absent day must never be read as zero "
    "consumption."
)


def _paragraphs(*chunks: str) -> str:
    return " ".join(" ".join(chunk.split()) for chunk in chunks)


# ---------------------------------------------------------- column comments

#: Comments for the canonical columns of PLAN.md §3, shared by every table that
#: has them. One definition, so ``ts_local``'s DST caveat reads identically in
#: ``energy_raw_30s`` and ``energy_daily``.
#:
#: The two timestamp entries are deliberately **partition-neutral**: they are the
#: fallback, and a table that declares partition keys gets the specific version
#: from :func:`_partition_scope_comments` instead. Naming ``year/month/day`` here
#: is what sent a query author to ``WHERE month = 8`` on ``energy_daily``, which
#: is partitioned by year only and answers with ``COLUMN_NOT_FOUND``.
CANONICAL_COLUMN_COMMENTS: Final[Mapping[str, str]] = {
    "ts_utc": (
        "CANONICAL INSTANT (UTC, microseconds). All sorting, hourly bucketing "
        "and dedupe use ts_utc, never ts_local — it is the only unambiguous "
        "clock across DST. Prune with this table's LOCAL-date partition columns "
        "first, then filter precisely on ts_utc."
    ),
    "ts_local": (
        f"Naive WALL CLOCK in {_TZ}: no offset attached, for humans and LLMs. "
        "Deliberately AMBIGUOUS during the DST fall-back hour, which occurs "
        "twice. Source of this table's LOCAL-date partition values. Never sort, "
        "bucket or dedupe on it."
    ),
    "source": (
        "Where the row came from: 'leviton' (LWHEM-2 load centers), 'bryant' "
        "(Carrier Infinity HVAC), 'lge' (utility meter, designed for but not yet "
        "collected). Part of the dim_channel join key."
    ),
    "device_id": (
        "The physical device: for leviton the hub id, which is the panel serial; "
        "for bryant the system serial; for lge the meter id. Part of the "
        "dim_channel join key (source, device_id, channel_id)."
    ),
    "channel_id": (
        "Channel in the device. Leviton: breaker_p{position} (2-pole = ONE "
        "channel), ct_{channel}_{a,b} per CT leg, panel_leg_{a,b} for hub "
        "volts/hz. Bryant: zone_{n}, system; daily energy uses "
        f"{_DAILY_CHANNELS}."
    ),
    "metric": (
        # The full catalog stopped fitting at 28 metrics: the raw_30s names alone
        # are 251 of the 255 characters allowed, leaving no room for a word of
        # prose. So this enumerates the metrics a reader gets WRONG — watts, the
        # mutually exclusive stage pair, and the day-grain pair that is barred
        # from this table — names the rest by family, and points at the one
        # enumeration that cannot go stale. Day-grain names are generated.
        "What is measured; enumerate with SELECT DISTINCT metric. "
        "watts/amps/volts/hz (leviton); temperatures, setpoints, humidity, mode, "
        "stage|stage_pct, fan, rpm, cfm, static_pressure (bryant status); "
        f"{', '.join(sorted(model.DAY_GRAIN_METRICS))} (energy_daily only)."
    ),
    "value": (
        # Budget note: the Glue limit is 255 characters and the decode below is
        # generated, so appending an enum value grows this string. If it ever
        # overflows, shorten THIS prose — never drop a decode entry.
        f"The measured number (see metric/unit). Enum rows ({_ENUM_METRIC_NAMES}) "
        f"carry an integer CODE, not a quantity: {_DECODE_POINTER}."
    ),
    "unit": (
        f"Unit of value: {_unit_list_text(_CATALOG_METRICS)}. Constant per "
        "metric (model.UNIT_FOR_METRIC), never invented at the call site. "
        "'enum' is a small integer code — see the value column's decode."
    ),
    "interval_s": (
        # Present for PLAN.md §13's future energy_meter table, so that dataset
        # drops in without touching this module.
        "Length in seconds of the metering interval this row covers; ts_utc is "
        "the interval START, not its midpoint or end. Only interval-metered "
        "datasets carry this column."
    ),
}

#: Where a table's partition values are derived from, best first. Every
#: partitioned table has exactly one of these columns, and its partition
#: comments must name that one — ``energy_hourly`` has no ``ts_local`` at all,
#: so a comment sourcing its year/month from ``ts_local`` names a column the
#: reader cannot find.
_PARTITION_SOURCE_COLUMNS: Final[tuple[str, ...]] = ("ts_local", "local_hour_start")


def _partition_column_comments(source_column: str) -> Mapping[str, str]:
    """Comments for the Hive partition columns of a table keyed on ``source_column``.

    These are the same three keys on every partitioned table; a table simply
    uses the prefix of the list it needs. Only the derivation differs, and it is
    passed in rather than assumed.
    """
    return {
        "year": (
            f"LOCAL calendar year ({_TZ}) taken from {source_column}, NOT from "
            f"UTC. Partition projection: integer, range {PROJECTION_YEAR_RANGE}, "
            "so no partition ever needs registering. An integer column: WHERE "
            "year = 2026."
        ),
        "month": (
            f"LOCAL calendar month from {source_column}, 1-12. The S3 path is "
            "zero-padded (month=08) by projection digits=2, but THE COLUMN IS AN "
            "INTEGER: write WHERE month = 8, not month = '08'. Projection: "
            f"integer, range {PROJECTION_MONTH_RANGE}."
        ),
        "day": (
            f"LOCAL day of month from {source_column}, 1-31. Path zero-padded "
            "(day=05) by projection digits=2, but THE COLUMN IS AN INTEGER: "
            f"WHERE day = 5. Projection: integer, range {PROJECTION_DAY_RANGE}. "
            "A local day is 23 or 25 hours long on DST days."
        ),
    }


def _partition_scope_comments(partition_keys: tuple[str, ...]) -> Mapping[str, str]:
    """``ts_utc``/``ts_local`` comments for a table with exactly these keys.

    The partition advice is per table because the partition columns are: a
    reader told to "prune with the year/month/day partitions" on
    ``energy_daily`` — which is partitioned by year ONLY — writes
    ``WHERE year = 2026 AND month = 8 AND day = 15`` and gets
    ``COLUMN_NOT_FOUND``, having been actively misled by the catalog.
    """
    if not partition_keys:
        return {}
    names = "/".join(partition_keys)
    absent = [key for key in PARTITION_KEY_NAMES if key not in partition_keys]
    if not absent:
        return {
            "ts_utc": (
                "CANONICAL INSTANT (UTC, microseconds). All sorting, hourly "
                "bucketing and dedupe use ts_utc, never ts_local — the only "
                f"unambiguous clock across DST. Prune with the {names} "
                "partitions (LOCAL date) first, then filter precisely on ts_utc."
            ),
            "ts_local": (
                f"Naive WALL CLOCK in {_TZ}: no offset attached, for humans and "
                "LLMs. Deliberately AMBIGUOUS during the DST fall-back hour, "
                f"which occurs twice. Source of the {names} partition values. "
                "Never sort, bucket or dedupe on it."
            ),
        }
    missing = " or ".join(absent)
    return {
        "ts_utc": (
            "CANONICAL INSTANT (UTC, microseconds). All sorting, bucketing and "
            f"dedupe use ts_utc, never ts_local. Partitioned on {names} ONLY "
            f"(LOCAL date): no {missing} column exists here, so a WHERE on one "
            f"is COLUMN_NOT_FOUND. Prune on {names}, then filter on ts_utc."
        ),
        "ts_local": (
            f"Naive WALL CLOCK in {_TZ}, no offset attached. AMBIGUOUS during "
            "the DST fall-back hour, which occurs twice. Source of the "
            f"{names} partition value; this table has no {missing} column. "
            "Never sort, bucket or dedupe on it."
        ),
    }

_HOURLY_COLUMN_COMMENTS: Final[Mapping[str, str]] = {
    "hour_start_utc": (
        "Start of the hour bucket as a UTC instant — the actual GROUP BY key. "
        "Bucketing on UTC keeps the two 01:00 local hours of a DST fall-back day "
        "distinct: that local day has 25 buckets per channel/metric and a "
        "spring-forward day has 23."
    ),
    "local_hour_start": (
        f"The bucket start as a naive local wall clock ({_TZ}), for readability. "
        "AMBIGUOUS on the DST fall-back day, where two different rows both read "
        "01:00. Group and join on hour_start_utc; label with this."
    ),
    # `unit` is overridden here because the canonical comment points at the
    # `value` column for the enum decode and this table has no `value` column.
    "unit": (
        "Unit of the aggregates: "
        f"{_unit_list_text(_metrics_for_table(TABLE_ENERGY_HOURLY))}. "
        f"Constant per metric. 'enum' rows ({_ENUM_METRIC_NAMES}) are CODES: "
        f"mean/p95 over them are MEANINGLESS. {_DECODE_POINTER.capitalize()}."
    ),
    "mean": (
        "Mean of the samples ACTUALLY OBSERVED this hour — no gap filling. For "
        "'watts' it is the mean power kwh comes from; for stage_pct, mean "
        f"capacity. MEANINGLESS for unit='enum' ({_ENUM_METRIC_NAMES}): "
        "averaging codes is not a state."
    ),
    "min": (
        "Smallest observed value in the hour. A 0.0 may be a genuine Leviton fw "
        "v2 spurious zero rather than an idle circuit; raw is archived "
        "verbatim, filtering is a query-time choice. For unit='enum' rows this "
        "one IS meaningful: a real observed code."
    ),
    "max": (
        "Largest observed value in the hour — the peak of the samples observed. "
        "For unit='enum' rows this one IS meaningful: the highest code seen in "
        "the hour (e.g. the top stage the outdoor unit reached), unlike "
        "mean/p95."
    ),
    "p95": (
        "95th percentile of the observed values (DuckDB quantile_cont, "
        "interpolating). Peak-ish draw without one spurious sample dominating "
        "the way max does. MEANINGLESS for unit='enum' rows — it interpolates "
        "between integer codes."
    ),
    "sample_count": (
        "Raw samples observed in this hour. THIS IS WHAT DISTINGUISHES 'the "
        "load was off' FROM 'the collector was down': a full hour of watts at "
        "30s is ~120 (~118 in practice). A low count means missed polls; "
        "mean/kwh then cover only the observed time."
    ),
    "first_ts_utc": (
        "ts_utc of the earliest sample in this bucket. Well after the hour start "
        "means the hour begins with a gap — this is how you locate the missing "
        "minutes sample_count is reporting."
    ),
    "last_ts_utc": (
        "ts_utc of the latest sample in this bucket. Well before the hour end "
        "means the hour ends with a gap (a collector restart, a cloud outage)."
    ),
    "kwh": (
        "Energy over OBSERVED TIME ONLY: mean * sample_count * poll_interval_s / "
        "3.6e6. Never extrapolated across a gap: a half-observed hour yields "
        "half the kwh at equal watts. NULL — never 0 — for every metric except "
        "watts; 0 would read as 'no energy used'."
    ),
}

_DIM_COLUMN_COMMENTS: Final[Mapping[str, str]] = {
    "label": (
        "Human/LLM-readable name of the circuit, e.g. 'HVAC subpanel feeder "
        "(leg A)'. Comes from the blackstart panel inventory when "
        "blackstart_device_id is set; an explicit label in "
        "config/channel_map.json overrides it."
    ),
    "short_label": "Compact form of label, for chart axes and narrow tables.",
    "panel": (
        "Which load center the channel belongs to, as the blackstart inventory "
        "names it (e.g. 'A', 'B'). NULL for channels that are not breakers."
    ),
    "slots": (
        "Breaker slot number(s) in the panel as a COMMA-SEPARATED STRING, e.g. "
        "'1,3' for a 2-pole breaker — a string, not an array, so it stays "
        "trivially printable and joinable. Matches the blackstart slot numbers."
    ),
    "category": (
        "Normalized role of the circuit (e.g. hvac, kitchen, lighting, "
        "backup-feed), derived from the blackstart circuitType/role unless "
        "channel_map.json overrides it. Use it to group circuits in a query."
    ),
    "room": "Room or area the circuit serves, where the inventory records one.",
    "priority": (
        "Backup/load-shed priority from the blackstart inventory, e.g. "
        "'critical'. A STRING, not a number, and NULL when the inventory gives "
        "the circuit none."
    ),
    "estimated_watts": (
        "Estimated steady draw in watts from the blackstart inventory. A "
        "PLANNING ESTIMATE, NEVER A MEASUREMENT — for what actually happened, "
        "use energy_hourly.mean/kwh. Present even for circuits never metered."
    ),
    "blackstart_device_id": (
        "Join key back to the blackstart panel inventory, e.g. 'A-1-3' "
        "(panel-slots). NULL where no inventory entry exists — Bryant channels "
        "have none, since blackstart only describes breakers."
    ),
    "updated_at": (
        "When this row was built (UTC). The whole file is regenerated and "
        "atomically overwritten by 'energycap build-dim', so every row in it "
        "shares one timestamp; it dates the mapping, not any measurement."
    ),
}


# ---------------------------------------------------------- table comments

DATABASE_DESCRIPTION: Final[str] = _fit(
    _paragraphs(
        "Household energy and HVAC time series from the energyDataCapture "
        "pipeline (energycap): 30-second Leviton LWHEM-2 load-center and "
        "Bryant/Carrier Infinity readings, an hourly rollup derived from them, "
        "Bryant daily energy totals, and dim_channel, the semantic layer that "
        "says what each channel actually is.",
        f"Everything is partitioned on the LOCAL date in {_TZ}, never on UTC.",
        "Start from dim_channel and join on (source, device_id, channel_id).",
        _GAP_CLAUSE,
        # The database description is the first thing an LLM reads, and this is
        # the one gap that looks like a vocabulary question rather than a gap.
        STAGE_REPRESENTATION_NOTE,
    ),
    GLUE_DESCRIPTION_MAX_LEN,
    "the database description",
)

_RAW_30S_DESCRIPTION = _paragraphs(
    "GRAIN: one row per observation — one (instant, source, device, channel, "
    "metric) sample from the 30s Leviton + Bryant/Carrier poll. Long format: a "
    "new sensor adds rows, not columns.",
    _local_partition_clause(("year", "month", "day"), "ts_local"),
    _DEDUPE_CLAUSE,
    "A local day normally holds EITHER hourly part-*.parquet files OR the "
    "compacted day-*.parquet, never both: the compactor writes day-{D}.parquet, "
    "then archives the parts to energy/raw_30s_parts_archive/ (not a table "
    "here), so nothing is double counted. THE ONE EXCEPTION: both are present "
    "while a compaction is in flight, and stay so if it died between those "
    "steps; rows they share "
    "count twice. The tell is part-*.parquet beside day-*.parquet, or totals ~2x "
    "neighbouring days'. Re-run `energycap compact-daily --start D --end D`; "
    "until then, dedupe that day on the key above.",
    _GAP_CLAUSE,
    # The gap rule has a metric-shaped instance here, and it is invisible from
    # the metric list: on this system one of the two `stage` metrics can never
    # appear at all, so filtering on it returns an empty result forever.
    STAGE_REPRESENTATION_NOTE,
    # The roster is already on this table's `value` column; the description only
    # needs to say where the codes are decoded.
    "Enum rows carry integer codes; decoded in dim_channel's description.",
    # Tightened 2026-08-22 to leave headroom: this description sits within a few
    # characters of the 2048 budget, and every new metric grows the comments
    # around it. The facts are unchanged; the words are fewer.
    "Day-grain rows (kwh_day, cost_day_usd) are barred here — they live in "
    "energy_daily. Join dim_channel on (source, device_id, channel_id); prefer "
    "energy_hourly beyond a day.",
)

_HOURLY_DESCRIPTION = _paragraphs(
    "GRAIN: one row per (hour bucket, source, device, channel, metric) derived "
    "from energy_raw_30s. Regenerable: `energycap rollup --start D --end D` "
    "rebuilds whole local days, healing late data and bug fixes.",
    f"{HOURLY_GAP_WARNING}",
    "sample_count is on every row for that reason; kwh is observed-time-only — "
    "mean * sample_count * poll_interval_s / 3.6e6, never extrapolated across a "
    "gap — and NULL (not 0) for every metric but watts.",
    # The rollup excludes only DAY_GRAIN_METRICS, so the enum metrics ARE here,
    # aggregated over their integer codes. There is no `value` column to hang
    # the decode on, so it lives here — where a reader meets it before writing
    # an AVG() that returns a number with no meaning. STAGE_MEAN_NOTE keeps the
    # warning from swallowing stage_pct, which really is averageable.
    f"{ENUM_ROLLUP_WARNING} {STAGE_MEAN_NOTE} {_DECODE_POINTER.capitalize()}.",
    STAGE_REPRESENTATION_NOTE,
    "Buckets are LOCAL-day hours keyed by hour_start_utc: 25 per series on the "
    "DST fall-back day, 23 on spring-forward; local_hour_start is the label, "
    "ambiguous there.",
    _local_partition_clause(("year", "month"), "local_hour_start"),
    _GAP_CLAUSE_HOURLY,
    "Dedupe key: (hour_start_utc, source, device_id, channel_id, metric). "
    "Day-grain metrics (kwh_day, cost_day_usd) are excluded from the rollup "
    "input.",
)

_DAILY_DESCRIPTION = _paragraphs(
    "GRAIN: one row per (local day, HVAC component, metric) — Bryant/Carrier "
    "daily energy exactly as the Carrier cloud reported it. ts_utc is LOCAL "
    "MIDNIGHT of the measured day converted to UTC and ts_local is that local "
    "midnight; the timestamp labels a whole day, it is not an instant.",
    "channel_id is the lowercase component name "
    f"({_DAILY_CHANNELS}) and metric is kwh_day (kWh) or cost_day_usd (USD).",
    f"{_local_partition_clause(('year',), 'ts_local')} There is no month or day "
    "column here, so a WHERE on one is COLUMN_NOT_FOUND: filter the rest of the "
    "date on ts_local or ts_utc.",
    _DEDUPE_CLAUSE,
    _GAP_CLAUSE_DAY,
    "Carrier is fetched for day1 (yesterday) and day2 (the day before, as a "
    "revision), so the same day is written twice and dedupes to one row; the "
    "whole month file is regenerated on every write, which is what makes both "
    "the daily fetch and `energycap backfill` idempotent.",
    "These day-grain rows never appear in energy_raw_30s and are excluded from "
    "energy_hourly — do not add them to a 30s or hourly sum, and do not compare "
    "them to instantaneous watts.",
    "Components structurally disabled on this system (no gas, no reheat, no "
    "loop pump) emit NO ROW at all. An absent component is 'this house does not "
    "have it'; a present component reporting 0.0 really measured zero. "
    "Historical rows loaded by `energycap backfill` are written exactly as "
    "recorded, zeros included, because we cannot know retroactively which "
    "components were disabled.",
)

_DIM_DESCRIPTION = _paragraphs(
    "GRAIN: one row per channel — (source, device_id, channel_id). The semantic "
    "layer: what each Leviton breaker/CT and each Bryant channel actually is. "
    "THIS IS THE TABLE TO START FROM; join it to energy_raw_30s, energy_hourly "
    "and energy_daily on (source, device_id, channel_id).",
    "Not time series and not partitioned: a single Parquet file, rebuilt and "
    "atomically overwritten by `energycap build-dim` from the hand-maintained "
    "config/channel_map.json joined to the blackstart panel inventory. "
    "Blackstart is the source of truth for label/panel/slots/category/priority/"
    "estimated_watts; explicit fields in channel_map.json override it.",
    "COVERAGE IS NOT GUARANTEED. A live channel nobody has mapped yet is simply "
    "absent (build-dim warns about it), so use a LEFT JOIN — an unmapped "
    "channel must never drop a real measurement from a result.",
    "estimated_watts is a planning estimate from the inventory, never a "
    "measurement; use energy_hourly for what actually happened.",
    # The single authoritative home for the enum decode, and deliberately here:
    # this is the semantic-layer table, the decode is a dictionary, and one copy
    # in a 2048-character field beats two copies that both overflow. Every
    # enum-bearing column and both raw/hourly descriptions point at this
    # sentence, and it is generated from bryant.ENUM_TABLES so an appended code
    # appears on the next `create-glue-tables` run.
    # The decode ENDS this description on purpose: it is parsed back out by the
    # tests (and by anyone scripting against the catalog) as "everything after
    # 'Enum decode:'", so a sentence after it would be read as another entry.
    "For unit='enum' rows in energy_raw_30s and energy_hourly; codes are "
    "append-only and never renumbered, so an archived value keeps its meaning. "
    f"Enum decode: {_ENUM_DECODE}.",
)


# ============================================================================
# Table specs
# ============================================================================


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Everything needed to render one Glue table, and nothing else.

    ``prefix_builder`` is an :mod:`energy_capture.aws.s3io` key builder — it is
    called with a probe date purely to *read the layout* it produces, so the
    table Location and the projection template are derived from the same code
    that writes the objects rather than re-typed here.
    """

    name: str
    schema: pa.Schema
    prefix_builder: Callable[[date], str]
    partition_keys: tuple[str, ...]
    description: str
    column_comments: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        unknown = [key for key in self.partition_keys if key not in PARTITION_KEY_NAMES]
        if unknown:
            raise ValueError(f"{self.name}: unknown partition keys {unknown}")
        expected = PARTITION_KEY_NAMES[: len(self.partition_keys)]
        if tuple(self.partition_keys) != expected:
            raise ValueError(
                f"{self.name}: partition keys {self.partition_keys} must be an "
                f"outermost-first prefix of {PARTITION_KEY_NAMES}"
            )
        collisions = sorted(set(self.partition_keys) & set(self.schema.names))
        if collisions:
            raise ValueError(
                f"{self.name}: {collisions} are Hive partition keys and must not "
                "also be data columns (Glue rejects the duplicate)"
            )

    # -- derived layout ----------------------------------------------------

    def _probe_segments(self) -> list[str]:
        prefix = self.prefix_builder(_TEMPLATE_PROBE_DATE)
        return [segment for segment in prefix.strip("/").split("/") if segment]

    def location(self, bucket: str) -> str:
        """``s3://bucket/<prefix>/`` — the root above the partition directories."""
        root: list[str] = []
        for segment in self._probe_segments():
            if _partition_key_of(segment) is not None:
                break
            root.append(segment)
        return s3io.s3_uri(bucket, "/".join(root) + "/")

    def location_template(self, bucket: str) -> str:
        """``storage.location.template`` for partition projection.

        Derived by replacing each ``key=value`` segment of a real prefix with
        ``key=${key}``. If the S3 layout in :mod:`s3io` ever changes shape, this
        follows it — and if it stops matching ``partition_keys``, this raises.
        """
        segments: list[str] = []
        found: list[str] = []
        for segment in self._probe_segments():
            key = _partition_key_of(segment)
            if key is None:
                segments.append(segment)
                continue
            found.append(key)
            segments.append(f"{key}=${{{key}}}")
        if tuple(found) != tuple(self.partition_keys):
            raise ValueError(
                f"{self.name}: the s3io layout partitions on {tuple(found)} but "
                f"the spec declares {self.partition_keys}; they must agree"
            )
        return s3io.s3_uri(bucket, "/".join(segments))

    # -- derived catalog pieces -------------------------------------------

    def _generated_comments(self) -> Mapping[str, str]:
        """Per-table comments derived from this table's own shape.

        Currently the ``ts_utc``/``ts_local`` partition advice, which has to name
        the partition columns *this* table has. Generated rather than written out
        per spec so a new table (PLAN.md §13's ``energy_meter``, partitioned by
        year only) cannot inherit another table's partition list.
        """
        return _partition_scope_comments(self.partition_keys)

    def comment_for(self, column: str) -> str:
        """The comment for one data column; raises if there is not a real one."""
        comment = (
            self.column_comments.get(column)
            or self._generated_comments().get(column)
            or CANONICAL_COLUMN_COMMENTS.get(column)
        )
        if not comment or not comment.strip():
            raise ValueError(
                f"{self.name}.{column} has no comment. Table and column comments "
                "are a first-class deliverable (PLAN.md §12) — write the real "
                "string; a placeholder here is a defect."
            )
        return _fit(comment, GLUE_COMMENT_MAX_LEN, f"the comment on {self.name}.{column}")

    def columns(self) -> list[dict[str, str]]:
        """Glue ``Column`` entries for the data columns, in schema order."""
        return [
            {
                "Name": field_.name,
                "Type": arrow_to_glue_type(field_.type),
                "Comment": self.comment_for(field_.name),
            }
            for field_ in self.schema
        ]

    def partition_source_column(self) -> str:
        """The column this table's partition values are derived from.

        Read off the schema rather than assumed: ``energy_hourly`` has no
        ``ts_local``, so a partition comment claiming its year/month come from
        ``ts_local`` sends a reader looking for a column that is not there.
        """
        for candidate in _PARTITION_SOURCE_COLUMNS:
            if candidate in self.schema.names:
                return candidate
        raise ValueError(
            f"{self.name} is partitioned on {self.partition_keys} but carries "
            f"none of {_PARTITION_SOURCE_COLUMNS}; the partition column comments "
            "have to name the column the values come from, so add the local "
            "timestamp column to _PARTITION_SOURCE_COLUMNS rather than letting "
            "the comment name one this table does not have"
        )

    def partition_columns(self) -> list[dict[str, str]]:
        """Glue ``Column`` entries for the Hive partition keys."""
        if not self.partition_keys:
            return []
        comments = _partition_column_comments(self.partition_source_column())
        return [
            {
                "Name": key,
                "Type": PARTITION_COLUMN_TYPE,
                "Comment": _fit(
                    comments[key],
                    GLUE_COMMENT_MAX_LEN,
                    f"the comment on {self.name}.{key}",
                ),
            }
            for key in self.partition_keys
        ]


def _partition_key_of(segment: str) -> str | None:
    """``'year=2024'`` -> ``'year'``; anything else -> ``None``."""
    key, separator, _ = segment.partition("=")
    if separator and key in PARTITION_KEY_NAMES:
        return key
    return None


def _dim_channel_prefix(_local_day: date) -> str:
    """``energy/dim_channel/`` — derived from the key builder, not re-typed."""
    key = s3io.dim_channel_key()
    return key.rsplit("/", 1)[0] + "/"


def table_specs() -> tuple[TableSpec, ...]:
    """The four tables of PLAN.md §12, in creation order.

    Specs are bucket-independent — a bucket is supplied when a location is
    rendered (:meth:`TableSpec.location`, :func:`table_input`).

    PLAN.md §13's ``energy_meter`` is a fifth entry when it is built::

        TableSpec(
            name="energy_meter",
            schema=model.METER_SCHEMA,
            prefix_builder=s3io.meter_year_prefix,
            partition_keys=("year",),
            description=...,
        )

    Nothing else changes: ``interval_s`` already has a comment, the projection
    parameters are generated from ``partition_keys``, and the location template
    is read out of the ``s3io`` builder.
    """
    return (
        TableSpec(
            name=TABLE_ENERGY_RAW_30S,
            schema=model.RAW_30S_SCHEMA,
            prefix_builder=s3io.raw_30s_day_prefix,
            partition_keys=("year", "month", "day"),
            description=_RAW_30S_DESCRIPTION,
        ),
        TableSpec(
            name=TABLE_ENERGY_HOURLY,
            schema=model.HOURLY_SCHEMA,
            prefix_builder=s3io.hourly_month_prefix,
            partition_keys=("year", "month"),
            description=_HOURLY_DESCRIPTION,
            column_comments=_HOURLY_COLUMN_COMMENTS,
        ),
        TableSpec(
            name=TABLE_ENERGY_DAILY,
            schema=model.DAILY_SCHEMA,
            prefix_builder=s3io.daily_year_prefix,
            partition_keys=("year",),
            description=_DAILY_DESCRIPTION,
            column_comments={
                "metric": (
                    "Day-grain metric: 'kwh_day' (unit kWh) or 'cost_day_usd' "
                    "(unit USD). These two live ONLY in this table — barred from "
                    "energy_raw_30s and excluded from the energy_hourly rollup, "
                    "because a day total would poison hourly math."
                ),
                "unit": (
                    # Generated: exactly the units this table's two metrics use.
                    # The canonical comment lists the whole catalog vocabulary
                    # and points at an enum decode, and neither belongs here.
                    "Unit of value: "
                    f"{_unit_list_text(_metrics_for_table(TABLE_ENERGY_DAILY))} "
                    "only — kWh for metric='kwh_day', USD for "
                    "metric='cost_day_usd'. Constant per metric "
                    "(model.UNIT_FOR_METRIC); no other unit occurs in this "
                    "table, and no row here carries an integer code."
                ),
                "value": (
                    "The day's total: kWh when metric='kwh_day', US dollars when "
                    "metric='cost_day_usd', exactly as Carrier reported it. No "
                    "enum metrics exist in this table. A 0.0 here is a real "
                    "measured zero; a disabled component emits no row at all."
                ),
                "channel_id": (
                    "The HVAC energy component, lowercased: "
                    f"{_DAILY_CHANNELS}. (The camelCase spellings in Carrier's "
                    "API — hPHeat, fanGas, loopPump — are normalised to these.) "
                    "Join dim_channel on (source, device_id, channel_id)."
                ),
            },
        ),
        TableSpec(
            name=TABLE_DIM_CHANNEL,
            schema=DIM_CHANNEL_SCHEMA,
            prefix_builder=_dim_channel_prefix,
            partition_keys=(),
            description=_DIM_DESCRIPTION,
            column_comments=_DIM_COLUMN_COMMENTS,
        ),
    )


# ============================================================================
# Rendering a TableInput
# ============================================================================


def partition_projection_parameters(
    spec: TableSpec, bucket: str
) -> dict[str, str]:
    """Athena partition-projection table properties for ``spec`` (PLAN.md §12).

    Empty for an unpartitioned table. Otherwise ``projection.enabled=true``, one
    ``type``/``range``(/``digits``) triple per key, and the
    ``storage.location.template`` derived from the ``s3io`` layout — so Athena
    never needs a partition registered or a crawler run.
    """
    if not spec.partition_keys:
        return {}
    parameters = {"projection.enabled": "true"}
    for key in spec.partition_keys:
        for setting, value in _PROJECTION_BY_KEY[key].items():
            parameters[f"projection.{key}.{setting}"] = value
    parameters["storage.location.template"] = spec.location_template(bucket)
    return parameters


def table_input(spec: TableSpec, bucket: str) -> dict[str, Any]:
    """The complete Glue ``TableInput`` for ``spec``.

    A pure function of the spec and the bucket: same inputs, byte-identical
    output. That is what makes the create-or-update idempotent — a second run
    produces exactly the same definition and therefore writes nothing.
    """
    description = _fit(
        spec.description, GLUE_DESCRIPTION_MAX_LEN, f"the {spec.name} table comment"
    )
    parameters = {
        "EXTERNAL": "TRUE",
        "classification": "parquet",
        **partition_projection_parameters(spec, bucket),
    }
    return {
        "Name": spec.name,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "PartitionKeys": spec.partition_columns(),
        "StorageDescriptor": {
            "Columns": spec.columns(),
            "Location": spec.location(bucket),
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "Compressed": False,
            "StoredAsSubDirectories": False,
            "SerdeInfo": {
                "SerializationLibrary": PARQUET_SERDE,
                "Parameters": {"serialization.format": "1"},
            },
        },
    }


def table_inputs(bucket: str) -> dict[str, dict[str, Any]]:
    """``{table name: TableInput}`` for every table, rendered against ``bucket``."""
    return {spec.name: table_input(spec, bucket) for spec in table_specs()}


# ============================================================================
# Comparing what is there with what we want
# ============================================================================


def _columns_of(columns: Sequence[Mapping[str, Any]] | None) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (str(c.get("Name", "")), str(c.get("Type", "")), str(c.get("Comment", "") or ""))
        for c in (columns or ())
    )


def _managed_parameters(parameters: Mapping[str, str] | None) -> dict[str, str]:
    return {
        key: value
        for key, value in (parameters or {}).items()
        if key not in _UNMANAGED_TABLE_PARAMETERS
    }


def _table_differences(existing: Mapping[str, Any], desired: Mapping[str, Any]) -> list[str]:
    """Field names where the catalog disagrees with the desired definition.

    Empty means "already correct, do not issue an update" — which is how a
    re-run stays a true no-op instead of minting a new table version every time.
    """
    diffs: list[str] = []
    if (existing.get("Description") or "") != desired["Description"]:
        diffs.append("description")
    if (existing.get("TableType") or "") != desired["TableType"]:
        diffs.append("table_type")
    if _managed_parameters(existing.get("Parameters")) != desired["Parameters"]:
        diffs.append("parameters")
    if _columns_of(existing.get("PartitionKeys")) != _columns_of(desired["PartitionKeys"]):
        diffs.append("partition_keys")

    have = existing.get("StorageDescriptor") or {}
    want = desired["StorageDescriptor"]
    if _columns_of(have.get("Columns")) != _columns_of(want["Columns"]):
        diffs.append("columns")
    if str(have.get("Location") or "").rstrip("/") != want["Location"].rstrip("/"):
        diffs.append("location")
    if str(have.get("InputFormat") or "") != want["InputFormat"]:
        diffs.append("input_format")
    if str(have.get("OutputFormat") or "") != want["OutputFormat"]:
        diffs.append("output_format")
    have_serde = (have.get("SerdeInfo") or {}).get("SerializationLibrary") or ""
    if str(have_serde) != want["SerdeInfo"]["SerializationLibrary"]:
        diffs.append("serde")
    return diffs


# ============================================================================
# The stage entry point
# ============================================================================


def _error_code(exc: ClientError) -> str:
    return str((exc.response.get("Error") or {}).get("Code") or "")


def _get_table(client: Any, database: str, name: str) -> dict[str, Any] | None:
    try:
        return dict(client.get_table(DatabaseName=database, Name=name)["Table"])
    except ClientError as exc:
        if _error_code(exc) == _ENTITY_NOT_FOUND:
            return None
        raise


def _ensure_database(client: Any, database: str, *, dry_run: bool) -> tuple[bool, bool]:
    """``(exists, created)`` — creating the database if it is absent.

    An existing database is never modified: it may hold tables this pipeline
    does not own, and its description is not ours to overwrite.
    """
    try:
        client.get_database(Name=database)
    except ClientError as exc:
        if _error_code(exc) != _ENTITY_NOT_FOUND:
            raise
    else:
        log.debug("glue_database_exists", database=database)
        return True, False

    if dry_run:
        log.info("glue_database_would_create", database=database, dry_run=True)
        return False, False

    try:
        client.create_database(
            DatabaseInput={"Name": database, "Description": DATABASE_DESCRIPTION}
        )
    except ClientError as exc:
        if _error_code(exc) != _ALREADY_EXISTS:
            raise
        # Another runner won the race; that is a success, not a failure.
        log.info("glue_database_exists", database=database)
        return True, False
    log.info("glue_database_created", database=database)
    return True, True


def create_or_update_tables(
    *,
    database: str | None = None,
    dry_run: bool = False,
    bucket: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    """Create or update the Glue tables of PLAN.md §12. Idempotent.

    Creates the database if it is absent, then for each table: creates it if it
    is missing, updates it if its definition differs, and leaves it alone if it
    already matches. Nothing is ever deleted and no partition is registered —
    partition projection means there are none to register.

    ``dry_run`` reads the catalog and reports what would change without issuing
    a single write.

    Returns a mapping of counts for the CLI's ``stage_ok`` log line.
    """
    resolved_database = database or get_settings().glue_database
    resolved_bucket = bucket or s3io.default_bucket()
    glue = client if client is not None else s3io.get_client("glue")

    specs = table_specs()
    database_exists, database_created = _ensure_database(
        glue, resolved_database, dry_run=dry_run
    )

    created: list[str] = []
    updated: list[str] = []
    unchanged: list[str] = []

    for spec in specs:
        desired = table_input(spec, resolved_bucket)
        existing = (
            _get_table(glue, resolved_database, spec.name) if database_exists else None
        )

        if existing is None:
            created.append(spec.name)
            if not dry_run:
                glue.create_table(DatabaseName=resolved_database, TableInput=desired)
            log.info(
                "glue_table_created",
                database=resolved_database,
                table=spec.name,
                location=desired["StorageDescriptor"]["Location"],
                columns=len(desired["StorageDescriptor"]["Columns"]),
                partition_keys=list(spec.partition_keys),
                dry_run=dry_run,
            )
            continue

        diffs = _table_differences(existing, desired)
        if not diffs:
            unchanged.append(spec.name)
            log.info(
                "glue_table_unchanged",
                database=resolved_database,
                table=spec.name,
                dry_run=dry_run,
            )
            continue

        updated.append(spec.name)
        if not dry_run:
            glue.update_table(DatabaseName=resolved_database, TableInput=desired)
        log.info(
            "glue_table_updated",
            database=resolved_database,
            table=spec.name,
            changed=diffs,
            dry_run=dry_run,
        )

    summary: dict[str, Any] = {
        "database": resolved_database,
        "database_created": database_created,
        "bucket": resolved_bucket,
        "tables": len(specs),
        "created": len(created),
        "updated": len(updated),
        "unchanged": len(unchanged),
        "created_tables": created,
        "updated_tables": updated,
        "dry_run": dry_run,
    }
    log.info("glue_tables_done", **summary)
    return summary
