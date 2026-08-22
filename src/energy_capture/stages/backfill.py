"""Backfill historical Bryant daily energy into ``energy/daily`` (PLAN.md §8).

Two legacy stores hold the Bryant daily energy that predates this pipeline, and
this stage lands both of them in the canonical ``energy/daily`` dataset:

**Source A — DynamoDB ``bryant-energy-data`` (us-east-1).** One item per date,
partition key ``date`` (``YYYY-MM-DD``), attributes ``serial_number``,
``period_type`` (``day1``/``day2``), ``collected_at``, and the 16 ``Decimal``
metric attributes ``eHeatKwh`` … ``reheatDollars``. The table is tiny, so it is
read with a single ``Scan``. **This stage is read-only against DynamoDB**: it
issues nothing but ``Scan``, so ``dynamodb:Scan`` on that one table is the
entire IAM requirement (PLAN.md §8). Nothing here writes, updates or deletes —
the old collector keeps running as a safety net and must not be disturbed
(PLAN.md §2.2).

**Source B — the old collector's JSON files** (default
``~/code/bryantDataCollector/energy_data/energy_YYYY_MM.json``). An object keyed
``YYYY-MM-DD`` -> ``{period_type, collected_at, data: {…16 camelCase fields…}}``
with **no serial**, so ``CARRIER_SERIAL`` supplies ``device_id``. The path is a
parameter (``legacy_path=``) with an environment override, never a hardcoded
literal at the point of use.

Row mapping is §7.2's, exactly — the same rows the live daily fetch produces, so
the two are interchangeable under the canonical dedupe key:

* ``source='bryant'``, ``device_id`` = system serial,
* ``channel_id`` = the component **lowercased** (``eheat``, ``cooling``, ``fan``,
  ``fangas``, ``hpheat``, ``looppump``, ``gas``, ``reheat``),
* ``metric`` = ``kwh_day`` (``kWh``) or ``cost_day_usd`` (``USD``),
* ``ts_utc`` = **local midnight of the measured day, converted to UTC**
  (:func:`energy_capture.timeutil.local_midnight_utc`), ``ts_local`` = that local
  midnight.

The camelCase attribute -> ``(channel_id, metric)`` correspondence is the
explicit 16-row table :data:`ATTRIBUTE_MAP`. It is a table and not string
munging on purpose: ``hPHeatDollars`` -> ``hpheat`` and ``fanGasKwh`` ->
``fangas`` are not derivable by any rule that also survives a future rename, and
an unrecognised attribute is WARNed rather than dropped in silence
(``backfill_unknown_attribute``), so a component can never disappear quietly.

Two rules that look wrong until you read §8
-------------------------------------------

**Recorded zeros are written as recorded.** The live daily fetch (§7.2) *skips*
components whose ``energyConfig.<name>.enabled`` is false, because a structurally
absent component is not a zero. History carries no ``energyConfig``, and we
cannot know retroactively whether a component was disabled or merely idle — so
§8 is explicit: **do not skip zeros here.** A zero in DynamoDB is what the API
said that day, and recording what the API said is cardinal rule 2. (The old
collector also coerced nulls to ``Decimal("0")`` before writing; §8 calls that
known source-side lossage and accepts it. Nothing in this pipeline can undo it.)

**A missing attribute is still a gap.** Zero is recorded; *absent* is not
invented. An attribute that is absent, null, non-numeric or non-finite emits no
row at all (cardinal rule 1).

Idempotency
-----------
``run(start, end)`` takes LOCAL dates and rewrites every affected monthly file
``energy/daily/year=YYYY/bryant-{YYYYMM}.parquet`` (the deterministic name from
:func:`energy_capture.aws.s3io.daily_key`) completely, from a full merge:

    in-range DynamoDB rows, then in-range legacy JSON rows, then every row the
    existing monthly object already held

deduped first-occurrence-wins on
:data:`energy_capture.model.DEDUPE_KEY`. That order is the whole precedence
policy: DynamoDB beats the legacy JSON where both cover a date (§8 — DynamoDB
carries provenance: ``serial_number``, ``period_type``, ``collected_at``), and a
backfilled row beats whatever the object held before (§10's "latest write wins
at the file level"). Rows the backfill sources do not cover — other months' days
inside the same file, or days only the live fetch ever saw — are carried through
untouched, because *regenerating* a file must never mean deleting data that this
run had no opinion about.

Two runs over the same range therefore produce byte-identical objects: the
inputs are the same, the dedupe is order-deterministic, and
``model.observations_to_table`` sorts deterministically.

Day-grain rows never reach ``raw_30s``
--------------------------------------
Everything here is written through ``Dataset.DAILY`` and
:func:`energy_capture.aws.s3io.daily_key`. This module names no ``raw_30s``
key builder at all, and ``model.observations_to_table`` independently rejects a
non-day-grain metric for that dataset (CLAUDE.md rule 6).
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow as pa
from botocore.client import BaseClient

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.stages import dailystore
from energy_capture.config import get_settings
from energy_capture.health import StatusStore, get_status_store
from energy_capture.logging import get_logger

__all__ = [
    "ATTRIBUTE_INDEX",
    "ATTRIBUTE_MAP",
    "COMPONENTS",
    "DATE_ATTRIBUTE",
    "DEFAULT_LEGACY_JSON_PATH",
    "DYNAMODB_REGION",
    "LEGACY_GLOB",
    "LEGACY_PATH_ENV",
    "NON_METRIC_ATTRIBUTES",
    "ORIGIN_DYNAMODB",
    "ORIGIN_EXISTING",
    "ORIGIN_LEGACY_JSON",
    "SOURCE_PRECEDENCE",
    "STATUS_SECTION",
    "AttributeSpec",
    "BackfillError",
    "DailyRecord",
    "MonthResult",
    "build_month_table",
    "collect_records",
    "group_by_month",
    "legacy_json_files",
    "load_legacy_file",
    "load_legacy_json",
    "record_to_observations",
    "records_to_observations",
    "resolve_legacy_path",
    "run",
    "scan_dynamodb",
]

log = get_logger("backfill")

#: ``status.json`` section this stage owns. PLAN.md §11 does not list one —
#: backfill is a manual, occasional operation — but every other stage records
#: its outcome there and an operator running a multi-year import wants the same.
STATUS_SECTION = "backfill"


class BackfillError(RuntimeError):
    """At least one affected month failed to regenerate.

    Raised only after every month has been attempted, so one unreadable existing
    object cannot strand the rest of an import — the same policy as
    ``uploader.UploadFailed``, ``compactor.CompactionError`` and
    ``rollup.RollupError`` (DEVIATIONS.md #26).
    """


# ------------------------------------------------------------- the 16-row map


@dataclass(frozen=True, slots=True)
class AttributeSpec:
    """One legacy attribute and the canonical row it becomes.

    Attributes:
        attribute: the camelCase key as it appears in *both* legacy stores (the
            DynamoDB item attribute and the legacy JSON ``data`` key — they were
            written from the same GraphQL ``energyPeriods`` object, so they are
            spelled identically).
        channel_id: the component, lowercased (PLAN.md §7.2).
        metric: ``kwh_day`` or ``cost_day_usd``.
    """

    attribute: str
    channel_id: str
    metric: str

    @property
    def unit(self) -> str:
        """Canonical unit, resolved through :mod:`energy_capture.model`."""
        return model.unit_for_metric(self.metric)


METRIC_KWH = "kwh_day"
METRIC_COST = "cost_day_usd"

#: The eight energy components, in the order PLAN.md §7.2 lists them.
COMPONENTS: tuple[str, ...] = (
    "eheat",
    "cooling",
    "fan",
    "fangas",
    "hpheat",
    "looppump",
    "gas",
    "reheat",
)

#: **The mapping.** Sixteen rows, written out one by one and never derived by
#: lowercasing or suffix-stripping a field name. The casing traps this avoids are
#: real: ``eHeat``/``hPHeat``/``fanGas``/``loopPump`` are camelCase in
#: ``energyPeriods`` but lowercase (``eheat``, ``hpheat``, ``fangas``,
#: ``looppump``) in ``energyConfig`` and in ``channel_id`` (PLAN.md §7.2,
#: CLAUDE.md "API gotchas"). Pinned attribute-by-attribute in
#: ``tests/test_backfill.py`` so a rename cannot silently drop a component.
ATTRIBUTE_MAP: tuple[AttributeSpec, ...] = (
    AttributeSpec("eHeatKwh", "eheat", METRIC_KWH),
    AttributeSpec("eHeatDollars", "eheat", METRIC_COST),
    AttributeSpec("coolingKwh", "cooling", METRIC_KWH),
    AttributeSpec("coolingDollars", "cooling", METRIC_COST),
    AttributeSpec("fanKwh", "fan", METRIC_KWH),
    AttributeSpec("fanDollars", "fan", METRIC_COST),
    AttributeSpec("fanGasKwh", "fangas", METRIC_KWH),
    AttributeSpec("fanGasDollars", "fangas", METRIC_COST),
    AttributeSpec("hPHeatKwh", "hpheat", METRIC_KWH),
    AttributeSpec("hPHeatDollars", "hpheat", METRIC_COST),
    AttributeSpec("loopPumpKwh", "looppump", METRIC_KWH),
    AttributeSpec("loopPumpDollars", "looppump", METRIC_COST),
    AttributeSpec("gasKwh", "gas", METRIC_KWH),
    AttributeSpec("gasDollars", "gas", METRIC_COST),
    AttributeSpec("reheatKwh", "reheat", METRIC_KWH),
    AttributeSpec("reheatDollars", "reheat", METRIC_COST),
)

#: ``attribute -> AttributeSpec``, for lookup while parsing an item.
ATTRIBUTE_INDEX: dict[str, AttributeSpec] = {
    spec.attribute: spec for spec in ATTRIBUTE_MAP
}

#: Keys that legitimately appear alongside the 16 metrics and are not metrics.
#: ``energyPeriodType`` rides along inside the legacy JSON ``data`` object;
#: ``date``/``serial_number``/``period_type``/``collected_at`` are the DynamoDB
#: item's own bookkeeping. Anything *else* is WARNed rather than ignored.
NON_METRIC_ATTRIBUTES: frozenset[str] = frozenset(
    {"date", "serial_number", "period_type", "collected_at", "energyPeriodType"}
)

#: DynamoDB item attribute holding the partition key (a ``YYYY-MM-DD`` string).
DATE_ATTRIBUTE = "date"

#: Region the legacy table lives in — a confirmed value (PLAN.md §8, §2.6), not
#: ``AWS_REGION``: the bucket may move, the old collector's table does not.
DYNAMODB_REGION = "us-east-1"

# --------------------------------------------------------------- origins

ORIGIN_DYNAMODB = "dynamodb"
ORIGIN_LEGACY_JSON = "legacy_json"
#: Rows already present in the monthly object; carried through, never preferred.
ORIGIN_EXISTING = "existing"

#: Precedence, highest first — the order rows are concatenated in before the
#: first-occurrence-wins dedupe. PLAN.md §8: prefer DynamoDB, it has provenance.
SOURCE_PRECEDENCE: tuple[str, ...] = (ORIGIN_DYNAMODB, ORIGIN_LEGACY_JSON)

# --------------------------------------------------------------- legacy JSON

#: Where the old collector's exports live on the Mac (PLAN.md §8 names
#: ``energy_2026_01.json`` in this directory).
DEFAULT_LEGACY_JSON_PATH = Path("~/code/bryantDataCollector/energy_data")

#: Environment override for that path, via ``Settings.bryant_legacy_json_path``
#: like every other knob (PLAN.md §14). Named here so the stage can document and
#: test the variable without importing the field name as a string literal.
LEGACY_PATH_ENV = "BRYANT_LEGACY_JSON_PATH"

#: Filenames picked up when the resolved path is a directory.
LEGACY_GLOB = "energy_*.json"


# ------------------------------------------------------------------- records


@dataclass(frozen=True, slots=True)
class DailyRecord:
    """One measured local day from one legacy store, before row mapping.

    Attributes:
        local_day: the LOCAL date the energy was measured on (the DynamoDB
            partition key / the legacy JSON object key).
        serial: Bryant system serial — ``device_id`` on every emitted row.
        origin: :data:`ORIGIN_DYNAMODB` or :data:`ORIGIN_LEGACY_JSON`.
        values: ``attribute -> raw value`` for the attributes actually present.
            ``Decimal`` from DynamoDB, ``Decimal``/``int`` from the JSON (it is
            parsed with ``parse_float=Decimal`` so no precision is lost before
            this module gets a chance to check for it).
        period_type: ``day1``/``day2`` provenance, for logging only.
        collected_at: when the old collector fetched it, for logging only.
        origin_detail: file path or table name, for logging only.
    """

    local_day: date
    serial: str
    origin: str
    values: Mapping[str, Any]
    period_type: str | None = None
    collected_at: str | None = None
    origin_detail: str | None = None

    @property
    def sort_key(self) -> tuple[int, str]:
        """Deterministic ordering: precedence first, then date."""
        rank = (
            SOURCE_PRECEDENCE.index(self.origin)
            if self.origin in SOURCE_PRECEDENCE
            else len(SOURCE_PRECEDENCE)
        )
        return (rank, self.local_day.isoformat())


# ------------------------------------------------------------ value coercion


def _is_number(value: Any) -> bool:
    return isinstance(value, (Decimal, int, float)) and not isinstance(value, bool)


def _coerce_number(
    value: Any, *, attribute: str, local_day: date, origin: str
) -> float | None:
    """Legacy value -> ``float``, or ``None`` when there is no number to record.

    ``None`` means **emit no row** (cardinal rule 1): an absent, null,
    non-numeric or non-finite attribute is a gap, not a zero. A genuine ``0`` is
    a number and comes back as ``0.0`` — §8 requires historical zeros to be
    written as recorded.

    ``Decimal`` -> ``float`` is checked, not assumed: if the ``float`` does not
    round-trip back to the same decimal value the loss is logged
    (``backfill_precision_loss``) with the exact original. The row is still
    emitted — dropping it would manufacture a gap where a measurement exists —
    but the loss is never silent, which is what PLAN.md's "must not lose
    precision silently" asks for. DynamoDB numbers carry up to 38 significant
    digits; an IEEE double carries ~15–17, so this can only fire on a value the
    Carrier API never produced.
    """
    if value is None:
        return None

    if isinstance(value, str):
        try:
            value = Decimal(value.strip())
        except (InvalidOperation, ValueError):
            log.warning(
                "backfill_non_numeric_attribute",
                attribute=attribute,
                local_date=local_day.isoformat(),
                origin=origin,
                value_type="str",
            )
            return None
        log.warning(
            "backfill_string_number",
            attribute=attribute,
            local_date=local_day.isoformat(),
            origin=origin,
        )

    if not _is_number(value):
        log.warning(
            "backfill_non_numeric_attribute",
            attribute=attribute,
            local_date=local_day.isoformat(),
            origin=origin,
            value_type=type(value).__name__,
        )
        return None

    if isinstance(value, Decimal) and not value.is_finite():
        log.warning(
            "backfill_non_finite_attribute",
            attribute=attribute,
            local_date=local_day.isoformat(),
            origin=origin,
            value=str(value),
        )
        return None

    number = float(value)
    if not math.isfinite(number):
        log.warning(
            "backfill_non_finite_attribute",
            attribute=attribute,
            local_date=local_day.isoformat(),
            origin=origin,
            value=repr(value),
        )
        return None

    if isinstance(value, Decimal) and Decimal(repr(number)) != value:
        log.warning(
            "backfill_precision_loss",
            attribute=attribute,
            local_date=local_day.isoformat(),
            origin=origin,
            decimal=str(value),
            float_value=repr(number),
        )
    return number


def _warn_unknown_attributes(values: Iterable[str], *, local_day: date, origin: str) -> None:
    """WARN for a key that is neither one of the 16 metrics nor bookkeeping.

    This is the tripwire for a future field rename: if Carrier ever ships
    ``heatPumpHeatKwh`` instead of ``hPHeatKwh``, the component does not vanish
    quietly — it shows up here as an unmapped attribute.
    """
    unknown = sorted(
        name
        for name in values
        if name not in ATTRIBUTE_INDEX and name not in NON_METRIC_ATTRIBUTES
    )
    if unknown:
        log.warning(
            "backfill_unknown_attribute",
            attributes=unknown,
            local_date=local_day.isoformat(),
            origin=origin,
        )


# ------------------------------------------------------------- row mapping


def record_to_observations(record: DailyRecord) -> list[model.Observation]:
    """Map one :class:`DailyRecord` to canonical rows (PLAN.md §7.2).

    ``ts_utc`` is local midnight of ``record.local_day`` converted to UTC and
    ``ts_local`` is that same local midnight — both via
    :mod:`energy_capture.timeutil`, the only place this codebase converts. US DST
    transitions happen at 02:00, so local midnight is never skipped or repeated
    and the conversion is unambiguous on both transition days (the offset simply
    differs: EST on the spring-forward date, EDT on the fall-back date).

    Rows are emitted in :data:`ATTRIBUTE_MAP` order; the caller sorts.
    """
    ts_utc = timeutil.local_midnight_utc(record.local_day)
    _warn_unknown_attributes(
        record.values, local_day=record.local_day, origin=record.origin
    )

    rows: list[model.Observation] = []
    for spec in ATTRIBUTE_MAP:
        if spec.attribute not in record.values:
            # Absent is not zero. Gaps stay gaps (CLAUDE.md rule 1).
            continue
        value = _coerce_number(
            record.values[spec.attribute],
            attribute=spec.attribute,
            local_day=record.local_day,
            origin=record.origin,
        )
        if value is None:
            continue
        if spec.channel_id == "gas" and spec.metric == METRIC_KWH and value != 0.0:
            # PLAN.md §7.2: the field says kWh but gas probably is not kWh, and
            # this system is a heat pump with electric strips. Record it as-is
            # and flag it for a human — never guess a conversion.
            log.warning(
                "backfill_gas_kwh_nonzero",
                local_date=record.local_day.isoformat(),
                origin=record.origin,
                value=value,
            )
        rows.append(
            model.make_observation(
                ts_utc=ts_utc,
                source=model.SOURCE_BRYANT,
                device_id=record.serial,
                channel_id=spec.channel_id,
                metric=spec.metric,
                value=value,
            )
        )
    return rows


def records_to_observations(
    records: Iterable[DailyRecord],
) -> list[tuple[str, model.Observation]]:
    """``(origin, Observation)`` pairs for ``records``, in precedence order.

    Sorting by :attr:`DailyRecord.sort_key` puts every DynamoDB record ahead of
    every legacy-JSON record, which is what makes the later first-occurrence-wins
    dedupe implement PLAN.md §8's "prefer DynamoDB".
    """
    pairs: list[tuple[str, model.Observation]] = []
    for record in sorted(records, key=lambda r: r.sort_key):
        for obs in record_to_observations(record):
            pairs.append((record.origin, obs))
    return pairs


def _dedupe_with_origin(
    pairs: Sequence[tuple[str, model.Observation]],
) -> tuple[list[model.Observation], dict[str, int]]:
    """First occurrence of each :data:`~energy_capture.model.DEDUPE_KEY` wins.

    Mirrors :func:`energy_capture.model.dedupe_observations` exactly (the model
    owns the semantics); this variant only additionally reports which origin each
    surviving row came from, so the run summary can say how many rows DynamoDB
    actually won.
    """
    seen: set[tuple[Any, ...]] = set()
    kept: list[model.Observation] = []
    counts: dict[str, int] = {}
    for origin, obs in pairs:
        key = tuple(getattr(obs, name) for name in model.DEDUPE_KEY)
        if key in seen:
            continue
        seen.add(key)
        kept.append(obs)
        counts[origin] = counts.get(origin, 0) + 1
    return kept, counts


# ------------------------------------------------------- source A: DynamoDB


def _dynamodb_client(
    client: BaseClient | None = None, *, region: str | None = None
) -> BaseClient:
    """A DynamoDB client pinned to the legacy table's region.

    ``AWS_REGION`` governs where the S3 bucket lives; the legacy table is a
    confirmed value in **us-east-1** (PLAN.md §8, §2.6) and does not move just
    because the bucket does. Pointing a client at the wrong region would fail
    with ``ResourceNotFoundException`` — indistinguishable, at a glance, from
    "the table is empty" — so the region is explicit here rather than inherited.
    """
    if client is not None:
        return client
    return s3io.get_session().client(
        "dynamodb", region_name=region or DYNAMODB_REGION
    )


def _deserialize_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Low-level ``AttributeValue`` item -> plain Python (numbers as ``Decimal``)."""
    from boto3.dynamodb.types import TypeDeserializer

    deserializer = TypeDeserializer()
    out: dict[str, Any] = {}
    for name, value in item.items():
        try:
            out[name] = deserializer.deserialize(value)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            log.warning("backfill_undeserializable_attribute", attribute=name)
    return out


def scan_dynamodb(
    *,
    table: str | None = None,
    client: BaseClient | None = None,
    region: str | None = None,
    serial_default: str | None = None,
) -> list[DailyRecord]:
    """Read every item of the legacy table with a single paginated ``Scan``.

    **Read-only.** ``Scan`` is the only DynamoDB operation this module can
    perform; the required IAM permission is exactly ``dynamodb:Scan`` on
    ``DYNAMODB_TABLE`` (PLAN.md §8). ``ConsistentRead`` is on — the table is
    tiny, and a backfill that silently read a stale replica would be a bad thing
    to discover months later.

    An item whose ``date`` is missing or unparseable is WARNed and skipped: the
    partition key is the measured day, and a row cannot be placed in time without
    it. An item with no ``serial_number`` falls back to ``CARRIER_SERIAL`` with a
    WARN — the measurement is real even when the provenance attribute is not.
    """
    table_name = table or get_settings().require("dynamodb_table")
    fallback_serial = serial_default or get_settings().require("carrier_serial")
    ddb = _dynamodb_client(client, region=region)

    records: list[DailyRecord] = []
    skipped = 0
    scanned = 0
    paginator = ddb.get_paginator("scan")
    for page in paginator.paginate(TableName=table_name, ConsistentRead=True):
        for raw in page.get("Items", ()):
            scanned += 1
            item = _deserialize_item(raw)
            local_day = _parse_record_date(
                item.get(DATE_ATTRIBUTE), origin=ORIGIN_DYNAMODB, detail=table_name
            )
            if local_day is None:
                skipped += 1
                continue
            serial = str(item.get("serial_number") or "").strip()
            if not serial:
                log.warning(
                    "backfill_missing_serial",
                    local_date=local_day.isoformat(),
                    origin=ORIGIN_DYNAMODB,
                    fallback=fallback_serial,
                )
                serial = fallback_serial
            records.append(
                DailyRecord(
                    local_day=local_day,
                    serial=serial,
                    origin=ORIGIN_DYNAMODB,
                    values={
                        name: value
                        for name, value in item.items()
                        if name != DATE_ATTRIBUTE
                    },
                    period_type=_as_optional_str(item.get("period_type")),
                    collected_at=_as_optional_str(item.get("collected_at")),
                    origin_detail=table_name,
                )
            )

    log.info(
        "backfill_dynamodb_scanned",
        table=table_name,
        items=scanned,
        records=len(records),
        skipped=skipped,
    )
    return records


def _as_optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _parse_record_date(value: Any, *, origin: str, detail: str | None) -> date | None:
    """``YYYY-MM-DD`` -> :class:`datetime.date`; WARN and ``None`` on anything else."""
    if value is None:
        log.warning("backfill_missing_date", origin=origin, detail=detail)
        return None
    try:
        return timeutil.parse_local_date(str(value))
    except (ValueError, TypeError):
        log.warning(
            "backfill_unparseable_date", origin=origin, detail=detail, value=str(value)
        )
        return None


# ----------------------------------------------------- source B: legacy JSON


def resolve_legacy_path(path: Path | str | None = None) -> Path:
    """Resolve the legacy JSON location: argument, then settings, then the default.

    The settings hop goes through :class:`~energy_capture.config.Settings` rather
    than ``os.environ`` so this knob is discoverable in the same place as every
    other one, and so ``.env`` reaches it like it reaches the rest.
    """
    if path is not None:
        return Path(path).expanduser()
    configured = get_settings().bryant_legacy_json_path
    if configured is not None and str(configured).strip():
        return Path(configured).expanduser()
    return DEFAULT_LEGACY_JSON_PATH.expanduser()


def legacy_json_files(path: Path | str | None = None) -> list[Path]:
    """The legacy JSON files to read: one file, or ``energy_*.json`` in a directory.

    A path that does not exist is a WARN and an empty list, not an error — an
    operator may have only the DynamoDB half of the history, and a missing
    directory must not abort an import that would otherwise succeed.
    """
    resolved = resolve_legacy_path(path)
    if resolved.is_dir():
        files = sorted(resolved.glob(LEGACY_GLOB))
        if not files:
            log.warning("backfill_legacy_dir_empty", path=str(resolved), glob=LEGACY_GLOB)
        return files
    if resolved.exists():
        return [resolved]
    log.warning("backfill_legacy_path_missing", path=str(resolved))
    return []


def load_legacy_file(path: Path | str, *, serial: str) -> list[DailyRecord]:
    """Parse one legacy JSON file into records.

    Parsed with ``parse_float=Decimal`` so the JSON text is converted to a float
    exactly once — in :func:`_coerce_number`, where the loss is checked — rather
    than twice with the first conversion invisible.
    """
    file_path = Path(path)
    with file_path.open("r", encoding="utf-8") as handle:
        document = json.load(handle, parse_float=Decimal)

    if not isinstance(document, Mapping):
        raise BackfillError(
            f"{file_path}: expected an object keyed by YYYY-MM-DD, "
            f"got {type(document).__name__}"
        )

    records: list[DailyRecord] = []
    for key, entry in document.items():
        local_day = _parse_record_date(
            key, origin=ORIGIN_LEGACY_JSON, detail=str(file_path)
        )
        if local_day is None:
            continue
        if not isinstance(entry, Mapping):
            log.warning(
                "backfill_legacy_entry_not_object",
                local_date=local_day.isoformat(),
                path=str(file_path),
            )
            continue
        data = entry.get("data")
        if not isinstance(data, Mapping):
            log.warning(
                "backfill_legacy_entry_without_data",
                local_date=local_day.isoformat(),
                path=str(file_path),
            )
            continue
        records.append(
            DailyRecord(
                local_day=local_day,
                serial=serial,
                origin=ORIGIN_LEGACY_JSON,
                values=dict(data),
                period_type=_as_optional_str(entry.get("period_type")),
                collected_at=_as_optional_str(entry.get("collected_at")),
                origin_detail=str(file_path),
            )
        )

    log.info(
        "backfill_legacy_file_read",
        path=str(file_path),
        records=len(records),
        entries=len(document),
    )
    return records


def load_legacy_json(
    path: Path | str | None = None, *, serial: str | None = None
) -> list[DailyRecord]:
    """Every legacy JSON record under ``path`` (file or directory).

    The legacy files carry **no serial** (PLAN.md §8), so ``CARRIER_SERIAL``
    supplies ``device_id`` — which is also what makes these rows collide with the
    DynamoDB rows on the canonical dedupe key, and therefore what makes
    "DynamoDB wins" mean anything.
    """
    resolved_serial = serial or get_settings().require("carrier_serial")
    records: list[DailyRecord] = []
    for file_path in legacy_json_files(path):
        records.extend(load_legacy_file(file_path, serial=resolved_serial))
    return records


# ------------------------------------------------------------- gathering


def collect_records(
    *,
    start: date,
    end: date,
    origins: Sequence[str] = SOURCE_PRECEDENCE,
    table: str | None = None,
    client: BaseClient | None = None,
    legacy_path: Path | str | None = None,
    serial: str | None = None,
) -> list[DailyRecord]:
    """Read both legacy stores and keep the records inside ``[start, end]``.

    Both stores are read whole (the table is tiny and the JSON is a few KB) and
    filtered afterwards, so the range never turns into a query predicate that
    could silently miss a differently-keyed item.
    """
    if end < start:
        raise ValueError(f"end {end.isoformat()} is before start {start.isoformat()}")

    records: list[DailyRecord] = []
    if ORIGIN_DYNAMODB in origins:
        records.extend(
            scan_dynamodb(table=table, client=client, serial_default=serial)
        )
    if ORIGIN_LEGACY_JSON in origins:
        records.extend(load_legacy_json(legacy_path, serial=serial))

    in_range = [r for r in records if start <= r.local_day <= end]
    log.info(
        "backfill_records_collected",
        start=start.isoformat(),
        end=end.isoformat(),
        total=len(records),
        in_range=len(in_range),
        dynamodb=sum(1 for r in in_range if r.origin == ORIGIN_DYNAMODB),
        legacy_json=sum(1 for r in in_range if r.origin == ORIGIN_LEGACY_JSON),
    )
    return in_range


def _month_start(local_day: date) -> date:
    """First LOCAL day of the month — the key ``s3io.daily_key`` is built from."""
    return local_day.replace(day=1)


def group_by_month(
    records: Iterable[DailyRecord],
) -> dict[date, list[DailyRecord]]:
    """Records grouped by the first day of their LOCAL month, in date order."""
    grouped: dict[date, list[DailyRecord]] = {}
    for record in sorted(records, key=lambda r: r.sort_key):
        grouped.setdefault(_month_start(record.local_day), []).append(record)
    return dict(sorted(grouped.items()))


# --------------------------------------------------------------- the writer


@dataclass(frozen=True, slots=True)
class MonthResult:
    """Outcome of regenerating one monthly ``energy/daily`` object."""

    month_start: date
    #: The S3 key this month occupies when a bucket is configured, and the one it
    #: WOULD occupy otherwise.
    key: str
    #: The local Parquet file, which is always written.
    path: str
    #: Rows in the object that was written (or would have been, on a dry run).
    rows: int
    #: Distinct LOCAL days the backfill sources contributed.
    days: int
    #: Rows that survived dedupe, by origin (``dynamodb``/``legacy_json``/``existing``).
    rows_by_origin: Mapping[str, int]
    #: Rows the existing object held before this run.
    existing_rows: int
    written: bool


def _existing_rows(
    destination: dailystore.MonthDestination,
) -> list[model.Observation]:
    """Rows the month already holds, from every configured destination."""
    return dailystore.existing_rows(destination)


def build_month_table(
    records: Sequence[DailyRecord],
    existing: Sequence[model.Observation] = (),
) -> tuple[pa.Table, dict[str, int]]:
    """Merge backfill rows over existing rows into the month's Arrow table.

    Concatenation order **is** the precedence policy (see the module docstring):
    DynamoDB, then legacy JSON, then whatever the object already held. The dedupe
    inside :func:`energy_capture.model.observations_to_table` keeps the first
    occurrence of each canonical key, and the sort that follows is deterministic,
    so re-running writes byte-identical bytes.
    """
    pairs = records_to_observations(records)
    pairs.extend((ORIGIN_EXISTING, obs) for obs in existing)
    kept, counts = _dedupe_with_origin(pairs)
    table = model.observations_to_table(
        kept,
        dataset=model.Dataset.DAILY,
        # Already deduped above (with origin accounting); sorting is what is
        # wanted here, and `validate` still enforces "day-grain only".
        dedupe=False,
    )
    return table, counts


# ---------------------------------------------------------------- CLI entry


def run(
    *,
    start: date,
    end: date,
    bucket: str | None = None,
    out_dir: Path | str | None = None,
    client: BaseClient | None = None,
    dynamodb_client: BaseClient | None = None,
    table: str | None = None,
    legacy_path: Path | str | None = None,
    serial: str | None = None,
    origins: Sequence[str] = SOURCE_PRECEDENCE,
    store: StatusStore | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """``energycap backfill --start … --end …`` (PLAN.md §8).

    Reads the legacy DynamoDB table (``Scan`` only, never a write) and the old
    collector's JSON, maps both with §7.2's row mapping, prefers DynamoDB where
    they overlap, and regenerates every affected
    ``energy/daily/year=YYYY/bryant-{YYYYMM}.parquet`` completely — merged over
    whatever the object already held, so nothing outside the requested range is
    lost. Idempotent: a second run over the same range writes identical bytes.

    Every month is attempted even if an earlier one failed; :class:`BackfillError`
    is raised at the end if any did, so the CLI exits non-zero and the failure
    reaches ``status.json``.
    """
    # Local always, S3 as a mirror when one is configured (dailystore).
    target_bucket = bucket if bucket is not None else s3io.configured_bucket()
    status = store if store is not None else get_status_store()

    log.info(
        "backfill_start",
        start=start.isoformat(),
        end=end.isoformat(),
        bucket=target_bucket,
        origins=list(origins),
        dry_run=dry_run,
    )

    records = collect_records(
        start=start,
        end=end,
        origins=origins,
        table=table,
        client=dynamodb_client,
        legacy_path=legacy_path,
        serial=serial,
    )
    if not records:
        # A range with no legacy data is not an error and must not rewrite a
        # single object: an absent row is a truthful gap (CLAUDE.md rule 1).
        log.warning(
            "backfill_no_records", start=start.isoformat(), end=end.isoformat()
        )

    by_month = group_by_month(records)
    results: list[MonthResult] = []
    failures: list[tuple[date, BaseException]] = []

    for month_start, month_records in by_month.items():
        try:
            results.append(
                _backfill_month(
                    month_start,
                    month_records,
                    bucket=target_bucket,
                    out_dir=out_dir,
                    client=client,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:
            log.error(
                "backfill_month_failed",
                month=f"{month_start:%Y-%m}",
                error=f"{type(exc).__name__}: {exc}",
            )
            status.record_failure(
                STATUS_SECTION, exc, last_month_attempted=f"{month_start:%Y-%m}"
            )
            failures.append((month_start, exc))

    rows = sum(r.rows for r in results)
    totals: dict[str, int] = {}
    for result in results:
        for origin, count in result.rows_by_origin.items():
            totals[origin] = totals.get(origin, 0) + count

    summary: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "records": len(records),
        "records_dynamodb": sum(1 for r in records if r.origin == ORIGIN_DYNAMODB),
        "records_legacy_json": sum(
            1 for r in records if r.origin == ORIGIN_LEGACY_JSON
        ),
        "days": len({r.local_day for r in records}),
        "months": len(results),
        "months_failed": len(failures),
        "rows": rows,
        "rows_from_dynamodb": totals.get(ORIGIN_DYNAMODB, 0),
        "rows_from_legacy_json": totals.get(ORIGIN_LEGACY_JSON, 0),
        "rows_preserved": totals.get(ORIGIN_EXISTING, 0),
        "keys": [r.key for r in results],
        "dry_run": dry_run,
    }

    if failures:
        log.error("backfill_failed", **summary)
        months = ", ".join(f"{m:%Y-%m}" for m, _ in failures)
        raise BackfillError(
            f"{len(failures)} of {len(by_month)} month(s) failed to backfill: {months}"
        ) from failures[0][1]

    if results:
        status.record_success(
            STATUS_SECTION,
            last_range=f"{start.isoformat()}..{end.isoformat()}",
            months=len(results),
            rows=rows,
            days=summary["days"],
        )
    log.info("backfill_ok", **summary)
    return summary


def _backfill_month(
    month_start: date,
    records: Sequence[DailyRecord],
    *,
    bucket: str | None,
    out_dir: Path | str | None,
    client: BaseClient | None,
    dry_run: bool,
) -> MonthResult:
    """Regenerate one month, to every destination. Raises on any failure."""
    destination = dailystore.MonthDestination(
        month_start, out_dir=out_dir, bucket=bucket, client=client
    )
    existing = _existing_rows(destination)
    table, counts = build_month_table(records, existing)
    outcome = dailystore.write_month_table(table, destination, dry_run=dry_run)

    result = MonthResult(
        month_start=month_start,
        key=destination.key,
        path=str(destination.path),
        rows=table.num_rows,
        days=len({r.local_day for r in records}),
        rows_by_origin=counts,
        existing_rows=len(existing),
        written=not dry_run,
    )
    log.info(
        "backfill_month_ok",
        month=f"{month_start:%Y-%m}",
        key=destination.key,
        path=str(destination.path),
        s3=outcome.get("s3"),
        rows=result.rows,
        days=result.days,
        existing_rows=result.existing_rows,
        rows_from_dynamodb=counts.get(ORIGIN_DYNAMODB, 0),
        rows_from_legacy_json=counts.get(ORIGIN_LEGACY_JSON, 0),
        rows_preserved=counts.get(ORIGIN_EXISTING, 0),
        dry_run=dry_run,
    )
    return result
