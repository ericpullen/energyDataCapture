"""Bryant daily energy: the Carrier cloud -> ``energy/daily`` (PLAN.md §7.2).

The migrated behaviour of the old Lambda collector, landing in this pipeline's
canonical dataset instead of DynamoDB. Once a day at ~08:30 **local** the
scheduler fires this stage; it issues one ``getInfinityEnergy`` GraphQL query and
writes day-grain rows for the two days the Carrier cloud is willing to talk
about:

``day1``
    yesterday.
``day2``
    the day before yesterday — fetched **again**, as a revision/catch-up. The
    cloud restates a day after the fact, and re-fetching it is how a late
    correction (or a night this container spent down) reaches the archive. The
    overlap is not a problem: the canonical dedupe key
    ``(ts_utc, source, device_id, channel_id, metric)`` collapses it, and the
    fresher row wins because it is concatenated first (see :func:`build_month_table`).

Row mapping (PLAN.md §7.2), exactly the mapping ``stages/backfill.py`` uses for
the same data out of the legacy stores, so the two are interchangeable:

* ``source='bryant'``, ``device_id`` = ``CARRIER_SERIAL``,
* ``channel_id`` = the component **lowercased** (``eheat``, ``cooling``, ``fan``,
  ``fangas``, ``hpheat``, ``looppump``, ``gas``, ``reheat``),
* ``metric`` = ``kwh_day`` (``kWh``) or ``cost_day_usd`` (``USD``),
* ``ts_utc`` = **local midnight of the measured day converted to UTC**
  (:func:`energy_capture.timeutil.local_midnight_utc`), ``ts_local`` = that local
  midnight. US DST transitions happen at 02:00, so local midnight is neither
  skipped nor repeated: the conversion is unambiguous on both transition days,
  it simply picks up a different offset (``05:00Z`` on the spring-forward date,
  ``04:00Z`` on the fall-back date).

The casing trap
---------------
The component names are **camelCase inside ``energyPeriods``** (``eHeatKwh``,
``hPHeatDollars``, ``fanGasKwh``, ``loopPumpKwh``) and **lowercase inside
``energyConfig``** (``eheat``, ``hpheat``, ``fangas``, ``looppump``). No rule
that lowercases one into the other survives contact with ``hPHeat``, so
:data:`COMPONENTS` spells all three names out per component — config key, period
field, ``channel_id`` — and a period field or config key that is not in that
table is WARNed rather than dropped in silence.

Disabled components are absent, not zero
----------------------------------------
``energyConfig.<name>.enabled == false`` means the component **does not exist on
this system** — this house has no gas, no reheat, no loop pump. Writing a ``0``
for one would assert "the gas furnace burned nothing yesterday", which is
precisely the fabrication CLAUDE.md rule 1 forbids. So ``energyConfig`` is
fetched in the *same* query and a component that is not explicitly enabled emits
**no rows at all**. A component that *is* enabled and reports ``0`` does emit a
zero: that is a measurement, and recording what the API said is cardinal rule 2.

``gasKwh``
----------
PLAN.md §7.2 flags it: the field is named kWh but gas is very unlikely to be
kWh. This system is a heat pump with electric strips, so ``energyConfig.gas`` is
expected to be disabled and the field drops out on its own. If it is ever
enabled *and* nonzero we still write ``metric='kwh_day'`` verbatim and log a
WARN (``daily_gas_kwh_nonzero``) for a human to look at. Guessing a therm->kWh
conversion would be inventing data.

Never ``raw_30s``, never the spool
----------------------------------
Day-grain rows would poison the hourly rollup (CLAUDE.md rule 6). This module
names no ``raw_30s`` key builder and touches no :class:`SpoolDB`; everything it
writes goes through ``Dataset.DAILY`` and :func:`energy_capture.aws.s3io.daily_key`,
and ``model.observations_to_table`` independently rejects a non-day-grain metric
for that dataset.

Idempotency, and how this agrees with ``backfill``
--------------------------------------------------
Both stages write the *same* monthly objects
(``energy/daily/year=YYYY/bryant-{YYYYMM}.parquet``), so they follow one
convention:

1. the key comes from :func:`energy_capture.aws.s3io.daily_key`;
2. a month is **regenerated whole** — read the existing object, concatenate the
   stage's own rows *in front of* it, dedupe first-occurrence-wins on
   :data:`energy_capture.model.DEDUPE_KEY`, sort deterministically, write;
3. rows the stage has no opinion about (other days in the same month, days only
   the other stage ever saw) are carried through untouched.

Consequence: a month holding both backfilled history and freshly-fetched days
converges no matter which stage runs last. Each run re-reads what the other
wrote and preserves it; only the ``(day, channel, metric)`` cells the running
stage actually fetched are replaced. Two runs over the same inputs produce
byte-identical objects.

Range semantics
---------------
``run(start=…, end=…)`` takes LOCAL dates like every other stage. The Carrier
cloud only serves ``day1``/``day2``, so the range is a **filter**, not a query
window: whichever of those two dates falls inside ``[start, end]`` is written,
and any other date in the range is reported once at WARN
(``daily_range_unavailable``) and left as the gap it is. Historical dates come
from ``energycap backfill``, not from here.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa
from botocore.client import BaseClient

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import Settings, get_settings
from energy_capture.health import StatusStore, get_status_store
from energy_capture.logging import get_logger
from energy_capture.sources import carrier_auth

__all__ = [
    "COMPONENTS",
    "COMPONENT_BY_CONFIG_KEY",
    "CONFIG_NON_COMPONENT_KEYS",
    "ENERGY_QUERY",
    "METRIC_COST",
    "METRIC_KWH",
    "OPERATION_NAME",
    "PERIOD_FIELD",
    "PERIOD_OFFSET_DAYS",
    "STAGE",
    "STATUS_SECTION",
    "ComponentSpec",
    "DailyFetchError",
    "DayRows",
    "EnergyResponse",
    "MonthResult",
    "build_month_table",
    "coerce_number",
    "enabled_components",
    "fetch_energy",
    "is_enabled",
    "payload_to_days",
    "period_local_date",
    "period_to_observations",
    "run",
]

#: Log ``stage`` field.
STAGE = "daily"

#: ``status.json`` section this stage owns (PLAN.md §11: ``bryant_daily``).
STATUS_SECTION = "bryant_daily"

log = get_logger(STAGE)


class DailyFetchError(RuntimeError):
    """The Carrier response could not be turned into rows, or a month failed.

    Deliberately *not* a :class:`~energy_capture.sources.base.SourceTransientError`:
    the transport already classifies network/auth/throttle failures and raises
    those itself. This one means "the payload arrived and does not have the shape
    §7.2 describes", which is a data-integrity problem, not a retryable blip.
    """


# ----------------------------------------------------------------- the query

#: ``getInfinityEnergy`` — ported verbatim (field for field) from the old
#: collector's proven query, ``~/code/bryantDataCollector/carrier_energy.py``
#: ``CarrierEnergy.get_infinity_energy``. That query has been running daily in
#: production against this exact account, so its field set is *known* to resolve;
#: nothing was added to it speculatively.
#:
#: The two halves and their casing (PLAN.md §7.2, CLAUDE.md "API gotchas"):
#: ``energyConfig`` keys are lowercase (``eheat``, ``fangas``, ``hpheat``,
#: ``looppump``), ``energyPeriods`` fields are camelCase (``eHeatKwh``,
#: ``fanGasKwh``, ``hPHeatDollars``, ``loopPumpKwh``). ``energyConfig`` is
#: requested in the *same* round trip because a component's ``enabled`` flag is
#: what separates "structurally absent" from "zero".
ENERGY_QUERY = """
query getInfinityEnergy($serial: String!) {
  infinityEnergy(serial: $serial) {
    energyConfig {
      cooling { display enabled }
      eheat { display enabled }
      fan { display enabled }
      fangas { display enabled }
      gas { display enabled }
      hpheat { display enabled }
      looppump { display enabled }
      reheat { display enabled }
      hspf
      seer
    }
    energyPeriods {
      energyPeriodType
      eHeatDollars
      eHeatKwh
      coolingDollars
      coolingKwh
      fanDollars
      fanGasDollars
      fanGasKwh
      fanKwh
      hPHeatDollars
      hPHeatKwh
      loopPumpDollars
      loopPumpKwh
      gasDollars
      gasKwh
      reheatDollars
      reheatKwh
    }
  }
}
"""

#: GraphQL ``operationName`` for :data:`ENERGY_QUERY`.
OPERATION_NAME = "getInfinityEnergy"

#: Root field of the ``data`` object the query returns.
ROOT_FIELD = "infinityEnergy"

CONFIG_FIELD = "energyConfig"
PERIODS_FIELD = "energyPeriods"

#: Field naming the period inside an ``energyPeriods`` entry.
PERIOD_FIELD = "energyPeriodType"

METRIC_KWH = "kwh_day"
METRIC_COST = "cost_day_usd"


# ------------------------------------------------------------- the components


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    """One energy component, and the three different spellings it answers to.

    Attributes:
        channel_id: what we write — the component lowercased (PLAN.md §7.2).
            Identical to :attr:`config_key`; kept as its own name because it is
            the *canonical* identity and the config key is an API detail.
        config_key: the key inside ``energyConfig`` (lowercase).
        kwh_field: the ``energyPeriods`` field holding kWh (camelCase).
        dollars_field: the ``energyPeriods`` field holding dollars (camelCase).
    """

    channel_id: str
    config_key: str
    kwh_field: str
    dollars_field: str

    def field_for(self, metric: str) -> str:
        return self.kwh_field if metric == METRIC_KWH else self.dollars_field


#: **The mapping.** Eight rows, written out one by one and never derived by
#: lowercasing a field name: ``hPHeat`` -> ``hpheat``, ``fanGas`` -> ``fangas``
#: and ``loopPump`` -> ``looppump`` are not products of any rule that would also
#: survive a rename. Order is PLAN.md §7.2's. Pinned component-by-component in
#: ``tests/test_daily.py``.
COMPONENTS: tuple[ComponentSpec, ...] = (
    ComponentSpec("eheat", "eheat", "eHeatKwh", "eHeatDollars"),
    ComponentSpec("cooling", "cooling", "coolingKwh", "coolingDollars"),
    ComponentSpec("fan", "fan", "fanKwh", "fanDollars"),
    ComponentSpec("fangas", "fangas", "fanGasKwh", "fanGasDollars"),
    ComponentSpec("hpheat", "hpheat", "hPHeatKwh", "hPHeatDollars"),
    ComponentSpec("looppump", "looppump", "loopPumpKwh", "loopPumpDollars"),
    ComponentSpec("gas", "gas", "gasKwh", "gasDollars"),
    ComponentSpec("reheat", "reheat", "reheatKwh", "reheatDollars"),
)

COMPONENT_BY_CONFIG_KEY: dict[str, ComponentSpec] = {
    spec.config_key: spec for spec in COMPONENTS
}

#: Every ``energyPeriods`` field that maps to a row -> ``(spec, metric)``.
PERIOD_FIELD_INDEX: dict[str, tuple[ComponentSpec, str]] = {
    **{spec.kwh_field: (spec, METRIC_KWH) for spec in COMPONENTS},
    **{spec.dollars_field: (spec, METRIC_COST) for spec in COMPONENTS},
}

#: ``energyConfig`` keys that are not components (system efficiency ratings).
CONFIG_NON_COMPONENT_KEYS: frozenset[str] = frozenset({"hspf", "seer"})

#: ``energyPeriods`` keys that are not metrics.
PERIOD_NON_METRIC_KEYS: frozenset[str] = frozenset({PERIOD_FIELD})


# ------------------------------------------------------------------- periods

#: The two day-grain periods §7.2 collects, and how many days back each one is
#: from the day the fetch runs. ``day1`` is yesterday; ``day2`` is the day before
#: that, re-fetched as a revision. Nothing else in ``energyPeriods``
#: (``month1``, ``year1``, …) is a day, so nothing else is mapped.
PERIOD_OFFSET_DAYS: dict[str, int] = {"day1": 1, "day2": 2}


def period_local_date(period_type: str, *, today: date) -> date | None:
    """LOCAL date a period refers to, or ``None`` if it is not a day period.

    ``today`` is the local date the fetch runs on. The Carrier response carries
    no date of its own — ``energyPeriodType`` is relative to "now" — so this
    offset is the only thing that dates a row, exactly as the old collector did
    it (``lambda_collect_energy.get_date_for_period``). The job is scheduled at
    08:30 local, comfortably far from both midnights, so the arithmetic is never
    close to a boundary.
    """
    offset = PERIOD_OFFSET_DAYS.get(str(period_type))
    if offset is None:
        return None
    return today - timedelta(days=offset)


# ------------------------------------------------------------ value coercion


def is_enabled(value: Any) -> bool | None:
    """Interpret an ``energyConfig.<name>.enabled`` flag.

    Returns ``True``/``False``, or ``None`` when the flag is a value we do not
    recognise — which is treated as *not enabled* by :func:`enabled_components`,
    because emitting rows on a guess is how a phantom component gets into the
    archive. Carrier's scalars are frequently JSON strings and its "missing"
    sentinel is the literal string ``"None"``, so both are handled here.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value in (0, 1):
            return bool(value)
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "yes", "1", "on", "enabled"}:
            return True
        if text in {"false", "no", "0", "off", "disabled", "none", ""}:
            return False
    return None


def coerce_number(value: Any) -> float | None:
    """A period field -> ``float``, or ``None`` meaning **emit no row**.

    ``None`` covers everything that is not a real number: an absent field, JSON
    ``null``, Carrier's literal ``"None"`` string, a non-numeric string, and
    NaN/infinity. All of those are gaps, and a gap stays a gap (CLAUDE.md rule
    1) — never a zero. A genuine ``0`` is a number and survives as ``0.0``.

    Numeric *strings* are accepted because Carrier types most of its GraphQL
    scalars as ``String``; the old collector's captured responses show plain
    JSON numbers here, so both shapes are tolerated rather than assumed.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return None
        try:
            number = float(Decimal(text))
        except (InvalidOperation, ValueError, ArithmeticError):
            return None
    else:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


# -------------------------------------------------------------- the mapping


def enabled_components(
    config: Mapping[str, Any] | None, *, local_day: date | None = None
) -> list[ComponentSpec]:
    """The components this system actually has, per ``energyConfig``.

    A component is included **only** when its config entry says ``enabled`` is
    true. Absent entry, ``enabled: false``, an unreadable flag, or a config
    object that is not a mapping all mean "no rows for that component" — the
    difference between structurally absent and measured-zero is the whole point
    of fetching ``energyConfig`` (PLAN.md §7.2).

    Raises:
        DailyFetchError: if ``energyConfig`` is missing entirely. Without it
            every component is indistinguishable from a disabled one, and
            writing the periods anyway would fabricate rows for hardware this
            house does not have.
    """
    if not isinstance(config, Mapping):
        raise DailyFetchError(
            f"{ROOT_FIELD}.{CONFIG_FIELD} is missing from the Carrier response; "
            "without it a disabled component cannot be told from a measured "
            "zero, so no rows are written (PLAN.md §7.2)"
        )

    day = local_day.isoformat() if local_day is not None else None
    unknown = sorted(
        key
        for key in config
        if key not in COMPONENT_BY_CONFIG_KEY and key not in CONFIG_NON_COMPONENT_KEYS
    )
    if unknown:
        # Tripwire for a renamed component: it must never disappear quietly.
        log.warning("daily_unknown_config_component", components=unknown, local_date=day)

    out: list[ComponentSpec] = []
    for spec in COMPONENTS:
        entry = config.get(spec.config_key)
        if entry is None:
            log.warning(
                "daily_component_absent_from_config",
                component=spec.channel_id,
                local_date=day,
            )
            continue
        flag = entry.get("enabled") if isinstance(entry, Mapping) else None
        state = is_enabled(flag)
        if state is None:
            log.warning(
                "daily_component_enabled_unreadable",
                component=spec.channel_id,
                local_date=day,
                value=repr(flag)[:40],
            )
            continue
        if state:
            out.append(spec)
    return out


def period_to_observations(
    period: Mapping[str, Any],
    components: Sequence[ComponentSpec],
    *,
    serial: str,
    local_day: date,
) -> list[model.Observation]:
    """Map one ``energyPeriods`` entry to canonical rows for ``local_day``.

    ``components`` is the enabled set from :func:`enabled_components`; anything
    outside it emits nothing. Within it, a field that is absent or unreadable
    emits nothing either — only real numbers become rows.
    """
    ts_utc = timeutil.local_midnight_utc(local_day)
    _warn_unknown_period_fields(period, local_day=local_day)

    rows: list[model.Observation] = []
    for spec in components:
        for metric in (METRIC_KWH, METRIC_COST):
            field = spec.field_for(metric)
            raw = period.get(field)
            value = coerce_number(raw)
            if value is None:
                if field in period and raw is not None:
                    log.warning(
                        "daily_value_unreadable",
                        component=spec.channel_id,
                        field=field,
                        metric=metric,
                        local_date=local_day.isoformat(),
                        value=repr(raw)[:40],
                    )
                continue
            if spec.channel_id == "gas" and metric == METRIC_KWH and value != 0.0:
                # PLAN.md §7.2: the field says kWh but gas is probably not kWh,
                # and this system is a heat pump with electric strips. Record it
                # verbatim, flag it for a human, never guess a conversion.
                log.warning(
                    "daily_gas_kwh_nonzero",
                    local_date=local_day.isoformat(),
                    value=value,
                    detail=(
                        "energyConfig.gas is enabled and gasKwh is nonzero; the "
                        "field name says kWh but gas almost certainly is not. "
                        "Recorded verbatim as kwh_day — needs human review "
                        "(PLAN.md §7.2)."
                    ),
                )
            rows.append(
                model.make_observation(
                    ts_utc=ts_utc,
                    source=model.SOURCE_BRYANT,
                    device_id=serial,
                    channel_id=spec.channel_id,
                    metric=metric,
                    value=value,
                )
            )
    return rows


def _warn_unknown_period_fields(period: Mapping[str, Any], *, local_day: date) -> None:
    """WARN for a period field that is neither a mapped metric nor bookkeeping."""
    unknown = sorted(
        name
        for name in period
        if name not in PERIOD_FIELD_INDEX and name not in PERIOD_NON_METRIC_KEYS
    )
    if unknown:
        log.warning(
            "daily_unknown_period_field",
            fields=unknown,
            local_date=local_day.isoformat(),
        )


@dataclass(frozen=True, slots=True)
class DayRows:
    """The rows one ``energyPeriods`` entry produced, and where they came from."""

    local_day: date
    period_type: str
    observations: tuple[model.Observation, ...]

    @property
    def rows(self) -> int:
        return len(self.observations)


def payload_to_days(
    payload: Mapping[str, Any],
    *,
    serial: str,
    today: date,
    wanted: Iterable[date] | None = None,
) -> list[DayRows]:
    """Turn one ``infinityEnergy`` object into per-day rows (PLAN.md §7.2).

    Args:
        payload: the ``infinityEnergy`` object (``energyConfig`` +
            ``energyPeriods``).
        serial: ``CARRIER_SERIAL`` — the ``device_id`` of every row.
        today: the LOCAL date the fetch is running on; ``day1``/``day2`` are
            dated relative to it.
        wanted: restrict to these LOCAL dates (the ``--start/--end`` filter).
            ``None`` keeps both days.

    Returns:
        One :class:`DayRows` per day period, in ``day1``-then-``day2`` order
        (which is the precedence order: ``day1`` is the fresher statement of a
        date, and only ever collides with ``day2`` across *different* runs).
    """
    periods = payload.get(PERIODS_FIELD)
    if not isinstance(periods, Sequence) or isinstance(periods, (str, bytes)):
        raise DailyFetchError(
            f"{ROOT_FIELD}.{PERIODS_FIELD} is missing or is not a list in the "
            "Carrier response"
        )

    components = enabled_components(payload.get(CONFIG_FIELD))
    log.info(
        "daily_components_enabled",
        enabled=[spec.channel_id for spec in components],
        disabled=[
            spec.channel_id for spec in COMPONENTS if spec not in components
        ],
    )

    keep = set(wanted) if wanted is not None else None
    seen_periods: list[str] = []
    out: list[DayRows] = []
    for entry in periods:
        if not isinstance(entry, Mapping):
            continue
        period_type = str(entry.get(PERIOD_FIELD) or "")
        seen_periods.append(period_type)
        local_day = period_local_date(period_type, today=today)
        if local_day is None:
            # month1/year1/... are aggregates over a span, not a day. They have
            # no place in a day-grain dataset and are silently ignored.
            continue
        if keep is not None and local_day not in keep:
            log.info(
                "daily_period_out_of_range",
                period=period_type,
                local_date=local_day.isoformat(),
            )
            continue
        observations = period_to_observations(
            entry, components, serial=serial, local_day=local_day
        )
        out.append(
            DayRows(
                local_day=local_day,
                period_type=period_type,
                observations=tuple(observations),
            )
        )

    out.sort(key=lambda d: PERIOD_OFFSET_DAYS.get(d.period_type, 99))
    if not out:
        log.warning(
            "daily_no_day_periods",
            periods=seen_periods,
            today=today.isoformat(),
            detail=(
                "the response carried no day1/day2 period in range; no rows "
                "written (a gap stays a gap)"
            ),
        )
    return out


# --------------------------------------------------------------- the fetch


@dataclass(frozen=True, slots=True)
class EnergyResponse:
    """One successful ``getInfinityEnergy`` call.

    Attributes:
        payload: the ``infinityEnergy`` object (``energyConfig`` +
            ``energyPeriods``).
        status_fields: the transport's counters for ``status.json`` — throttle
            state, token expiry, grant counts. Never a credential.
    """

    payload: Mapping[str, Any]
    status_fields: Mapping[str, Any]


async def fetch_energy(
    *,
    serial: str,
    client: carrier_auth.CarrierGraphQLClient | None = None,
    settings: Settings | None = None,
) -> EnergyResponse:
    """Issue :data:`ENERGY_QUERY` and return the ``infinityEnergy`` object.

    Auth, token caching, the 401 ladder, ``Retry-After`` handling and the
    connection pool all live in :mod:`energy_capture.sources.carrier_auth`; this
    function only names the query. When ``client`` is omitted a stack is built
    from the environment and closed again before returning — the daily fetch
    runs once a day, so a short-lived pool is right, and the on-disk token cache
    is what keeps that from costing an Okta round trip.

    Raises:
        SourceAuthError / SourceTransientError: from the transport (a GraphQL
            ``errors`` array, a 5xx, a throttle) — the caller emits no rows.
        DailyFetchError: the query succeeded but carried no ``infinityEnergy``.
    """
    owns = client is None
    if client is None:
        _, client = carrier_auth.carrier_stack_from_settings(settings)
    try:
        data = await client.query(
            ENERGY_QUERY,
            variables={"serial": serial},
            operation_name=OPERATION_NAME,
        )
        fields = client.status_fields()
    finally:
        if owns:
            await client.close()

    payload = data.get(ROOT_FIELD)
    if not isinstance(payload, Mapping):
        raise DailyFetchError(
            f"Carrier {OPERATION_NAME} returned no {ROOT_FIELD} object for the "
            "requested serial"
        )
    return EnergyResponse(payload=dict(payload), status_fields=fields)


def _fetch_sync(*, serial: str, settings: Settings | None = None) -> EnergyResponse:
    """:func:`fetch_energy` from synchronous code.

    ``run()`` is synchronous because that is what the CLI and the scheduler both
    expect (``runtime._job_bryant_daily`` hands it to ``asyncio.to_thread``, so
    there is no running loop to clash with).
    """
    return asyncio.run(fetch_energy(serial=serial, settings=settings))


# ---------------------------------------------------------- the month writer


def _month_start(local_day: date) -> date:
    """First LOCAL day of the month — what ``s3io.daily_key`` is built from."""
    return local_day.replace(day=1)


def _existing_rows(
    bucket: str, key: str, *, client: BaseClient | None
) -> list[model.Observation]:
    """Rows already in the monthly object, or ``[]`` when it does not exist."""
    if not s3io.key_exists(bucket, key, client=client):
        return []
    table = s3io.read_table(bucket, key, client=client)
    return model.table_to_observations(table, dataset=model.Dataset.DAILY)


def build_month_table(
    fetched: Sequence[model.Observation],
    existing: Sequence[model.Observation] = (),
) -> pa.Table:
    """Merge freshly-fetched rows over the month's existing rows.

    Concatenation order **is** the precedence policy: what we just fetched comes
    first, so a ``day2`` revision replaces the ``day1`` value written for that
    date on the previous run, and everything the object already held that this
    run has no opinion about (other days, backfilled history) is carried through
    untouched. The dedupe inside
    :func:`energy_capture.model.observations_to_table` keeps the first occurrence
    of each :data:`~energy_capture.model.DEDUPE_KEY` and the sort that follows is
    deterministic, so a re-run writes byte-identical bytes.

    This is the same merge ``stages/backfill.py`` performs on the same objects —
    see the module docstring for why that makes the two stages converge.
    """
    rows = list(fetched) + list(existing)
    return model.observations_to_table(rows, dataset=model.Dataset.DAILY)


@dataclass(frozen=True, slots=True)
class MonthResult:
    """Outcome of regenerating one monthly ``energy/daily`` object."""

    month_start: date
    key: str
    #: Rows in the object that was written (or would have been, on a dry run).
    rows: int
    #: Rows this fetch contributed (before dedupe against the existing object).
    fetched_rows: int
    #: Rows the existing object held before this run.
    existing_rows: int
    #: LOCAL days this run wrote into the month.
    days: tuple[date, ...]
    written: bool


def _write_month(
    month_start: date,
    fetched: Sequence[model.Observation],
    days: Sequence[date],
    *,
    bucket: str,
    client: BaseClient | None,
    dry_run: bool,
) -> MonthResult:
    """Regenerate one monthly object whole. Raises on any failure."""
    key = s3io.daily_key(month_start, source=model.SOURCE_BRYANT)
    existing = _existing_rows(bucket, key, client=client)
    table = build_month_table(fetched, existing)

    if not dry_run:
        s3io.write_table_atomic(table, bucket, key, client=client)

    result = MonthResult(
        month_start=month_start,
        key=key,
        rows=table.num_rows,
        fetched_rows=len(fetched),
        existing_rows=len(existing),
        days=tuple(days),
        written=not dry_run,
    )
    log.info(
        "daily_month_ok",
        month=f"{month_start:%Y-%m}",
        key=key,
        rows=result.rows,
        fetched_rows=result.fetched_rows,
        existing_rows=result.existing_rows,
        days=[d.isoformat() for d in days],
        dry_run=dry_run,
    )
    return result


# ------------------------------------------------------------------- stage


def run(
    *,
    start: date,
    end: date,
    bucket: str | None = None,
    client: BaseClient | None = None,
    serial: str | None = None,
    now: datetime | None = None,
    payload: Mapping[str, Any] | None = None,
    status: StatusStore | None = None,
    settings: Settings | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """``energycap fetch-daily --start … --end …`` (PLAN.md §7.2, §10).

    Fetches ``day1`` (yesterday) and ``day2`` (the day before, as a revision),
    keeps whichever of those LOCAL dates falls inside ``[start, end]``, and
    regenerates every monthly ``energy/daily`` object they touch — merged over
    whatever the object already held, so nothing outside this fetch is lost.

    Args:
        start: first LOCAL date to keep, inclusive.
        end: last LOCAL date to keep, inclusive.
        bucket: destination bucket; defaults to ``S3_BUCKET``.
        client: boto3 S3 client; defaults to the cached one.
        serial: Bryant system serial; defaults to ``CARRIER_SERIAL``.
        now: reference instant for dating ``day1``/``day2`` (tests).
        payload: an already-fetched ``infinityEnergy`` object — skips the network
            entirely. This is the seam the offline tests drive.
        status: ``status.json`` writer; defaults to the process-wide store.
        settings: override :class:`Settings` (tests).
        dry_run: map and merge everything, write nothing.

    Returns:
        A mapping of loggable fields (rows, days, months, keys), which the CLI
        folds into its ``stage_ok`` line.

    Raises:
        DailyFetchError: a malformed payload, or a month that failed to write.
        SourceAuthError / SourceTransientError: propagated from the transport.
    """
    if end < start:
        raise ValueError(f"end {end.isoformat()} is before start {start.isoformat()}")

    resolved = settings if settings is not None else get_settings()
    target_bucket = bucket if bucket is not None else s3io.default_bucket()
    device_id = serial if serial is not None else resolved.require("carrier_serial")
    store = status if status is not None else get_status_store()
    reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
    today = timeutil.local_date_of(reference)

    wanted = [d for d in timeutil.iter_local_dates(start, end)]
    available = {period_local_date(p, today=today) for p in PERIOD_OFFSET_DAYS}
    unavailable = [d for d in wanted if d not in available]
    if unavailable:
        # The cloud serves only day1/day2. Anything else in the range is not a
        # failure and is not fetched — it stays the gap it already was, and
        # `energycap backfill` is the documented way to fill history.
        log.warning(
            "daily_range_unavailable",
            local_dates=[d.isoformat() for d in unavailable],
            today=today.isoformat(),
            detail=(
                "the Carrier cloud serves only day1/day2 (yesterday and the day "
                "before); use `energycap backfill` for older dates"
            ),
        )

    log.info(
        "daily_start",
        start=start.isoformat(),
        end=end.isoformat(),
        today=today.isoformat(),
        bucket=target_bucket,
        device_id=device_id,
        dry_run=dry_run,
        fetched=payload is None,
    )

    status_fields: Mapping[str, Any] = {}
    try:
        if payload is None:
            response = _fetch_sync(serial=device_id, settings=settings)
            energy, status_fields = response.payload, response.status_fields
        else:
            energy = payload
        days = payload_to_days(
            energy, serial=device_id, today=today, wanted=wanted
        )
    except Exception as exc:
        log.error(
            "daily_fetch_failed",
            start=start.isoformat(),
            end=end.isoformat(),
            error=f"{type(exc).__name__}: {exc}",
        )
        _record(store, failure=exc, fields=status_fields)
        raise

    by_month: dict[date, list[DayRows]] = {}
    for day in days:
        by_month.setdefault(_month_start(day.local_day), []).append(day)

    results: list[MonthResult] = []
    failures: list[tuple[date, BaseException]] = []
    for month_start in sorted(by_month):
        group = by_month[month_start]
        fetched = [obs for day in group for obs in day.observations]
        try:
            results.append(
                _write_month(
                    month_start,
                    fetched,
                    [day.local_day for day in group],
                    bucket=target_bucket,
                    client=client,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:
            # Every month is attempted before anything is raised, so one bad
            # month cannot strand the other (DEVIATIONS.md #26).
            log.error(
                "daily_month_failed",
                month=f"{month_start:%Y-%m}",
                error=f"{type(exc).__name__}: {exc}",
            )
            failures.append((month_start, exc))

    summary: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "today": today.isoformat(),
        "device_id": device_id,
        "periods": [day.period_type for day in days],
        "days": [day.local_day.isoformat() for day in days],
        "rows": sum(day.rows for day in days),
        "months": len(results),
        "months_failed": len(failures),
        "keys": [r.key for r in results],
        "dates_unavailable": [d.isoformat() for d in unavailable],
        "dry_run": dry_run,
    }

    if failures:
        log.error("daily_failed", **summary)
        months = ", ".join(f"{m:%Y-%m}" for m, _ in failures)
        error = DailyFetchError(
            f"{len(failures)} of {len(by_month)} month(s) failed to write: {months}"
        )
        _record(store, failure=error, fields=status_fields, extra=summary)
        raise error from failures[0][1]

    _record(store, fields=status_fields, extra=summary)
    log.info("daily_ok", **summary)
    return summary


def _record(
    store: StatusStore | None,
    *,
    failure: BaseException | None = None,
    fields: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> None:
    """Reflect the run in ``status.json``'s ``bryant_daily`` section (PLAN.md §11).

    The transport's own counters (throttle state, token expiry, grant counts —
    never a credential) are merged into the same section rather than a new one,
    because the poller owns ``bryant_status`` and two writers on one section
    would fight (DEVIATIONS.md #20).
    """
    if store is None:
        return
    try:
        payload: dict[str, Any] = dict(fields or {})
        if extra is not None:
            payload.update(
                {
                    "last_days_fetched": list(extra.get("days", ())),
                    "rows": extra.get("rows", 0),
                    "months": extra.get("months", 0),
                }
            )
        if failure is not None:
            store.record_failure(STATUS_SECTION, failure, **payload)
        else:
            store.record_success(STATUS_SECTION, **payload)
    except Exception as exc:  # pragma: no cover - telemetry must not break a stage
        log.warning("status_update_failed", error=f"{type(exc).__name__}: {exc}")
