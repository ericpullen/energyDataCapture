"""Bryant Evolution / Carrier Infinity **status** poller (PLAN.md §7.3, §7.4).

The 30-second companion to the Leviton electrical data: what the HVAC system is
*doing* (zone temperatures, setpoints, humidity, fan, operating mode, compressor
stage, blower RPM/CFM) so that energy can be correlated with state. The once-a-day
*energy* fetch (§7.2) is a different code path and lives in ``stages/daily.py``;
this module never touches it.

Layout, outside-in:

* :class:`BryantStatusSource` — the :class:`~energy_capture.sources.base.Source`
  ``stages/poller.py`` drives. It owns the GraphQL operation choice, the row
  mapping (§7.3) and the poll-cycle error policy. It contains **no auth code**:
  tokens, refresh, retries, 429 backoff and the 401 ladder all live in
  ``sources/carrier_auth.py``.
* :class:`SystemStatus` / :class:`ZoneStatus` — our own row-shaped view of one
  ``InfinityStatus`` payload. Every Carrier quirk (numbers as strings, the
  literal string ``"None"`` for null, per-zone garbage on disabled zones) is
  absorbed in :meth:`SystemStatus.from_payload`, so the mapping functions — and
  their tests — deal only in ``float | None``.
* :data:`MODE_CODES` / :data:`STAGE_CODES` / :data:`FAN_CODES` — the
  **append-only** enum tables of §7.3/§15.9.

The six traps this module exists to survive
-------------------------------------------

**1. Every numeric field is a JSON string.** ``oat`` arrives as ``"30"``,
``statpress`` as ``"1.399999976158142"``, ``htsp`` as either ``"74"`` or
``"60.0"``. Everything is parsed with :func:`float`, never :func:`int`.

**2. Missing values arrive as the literal string ``"None"``, not JSON null.** A
naive truthiness check passes it and a naive ``float()`` raises. :func:`_clean`
treats ``"None"`` (any case), ``""`` and JSON ``null`` identically: no value,
therefore **no row** (CLAUDE.md rule 1).

**3. A disabled zone still carries plausible-looking garbage.** In the reference
capture zone 2 reports ``rh: "34"``, ``htsp: "60.0"``, ``clsp: "80.0"`` and even
``zoneconditioning: "active_heat"`` — for a zone that does not physically exist.
Only ``rt`` is honestly ``"None"``. Filtering on nullness alone would fabricate
~80k rows a day. The only correct filter, and the one both reference
implementations use, is ``zone["enabled"] == "on"`` — a strict positive test, so
``"off"``, ``"None"``, ``None`` and a missing key all exclude the zone.

**4. The temperature unit is data, not a constant.** ``status.cfgem`` is ``"F"``
or ``"C"`` and governs ``rt``/``htsp``/``clsp``. Celsius readings are converted
to °F (the metric names and :data:`~energy_capture.model.UNIT_DEGF` are
Fahrenheit); if ``cfgem`` is absent or unrecognised, **no temperature row is
emitted at all** rather than a number labelled with a unit we cannot justify.

**5. ``humlvl``/``filtrlvl``/``uvlvl`` are not readings.** They are consumable
*used* percentages, not humidity/filter/UV measurements. Room humidity is zone
``rh`` and nothing else. They are requested (for a future live inspection) and
deliberately not mapped.

**6. ``odu.opstat`` has two shapes, and the hardware picks.** A staged compressor
reports a word (``"off"``/``"low"``/``"high"``) and a variable-capacity one
reports a 0-100 capacity percentage (``"35"``). They are two different
measurements and get two different metrics — ``stage`` (enum) and ``stage_pct``
(pct) — so the metric name always tells a reader which one a row is. Both paths
stay live for the life of the archive, at most one emits per cycle, and
:data:`STAGE_CODES` is never renumbered to accommodate the other. See
:meth:`BryantStatusSource._add_stage` and DEVIATIONS.md #59.

Unverified fields
-----------------

The research behind this module distinguishes fields observed in a real captured
response from fields that merely exist in the introspected schema. The second
category was *requested* so a live call could be inspected, and not mapped until
one had been.

**That capture happened on 2026-08-17 and every one of those fields came back
populated**, so as of 2026-08-22 the compressor telemetry and the per-unit state
strings ARE mapped: ``odu.comprpm`` → ``compressor_rpm``, ``odu.oducoiltmp`` →
``outdoor_coil_temp_f``, ``idu.statpress`` → ``static_pressure``, the three
airflow fields → ``idu_cfm``/``idu_iducfm``/``odu_iducfm``, and ``oprstsmsg`` /
``odu.opmode`` / ``idu.opstat`` → ``op_status`` / ``odu_mode`` /
``idu_status`` with their own append-only tables.

Two things are deliberately still unmapped. ``zones[].damperposition``,
``occupancy``, ``hold``, ``currentActivity`` and ``zoneconditioning`` are real
but near-worthless on a single-zone install, and ``humlvl``/``filtrlvl``/``uvlvl``
remain consumable *used* percentages rather than readings (see trap 5). A field
whose shape is unverified still must not be given a schema.

Likewise the GraphQL operation itself: ``infinityStatus(serial:)`` is present in
the maintainer-committed introspection dump of the live endpoint but is called by
nobody in the wild. So the module keeps the reference-proven
``getInfinitySystems($userName)`` form as a fallback and switches to it
automatically, once, if the per-serial root field does not resolve
(:data:`OPERATION_STATUS` → :data:`OPERATION_SYSTEMS`).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger
from energy_capture.model import SOURCE_BRYANT, Observation
from energy_capture.sources.base import (
    BaseSource,
    DiscoveredChannel,
    DiscoveredDevice,
    Discovery,
    PollCycle,
    SourceAuthError,
    SourceTransientError,
)
from energy_capture.sources.carrier_auth import (
    CarrierAuthError,
    CarrierGraphQLClient,
    CarrierGraphQLError,
    graphql_client_from_settings,
)
from energy_capture.timeutil import now_utc

__all__ = [
    "ENUM_TABLES",
    "FAN_CODES",
    "IDU_STATUS_CODES",
    "ODU_MODE_CODES",
    "OP_STATUS_CODES",
    "MODE_CODES",
    "NULL_SENTINEL",
    "OPERATION_STATUS",
    "OPERATION_SYSTEMS",
    "STAGE_CODES",
    "STAGE_METRIC",
    "STAGE_PCT_MAX",
    "STAGE_PCT_METRIC",
    "STAGE_PCT_MIN",
    "STAGE_REPR_ENUM",
    "STAGE_REPR_PCT",
    "STAGE_SOURCE",
    "STATUS_QUERY",
    "STATUS_SECTION",
    "SYSTEM_CHANNEL",
    "SYSTEMS_QUERY",
    "TEMP_UNIT_C",
    "TEMP_UNIT_F",
    "BryantStatusSource",
    "SystemStatus",
    "ZoneStatus",
    "enum_decode_text",
    "stage_metric_for",
    "zone_channel_id",
]


# ============================================================================
# Enum encoding tables — APPEND-ONLY FOREVER (PLAN.md §7.3, §15.9)
# ============================================================================
#
# `mode`, `stage` and `fan` are stored as a small integer in `value` with
# `unit="enum"` (PLAN.md §3: the long schema has no string value column). These
# three tables are the *only* definition of what those integers mean, they are
# quoted verbatim into the Glue column comment for `value` and into the README,
# and `tests/test_bryant_status.py` pins every entry.
#
# THE RULES, which are not negotiable:
#
#   * Renumbering an existing entry silently rewrites the meaning of every row
#     ever archived. Years of history would change meaning with no diff in the
#     data. Never do it.
#   * New API strings are APPENDED with the next unused integer. Never reuse a
#     retired number.
#   * An API string that is not in the table logs a WARN and emits NO ROW — a
#     gap. Never a fallback bucket, never an invented number, never "unknown=99".
#
# Confidence, so a future editor knows what is evidence and what is a seed:
#
#   * `fan` is the only table taken from a closed, strictly-constructed enum in
#     the reference client (`off`/`low`/`med`/`high`). Note `"auto"` is NOT an
#     API value — it is a Home Assistant display label substituted for `"off"`.
#   * `mode` here is the *operating* mode reported by status. Its domain is
#     demonstrably open: the reference's own list (gasheat/electric/hpheat/
#     dehumidify) is contradicted by its own captured response, which says
#     "heat". Only "heat" is confirmed-observed. The rest are seeded from the
#     reference's code and from the user-selected-mode enum, and extended only
#     after a live observation.
#   * `stage` is the least certain of the three — see :data:`STAGE_SOURCE`.

#: ``status.mode`` — the operating mode, system scope.
MODE_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "off": 0,
        "heat": 1,
        "cool": 2,
        "auto": 3,
        "fanonly": 4,
        "hpheat": 5,
        "electric": 6,
        "gasheat": 7,
        "dehumidify": 8,
    }
)

#: ``odu.opstat`` — the outdoor unit's operating stage (see :data:`STAGE_SOURCE`).
STAGE_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "off": 0,
        "low": 1,
        "high": 2,
        "idle": 3,
        "dehumidify": 4,
    }
)

#: ``zones[].fan`` — the per-zone fan state. Closed enum, high confidence.
FAN_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "off": 0,
        "low": 1,
        "med": 2,
        "high": 3,
    }
)

#: ``oprstsmsg`` — the system's own one-word account of what it is doing.
#: Observed live: ``idle``. The rest are the vocabulary the reference clients
#: display; anything outside the table WARNs and emits no row, which is how the
#: real domain gets discovered rather than guessed. Append only, never renumber.
OP_STATUS_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "idle": 0,
        "cooling": 1,
        "heating": 2,
        "fanonly": 3,
        "defrost": 4,
        "dehumidify": 5,
        "off": 6,
    }
)

#: ``odu.opmode`` — what the OUTDOOR unit says it is doing, as distinct from
#: ``odu.opstat`` (its stage/capacity) and from ``mode`` (the system's intent).
#: Observed live: ``cooling``.
ODU_MODE_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "off": 0,
        "cooling": 1,
        "heating": 2,
        "defrost": 3,
        "dehumidify": 4,
        "idle": 5,
        # Both spellings are real: the 2026-08-16 capture says "cool" and the
        # 2026-08-17 one says "cooling". APPENDED at the next unused integers
        # rather than folded into 1/2 — renumbering an archived code is the one
        # thing this table may never do, and a synonym is cheaper than a lie.
        "cool": 6,
        "heat": 7,
    }
)

#: ``idu.opstat`` — the INDOOR unit's operating state. Observed live: ``off``
#: (the air handler idle while the compressor ran, which is itself worth having
#: recorded). Deliberately its own table rather than a reuse of
#: :data:`STAGE_CODES`: they are different fields on different hardware, and
#: sharing a table would tie their futures together.
IDU_STATUS_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "off": 0,
        "on": 1,
        "low": 2,
        "high": 3,
        "idle": 4,
    }
)

#: metric -> its append-only decode table. What ``build-dim``/Glue/README quote.
ENUM_TABLES: Final[Mapping[str, Mapping[str, int]]] = MappingProxyType(
    {
        "mode": MODE_CODES,
        "stage": STAGE_CODES,
        "fan": FAN_CODES,
        "op_status": OP_STATUS_CODES,
        "odu_mode": ODU_MODE_CODES,
        "idu_status": IDU_STATUS_CODES,
    }
)


def enum_decode_text(metric: str) -> str:
    """``"0=off, 1=heat, …"`` — the decode string for a Glue comment or README.

    Sorted by code, so the Glue column comment written by ``aws/glue.py`` is
    stable across runs and a renumber would show up as a diff there too.
    """
    table = ENUM_TABLES[metric]
    return ", ".join(f"{code}={name}" for name, code in sorted(table.items(), key=lambda kv: kv[1]))


# ============================================================================
# Constants
# ============================================================================

#: ``channel_id`` for system-scope metrics (PLAN.md §7.3's table).
SYSTEM_CHANNEL: Final[str] = "system"

#: ``status.json`` section this source owns. It is deliberately **not**
#: ``bryant_status``: DEVIATIONS.md #20 gives that section to ``stages/poller.py``
#: (last success, consecutive failures, channels seen) and a source that also
#: wrote it would double-count the failure counter. This section carries what
#: only this module can see — the token/throttle counters of §7.1 and the
#: effective cadence §7.3 asks for when the cloud throttles.
STATUS_SECTION: Final[str] = "bryant_auth"

#: ``status.json`` fields that must NOT decide whether the file is rewritten.
#:
#: Two kinds, one reason — writing ``status.json`` 2,880 times a day to say the
#: same thing is the same defect as logging the same WARN 2,880 times:
#:
#: * genuinely volatile transport state (a backoff countdown), and
#: * **monotonic per-cycle counters**. On a variable-capacity system every single
#:   cycle bumps ``numeric_stage_samples``/``stage_pct_rows``, and on a system
#:   with one unmapped enum word every cycle bumps ``unknown_enum_values``, so
#:   comparing them would make every cycle look like a change. The *conditions*
#:   those counters describe are carried by ``distinct_warnings`` and
#:   ``stage_representation``, which move only when something new happens; the
#:   counters themselves are still written out with every write.
_VOLATILE_STATUS_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "backoff_remaining_s",
        "throttled",
        "effective_interval_s",
        "unknown_enum_values",
        "numeric_stage_samples",
        "stage_pct_rows",
        "stage_enum_rows",
        "stage_pct_out_of_range",
    }
)

#: Carrier's null: the literal four-character string, not JSON ``null``.
NULL_SENTINEL: Final[str] = "none"

TEMP_UNIT_F: Final[str] = "F"
TEMP_UNIT_C: Final[str] = "C"

#: Which unit's ``opstat`` becomes the ``stage`` / ``stage_pct`` metric.
#:
#: PLAN.md §7.3 says "odu/idu operating stage" without choosing. The **outdoor**
#: unit is the compressor — the thing that draws the power the Leviton CTs
#: measure — so ``odu.opstat`` is what makes ``stage`` correlate with watts.
#: There is deliberately no fallback to ``idu.opstat``: silently swapping in a
#: different physical unit's state would make the same metric mean two things.
STAGE_SOURCE: Final[str] = "odu"

# --------------------------------------------------------------------------
# The two renderings of `odu.opstat` (PLAN.md §7.3, DEVIATIONS.md #59)
# --------------------------------------------------------------------------
#
# `odu.opstat` has two mutually exclusive shapes, decided by the hardware:
#
#   * a WORD on a single-/two-/multi-stage compressor ("off"/"low"/"high"/…) →
#     :data:`STAGE_METRIC`, an enum code from :data:`STAGE_CODES`;
#   * a 0-100 CAPACITY PERCENTAGE on a variable-capacity (Greenspeed/inverter)
#     compressor → :data:`STAGE_PCT_METRIC`, a real percentage measurement.
#
# The live system this pipeline was built for is the second kind: `odu.type` is
# `gs3ngiphp` and `opstat` reads e.g. `"35"`. Encoding that as an enum code would
# be a lie (35 is not a stage) and putting a percentage in a column documented as
# an enum would give one metric two incompatible meanings, so the two get two
# metric names. **The metric name is the representation tag**: a row's `metric`
# is the only thing a reader needs to know how to interpret `value`, and the
# canonical unit (`enum` vs `pct`) follows from it in `model.UNIT_FOR_METRIC`.
#
# Both paths stay live forever, and per cycle at most one of them emits:
#
#   * a system can report words at one time and numbers at another (a repaired,
#     replaced or firmware-updated outdoor unit), so an archive can legitimately
#     contain both metrics across its life — and, being different metric names,
#     the two never collide in the `(ts_utc, source, device_id, channel_id,
#     metric)` dedupe key and never average together in a rollup;
#   * :data:`STAGE_CODES` is untouched and stays APPEND-ONLY: `stage_pct` adds a
#     metric, it does not retire, renumber or reinterpret a single archived code.

#: The enum rendering of ``odu.opstat`` (a word).
STAGE_METRIC: Final[str] = "stage"

#: The percentage rendering of ``odu.opstat`` (a variable-capacity unit's 0-100
#: compressor capacity). ``unit="pct"``, ``channel_id="system"``.
STAGE_PCT_METRIC: Final[str] = "stage_pct"

#: Sanity bounds for :data:`STAGE_PCT_METRIC`. A "percentage" outside 0-100 is
#: not a percentage, so it is treated exactly like an unrecognised value: **no
#: row**, WARN, counted. It is deliberately *not* clamped — a clamped 100 is
#: indistinguishable from an observed 100 once archived, which is fabrication.
#: 0 is a real reading (compressor idle at 0% capacity), not a missing one.
STAGE_PCT_MIN: Final[float] = 0.0
STAGE_PCT_MAX: Final[float] = 100.0

#: Values of the ``representation`` field of ``bryant_stage_representation`` and
#: of ``status.json``'s ``stage_representation``.
STAGE_REPR_ENUM: Final[str] = "enum"
STAGE_REPR_PCT: Final[str] = "pct"

#: GraphQL operation names. :data:`OPERATION_STATUS` is the cheap per-serial
#: form; :data:`OPERATION_SYSTEMS` is the reference-proven fallback (§7.3).
OPERATION_STATUS: Final[str] = "getInfinityStatus"
OPERATION_SYSTEMS: Final[str] = "getInfinitySystems"


# The selection set is shared by both operations so they can never drift:
# `InfinitySystem.status` is the same `InfinityStatus` type as the one
# `infinityStatus(serial:)` returns, field for field. Every field below was
# machine-verified against the endpoint's introspection dump.
#
# Fields we request but do NOT map, on purpose: `filtrlvl`, `humlvl`, `uvlvl`
# (consumable *used* percentages, not readings), `vent`/`ventlvl`/`humid`/
# `vacatrunning`/`hold`/`currentActivity`/`zoneconditioning` (state we have no
# metric for yet), `utcTime`/`localTime`/`localTimeOffset` (the *server's*
# clock — used to detect a stale cached payload, never as `ts_utc`, which
# PLAN.md §6.5 defines as poll-completion time), and the `odu`/`idu`/zone
# telemetry whose population is unverified. Requesting them costs nothing and
# makes the first live response a complete artefact to inspect.
_STATUS_SELECTION: Final[str] = """
    utcTime
    localTime
    localTimeOffset
    isDisconnected
    cfgem
    mode
    oat
    filtrlvl
    humid
    humlvl
    uvlvl
    vent
    ventlvl
    vacatrunning
    oprstsmsg
    odu { type opstat opmode iducfm blwrpm comprpm oducoiltmp }
    idu { type opstat cfm iducfm blwrpm statpress coiltemp }
    zones {
      id
      name
      enabled
      rt
      rh
      fan
      htsp
      clsp
      hold
      currentActivity
      zoneconditioning
      damperposition
      occupancy
    }
"""

#: The per-serial status query (PLAN.md §7.3). Schema-verified, but executed by
#: nobody in the open-source ecosystem — hence :data:`SYSTEMS_QUERY`.
STATUS_QUERY: Final[str] = (
    "query %s($serial: String!) {\n  infinityStatus(serial: $serial) {%s  }\n}\n"
    % (OPERATION_STATUS, _STATUS_SELECTION)
)

#: The reference-proven fallback: status arrives as a sub-object of the
#: user's system list, keyed on **userName, not serial**, and the caller filters
#: on ``profile.serial``. The giant ``config { }`` block the reference also
#: requests is omitted — we would throw it away 2,880 times a day.
SYSTEMS_QUERY: Final[str] = (
    "query %s($userName: String!) {\n"
    "  infinitySystems(userName: $userName) {\n"
    "    profile { serial name model brand idutype odutype }\n"
    "    status {%s    }\n"
    "  }\n"
    "}\n" % (OPERATION_SYSTEMS, _STATUS_SELECTION)
)


# ============================================================================
# Parsing helpers — Carrier's strings-and-"None" dialect
# ============================================================================


def zone_channel_id(zone_id: str | int) -> str:
    """``zone_{n}`` from the API's own zone ``id`` (PLAN.md §7.4).

    The ``id`` is the identity, never the list position: the REST payload
    delivers it as a string (``"1"``) and the websocket as an int (``1``), so it
    is coerced with :func:`str` — but it is never re-derived from an index,
    because a future payload that omits a zone would silently renumber every
    channel after it.
    """
    return f"zone_{str(zone_id).strip()}"


def _clean(value: Any) -> Any:
    """Collapse Carrier's three spellings of "missing" into ``None``.

    JSON ``null``, the empty string and the literal string ``"None"`` (any case)
    all mean the same thing: the API has no value. This is the single
    highest-risk trap in the payload — ``float("None")`` raises and
    ``if value:`` passes.
    """
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == NULL_SENTINEL:
            return None
        return text
    return value


def _as_float(value: Any) -> float | None:
    """Parse a Carrier scalar as a float, or ``None`` — never a zero.

    :func:`float`, never :func:`int`: ``htsp`` appears as both ``"74"`` and
    ``"60.0"`` in the same captured response, and ``int("60.0")`` raises.
    """
    cleaned = _clean(value)
    if cleaned is None or isinstance(cleaned, bool):
        return None
    try:
        number = float(cleaned)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _as_text(value: Any) -> str | None:
    """A non-empty, non-``"None"`` string, or ``None``."""
    cleaned = _clean(value)
    if cleaned is None:
        return None
    text = str(cleaned).strip()
    return text or None


def _as_bool(value: Any) -> bool:
    """``isDisconnected`` is a real JSON boolean, but be liberal anyway."""
    cleaned = _clean(value)
    if isinstance(cleaned, bool):
        return cleaned
    if isinstance(cleaned, str):
        return cleaned.strip().lower() in {"true", "yes", "on", "1"}
    if isinstance(cleaned, (int, float)):
        return bool(cleaned)
    return False


def _mapping(value: Any) -> Mapping[str, Any]:
    """A nested object, or an empty mapping (``odu``/``idu`` can be absent)."""
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    """A JSON list, or an empty tuple. A string is not a list of zones."""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return ()


def _looks_numeric(text: str) -> bool:
    """True when an enum-shaped field actually holds a number.

    On a variable-capacity outdoor unit ``opstat`` is a 0–100 capacity
    percentage string rather than a word — see :meth:`BryantStatusSource._encode`.
    """
    try:
        float(text)
    except (TypeError, ValueError):
        return False
    return True


def stage_metric_for(raw: Any) -> str | None:
    """Which metric an ``odu.opstat`` value renders as, without emitting anything.

    ``stage_pct`` for a number, ``stage`` for a word the append-only table knows,
    ``None`` for a missing value or a word it does not (which emits no row at
    all). This is the classifier :meth:`BryantStatusSource._add_stage` branches
    on, exposed so ``energycap discover`` can answer "does this system produce
    ``stage`` or ``stage_pct``?" from one response, before any row exists.

    It reports the *rendering*, not the outcome: a number outside 0-100 is still
    a ``stage_pct``-shaped field, and is still refused as a row (see
    :data:`STAGE_PCT_MIN` / :data:`STAGE_PCT_MAX`).
    """
    text = _as_text(raw)
    if text is None:
        return None
    lowered = text.lower()
    if _looks_numeric(lowered):
        return STAGE_PCT_METRIC
    return STAGE_METRIC if lowered in STAGE_CODES else None


def _c_to_f(celsius: float) -> float:
    """Celsius → Fahrenheit. The metric names and ``unit='degF'`` are Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32.0


# ============================================================================
# Readings
# ============================================================================


@dataclass(frozen=True, slots=True)
class ZoneStatus:
    """One entry of ``status.zones`` — already cleaned of the ``"None"`` dialect.

    ``enabled`` is the *only* thing that decides whether this zone exists. All
    eight physically-possible zones are returned by the API whether or not they
    are installed, and the seven phantoms carry plausible numbers.

    Temperatures are in whatever ``status.cfgem`` says; conversion to °F happens
    in the mapping, which is the only place that knows the unit.
    """

    zone_id: str
    enabled: bool
    name: str | None = None
    indoor_temp: float | None = None
    humidity_pct: float | None = None
    setpoint_heat: float | None = None
    setpoint_cool: float | None = None
    fan: str | None = None
    hold: str | None = None
    current_activity: str | None = None
    conditioning: str | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ZoneStatus:
        return cls(
            zone_id=str(_as_text(payload.get("id")) or ""),
            # Strict positive test: "off", "None", null and a missing key all
            # mean the zone does not exist this cycle.
            enabled=(_as_text(payload.get("enabled")) or "").lower() == "on",
            name=_as_text(payload.get("name")),
            indoor_temp=_as_float(payload.get("rt")),
            humidity_pct=_as_float(payload.get("rh")),
            setpoint_heat=_as_float(payload.get("htsp")),
            setpoint_cool=_as_float(payload.get("clsp")),
            fan=_as_text(payload.get("fan")),
            hold=_as_text(payload.get("hold")),
            current_activity=_as_text(payload.get("currentActivity")),
            conditioning=_as_text(payload.get("zoneconditioning")),
        )

    @property
    def channel_id(self) -> str:
        return zone_channel_id(self.zone_id)

    @property
    def sort_key(self) -> tuple[int, str]:
        """Numeric-first ordering so ``zone_10`` follows ``zone_9``."""
        try:
            return (int(self.zone_id), "")
        except (TypeError, ValueError):
            return (1 << 30, self.zone_id)


@dataclass(frozen=True, slots=True)
class SystemStatus:
    """One ``InfinityStatus`` payload as this pipeline sees it (PLAN.md §7.3)."""

    device_id: str
    disconnected: bool = False
    #: ``"F"`` / ``"C"`` / ``None``. Governs ``rt``/``htsp``/``clsp``.
    temp_unit: str | None = None
    mode: str | None = None
    #: ``oat``, in °F *if* :attr:`temp_unit` is ``"F"`` (see the mapping).
    outdoor_temp: float | None = None
    odu_opstat: str | None = None
    idu_opstat: str | None = None
    #: ``odu.type`` — diagnostic only, never a metric. It is what *predicts*
    #: which rendering of ``opstat`` to expect (``gs*``/``varcap*`` = variable
    #: capacity = a percentage), so ``energycap discover`` shows it; nothing
    #: branches on it, because the value itself is the evidence.
    odu_type: str | None = None
    blower_rpm: float | None = None
    #: The reference client's fallback pick between ``idu.cfm`` and
    #: ``odu.iducfm``. Kept for continuity; the three explicit fields below are
    #: what a query should prefer, because they say which number they are.
    cfm: float | None = None
    #: ``odu.comprpm`` — compressor speed.
    compressor_rpm: float | None = None
    #: ``odu.oducoiltmp``, in whatever ``cfgem`` says (converted like any temp).
    outdoor_coil_temp: float | None = None
    #: ``idu.statpress`` — static pressure across the air handler.
    static_pressure: float | None = None
    #: The three airflow numbers, unblended: ``idu.cfm``, ``idu.iducfm``,
    #: ``odu.iducfm``. They disagreed by more than 2x in the first live capture,
    #: so which one a reader is looking at matters.
    idu_cfm: float | None = None
    idu_iducfm: float | None = None
    odu_iducfm: float | None = None
    #: ``oprstsmsg`` — the system's own account of what it is doing.
    op_status: str | None = None
    #: ``odu.opmode`` — the outdoor unit's, distinct from its stage.
    odu_mode: str | None = None
    #: The **server's** clock. Recorded for staleness diagnosis only; ``ts_utc``
    #: is poll-completion time per PLAN.md §6.5.
    server_utc_time: str | None = None
    zones: tuple[ZoneStatus, ...] = ()

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any], *, device_id: str) -> SystemStatus:
        odu = _mapping(payload.get("odu"))
        idu = _mapping(payload.get("idu"))
        zones = tuple(
            ZoneStatus.from_payload(zone)
            for zone in _sequence(payload.get("zones"))
            if isinstance(zone, Mapping)
        )
        unit = (_as_text(payload.get("cfgem")) or "").upper() or None
        return cls(
            device_id=device_id,
            disconnected=_as_bool(payload.get("isDisconnected")),
            temp_unit=unit if unit in (TEMP_UNIT_F, TEMP_UNIT_C) else None,
            mode=_as_text(payload.get("mode")),
            outdoor_temp=_as_float(payload.get("oat")),
            odu_opstat=_as_text(odu.get("opstat")),
            idu_opstat=_as_text(idu.get("opstat")),
            odu_type=_as_text(odu.get("type")),
            blower_rpm=_as_float(idu.get("blwrpm")),
            # The reference's own fallback chain: the indoor unit reports CFM,
            # and on some systems only the outdoor unit's `iducfm` is populated.
            cfm=_first_number(idu.get("cfm"), odu.get("iducfm")),
            compressor_rpm=_as_float(odu.get("comprpm")),
            outdoor_coil_temp=_as_float(odu.get("oducoiltmp")),
            static_pressure=_as_float(idu.get("statpress")),
            idu_cfm=_as_float(idu.get("cfm")),
            idu_iducfm=_as_float(idu.get("iducfm")),
            odu_iducfm=_as_float(odu.get("iducfm")),
            op_status=_as_text(payload.get("oprstsmsg")),
            odu_mode=_as_text(odu.get("opmode")),
            server_utc_time=_as_text(payload.get("utcTime")),
            zones=zones,
        )

    @property
    def enabled_zones(self) -> tuple[ZoneStatus, ...]:
        """Zones that physically exist this cycle, in stable zone-id order."""
        live = (zone for zone in self.zones if zone.enabled and zone.zone_id)
        return tuple(sorted(live, key=lambda zone: zone.sort_key))

    @property
    def stage_raw(self) -> str | None:
        """The raw ``opstat`` behind ``stage``/``stage_pct`` (:data:`STAGE_SOURCE`).

        Whether it is a word or a percentage is hardware, not configuration —
        see :meth:`BryantStatusSource._add_stage`.
        """
        return self.odu_opstat if STAGE_SOURCE == "odu" else self.idu_opstat


def _first_number(*values: Any) -> float | None:
    """The first parseable number among ``values``; ``None`` if there is none."""
    for value in values:
        number = _as_float(value)
        if number is not None:
            return number
    return None


# ============================================================================
# The source
# ============================================================================


class BryantStatusSource(BaseSource):
    """The Bryant status :class:`~energy_capture.sources.base.Source` (§7.3).

    Lifecycle, driven by ``stages/poller.py``::

        source = BryantStatusSource(settings)
        await source.start()      # one discovery pass (zones)
        rows = await source.poll()  # every BRYANT_POLL_INTERVAL_S (floor 30s)
        await source.close()

    ``poll()`` returns the rows it genuinely observed, or raises
    :class:`~energy_capture.sources.base.SourceTransientError` /
    :class:`~energy_capture.sources.base.SourceAuthError` with **zero rows**
    produced, exactly as ``sources/base.py`` specifies. Every exception the
    transport can raise is already one of those two, so nothing unexpected
    reaches the loop.

    Injection points (tests only; production passes none of them): ``client``
    (anything with :class:`CarrierGraphQLClient`'s surface), ``device_id``,
    ``username``, ``operation`` and ``monotonic``.
    """

    name = SOURCE_BRYANT

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: CarrierGraphQLClient | Any = None,
        device_id: str | None = None,
        username: str | None = None,
        operation: str = OPERATION_STATUS,
        allow_fallback: bool = True,
        status_store: Any = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        resolved = settings if settings is not None else get_settings()
        super().__init__(
            poll_interval_s=resolved.bryant_poll_interval_s,
            # Zones are re-read from every poll response for free (see
            # `_remember_zones`), so a separate periodic discovery task would
            # only double the request rate against a cloud whose tolerance for
            # 30s polling is unproven (PLAN.md §7.3).
            discovery_interval_s=None,
        )
        if operation not in (OPERATION_STATUS, OPERATION_SYSTEMS):
            raise ValueError(
                f"unknown operation {operation!r}; expected "
                f"{OPERATION_STATUS!r} or {OPERATION_SYSTEMS!r}"
            )
        self._settings = resolved
        self._log = get_logger("bryant")
        self._monotonic = monotonic
        self._status_store = status_store

        # Credentials are demanded at the point of use, never at construction:
        # importing or building this source must not require an environment.
        self._client: Any = client
        self._owns_client = client is None
        self._device_id = device_id
        self._username = username

        self._operation = operation
        self._allow_fallback = bool(allow_fallback)

        self._consecutive_failures = 0
        self._disconnected = False
        #: (metric, raw string) already WARNed about. A 30s loop must not emit
        #: 2,880 identical lines a day for one unmapped enum value; the count is
        #: kept so the condition stays visible in ``status.json``.
        self._warned_enums: set[tuple[str, str]] = set()
        self._unknown_enums: dict[str, int] = {}
        #: How many cycles saw a numeric ``odu.opstat`` (in range or not) — the
        #: counter DEVIATIONS.md #59/#75 says to watch to identify the hardware.
        self._numeric_stage_samples = 0
        #: Rows actually emitted under each rendering, and the numeric samples
        #: refused for being outside 0-100.
        self._stage_pct_rows = 0
        self._stage_enum_rows = 0
        self._stage_pct_out_of_range = 0
        #: Which rendering the last emitted stage row used (``None`` until one
        #: is emitted). A change is a hardware/firmware event worth one log line.
        self._stage_representation: str | None = None
        self._stage_representation_changes = 0
        self._last_status_fields: dict[str, Any] | None = None
        #: True when the last thing written to ``status.json`` was a failure.
        #: The next success must be written through even if every comparable
        #: counter is unchanged — see :meth:`_publish_status`.
        self._status_failed = False
        self._zone_ids: tuple[str, ...] = ()

    # --------------------------------------------------------------- accessors
    @property
    def device_id(self) -> str:
        """``CARRIER_SERIAL`` — the ``device_id`` column for every row (§7.4)."""
        if self._device_id is None:
            self._device_id = self._require("carrier_serial")
        return self._device_id

    def _require(self, field: str) -> str:
        """``Settings.require``, but a missing value is an *auth* failure.

        Configuration is resolved at the point of use, which means it is resolved
        inside ``poll()``. Letting a bare ``RuntimeError`` out would reach the
        loop's "a bug inside a source" branch and log a traceback every 30
        seconds; a missing credential is an ordinary, well-understood auth
        condition that ``status.json`` and ``/healthz`` already know how to show.
        """
        try:
            return self._settings.require(field)
        except RuntimeError as exc:
            raise SourceAuthError(str(exc)) from None

    @property
    def consecutive_failures(self) -> int:
        """Failed poll cycles since the last success (PLAN.md §6.6's discipline)."""
        return self._consecutive_failures

    @property
    def operation(self) -> str:
        """The GraphQL operation currently in use (§7.3's swappable query)."""
        return self._operation

    @property
    def disconnected(self) -> bool:
        """Whether the last payload said the thermostat is offline."""
        return self._disconnected

    @property
    def unknown_enum_counts(self) -> Mapping[str, int]:
        """metric -> how many samples were dropped for an unmapped enum string."""
        return MappingProxyType(dict(self._unknown_enums))

    @property
    def stage_representation(self) -> str | None:
        """``"enum"``/``"pct"`` — which metric the last stage row used, if any."""
        return self._stage_representation

    @property
    def zone_ids(self) -> tuple[str, ...]:
        """Zone ids seen ``enabled == "on"`` in the most recent response."""
        return self._zone_ids

    @property
    def client(self) -> Any:
        """The shared GraphQL transport (``sources/carrier_auth.py``)."""
        return self._ensure_client()

    def _ensure_client(self) -> Any:
        if self._client is None:
            # Credentials are demanded here, never at construction: building the
            # source must not require an environment, so `energycap run` boots
            # and reports the condition rather than refusing to start
            # (DEVIATIONS.md #46). A missing one surfaces as an auth failure.
            try:
                self._client = graphql_client_from_settings(self._settings)
            except RuntimeError as exc:
                raise SourceAuthError(str(exc)) from None
            self._owns_client = True
        return self._client

    def _user_name(self) -> str:
        if self._username is None:
            self._username = self._require("carrier_username")
        return self._username

    def _status(self) -> Any:
        if self._status_store is None:
            from energy_capture.health import get_status_store

            self._status_store = get_status_store()
        return self._status_store

    # --------------------------------------------------------------- lifecycle
    async def close(self) -> None:
        """Release the transport — but only if this source created it.

        ``carrier_stack_from_settings`` is meant to be shared with the daily
        energy stage; closing a client we were handed would pull the pool out
        from under it.
        """
        if self._client is not None and self._owns_client:
            await self._client.close()
            self._client = None

    # --------------------------------------------------------------- discovery
    async def discover(self, *, force: bool = False) -> Discovery:
        """Enumerate the system and its **enabled** zones (PLAN.md §7.4, §9).

        Zones that are absent or ``enabled != "on"`` never appear — the seven
        phantom zones a single-zone install reports are not channels, and
        ``energycap discover`` must not offer them for mapping.
        """
        cached = self.cached_discovery
        if cached is not None and not force:
            return cached
        payload = await self._fetch_status()
        status = SystemStatus.from_payload(payload, device_id=self.device_id)
        return self._remember_zones(status, log_change=False)

    def _remember_zones(self, status: SystemStatus, *, log_change: bool = True) -> Discovery:
        """Refresh the discovery cache from a status payload we already have."""
        zones = status.enabled_zones
        zone_ids = tuple(zone.zone_id for zone in zones)
        changed = zone_ids != self._zone_ids
        if changed and log_change and self._zone_ids:
            # A zone flipping enabled on/off simply starts/stops rows — that is a
            # correct gap — but it changes the channel set, so say so once.
            self._log.info(
                "bryant_zone_set_changed",
                previous=list(self._zone_ids),
                current=list(zone_ids),
            )
        self._zone_ids = zone_ids

        devices = (
            DiscoveredDevice(
                source=self.name,
                device_id=status.device_id,
                kind="system",
                label="Bryant Evolution system",
                details={
                    "disconnected": status.disconnected,
                    "temperature_unit": status.temp_unit,
                    "operation": self._operation,
                    "zones_enabled": len(zones),
                    "zones_reported": len(status.zones),
                },
            ),
        )
        channels = [
            DiscoveredChannel(
                source=self.name,
                device_id=status.device_id,
                channel_id=SYSTEM_CHANNEL,
                kind="system",
                label="HVAC system (outdoor temp, mode, stage/stage_pct, blower)",
                details={
                    "mode": status.mode,
                    "stage_source": STAGE_SOURCE,
                    # What `odu.opstat` looks like on this house, so `discover`
                    # answers "does this system emit `stage` or `stage_pct`?"
                    # without a poll cycle and a log dive.
                    "odu_type": status.odu_type,
                    "odu_opstat": status.odu_opstat,
                    "stage_metric": stage_metric_for(status.stage_raw),
                },
            )
        ]
        for zone in zones:
            channels.append(
                DiscoveredChannel(
                    source=self.name,
                    device_id=status.device_id,
                    channel_id=zone.channel_id,
                    kind="zone",
                    # `zones[].name` is schema-present but unverified in a real
                    # response; channel_map.json is the source of truth for
                    # labels (PLAN.md §9), so this is a hint, never data.
                    label=zone.name,
                    details={
                        "zone_id": zone.zone_id,
                        "enabled": zone.enabled,
                        "currentActivity": zone.current_activity,
                        "zoneconditioning": zone.conditioning,
                    },
                )
            )
        return self._remember(
            Discovery(
                source=self.name,
                devices=devices,
                channels=tuple(channels),
                ts_utc=now_utc(),
            )
        )

    # ----------------------------------------------------------------- polling
    async def poll(self) -> list[Observation]:
        """One poll cycle → observations sharing a single ``ts_utc`` (§6.5).

        Failure policy mirrors ``sources/leviton.py``: the transport retries
        transient failures and runs the 401 ladder internally; whatever survives
        is counted here, logged **once per failed cycle**, and re-raised with
        zero rows produced. The loop never crashes and never reuses a reading.
        """
        try:
            payload = await self._fetch_status()
        except SourceAuthError as exc:
            self._consecutive_failures += 1
            self._log.error(
                "bryant_auth_failed",
                consecutive_failures=self._consecutive_failures,
                rows=0,
                error=str(exc),
            )
            self._publish_status(error=exc)
            raise
        except SourceTransientError as exc:
            self._consecutive_failures += 1
            self._log.warning(
                "bryant_poll_failed",
                consecutive_failures=self._consecutive_failures,
                rows=0,
                operation=self._operation,
                error=str(exc),
            )
            self._publish_status(error=exc)
            raise

        status = SystemStatus.from_payload(payload, device_id=self.device_id)
        self._remember_zones(status)

        # The response is complete: this is the instant every row is stamped
        # with (PLAN.md §6.5). All rows of the cycle therefore share one ts_utc.
        cycle = self.new_cycle(ts_utc=now_utc())
        if status.disconnected:
            # The cloud is telling us the thermostat is offline, so whatever is
            # in this payload is stale. Recording it would be fabrication with
            # extra steps: emit nothing and let the gap speak.
            if not self._disconnected:
                self._log.warning(
                    "bryant_system_disconnected",
                    device_id=status.device_id,
                    rows=0,
                    detail="payload is stale; emitting no rows (a gap stays a gap)",
                )
            self._disconnected = True
        else:
            if self._disconnected:
                self._log.info("bryant_system_reconnected", device_id=status.device_id)
            self._disconnected = False
            self._map_status(cycle, status)
        rows = cycle.finish()

        self._consecutive_failures = 0
        self._log.debug(
            "bryant_poll_ok",
            rows=len(rows),
            gaps=cycle.gaps,
            zones=len(status.enabled_zones),
            operation=self._operation,
            server_utc_time=status.server_utc_time,
        )
        self._publish_status()
        return rows

    # ------------------------------------------------------------- the fetch
    async def _fetch_status(self) -> Mapping[str, Any]:
        """Return one ``InfinityStatus`` object, whichever operation works.

        ``infinityStatus(serial:)`` is tried first: it returns exactly this
        system without the enormous ``config { }`` blob. It is schema-verified
        but has never been executed in the wild, so a GraphQL-level rejection or
        a ``null`` result falls back **once** to the reference-proven
        ``getInfinitySystems($userName)`` form and pins that choice for the
        process. Transport failures (5xx, 429, 401) are *not* fallback triggers —
        they say nothing about which field resolves.

        The awkward case, and the reason this is not a one-line ``except``:
        **permission-gating is the single likeliest way the per-serial field
        fails**, and a gateway that refuses a field answers ``200 OK`` with an
        ``errors`` array saying "not authorized". ``carrier_auth`` classifies
        that as a :class:`CarrierAuthError` — correct for a dead token, wrong
        here — so the fallback the whole module is built around would be
        defeated in exactly the case it exists for. The two are separated by
        :attr:`CarrierAuthError.errors`, which is populated only for a
        200-with-``errors`` body and empty for a transport 401/403: a rejected
        *field* falls back, a rejected *token* goes on raising into the auth
        ladder, which is the only thing that can fix it.
        """
        if self._operation == OPERATION_SYSTEMS:
            return await self._fetch_via_systems()

        rejection: Exception | None = None
        try:
            data = await self._query(
                STATUS_QUERY, {"serial": self.device_id}, OPERATION_STATUS
            )
            status = data.get("infinityStatus")
            if isinstance(status, Mapping) and status:
                return status
            reason = "infinityStatus resolved to null"
        except CarrierGraphQLError as exc:
            rejection = exc
            reason = str(exc)
        except CarrierAuthError as exc:
            if not exc.errors:
                # A bare 401/403: our token, not this field. Let the auth ladder
                # own it — switching queries would not help and would hide it.
                raise
            rejection = exc
            reason = str(exc)

        if not self._allow_fallback:
            # Reported as a *data* error, not an auth one, even when it arrived
            # as a GraphQL auth error: the field was refused, our token was not.
            raise CarrierGraphQLError(
                f"carrier {OPERATION_STATUS}: {reason}"
            ) from rejection

        self._log.warning(
            "bryant_status_query_fallback",
            reason=reason[:240],
            was=OPERATION_STATUS,
            now=OPERATION_SYSTEMS,
            field_rejected=isinstance(rejection, CarrierAuthError),
        )
        status = await self._fetch_via_systems()
        # Pinned only after the fallback actually worked, so a transient failure
        # of the fallback does not abandon the cheaper query forever.
        self._operation = OPERATION_SYSTEMS
        return status

    async def _fetch_via_systems(self) -> Mapping[str, Any]:
        """The reference-proven form: fetch every system, filter on serial."""
        data = await self._query(
            SYSTEMS_QUERY, {"userName": self._user_name()}, OPERATION_SYSTEMS
        )
        systems = data.get("infinitySystems")
        if not isinstance(systems, Sequence) or isinstance(systems, (str, bytes)):
            raise CarrierGraphQLError(
                f"carrier {OPERATION_SYSTEMS}: infinitySystems was not a list"
            )
        wanted = self.device_id
        for system in systems:
            if not isinstance(system, Mapping):
                continue
            serial = _as_text(_mapping(system.get("profile")).get("serial"))
            if serial == wanted:
                status = system.get("status")
                if isinstance(status, Mapping) and status:
                    return status
                raise CarrierGraphQLError(
                    f"carrier {OPERATION_SYSTEMS}: system carried no status object"
                )
        # The serial is configuration, not data: guessing "there is only one
        # system so it must be ours" would silently archive a stranger's house.
        raise CarrierGraphQLError(
            f"carrier {OPERATION_SYSTEMS}: CARRIER_SERIAL not among the "
            f"{len(systems)} system(s) this account can see"
        )

    async def _query(
        self, query: str, variables: Mapping[str, Any], operation_name: str
    ) -> Mapping[str, Any]:
        client = self._ensure_client()
        data = await client.query(
            query, variables=dict(variables), operation_name=operation_name
        )
        return data if isinstance(data, Mapping) else {}

    # ------------------------------------------------------------ row mapping
    def _map_status(self, cycle: PollCycle, status: SystemStatus) -> None:
        """Map one status payload onto rows (PLAN.md §7.3's table).

        Every value goes through :meth:`PollCycle.add`, which drops ``None`` and
        emits *no row*. Nothing here substitutes a zero, a previous reading or a
        default for a missing field.
        """
        device = status.device_id

        cycle.add(device, SYSTEM_CHANNEL, "outdoor_temp_f", self._outdoor_temp_f(status))
        cycle.add(device, SYSTEM_CHANNEL, "mode", self._encode("mode", status.mode))
        self._add_stage(cycle, status)
        cycle.add(device, SYSTEM_CHANNEL, "blower_rpm", status.blower_rpm)
        cycle.add(device, SYSTEM_CHANNEL, "cfm", status.cfm)
        cycle.add(device, SYSTEM_CHANNEL, "compressor_rpm", status.compressor_rpm)
        # A coil temperature is a temperature: it goes through `cfgem` like the
        # rest, so an unknown unit means no row rather than a bare number.
        cycle.add(
            device,
            SYSTEM_CHANNEL,
            "outdoor_coil_temp_f",
            self._temp_f(status, status.outdoor_coil_temp),
        )
        cycle.add(device, SYSTEM_CHANNEL, "static_pressure", status.static_pressure)
        cycle.add(device, SYSTEM_CHANNEL, "idu_cfm", status.idu_cfm)
        cycle.add(device, SYSTEM_CHANNEL, "idu_iducfm", status.idu_iducfm)
        cycle.add(device, SYSTEM_CHANNEL, "odu_iducfm", status.odu_iducfm)
        cycle.add(
            device,
            SYSTEM_CHANNEL,
            "op_status",
            self._encode("op_status", status.op_status),
        )
        cycle.add(device, SYSTEM_CHANNEL, "odu_mode", self._encode("odu_mode", status.odu_mode))
        cycle.add(
            device, SYSTEM_CHANNEL, "idu_status", self._encode("idu_status", status.idu_opstat)
        )

        for zone in status.enabled_zones:
            channel = zone.channel_id
            cycle.add(device, channel, "indoor_temp_f", self._temp_f(status, zone.indoor_temp))
            cycle.add(device, channel, "humidity_pct", zone.humidity_pct)
            cycle.add(device, channel, "setpoint_heat_f", self._temp_f(status, zone.setpoint_heat))
            cycle.add(device, channel, "setpoint_cool_f", self._temp_f(status, zone.setpoint_cool))
            cycle.add(device, channel, "fan", self._encode("fan", zone.fan))

    # -------------------------------------------------------- compressor stage
    def _add_stage(self, cycle: PollCycle, status: SystemStatus) -> None:
        """Emit **at most one** of ``stage`` / ``stage_pct`` for this cycle.

        ``odu.opstat`` is the compressor's operating state and the single most
        useful HVAC signal there is — it is what correlates with watts — but the
        hardware decides its shape (see the module constants above):

        * a **word** in :data:`STAGE_CODES` → ``stage``, unit ``enum``, exactly
          as before; the table is untouched and stays append-only;
        * a **number** → ``stage_pct``, unit ``pct``, the value *as reported*.
          Nothing is rounded, scaled or renamed: ``"35"`` is emitted as ``35.0``;
        * a word that is **not** in the table → WARN, **no row**. An unrecognised
          string must never become a number, and a numeric-looking string is the
          only thing this method will ever treat as a number;
        * a number outside 0-100 → WARN, **no row** (:data:`STAGE_PCT_MIN` /
          :data:`STAGE_PCT_MAX`). It cannot honestly be labelled ``pct``, and
          clamping it into range would archive a fabricated 0 or 100 that no
          later reader could tell from a real one. A gap says "we do not know",
          which is the truth;
        * absent / ``"None"`` / empty → no row, silently. An ordinary gap.

        Logging policy (this is an operational contract, not cosmetics): a
        variable-capacity system is in the numeric case on **every** cycle, so a
        per-cycle WARN would be 2,880 identical lines a day and would bury the
        one real warning that matters. The numeric case is therefore logged at
        **INFO, once**, on first observation and again only when the
        representation *changes* — the counters in ``status.json``
        (``numeric_stage_samples``, ``stage_pct_rows``, ``stage_enum_rows``,
        ``stage_pct_out_of_range``) carry the ongoing volume.
        """
        raw = status.stage_raw
        if raw is None:
            return
        text = str(raw).strip().lower()
        if not text or text == NULL_SENTINEL:
            return

        if stage_metric_for(text) != STAGE_PCT_METRIC:
            # A word: the original enum path, byte for byte. An unknown word
            # still WARNs and emits nothing (`_encode` returns None).
            code = self._encode(STAGE_METRIC, text)
            if code is None:
                return
            self._stage_enum_rows += 1
            self._note_stage_representation(STAGE_REPR_ENUM, text)
            cycle.add(status.device_id, SYSTEM_CHANNEL, STAGE_METRIC, code)
            return

        # A number: a variable-capacity outdoor unit reporting capacity percent.
        self._numeric_stage_samples += 1
        value = _as_float(text)
        if value is None or not (STAGE_PCT_MIN <= value <= STAGE_PCT_MAX):
            # `_as_float` also rejects NaN/inf, which `float()` — and therefore
            # `_looks_numeric` — happily accept from the strings "nan"/"inf".
            self._stage_pct_out_of_range += 1
            self._warn_once(
                STAGE_PCT_METRIC,
                text,
                "bryant_stage_pct_out_of_range",
                detail=(
                    f"odu.opstat parsed as a number outside {STAGE_PCT_MIN:g}-"
                    f"{STAGE_PCT_MAX:g}, so it is not a capacity percentage; "
                    "emitting no row. It is deliberately NOT clamped - a clamped "
                    "value is indistinguishable from a real one once archived"
                ),
            )
            return
        self._stage_pct_rows += 1
        self._note_stage_representation(STAGE_REPR_PCT, text)
        cycle.add(status.device_id, SYSTEM_CHANNEL, STAGE_PCT_METRIC, value)

    def _note_stage_representation(self, representation: str, value: str) -> None:
        """Log the first stage row, and any later change of representation.

        Once per process for a stable system, plus one line if the outdoor unit
        is ever replaced or its firmware changes shape mid-archive — which is the
        one moment a query joining the two metrics needs to know about.
        """
        if representation == self._stage_representation:
            return
        previous = self._stage_representation
        self._stage_representation = representation
        if previous is not None:
            self._stage_representation_changes += 1
        metric = STAGE_PCT_METRIC if representation == STAGE_REPR_PCT else STAGE_METRIC
        self._log.info(
            "bryant_stage_representation",
            representation=representation,
            metric=metric,
            value=value,
            previous=previous,
            changed=previous is not None,
            detail=(
                "odu.opstat is a numeric capacity percentage on this "
                "variable-capacity outdoor unit, which the append-only stage "
                f"enum cannot express; emitting metric {STAGE_PCT_METRIC!r} "
                "(unit pct) instead of 'stage' (DEVIATIONS.md #59)"
                if representation == STAGE_REPR_PCT
                else (
                    "odu.opstat is a word; emitting metric 'stage' as an enum "
                    f"code from the append-only table (not {STAGE_PCT_METRIC!r})"
                )
            ),
        )

    def _temp_f(self, status: SystemStatus, value: float | None) -> float | None:
        """A zone temperature in °F, honouring ``cfgem`` — or ``None``.

        ``rt``/``htsp``/``clsp`` are in whatever ``status.cfgem`` says; that the
        unit is *data* rather than a constant is verified in the reference
        client, which constructs a ``TemperatureUnits`` enum from the field. An
        absent or unrecognised ``cfgem`` means we cannot say what the number
        means, so no row is emitted — a gap beats a mislabelled reading.
        """
        if value is None:
            return None
        if status.temp_unit == TEMP_UNIT_F:
            return value
        if status.temp_unit == TEMP_UNIT_C:
            return _c_to_f(value)
        self._warn_once(
            "cfgem",
            str(status.temp_unit),
            "bryant_temperature_unit_unknown",
            detail="cfgem absent or unrecognised; emitting no temperature rows",
        )
        return None

    def _outdoor_temp_f(self, status: SystemStatus) -> float | None:
        """``oat`` in °F, or ``None``.

        A maintainer's code comment in the Home Assistant integration claims
        ``oat`` is *always* Fahrenheit regardless of ``cfgem``; that is an
        undocumented empirical claim, not a specification, and this system
        reports ``cfgem == "F"`` anyway. So no logic is built on it: when
        ``cfgem`` is ``"F"`` the value is emitted as received, and when it is
        anything else — including ``"C"``, where the claim and the field would
        disagree — no row is emitted and the condition is logged.
        """
        if status.outdoor_temp is None:
            return None
        if status.temp_unit == TEMP_UNIT_F:
            return status.outdoor_temp
        self._warn_once(
            "oat",
            str(status.temp_unit),
            "bryant_outdoor_temp_unit_unverified",
            detail=(
                "oat is claimed to be degF regardless of cfgem, but that is "
                "unverified; emitting no row rather than guessing a unit"
            ),
        )
        return None

    # ------------------------------------------------------------ enum coding
    def _encode(self, metric: str, raw: str | None) -> int | None:
        """Encode an API string with its append-only table, or return ``None``.

        Three outcomes, and only three:

        * a string in the table → its pinned integer;
        * ``None``/``"None"``/empty → no row, silently (an ordinary gap);
        * anything else → **WARN, no row**. Never an invented number and never a
          fallback bucket, because both would be indistinguishable from a real
          observation once archived (PLAN.md §7.3).

        The "anything else" case splits in the logs, because a *numeric* value
        where a word was expected means the field is not the enum we think it is
        — the way ``odu.opstat`` is a capacity percentage on a variable-capacity
        unit. That case has its own metric now (:meth:`_add_stage` emits
        ``stage_pct``) and never reaches here for ``stage``; for ``mode`` and
        ``fan`` a number is still unmapped-and-unexplained, so it is dropped,
        counted and logged under its own event.
        """
        if raw is None:
            return None
        text = str(raw).strip().lower()
        if not text or text == NULL_SENTINEL:
            return None
        table = ENUM_TABLES[metric]
        code = table.get(text)
        if code is not None:
            return code

        self._unknown_enums[metric] = self._unknown_enums.get(metric, 0) + 1
        if _looks_numeric(text):
            self._warn_once(
                metric,
                text,
                "bryant_enum_numeric",
                detail=(
                    "a number where the append-only enum table expects a word: "
                    "the field is not the enum this metric assumes. Emitting no "
                    "row - an unrecognised value must never become a code. "
                    f"(odu.opstat's numeric form has its own metric, "
                    f"{STAGE_PCT_METRIC!r}; see PLAN.md §7.3 / DEVIATIONS.md #59)"
                ),
            )
        else:
            self._warn_once(
                metric,
                text,
                "bryant_enum_unknown",
                detail=(
                    "not in the append-only mapping table; emitting no row. "
                    "Add it with the NEXT unused integer - never renumber."
                ),
            )
        return None

    def _warn_once(self, metric: str, value: str, event: str, *, detail: str) -> None:
        """WARN the first time a given (metric, value) is seen this process.

        The condition is permanent until someone edits the table, and a 30s loop
        would otherwise write the same line 2,880 times a day. The running count
        stays visible in ``status.json`` via :meth:`status_fields`.
        """
        key = (metric, value)
        if key in self._warned_enums:
            return
        self._warned_enums.add(key)
        self._log.warning(
            event,
            metric=metric,
            value=value,
            known=sorted(ENUM_TABLES[metric]) if metric in ENUM_TABLES else None,
            detail=detail,
        )

    # ---------------------------------------------------------------- status
    def status_fields(self) -> dict[str, Any]:
        """This source's ``status.json`` fields — counters only, never a secret.

        Includes the transport's throttle state, which is how PLAN.md §7.3's
        "record the effective cadence" is satisfied: ``poll_interval_s`` is what
        we ask for and ``effective_interval_s`` is what the cloud is actually
        allowing once a ``Retry-After`` has been honoured.
        """
        fields: dict[str, Any] = {
            "operation": self._operation,
            "poll_interval_s": self.poll_interval_s,
            "zones_enabled": len(self._zone_ids),
            "disconnected": self._disconnected,
            "unknown_enum_values": sum(self._unknown_enums.values()),
            # `odu.opstat`'s two renderings (DEVIATIONS.md #59). These are how a
            # variable-capacity system is identified without reading logs, and
            # how the "stage is silently empty" failure becomes visible: a rising
            # `numeric_stage_samples` with a zero `stage_pct_rows` means every
            # numeric sample is being refused as out of range.
            "stage_representation": self._stage_representation,
            "stage_representation_changes": self._stage_representation_changes,
            "numeric_stage_samples": self._numeric_stage_samples,
            "stage_pct_rows": self._stage_pct_rows,
            "stage_enum_rows": self._stage_enum_rows,
            "stage_pct_out_of_range": self._stage_pct_out_of_range,
            # How many DISTINCT (metric, value) conditions have warned. Unlike
            # the raw counters this only moves when something new happens, which
            # is what makes it usable as a write trigger in `_publish_status`.
            "distinct_warnings": len(self._warned_enums),
        }
        client = self._client
        if client is not None:
            try:
                fields.update(client.status_fields())
            except Exception:  # pragma: no cover - telemetry never breaks polling
                self._log.debug("bryant_status_fields_unavailable")
        retry_after = fields.get("retry_after_s")
        fields["effective_interval_s"] = (
            max(self.poll_interval_s, float(retry_after))
            if isinstance(retry_after, (int, float)) and fields.get("throttled")
            else self.poll_interval_s
        )
        return fields

    def _publish_status(self, *, error: BaseException | None = None) -> None:
        """Write :meth:`status_fields` to ``status.json`` when something changed.

        Deliberately **not** every cycle: at 30s that would rewrite the file
        2,880 times a day to say the same thing. A failure always writes; a
        success writes only when a *state* field moved (a throttle event, a token
        renewal, a zone appearing, a new enum/stage condition, the compressor
        changing representation) — **or when the previous write was a failure**,
        which is the whole of :attr:`_status_failed`. The per-cycle counters are
        excluded from that comparison by :data:`_VOLATILE_STATUS_FIELDS`, since
        on a variable-capacity system they move every single cycle by
        construction; they are still written whenever a write happens.

        That last clause is not defensive padding. A failed cycle stamps exactly
        the same comparable counters as a successful one, so without it the
        first success after any transient blip compares equal and is skipped:
        ``record_success`` never runs, and ``status.json`` reports the Carrier
        transport as permanently failing, with a frozen ``last_success_utc`` and
        a stale ``last_error``, until some unrelated counter happens to move.
        ``_consecutive_failures`` cannot stand in for the flag — :meth:`poll`
        resets it to 0 *before* calling here on the success path.

        This never writes the ``bryant_status`` section — that belongs to
        ``stages/poller.py`` (DEVIATIONS.md #20) and two writers would
        double-count its failure counter.
        """
        try:
            fields = self.status_fields()
        except Exception:  # pragma: no cover - defensive
            return
        comparable = {k: v for k, v in fields.items() if k not in _VOLATILE_STATUS_FIELDS}
        if error is None and not self._status_failed and comparable == self._last_status_fields:
            return
        self._last_status_fields = comparable
        self._status_failed = error is not None
        try:
            store = self._status()
            if error is None:
                store.record_success(STATUS_SECTION, **fields)
            else:
                store.record_failure(STATUS_SECTION, error, **fields)
        except Exception:  # pragma: no cover - status must never break the loop
            self._log.debug("bryant_status_write_failed", section=STATUS_SECTION)
