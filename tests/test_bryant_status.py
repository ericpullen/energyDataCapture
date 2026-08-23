"""Bryant status poller tests — offline, fixture-driven (PLAN.md §7.3, §7.4, §15.9).

Every test replays a recorded-shape Carrier GraphQL response from
``tests/fixtures/bryant/`` through the production path — payload →
:class:`SystemStatus` → row mapping → :class:`~energy_capture.model.Observation`
— with only the transport replaced. One test additionally drives the *real*
:class:`~energy_capture.sources.carrier_auth.CarrierGraphQLClient` over an
``httpx.MockTransport`` so the query text, operation name and variables that
would go on the wire are pinned too. No socket is ever opened and no credential
is real (``tests/conftest.py`` guarantees both).

What is pinned here, in order of how much it would hurt to get wrong:

1. **The enum tables never renumber** (§15.9). ``mode``/``stage``/``fan`` codes
   are asserted value by value. Renumbering silently rewrites the meaning of
   every row ever archived, so the suite must fail loudly and immediately.
2. **A disabled zone emits nothing** — even though it reports a humidity, two
   setpoints and a heating state. Filtering on nullness instead of
   ``enabled == "on"`` would fabricate ~80k rows a day.
3. **Gaps stay gaps.** The literal string ``"None"``, JSON ``null``, an absent
   key, an unmapped enum word and an out-of-range capacity percentage all
   produce *absent rows* — never a zero, never an invented enum code, never a
   clamped percentage.
4. **``odu.opstat``'s two renderings** (``stage`` enum vs ``stage_pct``): which
   one a payload produces, that only one ever does, that both stay possible for
   the life of the archive, and that a steady variable-capacity system does not
   log a WARNING every 30 seconds while doing it (DEVIATIONS.md #59).
5. **One ``ts_utc`` per cycle**, and a failed cycle produces exactly zero rows.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

import httpx
import pytest

from energy_capture import logging as ec_logging
from energy_capture import model
from energy_capture.config import Settings, get_settings
from energy_capture.health import StatusStore
from energy_capture.model import SOURCE_BRYANT, Observation
from energy_capture.sources import bryant as bryant_module
from energy_capture.sources.base import (
    Source,
    SourceAuthError,
    SourceTransientError,
)
from energy_capture.sources.bryant import (
    ENUM_TABLES,
    FAN_CODES,
    MODE_CODES,
    OPERATION_STATUS,
    OPERATION_SYSTEMS,
    STAGE_CODES,
    STAGE_METRIC,
    STAGE_PCT_MAX,
    STAGE_PCT_METRIC,
    STAGE_PCT_MIN,
    STAGE_REPR_ENUM,
    STAGE_REPR_PCT,
    STATUS_QUERY,
    STATUS_SECTION,
    SYSTEM_CHANNEL,
    SYSTEMS_QUERY,
    BryantStatusSource,
    SystemStatus,
    enum_decode_text,
    zone_channel_id,
)
from energy_capture.sources.carrier_auth import (
    CarrierAuth,
    CarrierAuthError,
    CarrierGraphQLClient,
    CarrierGraphQLError,
    CarrierRateLimitError,
    CarrierTransientError,
)
from energy_capture.stages.poller import SOURCE_FACTORIES

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bryant"

#: ``CARRIER_SERIAL`` in the pinned test environment (tests/conftest.py).
SERIAL = "TEST0000001"


def load_fixture(name: str) -> dict[str, Any]:
    """The ``data`` object of one recorded GraphQL response body."""
    payload = json.loads((FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8"))
    return payload["data"]


def rows_by_key(rows: list[Observation]) -> dict[tuple[str, str], tuple[float, str]]:
    """``(channel_id, metric) -> (value, unit)`` for compact assertions."""
    return {(row.channel_id, row.metric): (row.value, row.unit) for row in rows}


# ---------------------------------------------------------------- fake client


class FakeGraphQL:
    """Stands in for :class:`CarrierGraphQLClient`.

    Scripted like the Carrier auth tests: the **last** queued item is sticky, so
    a test only scripts the transitions it cares about. An item is either a
    ``data`` mapping to return or an exception instance to raise.
    """

    def __init__(self, *items: Any) -> None:
        self.queue: list[Any] = list(items)
        self.calls: list[SimpleNamespace] = []
        self.closed = 0
        self.fields: dict[str, Any] = {
            "throttle_events": 0,
            "retry_after_s": None,
            "throttled": False,
            "password_grants": 0,
            "refresh_grants": 1,
        }

    def push(self, *items: Any) -> None:
        self.queue.extend(items)

    async def query(
        self,
        query: str,
        *,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
        op: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            SimpleNamespace(
                query=query, variables=dict(variables or {}), operation_name=operation_name
            )
        )
        if not self.queue:
            raise AssertionError("unscripted GraphQL call")
        item = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        if isinstance(item, BaseException):
            raise item
        return item

    def status_fields(self) -> dict[str, Any]:
        return dict(self.fields)

    async def close(self) -> None:
        self.closed += 1

    @property
    def operations(self) -> list[str | None]:
        return [call.operation_name for call in self.calls]


@pytest.fixture
def status_store(tmp_path: Path) -> StatusStore:
    """A real :class:`StatusStore` writing to a throwaway file."""
    return StatusStore(tmp_path / "status.json", poll_intervals={}, load_existing=False)


def make_source(
    fixture: str | FakeGraphQL | None = "status_multizone",
    *,
    settings: Settings | None = None,
    status_store: StatusStore | None = None,
    **kwargs: Any,
) -> BryantStatusSource:
    """A source wired to a :class:`FakeGraphQL` (by fixture name or explicit)."""
    client = fixture if isinstance(fixture, FakeGraphQL) else FakeGraphQL(load_fixture(fixture))
    return BryantStatusSource(
        settings if settings is not None else get_settings(),
        client=client,
        device_id=SERIAL,
        username="carrier-user@example.invalid",
        status_store=status_store if status_store is not None else _NullStatus(),
        **kwargs,
    )


class _NullStatus:
    """A status store that records calls and writes nothing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[Any, ...], dict[str, Any]]] = []

    def record_success(self, section: str, **fields: Any) -> None:
        self.calls.append(("record_success", section, (), fields))

    def record_failure(self, section: str, error: Any, **fields: Any) -> None:
        self.calls.append(("record_failure", section, (error,), fields))

    def set(self, section: str, **fields: Any) -> None:
        self.calls.append(("set", section, (), fields))

    @property
    def sections(self) -> set[str]:
        return {call[1] for call in self.calls}


@pytest.fixture
def log_stream() -> Iterator[io.StringIO]:
    """Capture the package's JSON log lines for one test."""
    stream = io.StringIO()
    ec_logging.configure_logging("DEBUG", stream=stream, force=True)
    try:
        yield stream
    finally:
        ec_logging.configure_logging("INFO", force=True)


def log_events(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


# ============================================================================
# §15.9 — the append-only enum tables
# ============================================================================


def test_mode_codes_are_pinned_value_by_value() -> None:
    """Renumbering rewrites the meaning of every archived row. Never do it."""
    assert dict(MODE_CODES) == {
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


def test_stage_codes_are_pinned_value_by_value() -> None:
    assert dict(STAGE_CODES) == {"off": 0, "low": 1, "high": 2, "idle": 3, "dehumidify": 4}


def test_fan_codes_are_pinned_value_by_value() -> None:
    """``auto`` is deliberately absent: it is an HA display label, not an API value."""
    assert dict(FAN_CODES) == {"off": 0, "low": 1, "med": 2, "high": 3}
    assert "auto" not in FAN_CODES


def test_enum_codes_are_unique_and_dense_from_zero() -> None:
    """A duplicate or a gap is a sign someone renumbered instead of appending."""
    for metric, table in ENUM_TABLES.items():
        codes = sorted(table.values())
        assert len(set(codes)) == len(codes), f"{metric} has a duplicate code"
        assert codes == list(range(len(codes))), f"{metric} codes are not 0..n-1"


def test_the_enum_tables_cover_exactly_the_models_enum_metrics() -> None:
    assert set(ENUM_TABLES) == set(model.ENUM_METRICS)
    for metric in ENUM_TABLES:
        assert model.unit_for_metric(metric) == model.UNIT_ENUM


def test_the_enum_tables_cannot_be_mutated_in_place() -> None:
    """They are quoted into Glue comments and the README; nothing may edit them."""
    assert isinstance(MODE_CODES, MappingProxyType)
    with pytest.raises(TypeError):
        MODE_CODES["furnace"] = 99  # type: ignore[index]


def test_enum_decode_text_is_what_glue_and_the_readme_quote() -> None:
    assert enum_decode_text("fan") == "0=off, 1=low, 2=med, 3=high"
    assert enum_decode_text("mode").startswith("0=off, 1=heat, 2=cool")


# ============================================================================
# Row mapping
# ============================================================================


async def test_a_multizone_response_maps_to_exactly_the_expected_observations() -> None:
    """Every row pinned: channel, metric, value and unit (PLAN.md §7.3's table)."""
    source = make_source("status_multizone")

    rows = await source.poll()

    assert rows_by_key(rows) == {
        (SYSTEM_CHANNEL, "outdoor_temp_f"): (91.5, "degF"),
        (SYSTEM_CHANNEL, "mode"): (2.0, "enum"),  # cool
        (SYSTEM_CHANNEL, "stage"): (2.0, "enum"),  # odu.opstat = high
        (SYSTEM_CHANNEL, "blower_rpm"): (875.0, "rpm"),
        (SYSTEM_CHANNEL, "cfm"): (1180.0, "CFM"),
        (SYSTEM_CHANNEL, "idu_cfm"): (1180.0, "CFM"),
        (SYSTEM_CHANNEL, "odu_iducfm"): (1180.0, "CFM"),
        (SYSTEM_CHANNEL, "idu_status"): (3.0, "enum"),  # high
        # This fixture says odu.opmode = "cool" where the 2026-08-17 capture says
        # "cooling". Both are in the table at DIFFERENT codes (1 and 6): the
        # table is append-only, so a synonym gets the next unused integer and an
        # archived code never changes meaning.
        (SYSTEM_CHANNEL, "odu_mode"): (6.0, "enum"),  # cool
        (SYSTEM_CHANNEL, "static_pressure"): (0.6000000238418579, "inwc"),
        ("zone_1", "indoor_temp_f"): (71.5, "degF"),
        ("zone_1", "humidity_pct"): (38.0, "pct"),
        ("zone_1", "setpoint_heat_f"): (70.0, "degF"),
        ("zone_1", "setpoint_cool_f"): (78.0, "degF"),
        ("zone_1", "fan"): (2.0, "enum"),  # med
        ("zone_3", "indoor_temp_f"): (68.0, "degF"),
        ("zone_3", "humidity_pct"): (41.0, "pct"),
        ("zone_3", "setpoint_heat_f"): (68.0, "degF"),
        ("zone_3", "setpoint_cool_f"): (76.0, "degF"),
        ("zone_3", "fan"): (1.0, "enum"),  # low
    }
    assert len(rows) == 20
    assert {row.source for row in rows} == {SOURCE_BRYANT}
    assert {row.device_id for row in rows} == {SERIAL}


async def test_every_row_of_a_cycle_shares_one_ts_utc() -> None:
    """PLAN.md §6.5: one timestamp per source per cycle, taken when complete."""
    source = make_source("status_multizone")

    rows = await source.poll()

    assert len({row.ts_utc for row in rows}) == 1
    assert len({row.ts_local for row in rows}) == 1
    stamp = rows[0].ts_utc
    assert stamp.tzinfo is not None
    # ts_local is the naive local wall clock of the same instant (CLAUDE.md rule 3).
    assert rows[0].ts_local.tzinfo is None


async def test_a_single_zone_system_emits_one_zone_and_seven_phantoms_are_absent() -> None:
    """The real capture returns 8 zones with only zone 1 installed."""
    source = make_source("status_single_zone")

    rows = await source.poll()

    channels = {row.channel_id for row in rows}
    assert channels == {SYSTEM_CHANNEL, "zone_1"}
    assert rows_by_key(rows) == {
        (SYSTEM_CHANNEL, "outdoor_temp_f"): (30.0, "degF"),
        (SYSTEM_CHANNEL, "mode"): (1.0, "enum"),  # heat
        (SYSTEM_CHANNEL, "stage"): (0.0, "enum"),  # odu off
        (SYSTEM_CHANNEL, "blower_rpm"): (1224.0, "rpm"),
        (SYSTEM_CHANNEL, "cfm"): (1239.0, "CFM"),
        # Mapped 2026-08-22. This fixture has no comprpm/oducoiltmp/oprstsmsg,
        # so those produce NO rows rather than zeros — which is the point of
        # adding them field by field instead of assuming a payload shape.
        (SYSTEM_CHANNEL, "idu_cfm"): (1239.0, "CFM"),
        (SYSTEM_CHANNEL, "idu_status"): (2.0, "enum"),  # low
        (SYSTEM_CHANNEL, "odu_mode"): (0.0, "enum"),  # off
        (SYSTEM_CHANNEL, "static_pressure"): (1.399999976158142, "inwc"),
        ("zone_1", "indoor_temp_f"): (74.0, "degF"),
        ("zone_1", "humidity_pct"): (32.0, "pct"),
        ("zone_1", "setpoint_heat_f"): (74.0, "degF"),
        ("zone_1", "setpoint_cool_f"): (78.0, "degF"),
        ("zone_1", "fan"): (2.0, "enum"),
    }


async def test_a_disabled_zone_emits_nothing_even_though_it_reports_numbers() -> None:
    """The phantom zone carries rh/htsp/clsp and a heating state. It is not a channel."""
    payload = load_fixture("status_multizone")["infinityStatus"]
    zone_2 = next(z for z in payload["zones"] if z["id"] == "2")
    # Guard the fixture itself: this test is only meaningful while zone 2 lies.
    assert zone_2["enabled"] == "off"
    assert zone_2["rh"] == "34" and zone_2["htsp"] == "60.0"
    assert zone_2["zoneconditioning"] == "active_heat"

    rows = await make_source("status_multizone").poll()

    assert not [row for row in rows if row.channel_id == "zone_2"]


@pytest.mark.parametrize(
    "enabled",
    [
        pytest.param("off", id="off"),
        pytest.param("None", id="the-literal-string-None"),
        pytest.param(None, id="json-null"),
        pytest.param("", id="empty-string"),
        pytest.param("1", id="a-number-is-not-the-word-on"),
        pytest.param("enabled", id="a-different-word"),
    ],
)
async def test_only_the_exact_string_on_makes_a_zone_exist(enabled: str | None) -> None:
    """``enabled == "on"`` is a strict positive test, and must stay one.

    Anything else — a different word, Carrier's ``"None"`` sentinel, JSON null,
    an absent key — means the zone did not report itself as installed this
    cycle. Inverting the test (``!= "off"``) would resurrect every phantom.
    """
    payload = load_fixture("status_multizone")
    zone_1 = payload["infinityStatus"]["zones"][0]
    if enabled is None:
        zone_1.pop("enabled")
    else:
        zone_1["enabled"] = enabled

    rows = await make_source(FakeGraphQL(payload)).poll()

    assert not [row for row in rows if row.channel_id == "zone_1"]
    assert [row for row in rows if row.channel_id == "zone_3"], "zone 3 is unaffected"


async def test_the_enabled_test_is_case_insensitive_on_the_word_itself() -> None:
    """Only the *word* matters. A case change upstream must not blank the house.

    The asymmetry is deliberate: accepting ``"ON"`` cannot fabricate a zone
    (nothing else spells "installed"), whereas rejecting it would turn a
    cosmetic upstream change into a total, silent data blackout.
    """
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["zones"][0]["enabled"] = "ON"

    rows = await make_source(FakeGraphQL(payload)).poll()

    assert [row for row in rows if row.channel_id == "zone_1"]


async def test_a_zone_with_no_id_is_never_given_a_channel() -> None:
    """``zone_`` is not a channel name; an id-less zone is not addressable."""
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["zones"][0]["id"] = "None"

    rows = await make_source(FakeGraphQL(payload)).poll()

    assert {row.channel_id for row in rows} == {SYSTEM_CHANNEL, "zone_3"}


async def test_a_zone_that_disappears_stops_emitting_rows_and_says_so(
    log_stream: io.StringIO,
) -> None:
    """A zone flipping ``enabled`` off is a correct gap, but the channel set changed."""
    first = load_fixture("status_multizone")
    second = json.loads(json.dumps(first))
    for zone in second["infinityStatus"]["zones"]:
        if zone["id"] == "3":
            zone["enabled"] = "off"
    client = FakeGraphQL(first, second)
    source = make_source(client)

    assert source.zone_ids == ()
    await source.poll()
    assert source.zone_ids == ("1", "3")
    rows = await source.poll()

    assert source.zone_ids == ("1",)
    assert not [row for row in rows if row.channel_id == "zone_3"]
    assert any(e["event"] == "bryant_zone_set_changed" for e in log_events(log_stream))


async def test_zone_channels_are_named_from_the_api_id_not_the_list_position() -> None:
    payload = load_fixture("status_multizone")
    zones = payload["infinityStatus"]["zones"]
    zones[0]["id"] = "10"  # a 10-zone panel; position 0 is still zone 10
    source = make_source(FakeGraphQL(payload))

    rows = await source.poll()

    assert {row.channel_id for row in rows if row.channel_id != SYSTEM_CHANNEL} == {
        "zone_10",
        "zone_3",
    }
    assert zone_channel_id(7) == "zone_7"


# ============================================================================
# Gaps stay gaps
# ============================================================================


async def test_nulls_of_every_spelling_emit_no_rows_rather_than_zeros() -> None:
    """``"None"``, JSON ``null``, an absent key and an unmapped word all → no row."""
    source = make_source("status_nulls")

    rows = await source.poll()

    assert rows_by_key(rows) == {("zone_1", "setpoint_heat_f"): (68.0, "degF")}


def test_the_literal_string_none_is_never_parsed_as_a_number() -> None:
    """The single highest-risk trap: ``float("None")`` raises, ``if "None":`` passes."""
    status = SystemStatus.from_payload(
        {
            "cfgem": "F",
            "oat": "None",
            "mode": "None",
            "odu": {"opstat": " none "},
            "zones": [{"id": "1", "enabled": "on", "rt": "NONE", "fan": "None"}],
        },
        device_id=SERIAL,
    )
    # Numbers *and* strings: the sentinel is stripped at the parse boundary, so
    # nothing downstream ever has to remember that "None" is not a value.
    assert status.outdoor_temp is None
    assert status.mode is None
    assert status.odu_opstat is None
    assert status.zones[0].indoor_temp is None
    assert status.zones[0].fan is None


async def test_an_unknown_mode_string_emits_no_row_and_logs_a_warn(
    log_stream: io.StringIO,
) -> None:
    """PLAN.md §7.3: unknown enum string → WARN, no row. Never an invented number."""
    source = make_source("status_nulls")  # mode = "defrost"

    rows = await source.poll()

    assert not [row for row in rows if row.metric == "mode"]
    warns = [
        e
        for e in log_events(log_stream)
        if e["event"] == "bryant_enum_unknown" and e["metric"] == "mode"
    ]
    assert len(warns) == 1
    assert warns[0]["level"] == "WARNING"
    assert warns[0]["value"] == "defrost"
    assert source.unknown_enum_counts["mode"] == 1


async def test_fan_auto_is_not_an_api_value_and_emits_no_row(
    log_stream: io.StringIO,
) -> None:
    """``auto`` is a Home Assistant display label substituted for ``off``."""
    rows = await make_source("status_nulls").poll()

    assert not [row for row in rows if row.metric == "fan"]
    assert any(
        e["event"] == "bryant_enum_unknown" and e["value"] == "auto"
        for e in log_events(log_stream)
    )


async def test_an_unknown_enum_warns_once_per_distinct_value_not_once_per_cycle(
    log_stream: io.StringIO,
) -> None:
    """2,880 identical lines a day would bury the signal; the count stays visible."""
    source = make_source("status_nulls")

    for _ in range(3):
        await source.poll()

    warns = [
        e
        for e in log_events(log_stream)
        if e["event"] == "bryant_enum_unknown" and e["metric"] == "mode"
    ]
    assert len(warns) == 1
    assert source.unknown_enum_counts["mode"] == 3


# ============================================================================
# `odu.opstat`'s two renderings: `stage` (enum) and `stage_pct` (pct)
#
# DEVIATIONS.md #59, confirmed against the live system on 2026-08-17: this house
# has a variable-capacity (Greenspeed) outdoor unit, so `opstat` is a 0-100
# capacity percentage and the `stage` enum can never express it. Compressor
# capacity is the signal that correlates with watts, so it gets its own metric
# rather than a permanent gap — but the enum path stays, because the archive
# outlives the hardware.
# ============================================================================


def test_the_varcap_fixture_really_is_the_captured_variable_capacity_system() -> None:
    """Guard the fixture: every test below is only meaningful while this holds."""
    status = load_fixture("status_varcap")["infinityStatus"]

    assert status["odu"]["type"] == "gs3ngiphp"  # Greenspeed = variable capacity
    assert status["odu"]["opstat"] == "35"  # a percentage string, not a word
    assert status["odu"]["opstat"].lower() not in STAGE_CODES
    # The indoor unit reports a *word* in the same payload. STAGE_SOURCE = odu is
    # what decides which one becomes a row; a fallback would silently swap in a
    # different physical unit's state.
    assert status["idu"]["opstat"] == "off"


async def test_a_numeric_opstat_emits_one_stage_pct_row_and_no_stage_row() -> None:
    """35 is not a stage — it is 35% compressor capacity, unit pct, verbatim."""
    source = make_source("status_varcap")

    rows = await source.poll()

    stage_pct = [row for row in rows if row.metric == STAGE_PCT_METRIC]
    assert len(stage_pct) == 1
    assert stage_pct[0].value == 35.0
    assert stage_pct[0].unit == "pct"
    assert stage_pct[0].channel_id == SYSTEM_CHANNEL
    assert stage_pct[0].device_id == SERIAL
    assert not [row for row in rows if row.metric == STAGE_METRIC]
    # The rest of the cycle is untouched — including the enum metrics that are
    # *not* opstat, which is what proves this is a per-field decision.
    assert rows_by_key(rows) == {
        (SYSTEM_CHANNEL, "outdoor_temp_f"): (74.0, "degF"),
        (SYSTEM_CHANNEL, "mode"): (2.0, "enum"),
        (SYSTEM_CHANNEL, STAGE_PCT_METRIC): (35.0, "pct"),
        (SYSTEM_CHANNEL, "blower_rpm"): (433.0, "rpm"),
        (SYSTEM_CHANNEL, "cfm"): (500.0, "CFM"),
        # The real 2026-08-17 capture, which is the one that populates every
        # field added on 2026-08-22 — including all three airflow numbers, which
        # disagree by more than 2x and are therefore recorded separately.
        (SYSTEM_CHANNEL, "compressor_rpm"): (1190.0, "rpm"),
        (SYSTEM_CHANNEL, "outdoor_coil_temp_f"): (74.0, "degF"),
        (SYSTEM_CHANNEL, "static_pressure"): (0.13999998569488525, "inwc"),
        (SYSTEM_CHANNEL, "idu_cfm"): (500.0, "CFM"),
        (SYSTEM_CHANNEL, "idu_iducfm"): (513.0, "CFM"),
        (SYSTEM_CHANNEL, "odu_iducfm"): (1166.0, "CFM"),
        (SYSTEM_CHANNEL, "op_status"): (0.0, "enum"),  # idle
        (SYSTEM_CHANNEL, "odu_mode"): (1.0, "enum"),  # cooling
        (SYSTEM_CHANNEL, "idu_status"): (0.0, "enum"),  # off
        ("zone_1", "indoor_temp_f"): (68.0, "degF"),
        ("zone_1", "humidity_pct"): (53.0, "pct"),
        ("zone_1", "setpoint_heat_f"): (64.0, "degF"),
        ("zone_1", "setpoint_cool_f"): (68.0, "degF"),
        ("zone_1", "fan"): (3.0, "enum"),  # high
    }


async def test_the_stage_pct_row_carries_the_models_canonical_unit() -> None:
    """The unit is never invented at the call site (model.UNIT_FOR_METRIC)."""
    assert model.unit_for_metric(STAGE_PCT_METRIC) == model.UNIT_PCT

    rows = await make_source("status_varcap").poll()

    row = next(r for r in rows if r.metric == STAGE_PCT_METRIC)
    assert row.unit == model.unit_for_metric(STAGE_PCT_METRIC)


@pytest.mark.parametrize(
    "fixture,expected",
    [
        pytest.param("status_multizone", 2.0, id="high"),
        pytest.param("status_single_zone", 0.0, id="off"),
    ],
)
async def test_a_word_opstat_still_emits_stage_as_an_enum_and_never_stage_pct(
    fixture: str, expected: float
) -> None:
    """The enum path is unchanged: a staged compressor is still an enum code."""
    source = make_source(fixture)

    rows = await source.poll()

    assert rows_by_key(rows)[(SYSTEM_CHANNEL, STAGE_METRIC)] == (expected, "enum")
    assert not [row for row in rows if row.metric == STAGE_PCT_METRIC]
    assert source.stage_representation == STAGE_REPR_ENUM
    assert source.status_fields()["numeric_stage_samples"] == 0


async def test_an_unknown_opstat_word_emits_neither_metric_and_warns(
    log_stream: io.StringIO,
) -> None:
    """An unrecognised STRING must never become a number. Not a code, not a pct."""
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["odu"]["opstat"] = "turbo"
    source = make_source(FakeGraphQL(payload))

    rows = await source.poll()

    assert not [row for row in rows if row.metric in (STAGE_METRIC, STAGE_PCT_METRIC)]
    warns = [
        e
        for e in log_events(log_stream)
        if e["event"] == "bryant_enum_unknown" and e["metric"] == STAGE_METRIC
    ]
    assert len(warns) == 1 and warns[0]["level"] == "WARNING"
    assert warns[0]["value"] == "turbo"
    assert source.unknown_enum_counts[STAGE_METRIC] == 1
    assert source.status_fields()["stage_pct_rows"] == 0
    assert source.stage_representation is None


async def test_a_steady_variable_capacity_system_never_warns_per_cycle(
    log_stream: io.StringIO,
) -> None:
    """The defect this fixes: one WARNING per poll is 2,880 a day of pure noise.

    The live run before ``stage_pct`` existed logged ``bryant_enum_numeric`` at
    WARNING on **every** cycle, which buries the one warning that matters. The
    condition is now reported once, at INFO, on first observation.
    """
    source = make_source(FakeGraphQL(load_fixture("status_varcap")))

    for _ in range(10):
        rows = await source.poll()
        assert len([r for r in rows if r.metric == STAGE_PCT_METRIC]) == 1

    events = log_events(log_stream)
    assert not [e for e in events if e["level"] in ("WARNING", "ERROR")], (
        "a steady variable-capacity system must produce no warnings at all"
    )
    first = [e for e in events if e["event"] == "bryant_stage_representation"]
    assert len(first) == 1
    assert first[0]["level"] == "INFO"
    assert first[0]["representation"] == STAGE_REPR_PCT
    assert first[0]["metric"] == STAGE_PCT_METRIC
    assert first[0]["previous"] is None and first[0]["changed"] is False
    # The volume stays visible where it costs nothing: counters, not log lines.
    fields = source.status_fields()
    assert fields["numeric_stage_samples"] == 10
    assert fields["stage_pct_rows"] == 10
    assert fields["stage_representation"] == STAGE_REPR_PCT
    assert fields["stage_representation_changes"] == 0


async def test_a_change_of_representation_is_logged_once_in_each_direction(
    log_stream: io.StringIO,
) -> None:
    """A replaced outdoor unit switches shape mid-archive; that is worth a line.

    Both renderings must stay possible for the life of the archive, so the source
    may not latch onto whichever one it saw first.
    """
    numeric = load_fixture("status_varcap")
    word = load_fixture("status_multizone")  # odu.opstat = "high"
    client = FakeGraphQL(numeric, numeric, word, word, numeric, numeric)
    source = make_source(client)

    metrics: list[set[str]] = []
    for _ in range(6):
        rows = await source.poll()
        metrics.append({r.metric for r in rows if r.metric.startswith(STAGE_METRIC)})

    assert metrics == [
        {STAGE_PCT_METRIC},
        {STAGE_PCT_METRIC},
        {STAGE_METRIC},
        {STAGE_METRIC},
        {STAGE_PCT_METRIC},
        {STAGE_PCT_METRIC},
    ], "exactly one rendering per cycle, and the source never latches"
    changes = [
        (e["previous"], e["representation"])
        for e in log_events(log_stream)
        if e["event"] == "bryant_stage_representation"
    ]
    assert changes == [
        (None, STAGE_REPR_PCT),
        (STAGE_REPR_PCT, STAGE_REPR_ENUM),
        (STAGE_REPR_ENUM, STAGE_REPR_PCT),
    ]
    fields = source.status_fields()
    assert fields["stage_representation_changes"] == 2
    assert (fields["stage_pct_rows"], fields["stage_enum_rows"]) == (4, 2)


async def test_the_two_renderings_never_collide_in_the_dedupe_key() -> None:
    """A system that changed shape keeps both metrics; neither overwrites the other."""
    client = FakeGraphQL(load_fixture("status_varcap"), load_fixture("status_multizone"))
    source = make_source(client)

    rows = [row for cycle in (await source.poll(), await source.poll()) for row in cycle]
    stage_rows = [r for r in rows if r.metric in (STAGE_METRIC, STAGE_PCT_METRIC)]

    table = model.observations_to_table(stage_rows)
    assert table.num_rows == 2, "a dedupe on (ts,source,device,channel,metric) keeps both"
    assert set(table.column("metric").to_pylist()) == {STAGE_METRIC, STAGE_PCT_METRIC}


@pytest.mark.parametrize(
    "opstat",
    [
        pytest.param("101", id="just-over-100"),
        pytest.param("-1", id="negative"),
        pytest.param("1000", id="wildly-out-of-range"),
        pytest.param("nan", id="nan-parses-as-a-float-but-is-not-a-percentage"),
        pytest.param("inf", id="inf"),
    ],
)
async def test_an_out_of_range_percentage_is_a_gap_and_is_never_clamped(
    opstat: str, log_stream: io.StringIO
) -> None:
    """0-100 or nothing. A clamped 100 is indistinguishable from an observed 100."""
    payload = load_fixture("status_varcap")
    payload["infinityStatus"]["odu"]["opstat"] = opstat
    source = make_source(FakeGraphQL(payload))

    rows = await source.poll()

    assert not [row for row in rows if row.metric in (STAGE_METRIC, STAGE_PCT_METRIC)]
    warns = [
        e for e in log_events(log_stream) if e["event"] == "bryant_stage_pct_out_of_range"
    ]
    assert len(warns) == 1 and warns[0]["level"] == "WARNING"
    assert warns[0]["value"] == opstat
    fields = source.status_fields()
    assert fields["stage_pct_out_of_range"] == 1
    assert fields["numeric_stage_samples"] == 1, "it was still a numeric sample"
    assert fields["stage_pct_rows"] == 0
    assert source.stage_representation is None
    # The rest of the cycle survives: one impossible field is one gap.
    assert ("zone_1", "indoor_temp_f") in rows_by_key(rows)


async def test_an_out_of_range_percentage_warns_once_not_once_per_cycle(
    log_stream: io.StringIO,
) -> None:
    """Same discipline as an unmapped enum word: the condition is persistent."""
    payload = load_fixture("status_varcap")
    payload["infinityStatus"]["odu"]["opstat"] = "255"
    source = make_source(FakeGraphQL(payload))

    for _ in range(5):
        await source.poll()

    warns = [
        e for e in log_events(log_stream) if e["event"] == "bryant_stage_pct_out_of_range"
    ]
    assert len(warns) == 1
    assert source.status_fields()["stage_pct_out_of_range"] == 5


@pytest.mark.parametrize(
    "opstat,expected",
    [
        pytest.param("0", 0.0, id="zero-is-a-real-reading-not-a-gap"),
        pytest.param("100", 100.0, id="full-capacity"),
        pytest.param("37.5", 37.5, id="fractional-percentages-survive-verbatim"),
        pytest.param(" 42 ", 42.0, id="whitespace-is-stripped-not-a-value"),
    ],
)
async def test_in_range_percentages_are_recorded_verbatim(
    opstat: str, expected: float
) -> None:
    """CLAUDE.md rule 2: nothing rounds, scales or re-bases what the API said."""
    payload = load_fixture("status_varcap")
    payload["infinityStatus"]["odu"]["opstat"] = opstat

    rows = rows_by_key(await make_source(FakeGraphQL(payload)).poll())

    assert rows[(SYSTEM_CHANNEL, STAGE_PCT_METRIC)] == (expected, "pct")


def test_the_sanity_bounds_are_the_definition_of_a_percentage() -> None:
    assert (STAGE_PCT_MIN, STAGE_PCT_MAX) == (0.0, 100.0)


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param("35", STAGE_PCT_METRIC, id="the-real-systems-value"),
        pytest.param("0", STAGE_PCT_METRIC, id="zero"),
        pytest.param("150", STAGE_PCT_METRIC, id="out-of-range-is-still-pct-shaped"),
        pytest.param("high", STAGE_METRIC, id="a-known-word"),
        pytest.param("HIGH", STAGE_METRIC, id="case-folded"),
        pytest.param("turbo", None, id="an-unknown-word-renders-as-nothing"),
        pytest.param("None", None, id="the-none-sentinel"),
        pytest.param(None, None, id="json-null"),
        pytest.param("", None, id="empty"),
    ],
)
def test_stage_metric_for_says_which_rendering_a_value_is(
    raw: str | None, expected: str | None
) -> None:
    """``discover`` answers "stage or stage_pct?" from one response, before rows."""
    assert bryant_module.stage_metric_for(raw) == expected


async def test_discovery_reports_which_stage_metric_this_system_produces() -> None:
    """DEVIATIONS.md #75.1 is answered by ``energycap discover``, not a log dive."""
    varcap = await make_source("status_varcap").discover(force=True)
    staged = await make_source("status_multizone").discover(force=True)

    system = next(c for c in varcap.channels if c.channel_id == SYSTEM_CHANNEL)
    assert system.details["odu_type"] == "gs3ngiphp"
    assert system.details["odu_opstat"] == "35"
    assert system.details["stage_metric"] == STAGE_PCT_METRIC

    other = next(c for c in staged.channels if c.channel_id == SYSTEM_CHANNEL)
    assert other.details["stage_metric"] == STAGE_METRIC


async def test_a_null_opstat_is_a_silent_gap_in_both_renderings(
    log_stream: io.StringIO,
) -> None:
    """``"None"`` is an ordinary missing field, not an anomaly to shout about."""
    source = make_source("status_nulls")  # odu.opstat = "None"

    rows = await source.poll()

    assert not [row for row in rows if row.metric in (STAGE_METRIC, STAGE_PCT_METRIC)]
    events = log_events(log_stream)
    assert not [
        e
        for e in events
        if e["event"] in ("bryant_stage_pct_out_of_range", "bryant_stage_representation")
    ]
    assert not [e for e in events if e.get("metric") == STAGE_METRIC]
    assert source.status_fields()["numeric_stage_samples"] == 0


def test_stage_pct_is_a_measurement_not_an_enum() -> None:
    """It renders the same field as ``stage``, but averaging it is meaningful."""
    assert STAGE_PCT_METRIC in model.METRICS
    assert model.unit_for_metric(STAGE_PCT_METRIC) == "pct"
    assert STAGE_PCT_METRIC not in model.ENUM_METRICS
    assert STAGE_PCT_METRIC not in ENUM_TABLES
    assert not model.is_day_grain(STAGE_PCT_METRIC)


def test_adding_stage_pct_disturbed_no_existing_metric() -> None:
    """A new metric may not re-unit an old one; the archive would change meaning."""
    assert model.UNIT_FOR_METRIC[STAGE_METRIC] == model.UNIT_ENUM
    assert {
        "watts": "W",
        "amps": "A",
        "volts": "V",
        "hz": "Hz",
        "indoor_temp_f": "degF",
        "outdoor_temp_f": "degF",
        "setpoint_cool_f": "degF",
        "setpoint_heat_f": "degF",
        "humidity_pct": "pct",
        "stage": "enum",
        "stage_pct": "pct",
        "mode": "enum",
        "fan": "enum",
        "blower_rpm": "rpm",
        "cfm": "CFM",
        "compressor_rpm": "rpm",
        "outdoor_coil_temp_f": "degF",
        "static_pressure": "inwc",
        "idu_cfm": "CFM",
        "idu_iducfm": "CFM",
        "odu_iducfm": "CFM",
        "op_status": "enum",
        "odu_mode": "enum",
        "idu_status": "enum",
        "kwh_day": "kWh",
        "cost_day_usd": "USD",
        "kwh_interval": "kWh",
        "ccf_interval": "CCF",
    } == dict(model.UNIT_FOR_METRIC)


def test_stage_codes_are_still_append_only_after_stage_pct_arrived() -> None:
    """``stage_pct`` ADDS a metric. It retires and renumbers nothing.

    The duplicate of the pinning test above is deliberate: the temptation when
    adding the percentage path is to "tidy up" the now-unused-looking words, and
    every archived `stage` row's meaning depends on these five integers.
    """
    assert dict(STAGE_CODES) == {"off": 0, "low": 1, "high": 2, "idle": 3, "dehumidify": 4}
    assert ENUM_TABLES[STAGE_METRIC] is STAGE_CODES
    assert enum_decode_text(STAGE_METRIC) == "0=off, 1=low, 2=high, 3=idle, 4=dehumidify"


async def test_status_json_is_not_rewritten_every_cycle_on_a_varcap_system() -> None:
    """The counters rise every cycle by construction; the file must not follow.

    A comparison that included them would rewrite ``status.json`` 2,880 times a
    day on exactly the system this feature exists for.
    """
    store = _NullStatus()
    source = make_source(FakeGraphQL(load_fixture("status_varcap")), status_store=store)

    for _ in range(5):
        await source.poll()

    assert len(store.calls) == 1
    assert store.calls[0][3]["stage_pct_rows"] == 1  # the value at write time


async def test_a_new_stage_condition_still_forces_a_status_write() -> None:
    """Volatile counters are excluded, but a NEW condition must reach status.json."""
    store = _NullStatus()
    good = load_fixture("status_varcap")
    bad = json.loads(json.dumps(good))
    bad["infinityStatus"]["odu"]["opstat"] = "150"
    source = make_source(FakeGraphQL(good, good, bad, bad), status_store=store)

    for _ in range(4):
        await source.poll()

    assert [call[0] for call in store.calls] == ["record_success", "record_success"]
    assert store.calls[-1][3]["stage_pct_out_of_range"] == 1


async def test_a_disconnected_system_emits_zero_rows(log_stream: io.StringIO) -> None:
    """The payload is the cloud's stale cache; archiving it would be fabrication."""
    source = make_source("status_disconnected")

    rows = await source.poll()

    assert rows == []
    assert source.disconnected is True
    assert source.consecutive_failures == 0, "a disconnected system is not a poll failure"
    assert any(
        e["event"] == "bryant_system_disconnected" for e in log_events(log_stream)
    )


# ============================================================================
# Units — the temperature unit is data, not a constant
# ============================================================================


async def test_celsius_readings_are_converted_to_degf() -> None:
    """``cfgem`` governs rt/htsp/clsp; the metric names and unit are Fahrenheit."""
    rows = rows_by_key(await make_source("status_celsius").poll())

    assert rows[("zone_1", "indoor_temp_f")] == (pytest.approx(69.8), "degF")
    assert rows[("zone_1", "setpoint_heat_f")] == (pytest.approx(68.0), "degF")
    assert rows[("zone_1", "setpoint_cool_f")] == (pytest.approx(77.0), "degF")


async def test_outdoor_temp_emits_no_row_when_cfgem_is_not_fahrenheit(
    log_stream: io.StringIO,
) -> None:
    """``oat`` being always-degF is an undocumented code comment, not a spec."""
    rows = await make_source("status_celsius").poll()

    assert not [row for row in rows if row.metric == "outdoor_temp_f"]
    assert any(
        e["event"] == "bryant_outdoor_temp_unit_unverified"
        for e in log_events(log_stream)
    )


async def test_an_absent_cfgem_emits_no_temperature_rows_but_humidity_survives(
    log_stream: io.StringIO,
) -> None:
    """A number whose unit we cannot justify is worse than an honest gap."""
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["cfgem"] = "None"
    rows = await make_source(FakeGraphQL(payload)).poll()

    by_key = rows_by_key(rows)
    assert not [row for row in rows if row.unit == "degF"]
    assert by_key[("zone_1", "humidity_pct")] == (38.0, "pct")
    assert by_key[(SYSTEM_CHANNEL, "mode")] == (2.0, "enum")
    assert any(
        e["event"] == "bryant_temperature_unit_unknown" for e in log_events(log_stream)
    )


async def test_cfm_falls_back_to_the_outdoor_units_iducfm() -> None:
    """The reference's own fallback chain: ``idu.cfm`` then ``odu.iducfm``."""
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["idu"]["cfm"] = "None"
    payload["infinityStatus"]["odu"]["iducfm"] = "1042"

    rows = rows_by_key(await make_source(FakeGraphQL(payload)).poll())

    assert rows[(SYSTEM_CHANNEL, "cfm")] == (1042.0, "CFM")


async def test_values_are_recorded_verbatim() -> None:
    """CLAUDE.md rule 2: nothing here rounds, clamps or smooths what the API said."""
    payload = load_fixture("status_multizone")
    payload["infinityStatus"]["zones"][0]["rt"] = "71.55555555555556"
    payload["infinityStatus"]["oat"] = "0"  # a real zero is a real reading

    rows = rows_by_key(await make_source(FakeGraphQL(payload)).poll())

    assert rows[("zone_1", "indoor_temp_f")][0] == 71.55555555555556
    assert rows[(SYSTEM_CHANNEL, "outdoor_temp_f")] == (0.0, "degF")


# ============================================================================
# Failure discipline (PLAN.md §6.6's contract, mirrored)
# ============================================================================


async def test_a_failed_poll_cycle_emits_exactly_zero_rows(log_stream: io.StringIO) -> None:
    source = make_source(FakeGraphQL(CarrierTransientError("carrier graphql: HTTP 502")))

    with pytest.raises(SourceTransientError):
        await source.poll()

    assert source.consecutive_failures == 1
    warns = [e for e in log_events(log_stream) if e["event"] == "bryant_poll_failed"]
    assert len(warns) == 1 and warns[0]["rows"] == 0


async def test_the_failure_counter_resets_after_a_good_cycle() -> None:
    client = FakeGraphQL(
        CarrierTransientError("boom"),
        CarrierTransientError("boom"),
        load_fixture("status_multizone"),
    )
    source = make_source(client)

    for _ in range(2):
        with pytest.raises(SourceTransientError):
            await source.poll()
    assert source.consecutive_failures == 2

    assert await source.poll()
    assert source.consecutive_failures == 0


async def test_an_auth_failure_propagates_as_a_source_auth_error() -> None:
    """The poll loop tells auth from transient; both emit zero rows."""
    source = make_source(FakeGraphQL(SourceAuthError("carrier graphql: HTTP 401")))

    with pytest.raises(SourceAuthError):
        await source.poll()

    assert source.consecutive_failures == 1


async def test_a_graphql_errors_payload_never_becomes_rows() -> None:
    """A 200 with an ``errors`` array is a failure, not data (partial data discarded)."""
    source = make_source(
        FakeGraphQL(CarrierGraphQLError("carrier graphql: GraphQL errors (boom)")),
        allow_fallback=False,
    )

    with pytest.raises(SourceTransientError):
        await source.poll()


async def test_a_rate_limit_is_transient_and_exposes_the_effective_cadence(
    status_store: StatusStore,
) -> None:
    """PLAN.md §7.3: honour Retry-After and record the cadence actually in force."""
    client = FakeGraphQL(CarrierRateLimitError("carrier graphql: HTTP 429", retry_after_s=120.0))
    client.fields.update(throttled=True, retry_after_s=120.0, throttle_events=1)
    source = make_source(client, status_store=status_store)

    with pytest.raises(SourceTransientError):
        await source.poll()

    section = status_store.snapshot()[STATUS_SECTION]
    assert section["retry_after_s"] == 120.0
    assert section["throttled"] is True
    assert section["poll_interval_s"] == 30
    assert section["effective_interval_s"] == 120.0
    assert section["consecutive_failures"] == 1


async def test_the_source_never_writes_the_bryant_status_section() -> None:
    """DEVIATIONS.md #20: that section belongs to stages/poller.py, exclusively."""
    store = _NullStatus()
    source = make_source("status_multizone", status_store=store)

    await source.poll()

    assert "bryant_status" not in store.sections
    assert store.sections <= {STATUS_SECTION}


async def test_status_is_not_rewritten_on_every_identical_cycle() -> None:
    """At 30s, an unconditional write would rewrite the file 2,880 times a day."""
    store = _NullStatus()
    source = make_source(FakeGraphQL(load_fixture("status_multizone")), status_store=store)

    for _ in range(5):
        await source.poll()

    assert len(store.calls) == 1


async def test_a_success_after_a_failure_is_recorded_as_a_success(
    status_store: StatusStore,
) -> None:
    """A recovered transport must stop reporting as failing.

    A failed cycle stamps the *same* comparable counters as a successful one, so
    an equality short-circuit that ignores the failure leaves ``status.json``
    claiming the Carrier transport is permanently down — with a frozen
    ``last_success_utc`` — until some unrelated counter happens to move.
    """
    store = _NullStatus()
    client = FakeGraphQL(
        load_fixture("status_multizone"),
        CarrierTransientError("carrier graphql: HTTP 502"),
        load_fixture("status_multizone"),
    )
    source = make_source(client, status_store=store)

    for _ in range(4):
        try:
            await source.poll()
        except SourceTransientError:
            pass

    assert [call[0] for call in store.calls] == [
        "record_success",
        "record_failure",
        "record_success",  # the recovery, which the short-circuit swallowed
    ]


async def test_the_status_ledger_clears_the_error_after_a_recovery(
    status_store: StatusStore,
) -> None:
    """The same defect, seen through a real :class:`StatusStore`."""
    client = FakeGraphQL(
        load_fixture("status_multizone"),
        CarrierTransientError("carrier graphql: HTTP 502"),
        load_fixture("status_multizone"),
    )
    source = make_source(client, status_store=status_store)

    for _ in range(4):
        try:
            await source.poll()
        except SourceTransientError:
            pass

    section = status_store.snapshot()[STATUS_SECTION]
    assert section["consecutive_failures"] == 0
    assert "last_error" not in section


# ============================================================================
# The query, and the operation fallback
# ============================================================================


def test_both_queries_request_the_same_status_fields() -> None:
    """The fallback must be field-for-field identical or the schema would drift."""
    for field in ("cfgem", "isDisconnected", "oat", "zones", "enabled", "htsp", "opstat"):
        assert field in STATUS_QUERY, field
        assert field in SYSTEMS_QUERY, field
    assert "infinityStatus(serial: $serial)" in STATUS_QUERY
    assert "infinitySystems(userName: $userName)" in SYSTEMS_QUERY
    # The giant `config { }` block the reference also fetches is not requested.
    assert "config" not in SYSTEMS_QUERY


async def test_the_per_serial_query_is_sent_with_the_serial_as_its_variable() -> None:
    client = FakeGraphQL(load_fixture("status_multizone"))
    source = make_source(client)

    await source.poll()

    assert client.operations == [OPERATION_STATUS]
    assert client.calls[0].variables == {"serial": SERIAL}
    assert client.calls[0].query == STATUS_QUERY


async def test_a_null_infinity_status_falls_back_to_the_reference_proven_query(
    log_stream: io.StringIO,
) -> None:
    """``infinityStatus(serial:)`` is schema-verified but executed by nobody."""
    client = FakeGraphQL({"infinityStatus": None}, load_fixture("status_systems_fallback"))
    source = make_source(client)

    rows = rows_by_key(await source.poll())

    assert client.operations == [OPERATION_STATUS, OPERATION_SYSTEMS]
    assert client.calls[1].variables == {"userName": "carrier-user@example.invalid"}
    assert source.operation == OPERATION_SYSTEMS
    assert rows[(SYSTEM_CHANNEL, "outdoor_temp_f")] == (88.0, "degF")  # OUR system
    assert any(
        e["event"] == "bryant_status_query_fallback" for e in log_events(log_stream)
    )


async def test_a_graphql_error_on_the_new_query_also_triggers_the_fallback() -> None:
    client = FakeGraphQL(
        CarrierGraphQLError("Cannot query field infinityStatus"),
        load_fixture("status_systems_fallback"),
    )
    source = make_source(client)

    assert await source.poll()
    assert source.operation == OPERATION_SYSTEMS


async def test_the_fallback_is_pinned_only_after_it_actually_works() -> None:
    """A transient failure of the fallback must not abandon the cheaper query."""
    client = FakeGraphQL(
        CarrierGraphQLError("Cannot query field infinityStatus"),
        CarrierTransientError("carrier graphql: HTTP 503"),
        load_fixture("status_multizone"),
    )
    source = make_source(client)

    with pytest.raises(SourceTransientError):
        await source.poll()
    assert source.operation == OPERATION_STATUS

    assert await source.poll()


async def test_a_field_level_authorization_error_also_triggers_the_fallback() -> None:
    """Permission-gating is the likeliest way ``infinityStatus(serial:)`` fails.

    ``carrier_auth._errors_are_auth`` reclassifies a 200-with-``errors`` payload
    saying "not authorized" as a :class:`CarrierAuthError`, so a fallback that
    only caught :class:`CarrierGraphQLError` would be defeated in exactly the
    case it exists for. The ``errors`` array is what separates "the gateway
    rejected this FIELD" from "our token is bad".
    """
    denied = CarrierAuthError(
        "carrier getInfinityStatus: GraphQL auth error (Not authorized)",
        errors=[
            {
                "message": "Not authorized to access field 'infinityStatus'",
                "path": ["infinityStatus"],
                "extensions": {"code": "FORBIDDEN"},
            }
        ],
    )
    client = FakeGraphQL(denied, load_fixture("status_systems_fallback"))
    source = make_source(client)

    rows = rows_by_key(await source.poll())

    assert client.operations == [OPERATION_STATUS, OPERATION_SYSTEMS]
    assert source.operation == OPERATION_SYSTEMS
    assert rows[(SYSTEM_CHANNEL, "outdoor_temp_f")] == (88.0, "degF")


async def test_a_field_level_denial_with_no_fallback_is_a_data_error() -> None:
    """With the fallback disabled it is still not our token that is at fault."""
    denied = CarrierAuthError(
        "carrier getInfinityStatus: GraphQL auth error (Not authorized)",
        errors=[{"message": "not authorized", "path": ["infinityStatus"]}],
    )
    source = make_source(FakeGraphQL(denied), allow_fallback=False)

    with pytest.raises(SourceTransientError):
        await source.poll()


async def test_a_transport_401_is_an_auth_failure_not_a_fallback_trigger() -> None:
    """A dead token says nothing about which schema field resolves."""
    client = FakeGraphQL(CarrierAuthError("carrier getInfinityStatus: HTTP 401"))
    source = make_source(client)

    with pytest.raises(SourceAuthError):
        await source.poll()

    assert client.operations == [OPERATION_STATUS]
    assert source.operation == OPERATION_STATUS


async def test_a_transport_failure_is_not_a_reason_to_change_the_query() -> None:
    """5xx/429/401 say nothing about which schema field resolves."""
    client = FakeGraphQL(CarrierTransientError("carrier graphql: HTTP 502"))
    source = make_source(client)

    with pytest.raises(SourceTransientError):
        await source.poll()

    assert client.operations == [OPERATION_STATUS]
    assert source.operation == OPERATION_STATUS


async def test_the_fallback_picks_our_serial_not_the_first_system() -> None:
    client = FakeGraphQL(load_fixture("status_systems_fallback"))
    source = make_source(client, operation=OPERATION_SYSTEMS)

    rows = rows_by_key(await source.poll())

    # The foreign system reports oat 55 / mode off; ours reports 88 / cool.
    assert rows[(SYSTEM_CHANNEL, "outdoor_temp_f")] == (88.0, "degF")
    assert rows[(SYSTEM_CHANNEL, "mode")] == (2.0, "enum")


async def test_a_serial_that_is_not_in_the_response_is_an_error_not_a_guess() -> None:
    """Assuming "there is only one system" would archive a stranger's house."""
    payload = load_fixture("status_systems_fallback")
    payload["infinitySystems"] = payload["infinitySystems"][:1]  # only the foreign one
    source = make_source(FakeGraphQL(payload), operation=OPERATION_SYSTEMS)

    with pytest.raises(SourceTransientError):
        await source.poll()


# ============================================================================
# Wiring: the real transport, the poller registry, the interval floor
# ============================================================================


async def test_the_query_reaches_the_wire_through_the_real_graphql_client(
    tmp_path: Path,
) -> None:
    """One end-to-end pass over httpx.MockTransport: no fake client, no socket."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "oauth2" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "access_token": "bryant-test-access-0123456789",
                    "refresh_token": "bryant-test-refresh-0123456789",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            )
        return httpx.Response(
            200, json={"data": load_fixture("status_single_zone")}
        )

    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport)
    auth = CarrierAuth(
        username="carrier-user@example.invalid",
        password="not-a-real-carrier-password",
        token_path=tmp_path / "carrier.json",
        client=http,
        owns_client=False,
    )
    client = CarrierGraphQLClient(auth, client=http, owns_client=True)
    source = BryantStatusSource(
        get_settings(),
        client=client,
        device_id=SERIAL,
        status_store=_NullStatus(),
    )
    try:
        rows = await source.poll()
    finally:
        await client.close()
        await http.aclose()

    assert len(rows) == 14
    graphql_request = seen[-1]
    body = json.loads(graphql_request.content.decode("utf-8"))
    assert body["operationName"] == OPERATION_STATUS
    assert body["variables"] == {"serial": SERIAL}
    assert "infinityStatus" in body["query"]
    assert graphql_request.headers["authorization"].startswith("Bearer ")
    assert graphql_request.headers["origin"] == "https://my.carrier.com"


def test_the_poller_discovers_this_source_by_its_registered_name(
    settings: Settings,
) -> None:
    """``poller.SOURCE_FACTORIES['bryant']`` expects exactly this class name."""
    source = SOURCE_FACTORIES["bryant"](settings)

    assert isinstance(source, BryantStatusSource)
    assert isinstance(source, Source)
    assert source.name == SOURCE_BRYANT == "bryant"


def test_constructing_the_source_opens_no_client_and_needs_no_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Credentials are demanded at the point of use, so a build never explodes."""
    monkeypatch.delenv("CARRIER_USERNAME", raising=False)
    monkeypatch.delenv("CARRIER_PASSWORD", raising=False)
    from energy_capture.config import reset_settings_cache

    reset_settings_cache()

    source = BryantStatusSource(get_settings())

    assert source._client is None
    assert source.device_id  # CARRIER_SERIAL has a default


async def test_a_missing_credential_is_an_auth_failure_not_a_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop must show "your credentials are missing", not a traceback."""
    monkeypatch.delenv("CARRIER_USERNAME", raising=False)
    monkeypatch.delenv("CARRIER_PASSWORD", raising=False)
    from energy_capture.config import reset_settings_cache

    reset_settings_cache()
    source = BryantStatusSource(get_settings(), status_store=_NullStatus())

    with pytest.raises(SourceAuthError, match="CARRIER_"):
        await source.poll()

    assert source.consecutive_failures == 1


def test_the_poll_interval_floor_is_enforced_in_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PLAN.md §7.3/§6.6: never faster than 30s, whatever the env var says."""
    monkeypatch.setenv("BRYANT_POLL_INTERVAL_S", "5")
    from energy_capture.config import reset_settings_cache

    reset_settings_cache()

    source = make_source("status_multizone", settings=get_settings())

    assert source.poll_interval_s == 30


async def test_a_shared_client_is_not_closed_by_this_source() -> None:
    """``carrier_stack_from_settings`` is shared with the daily energy stage."""
    client = FakeGraphQL(load_fixture("status_multizone"))
    source = make_source(client)

    await source.close()

    assert client.closed == 0


async def test_the_source_needs_no_background_tasks() -> None:
    """Zones come free with every poll; a discovery task would double the calls."""
    assert make_source("status_multizone").background_tasks() == ()


# ============================================================================
# Discovery (PLAN.md §9)
# ============================================================================


async def test_discovery_lists_the_system_and_only_the_enabled_zones() -> None:
    source = make_source("status_single_zone")

    discovery = await source.discover(force=True)

    assert [d.device_id for d in discovery.devices] == [SERIAL]
    assert [c.channel_id for c in discovery.channels] == [SYSTEM_CHANNEL, "zone_1"]
    assert discovery.channel_keys() == {
        (SOURCE_BRYANT, SERIAL, SYSTEM_CHANNEL),
        (SOURCE_BRYANT, SERIAL, "zone_1"),
    }


async def test_discovery_prints_paste_ready_channel_map_skeletons() -> None:
    source = make_source("status_multizone")

    skeleton = (await source.discover(force=True)).skeleton()

    assert [entry["channel_id"] for entry in skeleton] == [
        SYSTEM_CHANNEL,
        "zone_1",
        "zone_3",
    ]
    assert all(entry["source"] == SOURCE_BRYANT for entry in skeleton)


async def test_a_poll_refreshes_the_discovery_cache_for_free() -> None:
    client = FakeGraphQL(load_fixture("status_multizone"))
    source = make_source(client)

    await source.poll()

    assert len(client.calls) == 1, "poll must not cost a second request"
    cached = source.cached_discovery
    assert cached is not None
    assert {c.channel_id for c in cached.channels} == {SYSTEM_CHANNEL, "zone_1", "zone_3"}


# ============================================================================
# Secrets
# ============================================================================


async def test_no_credential_reaches_the_log_stream(log_stream: io.StringIO) -> None:
    """CLAUDE.md rule 8, checked on this module's own log surface."""
    client = FakeGraphQL(
        load_fixture("status_nulls"),
        CarrierTransientError("carrier graphql: HTTP 502"),
    )
    source = make_source(client)

    await source.poll()
    with pytest.raises(SourceTransientError):
        await source.poll()

    output = log_stream.getvalue()
    assert output.strip()
    assert "not-a-real-carrier-password" not in output
    for line in output.splitlines():
        json.loads(line)  # every line is still valid JSON


def test_status_fields_are_json_safe_counters_only() -> None:
    """Whatever this returns lands in status.json, so it must hold no secret."""
    source = make_source("status_multizone")

    fields = source.status_fields()

    json.dumps(fields)  # raises if anything is not serialisable
    assert set(fields) >= {
        "operation",
        "poll_interval_s",
        "effective_interval_s",
        "zones_enabled",
        "disconnected",
        "unknown_enum_values",
        "numeric_stage_samples",
        "stage_representation",
        "stage_representation_changes",
        "stage_pct_rows",
        "stage_enum_rows",
        "stage_pct_out_of_range",
        "distinct_warnings",
    }
    assert all(
        not isinstance(value, str) or len(value) < 64 for value in fields.values()
    ), "a long opaque string in status.json is how a token leaks"
    assert fields["operation"] == OPERATION_STATUS
    assert fields["poll_interval_s"] == 30
    assert fields["effective_interval_s"] == 30


def test_the_module_contains_no_auth_code() -> None:
    """Auth lives in carrier_auth.py; a second copy would be a second bug source."""
    text = Path(bryant_module.__file__).read_text(encoding="utf-8")
    for forbidden in ("grant_type", "client_id", "sso.carrier.com", "carrier_password"):
        assert forbidden not in text, f"{forbidden!r} must not appear in sources/bryant.py"


async def test_start_runs_exactly_one_discovery_pass() -> None:
    client = FakeGraphQL(load_fixture("status_multizone"))
    source = make_source(client)

    await source.start()

    assert len(client.calls) == 1
    assert source.zone_ids == ("1", "3")


async def test_a_source_that_cannot_start_raises_the_documented_error() -> None:
    """The poller catches this and keeps the source (DEVIATIONS.md #46)."""
    source = make_source(FakeGraphQL(CarrierTransientError("carrier graphql: HTTP 503")))

    with pytest.raises(SourceTransientError):
        await source.start()
