"""Replay of the REAL Carrier response, whole and untrimmed (PLAN.md §7.3).

Every other Bryant test replays a fixture someone *shaped* — one zone, two
zones, a null here, a Celsius payload there. This module replays the one payload
nobody shaped: ``tests/fixtures/bryant/status_live_capture.json`` is the
``getInfinityStatus`` response the live Carrier cloud returned to ``energycap
discover`` on 2026-08-17, copied field for field out of the gitignored
``data/discover-raw.json`` with all eight reported zones intact and the system
serial (which appears only in the request *variables*) redacted.

That is what makes this the highest-value test in the ``stage_pct`` change: it
is evidence about the hardware, not about our imagination of it. The house has a
**variable-capacity** outdoor unit (``odu.type = "gs3ngiphp"``, Greenspeed), so
``odu.opstat`` is the string ``"35"`` — a 0-100 compressor capacity percentage,
not a word :data:`~energy_capture.sources.bryant.STAGE_CODES` can encode. Before
``stage_pct`` existed this payload produced **no compressor row at all** and one
``bryant_enum_numeric`` WARNING per poll — 2,880 a day — for the single signal
that correlates with watts. What is pinned here:

1. the exact row set this payload produces, metric by metric, including one
   ``stage_pct`` = 35.0 ``pct`` on ``system`` and **zero** ``stage`` rows;
2. that the seven phantom zones — which report humidity and both setpoints —
   contribute nothing, because they are ``enabled: "off"``;
3. that twenty consecutive numeric cycles log at most one WARNING, which is the
   log-spam defect stated as an assertion rather than as prose;
4. that the append-only enum tables still decode this real payload's ``mode``
   and ``fan``, unrenumbered, and that ``stage``'s absence is a gap and not a
   silently-invented code;
5. that the trimmed ``status_varcap.json`` — which the Glue and README tests
   pin their claims to — still says the same thing about the same house.

Offline like the rest of the suite: the transport is a fake, no socket is opened
(``tests/conftest.py`` enforces that), and ``data/discover-raw.json`` is *not*
required to be present. When it happens to be (the collector host), one test
additionally re-derives the fixture from it, so drift between the capture and
the committed copy is caught where it happens.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from energy_capture import model
from energy_capture.aws import glue
from energy_capture.sources import bryant as bryant_module
from energy_capture.sources.bryant import (
    FAN_CODES,
    MODE_CODES,
    OPERATION_STATUS,
    STAGE_CODES,
    STAGE_METRIC,
    STAGE_PCT_METRIC,
    STAGE_REPR_PCT,
    SYSTEM_CHANNEL,
)

from tests.test_bryant_status import (
    FIXTURE_DIR,
    SERIAL,
    FakeGraphQL,
    log_events,
    log_stream,  # noqa: F401 — re-exported pytest fixture
    make_source,
    rows_by_key,
)

#: The verbatim capture. Committed; the raw file it came from is not.
CAPTURE = FIXTURE_DIR / "status_live_capture.json"

#: The same capture, trimmed to two zones — what the Glue/README tests read.
TRIMMED = FIXTURE_DIR / "status_varcap.json"

#: Where ``energycap discover --raw`` wrote the original. Gitignored, mode 0600,
#: present only on the machine that made the call.
RAW_CAPTURE = Path(__file__).resolve().parent.parent / "data" / "discover-raw.json"


def _capture() -> dict[str, Any]:
    return json.loads(CAPTURE.read_text(encoding="utf-8"))


def _status() -> dict[str, Any]:
    return _capture()["data"]["infinityStatus"]


def _source() -> bryant_module.BryantStatusSource:
    """A source that will answer with the live capture on every cycle."""
    return make_source(FakeGraphQL(_capture()["data"]))


# ============================================================================
# The fixture is the capture
# ============================================================================


def test_the_committed_capture_is_the_untrimmed_live_response() -> None:
    """Guard the evidence. Every assertion below is about *this* payload.

    If someone edits the fixture into a hypothetical, the rest of this module
    would keep passing while proving nothing — so the shape of the capture is
    itself asserted, and the serial's absence with it.
    """
    payload = _capture()
    status = payload["data"]["infinityStatus"]

    assert payload["_capture"]["operation"] == OPERATION_STATUS
    assert payload["_capture"]["variables"] == {"serial": "<CARRIER_SERIAL>"}
    # The house's serial is a credential-adjacent identifier and never lands in
    # the repo; the envelope itself never carried it.
    text = CAPTURE.read_text(encoding="utf-8")
    assert "4022W" not in text.upper()
    assert "serial" not in json.dumps(status)

    assert status["odu"]["type"] == "gs3ngiphp"  # Greenspeed = variable capacity
    assert status["odu"]["opstat"] == "35"  # a percentage string, not a word
    assert status["odu"]["opstat"].lower() not in STAGE_CODES
    assert status["idu"]["opstat"] == "off"  # a word, in the same payload
    assert status["cfgem"] == "F"
    assert status["isDisconnected"] is False
    assert len(status["zones"]) == 8
    assert [z["id"] for z in status["zones"] if z["enabled"] == "on"] == ["1"]


def test_the_committed_capture_still_matches_the_raw_file_when_it_is_present() -> None:
    """Drift check, on the one machine that can perform it.

    ``data/discover-raw.json`` is gitignored, so on any other machine this test
    asserts what it can: that the fixture records where it came from. On the
    collector it re-derives the payload from the raw capture and demands they be
    identical — the point at which a hand-edit of the fixture would otherwise
    become invisible.
    """
    payload = _capture()
    assert payload["_capture"]["source_file"].startswith("data/discover-raw.json")

    if not RAW_CAPTURE.exists():
        return
    raw = json.loads(RAW_CAPTURE.read_text(encoding="utf-8"))
    entry = next(
        e
        for e in raw["sources"]["bryant"]["graphql"]
        if e["operation"] == OPERATION_STATUS
    )
    assert payload["data"]["infinityStatus"] == entry["data"]["infinityStatus"], (
        "tests/fixtures/bryant/status_live_capture.json no longer matches "
        "data/discover-raw.json — re-derive it rather than editing it"
    )


def test_the_trimmed_fixture_the_docs_are_pinned_to_is_the_same_house() -> None:
    """``status_varcap.json`` is this capture minus six phantom zones.

    ``tests/test_glue.py`` and ``tests/test_docs.py`` check their claims about
    which metric this system emits against the *trimmed* fixture. That is only
    honest while the trim still describes the captured hardware, so the two are
    compared here — the one place that knows both files exist.
    """
    trimmed = json.loads(TRIMMED.read_text(encoding="utf-8"))["data"]["infinityStatus"]
    live = _status()

    assert trimmed["odu"] == live["odu"]
    assert trimmed["idu"] == live["idu"]
    for field in ("cfgem", "mode", "oat", "isDisconnected", "utcTime"):
        assert trimmed[field] == live[field], field
    assert trimmed["zones"] == live["zones"][:2], "the trim is no longer a prefix"
    assert bryant_module.stage_metric_for(live["odu"]["opstat"]) == STAGE_PCT_METRIC


# ============================================================================
# The rows the real payload produces
# ============================================================================


async def test_the_live_capture_emits_one_stage_pct_row_at_35_and_no_stage_row() -> None:
    """The whole reason ``stage_pct`` exists, asserted against the real payload.

    ``"35"`` is not a stage; it is 35% compressor capacity. It is emitted
    verbatim (not rounded, not scaled, not bucketed into the enum) under a
    metric whose *name* says which rendering it is.
    """
    rows = await _source().poll()

    stage_pct = [row for row in rows if row.metric == STAGE_PCT_METRIC]
    assert len(stage_pct) == 1
    assert stage_pct[0].value == 35.0
    assert stage_pct[0].unit == "pct" == model.unit_for_metric(STAGE_PCT_METRIC)
    assert stage_pct[0].channel_id == SYSTEM_CHANNEL
    assert stage_pct[0].device_id == SERIAL
    assert stage_pct[0].source == model.SOURCE_BRYANT
    # Not "a stage row with a strange value" — no stage row at all, ever, here.
    assert not [row for row in rows if row.metric == STAGE_METRIC]


async def test_the_live_capture_produces_exactly_this_row_set() -> None:
    """Ten rows, whole-payload, pinned — the record of what this house reports.

    Read the absences as carefully as the presences: no ``stage`` (the metric
    this hardware cannot express), and nothing at all from ``zone_2``-``zone_8``.
    """
    rows = await _source().poll()

    assert rows_by_key(rows) == {
        (SYSTEM_CHANNEL, "outdoor_temp_f"): (74.0, "degF"),
        (SYSTEM_CHANNEL, "mode"): (2.0, "enum"),  # cool
        (SYSTEM_CHANNEL, STAGE_PCT_METRIC): (35.0, "pct"),
        (SYSTEM_CHANNEL, "blower_rpm"): (433.0, "rpm"),  # idu.blwrpm
        (SYSTEM_CHANNEL, "cfm"): (500.0, "CFM"),  # idu.cfm
        ("zone_1", "indoor_temp_f"): (68.0, "degF"),
        ("zone_1", "humidity_pct"): (53.0, "pct"),
        ("zone_1", "setpoint_heat_f"): (64.0, "degF"),
        ("zone_1", "setpoint_cool_f"): (68.0, "degF"),
        ("zone_1", "fan"): (3.0, "enum"),  # high
    }
    assert len(rows) == 10
    # PLAN.md §6.5: one cycle, one instant, so a rollup buckets them together.
    assert len({row.ts_utc for row in rows}) == 1


async def test_the_seven_phantom_zones_contribute_nothing() -> None:
    """They report ``rh``, ``htsp`` and ``clsp``; they are still not rooms.

    Filtering on nullness instead of ``enabled == "on"`` would fabricate seven
    zones' worth of humidity and setpoints every 30 seconds — ~80k invented rows
    a day, all of them plausible.
    """
    status = _status()
    phantoms = [z for z in status["zones"] if z["enabled"] != "on"]
    assert len(phantoms) == 7
    assert all(z["rh"] and z["htsp"] and z["clsp"] for z in phantoms), (
        "the phantom zones no longer carry readings, so this test proves nothing"
    )

    rows = await _source().poll()

    assert {row.channel_id for row in rows} == {SYSTEM_CHANNEL, "zone_1"}


async def test_the_live_capture_answers_discovery_without_polling() -> None:
    """``energycap discover`` settles DEVIATIONS.md #75.1/.2/.6/.10 in one call."""
    discovery = await _source().discover(force=True)

    device = discovery.devices[0].details
    assert device["operation"] == OPERATION_STATUS  # #75.2: the per-serial query resolves
    assert device["temperature_unit"] == "F"  # #75.10: cfgem is Fahrenheit
    assert (device["zones_enabled"], device["zones_reported"]) == (1, 8)  # #75.6
    assert device["disconnected"] is False

    system = next(c for c in discovery.channels if c.channel_id == SYSTEM_CHANNEL)
    assert system.details["odu_type"] == "gs3ngiphp"
    assert system.details["odu_opstat"] == "35"
    assert system.details["stage_metric"] == STAGE_PCT_METRIC  # #75.1
    assert [c.channel_id for c in discovery.channels] == [SYSTEM_CHANNEL, "zone_1"]


# ============================================================================
# The log-spam defect, as an assertion
# ============================================================================


async def test_twenty_consecutive_live_cycles_log_at_most_one_warning(
    log_stream: io.StringIO,  # noqa: F811 — the imported fixture
) -> None:
    """N numeric cycles, ≤ 1 WARNING — not N (the observed defect).

    The live run logged ``bryant_enum_numeric`` at WARNING on **every** cycle,
    because a variable-capacity system is in the numeric branch on every cycle.
    At 30s that is 2,880 identical lines a day, which is how a real warning gets
    missed. The condition is now INFO, once, on first observation; the volume
    lives in ``status.json``'s counters.
    """
    source = _source()

    cycles = 20
    for _ in range(cycles):
        rows = await source.poll()
        assert len([r for r in rows if r.metric == STAGE_PCT_METRIC]) == 1

    events = log_events(log_stream)
    warnings = [e for e in events if e["level"] in ("WARNING", "ERROR")]
    assert len(warnings) <= 1, (
        f"{len(warnings)} warnings in {cycles} steady cycles: "
        f"{[e['event'] for e in warnings]}"
    )
    assert not warnings, "a steady variable-capacity system warns about nothing"

    told_once = [e for e in events if e["event"] == "bryant_stage_representation"]
    assert len(told_once) == 1
    assert told_once[0]["level"] == "INFO"
    assert told_once[0]["representation"] == STAGE_REPR_PCT
    assert told_once[0]["changed"] is False

    # The cycles genuinely happened and were genuinely numeric — otherwise
    # "no warnings" would be true for the wrong reason.
    fields = source.status_fields()
    assert fields["numeric_stage_samples"] == cycles
    assert fields["stage_pct_rows"] == cycles
    assert fields["stage_enum_rows"] == 0
    assert fields["stage_pct_out_of_range"] == 0
    assert source.stage_representation == STAGE_REPR_PCT


# ============================================================================
# Nothing was renumbered, and the vocabularies still agree
# ============================================================================


def test_the_live_capture_renumbered_no_enum_table() -> None:
    """``stage_pct`` ADDED a metric. It retired and renumbered nothing.

    ``stage``'s codes stay exactly where they were even though this house can
    never emit one: the archive outlives the hardware, and a replacement outdoor
    unit must decode against the same table.
    """
    assert dict(STAGE_CODES) == {
        "off": 0,
        "low": 1,
        "high": 2,
        "idle": 3,
        "dehumidify": 4,
    }
    assert MODE_CODES["cool"] == 2 and FAN_CODES["high"] == 3
    assert STAGE_PCT_METRIC not in model.ENUM_METRICS
    assert STAGE_METRIC in model.ENUM_METRICS


async def test_every_metric_this_house_emits_is_in_every_vocabulary() -> None:
    """model → Glue → README, checked against real rows rather than a list.

    A metric that a live payload produces but a catalog does not name is a row
    an LLM will never find; ``stage_pct`` was exactly that until this change.
    """
    rows = await _source().poll()
    emitted = {row.metric for row in rows}
    assert STAGE_PCT_METRIC in emitted

    readme = (Path(__file__).resolve().parent.parent / "README.md").read_text(
        encoding="utf-8"
    )
    raw_30s = set(glue._metrics_for_table(glue.TABLE_ENERGY_RAW_30S))
    hourly = set(glue._metrics_for_table(glue.TABLE_ENERGY_HOURLY))

    for metric in sorted(emitted):
        row = next(r for r in rows if r.metric == metric)
        assert model.unit_for_metric(metric) == row.unit, metric
        assert metric in raw_30s, f"{metric} can be collected but no raw table holds it"
        assert metric in hourly, f"{metric} is never rolled up"
        assert f"`{metric}`" in readme, f"the README never names `{metric}`"
