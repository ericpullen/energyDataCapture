"""The Bryant daily energy fetch (PLAN.md §7.2, §15.2, §15.3).

Everything here runs offline: the Carrier response comes from a committed
fixture, the one test that exercises the real transport does it over
``httpx.MockTransport``, and S3 is ``moto``. No socket is opened and no
credential is real.

The properties these tests exist to pin, in the order PLAN.md §7.2 states them:

* the camelCase/lowercase casing split between ``energyPeriods`` and
  ``energyConfig`` is spelled out per component, not derived;
* a component whose ``energyConfig`` says ``enabled: false`` emits **nothing** —
  never a zero, because it is structurally absent (CLAUDE.md rule 1). An
  *enabled* component reporting ``0`` does emit that zero: it is a measurement;
* ``ts_utc`` is local midnight of the measured day converted to UTC, verified on
  **both** DST transition days (§15.3);
* ``day1`` and ``day2`` both land, and a ``day2`` revision of a date already
  written as ``day1`` collapses on the canonical dedupe key, fresher value
  winning (§15.2);
* ``gasKwh`` enabled *and* nonzero is written verbatim and WARNed, never
  converted;
* a re-run is byte-identical, and the monthly file is regenerated **whole** —
  rows the fetch has no opinion about (backfilled history in the same month)
  survive it;
* day-grain rows never go anywhere near ``raw_30s`` (CLAUDE.md rule 6).
"""

from __future__ import annotations

import asyncio
import io
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore
from energy_capture.logging import configure_logging
from energy_capture.sources import carrier_auth
from energy_capture.stages import daily
from tests.conftest import BUCKET, utc

FIXTURES = Path(__file__).parent / "fixtures" / "bryant"

#: Matches ``CARRIER_SERIAL`` in the pinned test environment.
SERIAL = "TEST0000001"

#: The reference "now" for most tests: 08:30 local on 2026-08-17 (EDT, UTC-4),
#: which is when the scheduler fires. day1 = 2026-08-16, day2 = 2026-08-15.
NOW = utc(2026, 8, 17, 12, 30)
DAY1 = date(2026, 8, 16)
DAY2 = date(2026, 8, 15)

#: The four components enabled on this house (heat pump + electric strips).
ENABLED = ("eheat", "cooling", "fan", "hpheat")
#: The four that are structurally absent — no gas, no reheat, no loop pump.
DISABLED = ("fangas", "gas", "looppump", "reheat")


# ----------------------------------------------------------------- helpers


def response(name: str) -> dict[str, Any]:
    """A whole GraphQL response body, exactly as the transport would see it."""
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def payload(name: str) -> dict[str, Any]:
    """The ``infinityEnergy`` object out of a fixture response."""
    return response(name)["data"]["infinityEnergy"]


@pytest.fixture
def status(tmp_path: Path) -> StatusStore:
    return StatusStore(tmp_path / "status.json", load_existing=False)


@pytest.fixture
def log_stream():
    """Capture the structured JSON log lines this stage emits."""
    buffer = io.StringIO()
    configure_logging("DEBUG", stream=buffer, force=True)
    yield buffer
    configure_logging("INFO", stream=io.StringIO(), force=True)


def log_events(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in stream.getvalue().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("event") == event:
            out.append(record)
    return out


def cells(observations) -> dict[tuple[str, str, str], float]:
    """``(local_date, channel_id, metric) -> value`` — the whole row set, flat."""
    return {
        (o.ts_local.date().isoformat(), o.channel_id, o.metric): o.value
        for o in observations
    }


def all_rows(days: list[daily.DayRows]) -> list[model.Observation]:
    return [obs for day in days for obs in day.observations]


def table_cells(table) -> dict[tuple[str, str, str], float]:
    return {
        (row["ts_local"].date().isoformat(), row["channel_id"], row["metric"]): row["value"]
        for row in table.to_pylist()
    }


def read_key(s3_client, key: str) -> pa.Table:
    body = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pa_pq.read_table(pa.BufferReader(body))


def object_bytes(s3_client, key: str) -> bytes:
    return s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def bucket_keys(s3_client) -> list[str]:
    listing = s3_client.list_objects_v2(Bucket=BUCKET)
    return sorted(item["Key"] for item in listing.get("Contents", ()))


# =========================================================== the component map


def test_the_component_table_spells_out_all_three_names() -> None:
    """The casing trap of PLAN.md §7.2, pinned component by component.

    ``energyConfig`` is lowercase, ``energyPeriods`` is camelCase, and no rule
    that lowercases one into the other survives ``hPHeat`` / ``fanGas`` /
    ``loopPump``. If this test fails, a component is about to silently vanish.
    """
    assert [
        (s.channel_id, s.config_key, s.kwh_field, s.dollars_field)
        for s in daily.COMPONENTS
    ] == [
        ("eheat", "eheat", "eHeatKwh", "eHeatDollars"),
        ("cooling", "cooling", "coolingKwh", "coolingDollars"),
        ("fan", "fan", "fanKwh", "fanDollars"),
        ("fangas", "fangas", "fanGasKwh", "fanGasDollars"),
        ("hpheat", "hpheat", "hPHeatKwh", "hPHeatDollars"),
        ("looppump", "looppump", "loopPumpKwh", "loopPumpDollars"),
        ("gas", "gas", "gasKwh", "gasDollars"),
        ("reheat", "reheat", "reheatKwh", "reheatDollars"),
    ]


def test_the_channel_ids_are_the_eight_lowercase_component_names() -> None:
    assert sorted(s.channel_id for s in daily.COMPONENTS) == sorted(
        ["eheat", "cooling", "fan", "fangas", "hpheat", "looppump", "gas", "reheat"]
    )


def test_this_stage_and_backfill_agree_on_the_mapping() -> None:
    """The live fetch and the backfill write the same rows into the same files.

    They must therefore agree on every ``(camelCase field -> channel_id,
    metric)`` pair, or a backfilled day and a fetched day would land under
    different ``channel_id``s and never dedupe against each other.
    """
    # No importorskip: backfill has landed, and an escape hatch here would let
    # the two tables drift apart silently — which is the one failure this test
    # exists to prevent.
    from energy_capture.stages import backfill

    theirs = {
        (spec.attribute, spec.channel_id, spec.metric) for spec in backfill.ATTRIBUTE_MAP
    }
    ours = {
        (field, spec.channel_id, metric)
        for spec in daily.COMPONENTS
        for field, metric in (
            (spec.kwh_field, daily.METRIC_KWH),
            (spec.dollars_field, daily.METRIC_COST),
        )
    }
    assert ours == theirs
    assert len(ours) == 16  # 8 components x {kWh, USD}
    # ...and on the component list itself, so a component added to one stage and
    # not the other fails here rather than at 08:30 in production.
    assert tuple(spec.channel_id for spec in daily.COMPONENTS) == backfill.COMPONENTS


def test_the_query_is_the_ported_getInfinityEnergy_operation() -> None:
    """Ported from the old collector's proven query — same operation, same fields."""
    assert daily.OPERATION_NAME == "getInfinityEnergy"
    assert "query getInfinityEnergy($serial: String!)" in daily.ENERGY_QUERY
    assert "infinityEnergy(serial: $serial)" in daily.ENERGY_QUERY
    # energyConfig must ride along in the SAME query: the `enabled` flag is what
    # separates "structurally absent" from "measured zero" (PLAN.md §7.2).
    assert "energyConfig" in daily.ENERGY_QUERY
    assert "energyPeriodType" in daily.ENERGY_QUERY
    for spec in daily.COMPONENTS:
        assert f"{spec.config_key} {{ display enabled }}" in daily.ENERGY_QUERY
        assert spec.kwh_field in daily.ENERGY_QUERY
        assert spec.dollars_field in daily.ENERGY_QUERY


# ================================================================== mapping


def test_a_realistic_response_maps_to_exactly_the_expected_rows() -> None:
    days = daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))

    assert [(d.period_type, d.local_day) for d in days] == [("day1", DAY1), ("day2", DAY2)]
    assert cells(all_rows(days)) == {
        # day1 = 2026-08-16
        ("2026-08-16", "eheat", "kwh_day"): 4.0,
        ("2026-08-16", "eheat", "cost_day_usd"): 0.44,
        ("2026-08-16", "cooling", "kwh_day"): 0.0,
        ("2026-08-16", "cooling", "cost_day_usd"): 0.0,
        ("2026-08-16", "fan", "kwh_day"): 3.0,
        ("2026-08-16", "fan", "cost_day_usd"): 0.33,
        ("2026-08-16", "hpheat", "kwh_day"): 21.0,
        ("2026-08-16", "hpheat", "cost_day_usd"): 2.3099999,
        # day2 = 2026-08-15
        ("2026-08-15", "eheat", "kwh_day"): 5.0,
        ("2026-08-15", "eheat", "cost_day_usd"): 0.55,
        ("2026-08-15", "cooling", "kwh_day"): 0.0,
        ("2026-08-15", "cooling", "cost_day_usd"): 0.0,
        ("2026-08-15", "fan", "kwh_day"): 1.0,
        ("2026-08-15", "fan", "cost_day_usd"): 0.11,
        ("2026-08-15", "hpheat", "kwh_day"): 22.0,
        ("2026-08-15", "hpheat", "cost_day_usd"): 2.4199998,
    }


def test_every_row_carries_the_canonical_identity_and_unit() -> None:
    rows = all_rows(
        daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))
    )
    assert {o.source for o in rows} == {model.SOURCE_BRYANT}
    assert {o.device_id for o in rows} == {SERIAL}
    assert {(o.metric, o.unit) for o in rows} == {("kwh_day", "kWh"), ("cost_day_usd", "USD")}


def test_a_disabled_component_emits_nothing_not_a_zero() -> None:
    """The whole reason ``energyConfig`` is fetched in the same query.

    The period object *does* carry zeros for the disabled components — the
    fixture asserts that below — so a mapper that ignored ``energyConfig`` would
    write four phantom components' worth of zeros every single day. Absent is
    not zero (CLAUDE.md rule 1).
    """
    document = payload("energy_response")
    period = document["energyPeriods"][0]
    for name in DISABLED:
        spec = daily.COMPONENT_BY_CONFIG_KEY[name]
        assert period[spec.kwh_field] == 0, "fixture must offer a tempting zero"
        assert document["energyConfig"][name]["enabled"] is False

    rows = all_rows(daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17)))
    assert {o.channel_id for o in rows} == set(ENABLED)


def test_an_enabled_component_reporting_zero_is_recorded() -> None:
    """Cooling in August-heat-pump-heating season: enabled, measured, zero.

    This is the other half of the rule. A zero from an enabled component is what
    the API said, and recording it verbatim is cardinal rule 2.
    """
    rows = all_rows(
        daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))
    )
    cooling = [o for o in rows if o.channel_id == "cooling"]
    assert len(cooling) == 4
    assert {o.value for o in cooling} == {0.0}


def test_aggregate_periods_are_ignored() -> None:
    """``month1``/``year1`` are spans, not days; they have no place in day grain."""
    document = payload("energy_response")
    assert [p["energyPeriodType"] for p in document["energyPeriods"]] == [
        "day1",
        "day2",
        "month1",
        "year1",
    ]
    days = daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17))
    assert [d.period_type for d in days] == ["day1", "day2"]


def test_period_local_date_dates_only_the_two_day_periods() -> None:
    today = date(2026, 8, 17)
    assert daily.period_local_date("day1", today=today) == date(2026, 8, 16)
    assert daily.period_local_date("day2", today=today) == date(2026, 8, 15)
    for other in ("month1", "year1", "", "day3"):
        assert daily.period_local_date(other, today=today) is None


# ----------------------------------------------------------- value coercion


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (4, 4.0),
        (0, 0.0),
        (2.3099999, 2.3099999),
        ("4", 4.0),
        ("0.44", 0.44),
        (" 12 ", 12.0),
        (None, None),
        ("None", None),  # Carrier's missing sentinel is the literal string
        ("none", None),
        ("", None),
        ("--", None),
        ("nan", None),
        ("inf", None),
        (True, None),  # a bool is not a measurement
        ([1], None),
    ],
)
def test_coerce_number(raw: Any, expected: float | None) -> None:
    assert daily.coerce_number(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("False", False),
        (1, True),
        (0, False),
        (None, False),
        ("None", False),
        ("maybe", None),
        (7, None),
    ],
)
def test_is_enabled(raw: Any, expected: bool | None) -> None:
    assert daily.is_enabled(raw) is expected


def test_missing_null_and_unreadable_values_emit_no_row(log_stream: io.StringIO) -> None:
    """Four flavours of "not a number", all on *enabled* components.

    None of them may become a zero. The kWh side and the dollars side are
    independent, so a readable half still produces its row.
    """
    rows = all_rows(
        daily.payload_to_days(payload("energy_partial"), serial=SERIAL, today=date(2026, 8, 17))
    )
    assert cells(rows) == {
        # eheat: numeric strings, both readable
        ("2026-08-16", "eheat", "kwh_day"): 4.0,
        ("2026-08-16", "eheat", "cost_day_usd"): 0.44,
        # cooling: "None" kWh and null dollars -> nothing at all
        # fan: kWh present, dollars field absent from the object
        ("2026-08-16", "fan", "kwh_day"): 3.0,
        # hpheat: junk kWh, valid dollars
        ("2026-08-16", "hpheat", "cost_day_usd"): 2.31,
    }
    assert not [o for o in rows if o.channel_id == "cooling"]
    # The unreadable ones are visible, not silent.
    unreadable = {e["field"] for e in log_events(log_stream, "daily_value_unreadable")}
    assert unreadable == {"coolingKwh", "hPHeatKwh"}


def test_a_missing_energy_config_is_an_error_not_an_assumption() -> None:
    """Without ``energyConfig`` a disabled component cannot be told from a zero.

    Writing the periods anyway would fabricate rows for hardware this house does
    not have, so the run fails instead.
    """
    document = payload("energy_response")
    document.pop("energyConfig")
    with pytest.raises(daily.DailyFetchError, match="energyConfig"):
        daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17))


def test_a_component_absent_from_config_is_skipped_with_a_warn(
    log_stream: io.StringIO,
) -> None:
    document = payload("energy_response")
    document["energyConfig"].pop("hpheat")

    rows = all_rows(daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17)))
    assert "hpheat" not in {o.channel_id for o in rows}
    events = log_events(log_stream, "daily_component_absent_from_config")
    assert events and events[0]["component"] == "hpheat"


def test_an_unreadable_enabled_flag_is_skipped_with_a_warn(log_stream: io.StringIO) -> None:
    """A flag we cannot interpret is not a licence to emit rows."""
    document = payload("energy_response")
    document["energyConfig"]["hpheat"]["enabled"] = "sometimes"

    rows = all_rows(daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17)))
    assert "hpheat" not in {o.channel_id for o in rows}
    assert log_events(log_stream, "daily_component_enabled_unreadable")


def test_an_unknown_config_component_is_warned_not_dropped_silently(
    log_stream: io.StringIO,
) -> None:
    """Tripwire for a renamed component."""
    document = payload("energy_response")
    document["energyConfig"]["heatPumpHeat"] = {"display": "?", "enabled": True}

    daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17))
    events = log_events(log_stream, "daily_unknown_config_component")
    assert events and events[0]["components"] == ["heatPumpHeat"]


def test_a_response_with_no_day_periods_writes_nothing_and_warns(
    log_stream: io.StringIO,
) -> None:
    document = payload("energy_response")
    document["energyPeriods"] = [
        p for p in document["energyPeriods"] if p["energyPeriodType"] not in {"day1", "day2"}
    ]
    assert daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17)) == []
    assert log_events(log_stream, "daily_no_day_periods")


def test_a_malformed_periods_list_raises() -> None:
    document = payload("energy_response")
    document["energyPeriods"] = None
    with pytest.raises(daily.DailyFetchError, match="energyPeriods"):
        daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17))


# --------------------------------------------------------------- the gas WARN


def test_enabled_and_nonzero_gas_is_recorded_verbatim_and_warned(
    log_stream: io.StringIO,
) -> None:
    """PLAN.md §7.2: keep ``metric=kwh_day``, log a WARN, never guess a unit."""
    rows = all_rows(
        daily.payload_to_days(payload("energy_gas_enabled"), serial=SERIAL, today=date(2026, 8, 17))
    )
    gas_kwh = [o for o in rows if o.channel_id == "gas" and o.metric == "kwh_day"]
    assert len(gas_kwh) == 1
    assert gas_kwh[0].value == 37.0
    assert gas_kwh[0].unit == "kWh"  # verbatim: no conversion was invented

    events = log_events(log_stream, "daily_gas_kwh_nonzero")
    assert len(events) == 1
    assert events[0]["value"] == 37.0
    assert events[0]["local_date"] == "2026-08-16"
    assert events[0]["level"] == "WARNING"


def test_enabled_gas_reporting_zero_does_not_warn(log_stream: io.StringIO) -> None:
    document = payload("energy_gas_enabled")
    document["energyPeriods"][0]["gasKwh"] = 0
    rows = all_rows(daily.payload_to_days(document, serial=SERIAL, today=date(2026, 8, 17)))
    assert ("2026-08-16", "gas", "kwh_day") in cells(rows)
    assert not log_events(log_stream, "daily_gas_kwh_nonzero")


def test_a_disabled_gas_component_cannot_warn_because_it_emits_nothing(
    log_stream: io.StringIO,
) -> None:
    """This house: ``energyConfig.gas.enabled`` is false, so gas drops out."""
    rows = all_rows(
        daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))
    )
    assert not [o for o in rows if o.channel_id == "gas"]
    assert not log_events(log_stream, "daily_gas_kwh_nonzero")


# ==================================================================== DST


def test_ts_is_local_midnight_converted_to_utc() -> None:
    rows = all_rows(
        daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))
    )
    for obs in rows:
        day = obs.ts_local.date()
        assert obs.ts_local == datetime(day.year, day.month, day.day)
        assert obs.ts_utc == timeutil.local_midnight_utc(day)
    # August is EDT (UTC-4).
    assert {o.ts_utc for o in rows} == {utc(2026, 8, 16, 4), utc(2026, 8, 15, 4)}


def test_local_midnight_on_the_spring_forward_day() -> None:
    """2026-03-08 is 23 hours long; its midnight is still EST (UTC-5).

    The transition is at 02:00, so midnight is neither skipped nor doubled —
    but the offset it lands on is the thing a naive ``+5h`` would get wrong on
    the *other* side of the year.
    """
    days = daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 3, 9))
    assert [d.local_day for d in days] == [date(2026, 3, 8), date(2026, 3, 7)]
    assert timeutil.local_hours_in_day(date(2026, 3, 8)) == 23

    by_day = {d.local_day: d.observations for d in days}
    assert {o.ts_utc for o in by_day[date(2026, 3, 8)]} == {utc(2026, 3, 8, 5)}
    assert {o.ts_local for o in by_day[date(2026, 3, 8)]} == {datetime(2026, 3, 8)}
    assert {o.ts_utc for o in by_day[date(2026, 3, 7)]} == {utc(2026, 3, 7, 5)}


def test_local_midnight_on_the_fall_back_day() -> None:
    """2026-11-01 is 25 hours long; its midnight is still EDT (UTC-4)."""
    days = daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 11, 2))
    assert [d.local_day for d in days] == [date(2026, 11, 1), date(2026, 10, 31)]
    assert timeutil.local_hours_in_day(date(2026, 11, 1)) == 25

    by_day = {d.local_day: d.observations for d in days}
    assert {o.ts_utc for o in by_day[date(2026, 11, 1)]} == {utc(2026, 11, 1, 4)}
    assert {o.ts_local for o in by_day[date(2026, 11, 1)]} == {datetime(2026, 11, 1)}
    assert {o.ts_utc for o in by_day[date(2026, 10, 31)]} == {utc(2026, 10, 31, 4)}


def test_a_dst_day_lands_in_the_local_date_partition(s3, status: StatusStore) -> None:
    """Partitioning is on the LOCAL date even though ``ts_utc`` is 05:00Z."""
    summary = daily.run(
        start=date(2026, 3, 7),
        end=date(2026, 3, 8),
        now=utc(2026, 3, 9, 12, 30),
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    assert summary["keys"] == ["energy/daily/year=2026/bryant-202603.parquet"]
    table = read_key(s3, summary["keys"][0])
    assert {row["ts_local"].date() for row in table.to_pylist()} == {
        date(2026, 3, 7),
        date(2026, 3, 8),
    }


# ============================================================== the S3 stage


def test_run_lands_both_days_in_the_monthly_file(s3, status: StatusStore) -> None:
    summary = daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )

    key = "energy/daily/year=2026/bryant-202608.parquet"
    assert summary["keys"] == [key]
    assert summary["rows"] == 16
    assert summary["days"] == ["2026-08-16", "2026-08-15"]

    table = read_key(s3, key)
    assert table.num_rows == 16
    assert table.schema == model.DAILY_SCHEMA
    assert table_cells(table)[("2026-08-16", "hpheat", "kwh_day")] == 21.0
    assert table_cells(table)[("2026-08-15", "eheat", "cost_day_usd")] == 0.55


def test_nothing_is_written_outside_energy_daily(s3, status: StatusStore) -> None:
    """CLAUDE.md rule 6: day-grain rows would poison the hourly rollup."""
    daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    keys = bucket_keys(s3)
    assert keys == ["energy/daily/year=2026/bryant-202608.parquet"]
    assert not any(k.startswith(s3io.RAW_30S_PREFIX) for k in keys)


def test_the_rows_are_rejected_by_the_raw_30s_dataset() -> None:
    """Belt and braces: the model itself refuses to put these in ``raw_30s``."""
    rows = all_rows(
        daily.payload_to_days(payload("energy_response"), serial=SERIAL, today=date(2026, 8, 17))
    )
    with pytest.raises(ValueError, match="raw_30s"):
        model.observations_to_table(rows, dataset=model.Dataset.RAW_30S)


def test_a_day2_revision_replaces_the_day1_row_for_the_same_date(
    s3, status: StatusStore
) -> None:
    """PLAN.md §15.2 for the daily dataset.

    Day one writes 2026-08-16 as ``day1``. Day two the cloud restates it as
    ``day2`` with different numbers. The canonical dedupe key collapses the
    overlap and the *fresher* statement wins — one row, not two.
    """
    key = "energy/daily/year=2026/bryant-202608.parquet"

    daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    assert table_cells(read_key(s3, key))[("2026-08-16", "hpheat", "kwh_day")] == 21.0

    daily.run(
        start=date(2026, 8, 16),
        end=date(2026, 8, 17),
        now=utc(2026, 8, 18, 12, 30),
        payload=payload("energy_revision"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )

    table = read_key(s3, key)
    values = table_cells(table)
    # Three distinct days, one row per (day, channel, metric) — no duplicates.
    assert table.num_rows == 24
    assert len(values) == 24
    assert {ts.date().isoformat() for ts in table.column("ts_local").to_pylist()} == {
        "2026-08-15",
        "2026-08-16",
        "2026-08-17",
    }
    # The revision won.
    assert values[("2026-08-16", "hpheat", "kwh_day")] == 26.0
    assert values[("2026-08-16", "eheat", "kwh_day")] == 7.0
    # The day it did not restate is untouched.
    assert values[("2026-08-15", "hpheat", "kwh_day")] == 22.0
    # And the new day1 landed.
    assert values[("2026-08-17", "cooling", "kwh_day")] == 12.0


def test_a_rerun_is_byte_identical(s3, status: StatusStore) -> None:
    """CLAUDE.md rule 7: deterministic name, deterministic bytes."""
    key = "energy/daily/year=2026/bryant-202608.parquet"
    kwargs = dict(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    daily.run(**kwargs)
    first = object_bytes(s3, key)
    daily.run(**kwargs)
    assert object_bytes(s3, key) == first


def test_a_month_is_regenerated_whole_not_appended_to(
    s3, status: StatusStore, day_grain_obs
) -> None:
    """The merge contract this stage shares with ``stages/backfill.py``.

    Seed the month with backfilled history (a day this fetch knows nothing
    about) *and* a stale value for a day it does know. After the run: the
    history survives untouched, the stale cell is replaced rather than
    duplicated, and the file is one regenerated object rather than an append.
    """
    key = "energy/daily/year=2026/bryant-202608.parquet"
    seeded = [
        day_grain_obs(date(2026, 8, 1), channel_id="hpheat", metric="kwh_day", value=99.0),
        day_grain_obs(date(2026, 8, 1), channel_id="hpheat", metric="cost_day_usd", value=9.9),
        # A stale statement of a day this fetch *will* restate.
        day_grain_obs(date(2026, 8, 16), channel_id="hpheat", metric="kwh_day", value=1.0),
    ]
    s3io.write_table_atomic(
        model.observations_to_table(seeded, dataset=model.Dataset.DAILY),
        BUCKET,
        key,
        client=s3,
    )

    daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )

    table = read_key(s3, key)
    values = table_cells(table)
    # 16 fetched + the two August-1st rows the fetch had no opinion about.
    assert table.num_rows == 18
    assert len(values) == 18
    assert values[("2026-08-01", "hpheat", "kwh_day")] == 99.0
    assert values[("2026-08-01", "hpheat", "cost_day_usd")] == 9.9
    assert values[("2026-08-16", "hpheat", "kwh_day")] == 21.0  # fresh beat stale


def test_two_months_get_two_files(s3, status: StatusStore) -> None:
    """day1 and day2 straddle a month boundary once a month."""
    summary = daily.run(
        start=date(2026, 8, 31),
        end=date(2026, 9, 1),
        now=utc(2026, 9, 2, 12, 30),
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    assert summary["keys"] == [
        "energy/daily/year=2026/bryant-202608.parquet",
        "energy/daily/year=2026/bryant-202609.parquet",
    ]
    assert read_key(s3, summary["keys"][0]).num_rows == 8
    assert read_key(s3, summary["keys"][1]).num_rows == 8


def test_the_range_narrows_which_days_are_written(
    s3, status: StatusStore, log_stream: io.StringIO
) -> None:
    """``--start/--end`` are LOCAL dates and they *filter* the fetched pair."""
    summary = daily.run(
        start=DAY1,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    assert summary["days"] == ["2026-08-16"]
    table = read_key(s3, summary["keys"][0])
    assert {ts.date() for ts in table.column("ts_local").to_pylist()} == {DAY1}
    assert log_events(log_stream, "daily_period_out_of_range")


def test_a_range_the_cloud_cannot_serve_warns_and_stays_a_gap(
    s3, status: StatusStore, log_stream: io.StringIO
) -> None:
    """History comes from ``energycap backfill``; this stage does not invent it."""
    summary = daily.run(
        start=date(2026, 8, 10),
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    assert summary["dates_unavailable"] == [
        "2026-08-10",
        "2026-08-11",
        "2026-08-12",
        "2026-08-13",
        "2026-08-14",
    ]
    events = log_events(log_stream, "daily_range_unavailable")
    assert events and "backfill" in events[0]["detail"]
    # Only the two days the cloud actually served exist.
    assert read_key(s3, summary["keys"][0]).num_rows == 16


def test_a_dry_run_writes_nothing(s3, status: StatusStore) -> None:
    summary = daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
        dry_run=True,
    )
    assert summary["rows"] == 16
    assert bucket_keys(s3) == []


def test_an_inverted_range_is_a_usage_error(s3, status: StatusStore) -> None:
    with pytest.raises(ValueError, match="before start"):
        daily.run(
            start=DAY1,
            end=DAY2,
            now=NOW,
            payload=payload("energy_response"),
            bucket=BUCKET,
            client=s3,
            status=status,
        )


# ============================================================== status.json


def test_the_run_is_recorded_in_status_json(s3, status: StatusStore) -> None:
    daily.run(
        start=DAY2,
        end=DAY1,
        now=NOW,
        payload=payload("energy_response"),
        bucket=BUCKET,
        client=s3,
        status=status,
    )
    section = status.section("bryant_daily")
    assert section["last_success_utc"] is not None
    assert section["consecutive_failures"] == 0
    assert section["rows"] == 16
    assert section["last_days_fetched"] == ["2026-08-16", "2026-08-15"]


def test_a_malformed_payload_records_a_failure_and_raises(
    s3, status: StatusStore
) -> None:
    document = payload("energy_response")
    document.pop("energyConfig")
    with pytest.raises(daily.DailyFetchError):
        daily.run(
            start=DAY2,
            end=DAY1,
            now=NOW,
            payload=document,
            bucket=BUCKET,
            client=s3,
            status=status,
        )
    section = status.section("bryant_daily")
    assert section["consecutive_failures"] == 1
    assert section["last_success_utc"] is None
    assert bucket_keys(s3) == []


# =============================================================== transport


class FakeCarrier:
    """Okta + GraphQL over one ``httpx.MockTransport`` handler. No sockets."""

    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body
        self.graphql_requests: list[dict[str, Any]] = []
        self.graphql_headers: list[httpx.Headers] = []
        self.token_requests = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "sso.carrier.com":
            self.token_requests += 1
            return httpx.Response(
                200,
                json={
                    "access_token": "fake-access-token-aaaaaaaaaaaa",
                    "refresh_token": "fake-refresh-token-bbbbbbbbbbbb",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "openid offline_access",
                },
            )
        if request.url.host == "dataservice.infinity.iot.carrier.com":
            self.graphql_requests.append(json.loads(request.content))
            self.graphql_headers.append(request.headers)
            return httpx.Response(200, json=self.body)
        raise AssertionError(f"unexpected host {request.url.host}")


def drive(carrier: FakeCarrier, token_path: Path) -> daily.EnergyResponse:
    """Run one real ``fetch_energy`` through the mocked transport."""

    async def go() -> daily.EnergyResponse:
        http = httpx.AsyncClient(transport=httpx.MockTransport(carrier.handler))
        auth = carrier_auth.CarrierAuth(
            username="test-carrier@example.invalid",
            password="not-a-real-carrier-password",
            token_path=token_path,
            client=http,
        )
        client = carrier_auth.CarrierGraphQLClient(auth, client=http, owns_client=True)
        try:
            return await daily.fetch_energy(serial=SERIAL, client=client)
        finally:
            await client.close()

    return asyncio.run(go())


def test_the_query_goes_out_through_the_shared_carrier_transport(
    settings, spool_dir: Path
) -> None:
    """One GraphQL POST, authenticated, with the ported operation and variables."""
    carrier = FakeCarrier(response("energy_response"))
    result = drive(carrier, settings.carrier_token_path)

    assert len(carrier.graphql_requests) == 1
    body = carrier.graphql_requests[0]
    assert body["operationName"] == daily.OPERATION_NAME
    assert body["variables"] == {"serial": SERIAL}
    assert body["query"] == daily.ENERGY_QUERY
    assert carrier.graphql_headers[0]["authorization"].startswith("Bearer ")

    assert result.payload["energyConfig"]["hpheat"]["enabled"] is True
    days = daily.payload_to_days(result.payload, serial=SERIAL, today=date(2026, 8, 17))
    assert [d.period_type for d in days] == ["day1", "day2"]

    # status.json gets counters, never a credential.
    assert json.dumps(dict(result.status_fields))  # JSON-serialisable
    assert "not-a-real-carrier-password" not in json.dumps(dict(result.status_fields))
    assert "fake-access-token-aaaaaaaaaaaa" not in json.dumps(dict(result.status_fields))


def test_a_graphql_errors_array_raises_and_produces_no_rows(
    settings, spool_dir: Path
) -> None:
    """A 200 carrying ``errors`` is a failure, not data — so the day is a gap."""
    carrier = FakeCarrier(
        {"errors": [{"message": "Serial not found"}], "data": {"infinityEnergy": None}}
    )
    with pytest.raises(carrier_auth.CarrierGraphQLError):
        drive(carrier, settings.carrier_token_path)


def test_a_response_without_an_infinity_energy_object_raises(
    settings, spool_dir: Path
) -> None:
    carrier = FakeCarrier({"data": {"infinityEnergy": None}})
    with pytest.raises(daily.DailyFetchError, match="infinityEnergy"):
        drive(carrier, settings.carrier_token_path)
