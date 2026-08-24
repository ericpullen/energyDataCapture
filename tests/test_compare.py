"""``energycap compare-meter`` — the sub-metering's own accuracy check.

This stage exists to answer one question: do the two service-feed CT pairs,
summed, add up to what the utility meter recorded? So the tests are mostly about
the ways that number could be quietly wrong:

* summing the wrong channels (``panel_leg_*`` is **voltage** — adding it in
  would inflate the panel side by hundreds of "kWh" that are really volts);
* comparing an hour the collector only half observed and calling the shortfall a
  measurement error;
* inventing a value for an hour only one side covers.

The panel side deliberately goes through the real ``rollup_day``/``rollup.sql``,
so these also serve as a check that the comparison cannot drift away from what
``energy/hourly`` would say.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from energy_capture import model, timeutil
from energy_capture.spool.sqlite import open_spool
from energy_capture.stages import compare, greenbutton

# 2026-08-16 is an ordinary EDT day: local midnight is 04:00Z.
LOCAL_DAY = timeutil.parse_local_date("2026-08-16")
HOUR_UTC = datetime(2026, 8, 16, 16, 0, tzinfo=UTC)  # noon local
POLL_S = 30
FULL_HOUR_SAMPLES = 3600 // POLL_S  # 120


@pytest.fixture
def spool(spool_dir: Path):
    with open_spool(spool_dir / "compare.db") as db:
        yield db


def fill_hour(
    spool,
    make_obs,
    *,
    start: datetime = HOUR_UTC,
    watts: float = 1000.0,
    channels: tuple[str, ...] = ("ct_1_a", "ct_1_b"),
    device_id: str = "hub-a",
    samples: int = FULL_HOUR_SAMPLES,
) -> None:
    """Append a steady load on ``channels`` for ``samples`` 30-second ticks."""
    rows = [
        make_obs(
            start + timedelta(seconds=POLL_S * tick),
            device_id=device_id,
            channel_id=channel,
            metric="watts",
            value=watts,
        )
        for tick in range(samples)
        for channel in channels
    ]
    spool.append(rows)


def meter_table(tmp_path: Path, *, kwh: float, start: datetime = HOUR_UTC):
    """One hourly meter reading, produced by the real importer."""
    export = tmp_path / "gb.xml"
    export.write_text(
        _espi(int(start.timestamp()), 3600, int(round(kwh * 1000)))
    )
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")
    return [pq.read_table(Path(f)) for f in summary["files"]]


def _espi(start: int, duration: int, wh: int) -> str:
    base = "https://api.example.com/espi/1_1/resource"
    mr = f"{base}/Subscription/9/UsagePoint/1308468/MeterReading/1"
    rt = f"{base}/ReadingType/1"
    return f"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:espi="http://naesb.org/espi">
  <entry><link rel="self" href="{rt}"/><content><espi:ReadingType>
    <espi:powerOfTenMultiplier>0</espi:powerOfTenMultiplier>
    <espi:uom>72</espi:uom><espi:flowDirection>1</espi:flowDirection>
  </espi:ReadingType></content></entry>
  <entry><link rel="self" href="{mr}"/><link rel="related" href="{rt}"/>
    <content><espi:MeterReading/></content></entry>
  <entry><link rel="self" href="{mr}/IntervalBlock/1"/><content>
    <espi:IntervalBlock>
      <espi:interval><espi:duration>3600</espi:duration>
        <espi:start>{start}</espi:start></espi:interval>
      <espi:IntervalReading>
        <espi:timePeriod><espi:duration>{duration}</espi:duration>
          <espi:start>{start}</espi:start></espi:timePeriod>
        <espi:value>{wh}</espi:value>
      </espi:IntervalReading>
    </espi:IntervalBlock></content></entry>
</feed>
"""


# ------------------------------------------------------------------ the math


def test_two_feed_legs_at_1kw_for_an_hour_are_two_kwh(spool, make_obs, tmp_path) -> None:
    """The arithmetic, end to end, with the meter agreeing exactly."""
    fill_hour(spool, make_obs, watts=1000.0)  # 2 legs x 1 kW x 1 h = 2 kWh

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)

    assert hour.panel_kwh == pytest.approx(2.0)
    assert hour.meter_kwh == pytest.approx(2.0)
    assert hour.difference_kwh == pytest.approx(0.0)
    assert hour.coverage == pytest.approx(1.0)


def test_the_panel_side_reports_the_gap_when_the_meter_reads_higher(
    spool, make_obs, tmp_path
) -> None:
    """The realistic case: something is on a circuit the CTs do not see."""
    fill_hour(spool, make_obs, watts=1000.0)
    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.5),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)

    assert hour.difference_kwh == pytest.approx(-0.5)
    assert hour.difference_pct == pytest.approx(-20.0)


def test_voltage_channels_are_never_summed_into_the_panel_total(
    spool, make_obs, tmp_path
) -> None:
    """``panel_leg_a`` is ~240 V. Adding it in would dwarf the real answer."""
    fill_hour(spool, make_obs, watts=1000.0)
    spool.append(
        [
            make_obs(
                HOUR_UTC + timedelta(seconds=POLL_S * tick),
                channel_id="panel_leg_a",
                metric="volts",
                value=241.3,
            )
            for tick in range(FULL_HOUR_SAMPLES)
        ]
    )

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)
    assert hour.panel_kwh == pytest.approx(2.0)


def test_branch_circuits_are_not_added_to_the_feed_total(
    spool, make_obs, tmp_path
) -> None:
    """The one that matters: breakers are *inside* the feed, not beside it.

    Every branch circuit's watts are already counted by the feed CT it hangs
    off. Summing breakers in as well would double-count the whole house and
    make the panels look wildly high against the meter. Unlike the voltage
    channels, these are genuine ``watts`` rows with a real ``kwh``, so only the
    channel filter stops them.
    """
    fill_hour(spool, make_obs, watts=1000.0)
    fill_hour(spool, make_obs, watts=4000.0, channels=("breaker_p19", "breaker_p23"))

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)
    assert hour.panel_kwh == pytest.approx(2.0), "a branch circuit was double-counted"


def test_both_hubs_feed_ct_pairs_are_added_together(spool, make_obs, tmp_path) -> None:
    """Whole house is four CTs: two legs on each of the two panels."""
    fill_hour(spool, make_obs, watts=1000.0, device_id="hub-a")
    fill_hour(spool, make_obs, watts=500.0, device_id="hub-b")

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=3.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)
    assert hour.panel_kwh == pytest.approx(3.0)  # 2 kWh + 1 kWh


# -------------------------------------------------------------- honest gaps


def test_a_half_observed_hour_reports_its_coverage(spool, make_obs, tmp_path) -> None:
    """kWh is observed-time-only, so half an hour of samples is half the energy.

    The number is *correct* — it is what was observed — and it would be a
    catastrophic misreading to call the shortfall a CT error. ``coverage`` is
    what says so.
    """
    fill_hour(spool, make_obs, watts=1000.0, samples=FULL_HOUR_SAMPLES // 2)

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)

    assert hour.panel_kwh == pytest.approx(1.0)
    assert hour.coverage == pytest.approx(0.5)
    assert hour.expected_samples == FULL_HOUR_SAMPLES


def test_a_low_coverage_hour_is_excluded_from_the_total_and_said_so(
    spool, make_obs, tmp_path
) -> None:
    fill_hour(spool, make_obs, watts=1000.0, samples=FULL_HOUR_SAMPLES // 2)
    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    report = compare.format_report(rows, min_coverage=0.9)

    assert "NOT in the total" in report
    assert "No hour had both a meter reading and adequate panel coverage." in report


def test_an_hour_only_one_side_covers_is_never_filled_in(
    spool, make_obs, tmp_path
) -> None:
    """Gaps stay gaps: no zero, no interpolation, no carried-forward value."""
    fill_hour(spool, make_obs, watts=1000.0)
    # The meter reading is for a different hour entirely.
    tables = meter_table(tmp_path, kwh=2.0, start=HOUR_UTC + timedelta(hours=3))

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=tables,
        poll_interval_s=POLL_S,
    )
    panel_only = next(r for r in rows if r.hour_start_utc == HOUR_UTC)
    meter_only = next(r for r in rows if r.hour_start_utc == HOUR_UTC + timedelta(hours=3))

    assert panel_only.meter_kwh is None
    assert panel_only.difference_kwh is None
    assert meter_only.panel_kwh is None
    assert meter_only.sample_count == 0


def test_the_weakest_channel_sets_the_reported_sample_count(
    spool, make_obs, tmp_path
) -> None:
    """One CT missing half the hour understates the sum by that CT's share."""
    fill_hour(spool, make_obs, watts=1000.0, channels=("ct_1_a",))
    fill_hour(
        spool, make_obs, watts=1000.0, channels=("ct_1_b",), samples=FULL_HOUR_SAMPLES // 4
    )

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.0),
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)

    assert hour.sample_count == FULL_HOUR_SAMPLES // 4
    assert hour.coverage == pytest.approx(0.25)
    assert hour.panel_kwh == pytest.approx(1.25)  # 1.0 + 0.25


# ---------------------------------------------------------------- 15-minute


def test_four_quarter_hour_meter_readings_make_one_hour(
    spool, make_obs, tmp_path
) -> None:
    """15-minute intervals never straddle an hour, so they simply sum into it."""
    fill_hour(spool, make_obs, watts=1000.0)
    quarters = [
        (int((HOUR_UTC + timedelta(minutes=15 * n)).timestamp()), 900, 500)
        for n in range(4)
    ]
    export = tmp_path / "quarters.xml"
    export.write_text(_espi_multi(quarters))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=[pq.read_table(Path(f)) for f in summary["files"]],
        poll_interval_s=POLL_S,
    )
    hour = next(r for r in rows if r.hour_start_utc == HOUR_UTC)
    assert hour.meter_kwh == pytest.approx(2.0)  # 4 x 500 Wh


def _espi_multi(readings: list[tuple[int, int, int]]) -> str:
    base = "https://api.example.com/espi/1_1/resource"
    mr = f"{base}/Subscription/9/UsagePoint/1308468/MeterReading/1"
    rt = f"{base}/ReadingType/1"
    body = "".join(
        f"""<espi:IntervalReading><espi:timePeriod>
          <espi:duration>{d}</espi:duration><espi:start>{s}</espi:start>
        </espi:timePeriod><espi:value>{v}</espi:value></espi:IntervalReading>"""
        for s, d, v in readings
    )
    return f"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:espi="http://naesb.org/espi">
  <entry><link rel="self" href="{rt}"/><content><espi:ReadingType>
    <espi:powerOfTenMultiplier>0</espi:powerOfTenMultiplier><espi:uom>72</espi:uom>
    <espi:flowDirection>1</espi:flowDirection></espi:ReadingType></content></entry>
  <entry><link rel="self" href="{mr}"/><link rel="related" href="{rt}"/>
    <content><espi:MeterReading/></content></entry>
  <entry><link rel="self" href="{mr}/IntervalBlock/1"/><content>
    <espi:IntervalBlock><espi:interval><espi:duration>3600</espi:duration>
      <espi:start>{readings[0][0]}</espi:start></espi:interval>{body}
    </espi:IntervalBlock></content></entry>
</feed>
"""


# ----------------------------------------------------------------- reporting


def test_the_report_states_the_direction_of_the_disagreement(
    spool, make_obs, tmp_path
) -> None:
    fill_hour(spool, make_obs, watts=1000.0)
    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=2.5),
        poll_interval_s=POLL_S,
    )
    report = compare.format_report(rows)

    assert "TOTAL" in report
    assert "below the meter" in report
    assert "20.0%" in report


def test_no_meter_data_at_all_does_not_pretend_to_a_comparison(
    spool, make_obs
) -> None:
    fill_hour(spool, make_obs, watts=1000.0)
    rows = compare.compare_range(
        start=LOCAL_DAY, end=LOCAL_DAY, spool=spool, meter_tables=[], poll_interval_s=POLL_S
    )
    report = compare.format_report(rows)
    assert "No hour had both a meter reading" in report


def test_the_default_channels_are_the_feed_cts_not_the_voltage_legs() -> None:
    """Pinned: this constant is the difference between kWh and volts."""
    assert compare.DEFAULT_PANEL_CHANNELS == ("ct_1_a", "ct_1_b")
    assert not any(
        channel.startswith("panel_leg") for channel in compare.DEFAULT_PANEL_CHANNELS
    )


# ------------------------------------------------------- which meter is it


def _meter_rows(rows: list[tuple[str, datetime, float]]):
    """A meter table straight from the model, bypassing the XML round trip."""
    return [
        model.observations_to_table(
            [
                model.make_observation(
                    ts_utc=ts,
                    source=model.SOURCE_LGE,
                    device_id=device,
                    channel_id="electric_main",
                    metric="kwh_interval",
                    value=value,
                    interval_s=900,
                )
                for device, ts, value in rows
            ],
            dataset=model.Dataset.METER,
        )
    ]


def test_identical_meter_ids_are_collapsed_to_one_not_summed() -> None:
    """The real export carries three ids with an identical series.

    Measured on a live LG&E download (2026-08-18): 1308468, 944401 and 944006
    each carry the same watt-hours for every interval of ten days — the same
    service through meter changes. Summing them would report three times the
    household's consumption and make the panels look like they measure a third
    of the house.
    """
    stamps = [HOUR_UTC + timedelta(minutes=15 * n) for n in range(4)]
    tables = _meter_rows(
        [
            (device, ts, 0.5)
            for device in ("1308468", "944401", "944006")
            for ts in stamps
        ]
    )

    chosen, note = compare.resolve_meter(tables)
    assert chosen == "1308468"
    assert note and "IDENTICAL" in note and "treble" in note

    hours = compare.meter_hours(tables, LOCAL_DAY, LOCAL_DAY, device_id=chosen)
    assert hours[HOUR_UTC] == pytest.approx(2.0), "the duplicates were summed"


def test_genuinely_different_meters_refuse_to_be_guessed_between() -> None:
    """Two real meters is a question for a human, not a default."""
    tables = _meter_rows(
        [("1308468", HOUR_UTC, 2.0), ("944401", HOUR_UTC, 7.5)]
    )
    with pytest.raises(compare.AmbiguousMeterError, match="--meter"):
        compare.resolve_meter(tables)


def test_an_explicit_meter_choice_is_honoured() -> None:
    tables = _meter_rows(
        [("1308468", HOUR_UTC, 2.0), ("944401", HOUR_UTC, 7.5)]
    )
    chosen, note = compare.resolve_meter(tables, requested="944401")
    assert (chosen, note) == ("944401", None)

    hours = compare.meter_hours(tables, LOCAL_DAY, LOCAL_DAY, device_id="944401")
    assert hours[HOUR_UTC] == pytest.approx(7.5)


def test_an_unknown_meter_choice_lists_what_is_there() -> None:
    tables = _meter_rows([("1308468", HOUR_UTC, 2.0)])
    with pytest.raises(compare.AmbiguousMeterError, match="1308468"):
        compare.resolve_meter(tables, requested="nope")


def test_the_meter_rows_it_reads_are_the_meter_dataset(tmp_path: Path) -> None:
    """Guards the join: compare reads what import-greenbutton writes."""
    tables = meter_table(tmp_path, kwh=1.0)
    assert tables[0].schema.names == model.METER_SCHEMA.names
    loaded = compare.load_meter_tables(tmp_path / "meter")
    assert loaded and loaded[0].num_rows == tables[0].num_rows


# ------------------------------------------- two resolutions of one meter


def test_two_interval_series_are_not_summed_together() -> None:
    """LG&E publishes the same energy at 900s and 3600s. Adding both doubles it.

    Both are stored — discarding one at ingest would be filtering the
    custodian's data — so the *comparison* is where the choice has to happen.
    Measured on the live feed 2026-08-18: meter 1326254 reported 76.0 kWh over
    the 900s series and 76.1 kWh over the 3600s one for the same four days.
    """
    stamps = [HOUR_UTC + timedelta(minutes=15 * n) for n in range(4)]
    rows = [("1308468", ts, 0.5, 900) for ts in stamps]
    rows.append(("1308468", HOUR_UTC, 2.0, 3600))
    tables = [
        model.observations_to_table(
            [
                model.make_observation(
                    ts_utc=ts,
                    source=model.SOURCE_LGE,
                    device_id=device,
                    channel_id="electric_main",
                    metric="kwh_interval",
                    value=value,
                    interval_s=interval,
                )
                for device, ts, value, interval in rows
            ],
            dataset=model.Dataset.METER,
        )
    ]

    interval, note = compare.resolve_interval(tables)
    assert interval == 900, "the finest series is the informative one"
    assert note and "double" in note

    hours = compare.meter_hours(tables, LOCAL_DAY, LOCAL_DAY, interval_s=interval)
    assert hours[HOUR_UTC] == pytest.approx(2.0), "the two series were summed"


def test_a_single_interval_series_needs_no_note() -> None:
    tables = _meter_rows([("1308468", HOUR_UTC, 2.0)])
    assert compare.resolve_interval(tables) == (900, None)


# ------------------------------------------------- a whole hub going missing


def test_a_wholly_missing_hub_is_not_full_coverage(spool, make_obs, tmp_path) -> None:
    """The gap sample_count cannot see.

    `sample_count` is the MINIMUM across the channels that produced rows. A hub
    absent for the entire hour produces none, so it contributes nothing to the
    minimum: the surviving hub reports its full count, coverage reads 100%, and
    the summed panel energy is short by a whole panel. One hub offline for a day
    published "the panels read ~50% below the meter" at full coverage.
    """
    # Only hub-a reports. hub-b is silent for the whole hour.
    fill_hour(spool, make_obs, channels=("ct_1_a", "ct_1_b"), device_id="hub-a")

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=4.0),
        poll_interval_s=POLL_S,
        expected_series=4,  # both hubs, two legs each
    )
    hour = next(r for r in rows if r.panel_kwh is not None)

    # Coverage is the trap: it is perfect, because every channel that reported
    # reported fully.
    assert hour.coverage == pytest.approx(1.0)
    assert hour.sample_count == FULL_HOUR_SAMPLES

    # ...and the series count is what catches it.
    assert hour.series_seen == 2
    assert hour.series_expected == 4
    assert hour.series_complete is False

    report = compare.format_report(rows)
    assert "TOTAL" not in report, "an hour missing a whole hub must not be totalled"
    assert "FEED SERIES" in report
    assert "2/4" in report


def test_all_feeds_reporting_is_complete_and_totalled(spool, make_obs, tmp_path) -> None:
    """The control: same shape, both hubs present."""
    for hub in ("hub-a", "hub-b"):
        fill_hour(spool, make_obs, channels=("ct_1_a", "ct_1_b"), device_id=hub)

    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=4.0),
        poll_interval_s=POLL_S,
        expected_series=4,
    )
    hour = next(r for r in rows if r.panel_kwh is not None)
    assert hour.series_seen == 4
    assert hour.series_complete is True
    assert "TOTAL" in compare.format_report(rows)


def test_an_unreadable_channel_map_says_so_instead_of_passing_everything(
    spool, make_obs, tmp_path
) -> None:
    """Unknown expectation must not read as 'nothing missing'.

    With no map there is no way to tell four reporting feeds from two, so no
    hour is excluded for a missing hub — but the report has to SAY that, or a
    silently-degraded run is indistinguishable from a clean one.
    """
    fill_hour(spool, make_obs, channels=("ct_1_a", "ct_1_b"), device_id="hub-a")
    rows = compare.compare_range(
        start=LOCAL_DAY,
        end=LOCAL_DAY,
        spool=spool,
        meter_tables=meter_table(tmp_path, kwh=4.0),
        poll_interval_s=POLL_S,
        expected_series=0,  # the map could not be read
    )
    hour = next(r for r in rows if r.panel_kwh is not None)
    assert hour.series_complete is False  # unknown is not complete...

    report = compare.format_report(rows)
    assert "TOTAL" in report  # ...but it does not silently drop the hour either
    assert "channel map could not be read" in report
    assert "2/?" in report


def test_the_expected_feed_series_come_from_the_map_not_the_data(tmp_path) -> None:
    """Deriving the expectation from the measurements would be circular: a hub
    that stopped reporting entirely would simply not be 'expected', so its
    absence could never make an hour incomplete."""
    import json

    path = tmp_path / "channel_map.json"
    path.write_text(json.dumps({"mappings": [
        {"source": "leviton", "device_id": "hub-a", "channel_id": "ct_1_a"},
        {"source": "leviton", "device_id": "hub-a", "channel_id": "ct_1_b"},
        {"source": "leviton", "device_id": "hub-b", "channel_id": "ct_1_a"},
        {"source": "leviton", "device_id": "hub-b", "channel_id": "ct_1_b"},
        # Not a feed CT, and must not be counted.
        {"source": "leviton", "device_id": "hub-a", "channel_id": "breaker_p1"},
        # A different source entirely.
        {"source": "lge", "device_id": "1308468", "channel_id": "electric_main"},
    ]}))
    series = compare.expected_feed_series(map_path=path)
    assert series == frozenset({
        ("hub-a", "ct_1_a"), ("hub-a", "ct_1_b"),
        ("hub-b", "ct_1_a"), ("hub-b", "ct_1_b"),
    })
    # Absent or unreadable is an empty set, never a guess.
    assert compare.expected_feed_series(map_path=tmp_path / "nope.json") == frozenset()


def test_the_primary_flag_answers_the_ambiguous_meter_question(tmp_path) -> None:
    """B6: the documented recipe errored as written.

    The README said to fetch both meters and then run `compare-meter` with no
    --meter, which is a guaranteed AmbiguousMeterError. `meterview` already
    consulted the map's `primary` flag and the CLI did not, so the two
    disagreed about whether the question was even answerable.
    """
    import json

    path = tmp_path / "channel_map.json"
    path.write_text(json.dumps({"mappings": [
        {"source": "lge", "device_id": "1308468", "channel_id": "electric_main",
         "primary": True},
        {"source": "lge", "device_id": "1326254", "channel_id": "electric_main"},
    ]}))
    assert compare.primary_meter_from_map(path) == "1308468"


def test_a_non_bool_primary_is_not_a_primary(tmp_path) -> None:
    """`bool("no")` is True, which is how a typo becomes a silent wrong answer
    (review B7). Only a real `true` counts."""
    import json

    path = tmp_path / "channel_map.json"
    path.write_text(json.dumps({"mappings": [
        {"source": "lge", "device_id": "1326254", "channel_id": "electric_main",
         "primary": "no"},
    ]}))
    assert compare.primary_meter_from_map(path) is None


def test_no_map_means_no_guess(tmp_path) -> None:
    """Refusing to choose is better than choosing the barn."""
    assert compare.primary_meter_from_map(tmp_path / "absent.json") is None
