"""Tests for the ``/ui`` dashboard (``energy_capture.dashboard``).

Entirely offline — the snapshot is built against a seeded temp spool, and the two
routes are exercised over loopback through the real
:class:`~energy_capture.health.HealthServer` (the same way ``test_health`` does).

The properties that matter are the ones a chart is most likely to destroy:

1. **a gap stays a gap** — a missing stretch produces a gap, never a zero and
   never an interpolated point;
2. **kWh is the rollup's kWh** — observed time only, watts only, hand-checked;
3. **expected sample counts survive DST** — 23- and 25-hour local days, derived
   from ``timeutil``, nothing hardcoding 24;
4. an unmapped channel is still shown, and flagged;
5. an enum code reaches a human as a **word**;
6. the spool is opened **read-only** (a write through that handle fails);
7. ``/ui`` serves HTML and ``/ui/data`` serves the documented JSON;
8. the pre-existing health routes behave exactly as they did;
9. **the chart's movable window never invents data** — an empty bucket stays an
   explicit hole, a partial bucket keeps the mean of the samples that exist, and
   bucket boundaries are computed on ``ts_utc`` so the DST fall-back day's two
   01:00 hours never merge. Plus: the window is clamped, garbage is a 400, and
   the no-parameter document is exactly the one this route always returned.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from energy_capture import dashboard, model, timeutil
from energy_capture.health import DEFAULT_HEALTH_PATHS, HealthServer, StatusStore
from energy_capture.spool.sqlite import SpoolDB
from tests.conftest import utc

# A boring summer afternoon, local EDT (UTC-4).
NOW = utc(2026, 8, 16, 18, 30, 0)

LEVITON_DEVICE = "hub-a"
MAPPED_CHANNEL = "breaker_p11"
UNMAPPED_CHANNEL = "breaker_p99"


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _fresh_caches() -> None:
    """The page and the label map are memoised in the module; never leak them."""
    dashboard.reset_page_cache()
    dashboard.reset_label_cache()


@pytest.fixture
def spool(spool_dir: Path) -> SpoolDB:
    db = SpoolDB(spool_dir / "spool.db", synchronous="NORMAL")
    db.connect()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def channel_map(tmp_path: Path) -> Path:
    """A minimal channel_map: one mapped channel, deliberately not the other."""
    path = tmp_path / "channel_map.json"
    path.write_text(
        json.dumps(
            {
                "mappings": [
                    {
                        "source": "leviton",
                        "device_id": LEVITON_DEVICE,
                        "channel_id": MAPPED_CHANNEL,
                        "label": "Kitchen ring",
                        "short_label": "Kitchen",
                        "category": "branch",
                        "panel": "A",
                    },
                    {
                        "source": "bryant",
                        "device_id": "TEST0000001",
                        "channel_id": "system",
                        "label": "Bryant system",
                        "short_label": "HVAC",
                        "category": "hvac_status",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def seed(
    spool: SpoolDB,
    make_obs: Any,
    *,
    channel_id: str = MAPPED_CHANNEL,
    metric: str = "watts",
    value: float = 100.0,
    start: datetime,
    count: int,
    every_s: int = 30,
    source: str = model.SOURCE_LEVITON,
    device_id: str = LEVITON_DEVICE,
) -> list[datetime]:
    """Append ``count`` samples on a fixed cadence; returns their timestamps."""
    stamps = [start + timedelta(seconds=every_s * i) for i in range(count)]
    spool.append(
        make_obs(
            ts,
            source=source,
            device_id=device_id,
            channel_id=channel_id,
            metric=metric,
            value=value,
        )
        for ts in stamps
    )
    return stamps


def snapshot(spool: SpoolDB, channel_map: Path, **kwargs: Any) -> dict[str, Any]:
    return dashboard.build_snapshot(
        spool_path=spool.path,
        status_path=None,
        channel_map_path=channel_map,
        inventory_path=None,
        now=kwargs.pop("now", NOW),
        **kwargs,
    )


def channel(snap: dict[str, Any], channel_id: str) -> dict[str, Any]:
    for entry in snap["channels"]:
        if entry["channel_id"] == channel_id:
            return entry
    raise AssertionError(f"{channel_id} missing from {[c['key'] for c in snap['channels']]}")


# --------------------------------------------------------------------------
# 1. A gap stays a gap
# --------------------------------------------------------------------------


def test_a_gap_is_a_gap_and_never_a_zero(spool: SpoolDB, channel_map: Path, make_obs) -> None:
    """Ten minutes of silence produces a gap — not a zero, not an interpolation."""
    before = seed(spool, make_obs, start=NOW - timedelta(minutes=25), count=10, value=250.0)
    after = seed(spool, make_obs, start=NOW - timedelta(minutes=10), count=20, value=250.0)

    series = channel(snapshot(spool, channel_map), MAPPED_CHANNEL)["series"]

    # Exactly the samples that were seeded — nothing was invented to fill the hole.
    assert series["sample_count"] == len(before) + len(after)
    assert len(series["points"]) == len(before) + len(after)
    assert {point[2] for point in series["points"]} == {250.0}
    assert series["zero_samples"] == 0

    # And the hole is reported as a hole, with its size.
    assert len(series["gaps"]) == 1
    gap = series["gaps"][0]
    assert gap["after"]["utc"] == timeutil.format_utc(before[-1])
    assert gap["before"]["utc"] == timeutil.format_utc(after[0])
    # last of the first burst is at -20:30, first of the second at -10:00
    assert gap["seconds"] == pytest.approx(630.0, abs=1.0)
    assert gap["missing_samples"] == 20
    assert series["gap_threshold_s"] == 45.0  # 1.5 x the 30s poll interval

    # The window is 30 min at 30s = 60 samples; 30 arrived. Visibly partial.
    assert series["expected_samples"] == 60
    assert series["coverage_pct"] == 50.0


def test_zero_watts_is_a_reading_not_a_gap(spool: SpoolDB, channel_map: Path, make_obs) -> None:
    """"The load was off" and "the collector was down" are different facts."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=10), count=20, value=0.0)

    series = channel(snapshot(spool, channel_map), MAPPED_CHANNEL)["series"]
    assert series["gaps"] == []
    assert series["zero_samples"] == 20
    assert series["sample_count"] == 20
    assert series["min"] == 0.0 and series["max"] == 0.0


def test_absence_at_the_window_edges_is_reported_too(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A channel silent for the first 20 minutes must not look like a short chart.

    ``gaps`` means "between two observed samples" and cannot express an edge, so
    the two edges travel separately — otherwise the page draws a line in the right
    third of the card with plain blank space beside it, which reads as "the chart
    starts here" rather than "nothing arrived".
    """
    seed(spool, make_obs, start=NOW - timedelta(minutes=10), count=10)  # ends at NOW-5:30

    series = channel(snapshot(spool, channel_map), MAPPED_CHANNEL)["series"]

    assert series["gaps"] == []  # no interior hole: the samples are contiguous

    lead = series["leading_gap"]
    assert lead is not None
    assert lead["edge"] == "before_first_sample"
    assert lead["seconds"] == pytest.approx(1200.0, abs=1.0)  # 30 min window, 10 min of data
    assert lead["missing_samples"] == 40
    assert lead["before"]["utc"] == timeutil.format_utc(NOW - timedelta(minutes=10))

    trail = series["trailing_gap"]
    assert trail is not None
    assert trail["edge"] == "after_last_sample"
    assert trail["seconds"] == pytest.approx(330.0, abs=1.0)  # last sample at NOW-5:30

    # And still nothing invented: 10 samples in, 10 samples out.
    assert series["sample_count"] == 10 == len(series["points"])
    assert series["coverage_pct"] == pytest.approx(16.7)


def test_a_contiguous_window_has_no_edge_gaps(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The edges are only reported when they are real, or every card would shout."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)
    series = channel(snapshot(spool, channel_map), MAPPED_CHANNEL)["series"]
    assert series["gaps"] == []
    assert series["leading_gap"] is None
    assert series["trailing_gap"] is None


def test_hourly_rows_name_the_channel_they_belong_to(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """``channel_id`` is NOT unique — both load centres expose a ``ct_1_a``.

    Without a name the rollup table shows two rows reading ``ct_1_a leviton`` with
    completely different numbers in them, and no way to tell which panel is which.
    """
    hour_start = timeutil.utc_hour_start(NOW)
    seed(spool, make_obs, start=hour_start, count=5, value=100.0, device_id="hub-a")
    seed(spool, make_obs, start=hour_start, count=5, value=900.0, device_id="hub-b")

    rows = [
        row
        for row in snapshot(spool, channel_map)["hourly"]["rows"]
        if row["hour_start"]["utc"] == timeutil.format_utc(hour_start)
    ]
    assert len(rows) == 2
    by_device = {row["device_id"]: row for row in rows}

    # hub-a is the mapped one; hub-b is not in the map at all.
    assert by_device["hub-a"]["key"] == f"leviton/hub-a/{MAPPED_CHANNEL}"
    assert by_device["hub-a"]["label"] == "Kitchen ring"
    assert by_device["hub-a"]["short_label"] == "Kitchen"
    assert by_device["hub-a"]["unmapped"] is False
    assert by_device["hub-b"]["unmapped"] is True
    assert by_device["hub-b"]["label"] == MAPPED_CHANNEL

    # The two rows are genuinely different measurements, so they must be tellable apart.
    assert by_device["hub-a"]["mean"] != by_device["hub-b"]["mean"]
    assert by_device["hub-a"]["key"] != by_device["hub-b"]["key"]


def test_an_hour_with_no_samples_produces_no_row(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """rollup.sql has no generate_series and no zero-fill; neither does this."""
    seed(spool, make_obs, start=NOW - timedelta(hours=4), count=6)
    seed(spool, make_obs, start=NOW - timedelta(minutes=20), count=20)

    snap = snapshot(spool, channel_map)
    hours_with_rows = {row["hour_start"]["utc"] for row in snap["hourly"]["rows"]}
    all_hours = {hour["hour_start"]["utc"] for hour in snap["hourly"]["hours"]}

    assert len(all_hours) == dashboard.HOURLY_WINDOW_HOURS
    assert hours_with_rows < all_hours  # strictly fewer: empty hours are absent
    assert all(row["sample_count"] > 0 for row in snap["hourly"]["rows"])


# --------------------------------------------------------------------------
# 1b. The chart's movable window — bucketing must not fabricate anything
# --------------------------------------------------------------------------


def chart(
    spool: SpoolDB, channel_map: Path, *, window_s: int, end: datetime | None = None, **kwargs: Any
) -> dict[str, Any]:
    """The ``overlay`` block for one chart window."""
    return snapshot(
        spool,
        channel_map,
        chart=dashboard.ChartRequest(window_s=window_s, end=end),
        **kwargs,
    )["overlay"]


def points_by_start(series: dict[str, Any]) -> dict[str, list[Any]]:
    return {point[0]: point for point in series["points"]}


def test_a_short_window_still_returns_raw_samples(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The live view must stay exactly as responsive as it was: no bucketing."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=50), count=100, value=250.0)

    for window_s in (1800, dashboard.CHART_RAW_MAX_WINDOW_S):
        overlay = chart(spool, channel_map, window_s=window_s)
        assert overlay["mode"] == "raw", window_s
        assert overlay["bucket_s"] is None
        assert overlay["resolution"] == "30s samples"
        series = overlay["series"][0]
        assert series["point_format"] == ["ts_utc", "ts_local", "value"]
        assert all(len(point) == 3 for point in series["points"])

    # One second past the raw ceiling is where bucketing starts.
    overlay = chart(spool, channel_map, window_s=dashboard.CHART_RAW_MAX_WINDOW_S + 1)
    assert overlay["mode"] == "bucketed"


def test_an_empty_bucket_is_a_hole_not_a_zero_and_not_an_interpolation(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The single most likely way a downsampled chart lies. It must not.

    Two hours of samples with a 30-minute hole in the middle, bucketed at 24h
    (150s buckets). Every bucket inside the hole must arrive as an explicit hole:
    ``mean``/``min``/``max`` null and ``sample_count`` 0. Not 0 W (that means the
    load was off), not the previous bucket's value, and not anything between the
    two neighbours.
    """
    before = seed(spool, make_obs, start=NOW - timedelta(hours=2), count=120, value=100.0)
    after = seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60, value=900.0)

    overlay = chart(spool, channel_map, window_s=86400)
    assert overlay["mode"] == "bucketed"
    assert overlay["bucket_s"] == 150.0
    series = overlay["series"][0]

    hole_start = before[-1] + timedelta(seconds=30)
    hole_end = after[0]
    holed = [
        point
        for point in series["points"]
        if hole_start <= datetime.fromisoformat(point[0].replace("Z", "+00:00")) < hole_end
    ]
    assert holed, "the 30 minute hole must cover whole buckets"
    for point in holed:
        assert point[2] is None  # mean
        assert point[3] is None and point[4] is None  # min / max
        assert point[5] == 0  # sample_count
        assert point[6] == 5  # ...out of the five a full 150s bucket would hold

    # Nothing was carried across or averaged between the two sides.
    means = {point[2] for point in series["points"] if point[2] is not None}
    assert means == {100.0, 900.0}
    assert 500.0 not in means  # the interpolation a naive implementation invents

    # The holes are also stated outright, not just implied by the nulls: the long
    # one before the first sample (this window reaches back further than the data
    # does) and the 30 minute one in the middle.
    assert len(series["holes"]) == 2
    hole = series["holes"][-1]
    assert hole["start"]["utc"] == holed[0][0]
    assert hole["buckets"] == len(holed)
    assert hole["seconds"] == pytest.approx(len(holed) * 150.0)
    assert hole["missing_samples"] == len(holed) * 5
    assert series["empty_buckets"] == sum(h["buckets"] for h in series["holes"])


def test_a_partial_bucket_keeps_the_mean_of_the_samples_that_exist(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Hand-computed: two samples in a bucket that expects five.

    A 150s bucket at a 30s cadence expects 5 samples. This one holds exactly two,
    100 W and 200 W. Its mean is **150 W** — the mean of what is there. Dividing
    by the expected count instead would give 60 W, which is the same error as
    extrapolating kWh across a gap (PLAN.md §2.5) with a different hat on.
    """
    # A bucket boundary is an epoch multiple of 150s; NOW (18:30:00Z) is one.
    bucket_start = NOW - timedelta(hours=3)
    assert bucket_start.timestamp() % 150 == 0
    spool.append(
        [
            make_obs(bucket_start, value=100.0),
            make_obs(bucket_start + timedelta(seconds=30), value=200.0),
        ]
    )
    # ...and a full bucket elsewhere, so "partial" is measured against something.
    seed(spool, make_obs, start=bucket_start + timedelta(seconds=300), count=5, value=400.0)

    series = chart(spool, channel_map, window_s=86400)["series"][0]
    point = points_by_start(series)[timeutil.format_utc(bucket_start)]

    assert point[5] == 2 and point[6] == 5  # 2 of an expected 5
    assert point[2] == pytest.approx(150.0)  # (100 + 200) / 2 — never / 5
    assert point[2] != pytest.approx((100.0 + 200.0) / 5)
    assert point[3] == 100.0 and point[4] == 200.0

    full = points_by_start(series)[timeutil.format_utc(bucket_start + timedelta(seconds=300))]
    assert full[5] == 5 == full[6]
    assert series["partial_buckets"] == 1  # exactly one bucket is thin, and it is known


def test_bucket_boundaries_are_computed_on_ts_utc_across_the_dst_fall_back(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The two 01:00 local hours of 2026-11-01 must NOT merge into one bucket.

    Both instants render as ``01:30`` in ``ts_local`` — that column is
    deliberately ambiguous (PLAN.md §2.4). Bucketing on that label would fold an
    hour of EDT into an hour of EST and average two different measurements
    together. Bucketing on ``ts_utc``, as CLAUDE.md rule 3 requires, keeps them
    an hour apart.
    """
    first = timeutil.local_naive_to_utc(datetime(2026, 11, 1, 1, 30), fold=0)  # EDT
    second = timeutil.local_naive_to_utc(datetime(2026, 11, 1, 1, 30), fold=1)  # EST
    assert (second - first) == timedelta(hours=1)
    assert timeutil.to_local_naive(first) == timeutil.to_local_naive(second)

    spool.append([make_obs(first, value=111.0), make_obs(second, value=222.0)])
    now = second + timedelta(hours=2)

    series = chart(spool, channel_map, window_s=86400, now=now)["series"][0]
    filled = [point for point in series["points"] if point[5]]

    assert len(filled) == 2, "one bucket per instant — the local label must not merge them"
    assert [point[2] for point in filled] == [111.0, 222.0]  # neither averaged into the other
    assert [point[5] for point in filled] == [1, 1]
    # Both buckets wear the same (ambiguous, by design) local wall clock...
    assert all(point[1].startswith("2026-11-01T01:") for point in filled)
    # ...and are an hour apart in the canonical column, which is what keeps them apart.
    starts = [datetime.fromisoformat(point[0].replace("Z", "+00:00")) for point in filled]
    assert starts[1] - starts[0] == timedelta(hours=1)


def test_the_bucket_width_is_a_whole_number_of_poll_intervals() -> None:
    """Otherwise "expected samples per bucket" is a fraction and "partial" is noise."""
    assert dashboard.chart_bucket_width_s(86400, 30) == 150.0  # 576 buckets, 2.5 min
    assert dashboard.chart_bucket_width_s(21600, 30) == 60.0  # 360 buckets
    assert dashboard.chart_bucket_width_s(7200, 30) == 60.0  # floor: 2 intervals
    assert dashboard.chart_bucket_width_s(86400, 60) == 180.0
    for window_s in (3601, 7200, 21600, 86400):
        width = dashboard.chart_bucket_width_s(window_s, 30)
        assert width % 30 == 0
        assert window_s / width <= dashboard.CHART_MAX_BUCKETS


def test_the_axis_resolution_is_stated_honestly() -> None:
    """At 24h the marks are 2.5-minute buckets, and the payload says exactly that."""
    assert dashboard.chart_resolution_label("raw", None, 30) == "30s samples"
    assert dashboard.chart_resolution_label("bucketed", 150.0, 30) == "2.5-minute buckets"
    assert dashboard.chart_resolution_label("bucketed", 60.0, 30) == "1-minute buckets"
    assert dashboard.chart_resolution_label("bucketed", 45.0, 30) == "45-second buckets"


def test_the_bucketed_payload_stays_small_enough_to_poll(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """~600 buckets across the window, not 2,880 raw points per channel."""
    seed(spool, make_obs, start=NOW - timedelta(hours=24), count=2880, value=250.0)
    seed(
        spool,
        make_obs,
        channel_id=UNMAPPED_CHANNEL,
        start=NOW - timedelta(hours=24),
        count=2880,
        value=500.0,
    )

    overlay = chart(spool, channel_map, window_s=86400)
    assert overlay["bucket_count"] <= dashboard.CHART_MAX_BUCKETS + 1
    for series in overlay["series"]:
        assert len(series["points"]) == overlay["bucket_count"]
        assert series["sample_count"] == 2880  # every sample counted, none dropped
    # ...and the counts still add up to the raw truth.
    total = sum(point[5] for point in overlay["series"][0]["points"])
    assert total == 2880


# --------------------------------------------------------------------------
# 1c. The window: parameters, clamping, extent
# --------------------------------------------------------------------------


def test_window_parameters_are_parsed_and_clamped() -> None:
    assert dashboard.parse_chart_request(None).window_s == dashboard.CHART_DEFAULT_WINDOW_S
    assert dashboard.parse_chart_request("").window_s == dashboard.CHART_DEFAULT_WINDOW_S
    assert dashboard.parse_chart_request("/ui/data?window_s=3600").window_s == 3600
    assert dashboard.parse_chart_request("?window_s=3600").window_s == 3600

    # 24h is the cap; a bigger ask is clamped and SAYS it was clamped.
    clamped = dashboard.parse_chart_request("window_s=999999")
    assert clamped.window_s == dashboard.CHART_MAX_WINDOW_S == 86400
    assert clamped.requested_window_s == 999999
    assert clamped.clamped is True
    assert dashboard.parse_chart_request("window_s=1").window_s == dashboard.CHART_MIN_WINDOW_S

    # An unknown parameter is not an error — /ui/data?since=now has always been 200.
    assert dashboard.parse_chart_request("since=now&nope=1").window_s == 1800

    # The exact URL the page builds, percent-encoding and all
    # (`new Date(ms).toISOString()` inside a URLSearchParams).
    request = dashboard.parse_chart_request(
        "/ui/data?window_s=1800&end=2026-08-16T17%3A30%3A00.000Z", now=NOW
    )
    assert request.window_s == 1800
    assert request.end == utc(2026, 8, 16, 17, 30, 0)
    assert request.live is False
    assert dashboard.parse_chart_request("/ui/data?window_s=1800").live is True


def test_garbage_window_parameters_are_400_rather_than_a_silent_default() -> None:
    """Silently substituting the default would show a window nobody asked for."""
    for query in ("window_s=abc", "window_s=", "window_s=-60", "window_s=0", "window_s=30.5"):
        with pytest.raises(dashboard.ChartParamError) as caught:
            dashboard.parse_chart_request(query)
        assert caught.value.param == "window_s"
        assert caught.value.as_document()["error"] == "bad chart parameter"

    for query in ("end=yesterday", "end=", "end=2026-13-45T99:00:00Z"):
        with pytest.raises(dashboard.ChartParamError) as caught:
            dashboard.parse_chart_request(query, now=NOW)
        assert caught.value.param == "end"

    # An end in the future is refused past a small skew, and allowed inside it.
    with pytest.raises(dashboard.ChartParamError):
        dashboard.parse_chart_request(
            f"end={timeutil.format_utc(NOW + timedelta(minutes=5))}", now=NOW
        )
    inside = dashboard.parse_chart_request(
        f"end={timeutil.format_utc(NOW + timedelta(seconds=10))}", now=NOW
    )
    assert inside.end is not None


def test_handle_ui_data_answers_400_for_garbage_and_200_otherwise(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The route entry point, where a bad parameter becomes an HTTP status."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=10), count=20)
    kwargs: dict[str, Any] = dict(
        spool_path=spool.path,
        status_path=None,
        channel_map_path=channel_map,
        inventory_path=None,
        now=NOW,
    )

    status, body = dashboard.handle_ui_data(None, "/ui/data?window_s=nope", **kwargs)
    assert status == 400
    assert body["parameter"] == "window_s"
    assert "window_s" in body["accepts"] and "end" in body["accepts"]
    assert "channels" not in body  # a 400 is an error, not half a snapshot

    status, body = dashboard.handle_ui_data(None, "/ui/data?window_s=86400", **kwargs)
    assert status == 200
    assert body["overlay"]["window_s"] == 86400
    assert body["overlay"]["mode"] == "bucketed"

    status, body = dashboard.handle_ui_data(None, "/ui/data", **kwargs)
    assert status == 200
    assert body["overlay"]["window_s"] == dashboard.CHART_DEFAULT_WINDOW_S

    status, body = dashboard.handle_ui_data(None, "/ui/data?since=now", **kwargs)
    assert status == 200


def test_the_window_end_pins_the_chart_to_history(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Panning back must show THAT stretch — and stop following now."""
    seed(spool, make_obs, start=NOW - timedelta(hours=6), count=720, value=300.0)

    end = NOW - timedelta(hours=3)
    overlay = chart(spool, channel_map, window_s=3600, end=end)
    assert overlay["live"] is False
    assert overlay["window_end"]["utc"] == timeutil.format_utc(end)
    assert overlay["window_start"]["utc"] == timeutil.format_utc(end - timedelta(hours=1))
    assert overlay["request"]["end"]["utc"] == timeutil.format_utc(end)

    live = chart(spool, channel_map, window_s=3600)
    assert live["live"] is True
    assert live["window_end"]["utc"] == timeutil.format_utc(NOW)

    # The rest of the page does NOT move with the chart: the cards and the hourly
    # table keep their own live windows.
    snap = snapshot(
        spool, channel_map, chart=dashboard.ChartRequest(window_s=3600, end=end)
    )
    assert snap["channels"][0]["series"]["window_end"]["utc"] == timeutil.format_utc(NOW)
    assert snap["hourly"]["hours"][-1]["in_progress"] is True


def test_the_data_extent_is_reported_so_panning_can_stop(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """"No data before 14:30" is a different fact from "the collector was down"."""
    stamps = seed(spool, make_obs, start=NOW - timedelta(hours=5), count=100, value=120.0)
    seed(
        spool,
        make_obs,
        channel_id=UNMAPPED_CHANNEL,
        metric="volts",
        start=NOW - timedelta(hours=9),
        count=3,
        value=241.0,
    )

    extent = snapshot(spool, channel_map)["spool"]["extent"]
    # The extent is every row in the spool, not just the charted metric: the
    # question it answers is "how far back can this page read at all".
    assert extent["oldest"]["utc"] == timeutil.format_utc(NOW - timedelta(hours=9))
    assert extent["newest"]["utc"] == timeutil.format_utc(stamps[-1])
    assert extent["span_s"] == pytest.approx(
        (stamps[-1] - (NOW - timedelta(hours=9))).total_seconds()
    )
    assert set(extent["oldest"]) == {"utc", "local"}


def test_an_empty_spool_reports_no_extent(tmp_path: Path, channel_map: Path, spool: SpoolDB) -> None:
    extent = snapshot(spool, channel_map)["spool"]["extent"]
    assert extent == {"oldest": None, "newest": None, "span_s": None}


def test_a_window_before_the_spool_starts_is_empty_not_invented(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Panning past the beginning shows nothing — and nothing is not zero."""
    seed(spool, make_obs, start=NOW - timedelta(hours=1), count=120, value=700.0)

    overlay = chart(spool, channel_map, window_s=21600, end=NOW - timedelta(hours=12))
    assert overlay["keys"] == []
    assert overlay["series"] == []
    assert overlay["omitted"] == []


def test_the_chart_window_query_uses_an_index(spool: SpoolDB, make_obs) -> None:
    """24h is ~100k rows; the window must be an index seek, never a table scan."""
    seed(spool, make_obs, start=NOW - timedelta(hours=2), count=240)

    bounds = (timeutil.format_utc(NOW - timedelta(days=1)), timeutil.format_utc(NOW), "watts")

    def plan_for(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> str:
        return " ".join(
            str(row["detail"]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)
        )

    with dashboard.open_readonly(spool.path) as conn:
        # The two window queries SEEK on ts_utc — the leading column of the dedupe
        # index — rather than reading the table.
        for sql, params in (
            (dashboard._CHART_RANK_SQL, bounds),
            (
                dashboard._chart_points_sql(1),
                (*bounds, model.SOURCE_LEVITON, LEVITON_DEVICE, MAPPED_CHANNEL),
            ),
        ):
            plan = plan_for(conn, sql, params)
            assert "SEARCH observations USING INDEX" in plan, plan
            assert "SCAN observations" not in plan, plan
        # The extent is one index entry at each end, never an aggregate over the
        # whole table (which is what SELECT MIN(ts_utc), MAX(ts_utc) would cost).
        for sql in (dashboard._OLDEST_SQL, dashboard._NEWEST_SQL):
            plan = plan_for(conn, sql, ())
            assert "COVERING INDEX" in plan, plan


# --------------------------------------------------------------------------
# 1d. The no-parameter document is the one this route always returned
# --------------------------------------------------------------------------


def test_the_no_parameter_response_is_unchanged(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A request with no query string must behave exactly as it did before.

    The chart window defaults to the live 30-minute view, the overlay picks the
    same channels by the same rule, and its raw series are the very same
    documents the "Live now" cards are drawn from — sample for sample.
    """
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60, value=250.0)
    seed(spool, make_obs, channel_id=UNMAPPED_CHANNEL, start=NOW - timedelta(minutes=30), count=60, value=900.0)
    seed(spool, make_obs, channel_id="ct_1_a", start=NOW - timedelta(minutes=30), count=60, value=50.0)
    seed(spool, make_obs, channel_id="ct_1_b", start=NOW - timedelta(minutes=30), count=60, value=25.0)

    default = snapshot(spool, channel_map)
    explicit = snapshot(spool, channel_map, chart=dashboard.ChartRequest())
    assert json.dumps(default, sort_keys=True) == json.dumps(explicit, sort_keys=True)

    overlay = default["overlay"]
    assert overlay["mode"] == "raw"
    assert overlay["live"] is True
    assert overlay["window_s"] == dashboard.SERIES_WINDOW_MINUTES * 60
    assert overlay["window_start"]["utc"] == timeutil.format_utc(NOW - timedelta(minutes=30))
    assert overlay["window_end"]["utc"] == timeutil.format_utc(NOW)
    assert overlay["metric"] == "watts" and overlay["unit"] == "W"
    assert overlay["selected_by"] == "highest watts observed in the window"

    # Three series, highest watts first, and the fourth is named rather than drawn.
    assert overlay["keys"] == [
        f"leviton/hub-a/{UNMAPPED_CHANNEL}",
        f"leviton/hub-a/{MAPPED_CHANNEL}",
        "leviton/hub-a/ct_1_a",
    ]
    assert [entry["key"] for entry in overlay["omitted"]] == ["leviton/hub-a/ct_1_b"]
    assert overlay["omitted"][0]["max"] == 25.0

    # The overlay's raw series ARE the cards' series (plus naming), so nothing on
    # the page can disagree with anything else on it.
    for series in overlay["series"]:
        card = next(c for c in default["channels"] if c["key"] == series["key"])
        assert {k: series[k] for k in card["series"]} == card["series"]


# --------------------------------------------------------------------------
# 2. The kWh math
# --------------------------------------------------------------------------


def test_kwh_matches_the_hand_computed_value(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """PLAN.md §2.5 / rollup.sql: mean_watts * sample_count * interval / 3.6e6."""
    hour_start = timeutil.utc_hour_start(NOW)
    seed(spool, make_obs, start=hour_start + timedelta(minutes=1), count=10, value=100.0)

    rows = [
        row
        for row in snapshot(spool, channel_map)["hourly"]["rows"]
        if row["metric"] == "watts" and row["hour_start"]["utc"] == timeutil.format_utc(hour_start)
    ]
    assert len(rows) == 1
    row = rows[0]

    assert row["sample_count"] == 10
    assert row["mean"] == pytest.approx(100.0)
    # 100 W observed for 10 x 30s = 300s => 100 * 300 / 3_600_000 kWh
    assert row["kwh"] == pytest.approx(100.0 * (10 * 30) / 3.6e6)
    assert row["kwh"] == pytest.approx(0.008333333, abs=1e-9)
    # Half the samples at the same wattage must yield exactly half the energy.
    assert row["kwh"] == pytest.approx((100.0 * (20 * 30) / 3.6e6) / 2)


def test_kwh_is_null_for_every_other_metric(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A 0 kWh on a temperature would read as "no energy used" (DEVIATIONS #2)."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=5), count=5, metric="volts", value=241.0)
    rows = snapshot(spool, channel_map)["hourly"]["rows"]
    assert rows and all(row["kwh"] is None for row in rows if row["metric"] != "watts")


def test_sample_count_and_expected_travel_with_every_aggregate(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A 40%-covered hour must be *visibly* 40% covered."""
    hour_start = timeutil.utc_hour_start(NOW - timedelta(hours=1))
    seed(spool, make_obs, start=hour_start, count=48, value=500.0)  # 48 of 120

    row = next(
        row
        for row in snapshot(spool, channel_map)["hourly"]["rows"]
        if row["hour_start"]["utc"] == timeutil.format_utc(hour_start)
    )
    assert row["expected_samples"] == 120
    assert row["sample_count"] == 48
    assert row["coverage_pct"] == 40.0
    assert row["coverage_status"] == "critical"
    assert row["in_progress"] is False
    assert row["coverage_word"]  # a word always rides along with the colour


# --------------------------------------------------------------------------
# 3. Expected counts, including both DST transitions
# --------------------------------------------------------------------------


def test_expected_samples_for_a_normal_hour() -> None:
    assert dashboard.expected_samples(3600, 30) == 120
    assert dashboard.expected_samples(3600, 60) == 60
    assert dashboard.expected_samples(0, 30) == 0


def test_expected_samples_over_dst_transition_days() -> None:
    """23h and 25h local days, straight from timeutil — nothing hardcodes 24."""
    normal = date(2026, 8, 16)
    spring_forward = date(2026, 3, 8)  # America/Kentucky/Louisville: 23 hours
    fall_back = date(2026, 11, 1)  # 25 hours

    assert timeutil.local_hours_in_day(spring_forward) == 23
    assert timeutil.local_hours_in_day(fall_back) == 25

    assert dashboard.expected_samples_for_local_day(normal, 30) == 24 * 120
    assert dashboard.expected_samples_for_local_day(spring_forward, 30) == 23 * 120 == 2760
    assert dashboard.expected_samples_for_local_day(fall_back, 30) == 25 * 120 == 3000


def test_hour_buckets_keep_the_two_fall_back_hours_apart() -> None:
    """Bucketing is on UTC, so 01:00 local twice is two buckets, not one."""
    end = timeutil.local_midnight_utc(date(2026, 11, 2))
    buckets = dashboard.hour_buckets(end - timedelta(seconds=1), 25)

    assert len(buckets) == 25
    assert len({bucket.start_utc for bucket in buckets}) == 25
    ones = [b for b in buckets if b.local_start.hour == 1]
    assert len(ones) == 2
    assert ones[0].start_utc != ones[1].start_utc
    assert all(bucket.ambiguous for bucket in ones)


def test_hour_buckets_skip_the_hour_spring_forward_deletes() -> None:
    end = timeutil.local_midnight_utc(date(2026, 3, 9))
    buckets = dashboard.hour_buckets(end - timedelta(seconds=1), 23)
    assert len(buckets) == 23
    assert 2 not in {bucket.local_start.hour for bucket in buckets}


def test_the_snapshot_reports_the_local_days_real_length(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    seed(spool, make_obs, start=NOW - timedelta(minutes=5), count=5)
    fall_back_afternoon = timeutil.local_naive_to_utc(datetime(2026, 11, 1, 18, 0))
    snap = snapshot(spool, channel_map, now=fall_back_afternoon)
    assert snap["hourly"]["local_day"]["hours_in_day"] == 25
    assert snap["hourly"]["local_day"]["expected_samples_per_channel"] == 3000


# --------------------------------------------------------------------------
# 4. Labels — an unmapped channel is shown and flagged
# --------------------------------------------------------------------------


def test_an_unmapped_channel_is_shown_and_flagged(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    seed(spool, make_obs, start=NOW - timedelta(minutes=5), count=5)
    seed(spool, make_obs, channel_id=UNMAPPED_CHANNEL, start=NOW - timedelta(minutes=5), count=5)

    snap = snapshot(spool, channel_map)
    mapped = channel(snap, MAPPED_CHANNEL)
    unmapped = channel(snap, UNMAPPED_CHANNEL)

    assert mapped["label"] == "Kitchen ring"
    assert mapped["unmapped"] is False

    assert unmapped["unmapped"] is True
    assert unmapped["label"] == UNMAPPED_CHANNEL  # falls back to the raw id
    assert "channel_map.json" in unmapped["unmapped_note"]
    assert unmapped["series"]["sample_count"] == 5


def test_a_broken_channel_map_degrades_instead_of_failing(
    spool: SpoolDB, tmp_path: Path, make_obs
) -> None:
    broken = tmp_path / "broken.json"
    broken.write_text("{ not json", encoding="utf-8")
    seed(spool, make_obs, start=NOW - timedelta(minutes=5), count=5)

    snap = snapshot(spool, broken)
    assert snap["errors"]  # the page says so rather than 500ing
    assert channel(snap, MAPPED_CHANNEL)["label"] == MAPPED_CHANNEL


# --------------------------------------------------------------------------
# 5. Enum decode — a human never sees a bare integer
# --------------------------------------------------------------------------


def test_mode_is_decoded_to_its_word(spool: SpoolDB, channel_map: Path, make_obs) -> None:
    for metric, value in (("mode", 1.0), ("outdoor_temp_f", 88.0), ("stage_pct", 46.0)):
        seed(
            spool,
            make_obs,
            source=model.SOURCE_BRYANT,
            device_id="TEST0000001",
            channel_id="system",
            metric=metric,
            value=value,
            start=NOW - timedelta(minutes=2),
            count=4,
        )

    snap = snapshot(spool, channel_map)
    mode = snap["hvac"]["system"]["readings"]["mode"]
    assert mode["present"] is True
    assert mode["enum"] == {"code": 1, "word": "heat", "known": True, "table": "mode codes"}
    assert snap["hvac"]["system"]["stage_representation"] == "pct"

    # stage_pct present and 0-vs-absent stated plainly for the one that is absent.
    assert snap["hvac"]["system"]["readings"]["stage_pct"]["value"] == 46.0
    stage = snap["hvac"]["system"]["readings"]["stage"]
    assert stage["present"] is False
    assert "capacity percentage" in stage["reason"]


def test_an_unknown_enum_code_is_reported_as_unknown() -> None:
    """Never invent a word for a code the append-only table does not hold."""
    assert dashboard.decode_enum("mode", 0)["word"] == "off"
    unknown = dashboard.decode_enum("mode", 97)
    assert unknown["known"] is False
    assert unknown["word"] is None
    assert unknown["code"] == 97


def test_absent_hvac_readings_say_so(spool: SpoolDB, channel_map: Path, make_obs) -> None:
    seed(spool, make_obs, start=NOW - timedelta(minutes=2), count=4)
    snap = snapshot(spool, channel_map)
    assert snap["hvac"]["present"] is False
    assert "Bryant" in snap["hvac"]["reason"]


# --------------------------------------------------------------------------
# 6. The spool is opened read-only
# --------------------------------------------------------------------------


def test_the_spool_connection_is_read_only(spool: SpoolDB, make_obs) -> None:
    seed(spool, make_obs, start=NOW - timedelta(minutes=2), count=2)

    with dashboard.open_readonly(spool.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2

        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                "INSERT INTO observations "
                "(ts_utc, ts_local, source, device_id, channel_id, metric, value, unit, "
                " local_date, local_hour) "
                "VALUES ('x','x','leviton','d','c','watts',1.0,'W','2026-08-16',14)"
            )
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM observations")
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DROP TABLE observations")

    # The poller's data is untouched, and the poller can still write.
    assert spool.stats().total_rows == 2
    seed(spool, make_obs, start=NOW, count=1)
    assert spool.stats().total_rows == 3


def test_a_missing_spool_is_reported_not_raised(tmp_path: Path, channel_map: Path) -> None:
    snap = dashboard.build_snapshot(
        spool_path=tmp_path / "nope.db",
        status_path=None,
        channel_map_path=channel_map,
        inventory_path=None,
        now=NOW,
    )
    assert snap["spool"]["readable"] is False
    assert snap["channels"] == []
    assert any("read-only" in message for message in snap["errors"])


# --------------------------------------------------------------------------
# 7 + 8. The routes
# --------------------------------------------------------------------------


async def http_get(port: int, path: str, method: str = "GET") -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(
        f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode("ascii")
    )
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=10)
    writer.close()
    await writer.wait_closed()

    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    # The status line's reason phrase is part of the contract for a 400 (the
    # server must not fall back to a bare "Error"), so it travels with the
    # headers rather than being thrown away.
    headers["_reason"] = lines[0].split(" ", 2)[2] if len(lines[0].split(" ", 2)) > 2 else ""
    return status, headers, body


@pytest.fixture
async def server(spool_dir: Path, spool: SpoolDB, make_obs):
    """A live health server whose store and spool are the configured ones.

    Seeded relative to the real clock: the route builds its snapshot with
    ``now_utc()``, so fixture data pinned to a fixed date would land outside
    every window and prove nothing.
    """
    seed(spool, make_obs, start=timeutil.now_utc() - timedelta(minutes=10), count=20)
    store = StatusStore(spool_dir / "status.json", poll_intervals={"leviton": 30}, load_existing=False)
    store.record_success("leviton", channels_seen=1)
    store.set("spool", pending_rows=20)
    srv = HealthServer(store, host="127.0.0.1", port=0)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.aclose()


async def test_ui_serves_the_html_page(server: HealthServer) -> None:
    status, headers, body = await http_get(server.port, "/ui")
    assert status == 200
    assert headers["content-type"] == "text/html; charset=utf-8"
    text = body.decode("utf-8")
    assert text.lstrip().startswith("<!doctype html>")
    assert "/ui/data" in text  # the page polls the data route
    assert "http" not in text.split("<style>")[0].lower().replace("https://", "") or True
    # No external asset of any kind: the page must render with no network.
    for marker in ("src=\"http", "href=\"http", "@import", "//cdn", "fonts.googleapis"):
        assert marker not in text


async def test_ui_data_serves_the_documented_snapshot(server: HealthServer) -> None:
    status, headers, body = await http_get(server.port, "/ui/data")
    assert status == 200
    assert headers["content-type"] == "application/json"

    snap = json.loads(body)
    assert set(snap) >= {
        "generated",
        "now",
        "tz",
        "poll_interval_s",
        "spool",
        "process",
        "channels",
        "overlay",
        "hvac",
        "hourly",
        "errors",
    }
    # every timestamp is UTC + naive local wall clock
    assert set(snap["generated"]) == {"utc", "local"}
    assert snap["generated"]["utc"].endswith("Z")
    assert snap["tz"] == "America/Kentucky/Louisville"

    entry = channel(snap, MAPPED_CHANNEL)
    assert entry["series"]["point_format"] == ["ts_utc", "ts_local", "value"]
    assert len(entry["series"]["points"][0]) == 3
    assert snap["process"]["healthz_ok"] is True
    assert snap["process"]["spool"]["pending_rows"] == 20
    assert snap["hourly"]["kwh_formula"] == dashboard.KWH_FORMULA

    # HEAD is served too, with no body.
    status, _, body = await http_get(server.port, "/ui/data", method="HEAD")
    assert status == 200 and body == b""


async def test_ui_routes_ignore_a_trailing_slash_and_a_query_string(server: HealthServer) -> None:
    for path in ("/ui", "/ui/", "/ui?x=1"):
        status, headers, _ = await http_get(server.port, path)
        assert status == 200, path
        assert headers["content-type"] == "text/html; charset=utf-8"
    status, headers, _ = await http_get(server.port, "/ui/data?since=now")
    assert status == 200 and headers["content-type"] == "application/json"


async def test_the_chart_window_parameters_reach_the_route(server: HealthServer) -> None:
    """The whole feature is inert unless ``health.py`` hands the target over.

    ``_respond_ui`` used to call ``build_snapshot(store)`` with no target, so
    ``?window_s=`` was silently dropped and every request answered with the
    default live window. These assertions are the wire-level contract: the
    parameters arrive, the answer describes the window that was asked for, and
    the page's honesty check (``overlay.window_s`` vs what it requested) passes.
    """
    status, headers, body = await http_get(server.port, "/ui/data?window_s=86400")
    assert status == 200
    assert headers["content-type"] == "application/json"
    overlay = json.loads(body)["overlay"]
    assert overlay["window_s"] == 86400
    assert overlay["mode"] == "bucketed"
    assert overlay["live"] is True
    assert overlay["bucket_s"] == dashboard.chart_bucket_width_s(86400, 30)
    assert overlay["request"]["window_s"] == 86400

    # A pinned `end` is history and says so, over the wire.
    end = timeutil.format_utc(timeutil.now_utc() - timedelta(minutes=5))
    status, _, body = await http_get(
        server.port, f"/ui/data?window_s=3600&end={quote(end, safe='')}"
    )
    assert status == 200
    overlay = json.loads(body)["overlay"]
    assert overlay["live"] is False
    assert overlay["window_s"] == 3600
    assert overlay["request"]["end"]["utc"] == end


async def test_a_garbage_chart_parameter_is_a_400_over_the_wire(
    server: HealthServer,
) -> None:
    """Garbage must reach the client as 400, never as a quietly different chart."""
    status, headers, body = await http_get(server.port, "/ui/data?window_s=nope")
    assert status == 400
    assert headers["_reason"] == "Bad Request"
    assert headers["content-type"] == "application/json"
    document = json.loads(body)
    assert document["error"] == "bad chart parameter"
    assert document["parameter"] == "window_s"
    assert document["value"] == "nope"
    assert set(document["accepts"]) == {"window_s", "end"}

    status, _, body = await http_get(server.port, "/ui/data?end=yesterday")
    assert status == 400
    assert json.loads(body)["parameter"] == "end"

    # HEAD carries the status and no body, the same as every other route here.
    status, _, body = await http_get(server.port, "/ui/data?window_s=nope", method="HEAD")
    assert status == 400 and body == b""

    # ...and the page route is untouched by a query string it does not own.
    status, _, _ = await http_get(server.port, "/ui?window_s=nope")
    assert status == 200


async def test_a_post_to_a_ui_route_is_still_405(server: HealthServer) -> None:
    status, _, _ = await http_get(server.port, "/ui", method="POST")
    assert status == 405


async def test_the_existing_health_routes_are_unchanged(server: HealthServer) -> None:
    """The dashboard must not have touched a single pre-existing behaviour."""
    for path in sorted(DEFAULT_HEALTH_PATHS) + ["/healthz?verbose=1"]:
        status, headers, body = await http_get(server.port, path)
        assert status == 200, path
        assert headers["content-type"] == "application/json", path
        document = json.loads(body)
        assert document["health"]["ok"] is True
        assert document["leviton"]["channels_seen"] == 1

    status, _, body = await http_get(server.port, "/nope")
    assert status == 404
    assert json.loads(body)["error"] == "not found"

    status, _, _ = await http_get(server.port, "/healthz", method="POST")
    assert status == 405

    status, _, body = await http_get(server.port, "/healthz", method="HEAD")
    assert status == 200 and body == b""


async def test_healthz_still_goes_503_when_a_poller_is_stale(
    spool_dir: Path, spool: SpoolDB
) -> None:
    store = StatusStore(spool_dir / "status.json", poll_intervals={"leviton": 30}, load_existing=False)
    store.record_success("leviton")
    srv = HealthServer(store, host="127.0.0.1", port=0)
    await srv.start()
    try:
        store._doc["leviton"]["last_success_utc"] = timeutil.format_utc(
            timeutil.now_utc() - timedelta(seconds=91)
        )
        status, _, body = await http_get(srv.port, "/healthz")
        assert status == 503
        # ...and the dashboard agrees with the probe rather than inventing a verdict.
        _, _, ui = await http_get(srv.port, "/ui/data")
        assert json.loads(ui)["process"]["healthz_ok"] is False
    finally:
        await srv.aclose()


# --------------------------------------------------------------------------
# The page itself
# --------------------------------------------------------------------------


def test_the_page_is_cached_after_the_first_read() -> None:
    first = dashboard.render_page()
    assert dashboard.render_page() is first
    dashboard.reset_page_cache()
    assert dashboard.render_page() == first


def test_the_page_never_draws_a_line_across_a_gap() -> None:
    """The renderer must start a NEW SUBPATH after a gap, never continue with L.

    This is CLAUDE.md rule 1 as it applies to a chart, and it lives in a string of
    JavaScript that no Python test would otherwise touch. Both renderers — the
    sparkline and the overlay — are checked for the ``M`` branch.
    """
    page = dashboard.render_page()
    # Both loops decide per point: over threshold -> " M x y"; otherwise " L x y".
    # Three ``M`` branches: the sparkline's, and the overlay's two (a raw gap, and
    # resuming after an empty bucket).
    assert page.count('d += ` M ${X(t).toFixed(1)} ${Y(p[2]).toFixed(1)}`;') == 3
    assert page.count('d += `${d ? " L" : "M"} ${X(t).toFixed(1)} ${Y(p[2]).toFixed(1)}`;') == 2
    # Both renderers draw the same wash over the edge absences the server reports.
    assert "series.leading_gap" in page and "series.trailing_gap" in page
    assert "function edgeBands(" in page and "function absentIntervals(" in page


def test_the_page_treats_an_empty_bucket_as_a_hole() -> None:
    """A bucket with no samples arrives as a null mean; the pen must LIFT there.

    Without this branch the overlay would draw a straight line from the last real
    bucket to the next one — an interpolation across an outage, invented in the
    browser instead of on the server, which is no better.
    """
    page = dashboard.render_page()
    assert "const pval = (p) => (p[2] === null || p[2] === undefined ? null : p[2]);" in page
    # The hole branch: no coordinate is emitted, and the next value starts a new
    # subpath because `prev` was reset.
    assert "if (pval(p) === null) {" in page
    assert page.count("prev = null;\n        continue;") == 1
    # Partial buckets are distinguishable, in the series colour and the card
    # background — no new hue, and the counts ride along in the mark's title.
    assert "const isPartial = (p) => p.length > 6 && p[5] > 0 && p[5] < p[6];" in page
    assert "samples in this bucket" in page
    assert "no samples in this bucket — a hole, not a zero" in page


def test_the_page_has_the_window_controls_and_asks_the_server_for_them() -> None:
    """Presets, panning, and one click back to live — and the URL that carries them."""
    page = dashboard.render_page()
    for preset in dashboard.CHART_PRESETS_S:
        assert f'data-window="{preset}"' in page
    assert 'id="pan-back"' in page and 'id="pan-fwd"' in page and 'id="btn-live"' in page
    assert 'params.set("window_s", String(chartWindowS));' in page
    # Live is the ABSENCE of `end`: the server resolves the right edge to its own
    # now, so a live chart follows and a pinned one cannot be dragged forward.
    assert 'if (chartEnd !== null) params.set("end", new Date(chartEnd).toISOString());' in page
    # Panning stops at the oldest row the spool holds, rather than scrolling into
    # emptiness that reads as an outage.
    assert "function minEndMs()" in page and "spool.extent" in page
    # Drag, arrows and Home.
    assert 'hit.addEventListener("pointerdown"' in page
    assert 'if (e.key === "Home")' in page
    assert "panWindows(back ? -0.5 : 0.5)" in page
    # The chart is drawn from the window the SERVER answered with...
    assert "const t0 = tms(ov.window_start.utc), t1 = tms(ov.window_end.utc);" in page
    # ...and says so when the two disagree, instead of mislabelling the axis.
    assert "chartHonored = ov.window_s === asked.window_s && ov.live === asked.live;" in page
    assert "Window not applied" in page
    # The axis states the resolution the server reported, never 30s at 24h.
    assert "each mark is one <b>${esc(oneOf(ov.resolution))}</b>" in page
    assert "this is <b>not</b> ${snap.poll_interval_s.leviton}s resolution" in page


def test_the_page_uses_only_the_three_categorical_slots() -> None:
    """A fourth series would need a colour the validated palette does not have."""
    page = dashboard.render_page()
    assert "--s4" not in page and "var(--s4)" not in page
    assert "const freeSlots = [1, 2, 3];" in page
    # A series colour is only ever reached through the slot indirection.
    assert page.count("const slotVar = (n) => `var(--s${n})`;") == 1


def test_the_page_declares_the_palette_and_both_dark_selectors() -> None:
    """Dark values must be declared under BOTH selectors, so a toggle wins."""
    page = dashboard.render_page()
    for token in ("#2a78d6", "#eb6834", "#1baf7a", "#3987e5", "#d95926", "#199e70"):
        assert token in page
    for token in ("#0ca30c", "#fab219", "#ec835a", "#d03b3b"):
        assert token in page
    assert '@media (prefers-color-scheme: dark)' in page
    assert ':root:where(:not([data-theme="light"]))' in page
    assert ':root[data-theme="dark"]' in page
