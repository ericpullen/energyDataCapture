"""``/ui/history`` — the archive view.

The SQL blocks are exercised against **local** Parquet written by these tests, so
nothing here touches S3: ``historyview``'s block functions take a
:class:`~energy_capture.historyview.Sources` whose members are just paths, and
DuckDB reads a local file exactly as it reads an ``s3://`` URI.

The tests that matter most are the ones pinning the traps the module was written
around — grouping by ``channel_id`` alone, summing across measurement levels,
un-pinned ``interval_s``, and coverage measured against hours *present* rather
than hours *expected*. Each of those produced a wrong number on real data during
the S3 rollout.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from energy_capture import dashboard, historyview, model

HUB_A = "1000_0046_1D52"
HUB_B = "1000_0046_1D48"
SERIAL = "4022W200213"
HOUSE = "1308468"
BARN = "1326254"


# --------------------------------------------------------------- fixtures


def _hourly_rows(day: date, channels, *, hours: int = 24, watts: float = 100.0):
    """One watts row per hour per channel, with a full ``sample_count``."""
    rows = []
    for hour in range(hours):
        local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
        utc = local + timedelta(hours=4)  # EDT; only ordering matters here
        for device_id, channel_id in channels:
            rows.append(
                {
                    "hour_start_utc": utc,
                    "local_hour_start": local,
                    "source": model.SOURCE_LEVITON,
                    "device_id": device_id,
                    "channel_id": channel_id,
                    "metric": "watts",
                    "unit": "W",
                    "mean": watts,
                    "min": watts,
                    "max": watts,
                    "p95": watts,
                    "sample_count": historyview.SAMPLES_PER_HOUR,
                    "first_ts_utc": utc,
                    "last_ts_utc": utc,
                    "kwh": watts * historyview.SAMPLES_PER_HOUR * 30 / 3.6e6,
                }
            )
    return rows


def _write(path: Path, rows, schema: pa.Schema) -> str:
    table = pa.Table.from_pylist(rows, schema=schema)
    # Timestamps in the fixtures are naive local wall clocks, which is what
    # local_hour_start/ts_local actually are (CLAUDE.md rule 3).
    pq.write_table(table, path)
    return str(path)


def _dim_rows(entries):
    from energy_capture.stages import dim

    out = []
    for source, device_id, channel_id, label, is_primary in entries:
        out.append(
            {
                "source": source,
                "device_id": device_id,
                "channel_id": channel_id,
                "label": label,
                "short_label": label,
                "panel": None,
                "slots": None,
                "category": None,
                "room": None,
                "priority": None,
                "estimated_watts": None,
                "blackstart_device_id": None,
                "is_primary": is_primary,
                "updated_at": datetime(2026, 8, 22, 4, tzinfo=model.UTC)
                if hasattr(model, "UTC")
                else datetime(2026, 8, 22, 4),
            }
        )
    return out, dim.DIM_SCHEMA


@pytest.fixture
def con():
    c = duckdb.connect(config={"threads": 1})
    c.execute("SET TimeZone='UTC'")
    yield c
    c.close()


@pytest.fixture
def archive(tmp_path):
    """A tiny local archive: two hubs, both with a ``ct_1_a`` and a ``breaker_p1``."""
    from energy_capture.stages import dim

    feeds = [(HUB_A, "ct_1_a"), (HUB_A, "ct_1_b"), (HUB_B, "ct_1_a"), (HUB_B, "ct_1_b")]
    branches = [(HUB_A, "breaker_p1"), (HUB_B, "breaker_p1")]
    legs = [(HUB_A, "panel_leg_a")]
    rows = []
    for day in (date(2026, 8, 18), date(2026, 8, 19)):
        rows += _hourly_rows(day, feeds, watts=1000.0)
        rows += _hourly_rows(day, branches, watts=250.0)
    # A partial day: 6 of 24 hours. This is the shape that broke the first
    # coverage formula.
    rows += _hourly_rows(date(2026, 8, 20), feeds, hours=6, watts=1000.0)
    hourly = _write(tmp_path / "hourly.parquet", rows, model.HOURLY_SCHEMA)

    dim_rows, dim_schema = _dim_rows([
        (model.SOURCE_LEVITON, HUB_A, "ct_1_a", "Panel A feed A", False),
        (model.SOURCE_LEVITON, HUB_A, "ct_1_b", "Panel A feed B", False),
        (model.SOURCE_LEVITON, HUB_B, "ct_1_a", "Panel B feed A", False),
        (model.SOURCE_LEVITON, HUB_B, "ct_1_b", "Panel B feed B", False),
        (model.SOURCE_LEVITON, HUB_A, "breaker_p1", "Panel A circuit 1", False),
        # HUB_B's breaker_p1 is deliberately NOT mapped, so the unmapped path is
        # exercised and the two same-named channels must stay distinguishable.
        (model.SOURCE_LGE, HOUSE, "electric_main", "House meter", True),
        (model.SOURCE_LGE, BARN, "electric_main", "Barn meter", False),
    ])
    dim_path = _write(tmp_path / "dim.parquet", dim_rows, dim_schema)

    meter_rows = []
    for day in (date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)):
        local = datetime.combine(day, datetime.min.time())
        for device, per in ((HOUSE, 1.0), (BARN, 0.5)):
            # The SAME energy published twice: 96 x 900s and 24 x 3600s.
            for i in range(96):
                meter_rows.append({
                    "ts_utc": local + timedelta(minutes=15 * i, hours=4),
                    "ts_local": local + timedelta(minutes=15 * i),
                    "source": model.SOURCE_LGE, "device_id": device,
                    "channel_id": "electric_main", "metric": "kwh_interval",
                    "value": per, "unit": "kWh", "interval_s": 900,
                })
            for i in range(24):
                meter_rows.append({
                    "ts_utc": local + timedelta(hours=i + 4),
                    "ts_local": local + timedelta(hours=i),
                    "source": model.SOURCE_LGE, "device_id": device,
                    "channel_id": "electric_main", "metric": "kwh_interval",
                    "value": per * 4, "unit": "kWh", "interval_s": 3600,
                })
    meter = _write(tmp_path / "meter.parquet", meter_rows, model.METER_SCHEMA)

    daily_rows = []
    for month, comp, value in (
        (1, "hpheat", 40.0), (1, "eheat", 20.0), (7, "cooling", 30.0), (7, "reheat", 0.0)
    ):
        local = datetime(2026, month, 5)
        daily_rows.append({
            "ts_utc": local + timedelta(hours=4), "ts_local": local,
            "source": model.SOURCE_BRYANT, "device_id": SERIAL, "channel_id": comp,
            "metric": "kwh_day", "value": value, "unit": "kWh",
        })
    daily = _write(tmp_path / "daily.parquet", daily_rows, model.DAILY_SCHEMA)

    return historyview.Sources(
        hourly=hourly, daily=daily, meter=meter, dim=dim_path, bucket="test-bucket"
    )


RANGE = historyview.HistoryRange(start=date(2026, 8, 18), end=date(2026, 8, 20))


# ------------------------------------------------------------------- range


def test_a_preset_counts_back_inclusively_from_today() -> None:
    rng = historyview.parse_range({"days": "7"}, today=date(2026, 8, 23))
    assert (rng.start, rng.end, rng.days) == (date(2026, 8, 17), date(2026, 8, 23), 7)


def test_an_explicit_range_wins_over_the_default() -> None:
    rng = historyview.parse_range({"start": "2026-01-01", "end": "2026-03-31"})
    assert rng.days == 90 and rng.preset is None


@pytest.mark.parametrize(
    "params",
    [
        {"days": "0"}, {"days": "-3"}, {"days": "nope"}, {"days": "9999"},
        {"start": "2026-01-01"}, {"end": "2026-01-01"},
        {"start": "01/01/2026", "end": "2026-01-02"},
        {"start": "2026-03-31", "end": "2026-01-01"},
    ],
)
def test_a_bad_range_is_an_error_never_a_silent_default(params) -> None:
    """A chart that quietly answers a different question than the URL asked is
    worse than an error, because the reader cannot tell."""
    with pytest.raises(historyview.RangeError):
        historyview.parse_range(params)


def test_expected_hours_follows_dst_not_a_flat_24() -> None:
    """23 on spring-forward, 25 on fall-back. This is the coverage denominator,
    so a flat 24 would mis-state two days a year."""
    rng = historyview.HistoryRange(start=date(2026, 1, 1), end=date(2026, 12, 31))
    assert rng.expected_hours(date(2026, 3, 8)) == 23
    assert rng.expected_hours(date(2026, 11, 1)) == 25
    assert rng.expected_hours(date(2026, 8, 20)) == 24
    assert rng.expected_samples(date(2026, 3, 8)) == 23 * 120


# ---------------------------------------------------------------- circuits


def test_the_two_hubs_sharing_a_channel_id_stay_two_series(con, archive) -> None:
    """The trap this module exists for: count(DISTINCT channel_id) says 4 where
    the truth is 7, because both panels have a ct_1_a and a breaker_p1."""
    block = historyview.circuits_block(con, archive, RANGE)
    keys = {r["series_key"] for r in block["rows"]}
    assert len(keys) == 6, keys
    assert f"leviton/{HUB_A}/ct_1_a" in keys
    assert f"leviton/{HUB_B}/ct_1_a" in keys
    # The naive grouping everyone reaches for first:
    assert len({r["channel_id"] for r in block["rows"]}) == 3


def test_levels_are_reported_separately_and_never_added(con, archive) -> None:
    """A branch breaker's watts are ALSO inside its panel's feed CT, so a total
    across levels double counts. The feed level is the house total."""
    block = historyview.circuits_block(con, archive, RANGE)
    by_level = {lvl["level"]: lvl for lvl in block["levels"]}
    assert set(by_level) == {"feed", "branch"}
    feed, branch = by_level["feed"]["kwh"], by_level["branch"]["kwh"]
    assert feed > branch > 0
    # No key anywhere in the document offers a single cross-level total.
    assert "kwh_total" not in block and "total_kwh" not in block


def test_an_unmapped_channel_keeps_its_identity_and_its_device(con, archive) -> None:
    """breaker_p0 exists on both hubs (#171). Labelling an unmapped channel by
    channel_id alone puts two different circuits on screen under one name."""
    block = historyview.circuits_block(con, archive, RANGE)
    unmapped = [r for r in block["rows"] if not r["mapped"]]
    assert len(unmapped) == 1
    label = unmapped[0]["label"]
    assert "breaker_p1" in label and "1D48" in label and "unmapped" in label
    # And it is still present — an INNER JOIN would have dropped real recorded data.
    assert unmapped[0]["kwh"] > 0


def test_panel_legs_have_no_energy_so_they_are_not_a_consumer(con, archive) -> None:
    """panel_leg_* reports only hz and volts. The fixture writes no watts row for
    it, so it must simply be absent rather than appear as a zero consumer."""
    block = historyview.circuits_block(con, archive, RANGE)
    assert not [r for r in block["rows"] if r["channel_id"].startswith("panel_leg")]


# ------------------------------------------------------------------- meter


def test_the_meter_delta_is_withheld_on_an_incomplete_day(con, archive) -> None:
    """2026-08-20 has 6 of 24 hours. Comparing that against a full meter day
    manufactures a -75% disagreement that is not real (DEVIATIONS #173b)."""
    block = historyview.meter_block(con, archive, RANGE)
    by_day = {r["local_day"]: r for r in block["rows"]}

    assert by_day["2026-08-18"]["complete"] is True
    assert by_day["2026-08-18"]["delta_pct"] is not None

    partial = by_day["2026-08-20"]
    assert partial["complete"] is False
    assert partial["delta_pct"] is None, "an incomplete day must not report a delta"
    # 6 of 24 hours, measured against the EXPECTED hours of the day, not present.
    assert partial["coverage_pct"] == pytest.approx(25.0, abs=0.1)


def test_only_one_interval_series_is_counted(con, archive) -> None:
    """Each meter publishes the same energy as 900s AND 3600s. The fixture writes
    96 x 1.0 and 24 x 4.0 per day — both 96 kWh. Summing both gives 192."""
    block = historyview.meter_block(con, archive, RANGE)
    day = next(r for r in block["rows"] if r["local_day"] == "2026-08-18")
    assert block["interval_s"] == 900
    assert day["meter_kwh"] == pytest.approx(96.0)
    assert day["intervals"] == 96


def test_the_barn_meter_is_never_included(con, archive) -> None:
    """Two meters, two services. The house is found through is_primary."""
    block = historyview.meter_block(con, archive, RANGE)
    day = next(r for r in block["rows"] if r["local_day"] == "2026-08-18")
    # House alone is 96; house + barn would be 144.
    assert day["meter_kwh"] == pytest.approx(96.0)


def test_every_feed_series_must_report_before_a_delta_is_shown(con, archive, tmp_path) -> None:
    """Three of four feeds at full coverage is still not comparable: the missing
    one's load is simply absent from the panel total."""
    rows = _hourly_rows(date(2026, 8, 21),
                        [(HUB_A, "ct_1_a"), (HUB_A, "ct_1_b"), (HUB_B, "ct_1_a")],
                        watts=1000.0)
    partial = _write(tmp_path / "three-feeds.parquet", rows, model.HOURLY_SCHEMA)
    src = historyview.Sources(hourly=partial, daily=archive.daily, meter=archive.meter,
                              dim=archive.dim, bucket="test-bucket")
    rng = historyview.HistoryRange(start=date(2026, 8, 21), end=date(2026, 8, 21))
    block = historyview.meter_block(con, src, rng)
    day = block["rows"][0]
    assert day["series_seen"] == 3
    assert day["coverage_pct"] == pytest.approx(100.0, abs=0.1), "each present feed is full"
    assert day["complete"] is False and day["delta_pct"] is None


# -------------------------------------------------------------------- hvac


def test_components_that_never_moved_get_no_colour_slot(con, archive) -> None:
    """Four of eight categorical hues spent on always-zero components would say
    nothing — but the zeros are still reported, because the API sent them."""
    rng = historyview.HistoryRange(start=date(2026, 1, 1), end=date(2026, 12, 31))
    block = historyview.hvac_block(con, archive, rng)
    assert block["components"] == ["hpheat", "eheat", "cooling"] or set(
        block["components"]
    ) == {"hpheat", "eheat", "cooling"}
    assert block["components_always_zero"] == ["reheat"]
    assert block["totals"]["reheat"] == 0.0
    assert block["months"] == ["2026-01", "2026-07"]


# ---------------------------------------------------------------- coverage


def test_coverage_is_measured_against_expected_hours(con, archive) -> None:
    block = historyview.coverage_block(con, archive, RANGE)
    cells = {(c["series_key"], c["local_day"]): c for c in block["cells"]}
    full = cells[(f"leviton/{HUB_A}/ct_1_a", "2026-08-18")]
    assert full["coverage_pct"] == pytest.approx(100.0)
    assert full["hours_expected"] == 24
    partial = cells[(f"leviton/{HUB_A}/ct_1_a", "2026-08-20")]
    assert partial["hours_present"] == 6
    assert partial["coverage_pct"] == pytest.approx(25.0, abs=0.1)


def test_a_day_with_no_rows_has_no_cell_rather_than_a_zero(con, archive) -> None:
    """Cardinal rule 1 in the UI: absence is absence, never a zero."""
    rng = historyview.HistoryRange(start=date(2026, 8, 18), end=date(2026, 8, 22))
    block = historyview.coverage_block(con, archive, rng)
    days_present = {c["local_day"] for c in block["cells"]}
    assert "2026-08-21" not in days_present and "2026-08-22" not in days_present
    assert "2026-08-21" in block["days"], "the day is still on the axis"


# ------------------------------------------------------------------- route


def test_a_bad_range_is_a_400_from_the_route() -> None:
    status, doc = dashboard.handle_ui_history(target="/ui/history/data?days=nope")
    assert status == 400
    assert "days" in doc["error"]


def test_no_bucket_is_a_clear_message_not_a_traceback(monkeypatch) -> None:
    """An unconfigured deployment must still render the page, saying why it is
    empty — the live spool views are unaffected by a missing bucket."""
    from energy_capture.config import Settings

    historyview.reset_cache()
    settings = Settings(_env_file=None, s3_bucket="")
    doc = historyview.build_document(RANGE, settings=settings, use_cache=False)
    assert doc["source"] is None
    assert any("S3_BUCKET" in e for e in doc["errors"])
    assert doc["range"]["days"] == 3


def test_the_page_is_served_and_cached() -> None:
    dashboard.reset_page_cache()
    first = dashboard.render_history_page()
    assert "Energy history" in first
    assert first is dashboard.render_history_page()
    assert dashboard.UI_HISTORY_PAGE_PATH in dashboard.UI_PATHS
    assert dashboard.UI_HISTORY_DATA_PATH in dashboard.UI_PATHS


def test_the_page_ships_no_external_reference() -> None:
    """Self-contained: no CDN, no build step. Same rule as the other two pages."""
    page = dashboard.render_history_page()
    # The SVG namespace URI is an identifier, not a fetch, so it is exempt.
    body = page.replace("http://www.w3.org/2000/svg", "")
    for bad in ("http://", "https://", "//cdn", "<script src", "<link rel=\"stylesheet\""):
        assert bad not in body, f"{bad!r} must not appear in a self-contained page"


def test_the_page_declares_both_theme_scopes() -> None:
    """Dark mode is selected, not an automatic flip, and the toggle must beat the
    OS setting in both directions."""
    page = dashboard.render_history_page()
    assert "prefers-color-scheme: dark" in page
    assert '[data-theme="dark"]' in page
    assert ':not([data-theme="light"])' in page
