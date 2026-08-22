"""``/ui/hvac`` — the HVAC cross-check screen (``hvacview`` + its route).

The premise of this screen is a comparison between two clouds that share no key,
so most of what is worth testing is *not* the arithmetic. It is:

* that the comparison aligns on buckets, because the two sources' ``ts_utc``
  never match and a key join silently returns nothing;
* that an ABSENT Bryant capacity reading is treated as "the compressor is off"
  and not as a gap, while an absent Leviton row IS a gap;
* that the energy figure is observed-time only, so a gap shrinks it;
* that the screen says out loud that Bryant's own kWh cannot be compared yet,
  rather than drawing an empty chart;
* that which channels are HVAC comes from ``channel_map.json``, not from code.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from energy_capture import dashboard, hvacview, model, timeutil
from energy_capture.spool.sqlite import SpoolDB
from tests.conftest import utc

HUB = "1000_0046_1D48"
OTHER_HUB = "1000_0046_1D52"
SERIAL = "4022W200213"
COMPRESSOR = "breaker_p10"
NOW = utc(2026, 8, 22, 22, 0)


@pytest.fixture(autouse=True)
def _fresh_caches() -> None:
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
    """Three HVAC channels, plus two that must NOT reach this screen."""
    path = tmp_path / "channel_map.json"
    path.write_text(
        json.dumps(
            {
                "mappings": [
                    {
                        "source": "leviton",
                        "device_id": HUB,
                        "channel_id": COMPRESSOR,
                        "label": "Heat pump outdoor unit",
                        "short_label": "Heat pump",
                        "category": "hvac",
                        "panel": "B",
                    },
                    {
                        "source": "leviton",
                        "device_id": HUB,
                        "channel_id": "ct_2_a",
                        "label": "HVAC subpanel feeder (leg A)",
                        "category": "hvac",
                    },
                    {
                        "source": "leviton",
                        "device_id": HUB,
                        "channel_id": "ct_2_b",
                        "label": "HVAC subpanel feeder (leg B)",
                        "category": "hvac",
                    },
                    # Same channel_id, different hub, NOT hvac: the kitchen MWBC.
                    # channel_id repeats across hubs, so a screen that keyed on
                    # channel_id alone would silently average the two circuits.
                    {
                        "source": "leviton",
                        "device_id": OTHER_HUB,
                        "channel_id": COMPRESSOR,
                        "label": "Kitchen counter plugs + dishwasher",
                        "category": "mwbc",
                    },
                    {
                        "source": "bryant",
                        "device_id": SERIAL,
                        "channel_id": "system",
                        "label": "Bryant Evolution system",
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
    start: datetime,
    count: int,
    every_s: int = 30,
    compressor_w: Any = 1350.0,
    capacity_pct: Any = 45.0,
    blower_rpm: Any = 520.0,
    feeder_w: Any = 0.0,
    bryant_offset_s: int = 7,
) -> None:
    """One cadence of both clouds.

    ``bryant_offset_s`` is the point: the two poll loops are independent, so
    Bryant's rows land seconds away from Leviton's and never on the same
    ``ts_utc``. Callables may be passed for any value to vary it per sample.
    """

    def at(value: Any, index: int) -> Any:
        return value(index) if callable(value) else value

    rows = []
    for i in range(count):
        lev = start + timedelta(seconds=every_s * i)
        bry = lev + timedelta(seconds=bryant_offset_s)
        watts = at(compressor_w, i)
        if watts is not None:
            rows.append(
                make_obs(
                    lev, source=model.SOURCE_LEVITON, device_id=HUB,
                    channel_id=COMPRESSOR, metric="watts", value=float(watts),
                )
            )
        feeder = at(feeder_w, i)
        if feeder is not None:
            for leg in ("ct_2_a", "ct_2_b"):
                rows.append(
                    make_obs(
                        lev, source=model.SOURCE_LEVITON, device_id=HUB,
                        channel_id=leg, metric="watts", value=float(feeder) / 2.0,
                    )
                )
        capacity = at(capacity_pct, i)
        if capacity is not None:
            rows.append(
                make_obs(
                    bry, source=model.SOURCE_BRYANT, device_id=SERIAL,
                    channel_id="system", metric="stage_pct", value=float(capacity),
                    unit="pct",
                )
            )
        rows.append(
            make_obs(
                bry, source=model.SOURCE_BRYANT, device_id=SERIAL,
                channel_id="system", metric="mode", value=2.0, unit="enum",
            )
        )
        rpm = at(blower_rpm, i)
        if rpm is not None:
            rows.append(
                make_obs(
                    bry, source=model.SOURCE_BRYANT, device_id=SERIAL,
                    channel_id="system", metric="blower_rpm", value=float(rpm),
                    unit="rpm",
                )
            )
    spool.append(rows)


def block(spool: SpoolDB, channel_map: Path, **kwargs: Any) -> dict[str, Any]:
    status, document = dashboard.handle_ui_hvac(
        None,
        kwargs.pop("target", None),
        energy_out_dir=kwargs.pop("energy_out_dir", None),
        spool_path=spool.path,
        channel_map_path=channel_map,
        inventory_path=None,
        now=kwargs.pop("now", NOW),
        **kwargs,
    )
    assert status == 200
    return document["hvac"]


# ------------------------------------------------------- what gets compared


def test_only_channels_mapped_as_hvac_reach_the_screen(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """The panel side is a mapping decision, not a hardcoded hub id.

    The other hub's ``breaker_p10`` is a kitchen MWBC. It shares the channel_id
    and it must not appear here, which is the same reason device_id belongs in
    every GROUP BY.
    """
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)
    hvac = block(spool, channel_map)

    assert [e["channel_id"] for e in hvac["equipment"]] == [COMPRESSOR]
    assert [e["device_id"] for e in hvac["equipment"]] == [HUB]
    assert [f["channel_id"] for f in hvac["feeders"]] == ["ct_2_a", "ct_2_b"]


def test_no_hvac_channel_mapped_is_explained_not_empty(
    spool: SpoolDB, tmp_path: Path, make_obs
) -> None:
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"mappings": []}), encoding="utf-8")
    hvac = block(spool, empty)
    assert hvac["present"] is False
    assert "channel_map.json" in hvac["reason"]


# ------------------------------------------------------------- the premise


def test_the_two_clouds_are_aligned_on_buckets_not_on_ts_utc(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Bryant and Leviton never share a ts_utc; a key join would find nothing.

    The seeder deliberately offsets Bryant by 7 seconds, as the real loops do.
    Every bucket must still carry both sides.
    """
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60, bryant_offset_s=11)
    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=1800")

    both = [
        b for b in hvac["buckets"]
        if b["equipment_w"] is not None and b["capacity_pct"] is not None
    ]
    assert both, "no bucket carried both clouds — the alignment is broken"
    assert len(both) >= 25
    assert "never share a ts_utc" in hvac["alignment_note"]


def test_capacity_absent_means_off_and_is_not_a_gap(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Bryant omits stage_pct while the compressor is off (rule 1: no row, not a 0).

    So an absent capacity is the cloud saying "off", and the panel should agree
    by reading ~0 W. That agreement is reported as a count of disagreements, not
    as an adjective.
    """
    start = NOW - timedelta(minutes=60)
    # First half: off — no capacity row at all, breaker at 0 W.
    seed(spool, make_obs, start=start, count=60, compressor_w=0.0, capacity_pct=None)
    # Second half: running at 60%, 30 W per point.
    seed(
        spool, make_obs, start=start + timedelta(minutes=30), count=60,
        compressor_w=1800.0, capacity_pct=60.0,
    )
    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=3600")

    off = hvac["off_state"]
    assert off["absent_buckets"] > 0 and off["present_buckets"] > 0
    assert off["absent_max_w"] == 0.0
    assert off["present_min_w"] == 1800.0
    assert off["disagreements"] == 0

    # An absent capacity bucket is still a bucket, with Bryant samples in it:
    # that is what distinguishes "off" from "the poller was down".
    absent = [b for b in hvac["buckets"] if not b["capacity_present"]]
    assert absent and all(b["bryant_samples"] > 0 for b in absent)


def test_a_disagreement_is_counted_rather_than_smoothed(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Watts with no reported capacity is exactly what this screen exists to catch."""
    seed(
        spool, make_obs, start=NOW - timedelta(minutes=30), count=60,
        compressor_w=1500.0, capacity_pct=None,
    )
    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=1800")
    assert hvac["off_state"]["disagreements"] == hvac["off_state"]["absent_buckets"] > 0


# ------------------------------------------------------------- the agreement


def test_watts_per_capacity_point_is_the_headline(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A variable-speed compressor's draw is ~proportional to reported capacity."""
    capacities = [45.0, 60.0, 75.0, 85.0]
    seed(
        spool, make_obs, start=NOW - timedelta(minutes=40), count=80,
        capacity_pct=lambda i: capacities[i % 4],
        compressor_w=lambda i: 30.0 * capacities[i % 4],
    )
    agreement = block(spool, channel_map, target="/ui/hvac/data?window_s=2400")["agreement"]

    assert agreement["n"] >= 60
    assert agreement["r"] == pytest.approx(1.0, abs=1e-6)
    assert agreement["watts_per_point"] == pytest.approx(30.0, abs=0.05)
    assert agreement["implied_full_load_w"] == 3000
    assert {b["capacity_pct"] for b in agreement["bands"]} == {45, 60, 75, 85}


def test_too_few_samples_reports_n_rather_than_a_correlation(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A young channel must not be dressed up as a long baseline."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=1), count=2)
    agreement = block(spool, channel_map, target="/ui/hvac/data?window_s=600")["agreement"]
    assert agreement["n"] <= 2
    assert agreement["r"] is None


# --------------------------------------------------------------- the feeder


def test_the_feeder_profile_measures_what_the_clamps_carry(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Fan power goes as the cube of speed; a resistive load does not.

    The clamps are on the HVAC subpanel feeder, and what actually flows through
    them is a question the data answers rather than one the label settles. This
    test builds a blower-shaped feeder and asserts the cube term wins.
    """
    speeds = [400.0, 700.0, 1000.0, 1240.0]
    seed(
        spool, make_obs, start=NOW - timedelta(minutes=40), count=80,
        blower_rpm=lambda i: speeds[i % 4],
        feeder_w=lambda i: 450.0 * (speeds[i % 4] / 1240.0) ** 3,
        capacity_pct=60.0,
        compressor_w=1800.0,
    )
    profile = block(spool, channel_map, target="/ui/hvac/data?window_s=2400")["feeder_profile"]

    assert profile["n"] > 60
    assert profile["r_vs_rpm_cubed"] == pytest.approx(1.0, abs=1e-6)
    assert profile["r_vs_rpm_cubed"] > profile["r_vs_blower_rpm"]
    # The compressor ran at a constant capacity the whole time, so there is no
    # spread to correlate against: None, never a fabricated 0.0.
    assert profile["r_vs_capacity_pct"] is None
    # 700 rpm bands to 800, not 600: round() is half-to-even, and the band is
    # a display grouping rather than a measurement.
    assert [b["blower_rpm"] for b in profile["bands"]] == [400, 800, 1000, 1200]


def test_a_feeder_leg_that_stops_reporting_does_not_zero_the_other(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Legs are averaged first and summed after, so a missing leg is not a zero."""
    start = NOW - timedelta(minutes=40)
    seed(spool, make_obs, start=start, count=40, feeder_w=400.0)
    # ...then one later cycle where only leg A reported, still inside the window.
    spool.append(
        [
            make_obs(
                NOW - timedelta(minutes=5), source=model.SOURCE_LEVITON, device_id=HUB,
                channel_id="ct_2_a", metric="watts", value=200.0,
            )
        ]
    )
    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=3600")
    lonely = [b for b in hvac["buckets"] if b["feeder_legs"].get("ct_2_b") is None
              and b["feeder_legs"].get("ct_2_a") is not None]
    assert lonely, "expected a bucket with only leg A"
    assert lonely[0]["feeder_w"] == pytest.approx(200.0)


# ---------------------------------------------------------------- the energy


def test_energy_is_observed_time_only_so_a_gap_shrinks_it(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """Cardinal rule 5. Half the samples must yield half the kWh, not the same kWh."""
    start = NOW - timedelta(minutes=60)
    seed(spool, make_obs, start=start, count=60, compressor_w=1200.0)
    full = block(spool, channel_map, target="/ui/hvac/data?window_s=3600")["energy"]

    # Same wall-clock window, half the samples: every other cycle missing.
    spool.connect().execute("DELETE FROM observations")
    seed(
        spool, make_obs, start=start, count=60, every_s=60, compressor_w=1200.0,
    )
    halved = block(spool, channel_map, target="/ui/hvac/data?window_s=3600")["energy"]

    assert full["equipment"]["kwh"] == pytest.approx(1200.0 * 1800 / 3.6e6, rel=0.02)
    assert halved["equipment"]["kwh"] == pytest.approx(full["equipment"]["kwh"], rel=0.02)
    # ...and the mean watts are unchanged, which is the point: the figure follows
    # observed time, so it never invents energy for the cycles that are missing.
    assert halved["equipment"]["mean_w"] == pytest.approx(1200.0, rel=0.01)
    assert halved["equipment"]["observed_s"] <= full["equipment"]["observed_s"]


def test_bryant_energy_says_what_to_run_when_the_dataset_is_absent(
    spool: SpoolDB, channel_map: Path, make_obs, tmp_path: Path
) -> None:
    """An empty chart would read as zero. A reason names the command to run."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)
    hvac = block(spool, channel_map, energy_out_dir=tmp_path / "no-such-dir")
    assert hvac["bryant_energy"]["available"] is False
    assert "fetch-daily" in hvac["bryant_energy"]["reason"]


def test_bryant_components_are_reported_per_day_with_the_panel_beside_them(
    spool: SpoolDB, channel_map: Path, make_obs, tmp_path: Path
) -> None:
    """The whole reason to keep reading Bryant now that the breaker exists.

    ``ct_2_a``/``ct_2_b`` are one clamp pair on the HVAC subpanel feeder, so the
    blower and the electric strips are the same conductor and cannot be split
    from the panel side. Bryant reports them separately, which is the one thing
    this dataset adds — so the payload must carry the components, not just a
    total.
    """
    from energy_capture.stages import dailystore

    day = (NOW - timedelta(days=1)).date()
    rows = []
    for channel_id, kwh in (("cooling", 12.0), ("fan", 3.0), ("eheat", 4.0)):
        ts = timeutil.local_midnight_utc(day)
        rows.append(
            model.Observation(
                ts_utc=ts,
                ts_local=timeutil.to_local_naive(ts),
                source=model.SOURCE_BRYANT,
                device_id=SERIAL,
                channel_id=channel_id,
                metric="kwh_day",
                value=kwh,
                unit="kWh",
            )
        )
    out_dir = tmp_path / "daily"
    destination = dailystore.MonthDestination(
        dailystore.month_start_of(day), out_dir=out_dir, bucket=None
    )
    dailystore.write_month_table(dailystore.build_month_table(rows), destination)

    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)
    energy = block(spool, channel_map, energy_out_dir=out_dir)["bryant_energy"]

    assert energy["available"] is True
    assert energy["totals"] == {"cooling": 12.0, "fan": 3.0, "eheat": 4.0}
    (entry,) = [d for d in energy["days"] if d["local_day"] == day.isoformat()]
    assert entry["components"]["eheat"] == 4.0
    assert entry["components"]["looppump"] is None, "a disabled component is absent, not 0"
    assert entry["bryant_total_kwh"] == 19.0

    # The split is only meaningful if the page can say which side of the panel
    # each component lands on.
    assert energy["panel_side"]["eheat"] == "feeder"
    assert energy["panel_side"]["fan"] == "feeder"
    assert energy["panel_side"]["cooling"] == "equipment"


def test_a_partly_covered_day_is_not_offered_as_a_comparison(
    spool: SpoolDB, channel_map: Path, make_obs, tmp_path: Path
) -> None:
    """A delta over 30 minutes of samples measures coverage, not agreement."""
    from energy_capture.stages import dailystore

    day = NOW.date()
    ts = timeutil.local_midnight_utc(day)
    out_dir = tmp_path / "daily"
    dailystore.write_month_table(
        dailystore.build_month_table(
            [
                model.Observation(
                    ts_utc=ts,
                    ts_local=timeutil.to_local_naive(ts),
                    source=model.SOURCE_BRYANT,
                    device_id=SERIAL,
                    channel_id="cooling",
                    metric="kwh_day",
                    value=20.0,
                    unit="kWh",
                )
            ]
        ),
        dailystore.MonthDestination(
            dailystore.month_start_of(day), out_dir=out_dir, bucket=None
        ),
    )
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)
    energy = block(spool, channel_map, energy_out_dir=out_dir)["bryant_energy"]

    (entry,) = [d for d in energy["days"] if d["local_day"] == day.isoformat()]
    assert entry["panel"] is not None and entry["panel"]["coverage_pct"] < 95.0
    assert entry["delta_kwh"] is None
    assert "coverage" in entry["delta_reason"]


def test_a_day_whose_compressor_channel_did_not_exist_is_not_a_disagreement(
    spool: SpoolDB, channel_map: Path, make_obs, tmp_path: Path
) -> None:
    """The mistake this gate exists to prevent, from real data.

    On 2026-08-18..21 the feeder covered 100% of every day while the compressor
    breaker had not been installed yet. A single blended coverage number called
    those days fully covered, so the screen reported a -99% "disagreement" that
    was really a missing channel. Coverage is per group, and every mapped channel
    must have reported.
    """
    from energy_capture.stages import dailystore

    day = (NOW - timedelta(days=1)).date()
    ts = timeutil.local_midnight_utc(day)
    out_dir = tmp_path / "daily"
    dailystore.write_month_table(
        dailystore.build_month_table(
            [
                model.Observation(
                    ts_utc=ts,
                    ts_local=timeutil.to_local_naive(ts),
                    source=model.SOURCE_BRYANT,
                    device_id=SERIAL,
                    channel_id="cooling",
                    metric="kwh_day",
                    value=21.0,
                    unit="kWh",
                )
            ]
        ),
        dailystore.MonthDestination(
            dailystore.month_start_of(day), out_dir=out_dir, bucket=None
        ),
    )

    # A full day of the FEEDER only — no compressor rows at all, exactly as
    # before the breaker went in.
    start = timeutil.local_midnight_utc(day)
    seed(
        spool, make_obs, start=start, count=2880, every_s=30,
        compressor_w=None, feeder_w=20.0, capacity_pct=None, blower_rpm=None,
    )
    # A window wide enough to include yesterday, or the payload short-circuits
    # on "no rows in this window" before it ever reads the day-grain dataset.
    energy = block(
        spool,
        channel_map,
        energy_out_dir=out_dir,
        target="/ui/hvac/data?window_s=172800",
    )["bryant_energy"]

    (entry,) = [d for d in energy["days"] if d["local_day"] == day.isoformat()]
    assert entry["panel"]["feeder_coverage_pct"] == pytest.approx(100.0, abs=0.5)
    assert entry["panel"]["equipment_coverage_pct"] == 0.0
    assert entry["panel"]["all_channels_present"] is False
    assert entry["delta_pct"] is None, "a missing channel is not a disagreement"
    assert "did not exist yet" in entry["delta_reason"]


# ----------------------------------------------------------------- the route


def test_the_window_is_clamped_and_a_bad_one_never_400s(
    spool: SpoolDB, channel_map: Path, make_obs
) -> None:
    """A diagnostic screen answers with its default rather than an error page."""
    seed(spool, make_obs, start=NOW - timedelta(minutes=30), count=60)

    assert hvacview.parse_window_s(None) == (hvacview.DEFAULT_WINDOW_S, False)
    assert hvacview.parse_window_s("not-a-number") == (hvacview.DEFAULT_WINDOW_S, False)
    assert hvacview.parse_window_s("1") == (hvacview.MIN_WINDOW_S, True)
    assert hvacview.parse_window_s(str(10**9)) == (hvacview.MAX_WINDOW_S, True)

    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=banana")
    assert hvac["window"]["window_s"] == hvacview.DEFAULT_WINDOW_S
    hvac = block(spool, channel_map, target="/ui/hvac/data?window_s=1")
    assert hvac["window"]["window_s"] == hvacview.MIN_WINDOW_S
    assert hvac["window"]["clamped"] is True


def test_the_bucket_width_keeps_the_payload_pollable() -> None:
    """A week-long window must not answer with 20,000 buckets."""
    for window_s in hvacview.WINDOW_PRESETS_S:
        bucket_s = hvacview.bucket_width_s(window_s)
        assert window_s / bucket_s <= hvacview.MAX_BUCKETS
        assert bucket_s >= 30, "never narrower than the poll interval"


def test_an_unreadable_spool_is_a_message_not_an_exception(
    tmp_path: Path, channel_map: Path
) -> None:
    status, document = dashboard.handle_ui_hvac(
        None,
        None,
        spool_path=tmp_path / "does-not-exist.db",
        channel_map_path=channel_map,
        inventory_path=None,
        now=NOW,
    )
    assert status == 200
    assert document["hvac"]["present"] is False
    assert document["errors"]


def test_the_page_and_its_data_are_both_routed(spool: SpoolDB, channel_map: Path) -> None:
    """``/ui/hvac`` and ``/ui/hvac/data`` are the module's own routes."""
    assert dashboard.UI_HVAC_PAGE_PATH == "/ui/hvac"
    assert dashboard.UI_HVAC_DATA_PATH == "/ui/hvac/data"
    assert {dashboard.UI_HVAC_PAGE_PATH, dashboard.UI_HVAC_DATA_PATH} <= dashboard.UI_PATHS

    page = dashboard.render_hvac_page()
    assert page.startswith("<!doctype html>")
    # The page must carry no network dependency: it renders in a house, offline.
    for forbidden in ("http://", "https://", "cdn", "<script src"):
        assert forbidden not in page.lower(), forbidden
    # ...and it must link back to the dashboard it hangs off.
    assert 'href="/ui"' in page
