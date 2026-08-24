"""``check-channels`` is only worth having if it does not cry wolf.

The fault this exists to catch (DEVIATIONS #180) survived six days *because* the
signals that would have shown it were drowned in things that look wrong and are
not. So the tests that matter most here are the ones where the checker is handed
something that LOOKS broken and must stay silent: a water heater idle at exactly
0 W, a porch light holding 21 W, a partial first day of collection, a meter day
the utility has only half published.

Three of these tests are regressions on false positives this module actually
produced against real data before they were fixed, and they are marked as such.

Synthetic Parquet against an in-memory DuckDB; no S3, no network.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from energy_capture import model
from energy_capture.stages import digest, integrity

HUB_B = "1000_0046_1D48"
HUB_A = "1000_0046_1D52"
HOUSE = "1308468"
DAY = date(2026, 8, 23)


@pytest.fixture
def con():
    c = duckdb.connect(config={"threads": 1})
    c.execute("SET TimeZone='UTC'")
    yield c
    c.close()


def hour_row(
    day: date,
    hour: int,
    channel: str,
    *,
    lo: float,
    hi: float,
    mean: float | None = None,
    device: str = HUB_B,
    samples: int = 120,
    observed: int = 3600,
):
    local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
    utc = local + timedelta(hours=4)
    value = mean if mean is not None else (lo + hi) / 2
    return {
        "hour_start_utc": utc,
        "local_hour_start": local,
        "source": model.SOURCE_LEVITON,
        "device_id": device,
        "channel_id": channel,
        "metric": "watts",
        "unit": "W",
        "mean": value, "min": lo, "max": hi, "p95": hi,
        "sample_count": samples,
        "first_ts_utc": utc, "last_ts_utc": utc,
        "kwh": value * observed / 3.6e6,
        "observed_seconds": observed,
    }


def live_day(day: date, channel: str, *, device: str = HUB_B, watts: float = 500.0):
    """A full day of healthy, varying hours — min < max every hour."""
    return [
        hour_row(day, h, channel, lo=watts - 50, hi=watts + 50, device=device)
        for h in range(24)
    ]


def write(tmp_path, name, rows, schema):
    path = tmp_path / name
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return str(path)


def report(con, tmp_path, hourly, meter_rows=None, **kw):
    h = write(tmp_path, "rollup-x.parquet", hourly, model.HOURLY_SCHEMA)
    m = write(tmp_path, "lge.parquet", meter_rows or [], model.METER_SCHEMA)
    kw.setdefault("start", DAY)
    kw.setdefault("end", DAY)
    return integrity.build_report(con, hourly=h, meter=m, **kw)


def rules(rep) -> list[str]:
    return [f.rule for f in rep.findings]


# ------------------------------------------------------------------ the freeze


def test_two_consecutive_frozen_hours_is_a_finding(con, tmp_path) -> None:
    rows = live_day(DAY, "ct_1_a")
    for h in (5, 6):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    rep = report(con, tmp_path, rows)
    assert rules(rep) == ["frozen_channel"]
    assert "489.30 W for 2 consecutive hours" in rep.findings[0].headline
    assert (model.SOURCE_LEVITON, HUB_B, "ct_1_a") in rep.untrusted


def test_one_frozen_hour_is_normal_and_stays_quiet(con, tmp_path) -> None:
    """The healthy hub did exactly this twice in eight days: a real steady load."""
    rows = live_day(DAY, "ct_1_a")
    rows[4] = hour_row(DAY, 4, "ct_1_a", lo=531.17, hi=531.17, mean=531.17)
    assert report(con, tmp_path, rows).ok


def test_two_non_consecutive_frozen_hours_stay_quiet(con, tmp_path) -> None:
    rows = live_day(DAY, "ct_1_a")
    for h in (4, 9):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=531.17, hi=531.17, mean=531.17)
    assert report(con, tmp_path, rows).ok


def test_an_unwatched_hour_breaks_a_run_rather_than_bridging_it(con, tmp_path) -> None:
    """Frozen either side of an hour nobody watched is not evidence of three."""
    rows = live_day(DAY, "ct_1_a")
    rows[5] = hour_row(DAY, 5, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    rows[6] = hour_row(DAY, 6, "ct_1_a", lo=489.3, hi=489.3, mean=489.3, samples=40)
    rows[7] = hour_row(DAY, 7, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    assert report(con, tmp_path, rows).ok


def test_a_channel_with_no_watched_hour_is_named_not_passed(con, tmp_path) -> None:
    rows = [
        hour_row(DAY, h, "ct_1_a", lo=489.3, hi=489.3, mean=489.3, samples=10)
        for h in range(24)
    ]
    rep = report(con, tmp_path, rows)
    assert rep.ok
    assert rep.skipped_low_samples, "an unwatched channel must be named, never passed"
    assert rep.channels_checked == 0


def test_pinned_at_exactly_zero_is_off_not_frozen(con, tmp_path) -> None:
    """REGRESSION. The first false positive against real data.

    ``breaker_p19`` is the water heater. Idle for two hours it reports 0.0 W
    unchanging, and that is the truth — the healthy hub had 340 such hours. The
    stuck-at-zero failure mode belongs to ``feed_below_children``, which catches
    it without having to guess whether a zero is honest.
    """
    rows = live_day(DAY, "ct_1_a")
    for h in (5, 6, 7, 8):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=0.0, hi=0.0, mean=0.0)
    assert report(con, tmp_path, rows).ok


def test_breaker_channels_are_out_of_scope_for_the_freeze(con, tmp_path) -> None:
    """REGRESSION. Breakers report INTEGER watts.

    A porch light at 21 W or an idling mini-split pins to the same integer for
    hours as a matter of course; the healthy hub did it on ``breaker_p23``,
    ``p17``, ``p18`` and ``p6``. A CT repeating an exact float is a different
    claim. A breaker that really sticks surfaces through feed_below_children.
    """
    rows = [
        hour_row(DAY, h, "breaker_p6", lo=21.0, hi=21.0, mean=21.0) for h in range(24)
    ]
    assert report(con, tmp_path, rows).ok


def test_the_real_20260818_shape_fires(con, tmp_path) -> None:
    """CALIBRATION PIN: the faulty hub, three consecutive hours, day two.

    Changing INTEGRITY_FROZEN_MIN_HOURS must break this test. The whole
    threshold rests on these two shapes and the one below.
    """
    rows = live_day(date(2026, 8, 18), "ct_1_a")
    for h in (13, 14, 15):
        rows[h] = hour_row(date(2026, 8, 18), h, "ct_1_a", lo=526.19, hi=526.19, mean=526.19)
    rep = report(con, tmp_path, rows, start=date(2026, 8, 18), end=date(2026, 8, 18))
    assert rules(rep) == ["frozen_channel"]
    assert "3 consecutive hours" in rep.findings[0].headline


def test_the_real_healthy_hub_shape_does_not(con, tmp_path) -> None:
    """CALIBRATION PIN: both legs pinned for ONE hour at 04:00, and that is fine."""
    rows = live_day(date(2026, 8, 24), "ct_1_a", device=HUB_A)
    rows += live_day(date(2026, 8, 24), "ct_1_b", device=HUB_A)
    rows[4] = hour_row(date(2026, 8, 24), 4, "ct_1_a", lo=531.17, hi=531.17,
                       mean=531.17, device=HUB_A)
    rows[28] = hour_row(date(2026, 8, 24), 4, "ct_1_b", lo=351.83, hi=351.83,
                        mean=351.83, device=HUB_A)
    rep = report(con, tmp_path, rows, start=date(2026, 8, 24), end=date(2026, 8, 24))
    assert rep.ok, rules(rep)


# ----------------------------------------------------------- feed vs children


def feed_and_kids(day: date, hour: int, *, feed: float, kids: dict[str, float]):
    rows = [
        hour_row(day, hour, "ct_1_a", lo=feed / 2 - 1, hi=feed / 2 + 1, mean=feed / 2),
        hour_row(day, hour, "ct_1_b", lo=feed / 2 - 1, hi=feed / 2 + 1, mean=feed / 2),
    ]
    for channel, watts in kids.items():
        rows.append(
            hour_row(day, hour, channel, lo=watts - 1, hi=watts + 1, mean=watts)
        )
    return rows


def test_children_outdrawing_the_feed_is_impossible_and_reported(con, tmp_path) -> None:
    rows = feed_and_kids(
        DAY, 3, feed=1000.0,
        kids={"breaker_p1": 700.0, "breaker_p10": 700.0, "breaker_p14": 200.0},
    )
    rep = report(con, tmp_path, rows)
    assert "feed_below_children" in rules(rep)


def test_an_excess_inside_tolerance_stays_quiet(con, tmp_path) -> None:
    """Bare "any excess" fires 18 times on the HEALTHY hub. Tolerance is the point."""
    rows = feed_and_kids(
        DAY, 3, feed=1000.0,
        kids={"breaker_p1": 400.0, "breaker_p10": 400.0, "breaker_p14": 249.0},
    )
    assert report(con, tmp_path, rows).ok


def test_one_watt_past_tolerance_does_fire(con, tmp_path) -> None:
    inside = feed_and_kids(
        DAY, 3, feed=1000.0,
        kids={"breaker_p1": 400.0, "breaker_p10": 400.0, "breaker_p14": 350.0},
    )
    assert "feed_below_children" in rules(report(con, tmp_path, inside))


def test_a_barely_metered_panel_is_unjudgeable_not_clean(con, tmp_path) -> None:
    rows = feed_and_kids(DAY, 3, feed=100.0, kids={"breaker_p1": 900.0})
    rep = report(con, tmp_path, rows)
    assert rep.ok
    assert rep.skipped_unjudgeable, "too few metered circuits must be named"


def test_the_subpanel_ct_pair_counts_as_a_child(con, tmp_path) -> None:
    """``ct_2`` is downstream of the service feed, not a sibling of it."""
    rows = feed_and_kids(
        DAY, 3, feed=500.0,
        kids={"ct_2_a": 600.0, "ct_2_b": 600.0, "breaker_p1": 10.0},
    )
    assert "feed_below_children" in rules(report(con, tmp_path, rows))


# ------------------------------------------------------------------- the meter


def meter_rows(day: date, *, kwh_per_hour: float, hours: int = 24, interval_s: int = 3600):
    rows = []
    for hour in range(hours):
        local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
        rows.append({
            "ts_utc": local + timedelta(hours=4),
            "ts_local": local,
            "source": model.SOURCE_LGE,
            "device_id": HOUSE,
            "channel_id": "meter",
            "metric": "kwh_interval",
            "value": kwh_per_hour,
            "unit": "kWh",
            "interval_s": interval_s,
        })
    return rows


def panel_day(day: date, *, watts: float, observed: int = 3600, hours: int = 24):
    rows = []
    for h in range(hours):
        for leg in ("ct_1_a", "ct_1_b"):
            rows.append(
                hour_row(day, h, leg, lo=watts - 10, hi=watts + 10, mean=watts,
                         observed=observed)
            )
    return rows


def test_a_few_percent_of_disagreement_is_normal(con, tmp_path) -> None:
    """Measured clean days run -3.1% to -3.4%. That must never alarm."""
    rows = panel_day(DAY, watts=1000.0)          # 2 legs -> 48 kWh
    rep = report(con, tmp_path, rows,
                 meter_rows=meter_rows(DAY, kwh_per_hour=48.0 / 24 / 0.966),
                 meter_device=HOUSE)
    assert "meter_disagreement" not in rules(rep), rep.findings and rep.findings[0].headline


def test_a_large_disagreement_is_reported(con, tmp_path) -> None:
    rows = panel_day(DAY, watts=1000.0)          # 48 kWh
    rep = report(con, tmp_path, rows,
                 meter_rows=meter_rows(DAY, kwh_per_hour=60.0 / 24),
                 meter_device=HOUSE)
    assert "meter_disagreement" in rules(rep)


def test_a_partial_panel_day_is_skipped_not_called_a_fault(con, tmp_path) -> None:
    """REGRESSION. observed_seconds is summed over every feed LEG.

    A fully watched day reports ~series * seconds_in_the_day, so the first
    version of the gate could never fire and reported the partial first day of
    collection (2026-08-17, watched from 15:22 local) as a -60.4% instrument
    fault. It was a coverage gap.
    """
    rows = panel_day(DAY, watts=1000.0, hours=9)
    rep = report(con, tmp_path, rows,
                 meter_rows=meter_rows(DAY, kwh_per_hour=48.0 / 24),
                 meter_device=HOUSE)
    assert "meter_disagreement" not in rules(rep)
    assert any("watched" in s for s in rep.skipped_low_samples)


def test_a_half_published_meter_day_is_skipped_too(con, tmp_path) -> None:
    """REGRESSION. Green Button publishes on the UTILITY's lag, not ours.

    2026-08-23 reported the panels +59.3% ABOVE the meter, which was a
    half-published meter day beside a complete panel day.
    """
    rows = panel_day(DAY, watts=1000.0)
    rep = report(con, tmp_path, rows,
                 meter_rows=meter_rows(DAY, kwh_per_hour=48.0 / 24, hours=16),
                 meter_device=HOUSE)
    assert "meter_disagreement" not in rules(rep)
    assert any("intervals" in s for s in rep.skipped_low_samples)


def test_no_meter_overlap_is_skipped_and_named(con, tmp_path) -> None:
    rep = report(con, tmp_path, panel_day(DAY, watts=1000.0), meter_device=HOUSE)
    assert rep.ok
    assert any("no overlap" in s for s in rep.skipped_unjudgeable)


def test_two_interval_series_are_never_summed(con, tmp_path) -> None:
    """#169: every UsagePoint publishes the same energy at 900s AND 3600s."""
    both = meter_rows(DAY, kwh_per_hour=2.0) + meter_rows(
        DAY, kwh_per_hour=0.5, interval_s=900
    )
    rows = panel_day(DAY, watts=2000.0)          # 96 kWh, vs 48 kWh metered
    rep = report(con, tmp_path, rows, meter_rows=both, meter_device=HOUSE)
    found = [f for f in rep.findings if f.rule == "meter_disagreement"]
    assert found and "48.0 kWh" in found[0].headline, "must read ONE series only"


def test_an_unknown_primary_meter_is_a_note_not_a_pass(con, tmp_path) -> None:
    rep = report(con, tmp_path, panel_day(DAY, watts=1000.0),
                 meter_rows=meter_rows(DAY, kwh_per_hour=5.0), meter_device=None)
    assert rep.ok
    assert any("is_primary" in n for n in rep.notes)


def test_no_meter_at_all_says_the_scale_check_did_not_run(con, tmp_path) -> None:
    h = write(tmp_path, "rollup-x.parquet", panel_day(DAY, watts=1000.0),
              model.HOURLY_SCHEMA)
    rep = integrity.build_report(con, hourly=h, meter=None, start=DAY, end=DAY)
    assert any("scaled wrong" in n for n in rep.notes)


# ---------------------------------------------------------------- the negative


def test_a_negative_reading_is_reported(con, tmp_path) -> None:
    rows = live_day(DAY, "ct_1_a")
    rows[3] = hour_row(DAY, 3, "ct_1_a", lo=-120.0, hi=40.0, mean=-5.0)
    rep = report(con, tmp_path, rows)
    assert "negative_reading" in rules(rep)


# ------------------------------------------------------------------- ordering


def test_instrument_findings_sort_before_everything_else(con, tmp_path) -> None:
    rows = live_day(DAY, "ct_1_a")
    for h in (5, 6):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    rows[3] = hour_row(DAY, 3, "ct_1_a", lo=-10.0, hi=40.0, mean=-1.0)
    rep = report(con, tmp_path, rows)
    assert rules(rep)[0] == "negative_reading"


# ------------------------------------------------- the digest gate (#180)


def test_an_untrusted_channel_is_not_judged_against_its_band(con, tmp_path) -> None:
    """A frozen channel's kWh is arithmetic on a stuck reading."""
    from tests.test_digest import hourly_rows, sources

    history = []
    for n in range(14, 0, -1):
        history += hourly_rows(DAY - timedelta(days=n), "ct_1_a", watts=500.0,
                              device=HUB_B)
    today = hourly_rows(DAY, "ct_1_a", watts=5000.0, device=HUB_B)
    paths = sources(tmp_path, history + today)
    rep = digest.build_report(con, local_day=DAY, **paths)
    assert "above_band" in [f.rule for f in rep.findings], "control: it fires"

    gated = digest.build_report(
        con, local_day=DAY, untrusted=[(model.SOURCE_LEVITON, HUB_B, "ct_1_a")], **paths
    )
    assert "above_band" not in [f.rule for f in gated.findings]
    assert gated.skipped_untrusted


def test_an_untrusted_feed_also_silences_the_overnight_floor_rule(con, tmp_path) -> None:
    """That rule reads the feed CTs directly, so a stuck feed reads as a clean floor."""
    from tests.test_digest import hourly_rows, sources

    rows = []
    for n in range(14, 0, -1):
        rows += hourly_rows(DAY - timedelta(days=n), "ct_1_a", watts=500.0, device=HUB_B)
    rows += hourly_rows(DAY, "ct_1_a", watts=5000.0, device=HUB_B)
    paths = sources(tmp_path, rows)
    gated = digest.build_report(
        con, local_day=DAY, untrusted=[(model.SOURCE_LEVITON, HUB_B, "ct_1_a")], **paths
    )
    assert "phantom_load_growth" not in [f.rule for f in gated.findings]
    assert any("Overnight-floor check skipped" in n for n in gated.notes)


def test_an_untrusted_day_never_becomes_the_baseline(con, tmp_path) -> None:
    """The worse half: a fault admitted to history is the "normal" for next time."""
    from tests.test_digest import hourly_rows, sources

    rows = []
    for n in range(14, 0, -1):
        rows += hourly_rows(DAY - timedelta(days=n), "ct_1_a", watts=500.0, device=HUB_B)
    rows += hourly_rows(DAY, "ct_1_a", watts=500.0, device=HUB_B)
    paths = sources(tmp_path, rows)
    rep = digest.build_report(
        con, local_day=DAY, untrusted=[(model.SOURCE_LEVITON, HUB_B, "ct_1_a")], **paths
    )
    assert rep.compared == 0, "an untrusted channel must not be compared at all"


# ---------------------------------------------------------------- read-only


def test_the_checker_writes_nothing(con, tmp_path) -> None:
    """The first stage in this project that is purely read-only. Keep it that way."""
    rows = live_day(DAY, "ct_1_a")
    for h in (5, 6):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    write(tmp_path, "rollup-x.parquet", rows, model.HOURLY_SCHEMA)
    write(tmp_path, "lge.parquet", [], model.METER_SCHEMA)
    before = sorted(p.name for p in tmp_path.iterdir())
    integrity.build_report(
        con,
        hourly=str(tmp_path / "rollup-x.parquet"),
        meter=str(tmp_path / "lge.parquet"),
        start=DAY,
        end=DAY,
    )
    assert sorted(p.name for p in tmp_path.iterdir()) == before


def test_a_report_with_findings_is_not_ok(con, tmp_path) -> None:
    """``ok`` is what the CLI turns into a nonzero exit for a scheduler."""
    rows = live_day(DAY, "ct_1_a")
    for h in (5, 6):
        rows[h] = hour_row(DAY, h, "ct_1_a", lo=489.3, hi=489.3, mean=489.3)
    rep = report(con, tmp_path, rows)
    assert not rep.ok
    assert rep.to_dict()["ok"] is False
    assert rep.to_dict()["counts"]["frozen_channel"] == 1
