"""The digest is only worth having if it can be trusted not to cry wolf.

An anomaly detector that fires on the collector's own gaps gets muted, and a
muted detector is worse than none — that is exactly how six days of latched CT
zeros (#180) stayed hidden. So the tests that matter most here are the ones
where the digest is handed something that LOOKS anomalous and must stay quiet:
a half-watched day, a circuit with no history, a cold day with strip heat.

Synthetic Parquet against an in-memory DuckDB, so every rule is exercised
without touching S3.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from energy_capture import model
from energy_capture.stages import digest

HUB = "hub-a"
HOUSE, BARN = "1308468", "1326254"
DAY = date(2026, 8, 23)


@pytest.fixture
def con():
    c = duckdb.connect(config={"threads": 1})
    c.execute("SET TimeZone='UTC'")
    yield c
    c.close()


def hourly_rows(
    day: date,
    channel: str,
    *,
    watts: float,
    hours: int = 24,
    device: str = HUB,
    observed_per_hour: int = 3600,
):
    """One watts row per hour, with a full ``observed_seconds`` by default."""
    rows = []
    for hour in range(hours):
        local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
        utc = local + timedelta(hours=4)
        rows.append({
            "hour_start_utc": utc,
            "local_hour_start": local,
            "source": model.SOURCE_LEVITON,
            "device_id": device,
            "channel_id": channel,
            "metric": "watts",
            "unit": "W",
            "mean": watts, "min": watts, "max": watts, "p95": watts,
            "sample_count": observed_per_hour // 30,
            "first_ts_utc": utc, "last_ts_utc": utc,
            "kwh": watts * observed_per_hour / 3.6e6,
            "observed_seconds": observed_per_hour,
        })
    return rows


def temp_rows(day: date, low_f: float, high_f: float):
    rows = []
    for hour in range(24):
        local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
        value = low_f if hour < 12 else high_f
        rows.append({
            "hour_start_utc": local + timedelta(hours=4),
            "local_hour_start": local,
            "source": model.SOURCE_BRYANT,
            "device_id": "serial",
            "channel_id": "system",
            "metric": "outdoor_temp_f",
            "unit": "F",
            "mean": value, "min": low_f, "max": high_f, "p95": value,
            "sample_count": 120,
            "first_ts_utc": local + timedelta(hours=4),
            "last_ts_utc": local + timedelta(hours=4),
            "kwh": None,
            "observed_seconds": None,
        })
    return rows


def write(tmp_path, name, rows, schema):
    path = tmp_path / name
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return str(path)


def sources(tmp_path, hourly_rows_, daily_rows=None, meter_rows=None):
    hourly = write(tmp_path, "rollup-x.parquet", hourly_rows_, model.HOURLY_SCHEMA)
    daily = write(tmp_path, "bryant.parquet", daily_rows or [], model.DAILY_SCHEMA)
    meter = write(tmp_path, "lge.parquet", meter_rows or [], model.METER_SCHEMA)
    return {"hourly": hourly, "daily": daily, "meter": meter}


def steady_history(channel, *, watts, days=14, upto=DAY):
    """``days`` complete days ending the day before ``upto``."""
    rows = []
    for n in range(days, 0, -1):
        rows += hourly_rows(upto - timedelta(days=n), channel, watts=watts)
    return rows


# ------------------------------------------------------------------ the band


def test_the_band_is_not_widened_by_one_bad_day() -> None:
    """Median + MAD, not mean + standard deviation.

    A fault that starts today must not make tomorrow's identical fault look
    normal. One 100x day moves a standard deviation enough to swallow the next
    one; it does not move the MAD at all.
    """
    normal = [10.0] * 13
    band = digest.band_for(normal)
    assert band is not None and band.contains(10.0)

    with_outlier = digest.band_for([*normal, 1000.0])
    assert with_outlier is not None
    # The band barely moves, so a repeat of the anomaly still fires.
    assert not with_outlier.contains(1000.0)
    assert with_outlier.high < 100.0


def test_too_little_history_is_no_band_at_all() -> None:
    """Silence, not a pass. A circuit with three days of history has not been
    checked, and saying so is different from saying it is fine."""
    assert digest.band_for([10.0, 10.0, 10.0]) is None
    assert digest.band_for([]) is None
    assert digest.band_for([10.0] * digest.MIN_BASELINE_DAYS) is not None


def test_an_utterly_constant_circuit_still_gets_a_usable_band() -> None:
    """MAD is 0 when every day is identical, which would make the band a point
    and fire on a rounding difference."""
    band = digest.band_for([10.0] * 14)
    assert band is not None
    assert band.contains(10.4) and band.contains(9.6)


# ------------------------------------------------- the anti-cry-wolf tests


def test_a_half_watched_day_is_skipped_not_reported_as_a_drop(con, tmp_path) -> None:
    """THE test.

    A day the collector only half watched has roughly half the kWh. That is a
    fault in the collector, not the circuit, and reporting it as "usage halved"
    would train the owner to ignore the digest — which is precisely how #180
    stayed hidden for six days.
    """
    rows = steady_history("breaker_p11", watts=1000.0)
    # Yesterday: same load, half the day observed.
    rows += hourly_rows("breaker_p11" and DAY, "breaker_p11", watts=1000.0, hours=12)
    src = sources(tmp_path, rows)

    report = digest.build_report(con, local_day=DAY, **src)

    assert report.findings == [], report.body()
    assert report.compared == 0
    assert report.skipped_incomplete == ["breaker 11"]


def test_incomplete_days_are_kept_out_of_the_baseline_too(con, tmp_path) -> None:
    """A band built from half-watched days sits low, and then the first
    complete day looks like a spike. The gate has to apply to history as well
    as to the day under test."""
    rows = []
    for n in range(14, 7, -1):  # seven half-watched days
        rows += hourly_rows(DAY - timedelta(days=n), "breaker_p11", watts=1000.0, hours=12)
    for n in range(7, 0, -1):  # seven complete ones
        rows += hourly_rows(DAY - timedelta(days=n), "breaker_p11", watts=1000.0)
    rows += hourly_rows(DAY, "breaker_p11", watts=1000.0)

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))

    # The complete days alone form the band, so an identical complete day is
    # unremarkable rather than a doubling.
    assert report.compared == 1
    assert report.findings == [], report.body()


def test_a_circuit_with_no_history_is_named_not_judged(con, tmp_path) -> None:
    rows = hourly_rows(DAY, "breaker_p11", watts=1000.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert report.findings == []
    assert report.compared == 0
    assert report.skipped_unbaselined == ["breaker 11"]


def test_an_empty_archive_says_so_rather_than_reporting_all_clear(con, tmp_path) -> None:
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, []))
    assert report.findings == []
    # Any note, not notes[0]: the rules that cannot run now say so too, so the
    # empty-archive note is no longer necessarily first.
    assert any("archive" in note for note in report.notes), report.notes


# --------------------------------------------------------------- the rules


def test_a_circuit_above_its_band_is_reported_with_a_cost(con, tmp_path) -> None:
    rows = steady_history("breaker_p11", watts=1000.0)
    rows += hourly_rows(DAY, "breaker_p11", watts=3000.0)

    report = digest.build_report(
        con, local_day=DAY, rate_usd=0.1238, **sources(tmp_path, rows)
    )
    found = [f for f in report.findings if f.rule == "above_band"]
    assert len(found) == 1
    assert found[0].kwh == pytest.approx(48.0, abs=1.0)  # 2 kW x 24 h
    assert found[0].cost_usd == pytest.approx(48.0 * 0.1238, abs=0.3)


def test_a_circuit_that_went_quiet_is_a_finding_not_a_saving(con, tmp_path) -> None:
    """A freezer that stops running uses less power right up until the food
    spoils. The digest has to treat a big drop as a fault."""
    rows = steady_history("breaker_p07", watts=200.0)
    rows += hourly_rows(DAY, "breaker_p07", watts=2.0)

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    found = [f for f in report.findings if f.rule == "circuit_went_quiet"]
    assert len(found) == 1
    assert found[0].kwh is not None and found[0].kwh < 0


def cycling_history(channel, *, on_watts, off_watts=5.0, on_hours=8, days=14):
    """A thermostatic load: drawing for part of each day, idle the rest."""
    rows = []
    for n in range(days, 0, -1):
        day = DAY - timedelta(days=n)
        on = hourly_rows(day, channel, watts=on_watts, hours=on_hours)
        off = hourly_rows(day, channel, watts=off_watts)[on_hours:]
        rows += on + off
    return rows


def test_a_load_that_stops_cycling_is_reported(con, tmp_path) -> None:
    """A water-heater element that never switches off. The finding is the
    CHANGE: this circuit used to cycle and now does not."""
    rows = cycling_history("breaker_p20", on_watts=4500.0)
    rows += hourly_rows(DAY, "breaker_p20", watts=4500.0)  # on all 24 hours

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    found = [f for f in report.findings if f.rule == "stuck_load"]
    assert len(found) == 1, report.body()
    assert "usual 33%" in found[0].detail


def test_a_circuit_that_is_always_on_by_nature_is_never_called_stuck(con, tmp_path) -> None:
    """A fridge or a network rack sits at full duty every day of its life.
    Alarming on that nightly is the fastest way to get the digest muted — and
    then the real fault is missed too."""
    rows = steady_history("breaker_p07", watts=400.0)
    rows += hourly_rows(DAY, "breaker_p07", watts=400.0)

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "stuck_load" for f in report.findings), report.body()
    assert report.ok, report.body()


def test_a_load_still_cycling_normally_is_not_called_stuck(con, tmp_path) -> None:
    """The control: same circuit, still switching off."""
    rows = cycling_history("breaker_p20", on_watts=4500.0)
    rows += hourly_rows(DAY, "breaker_p20", watts=4500.0, hours=9)
    rows += hourly_rows(DAY, "breaker_p20", watts=5.0)[9:]

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "stuck_load" for f in report.findings)


def rpm_rows(day, means):
    """Bryant ``compressor_rpm`` hourly rows; ``means`` cycles across the 24."""
    rows = []
    for hour in range(24):
        local = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
        m = means[hour % len(means)]
        rows.append({
            "hour_start_utc": local + timedelta(hours=4), "local_hour_start": local,
            "source": model.SOURCE_BRYANT, "device_id": "serial", "channel_id": "system",
            "metric": "compressor_rpm", "unit": "rpm",
            "mean": m, "min": m, "max": m, "p95": m, "sample_count": 120,
            "first_ts_utc": local + timedelta(hours=4), "last_ts_utc": local + timedelta(hours=4),
            "kwh": None, "observed_seconds": None,
        })
    return rows


HVAC_KEY = (model.SOURCE_LEVITON, HUB, "breaker_p10")
HVAC_LABELS = {HVAC_KEY: {"short_label": "Heat pump", "panel": "B", "category": "hvac"}}


def test_a_modulating_hvac_compressor_running_all_day_is_not_stuck(con, tmp_path) -> None:
    """A variable-speed heat pump runs 24/7 in a heat wave, ramping its speed to
    hold setpoint. That is correct operation — the unit's own RPM proves it — not
    a stuck compressor, and it must not page."""
    rows = cycling_history("breaker_p10", on_watts=1500.0)
    rows += hourly_rows(DAY, "breaker_p10", watts=1500.0)   # drawing all 24 hours
    rows += rpm_rows(DAY, [500.0, 2500.0])                  # compressor MODULATING

    report = digest.build_report(
        con, local_day=DAY, labels=HVAC_LABELS, **sources(tmp_path, rows)
    )
    assert not any(f.rule == "stuck_load" for f in report.findings), report.body()
    assert any("modulating" in n for n in report.notes), report.notes


def test_a_pinned_hvac_compressor_running_all_day_is_still_flagged(con, tmp_path) -> None:
    """The real fault the rule must keep: a variable-speed compressor whose speed
    is PINNED while it draws all day is stuck, and still fires."""
    rows = cycling_history("breaker_p10", on_watts=1500.0)
    rows += hourly_rows(DAY, "breaker_p10", watts=1500.0)
    rows += rpm_rows(DAY, [1500.0])                         # compressor PINNED

    report = digest.build_report(
        con, local_day=DAY, labels=HVAC_LABELS, **sources(tmp_path, rows)
    )
    found = [f for f in report.findings if f.rule == "stuck_load"]
    assert len(found) == 1, report.body()
    assert "pinned" in found[0].detail


def test_strip_heat_on_a_mild_day_is_reported(con, tmp_path) -> None:
    """The most expensive silent fault available here, and the best measured:
    `eheat` kWh/day and `outdoor_temp_f` were both already collected and
    nothing ever joined them."""
    hourly = temp_rows(DAY, low_f=58.0, high_f=72.0)
    daily = [{
        "ts_utc": datetime.combine(DAY, datetime.min.time()) + timedelta(hours=4),
        "ts_local": datetime.combine(DAY, datetime.min.time()),
        "source": model.SOURCE_BRYANT, "device_id": "serial",
        "channel_id": "eheat", "metric": "kwh_day", "value": 22.0, "unit": "kWh",
    }]
    report = digest.build_report(
        con, local_day=DAY, rate_usd=0.1238, **sources(tmp_path, hourly, daily)
    )
    found = [f for f in report.findings if f.rule == "strip_heat_in_mild_weather"]
    assert len(found) == 1
    assert "58" in found[0].headline
    assert found[0].cost_usd == pytest.approx(22.0 * 0.1238, abs=0.1)


def test_strip_heat_on_a_cold_day_is_not_a_finding(con, tmp_path) -> None:
    """On a genuinely cold morning resistance heat is doing its job. Firing
    here would make the rule useless every winter."""
    hourly = temp_rows(DAY, low_f=8.0, high_f=25.0)
    daily = [{
        "ts_utc": datetime.combine(DAY, datetime.min.time()) + timedelta(hours=4),
        "ts_local": datetime.combine(DAY, datetime.min.time()),
        "source": model.SOURCE_BRYANT, "device_id": "serial",
        "channel_id": "eheat", "metric": "kwh_day", "value": 60.0, "unit": "kWh",
    }]
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, hourly, daily))
    assert not any(f.rule == "strip_heat_in_mild_weather" for f in report.findings)


def test_the_barn_outside_its_charging_envelope_is_reported(con, tmp_path) -> None:
    meter = [{
        "ts_utc": datetime.combine(DAY, datetime.min.time()) + timedelta(hours=4),
        "ts_local": datetime.combine(DAY, datetime.min.time()),
        "source": model.SOURCE_LGE, "device_id": BARN,
        "channel_id": "electric_main", "metric": "kwh_interval",
        "value": 95.0, "unit": "kWh", "interval_s": 900,
    }]
    report = digest.build_report(
        con, local_day=DAY, barn_device=BARN, **sources(tmp_path, [], None, meter)
    )
    assert any(f.rule == "barn_envelope" for f in report.findings), report.body()


def test_a_normal_barn_day_is_silent(con, tmp_path) -> None:
    meter = [{
        "ts_utc": datetime.combine(DAY, datetime.min.time()) + timedelta(hours=4),
        "ts_local": datetime.combine(DAY, datetime.min.time()),
        "source": model.SOURCE_LGE, "device_id": BARN,
        "channel_id": "electric_main", "metric": "kwh_interval",
        "value": 18.0, "unit": "kWh", "interval_s": 900,
    }]
    report = digest.build_report(
        con, local_day=DAY, barn_device=BARN, **sources(tmp_path, [], None, meter)
    )
    assert not any(f.rule == "barn_envelope" for f in report.findings)


def meter_night(day, *, kwh_per_interval, device=HOUSE):
    """900 s meter intervals across the whole day (the rule reads 01:00-05:59)."""
    rows = []
    for q in range(96):
        local = datetime.combine(day, datetime.min.time()) + timedelta(minutes=15 * q)
        rows.append({
            "ts_utc": local + timedelta(hours=4), "ts_local": local,
            "source": model.SOURCE_LGE, "device_id": device,
            "channel_id": "electric_main", "metric": "kwh_interval",
            "value": kwh_per_interval, "unit": "kWh", "interval_s": 900,
        })
    return rows


def test_a_rising_overnight_floor_is_reported(con, tmp_path) -> None:
    """A 200 W fault is 3% of a busy day and invisible in the total, but it is
    a third of the floor at 03:00 — and 4.8 kWh every single day. Read from the
    utility meter, so a frozen clamp can no longer silence it."""
    meter = []
    for n in range(14, 0, -1):
        meter += meter_night(DAY - timedelta(days=n), kwh_per_interval=0.25)  # 1,000 W
    meter += meter_night(DAY, kwh_per_interval=0.35)                          # 1,400 W

    report = digest.build_report(
        con, local_day=DAY, meter_device=HOUSE, rate_usd=0.12,
        **sources(tmp_path, [], None, meter),
    )
    found = [f for f in report.findings if f.rule == "phantom_load_growth"]
    assert len(found) == 1, report.body()
    assert "1400 W, up from 1000 W" in found[0].headline
    assert found[0].cost_usd == pytest.approx(9.6 * 0.12, abs=0.01)


def test_the_overnight_floor_ignores_hours_the_heat_pump_ran(con, tmp_path) -> None:
    """A cold night runs the compressor till dawn. That raises the floor without
    anything having been left on, so those hours are not the floor."""
    meter = []
    for n in range(14, 0, -1):
        meter += meter_night(DAY - timedelta(days=n), kwh_per_interval=0.25)
    meter += meter_night(DAY, kwh_per_interval=0.75)                          # 3 kW all night
    hourly = rpm_rows(DAY, [2400.0])                                          # compressor on

    report = digest.build_report(
        con, local_day=DAY, meter_device=HOUSE,
        **sources(tmp_path, hourly, None, meter),
    )
    assert not any(f.rule == "phantom_load_growth" for f in report.findings)
    assert any("heat pump ran all night" in n for n in report.notes), report.notes


def test_a_frozen_feed_clamp_no_longer_silences_the_overnight_floor(con, tmp_path) -> None:
    """It used to skip whenever a feed CT was untrusted — nearly every night
    with Panel B's clamps — so the check had effectively never run."""
    meter = []
    for n in range(14, 0, -1):
        meter += meter_night(DAY - timedelta(days=n), kwh_per_interval=0.25)
    meter += meter_night(DAY, kwh_per_interval=0.35)
    report = digest.build_report(
        con, local_day=DAY, meter_device=HOUSE,
        untrusted=[(model.SOURCE_LEVITON, HUB, "ct_1_a")],
        **sources(tmp_path, [], None, meter),
    )
    assert any(f.rule == "phantom_load_growth" for f in report.findings), report.body()


def test_a_feeder_ct_drawing_all_day_is_not_a_stuck_load(con, tmp_path) -> None:
    """The HVAC subpanel CT with the fan set to run continuously: a setting,
    not a stuck element. Paged every fortnight in the replay."""
    rows = cycling_history("ct_2_a", on_watts=420.0)
    rows += hourly_rows(DAY, "ct_2_a", watts=420.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "stuck_load" for f in report.findings), report.body()


def test_feed_totals_are_not_judged_against_a_band(con, tmp_path) -> None:
    """A feed CT is the panel's total; a band on it repeats what the breakers
    beneath already said (the replay paged "Panel A feed A used 36.5 kWh")."""
    rows = steady_history("ct_1_a", watts=1000.0)
    rows += hourly_rows(DAY, "ct_1_a", watts=3000.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "above_band" for f in report.findings), report.body()


def test_a_modest_dip_on_a_busy_circuit_is_not_went_quiet(con, tmp_path) -> None:
    """Main-floor lights at 2.8 kWh against 3.6 is a quiet evening, not a
    failure. Only a drop to half or less counts."""
    rows = steady_history("breaker_p16", watts=150.0)            # 3.6 kWh/day
    rows += hourly_rows(DAY, "breaker_p16", watts=117.0)          # 2.8 kWh
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "circuit_went_quiet" for f in report.findings), report.body()


def test_a_circuit_that_barely_draws_cannot_go_quiet(con, tmp_path) -> None:
    """A kitchen circuit that "usually" uses 0.8 kWh and some days is not used
    at all is not a failure on the days it is not used."""
    rows = steady_history("breaker_p10", watts=33.0)             # 0.8 kWh/day
    rows += hourly_rows(DAY, "breaker_p10", watts=0.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert not any(f.rule == "circuit_went_quiet" for f in report.findings), report.body()


# --------------------------------------------------------------- the report


def test_a_clean_day_says_what_it_actually_checked(con, tmp_path) -> None:
    rows = steady_history("breaker_p11", watts=1000.0)
    rows += hourly_rows(DAY, "breaker_p11", watts=1000.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))

    assert report.ok
    assert "nothing unusual" in report.title()
    assert "1 circuits compared" in report.body()


def test_the_title_counts_the_findings(con, tmp_path) -> None:
    # Under STUCK_MIN_WATTS so only the band rule can fire.
    rows = steady_history("breaker_p11", watts=50.0)
    rows += hourly_rows(DAY, "breaker_p11", watts=190.0)
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert len(report.findings) == 1, report.body()
    assert "1 thing to look at" in report.title()


# --------------------------------------------------- cross-day notify policy


def _report_with(*findings: digest.Finding) -> digest.DigestReport:
    rep = digest.DigestReport(local_day=DAY)
    rep.findings.extend(findings)
    return rep


FROZEN = digest.Finding(
    rule="frozen_channel", key="frozen_channel:leviton/HUBB/ct_1_a", device="HUBB",
    headline="Panel B feed A reported exactly 539.79 W for 4 consecutive hours",
    detail="...",
)
DRYER = digest.Finding(
    rule="above_band", key="above_band:leviton/HUBA/breaker_p1",
    headline="Panel A breaker 1 — Dryer outlet used 15.1 kWh, usually 1.1",
    detail="...", kwh=14.0, cost_usd=1.73,
)
OVEN = digest.Finding(
    rule="above_band", key="above_band:leviton/HUBB/breaker_p14",
    headline="Panel B breaker 14 — Double oven used 0.6 kWh, usually 0.0",
    detail="...", kwh=0.6, cost_usd=0.08,
)


def _state(**days: list[digest.Finding]) -> dict:
    """``d3=[FROZEN]`` means: seen 3 days before DAY, and paged that day."""
    out: dict = {"days": {}, "pushed": {}}
    for name, findings in days.items():
        day = (DAY - timedelta(days=int(name[1:]))).isoformat()
        out["days"][day] = {
            "signatures": [f.signature() for f in findings],
            "instrument_devices": sorted({f.device for f in findings if f.instrument and f.device}),
        }
        for f in findings:
            out["pushed"].setdefault(f.signature(), day)
    return out


def decide(report, state=None, **kw):
    kw.setdefault("min_cost_usd", 0.50)
    return digest._decide_push(report, state, local_day=DAY, **kw)


def test_a_material_new_finding_pages() -> None:
    plan = decide(_report_with(DRYER))
    assert plan.send and plan.reason == "new" and plan.alerts == [DRYER]


def test_a_finding_below_the_cost_floor_rides_along_but_does_not_page() -> None:
    """Ordinary cooking was a page every few days at $0.07-0.20."""
    plan = decide(_report_with(OVEN))
    assert not plan.send and plan.alerts == [] and plan.also == [OVEN]


def test_a_repeat_does_not_page_even_after_skipping_a_day() -> None:
    """THE regression. The old rule compared with last night only, so a fault
    that skipped one day paged again — 8 of 9 mornings in November 2026."""
    tonight = digest.Finding(
        rule="above_band", key=DRYER.key, headline="Dryer used 12.0 kWh, usually 1.1",
        detail="...", cost_usd=1.36,
    )
    plan = decide(_report_with(tonight), _state(d2=[DRYER]))   # absent yesterday
    assert not plan.send and plan.also == [tonight]


def test_a_finding_absent_for_more_than_a_week_pages_again() -> None:
    plan = decide(_report_with(DRYER), _state(d9=[DRYER]))
    assert plan.send and plan.alerts == [DRYER]


def test_a_known_instrument_fault_is_one_status_line_not_a_page() -> None:
    """Three quarters of every replayed message was one Panel B clamp fault."""
    other_channel = digest.Finding(
        rule="frozen_channel", key="frozen_channel:leviton/HUBB/ct_2_a", device="HUBB",
        headline="Strip heat feed A reported exactly 421.36 W for 10 consecutive hours",
        detail="...",
    )
    plan = decide(_report_with(other_channel), _state(d1=[FROZEN], d3=[FROZEN], d5=[FROZEN]))
    assert not plan.send
    assert plan.alerts == [] and plan.known_instrument == {"HUBB": 4}


def test_an_instrument_problem_on_a_clean_hub_still_pages() -> None:
    fresh = digest.Finding(
        rule="feed_below_children", key="feed_below_children:HUBA", device="HUBA",
        headline="HUBA feed read less than its own circuits in 3 hours", detail="...",
    )
    plan = decide(_report_with(fresh), _state(d1=[FROZEN], d3=[FROZEN], d5=[FROZEN]))
    assert plan.send and plan.alerts == [fresh]


def test_the_weekly_run_always_sends() -> None:
    plan = decide(_report_with(), weekly=True)
    assert plan.send and plan.reason == "weekly"


def test_a_clean_night_stays_quiet() -> None:
    plan = decide(_report_with(), _state(d1=[DRYER]))
    assert not plan.send and plan.reason == "clear"


def test_the_one_night_state_format_is_upgraded_without_repaging() -> None:
    """Deploying this must not page every ongoing finding at once."""
    plan = decide(_report_with(DRYER), {"signatures": [DRYER.signature()]})
    assert not plan.send and plan.also == [DRYER]


def test_state_keeps_history_and_only_marks_delivered_alerts_pushed(tmp_path) -> None:
    path = tmp_path / "digest-state.json"
    plan = decide(_report_with(DRYER, OVEN))
    now = datetime(2026, 8, 24, 15, 0)
    digest._write_digest_state(path, plan, local_day=DAY, previous=None, now=now, notified=False)
    first = digest._read_digest_state(path)
    assert first["days"][DAY.isoformat()]["signatures"] == sorted(
        [DRYER.signature(), OVEN.signature()]
    )
    assert first["pushed"] == {} and first["notified_utc"] is None
    digest._write_digest_state(path, plan, local_day=DAY, previous=first, now=now, notified=True)
    second = digest._read_digest_state(path)
    assert second["pushed"] == {DRYER.signature(): DAY.isoformat()}, "OVEN never paged"
    assert second["notified_utc"]


# ----------------------------------------------------------- the push itself


def test_the_push_always_fits_and_says_what_it_dropped() -> None:
    """41 of 46 replayed digests were cut off mid-sentence by Pushover."""
    many = [
        digest.Finding(
            rule="above_band", key=f"above_band:leviton/HUBA/breaker_p{n}",
            headline=f"Panel A breaker {n} — Some circuit with a long name used 9.9 kWh, usually 1.0",
            detail="x" * 400, cost_usd=1.0 + n / 100,
        )
        for n in range(30)
    ]
    report = _report_with(*many)
    title, message = digest.render_push(report, decide(report))
    assert len(message) <= digest.PUSH_MAX_CHARS
    assert message.splitlines()[-1].startswith("+") and "more in the log" in message
    assert "xxxx" not in message, "the long detail belongs in the log, not the push"
    assert "30 new things" in title


def test_the_push_puts_the_known_fault_on_one_line() -> None:
    other = digest.Finding(
        rule="frozen_channel", key="frozen_channel:leviton/HUBB/ct_2_a", device="HUBB",
        headline="Strip heat feed A reported exactly 421.36 W for 10 consecutive hours",
        detail="...",
    )
    report = _report_with(DRYER, other, FROZEN)
    plan = decide(report, _state(d1=[FROZEN], d3=[FROZEN], d5=[FROZEN]))
    labels = {("leviton", "HUBB", "ct_1_a"): {"panel": "B"}}
    title, message = digest.render_push(report, plan, labels=labels)
    assert "1 new thing" in title
    assert message.count("Panel B readings inconsistent") == 1
    assert "421.36" not in message and "Dryer outlet" in message


def test_the_weekly_summary_compares_with_the_week_before_and_last_year(con, tmp_path) -> None:
    meter = []
    for n in range(14):
        kwh = 0.5 if n < 7 else 0.4                       # this week 48/day, before 38.4
        meter += meter_night(DAY - timedelta(days=n), kwh_per_interval=kwh)
    for n in range(7):
        meter += meter_night(DAY - timedelta(days=364 + n), kwh_per_interval=1.0)
    src = sources(tmp_path, [], None, meter)
    summary = digest.build_weekly(
        con, hourly=src["hourly"], daily=src["daily"], meter=src["meter"],
        meter_device=HOUSE, week_end=DAY, rate_usd=0.12,
        instrument_days={"HUBB": 5}, labels={("leviton", "HUBB", "ct_1_a"): {"panel": "B"}},
    )
    assert "336 kWh" in summary.title and "~$40" in summary.title
    assert "week before +25%" in summary.lines[0]
    assert "same week last year -50%" in summary.lines[0]
    assert any("Panel B readings inconsistent 5 of 7 days" in line for line in summary.lines)
