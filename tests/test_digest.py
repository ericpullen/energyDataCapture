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
    assert report.skipped_incomplete == ["breaker_p11"]


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
    assert report.skipped_unbaselined == ["breaker_p11"]


def test_an_empty_archive_says_so_rather_than_reporting_all_clear(con, tmp_path) -> None:
    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, []))
    assert report.findings == []
    assert report.notes and "archive" in report.notes[0]


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


def test_a_rising_overnight_floor_is_reported(con, tmp_path) -> None:
    """A 200 W fault is 3% of a busy day and invisible in the total, but it is
    a third of the floor at 03:00 — and 4.8 kWh every single day."""
    rows = []
    for n in range(14, 0, -1):
        for leg in ("ct_1_a", "ct_1_b"):
            rows += hourly_rows(DAY - timedelta(days=n), leg, watts=200.0)
    for leg in ("ct_1_a", "ct_1_b"):
        rows += hourly_rows(DAY, leg, watts=500.0)

    report = digest.build_report(con, local_day=DAY, **sources(tmp_path, rows))
    assert any(f.rule == "phantom_load_growth" for f in report.findings), report.body()


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
