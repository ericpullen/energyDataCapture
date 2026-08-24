"""``verify-bill`` is only trustworthy if it refuses to answer when it should.

The pricing arithmetic is pinned in ``test_tariff.py``. What matters here is the
other half: an archive with a hole in it produces a low kWh total, a low priced
total, and a bill that looks overstated by exactly the size of the hole. Every
test below is about that failure mode and the three gates against it — coverage,
cycle boundaries, and meter identity.

Synthetic meter tables throughout, built through the real ESPI importer so the
rows are shaped the way production rows are.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from energy_capture import tariff as T
from energy_capture.stages import greenbutton
from energy_capture.stages.verify_bill import meter_cycle, verify

TARIFF_PATH = Path("config/tariff.json")
HOUSE = "1308468"
INTERVAL_S = 900


@pytest.fixture(scope="module")
def tariffs() -> dict[str, T.Tariff]:
    return T.load_tariffs(TARIFF_PATH)


def espi(readings: list[tuple[int, int, int]], meter: str = HOUSE) -> str:
    """A minimal ESPI document: ``(epoch_start, duration_s, watt_hours)``."""
    base = "https://mymeter.example.com/espi"
    point = f"{base}/UsagePoint/{meter}"
    mr = f"{point}/MeterReading/1"
    body = "".join(
        f"<espi:IntervalReading><espi:timePeriod>"
        f"<espi:duration>{d}</espi:duration><espi:start>{s}</espi:start>"
        f"</espi:timePeriod><espi:value>{v}</espi:value></espi:IntervalReading>"
        for s, d, v in readings
    )
    return f"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:espi="http://naesb.org/espi">
  <entry><link rel="self" href="{point}"/>
    <content><espi:UsagePoint><espi:name>{meter}</espi:name>
    </espi:UsagePoint></content></entry>
  <entry><link rel="self" href="{mr}"/><link rel="related" href="{point}"/>
    <content><espi:MeterReading/></content></entry>
  <entry><link rel="self" href="{point}/ReadingType/1"/>
    <link rel="related" href="{mr}"/>
    <content><espi:ReadingType><espi:flowDirection>1</espi:flowDirection>
      <espi:intervalLength>{INTERVAL_S}</espi:intervalLength>
      <espi:powerOfTenMultiplier>0</espi:powerOfTenMultiplier>
      <espi:uom>72</espi:uom></espi:ReadingType></content></entry>
  <entry><link rel="self" href="{mr}/IntervalBlock/1"/><content>
    <espi:IntervalBlock><espi:interval><espi:duration>86400</espi:duration>
      <espi:start>{readings[0][0] if readings else 0}</espi:start></espi:interval>
      {body}</espi:IntervalBlock></content></entry>
</feed>
"""


def tables_for(
    tmp_path: Path,
    *,
    first_day: date,
    days: int,
    wh_per_interval: int = 1000,
    skip: set[date] | None = None,
    meter: str = HOUSE,
):
    """A steady load over ``days`` local days starting ``first_day``.

    ``skip`` drops whole local days, which is how a collector outage or a
    custodian gap actually looks — not as zeros, as absent rows.
    """
    from energy_capture import timeutil

    skip = skip or set()
    readings: list[tuple[int, int, int]] = []
    for offset in range(days):
        day = first_day + timedelta(days=offset)
        if day in skip:
            continue
        start_utc, end_utc = timeutil.local_day_bounds_utc(day)
        stamp = start_utc
        while stamp < end_utc:
            readings.append((int(stamp.timestamp()), INTERVAL_S, wh_per_interval))
            stamp += timedelta(seconds=INTERVAL_S)

    export = tmp_path / f"gb-{meter}.xml"
    export.write_text(espi(readings, meter=meter))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")
    return [pq.read_table(Path(f)) for f in summary["files"]]


# ------------------------------------------------------- the cycle boundary


def test_the_read_date_is_billed_and_the_previous_read_date_is_not(
    tmp_path: Path,
) -> None:
    """The off-by-one that the day count cannot catch.

    A cycle read 4/1 and again 4/11 bills ten days: 4/2..4/11. Loading eleven
    days and asking for that cycle must pick up exactly ten, and must leave out
    4/1 rather than 4/11.
    """
    tables = tables_for(tmp_path, first_day=date(2026, 4, 1), days=11)
    cycle = meter_cycle(
        tables,
        start=date(2026, 4, 1),
        end=date(2026, 4, 11),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.intervals == 10 * 96
    assert cycle.coverage == 1.0
    assert date(2026, 4, 1) not in cycle.kwh_by_date
    assert date(2026, 4, 11) in cycle.kwh_by_date
    assert sorted(cycle.kwh_by_date) == [
        date(2026, 4, d) for d in range(2, 12)
    ]


def test_a_spring_forward_day_is_twenty_three_hours_not_twenty_four(
    tmp_path: Path,
) -> None:
    """2026-03-08 loses an hour locally; expecting 96 intervals invents a gap."""
    tables = tables_for(tmp_path, first_day=date(2026, 3, 6), days=5)
    cycle = meter_cycle(
        tables,
        start=date(2026, 3, 6),
        end=date(2026, 3, 10),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    # 3/7, 3/8 (23h), 3/9, 3/10 -> 96 + 92 + 96 + 96
    assert cycle.expected_intervals == 96 + 92 + 96 + 96
    assert cycle.coverage == 1.0
    assert not cycle.missing_days


def test_a_fall_back_day_is_twenty_five_hours(tmp_path: Path) -> None:
    """2026-11-01 gains an hour; 96 expected would call a complete day complete
    while a whole hour of energy went unaccounted for."""
    tables = tables_for(tmp_path, first_day=date(2026, 10, 30), days=4)
    cycle = meter_cycle(
        tables,
        start=date(2026, 10, 30),
        end=date(2026, 11, 2),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.expected_intervals == 96 + 100 + 96
    assert cycle.coverage == 1.0


# ------------------------------------------------------------- the coverage gate


def test_a_hole_in_the_archive_withholds_the_verdict(
    tmp_path: Path, tariffs
) -> None:
    """The whole point of the gate.

    Three missing days out of thirty make the meter read ~10% low, which prices
    ~10% low, which makes a perfectly correct bill look 10% overstated. That is
    the most dangerous output this command could produce, so it produces none.
    """
    first = date(2026, 6, 27)
    missing = {date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)}
    tables = tables_for(tmp_path, first_day=first, days=32, skip=missing)

    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 7, 28),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.coverage < 0.995
    assert set(cycle.missing_days) == missing

    result = verify(tariff=tariffs[HOUSE], cycle=cycle, bill_kwh=3072, bill_total=400.0)
    assert result.verdict == "no_verdict_coverage"
    # The gap really would have flattered the reading, which is why it is gated.
    assert result.kwh_delta is not None and result.kwh_delta < 0


def test_a_complete_cycle_verifies(tmp_path: Path, tariffs) -> None:
    tables = tables_for(tmp_path, first_day=date(2026, 6, 27), days=32)
    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 7, 28),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.coverage == 1.0
    assert cycle.kwh == pytest.approx(32 * 96 * 1.0)

    priced = T.price_cycle(
        tariffs[HOUSE],
        start=date(2026, 6, 26),
        end=date(2026, 7, 28),
        kwh=cycle.kwh,
        kwh_by_date=cycle.kwh_by_date,
    )
    result = verify(
        tariff=tariffs[HOUSE],
        cycle=cycle,
        bill_kwh=cycle.kwh,
        bill_total=float(priced.total),
    )
    assert result.verdict == "verified"


def test_a_real_discrepancy_is_reported_as_one(tmp_path: Path, tariffs) -> None:
    """Coverage is perfect and the dollars still disagree — that is a finding,
    not a gap, and it must not be softened into one."""
    tables = tables_for(tmp_path, first_day=date(2026, 6, 27), days=32)
    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 7, 28),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    result = verify(
        tariff=tariffs[HOUSE], cycle=cycle, bill_kwh=cycle.kwh, bill_total=900.0
    )
    assert result.verdict == "discrepancy"
    assert result.dollar_delta is not None and result.dollar_delta < 0


def test_an_estimated_cycle_gets_no_verdict_even_at_full_coverage(
    tmp_path: Path, tariffs
) -> None:
    """Full coverage is not enough when the riders were guessed.

    2026-08 has no bill on file, so the fuel adjustment is last month's. The
    dollars might happen to land close; that would be luck, not verification.
    """
    tables = tables_for(tmp_path, first_day=date(2026, 7, 29), days=30)
    cycle = meter_cycle(
        tables,
        start=date(2026, 7, 28),
        end=date(2026, 8, 27),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.coverage == 1.0
    result = verify(
        tariff=tariffs[HOUSE], cycle=cycle, bill_kwh=cycle.kwh, bill_total=330.0
    )
    assert result.priced.riders_estimated
    assert result.verdict == "no_verdict_estimated_riders"


# ------------------------------------------------------------- identity gates


def test_a_day_count_that_disagrees_with_the_bill_is_refused(
    tmp_path: Path, tariffs
) -> None:
    """Asking for 6/26..7/29 when the bill on file reads 6/26..7/28 is a date
    slip. Priced anyway it charges 33 days against the wrong month's riders, so
    it is refused with both sets of dates named."""
    tables = tables_for(tmp_path, first_day=date(2026, 6, 27), days=33)
    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 7, 29),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    with pytest.raises(T.TariffError, match=r"2026-06-26\.\.2026-07-28"):
        verify(tariff=tariffs[HOUSE], cycle=cycle)


def test_the_barn_cannot_be_priced_with_the_house_tariff(tariffs) -> None:
    """Different schedules, different accounts, a 6% tax apart. The lookup is by
    device id precisely so this is unreachable rather than merely discouraged."""
    house = T.tariff_for(HOUSE, tariffs)
    barn = T.tariff_for("1326254", tariffs)
    assert house.rate_schedule != barn.rate_schedule

    cycle = next(c for c in barn.billing_cycles if c.cycle_start == date(2026, 6, 26))
    on_barn = T.price_cycle(
        barn, start=cycle.cycle_start, end=cycle.cycle_end, kwh=872, cycle=cycle
    )
    on_house = T.price_cycle(
        house, start=cycle.cycle_start, end=cycle.cycle_end, kwh=872, cycle=cycle
    )
    assert on_barn.total == cycle.billed_total
    assert abs(on_house.total - on_barn.total) > Decimal("30")


def test_readings_from_another_meter_are_not_counted(tmp_path: Path) -> None:
    """The download republishes the house under retired ids carrying an
    identical series. Summing them would treble the house."""
    tables_for(tmp_path, first_day=date(2026, 6, 27), days=32)
    tables = tables_for(
        tmp_path, first_day=date(2026, 6, 27), days=32, meter="944006"
    )
    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 7, 28),
        device_id=HOUSE,
        interval_s=INTERVAL_S,
    )
    assert cycle.kwh == pytest.approx(32 * 96 * 1.0)


def test_a_second_interval_series_is_not_added_in(tmp_path: Path) -> None:
    """LG&E publishes the same energy at 900s and 3600s. Adding both doubles it."""
    from energy_capture.stages.compare import resolve_interval

    tables = tables_for(tmp_path, first_day=date(2026, 6, 27), days=2)
    hourly = tmp_path / "hourly.xml"
    start = datetime(2026, 6, 27, 4, tzinfo=UTC)
    hourly.write_text(
        espi([(int((start + timedelta(hours=h)).timestamp()), 3600, 4000) for h in range(24)])
    )
    greenbutton.run(path=hourly, out_dir=tmp_path / "meter", interval_s=3600)
    tables = [
        pq.read_table(p) for p in sorted((tmp_path / "meter").glob("lge-*.parquet"))
    ]

    chosen, note = resolve_interval(tables, device_id=HOUSE)
    assert chosen == INTERVAL_S
    assert note is not None and "double" in note

    cycle = meter_cycle(
        tables,
        start=date(2026, 6, 26),
        end=date(2026, 6, 28),
        device_id=HOUSE,
        interval_s=chosen,
    )
    assert cycle.kwh == pytest.approx(2 * 96 * 1.0)
