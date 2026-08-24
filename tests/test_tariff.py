"""The tariff is only worth having if it reproduces real bills.

The centrepiece is :func:`test_every_transcribed_bill_reproduces_to_the_cent`,
which replays every cycle in ``config/tariff.json`` through
:func:`~energy_capture.tariff.price_cycle` and requires the printed total back.
That is a regression test on the committed file as much as on the code: a
mistyped rider percentage or a transposed digit added with next month's bill
fails here rather than quietly biasing every answer the command gives.

The rest pin the rules that are easy to get wrong and impossible to notice:
which base each percentage rider applies to (different on the two schedules in
this household), that money rounds half-up per line the way a bill does, and
that a cycle nobody has a bill for is reported as an estimate instead of a
reconciliation.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from energy_capture import tariff as T

TARIFF_PATH = Path("config/tariff.json")

#: The three cycles that span a mid-cycle rate change. Priced from the bill's
#: own total kWh with no daily series to split them, they fall back to
#: day-proration -- LG&E splits by metered usage -- so they land near, not on,
#: the printed total. Bounded here so the fallback can never quietly widen.
DAY_PRORATION_CYCLES = {
    ("1308468", date(2025, 12, 29)),
    ("1308468", date(2026, 3, 26)),
    ("1326254", date(2026, 3, 26)),
}
DAY_PRORATION_TOLERANCE = Decimal("0.25")


@pytest.fixture(scope="module")
def tariffs() -> dict[str, T.Tariff]:
    return T.load_tariffs(TARIFF_PATH)


def test_the_committed_tariff_loads_and_covers_both_services(tariffs) -> None:
    assert set(tariffs) == {"1308468", "1326254"}
    house, barn = tariffs["1308468"], tariffs["1326254"]

    # The single most expensive mistake available here is pricing one service
    # with the other's tariff: different schedules, different accounts, and the
    # barn pays a sales tax the house is exempt from.
    assert house.rate_schedule != barn.rate_schedule
    assert house.sales_tax_pct == 0
    assert barn.sales_tax_pct == 6
    assert house.rider_base_includes_fuel_adjustment
    assert not barn.rider_base_includes_fuel_adjustment
    assert barn.rider_base_deduction_per_kwh == Decimal("0.0286")


def test_every_transcribed_bill_reproduces_to_the_cent(tariffs) -> None:
    """Twenty real bills, ten per meter, priced from their own billed kWh."""
    checked = 0
    for meter, tariff in tariffs.items():
        for cycle in tariff.billing_cycles:
            assert cycle.billed_kwh is not None and cycle.billed_total is not None
            priced = T.price_cycle(
                tariff,
                start=cycle.cycle_start,
                end=cycle.cycle_end,
                kwh=cycle.billed_kwh,
                cycle=cycle,
            )
            delta = abs(priced.total - cycle.billed_total)
            if (meter, cycle.cycle_start) in DAY_PRORATION_CYCLES:
                assert priced.allocation == "day_proration"
                assert delta <= DAY_PRORATION_TOLERANCE, (
                    f"{meter} {cycle.cycle_start}: day-proration drifted {delta}"
                )
            else:
                assert priced.total == cycle.billed_total, (
                    f"{meter} {cycle.cycle_start}..{cycle.cycle_end}: "
                    f"priced {priced.total}, bill says {cycle.billed_total}"
                )
                assert priced.electric_charges == cycle.billed_electric_charges
                assert (
                    priced.school_tax + priced.sales_tax == cycle.billed_taxes_and_fees
                )
            checked += 1
    assert checked == 20, "both meters should carry ten transcribed bills"


def test_a_metered_split_beats_day_proration_on_the_cycle_that_proved_it(
    tariffs,
) -> None:
    """The 2026-01 house bill is why ``kwh_by_date`` exists.

    LG&E billed 355 kWh at the pre-2026-01-01 rate. Day-proration over the same
    two days gives 335 — a 20 kWh error, because they allocate by what the
    meter actually recorded. Handed a daily series that reproduces their split,
    the total comes out exact.
    """
    house = tariffs["1308468"]
    cycle = next(
        c for c in house.billing_cycles if c.cycle_start == date(2025, 12, 29)
    )
    first, limit = cycle.billed_span()
    assert (first, limit) == (date(2025, 12, 30), date(2026, 1, 29))

    # 355 kWh over the two days before the rate change, the rest spread after.
    change = date(2026, 1, 1)
    after = [change + timedelta(days=n) for n in range((limit - change).days)]
    by_date = {date(2025, 12, 30): 177.5, date(2025, 12, 31): 177.5}
    by_date |= dict.fromkeys(after, (cycle.billed_kwh - 355) / len(after))

    priced = T.price_cycle(
        house,
        start=cycle.cycle_start,
        end=cycle.cycle_end,
        kwh=cycle.billed_kwh,
        kwh_by_date=by_date,
        cycle=cycle,
    )
    assert priced.allocation == "metered"
    assert priced.total == cycle.billed_total

    energy_lines = [line for line in priced.lines if line.label == "Energy Charge"]
    assert [line.detail for line in energy_lines] == [
        "$0.10838 x 355 kWh",
        "$0.11867 x 4,665 kWh",
    ]


def test_billing_days_puts_the_read_date_inside_the_cycle_it_closes() -> None:
    """A cycle read 6/26 and again 7/28 bills 6/27..7/28 — 32 days.

    Measured, not assumed: see the docstring on ``billing_days``. The day COUNT
    is the same on the wrong convention, which is exactly why this needs a test
    of its own rather than being implied by the totals above.
    """
    first, limit = T.billing_days(date(2026, 6, 26), date(2026, 7, 28))
    assert first == date(2026, 6, 27)
    assert limit == date(2026, 7, 29)
    assert (limit - first).days == 32


def test_the_riders_apply_to_the_bases_the_bill_prints(tariffs) -> None:
    """Each percentage rider has its own base, and they are not interchangeable.

    Checked against the 2026-07-30 house bill, whose printed bases are
    $330.69 (environmental), $332.38 (retired asset) and $320.68 (pilot
    generation) — three different numbers on one bill.
    """
    house = tariffs["1308468"]
    cycle = next(c for c in house.billing_cycles if c.cycle_start == date(2026, 6, 26))
    priced = T.price_cycle(
        house, start=cycle.cycle_start, end=cycle.cycle_end, kwh=2690, cycle=cycle
    )
    details = {line.label: line.detail for line in priced.lines}
    assert details["Environmental Surcharge"] == "0.51% x $330.69"
    assert details["Retired Asset Recovery"] == "1.46% x $332.38"
    assert details["Pilot Generation Recovery"] == "0.43% x $320.68"


def test_general_service_excludes_fuel_and_deducts_per_kwh_from_the_rider_base(
    tariffs,
) -> None:
    """The barn's bill prints its rider base as a subtraction: ``($157.59 - $24.94)``."""
    barn = tariffs["1326254"]
    cycle = next(c for c in barn.billing_cycles if c.cycle_start == date(2026, 6, 26))
    priced = T.price_cycle(
        barn, start=cycle.cycle_start, end=cycle.cycle_end, kwh=872, cycle=cycle
    )
    details = {line.label: line.detail for line in priced.lines}
    assert details["Environmental Surcharge"] == "0.73% x ($157.59 - $24.94)"
    assert details["Pilot Generation Recovery"] == "0.6% x ($156.80 - $24.94)"
    # $0.0286/kWh x 872 kWh = $24.94, to the cent.
    assert priced.total == cycle.billed_total


def test_a_credit_rider_reduces_the_bill(tariffs) -> None:
    """``0.380% CR`` on the bill is a negative charge, not a positive one.

    Five of the ten house cycles carry credit riders; reading the sign off a
    percentage the bill prints without its ``CR`` marker would overstate every
    one of them.
    """
    house = tariffs["1308468"]
    cycle = next(c for c in house.billing_cycles if c.cycle_start == date(2025, 9, 25))
    assert cycle.environmental_surcharge_pct is not None
    assert cycle.environmental_surcharge_pct < 0

    priced = T.price_cycle(
        house, start=cycle.cycle_start, end=cycle.cycle_end, kwh=2646, cycle=cycle
    )
    env = next(l for l in priced.lines if l.label == "Environmental Surcharge")
    assert env.amount < 0
    assert priced.total == cycle.billed_total


def test_the_assistance_charge_is_the_one_line_never_taxed(tariffs) -> None:
    house = tariffs["1308468"]
    cycle = next(c for c in house.billing_cycles if c.cycle_start == date(2026, 6, 26))
    priced = T.price_cycle(
        house, start=cycle.cycle_start, end=cycle.cycle_end, kwh=2690, cycle=cycle
    )
    taxable = priced.electric_charges - cycle.home_energy_assistance_charge
    assert priced.school_tax == (taxable * Decimal("0.03")).quantize(Decimal("0.01"))


def test_sales_tax_is_levied_on_top_of_the_school_tax(tariffs) -> None:
    """The barn's 6% applies to (electric + school tax), not to electric alone."""
    barn = tariffs["1326254"]
    cycle = next(c for c in barn.billing_cycles if c.cycle_start == date(2026, 6, 26))
    priced = T.price_cycle(
        barn, start=cycle.cycle_start, end=cycle.cycle_end, kwh=872, cycle=cycle
    )
    naive = (priced.electric_charges * Decimal("0.06")).quantize(Decimal("0.01"))
    assert priced.sales_tax != naive
    assert priced.sales_tax == (
        (priced.electric_charges + priced.school_tax) * Decimal("0.06")
    ).quantize(Decimal("0.01"))


def test_a_cycle_with_no_bill_on_file_is_an_estimate_not_a_reconciliation(
    tariffs,
) -> None:
    """The fuel adjustment moves 22x across the observed months.

    Carrying last month's forward is the only thing that can be done, but
    presenting the result as a verified total would be a lie.
    """
    house = tariffs["1308468"]
    priced = T.price_cycle(
        house, start=date(2026, 7, 28), end=date(2026, 8, 27), kwh=2500
    )
    assert priced.riders_estimated
    assert any("ESTIMATE" in w for w in priced.warnings)


def test_pricing_before_the_oldest_bill_refuses_rather_than_extrapolating(
    tariffs,
) -> None:
    with pytest.raises(T.TariffError, match="no rate on file before"):
        T.price_cycle(
            tariffs["1308468"], start=date(2025, 1, 1), end=date(2025, 2, 1), kwh=2000
        )


def test_an_unknown_meter_names_the_ones_we_have(tariffs) -> None:
    with pytest.raises(T.UnknownMeterError, match="1308468"):
        T.tariff_for("944006", tariffs)


def test_money_rounds_half_up_per_line_the_way_a_bill_does() -> None:
    """Banker's rounding would send $0.005 to the even cent and lose the penny.

    ``0.11867 x 4665 = 553.59555`` -> ``553.60`` on the real bill.
    """
    assert T._money(Decimal("553.595")) == Decimal("553.60")
    assert T._money(Decimal("0.125")) == Decimal("0.13")
    assert round(0.125, 2) == 0.12  # what float rounding would have done


def test_the_marginal_rate_is_below_the_all_in_rate(tariffs) -> None:
    """One more kWh costs less than the average, because the daily charge is fixed.

    The digest will price anomalies with this; using the all-in average would
    overstate every one of them, worst on the barn where a $1.29/day charge is
    spread over ~1,000 kWh.
    """
    for meter, tariff in tariffs.items():
        cycle = tariff.latest_cycle()
        assert cycle is not None and cycle.billed_kwh and cycle.billed_total
        marginal = T.marginal_rate(tariff, on=cycle.cycle_end)
        all_in = cycle.billed_total / Decimal(str(cycle.billed_kwh))
        assert 0 < marginal < all_in, meter


def test_an_inferred_rate_date_only_warns_when_the_cycle_spans_it(tariffs) -> None:
    """Every cycle after 2026-04-01 sits *inside* the inferred-start period.

    Warning on all of them would train the reader to skip the warnings that
    matter. Only a change landing mid-cycle is worth saying anything about.
    """
    house = tariffs["1308468"]
    inferred = "effective date that was inferred"

    inside = next(c for c in house.billing_cycles if c.cycle_start == date(2026, 6, 26))
    quiet = T.price_cycle(
        house, start=inside.cycle_start, end=inside.cycle_end, kwh=2690, cycle=inside
    )
    assert len(quiet.warnings) == 0 or not any(inferred in w for w in quiet.warnings)

    spanning = next(
        c for c in house.billing_cycles if c.cycle_start == date(2026, 3, 26)
    )
    loud = T.price_cycle(
        house, start=spanning.cycle_start, end=spanning.cycle_end, kwh=2424, cycle=spanning
    )
    assert any(inferred in w for w in loud.warnings)
