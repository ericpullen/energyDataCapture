"""What a billing cycle should cost — LG&E's arithmetic, reproduced exactly.

The archive can already say how many kWh crossed the meter. This turns that into
dollars, so ``energycap verify-bill`` can answer the question the kWh comparison
could not: *is the amount on the bill right?*

Why this is a real module and not three multiplications
-------------------------------------------------------
Because an LG&E bill is not ``kWh x rate``. It is a fixed daily charge, three
per-kWh charges, three *percentage* riders — each applied to a **different**
base — a flat assistance charge, and two taxes, one of which is levied on top of
the other. The order matters, the bases matter, and they are **not the same on
the two rate schedules in this household**:

============================  =====================  ========================
                              Residential (house)    General Service (barn)
============================  =====================  ========================
Fuel adjustment in the        **yes**                **no**
percentage-rider base
Flat per-kWh deduction from   none                   **$0.0286/kWh**
that base
Home energy assistance        $0.30/month            not charged
Kentucky sales tax            exempt (primary        **6%**, on top of the
                              residence)             school tax
============================  =====================  ========================

Every rule above was derived from, and is re-verified against, ten real bills
per meter: :func:`price_cycle` reproduces all twenty to the cent
(``tests/test_tariff.py``). None of it is guessed from a published rate sheet.

The two kinds of number in ``config/tariff.json``
-------------------------------------------------
**Rate periods** are the tariff proper — the basic, energy and DSM rates. They
change only when the PSC approves a new rate, and they are open-ended, so a
future cycle can be priced with confidence.

**Billing cycles** are what the utility actually charged month by month: the
fuel adjustment and the three rider percentages, which LG&E **re-sets every
month** and which nobody can predict. Ten observed months span $0.00048 to
$0.01063/kWh on the fuel adjustment alone. So pricing a cycle we have no bill
for is an *estimate*, and :class:`PricedCycle` says so in
:attr:`~PricedCycle.riders_estimated` rather than quietly presenting a forecast
as a reconciliation.

Splitting a cycle across a rate change
--------------------------------------
When a rate changes mid-cycle the bill prints two lines, and the kWh split
between them is allocated by **actual metered usage**, not by day count — the
2026-01-30 house bill billed 355 kWh at the old rate where day-proration gives
335. That is a thing this project can reproduce, because it holds the 15-minute
meter series: pass ``kwh_by_date`` and the split is exact. Without it, the split
falls back to day-proration and :attr:`~PricedCycle.allocation` says which was
used.

Money is :class:`~decimal.Decimal`, rounded half-up at each printed line
-------------------------------------------------------------------------
LG&E rounds every line to the cent and sums the rounded lines; summing exact
products and rounding once gives a different answer. Float arithmetic with
banker's rounding gives a third. The bills are the specification, so this uses
``Decimal`` with ``ROUND_HALF_UP`` per line.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

from energy_capture import timeutil

__all__ = [
    "BillingCycle",
    "ChargeLine",
    "PricedCycle",
    "RatePeriod",
    "Tariff",
    "TariffError",
    "UnknownMeterError",
    "allocate_kwh",
    "billing_days",
    "load_tariffs",
    "price_cycle",
]

#: Hand-maintained, committed alongside the channel map (PLAN.md §9 semantics).
DEFAULT_TARIFF_PATH = Path("config/tariff.json")

CENT = Decimal("0.01")


def billing_days(read_start: date, read_end: date) -> tuple[date, date]:
    """The two meter READ dates -> the half-open span of days actually billed.

    A bill prints two read dates and a day count: read 9/25 and again 10/27,
    "32 Days Billed". Both the obvious readings of that are wrong. The days
    billed are ``(read_start, read_end]`` — the previous read date's usage
    belongs to the previous cycle, and the current read date's usage belongs to
    this one — which this returns half-open as ``[read_start+1, read_end+1)``.

    **This was measured, not assumed.** Summing the barn's 15-minute series over
    eight fully covered cycles gives a mean absolute error against the billed
    kWh of **3.67%** if the read date is treated as exclusive, and **0.16%** on
    this convention — a 23x improvement, with the residual error losing its
    alternating sign. The house's three covered cycles agree (0.56% -> 0.05%).
    Physically: the reads land late in the day, so usage on the read date has
    already accrued to the cycle being closed.

    The day COUNT is the same either way, so a bill still reconciles on the
    wrong convention; only the kWh move. That is what makes this worth a
    function and a docstring rather than a ``+1`` at the call site.
    """
    return read_start + timedelta(days=1), read_end + timedelta(days=1)


class TariffError(RuntimeError):
    """The tariff file cannot answer the question asked of it."""


class UnknownMeterError(TariffError):
    """No tariff is on file for this meter."""


def _money(value: Decimal | float | int | str) -> Decimal:
    """Round to the cent the way a bill does — half **up**, never half-even."""
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def _dec(value: float | int | str | None) -> Decimal:
    return Decimal(str(value or 0))


# --------------------------------------------------------------- the file


@dataclass(frozen=True)
class RatePeriod:
    """An open-ended tariff rate. ``effective_from`` is a LOCAL date, inclusive."""

    effective_from: date
    basic_service_charge_per_day: Decimal
    energy_charge_per_kwh: Decimal
    dsm_per_kwh: Decimal
    effective_from_inferred: bool = False
    note: str | None = None

    @classmethod
    def from_json(cls, blob: Mapping[str, Any]) -> RatePeriod:
        if not blob.get("effective_from"):
            raise TariffError(
                "a rate_period has no effective_from; a period with an unknown "
                "start date cannot be used to price anything"
            )
        return cls(
            effective_from=timeutil.parse_local_date(blob["effective_from"]),
            basic_service_charge_per_day=_dec(blob["basic_service_charge_per_day"]),
            energy_charge_per_kwh=_dec(blob["energy_charge_per_kwh"]),
            dsm_per_kwh=_dec(blob["dsm_per_kwh"]),
            effective_from_inferred=bool(blob.get("effective_from_inferred", False)),
            note=blob.get("note"),
        )


@dataclass(frozen=True)
class BillingCycle:
    """One observed bill: the monthly riders, and what LG&E charged for them.

    ``cycle_start`` and ``cycle_end`` are the two meter READ dates exactly as
    the bill prints them. The days actually billed are ``(cycle_start,
    cycle_end]`` — see :func:`billing_days`, which is where the evidence for
    that lives. Use :meth:`billed_span`, never the raw dates, to sum energy.
    """

    cycle_start: date
    cycle_end: date
    days_billed: int
    fuel_adjustment_per_kwh: Decimal
    environmental_surcharge_pct: Decimal | None
    retired_asset_recovery_pct: Decimal | None
    pilot_generation_recovery_pct: Decimal | None
    home_energy_assistance_charge: Decimal
    billed_kwh: float | None = None
    billed_electric_charges: Decimal | None = None
    billed_taxes_and_fees: Decimal | None = None
    billed_total: Decimal | None = None
    bill: str | None = None

    @classmethod
    def from_json(cls, blob: Mapping[str, Any]) -> BillingCycle:
        def pct(key: str) -> Decimal | None:
            value = blob.get(key)
            return None if value is None else _dec(value)

        def money(key: str) -> Decimal | None:
            value = blob.get(key)
            return None if value is None else _money(value)

        return cls(
            cycle_start=timeutil.parse_local_date(blob["cycle_start"]),
            cycle_end=timeutil.parse_local_date(blob["cycle_end"]),
            days_billed=int(blob["days_billed"]),
            fuel_adjustment_per_kwh=_dec(blob.get("fuel_adjustment_per_kwh")),
            environmental_surcharge_pct=pct("environmental_surcharge_pct"),
            retired_asset_recovery_pct=pct("retired_asset_recovery_pct"),
            pilot_generation_recovery_pct=pct("pilot_generation_recovery_pct"),
            home_energy_assistance_charge=_dec(
                blob.get("home_energy_assistance_charge")
            ),
            billed_kwh=blob.get("billed_kwh"),
            billed_electric_charges=money("billed_electric_charges"),
            billed_taxes_and_fees=money("billed_taxes_and_fees"),
            billed_total=money("billed_total"),
            bill=blob.get("bill"),
        )

    def covers(self, start: date, end: date) -> bool:
        """True when ``start``/``end`` are this bill's two printed read dates."""
        return self.cycle_start == start and self.cycle_end == end

    def billed_span(self) -> tuple[date, date]:
        """The half-open span of local days this bill actually charges for."""
        return billing_days(self.cycle_start, self.cycle_end)


@dataclass(frozen=True)
class Tariff:
    """One meter's rate schedule, its rate history, and its observed bills."""

    meter: str
    label: str
    account: str
    rate_schedule: str
    school_tax_pct: Decimal
    sales_tax_pct: Decimal
    rider_base_includes_fuel_adjustment: bool
    rider_base_deduction_per_kwh: Decimal
    rate_periods: tuple[RatePeriod, ...]
    billing_cycles: tuple[BillingCycle, ...] = ()
    notes: str | None = None

    @classmethod
    def from_json(cls, meter: str, blob: Mapping[str, Any]) -> Tariff:
        periods = tuple(
            sorted(
                (RatePeriod.from_json(p) for p in blob["rate_periods"]),
                key=lambda p: p.effective_from,
            )
        )
        if not periods:
            raise TariffError(f"meter {meter!r} has no rate_periods")
        return cls(
            meter=meter,
            label=blob.get("label", meter),
            account=blob.get("account", ""),
            rate_schedule=blob.get("rate_schedule", ""),
            school_tax_pct=_dec(blob.get("school_tax_pct")),
            sales_tax_pct=_dec(blob.get("sales_tax_pct")),
            rider_base_includes_fuel_adjustment=bool(
                blob.get("rider_base_includes_fuel_adjustment", True)
            ),
            rider_base_deduction_per_kwh=_dec(
                blob.get("rider_base_deduction_per_kwh")
            ),
            rate_periods=periods,
            billing_cycles=tuple(
                sorted(
                    (BillingCycle.from_json(c) for c in blob.get("billing_cycles", ())),
                    key=lambda c: c.cycle_start,
                )
            ),
            notes=blob.get("notes"),
        )

    # -- lookups ----------------------------------------------------------

    def periods_covering(self, first: date, limit: date) -> list[tuple[date, date, RatePeriod]]:
        """``(from, to, period)`` spans tiling the BILLED days ``[first, limit)``.

        Takes a billed span (see :func:`billing_days`), not the printed read
        dates. Raises rather than extrapolating backwards: the oldest rate
        period begins at the oldest bill on hand, and what the rate was before
        that is genuinely unknown.
        """
        if limit <= first:
            raise TariffError(f"cycle end {limit} must be after start {first}")
        earliest = self.rate_periods[0].effective_from
        if first < earliest:
            raise TariffError(
                f"{self.meter}: no rate on file before {earliest} — cannot price a "
                f"cycle whose first billed day is {first}. Add the older bill to "
                "config/tariff.json."
            )

        boundaries = [
            p.effective_from for p in self.rate_periods if first < p.effective_from < limit
        ]
        edges = [first, *boundaries, limit]
        spans: list[tuple[date, date, RatePeriod]] = []
        for begin, finish in zip(edges, edges[1:], strict=False):
            active = [p for p in self.rate_periods if p.effective_from <= begin][-1]
            spans.append((begin, finish, active))
        return spans

    def cycle_for(self, start: date, end: date) -> BillingCycle | None:
        """The observed bill for exactly this cycle, if we have it."""
        for cycle in self.billing_cycles:
            if cycle.covers(start, end):
                return cycle
        return None

    def latest_cycle(self) -> BillingCycle | None:
        return self.billing_cycles[-1] if self.billing_cycles else None


def load_tariffs(path: Path | str | None = None) -> dict[str, Tariff]:
    """Read ``config/tariff.json`` into ``{device_id: Tariff}``."""
    resolved = Path(path) if path else DEFAULT_TARIFF_PATH
    if not resolved.is_file():
        raise TariffError(
            f"no tariff file at {resolved} — verify-bill needs the rate parameters "
            "transcribed from a bill. See config/tariff.json in the repo."
        )
    blob = json.loads(resolved.read_text())
    return {
        meter: Tariff.from_json(meter, body)
        for meter, body in blob.get("tariffs", {}).items()
    }


def tariff_for(meter: str, tariffs: Mapping[str, Tariff]) -> Tariff:
    try:
        return tariffs[meter]
    except KeyError:
        raise UnknownMeterError(
            f"no tariff on file for meter {meter!r}; config/tariff.json has "
            f"{sorted(tariffs) or 'none'}. The two services are on DIFFERENT rate "
            "schedules — do not price one with the other's tariff."
        ) from None


# ------------------------------------------------------------- the pricing


@dataclass(frozen=True)
class ChargeLine:
    """One line as the bill prints it."""

    label: str
    detail: str
    amount: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {"label": self.label, "detail": self.detail, "amount": float(self.amount)}


@dataclass(frozen=True)
class PricedCycle:
    """What the cycle should cost, itemised, plus how much to trust it."""

    meter: str
    rate_schedule: str
    start: date
    end: date
    days: int
    kwh: float
    lines: tuple[ChargeLine, ...]
    electric_charges: Decimal
    school_tax: Decimal
    sales_tax: Decimal
    total: Decimal
    #: ``"single_period"`` | ``"metered"`` | ``"day_proration"`` — how kWh were
    #: split across a mid-cycle rate change.
    allocation: str
    #: True when no bill is on file for this cycle and the fuel adjustment and
    #: rider percentages were carried forward from an earlier month.
    riders_estimated: bool
    riders_from: str | None = None
    warnings: tuple[str, ...] = ()

    @property
    def all_in_per_kwh(self) -> Decimal | None:
        if not self.kwh:
            return None
        return (self.total / Decimal(str(self.kwh))).quantize(Decimal("0.00001"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "meter": self.meter,
            "rate_schedule": self.rate_schedule,
            "cycle_start": self.start.isoformat(),
            "cycle_end": self.end.isoformat(),
            "days": self.days,
            "kwh": self.kwh,
            "lines": [line.to_dict() for line in self.lines],
            "electric_charges": float(self.electric_charges),
            "school_tax": float(self.school_tax),
            "sales_tax": float(self.sales_tax),
            "total": float(self.total),
            "all_in_per_kwh": float(self.all_in_per_kwh) if self.all_in_per_kwh else None,
            "allocation": self.allocation,
            "riders_estimated": self.riders_estimated,
            "riders_from": self.riders_from,
            "warnings": list(self.warnings),
        }


def allocate_kwh(
    spans: Sequence[tuple[date, date, RatePeriod]],
    *,
    kwh: float,
    kwh_by_date: Mapping[date, float] | None = None,
) -> tuple[list[int], str]:
    """Split ``kwh`` across rate spans the way LG&E does — by usage if we can.

    Returns whole kWh per span (the bill prints integers) summing exactly to
    ``round(kwh)``, and which method was used. The remainder lands on the last
    span so the parts always re-add to the whole.
    """
    total = int(Decimal(str(kwh)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if len(spans) == 1:
        return [total], "single_period"

    weights: list[float]
    method: str
    covered = kwh_by_date is not None and all(
        day in kwh_by_date
        for begin, finish, _ in spans
        for day in timeutil.iter_local_dates(begin, finish - timedelta(days=1))
    )
    if covered:
        assert kwh_by_date is not None
        weights = [
            sum(
                kwh_by_date[day]
                for day in timeutil.iter_local_dates(begin, finish - timedelta(days=1))
            )
            for begin, finish, _ in spans
        ]
        method = "metered"
    else:
        weights = [float((finish - begin).days) for begin, finish, _ in spans]
        method = "day_proration"

    scale = sum(weights)
    if not scale:
        return [0] * (len(spans) - 1) + [total], method

    parts = [
        int(
            (Decimal(str(w)) / Decimal(str(scale)) * Decimal(total)).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
        )
        for w in weights[:-1]
    ]
    parts.append(total - sum(parts))
    return parts, method


def price_cycle(
    tariff: Tariff,
    *,
    start: date,
    end: date,
    kwh: float,
    kwh_by_date: Mapping[date, float] | None = None,
    cycle: BillingCycle | None = None,
) -> PricedCycle:
    """Price a cycle. ``start``/``end`` are the two meter READ dates on the bill.

    The days actually charged are ``(start, end]`` — :func:`billing_days` holds
    the measurement behind that. ``kwh_by_date`` is keyed on local date and only
    matters when a rate changed mid-cycle.

    ``cycle`` supplies the month's fuel adjustment and rider percentages. When
    omitted it is looked up by exact read dates, and if that misses, the most
    recent observed month is carried forward and the result is flagged
    :attr:`~PricedCycle.riders_estimated`.
    """
    first, limit = billing_days(start, end)
    spans = tariff.periods_covering(first, limit)
    warnings: list[str] = []

    resolved = cycle or tariff.cycle_for(start, end)
    estimated = False
    if resolved is None:
        resolved = tariff.latest_cycle()
        if resolved is None:
            raise TariffError(
                f"{tariff.meter}: no billing_cycles on file, so the fuel adjustment "
                "and rider percentages are unknown. Transcribe one bill into "
                "config/tariff.json."
            )
        estimated = True
        warnings.append(
            f"No bill on file for {start}..{end}. The fuel adjustment and the three "
            f"rider percentages were carried forward from the {resolved.cycle_start}.."
            f"{resolved.cycle_end} cycle — LG&E re-sets them monthly, and the observed "
            "range on the fuel adjustment alone is 22x, so treat the total as an "
            "ESTIMATE, not a reconciliation."
        )

    # Only an inferred date that actually falls INSIDE this cycle is worth
    # saying anything about. Every cycle after 2026-04-01 sits within an
    # inferred-start period, and warning on all of them would train the reader
    # to skip the warnings that matter.
    if any(period.effective_from_inferred for _, _, period in spans[1:]):
        warnings.append(
            "A rate change inside this cycle has an effective date that was inferred "
            "rather than printed on a bill (see the note in config/tariff.json)."
        )

    parts, allocation = allocate_kwh(spans, kwh=kwh, kwh_by_date=kwh_by_date)
    if allocation == "day_proration":
        warnings.append(
            "A rate changed mid-cycle and no daily meter series was available, so the "
            "kWh were split by DAY COUNT. LG&E splits by actual metered usage — the "
            "2026-01 house bill differed from day-proration by 20 kWh."
        )

    lines: list[ChargeLine] = []
    basic = energy = dsm = Decimal("0")

    for (begin, finish, period), part in zip(spans, parts, strict=True):
        days = (finish - begin).days
        amount = _money(period.basic_service_charge_per_day * days)
        basic += amount
        lines.append(
            ChargeLine(
                "Basic Service Charge",
                f"${period.basic_service_charge_per_day} x {days} Days",
                amount,
            )
        )
    for (_, _, period), part in zip(spans, parts, strict=True):
        amount = _money(period.energy_charge_per_kwh * part)
        energy += amount
        lines.append(
            ChargeLine(
                "Energy Charge", f"${period.energy_charge_per_kwh} x {part:,} kWh", amount
            )
        )
    for (_, _, period), part in zip(spans, parts, strict=True):
        amount = _money(period.dsm_per_kwh * part)
        dsm += amount
        lines.append(
            ChargeLine("Electric DSM", f"${period.dsm_per_kwh} x {part:,} kWh", amount)
        )

    total_kwh = Decimal(sum(parts))
    fac = _money(resolved.fuel_adjustment_per_kwh * total_kwh)
    lines.append(
        ChargeLine(
            "Electric Fuel Adjustment",
            f"${resolved.fuel_adjustment_per_kwh} x {int(total_kwh):,} kWh",
            fac,
        )
    )

    # The percentage riders. Each applies to a DIFFERENT base, and General
    # Service subtracts a flat per-kWh amount from all three.
    deduction = _money(tariff.rider_base_deduction_per_kwh * total_kwh)
    rider_base = basic + energy + dsm
    if tariff.rider_base_includes_fuel_adjustment:
        rider_base += fac
    rider_base -= deduction

    def show(base: Decimal) -> str:
        return f"${base}" if not deduction else f"(${base + deduction} - ${deduction})"

    env = Decimal("0")
    if resolved.environmental_surcharge_pct is not None:
        env = _money(resolved.environmental_surcharge_pct / 100 * rider_base)
        lines.append(
            ChargeLine(
                "Environmental Surcharge",
                f"{resolved.environmental_surcharge_pct}% x {show(rider_base)}",
                env,
            )
        )

    if resolved.retired_asset_recovery_pct is not None:
        rar_base = rider_base + env
        lines.append(
            ChargeLine(
                "Retired Asset Recovery",
                f"{resolved.retired_asset_recovery_pct}% x {show(rar_base)}",
                _money(resolved.retired_asset_recovery_pct / 100 * rar_base),
            )
        )

    if resolved.pilot_generation_recovery_pct is not None:
        pgr_base = basic + energy - deduction
        lines.append(
            ChargeLine(
                "Pilot Generation Recovery",
                f"{resolved.pilot_generation_recovery_pct}% x {show(pgr_base)}",
                _money(resolved.pilot_generation_recovery_pct / 100 * pgr_base),
            )
        )

    hea = _money(resolved.home_energy_assistance_charge)
    if hea:
        lines.append(ChargeLine("Home Energy Assistance Fund Charge", "", hea))

    electric = _money(sum((line.amount for line in lines), Decimal("0")))

    # The assistance charge is the one line that is never taxed; sales tax is
    # levied on top of the school tax, not alongside it.
    school = _money(tariff.school_tax_pct / 100 * (electric - hea))
    sales = (
        _money(tariff.sales_tax_pct / 100 * (electric + school))
        if tariff.sales_tax_pct
        else Decimal("0.00")
    )

    return PricedCycle(
        meter=tariff.meter,
        rate_schedule=tariff.rate_schedule,
        start=start,
        end=end,
        days=(end - start).days,
        kwh=kwh,
        lines=tuple(lines),
        electric_charges=electric,
        school_tax=school,
        sales_tax=sales,
        total=_money(electric + school + sales),
        allocation=allocation,
        riders_estimated=estimated,
        riders_from=resolved.bill,
        warnings=tuple(warnings),
    )


def marginal_rate(tariff: Tariff, *, on: date, cycle: BillingCycle | None = None) -> Decimal:
    """All-in $/kWh of *one more* kWh — everything variable, taxes included.

    This is the number to put against an anomaly ("the water heater wasted 40
    kWh") — not the all-in average, which is inflated by the fixed daily charge.
    """
    period = [p for p in tariff.rate_periods if p.effective_from <= on][-1]
    resolved = cycle or tariff.latest_cycle()
    if resolved is None:
        raise TariffError(f"{tariff.meter}: no billing_cycles on file")

    per_kwh = period.energy_charge_per_kwh + period.dsm_per_kwh
    base = per_kwh
    if tariff.rider_base_includes_fuel_adjustment:
        base += resolved.fuel_adjustment_per_kwh
    per_kwh += resolved.fuel_adjustment_per_kwh
    base -= tariff.rider_base_deduction_per_kwh

    env = (resolved.environmental_surcharge_pct or Decimal("0")) / 100 * base
    rar = (resolved.retired_asset_recovery_pct or Decimal("0")) / 100 * (base + env)
    pgr = (
        (resolved.pilot_generation_recovery_pct or Decimal("0"))
        / 100
        * (period.energy_charge_per_kwh - tariff.rider_base_deduction_per_kwh)
    )

    variable = per_kwh + env + rar + pgr
    variable *= 1 + tariff.school_tax_pct / 100
    variable *= 1 + tariff.sales_tax_pct / 100
    return variable.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
