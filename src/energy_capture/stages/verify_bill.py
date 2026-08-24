"""``energycap verify-bill`` — is the amount on the bill right?

``compare-meter`` answers "do the panels agree with the meter". This answers the
question that was actually costing money: **take the utility's own interval
readings, add them up over the billing cycle, price them through the tariff, and
see whether the number LG&E printed is the number the meter earned.**

Deliberately the meter side only
--------------------------------
The kWh here come from ``energy/meter`` — LG&E's own 15-minute readings, not the
Leviton CTs. That is the whole point: the utility's data is the utility's
evidence, and a disagreement between it and the bill is an arithmetic error on
their side, not an instrument tolerance on ours. It also means this command is
unaffected by every CT-side caveat in the project (coverage gates, the Panel B
zero-latch, clamp tolerance) — those belong to ``compare-meter``.

The three ways this could quietly lie, and what stops each
----------------------------------------------------------
**A short cycle looks like a cheap bill.** If the archive is missing intervals,
the summed kWh is low, the priced total is low, and the bill looks overstated by
exactly the size of the gap. So coverage is computed against the DST-aware
expected interval count for the cycle and printed on every run; below
``--min-coverage`` the verdict is withheld entirely rather than reported with an
asterisk.

**The wrong meter.** The two services are on different rate schedules and
different accounts, and the download republishes the house under two retired
ids. Meter selection reuses ``compare-meter``'s resolver, which refuses to guess
between genuinely different meters, and the tariff is then looked up by that
device id — so pricing the barn with the house's residential rate is not
reachable.

**Off-by-one on the cycle.** ``--start`` and ``--end`` are the two meter READ
dates exactly as the bill prints them, and the days billed are ``(start,
end]`` — see :func:`~energy_capture.tariff.billing_days` for the measurement
that settled which end is open. Getting it backwards costs a mean 3.67% on the
barn's cycles, and the day *count* is right either way so nothing else catches
it. When the cycle matches a bill already transcribed in
``config/tariff.json``, the day count is cross-checked against ``days_billed``
and a mismatch is refused.

Reading the verdict
-------------------
``kwh_delta_pct`` is the honest headline: the archive's kWh against the bill's
printed kWh. If that agrees and the dollars do not, the tariff is wrong (or LG&E
mis-billed). If the kWh disagree, nothing downstream matters until that is
explained.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa

from energy_capture import model, tariff as tariff_mod, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.stages.compare import (
    load_meter_tables,
    resolve_interval,
    resolve_meter,
)

STAGE = "verify_bill"
log = get_logger(STAGE)

#: Below this fraction of the cycle's expected intervals, no verdict is given.
DEFAULT_MIN_COVERAGE = 0.995

#: The bill is "verified" when the dollars land inside this. Tight on purpose:
#: this is arithmetic against the utility's own readings, not an instrument
#: comparison, so anything above a rounding cent wants an explanation.
DEFAULT_TOLERANCE_PCT = 1.0

__all__ = ["BillVerification", "MeterCycle", "meter_cycle", "run", "verify"]


@dataclass(frozen=True)
class MeterCycle:
    """What the meter recorded over a billing cycle, and how completely."""

    meter: str
    interval_s: int
    start: date
    end: date
    kwh: float
    intervals: int
    expected_intervals: int
    kwh_by_date: dict[date, float]
    missing_days: tuple[date, ...]

    @property
    def coverage(self) -> float:
        if not self.expected_intervals:
            return 0.0
        return self.intervals / self.expected_intervals

    def to_dict(self) -> dict[str, Any]:
        return {
            "meter": self.meter,
            "interval_s": self.interval_s,
            "cycle_start": self.start.isoformat(),
            "cycle_end": self.end.isoformat(),
            "kwh": round(self.kwh, 3),
            "intervals": self.intervals,
            "expected_intervals": self.expected_intervals,
            "coverage": round(self.coverage, 5),
            "missing_days": [d.isoformat() for d in self.missing_days],
        }


def meter_cycle(
    tables: Sequence[pa.Table],
    *,
    start: date,
    end: date,
    device_id: str,
    interval_s: int,
) -> MeterCycle:
    """Sum ``energy/meter`` over the days a cycle read ``start``..``end`` bills.

    ``start`` and ``end`` are the two meter READ dates as the bill prints them;
    the days summed are ``(start, end]``, per
    :func:`~energy_capture.tariff.billing_days`. Readings are placed by the
    local date of their interval **start**, which is the same convention the
    partitioning uses, so a cycle boundary here means the same thing it means
    everywhere else in the project.
    """
    first, limit = tariff_mod.billing_days(start, end)
    by_date: dict[date, float] = defaultdict(float)
    counts: dict[date, int] = defaultdict(int)

    for table in tables:
        for row in table.to_pylist():
            if row["metric"] != "kwh_interval":
                continue
            if row["device_id"] != device_id or int(row["interval_s"]) != interval_s:
                continue
            local_day = timeutil.local_date_of(timeutil.ensure_utc(row["ts_utc"]))
            if not (first <= local_day < limit):
                continue
            by_date[local_day] += float(row["value"])
            counts[local_day] += 1

    # DST-aware: a 23-hour local day holds fewer intervals than a 25-hour one,
    # and pretending every day is 24 hours would report a spurious gap every
    # spring and spurious completeness every autumn.
    expected = 0
    missing: list[date] = []
    for day in timeutil.iter_local_dates(first, limit - timedelta(days=1)):
        hours = timeutil.local_hours_in_day(day)
        want = hours * 3600 // interval_s
        expected += want
        if counts.get(day, 0) < want:
            missing.append(day)

    return MeterCycle(
        meter=device_id,
        interval_s=interval_s,
        start=start,
        end=end,
        kwh=sum(by_date.values()),
        intervals=sum(counts.values()),
        expected_intervals=expected,
        kwh_by_date=dict(by_date),
        missing_days=tuple(missing),
    )


@dataclass(frozen=True)
class BillVerification:
    """The archive's answer, the bill's answer, and the gap between them."""

    cycle: MeterCycle
    priced: tariff_mod.PricedCycle
    bill_kwh: float | None
    bill_total: Decimal | None
    tolerance_pct: float
    min_coverage: float

    @property
    def kwh_delta(self) -> float | None:
        if self.bill_kwh is None:
            return None
        return self.cycle.kwh - self.bill_kwh

    @property
    def kwh_delta_pct(self) -> float | None:
        if self.kwh_delta is None or not self.bill_kwh:
            return None
        return 100.0 * self.kwh_delta / self.bill_kwh

    @property
    def dollar_delta(self) -> Decimal | None:
        if self.bill_total is None:
            return None
        return self.priced.total - self.bill_total

    @property
    def dollar_delta_pct(self) -> float | None:
        if self.dollar_delta is None or not self.bill_total:
            return None
        return float(100 * self.dollar_delta / self.bill_total)

    @property
    def coverage_ok(self) -> bool:
        return self.cycle.coverage >= self.min_coverage

    @property
    def verdict(self) -> str:
        if not self.coverage_ok:
            return "no_verdict_coverage"
        if self.bill_total is None:
            return "no_bill_total"
        if self.priced.riders_estimated:
            return "no_verdict_estimated_riders"
        assert self.dollar_delta_pct is not None
        return (
            "verified"
            if abs(self.dollar_delta_pct) <= self.tolerance_pct
            else "discrepancy"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "meter": self.cycle.meter,
            "cycle": self.cycle.to_dict(),
            "priced": self.priced.to_dict(),
            "bill_kwh": self.bill_kwh,
            "bill_total": float(self.bill_total) if self.bill_total is not None else None,
            "kwh_delta": round(self.kwh_delta, 3) if self.kwh_delta is not None else None,
            "kwh_delta_pct": (
                round(self.kwh_delta_pct, 3) if self.kwh_delta_pct is not None else None
            ),
            "dollar_delta": float(self.dollar_delta) if self.dollar_delta is not None else None,
            "dollar_delta_pct": (
                round(self.dollar_delta_pct, 3)
                if self.dollar_delta_pct is not None
                else None
            ),
        }


def verify(
    *,
    tariff: tariff_mod.Tariff,
    cycle: MeterCycle,
    bill_kwh: float | None = None,
    bill_total: Decimal | float | str | None = None,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> BillVerification:
    """Price the cycle and set it against the bill.

    The kWh priced are the METER's, never the bill's — pricing the bill's own
    kWh and comparing to the bill's own total would only re-check LG&E's
    arithmetic against itself, which is not the question.
    """
    observed = tariff.cycle_for(cycle.start, cycle.end)

    # A bill that shares exactly one endpoint with the range asked for is a
    # date slip, not a new cycle — and it is the slip most likely to happen,
    # because the two read dates sit adjacent on the bill and are easy to
    # transcribe off by a day or to swap with the neighbouring cycle's. Priced
    # anyway it would silently charge the wrong number of days against the
    # wrong month's riders, so it is refused with both sets of dates named.
    if observed is None:
        for known in tariff.billing_cycles:
            if known.cycle_start == cycle.start or known.cycle_end == cycle.end:
                raise tariff_mod.TariffError(
                    f"{tariff.meter}: asked for {cycle.start}..{cycle.end}, but the "
                    f"bill on file for that read is {known.cycle_start}.."
                    f"{known.cycle_end} ({known.days_billed} days). Both dates are "
                    "the meter READ dates printed on the bill; the days billed are "
                    "(start, end]. Fix the range, or add this cycle to "
                    "config/tariff.json if it is genuinely a different bill."
                )

    if observed is not None and observed.days_billed != (cycle.end - cycle.start).days:
        raise tariff_mod.TariffError(
            f"cycle {cycle.start}..{cycle.end} spans "
            f"{(cycle.end - cycle.start).days} days but the bill on file says "
            f"{observed.days_billed} — config/tariff.json disagrees with itself."
        )

    priced = tariff_mod.price_cycle(
        tariff,
        start=cycle.start,
        end=cycle.end,
        kwh=cycle.kwh,
        kwh_by_date=cycle.kwh_by_date,
        cycle=observed,
    )
    return BillVerification(
        cycle=cycle,
        priced=priced,
        bill_kwh=bill_kwh if bill_kwh is not None else (observed.billed_kwh if observed else None),
        bill_total=(
            Decimal(str(bill_total))
            if bill_total is not None
            else (observed.billed_total if observed else None)
        ),
        tolerance_pct=tolerance_pct,
        min_coverage=min_coverage,
    )


def format_report(result: BillVerification, tariff: tariff_mod.Tariff) -> str:
    c, p = result.cycle, result.priced
    first, limit = tariff_mod.billing_days(c.start, c.end)
    last = limit - timedelta(days=1)
    out: list[str] = [
        f"{tariff.label}",
        f"  meter {c.meter}   {tariff.rate_schedule}"
        + (f"   account {tariff.account}" if tariff.account else ""),
        f"  read {c.start} and {c.end} -> billing {first} .. {last} ({p.days} days)",
        "",
        f"{'METER':<34}{c.kwh:>12,.3f} kWh  from {c.intervals:,}/{c.expected_intervals:,} "
        f"intervals at {c.interval_s}s ({c.coverage:.2%})",
    ]
    if result.bill_kwh is not None:
        out.append(f"{'BILL':<34}{result.bill_kwh:>12,.0f} kWh")
        assert result.kwh_delta is not None and result.kwh_delta_pct is not None
        out.append(
            f"{'DIFFERENCE':<34}{result.kwh_delta:>12,.3f} kWh  "
            f"({result.kwh_delta_pct:+.3f}%)"
        )
    out += ["", "Priced through the tariff:"]
    for line in p.lines:
        detail = f"({line.detail})" if line.detail else ""
        out.append(f"  {line.label + ' ' + detail:<62}{float(line.amount):>10,.2f}")
    out.append(f"  {'Total electric charges':<62}{float(p.electric_charges):>10,.2f}")
    if p.school_tax:
        out.append(f"  {'Rate Increase For School Tax':<62}{float(p.school_tax):>10,.2f}")
    if p.sales_tax:
        out.append(f"  {'Electric Sales Tax':<62}{float(p.sales_tax):>10,.2f}")
    out.append(f"  {'TOTAL':<62}{float(p.total):>10,.2f}")

    if result.bill_total is not None:
        assert result.dollar_delta is not None and result.dollar_delta_pct is not None
        out += [
            "",
            f"  {'Bill says':<62}{float(result.bill_total):>10,.2f}",
            f"  {'Difference':<62}{float(result.dollar_delta):>+10,.2f}  "
            f"({result.dollar_delta_pct:+.3f}%)",
        ]
    if p.all_in_per_kwh:
        out.append(f"\n  All-in ${p.all_in_per_kwh}/kWh over this cycle.")

    out.append("")
    verdicts = {
        "verified": (
            f"VERIFIED — the meter's own readings, priced through the tariff, land "
            f"within {result.tolerance_pct}% of the amount billed."
        ),
        "discrepancy": (
            "DISCREPANCY — this is outside tolerance. Check the kWh line first: if "
            "the meter and the bill agree on kWh, the tariff in config/tariff.json "
            "is stale or wrong; if they disagree, the archive is incomplete or the "
            "cycle dates are off."
        ),
        "no_verdict_coverage": (
            f"NO VERDICT — the archive holds {c.coverage:.2%} of this cycle's "
            f"intervals, under the {result.min_coverage:.2%} floor. Missing intervals "
            "understate the kWh and make the bill look overstated by exactly the size "
            "of the gap, so no verdict is given rather than a flattering one."
        ),
        "no_bill_total": (
            "NO VERDICT — no bill amount to compare against. Pass --bill-total, or "
            "transcribe the bill into config/tariff.json."
        ),
        "no_verdict_estimated_riders": (
            "NO VERDICT — the fuel adjustment and rider percentages for this cycle "
            "are not on file and were carried forward from an earlier month. LG&E "
            "re-sets them monthly, so the total above is an ESTIMATE. Add this "
            "cycle's bill to config/tariff.json to get a real verdict."
        ),
    }
    out.append(verdicts[result.verdict])

    if c.missing_days:
        shown = ", ".join(str(d) for d in c.missing_days[:8])
        more = f" (+{len(c.missing_days) - 8} more)" if len(c.missing_days) > 8 else ""
        out.append(f"\nIncomplete local days: {shown}{more}")
    for warning in p.warnings:
        out.append(f"\nNOTE: {warning}")
    return "\n".join(out)


def run(
    *,
    start: date,
    end: date,
    meter: str | None = None,
    bill_kwh: float | None = None,
    bill_total: float | None = None,
    tariff_path: Path | None = None,
    meter_dir: Path | None = None,
    source: str = model.SOURCE_LGE,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> dict[str, Any]:
    """``energycap verify-bill --start … --end … [--meter …]``."""
    settings = get_settings()
    directory = Path(meter_dir) if meter_dir else settings.spool_dir / "meter"
    tables = load_meter_tables(directory, source=source)
    if not tables:
        raise FileNotFoundError(
            f"no {source}-*.parquet under {directory} — import or fetch the meter "
            "series first (`energycap import-greenbutton`, `energycap fetch-greenbutton`)"
        )

    device_id, note = resolve_meter(tables, requested=meter)
    if device_id is None:
        raise FileNotFoundError(f"no meter readings under {directory}")
    if note:
        log.warning("verify_bill_meter_ambiguous", note=note, chosen=device_id)
    interval_s, interval_note = resolve_interval(tables, device_id=device_id)
    if interval_s is None:
        raise FileNotFoundError(f"no interval readings for meter {device_id}")
    if interval_note:
        log.warning("verify_bill_multiple_intervals", note=interval_note, chosen=interval_s)

    tariffs = tariff_mod.load_tariffs(tariff_path)
    tariff = tariff_mod.tariff_for(device_id, tariffs)

    cycle = meter_cycle(
        tables, start=start, end=end, device_id=device_id, interval_s=interval_s
    )
    result = verify(
        tariff=tariff,
        cycle=cycle,
        bill_kwh=bill_kwh,
        bill_total=bill_total,
        tolerance_pct=tolerance_pct,
        min_coverage=min_coverage,
    )

    report = format_report(result, tariff)
    for message in (note, interval_note):
        if message:
            report = f"NOTE: {message}\n\n{report}"
    print(report)  # noqa: T201 - this command's output *is* the report
    return result.to_dict()
