"""``energycap digest`` — the nightly "is anything wrong?" pass.

Everything else in this project reports what happened. This is the only thing
that says whether what happened was *unusual*, and it is the answer to the
owner's second goal. Until now that role was filled by a human noticing a shape
on a chart: six days of latched CT zeros (DEVIATIONS #180) and a three-day LG&E
lapse (#177) were both found that way, days late.

What it looks at, and why those things
--------------------------------------
Two kinds of check, deliberately kept apart.

**A trailing band, per circuit.** Yesterday's kWh for each channel against the
median of the previous ``BASELINE_DAYS``, with the width set by the median
absolute deviation rather than the standard deviation — one bad day must not
widen the band enough to hide the next one. This is the general net; it needs no
knowledge of what a circuit *is*.

**Five hard rules**, each written against a specific thing that can go wrong in
this house and would otherwise cost real money for months:

===========================  ==================================================
rule                          what it catches
===========================  ==================================================
``strip_heat_in_mild_weather``  resistance heat running when the heat pump
                                should carry the load — the single most
                                expensive silent fault available here, and the
                                best instrumented (``eheat`` kWh/day and
                                ``outdoor_temp_f`` are both already collected
                                and nothing joined them)
``stuck_load``                  an element or pump that never cycles off:
                                near-100% duty over the day
``circuit_went_quiet``          a freezer, sump or fridge that stopped drawing
                                what it always draws — the failure that looks
                                like *less* usage
``barn_envelope``               the barn outside its 3.6–40 kWh/day EV-charging
                                envelope
``phantom_load_growth``         the overnight floor creeping up, which is how a
                                small always-on fault hides inside a big total
===========================  ==================================================

Why it can be trusted not to cry wolf
-------------------------------------
**Every comparison is coverage-gated on ``observed_seconds``** (#190). A day the
collector only half watched has roughly half the kWh, which is a fault in the
*collector*, not the circuit — and firing on it would train the owner to ignore
the digest, which is precisely how #180 stayed hidden for six days. A day that
does not clear :data:`COMPLETE_COVERAGE` is not compared; it is counted and
named as skipped, so "quiet" and "not looked at" never look alike.

**Too little history is silence, not a pass.** A circuit needs
:data:`MIN_BASELINE_DAYS` comparable days before a band means anything. Below
that it is reported as un-baselined rather than judged.

Dollars, not just kilowatt-hours
--------------------------------
Findings carry a cost, priced with :func:`energy_capture.tariff.marginal_rate` —
the all-in cost of one MORE kWh, taxes included. Not the average rate, which is
inflated by fixed daily charges and would overstate every finding.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from energy_capture import model, tariff as tariff_mod, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger

STAGE = "digest"
log = get_logger(STAGE)

#: Days of history the band is built from.
BASELINE_DAYS: Final[int] = 21

#: Fewer comparable days than this and no band is published for that circuit.
MIN_BASELINE_DAYS: Final[int] = 7

#: A day must have watched at least this fraction of its seconds to be compared.
COMPLETE_COVERAGE: Final[float] = 0.95

#: Band half-width in robust standard deviations (MAD * 1.4826).
BAND_SIGMAS: Final[float] = 4.0

#: No band finding below this many kWh of absolute change. A 40% jump on a
#: 0.05 kWh/day circuit is noise wearing a percentage.
MIN_ABSOLUTE_KWH: Final[float] = 0.5

#: Consistency factor from MAD to a normal-equivalent sigma.
_MAD_TO_SIGMA: Final[float] = 1.4826

# -- hard-rule thresholds ---------------------------------------------------

#: Above this outdoor low, resistance heat has no business running.
MILD_OUTDOOR_LOW_F: Final[float] = 45.0
#: ...and this much of it in a day is worth saying out loud.
STRIP_HEAT_KWH: Final[float] = 3.0

#: Fraction of a day's hours a circuit may draw before it is "never off".
STUCK_DUTY: Final[float] = 0.98
#: ...but only if it NORMALLY cycles. A circuit whose median day is already
#: above this is an always-on load (a fridge, a network rack), and alarming on
#: it every night is the fastest way to get the whole digest muted.
STUCK_NORMAL_DUTY: Final[float] = 0.90
#: Only for circuits that actually pull power; ignores always-on electronics.
STUCK_MIN_WATTS: Final[float] = 200.0
#: Span of the compressor's hourly-mean RPM over the day, above which a
#: variable-speed HVAC circuit counts as MODULATING rather than stuck. An
#: inverter compressor legitimately runs 24/7 in a heat wave, ramping its speed
#: to hold setpoint — observed 0–2700 rpm — so "drew all day" is not a fault for
#: it the way it is for a well pump. Only a compressor whose speed is PINNED
#: (span below this) is actually stuck. 300 rpm cleanly separates the two: real
#: modulation here spans thousands, a stuck unit would be flat.
STUCK_HVAC_MODULATION_RPM: Final[float] = 300.0

#: The barn is ~100% EV charging (STATE.md).
BARN_MIN_KWH: Final[float] = 3.6
BARN_MAX_KWH: Final[float] = 40.0

#: Consecutive days below :data:`BARN_MIN_KWH` before the quiet side of the
#: envelope says anything. ONE quiet day is a day nobody drove, which is
#: ordinary and must never page; four in a row, on a meter whose own history
#: shows it normally charges, is a charger that has stopped. The
#: history requirement is the same discipline that stopped ``stuck_load``
#: alarming on every refrigerator: the finding is the CHANGE, and only the
#: channel's own past can say what changed.
BARN_QUIET_DAYS: Final[int] = 4

#: Fraction of the baseline window that must be above the floor before "quiet"
#: is abnormal for this meter at all.
BARN_ACTIVE_FRACTION: Final[float] = 0.5

#: Overnight window used for the phantom-load floor, local hours.
NIGHT_HOURS: Final[tuple[int, int]] = (1, 5)
#: Growth in that floor worth reporting.
PHANTOM_GROWTH_W: Final[float] = 150.0

#: "Went quiet" only means something for a circuit that normally draws real
#: energy, and only for a real drop. The replay of 2026-10-10..11-24 fired it on
#: main-floor lights at 2.8 kWh against 3.6 — a quiet evening — and on a
#: kitchen circuit that "usually" uses 0.8 kWh and some days is not used at all.
QUIET_MIN_MEDIAN_KWH: Final[float] = 1.0
QUIET_MAX_FRACTION: Final[float] = 0.5

#: The service-feed CTs. They are each panel's TOTAL, so a band on them repeats
#: whatever the breakers beneath already said, and they are exactly the channels
#: that freeze (#180). The panel total is judged against the utility meter by
#: ``integrity`` instead.
FEED_CHANNELS: Final[frozenset[str]] = frozenset({"ct_1_a", "ct_1_b"})

#: Rules about the meters rather than the house; mirrors ``integrity.RULES``
#: (which imports this module, so the list cannot be imported from there).
INSTRUMENT_RULES: Final[frozenset[str]] = frozenset({
    "negative_reading", "corrupt_reading", "frozen_channel",
    "feed_below_children", "meter_disagreement",
})

# -- push policy ---------------------------------------------------------------

#: Pushover's own limit is 1,024 characters; stay under it by building the
#: message to fit rather than cutting it mid-line.
PUSH_MAX_CHARS: Final[int] = 1000

#: A finding that already paged stays quiet until it has been ABSENT this long.
#: Comparing against yesterday only (the old rule) re-paged every fault that
#: skipped a day — 8 of 9 mornings in November 2026.
REPEAT_AFTER_DAYS: Final[int] = 7

#: An instrument finding seen on this many of the last :data:`CHRONIC_WINDOW_DAYS`
#: is a KNOWN fault: it is summarised in one status line and never pages alone.
CHRONIC_WINDOW_DAYS: Final[int] = 14
CHRONIC_MIN_DAYS: Final[int] = 3

#: Days of per-day signature history kept in ``digest-state.json``.
STATE_HISTORY_DAYS: Final[int] = 30

__all__ = [
    "BASELINE_DAYS",
    "Band",
    "DigestReport",
    "Finding",
    "band_for",
    "build_report",
    "run",
]


@dataclass(frozen=True)
class Finding:
    """One thing worth a human's attention."""

    rule: str
    headline: str
    detail: str
    #: Energy the finding is about, where that is meaningful.
    kwh: float | None = None
    cost_usd: float | None = None
    #: A stable identity for the *subject* of the finding — the channel, device
    #: or fixed concern it is about — deliberately free of the day's varying
    #: numbers. The headline says "454.24 W for 8 hours" and changes every night;
    #: this does not, so :func:`signature` can recognise the same ongoing fault
    #: across days and the digest can stop re-paging for it. ``None`` falls back
    #: to the headline, which means "treat every occurrence as new".
    key: str | None = None
    #: The hub an INSTRUMENT finding is about, so a push can say "Panel B
    #: clamps" once instead of listing every channel of the same fault.
    device: str | None = None

    @property
    def instrument(self) -> bool:
        """About the meters themselves, rather than about the house."""
        return self.rule in INSTRUMENT_RULES

    def line(self) -> str:
        money = f"  (~${self.cost_usd:.2f})" if self.cost_usd else ""
        return f"• {self.headline}{money}\n  {self.detail}"

    def short(self) -> str:
        """One phone-sized line. The ``detail`` stays in the log and /healthz."""
        money = f" (+${self.cost_usd:.2f})" if self.cost_usd else ""
        return f"• {self.headline}{money}"

    def signature(self) -> str:
        """What makes two findings "the same issue" for cross-day de-duplication."""
        return f"{self.rule}::{self.key or self.headline}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "headline": self.headline,
            "detail": self.detail,
            "kwh": self.kwh,
            "cost_usd": self.cost_usd,
            "device": self.device,
            "key": self.key,
        }


@dataclass(frozen=True)
class Band:
    """A circuit's normal range, from its own recent history."""

    median: float
    low: float
    high: float
    days: int

    def contains(self, value: float) -> bool:
        return self.low <= value <= self.high


@dataclass
class DigestReport:
    local_day: date
    findings: list[Finding] = field(default_factory=list)
    compared: int = 0
    skipped_incomplete: list[str] = field(default_factory=list)
    skipped_unbaselined: list[str] = field(default_factory=list)
    #: Channels `check-channels` says cannot be trusted for this day. They are
    #: not compared against their band and — more importantly — this day does
    #: not enter their baseline, because a frozen channel would otherwise become
    #: the "normal" it is later judged against (DEVIATIONS #180).
    skipped_untrusted: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    def title(self) -> str:
        if self.ok:
            return f"energycap {self.local_day}: nothing unusual"
        n = len(self.findings)
        return f"energycap {self.local_day}: {n} thing{'s' if n != 1 else ''} to look at"

    def body(self) -> str:
        lines = [f.line() for f in self.findings]
        if not lines:
            lines = [f"All {self.compared} circuits inside their usual range."]
        tail = [f"({self.compared} circuits compared"]
        if self.skipped_incomplete:
            tail.append(f", {len(self.skipped_incomplete)} skipped for coverage")
        if self.skipped_unbaselined:
            tail.append(f", {len(self.skipped_unbaselined)} not yet baselined")
        if self.skipped_untrusted:
            tail.append(f", {len(self.skipped_untrusted)} not trusted")
        tail.append(")")
        lines.append("".join(tail))
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "local_day": self.local_day.isoformat(),
            "ok": self.ok,
            "findings": [f.to_dict() for f in self.findings],
            "compared": self.compared,
            "skipped_incomplete": self.skipped_incomplete,
            "skipped_unbaselined": self.skipped_unbaselined,
            "skipped_untrusted": self.skipped_untrusted,
            "notes": self.notes,
        }


# ------------------------------------------------------------------ the band


def band_for(history: Sequence[float]) -> Band | None:
    """A robust normal range from a circuit's own recent days.

    Median and MAD, not mean and standard deviation. One genuinely anomalous
    day inflates a standard deviation enough to swallow the next one — the
    failure mode where a fault that starts today makes tomorrow's identical
    fault look normal. The median absolute deviation does not move.

    ``None`` when there is too little history to say anything, which is
    reported as un-baselined rather than treated as a pass.
    """
    values = [float(v) for v in history]
    if len(values) < MIN_BASELINE_DAYS:
        return None
    median = statistics.median(values)
    mad = statistics.median([abs(v - median) for v in values])
    sigma = mad * _MAD_TO_SIGMA
    # A circuit that is genuinely identical every day has MAD 0. Give it a
    # floor from its own size so it does not fire on a rounding difference.
    spread = max(sigma * BAND_SIGMAS, MIN_ABSOLUTE_KWH, median * 0.15)
    return Band(median=median, low=max(0.0, median - spread), high=median + spread, days=len(values))


# ------------------------------------------------------------------- the SQL

#: Per channel per local day: energy, and how much of the day was watched.
#: ``observed_seconds`` (#190) is what makes the coverage gate possible at all —
#: without it a half-watched day is indistinguishable from a quiet one.
DAILY_KWH_SQL: Final[str] = """
SELECT
    h.source,
    h.device_id,
    h.channel_id,
    h.local_hour_start::DATE      AS local_day,
    sum(h.kwh)                    AS kwh,
    sum(h.observed_seconds)       AS observed_seconds,
    count(*)                      AS hours,
    max(h.mean)                   AS peak_mean_w,
    count(*) FILTER (WHERE h.mean >= ?) AS hours_drawing
FROM read_parquet(?, union_by_name := true) h
WHERE h.metric = 'watts'
  AND h.local_hour_start >= ?
  AND h.local_hour_start <  ?
GROUP BY 1, 2, 3, 4
"""

#: The overnight floor, per night, from the UTILITY METER: the quietest
#: 15-minute interval of the small hours, as watts.
#:
#: It used to read the panel feed CTs, and skipped itself whenever one was
#: untrusted — which, with Panel B's clamps (#180), was nearly every night, so
#: the one rule that finds "something left on" never ran. The meter is
#: independent of every clamp.
#:
#: Hours in which the heat pump's compressor turned are excluded: in a cold
#: snap it runs all night, which raises the floor without anything having been
#: left on. A night with no compressor-free interval yields no floor at all.
NIGHT_FLOOR_SQL: Final[str] = """
WITH m AS (
    SELECT ts_local, value
    FROM read_parquet(?)
    WHERE metric = 'kwh_interval' AND interval_s = 900 AND device_id = ?
      AND ts_local >= ? AND ts_local < ?
      AND hour(ts_local) BETWEEN ? AND ?
),
hvac AS (
    SELECT local_hour_start, max(max) AS compressor_max
    FROM read_parquet(?, union_by_name := true)
    WHERE source = 'bryant' AND metric = 'compressor_rpm'
      AND local_hour_start >= ? AND local_hour_start < ?
    GROUP BY 1
)
SELECT m.ts_local::DATE      AS local_day,
       min(m.value) * 4000.0 AS floor_w,
       count(*)              AS intervals
FROM m LEFT JOIN hvac ON hvac.local_hour_start = date_trunc('hour', m.ts_local)
WHERE coalesce(hvac.compressor_max, 0) = 0
GROUP BY 1
"""

#: Outdoor temperature per day, for the strip-heat rule.
OUTDOOR_SQL: Final[str] = """
SELECT
    local_hour_start::DATE AS local_day,
    min(min)               AS low_f,
    max(max)               AS high_f
FROM read_parquet(?, union_by_name := true)
WHERE metric = 'outdoor_temp_f'
  AND local_hour_start >= ? AND local_hour_start < ?
GROUP BY 1
"""

#: Bryant per-component daily energy.
BRYANT_DAILY_SQL: Final[str] = """
SELECT ts_local::DATE AS local_day, channel_id, sum(value) AS kwh
FROM read_parquet(?)
WHERE metric = 'kwh_day' AND ts_local >= ? AND ts_local < ?
GROUP BY 1, 2
"""

#: Meter energy per day for one interval series (never sum two — #169).
METER_DAILY_SQL: Final[str] = """
SELECT ts_local::DATE AS local_day, device_id, sum(value) AS kwh
FROM read_parquet(?)
WHERE metric = 'kwh_interval' AND interval_s = ?
  AND ts_local >= ? AND ts_local < ?
GROUP BY 1, 2
"""

#: The span of the HVAC compressor's hourly-mean RPM over a day — how much it
#: modulated. Read straight from the unit (Bryant), not inferred from the panel.
COMPRESSOR_RPM_SQL: Final[str] = """
SELECT max(mean) - min(mean) AS mean_span, count(*) AS hours
FROM read_parquet(?, union_by_name := true)
WHERE metric = 'compressor_rpm' AND source = 'bryant'
  AND local_hour_start >= ? AND local_hour_start < ?
"""


def _rows(con: Any, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, list(params))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def _expected_seconds(local_day: date) -> int:
    """Seconds in a LOCAL day — 23, 24 or 25 hours' worth."""
    return len(list(timeutil.iter_local_hours(local_day))) * 3600


def _compressor_modulated(
    con: Any, hourly: str, local_day: date, *, min_span: float
) -> bool | None:
    """Did the HVAC compressor vary its speed over ``local_day``?

    ``True`` it modulated (variable-speed unit riding out a load — not stuck),
    ``False`` its speed was pinned (a real stuck compressor looks like this), and
    ``None`` when there is no compressor telemetry to judge by — in which case the
    caller must not suppress, because "cannot tell" is not "fine".
    """
    lo = timeutil.local_midnight_naive(local_day)
    hi = timeutil.local_midnight_naive(local_day + timedelta(days=1))
    try:
        rows = _rows(con, COMPRESSOR_RPM_SQL, [hourly, lo, hi])
    except Exception as exc:  # noqa: BLE001 - a missing metric is not a crash
        log.warning("digest_compressor_rpm_unavailable", error=f"{type(exc).__name__}: {exc}")
        return None
    if not rows or not rows[0].get("hours"):
        return None
    span = rows[0].get("mean_span")
    if span is None:
        return None
    return float(span) >= min_span


def _label(labels: Mapping[tuple[str, str, str], Any], key: tuple[str, str, str]) -> str:
    """A human name for a channel — panel, breaker number, and circuit name.

    Delegates to the one naming authority (:func:`dim.channel_display`) so a
    digest line reads ``Panel B breaker 10 — Heat pump outdoor unit`` rather than
    ``breaker_p10``.
    """
    from energy_capture.stages import dim

    return dim.channel_display(key, labels.get(key))


# ----------------------------------------------------------------- the rules


def build_report(
    con: Any,
    *,
    hourly: str,
    daily: str,
    meter: str,
    local_day: date,
    labels: Mapping[tuple[str, str, str], Any] | None = None,
    baseline_days: int = BASELINE_DAYS,
    rate_usd: Decimal | float | None = None,
    barn_device: str | None = None,
    meter_device: str | None = None,
    untrusted: Sequence[tuple[str, str, str]] = (),
) -> DigestReport:
    """Everything the digest knows, for one local day. Pure apart from ``con``.

    ``untrusted`` comes from ``check-channels`` (``stages.integrity``) and is the
    one input that can silence a circuit entirely. A channel that reported the
    same value for hours has a kWh total that is arithmetic on a stuck reading;
    judging it against a band is meaningless, and letting it INTO a band is
    worse, because the fault becomes the baseline.
    """
    report = DigestReport(local_day=local_day)
    labels = labels or {}
    rate = float(rate_usd) if rate_usd is not None else None

    def cost(kwh: float | None) -> float | None:
        return round(kwh * rate, 2) if (kwh is not None and rate) else None

    untrusted_keys = set(untrusted)
    window_start = local_day - timedelta(days=baseline_days)
    lo = timeutil.local_midnight_naive(window_start)
    hi = timeutil.local_midnight_naive(local_day + timedelta(days=1))

    daily_rows = _rows(con, DAILY_KWH_SQL, [STUCK_MIN_WATTS, hourly, lo, hi])

    # -- group by circuit, splitting yesterday from its history --------------
    history: dict[tuple[str, str, str], list[float]] = {}
    duty_history: dict[tuple[str, str, str], list[float]] = {}
    today: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in daily_rows:
        key = (row["source"], row["device_id"], row["channel_id"])
        day = row["local_day"]
        kwh = float(row["kwh"] or 0.0)
        observed = int(row["observed_seconds"] or 0)
        complete = observed >= COMPLETE_COVERAGE * _expected_seconds(day)
        hours_ = int(row["hours"] or 0)
        duty = (int(row["hours_drawing"] or 0) / hours_) if hours_ else 0.0
        if key in untrusted_keys:
            # Excluded from BOTH sides: not judged today, and not admitted to
            # the history that will judge it tomorrow.
            continue
        if key[2] in FEED_CHANNELS:
            continue
        if day == local_day:
            today[key] = {**row, "kwh": kwh, "complete": complete}
        elif complete:
            duty_history.setdefault(key, []).append(duty)
            # Incomplete days are excluded from the BASELINE too: a band built
            # from half-watched days sits low, and then the first complete day
            # looks like a spike.
            history.setdefault(key, []).append(kwh)

    for key in sorted(untrusted_keys):
        if key[2] not in FEED_CHANNELS:
            report.skipped_untrusted.append(_label(labels, key))

    for key, row in sorted(today.items()):
        name = _label(labels, key)
        if not row["complete"]:
            report.skipped_incomplete.append(name)
            continue
        band = band_for(history.get(key, []))
        if band is None:
            report.skipped_unbaselined.append(name)
            continue
        report.compared += 1
        kwh = row["kwh"]

        if kwh > band.high and abs(kwh - band.median) >= MIN_ABSOLUTE_KWH:
            excess = kwh - band.median
            report.findings.append(
                Finding(
                    rule="above_band",
                    key=f"above_band:{'/'.join(key)}",
                    headline=f"{name} used {kwh:.1f} kWh, usually {band.median:.1f}",
                    detail=(
                        f"{excess:+.1f} kWh above its {band.days}-day median "
                        f"(normal range {band.low:.1f}–{band.high:.1f})."
                    ),
                    kwh=round(excess, 2),
                    cost_usd=cost(excess),
                )
            )
        elif (
            kwh < band.low
            and band.median >= QUIET_MIN_MEDIAN_KWH
            and kwh <= QUIET_MAX_FRACTION * band.median
        ):
            # The failure that looks like an improvement. A freezer that stops
            # running uses less power right up until the food spoils.
            report.findings.append(
                Finding(
                    rule="circuit_went_quiet",
                    key=f"circuit_went_quiet:{'/'.join(key)}",
                    headline=f"{name} used only {kwh:.1f} kWh, usually {band.median:.1f}",
                    detail=(
                        "A circuit drawing far less than it always has is a "
                        "failure that looks like a saving — a tripped breaker, "
                        "a stopped compressor, an appliance nobody has opened."
                    ),
                    kwh=round(kwh - band.median, 2),
                )
            )

        # "Never switched off" is only a fault if this circuit normally DOES
        # switch off. A fridge or a network rack sits at full duty every day of
        # its life; alarming on that nightly is the fastest way to get the whole
        # digest muted, and then the real one is missed too.
        hours = int(row["hours"] or 0)
        drawing = int(row["hours_drawing"] or 0)
        past_duty = duty_history.get(key, [])
        normally_cycles = bool(past_duty) and statistics.median(past_duty) < STUCK_NORMAL_DUTY
        # Breakers only. A CT measures a feeder, not a thermostatic load: the
        # HVAC subpanel CT draws all day whenever the thermostat's fan is set to
        # run continuously, which is a setting, not a stuck element — the replay
        # paged it as "never switched off" every fortnight. A strip heater that
        # really sticks on is tens of kWh and trips the cost band regardless.
        if (
            key[2].startswith("breaker_")
            and hours >= 20
            and drawing >= STUCK_DUTY * hours
            and normally_cycles
        ):
            # A variable-speed HVAC compressor legitimately runs 24/7 in a heat
            # wave, MODULATING its speed to hold setpoint — that is not stuck. We
            # collect the compressor's own RPM, so ask it directly: only a
            # compressor whose speed was PINNED is actually stuck. A non-HVAC
            # circuit, or one with no compressor telemetry, is judged as before.
            is_hvac = (labels.get(key) or {}).get("category") == "hvac"
            modulated = (
                _compressor_modulated(
                    con, hourly, local_day, min_span=STUCK_HVAC_MODULATION_RPM
                )
                if is_hvac
                else None
            )
            if modulated:
                report.notes.append(
                    f"{name} ran continuously but its compressor was modulating "
                    "(variable-speed HVAC holding setpoint through sustained "
                    "load) — normal operation, not a stuck load."
                )
            else:
                usual = statistics.median(past_duty)
                stuck_detail = (
                    f"Above {STUCK_MIN_WATTS:.0f} W in every hour, against a "
                    f"usual {usual:.0%} of the day. "
                ) + (
                    "Its compressor speed was pinned rather than modulating — a "
                    "variable-speed unit that stops varying is a stuck compressor "
                    "or a failed control."
                    if is_hvac
                    else "A thermostatic load that stops cycling is a stuck "
                    "element, a failed thermostat or a leak."
                )
                report.findings.append(
                    Finding(
                        rule="stuck_load",
                        key=f"stuck_load:{'/'.join(key)}",
                        headline=f"{name} never switched off ({drawing}/{hours} hours drawing)",
                        detail=stuck_detail,
                        kwh=round(kwh, 2),
                        cost_usd=cost(kwh),
                    )
                )

    _strip_heat_rule(con, report, daily=daily, hourly=hourly, local_day=local_day, cost=cost)
    _barn_rule(
        con,
        report,
        meter=meter,
        local_day=local_day,
        barn_device=barn_device,
        cost=cost,
        baseline_days=baseline_days,
    )
    _phantom_rule(
        con,
        report,
        meter=meter,
        meter_device=meter_device,
        hourly=hourly,
        local_day=local_day,
        lo=lo,
        hi=hi,
        cost=cost,
    )

    if not report.compared and not report.findings:
        report.notes.append(
            "No circuit had both a complete day and enough history to compare "
            "against. That is a statement about the archive, not about the house."
        )
    return report


def _strip_heat_rule(con, report, *, daily, hourly, local_day, cost) -> None:
    """Resistance heat on a mild day: the expensive one, and the best measured.

    ``eheat`` kWh/day and ``outdoor_temp_f`` have both been collected all along
    and nothing ever joined them.
    """
    lo = timeutil.local_midnight_naive(local_day)
    hi = timeutil.local_midnight_naive(local_day + timedelta(days=1))
    try:
        heat = {r["channel_id"]: float(r["kwh"] or 0.0) for r in _rows(
            con, BRYANT_DAILY_SQL, [daily, lo, hi]
        )}
        weather = _rows(con, OUTDOOR_SQL, [hourly, lo, hi])
    except Exception as exc:  # noqa: BLE001 - a missing dataset is not a crash
        log.warning("digest_strip_heat_unavailable", error=f"{type(exc).__name__}: {exc}")
        report.notes.append("strip-heat rule could not read its data")
        return

    # NO Bryant daily rows at all is the collector not having fetched the day
    # yet -- which was the normal state at the old 06:00 firing, since the fetch
    # is at 08:30. `heat.get("eheat", 0.0)` turned that into "eheat used 0 kWh"
    # and the rule returned silently: cardinal rule 1 violated inside the very
    # tool written to enforce it. An absent day is now SAID, not assumed away.
    if not heat:
        report.notes.append(
            f"strip-heat rule skipped: no Bryant daily rows for {local_day} yet"
        )
        return
    if not weather:
        report.notes.append("strip-heat rule skipped: no outdoor temperature for the day")
        return

    # An eheat key that is absent while OTHER components reported is Carrier
    # omitting a structurally disabled component -- a real zero, not a gap.
    eheat = heat.get("eheat", 0.0)
    if eheat < STRIP_HEAT_KWH:
        return
    low_f = weather[0].get("low_f")
    if low_f is None or float(low_f) <= MILD_OUTDOOR_LOW_F:
        return
    report.findings.append(
        Finding(
            rule="strip_heat_in_mild_weather",
            key="strip_heat_in_mild_weather:eheat",
            headline=f"Strip heat ran {eheat:.1f} kWh with an outdoor low of {float(low_f):.0f}°F",
            detail=(
                "Resistance heat costs roughly three times what the heat pump "
                "does for the same warmth, and above "
                f"{MILD_OUTDOOR_LOW_F:.0f}°F the pump should be carrying it "
                "alone. Check for a failed defrost, an outdoor sensor, or "
                "emergency heat left on."
            ),
            kwh=round(eheat, 2),
            cost_usd=cost(eheat),
        )
    )


def _barn_rule(con, report, *, meter, local_day, barn_device, cost, baseline_days) -> None:
    """The barn is ~100% EV charging and lives in a known envelope — both ends.

    Only the HIGH end was implemented; :data:`BARN_MIN_KWH` was dead code, which
    left "the EV silently stopped charging" — the failure a homeowner actually
    wants to hear about — with no check at all.

    The low end cannot be a simple threshold, because one quiet day is a day
    nobody drove and paging on that is how a channel gets muted. So it needs
    two things to be true at once: :data:`BARN_QUIET_DAYS` consecutive days below
    the floor, and a baseline showing this meter normally charges
    (:data:`BARN_ACTIVE_FRACTION`). New meters and genuinely idle ones say
    nothing.
    """
    if not barn_device:
        report.notes.append(
            "barn envelope skipped: no LG&E meter in the channel map is marked as the barn"
        )
        return
    lo = timeutil.local_midnight_naive(local_day)
    hi = timeutil.local_midnight_naive(local_day + timedelta(days=1))
    window_lo = timeutil.local_midnight_naive(local_day - timedelta(days=baseline_days))
    try:
        rows = _rows(con, METER_DAILY_SQL, [meter, 900, window_lo, hi])
    except Exception as exc:  # noqa: BLE001
        log.warning("digest_barn_unavailable", error=f"{type(exc).__name__}: {exc}")
        report.notes.append("barn envelope could not read the meter")
        return

    by_day = {
        r["local_day"]: float(r["kwh"] or 0.0)
        for r in rows
        if r["device_id"] == barn_device
    }
    today_kwh = by_day.get(local_day)
    if today_kwh is None:
        # The meter feed lags; at the old 06:00 firing this was the normal case
        # and the rule simply produced nothing. Absence is reported now.
        report.notes.append(f"barn envelope skipped: no meter intervals for {local_day} yet")
        return

    if today_kwh > BARN_MAX_KWH:
        report.findings.append(
            Finding(
                rule="barn_envelope",
                key="barn_envelope",
                headline=(
                    f"Barn used {today_kwh:.1f} kWh "
                    f"(envelope {BARN_MIN_KWH:.1f}–{BARN_MAX_KWH:.0f})"
                ),
                detail="Above anything a normal charging day has drawn.",
                kwh=round(today_kwh, 2),
                cost_usd=cost(today_kwh),
            )
        )
        return

    if today_kwh >= BARN_MIN_KWH:
        return

    history = {day: kwh for day, kwh in by_day.items() if day < local_day}
    if len(history) < MIN_BASELINE_DAYS:
        return
    active = sum(1 for kwh in history.values() if kwh >= BARN_MIN_KWH)
    if active < BARN_ACTIVE_FRACTION * len(history):
        return  # this meter is not habitually charging; quiet is its normal

    quiet = 0
    day = local_day
    while by_day.get(day, 0.0) < BARN_MIN_KWH and day in by_day:
        quiet += 1
        day -= timedelta(days=1)
    if quiet < BARN_QUIET_DAYS:
        return
    report.findings.append(
        Finding(
            rule="barn_envelope",
            key="barn_envelope",
            headline=f"Barn has drawn under {BARN_MIN_KWH:.1f} kWh for {quiet} days running",
            detail=(
                "This meter is ~100% EV charging and normally charges most days "
                f"({active} of the last {len(history)}). Several days at the floor "
                "is a charger that stopped, a breaker that tripped, or a car that "
                "is not plugging in — not a quiet week."
            ),
            kwh=round(today_kwh, 2),
        )
    )


def _phantom_rule(
    con, report, *, meter, meter_device, hourly, local_day, lo, hi, cost
) -> None:
    """The overnight floor, which is where a small always-on fault hides.

    A 200 W fault is 3% of a busy day and invisible in the total, but it is a
    third of the floor at 03:00 and 4.8 kWh every single day.
    """
    if not meter_device:
        report.notes.append(
            "overnight-floor check skipped: no primary meter in the channel map"
        )
        return
    try:
        rows = _rows(
            con,
            NIGHT_FLOOR_SQL,
            [meter, meter_device, lo, hi, NIGHT_HOURS[0], NIGHT_HOURS[1], hourly, lo, hi],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("digest_phantom_unavailable", error=f"{type(exc).__name__}: {exc}")
        report.notes.append("overnight-floor check could not read the meter")
        return
    floors = {r["local_day"]: float(r["floor_w"] or 0.0) for r in rows}
    tonight = floors.pop(local_day, None)
    if tonight is None:
        report.notes.append(
            f"overnight-floor check skipped: no compressor-free meter interval "
            f"for {local_day} (not yet published, or the heat pump ran all night)"
        )
        return
    if len(floors) < MIN_BASELINE_DAYS:
        return
    median = statistics.median(floors.values())
    if tonight - median < PHANTOM_GROWTH_W:
        return
    daily_kwh = (tonight - median) * 24 / 1000.0
    report.findings.append(
        Finding(
            rule="phantom_load_growth",
            key="phantom_load_growth:night_floor",
            headline=f"Overnight floor is {tonight:.0f} W, up from {median:.0f} W",
            detail=(
                f"A floor that rises stays risen: {daily_kwh:.1f} kWh every day "
                "until it is found. Look for something switched on and forgotten "
                "rather than something used. Read from the utility meter, with "
                "heat-pump hours excluded."
            ),
            kwh=round(daily_kwh, 2),
            cost_usd=cost(daily_kwh),
        )
    )


# ----------------------------------------------------- cross-day notify policy
#
# The digest runs once a day. A hardware fault that persists for weeks — a
# latched CT, say — is a finding every single night, and the first version of
# this policy only compared against the PREVIOUS night: a fault that skipped a
# day came back as "new" and paged again. Replayed over 2026-10-10..11-24 that
# pushed on 46 of 46 days, 41 of them past Pushover's length limit, with three
# quarters of every message about one known Panel B clamp fault. The owner
# stopped reading them, which is the failure this whole stage exists to prevent.
#
# So the policy now separates three things:
#
# * an ALERT pages: a house finding worth at least ``DIGEST_MIN_COST_USD`` a day
#   (or one with no price, like a circuit gone quiet), or an instrument problem
#   on a hub that has been clean — and only if it has not already paged and
#   stayed present since (:data:`REPEAT_AFTER_DAYS`);
# * a KNOWN instrument fault (:data:`CHRONIC_MIN_DAYS` of the last
#   :data:`CHRONIC_WINDOW_DAYS` on that hub) becomes one status line;
# * everything else rides along, below the alerts, if there is room.
#
# Nothing is dropped from the record: every finding is still logged with its
# full detail and published to /healthz.


@dataclass
class PushPlan:
    """What to send for one digest run, and why."""

    send: bool
    reason: str
    alerts: list[Finding] = field(default_factory=list)
    #: House findings not paging today: below the cost floor, or already paged.
    also: list[Finding] = field(default_factory=list)
    #: Hub -> days in the chronic window with an instrument finding (incl. today),
    #: for hubs whose instrument findings are known and not paging.
    known_instrument: dict[str, int] = field(default_factory=dict)
    signatures: list[str] = field(default_factory=list)
    instrument_devices: list[str] = field(default_factory=list)


def _read_digest_state(path: Path) -> dict[str, Any] | None:
    try:
        body = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        # No history means "everything is new", which sends — failing toward a
        # notification, never away from one.
        return None
    return body if isinstance(body, dict) else None


def _normalise_state(state: Mapping[str, Any] | None, *, local_day: date) -> dict[str, Any]:
    """Current-format state, upgrading the one-night ``signatures`` form.

    The old form remembered only last night's set. Reading it as "seen and
    paged yesterday" means the first run after this change does not re-page
    every ongoing fault at once.
    """
    if not state:
        return {"days": {}, "pushed": {}}
    if "days" in state:
        return {
            "days": dict(state.get("days") or {}),
            "pushed": dict(state.get("pushed") or {}),
            "notified_utc": state.get("notified_utc"),
        }
    yesterday = (local_day - timedelta(days=1)).isoformat()
    sigs = list(state.get("signatures") or [])
    return {
        "days": {yesterday: {"signatures": sigs, "instrument_devices": []}},
        "pushed": {sig: yesterday for sig in sigs},
        "notified_utc": state.get("notified_utc"),
    }


def _decide_push(
    report: DigestReport,
    state: Mapping[str, Any] | None,
    *,
    local_day: date,
    min_cost_usd: float,
    always_notify: bool = False,
    weekly: bool = False,
) -> PushPlan:
    """Sort tonight's findings into alerts, ride-alongs and known faults."""
    st = _normalise_state(state, local_day=local_day)
    prior: dict[date, dict[str, Any]] = {}
    for day_s, entry in st["days"].items():
        try:
            day = date.fromisoformat(day_s)
        except ValueError:
            continue
        if day < local_day:
            prior[day] = entry if isinstance(entry, dict) else {"signatures": list(entry)}
    window_start = local_day - timedelta(days=CHRONIC_WINDOW_DAYS)

    def last_seen(sig: str) -> date | None:
        days = [d for d, e in prior.items() if sig in (e.get("signatures") or [])]
        return max(days) if days else None

    def device_days(device: str) -> int:
        return sum(
            1 for d, e in prior.items()
            if d >= window_start and device in (e.get("instrument_devices") or [])
        )

    plan = PushPlan(send=False, reason="clear")
    plan.signatures = sorted({f.signature() for f in report.findings})
    plan.instrument_devices = sorted({f.device for f in report.findings if f.instrument and f.device})

    for f in report.findings:
        sig = f.signature()
        seen = last_seen(sig)
        repeat = (
            sig in st["pushed"]
            and seen is not None
            and (local_day - seen).days <= REPEAT_AFTER_DAYS
        )
        if f.instrument:
            if f.device and device_days(f.device) >= CHRONIC_MIN_DAYS:
                plan.known_instrument[f.device] = device_days(f.device) + 1
                continue
            if repeat:
                plan.also.append(f)
            else:
                plan.alerts.append(f)
            continue
        material = f.cost_usd is None or f.cost_usd >= min_cost_usd
        if material and not repeat:
            plan.alerts.append(f)
        else:
            plan.also.append(f)

    by_cost = lambda f: -(f.cost_usd or 0.0)  # noqa: E731
    plan.alerts.sort(key=lambda f: (f.instrument, by_cost(f)))
    plan.also.sort(key=by_cost)

    if weekly:
        plan.send, plan.reason = True, "weekly"
    elif plan.alerts:
        plan.send, plan.reason = True, "new"
    elif always_notify:
        plan.send, plan.reason = True, "always-notify"
    elif report.findings:
        plan.reason = "nothing-new"
    return plan


def _write_digest_state(
    path: Path,
    plan: PushPlan,
    *,
    local_day: date,
    previous: Mapping[str, Any] | None,
    now: Any,
    notified: bool,
) -> None:
    st = _normalise_state(previous, local_day=local_day)
    days = st["days"]
    days[local_day.isoformat()] = {
        "signatures": plan.signatures,
        "instrument_devices": plan.instrument_devices,
    }
    keep_from = (local_day - timedelta(days=STATE_HISTORY_DAYS)).isoformat()
    days = {d: e for d, e in days.items() if d >= keep_from}
    pushed = dict(st["pushed"])
    if notified:
        for f in plan.alerts:
            pushed[f.signature()] = local_day.isoformat()
    pushed = {s: d for s, d in pushed.items() if d >= keep_from}
    state = {
        "days": dict(sorted(days.items())),
        "pushed": dict(sorted(pushed.items())),
        "checked_utc": timeutil.format_utc(now),
        "notified_utc": timeutil.format_utc(now) if notified else st.get("notified_utc"),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2) + "\n")
    except OSError as exc:  # pragma: no cover - defensive
        log.warning("digest_state_unwritable", path=str(path), error=str(exc))


def _panel_of(device: str, labels: Mapping[tuple[str, str, str], Any]) -> str:
    for key, meta in labels.items():
        if key[1] == device and (meta or {}).get("panel"):
            return f"Panel {meta['panel']}"
    return f"hub {device[-4:]}"


def _fit(sections: Sequence[tuple[str | None, Sequence[str]]], limit: int) -> str:
    """Join titled sections of lines, dropping the LAST lines first to fit.

    Sections are in priority order, so what falls off is the least important —
    and it says so, instead of a sentence stopping mid-word.
    """
    flat: list[tuple[str | None, str]] = [
        (title, line) for title, lines in sections for line in lines
    ]

    def render(items: list[tuple[str | None, str]], dropped: int) -> str:
        out: list[str] = []
        current: object = object()
        for title, line in items:
            if title != current:
                if out:
                    out.append("")
                if title:
                    out.append(title)
                current = title
            out.append(line)
        if dropped:
            out.append(f"+{dropped} more in the log")
        return "\n".join(out)

    keep = len(flat)
    while keep and len(render(flat[:keep], len(flat) - keep)) > limit:
        keep -= 1
    return render(flat[:keep], len(flat) - keep)


def render_push(
    report: DigestReport,
    plan: PushPlan,
    *,
    labels: Mapping[tuple[str, str, str], Any] | None = None,
    weekly: WeeklySummary | None = None,
    limit: int = PUSH_MAX_CHARS,
) -> tuple[str, str]:
    """``(title, message)`` for the phone — short lines, always under ``limit``."""
    labels = labels or {}
    day = f"{report.local_day:%a %b} {report.local_day.day}"
    n = len(plan.alerts)
    if weekly is not None:
        title = weekly.title
    elif n:
        title = f"energycap {day}: {n} new thing{'s' if n != 1 else ''} to look at"
    else:
        title = f"energycap {day}: nothing new"

    sections: list[tuple[str | None, Sequence[str]]] = []
    if weekly is not None:
        sections.append((None, weekly.lines))
    if plan.alerts:
        sections.append(("New:" if weekly is not None else None, [f.short() for f in plan.alerts]))
    status = [
        f"• {_panel_of(dev, labels)} readings inconsistent {days} of the last "
        f"{CHRONIC_WINDOW_DAYS + 1} days (known fault, not paging)"
        for dev, days in sorted(plan.known_instrument.items())
    ]
    if status:
        sections.append(("Instruments:", status))
    if plan.also:
        sections.append(("Also noticed (not paging):", [f.short() for f in plan.also]))
    if not plan.alerts and weekly is None:
        sections.append((None, [f"All {report.compared} circuits inside their usual range."]
                         if not report.findings else []))
    return title, _fit(sections, limit)


# ------------------------------------------------------------- weekly summary
#
# Monday's push used to be a bare "nothing unusual" heartbeat: proof the digest
# was alive, and nothing else. It is now a summary of the week — the heartbeat
# still, but one worth opening.

#: Same-week-last-year is 364 days back, so the weekdays line up.
YEAR_AGO_DAYS: Final[int] = 364

#: Circuits must move at least this much week-on-week to be named.
WEEKLY_MOVER_MIN_KWH: Final[float] = 2.0

WEEK_METER_SQL: Final[str] = """
SELECT sum(value) AS kwh, count(*) AS intervals
FROM read_parquet(?)
WHERE metric = 'kwh_interval' AND interval_s = 900 AND device_id = ?
  AND ts_local >= ? AND ts_local < ?
"""

WEEK_CIRCUITS_SQL: Final[str] = """
SELECT source, device_id, channel_id,
       sum(kwh) FILTER (WHERE local_hour_start >= ?) AS this_week,
       sum(kwh) FILTER (WHERE local_hour_start <  ?) AS last_week
FROM read_parquet(?, union_by_name := true)
WHERE metric = 'watts' AND source = 'leviton'
  AND (channel_id LIKE 'breaker_%' OR channel_id LIKE 'ct_2%')
  AND local_hour_start >= ? AND local_hour_start < ?
GROUP BY 1, 2, 3
"""

WEEK_HVAC_SQL: Final[str] = """
SELECT channel_id, sum(value) AS kwh
FROM read_parquet(?)
WHERE metric = 'kwh_day' AND ts_local >= ? AND ts_local < ?
GROUP BY 1
"""

#: Bryant component -> the words a person uses for it.
_HVAC_NAMES: Final[dict[str, str]] = {
    "hpheat": "heat pump heating",
    "cooling": "cooling",
    "eheat": "strip heat",
    "fan": "fan",
    "reheat": "reheat",
}


@dataclass
class WeeklySummary:
    title: str
    lines: list[str]


def _pct(now: float, then: float | None) -> str:
    if not then:
        return "n/a"
    return f"{100.0 * (now - then) / then:+.0f}%"


def build_weekly(
    con: Any,
    *,
    hourly: str,
    daily: str,
    meter: str,
    meter_device: str | None,
    week_end: date,
    labels: Mapping[tuple[str, str, str], Any] | None = None,
    rate_usd: Decimal | float | None = None,
    instrument_days: Mapping[str, int] | None = None,
) -> WeeklySummary:
    """The seven local days ending ``week_end``, against the week before and a year ago."""
    labels = labels or {}
    rate = float(rate_usd) if rate_usd is not None else None
    start = week_end - timedelta(days=6)
    lo = timeutil.local_midnight_naive(start)
    hi = timeutil.local_midnight_naive(week_end + timedelta(days=1))
    prev_lo = timeutil.local_midnight_naive(start - timedelta(days=7))
    expected = sum(
        len(list(timeutil.iter_local_hours(start + timedelta(days=n)))) * 4 for n in range(7)
    )
    span = f"{start:%b} {start.day}–{week_end:%b} {week_end.day}"
    lines: list[str] = []
    title = f"energycap week {span}"

    def meter_kwh(a, b) -> tuple[float | None, int]:
        if not meter_device:
            return None, 0
        try:
            rows = _rows(con, WEEK_METER_SQL, [meter, meter_device, a, b])
        except Exception as exc:  # noqa: BLE001
            log.warning("digest_weekly_meter_unavailable", error=f"{type(exc).__name__}: {exc}")
            return None, 0
        if not rows or rows[0]["kwh"] is None:
            return None, 0
        return float(rows[0]["kwh"]), int(rows[0]["intervals"] or 0)

    kwh, intervals = meter_kwh(lo, hi)
    if kwh is None:
        lines.append("House: no meter data for the week yet.")
    else:
        partial = intervals < 0.95 * expected
        before, before_n = meter_kwh(prev_lo, lo)
        ago, ago_n = meter_kwh(
            timeutil.local_midnight_naive(start - timedelta(days=YEAR_AGO_DAYS)),
            timeutil.local_midnight_naive(week_end + timedelta(days=1 - YEAR_AGO_DAYS)),
        )
        money = f" (~${kwh * rate:.0f})" if rate else ""
        title = f"energycap week {span}: {kwh:.0f} kWh{money}"
        compare = [f"week before {_pct(kwh, before)}"] if before and before_n >= 0.95 * expected else []
        if ago and ago_n >= 0.95 * expected:
            compare.append(f"same week last year {_pct(kwh, ago)}")
        lines.append(
            f"House {kwh:.0f} kWh{money}"
            + (f" — {', '.join(compare)}" if compare else "")
            + (f" (meter {100 * intervals / expected:.0f}% published)" if partial else "")
        )

    try:
        hvac = {
            r["channel_id"]: float(r["kwh"] or 0.0)
            for r in _rows(con, WEEK_HVAC_SQL, [daily, lo, hi])
        }
    except Exception as exc:  # noqa: BLE001
        log.warning("digest_weekly_hvac_unavailable", error=f"{type(exc).__name__}: {exc}")
        hvac = {}
    parts = [
        f"{_HVAC_NAMES[c]} {hvac[c]:.0f}"
        for c in _HVAC_NAMES
        if hvac.get(c, 0.0) >= 0.5
    ]
    if parts:
        total = sum(hvac.get(c, 0.0) for c in _HVAC_NAMES)
        lines.append(f"HVAC {total:.0f} kWh: " + ", ".join(parts))

    try:
        circuits = _rows(con, WEEK_CIRCUITS_SQL, [lo, lo, hourly, prev_lo, hi])
    except Exception as exc:  # noqa: BLE001
        log.warning("digest_weekly_circuits_unavailable", error=f"{type(exc).__name__}: {exc}")
        circuits = []
    movers = []
    for r in circuits:
        now_, then_ = r["this_week"], r["last_week"]
        if now_ is None or then_ is None:
            continue
        delta = float(now_) - float(then_)
        if abs(delta) >= WEEKLY_MOVER_MIN_KWH:
            key = (r["source"], r["device_id"], r["channel_id"])
            meta = labels.get(key) or {}
            name = str(meta.get("short_label") or meta.get("label") or "").strip()
            movers.append((abs(delta), delta, name or _label(labels, key)))
    movers.sort(reverse=True)
    if movers:
        lines.append(
            "Biggest changes: "
            + ", ".join(f"{name} {delta:+.0f} kWh" for _, delta, name in movers[:3])
        )

    for dev, days in sorted((instrument_days or {}).items()):
        lines.append(f"{_panel_of(dev, labels)} readings inconsistent {days} of 7 days")
    return WeeklySummary(title=title, lines=lines)


def _instrument_days(
    state: Mapping[str, Any] | None, plan: PushPlan, *, local_day: date
) -> dict[str, int]:
    """Hub -> days of the last seven (tonight included) with an instrument finding."""
    st = _normalise_state(state, local_day=local_day)
    counts: dict[str, int] = {dev: 1 for dev in plan.instrument_devices}
    for day_s, entry in st["days"].items():
        try:
            day = date.fromisoformat(day_s)
        except ValueError:
            continue
        if local_day - timedelta(days=6) <= day < local_day and isinstance(entry, dict):
            for dev in entry.get("instrument_devices") or []:
                counts[dev] = counts.get(dev, 0) + 1
    return counts


# ------------------------------------------------------------------- the run


def run(
    *,
    local_day: date | None = None,
    bucket: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
    weekly: bool = False,
    map_path: Path | None = None,
) -> dict[str, Any]:
    """``energycap digest`` — yesterday by default, pushed to Pushover.

    A thin wrapper around :func:`_run` that exists only to give the digest **its
    own** ``status.json`` section, written on failure as well as on success.

    Without one, a digest that threw landed in the shared ``scheduler`` section
    — which ``watch-health`` deliberately does not read, because that counter is
    a convenience summary shared by six jobs. So a digest that had been crashing
    every morning for a fortnight was invisible to both ``/healthz`` and the
    watchdog, and looked exactly like a fortnight of quiet nights. The section is
    what lets a watch rule ask the only question that matters here: *did this
    run at all, recently?*
    """
    try:
        result = _run(
            local_day=local_day,
            bucket=bucket,
            notify=notify,
            always_notify=always_notify,
            weekly=weekly,
            map_path=map_path,
        )
    except Exception as exc:
        _record_status(failure=exc, local_day=local_day)
        raise
    _record_status(result=result)
    return result


def _record_status(
    *,
    result: Mapping[str, Any] | None = None,
    failure: BaseException | None = None,
    local_day: date | None = None,
) -> None:
    """Publish the run to ``status.json``. Never raises: this is instrumentation."""
    try:
        from energy_capture.health import get_status_store

        store = get_status_store()
        if failure is not None:
            store.record_failure(
                "digest",
                failure,
                local_day=local_day.isoformat() if local_day else None,
            )
            return
        body = dict(result or {})
        findings = body.get("findings") or []
        store.record_success(
            "digest",
            local_day=body.get("local_day"),
            ok=body.get("ok"),
            findings=len(findings),
            compared=body.get("compared"),
            skipped_incomplete=len(body.get("skipped_incomplete") or []),
            skipped_unbaselined=len(body.get("skipped_unbaselined") or []),
            skipped_untrusted=len(body.get("skipped_untrusted") or []),
            worst=findings[0]["headline"] if findings else None,
            notified=body.get("notified"),
        )
    except Exception as exc:  # noqa: BLE001 - a status write must not fail the digest
        log.warning("digest_status_unavailable", error=f"{type(exc).__name__}: {exc}")


def _run(
    *,
    local_day: date | None = None,
    bucket: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
    weekly: bool = False,
    map_path: Path | None = None,
) -> dict[str, Any]:
    """The digest proper. See :func:`run` for why the status wrapper is separate."""
    from energy_capture.aws import s3io
    from energy_capture.stages import compare, dim, integrity, rollup

    settings = get_settings()
    target_bucket = bucket or settings.require("s3_bucket")
    day = local_day or (timeutil.local_date_of(timeutil.now_utc()) - timedelta(days=1))

    labels: dict[tuple[str, str, str], Any] = {}
    entries: list[Any] = []
    try:
        entries = dim.load_channel_map(map_path or compare.DEFAULT_CHANNEL_MAP)
        # Resolve through the blackstart inventory so breakers carry their real
        # name and panel, not just the ones the map states outright.
        labels = dim.resolved_label_index(
            map_path or compare.DEFAULT_CHANNEL_MAP,
            settings.blackstart_inventory_path,
        )
    except Exception as exc:  # noqa: BLE001 - labels are a nicety, not the point
        log.warning("digest_labels_unavailable", error=f"{type(exc).__name__}: {exc}")

    # The house's marginal rate prices the findings; the barn is whichever LG&E
    # meter is NOT the primary. Both come from the same hand-maintained map, so
    # neither is a heuristic over the data.
    primary = compare.primary_meter_from_map(map_path)
    barn_device = next(
        (
            e.device_id
            for e in entries
            if e.source == model.SOURCE_LGE and e.device_id != primary and not e.placeholder
            and e.category == "meter" and "barn" in (e.short_label or "").lower()
        ),
        None,
    ) if labels else None

    rate: Decimal | None = None
    try:
        tariffs = tariff_mod.load_tariffs()
        if primary and primary in tariffs:
            rate = tariff_mod.marginal_rate(tariffs[primary], on=day)
    except Exception as exc:  # noqa: BLE001 - dollars are a garnish, not the point
        log.warning("digest_tariff_unavailable", error=f"{type(exc).__name__}: {exc}")

    con = rollup.connect(s3=True)
    hourly_glob = f"s3://{target_bucket}/{s3io.HOURLY_PREFIX}/*/*/rollup-*.parquet"
    meter_glob = f"s3://{target_bucket}/{s3io.METER_PREFIX}/*/*.parquet"
    try:
        # Instruments first. A frozen channel makes every consumption number
        # below it arithmetic on a stuck reading, so integrity is established
        # before anything is judged and its findings sort to the top.
        integrity_report = integrity.build_report(
            con,
            hourly=hourly_glob,
            meter=meter_glob,
            start=day,
            end=day,
            labels=labels,
            meter_device=primary,
        )
        report = build_report(
            con,
            hourly=hourly_glob,
            daily=f"s3://{target_bucket}/{s3io.DAILY_PREFIX}/*/*.parquet",
            meter=meter_glob,
            local_day=day,
            labels=labels,
            rate_usd=rate,
            barn_device=barn_device,
            meter_device=primary,
            untrusted=integrity_report.untrusted,
        )
        report.findings[:0] = integrity_report.findings
        report.notes.extend(integrity_report.notes)
        state_path = settings.spool_dir / "digest-state.json"
        previous_state = _read_digest_state(state_path)
        plan = _decide_push(
            report,
            previous_state,
            local_day=day,
            min_cost_usd=settings.digest_min_cost_usd,
            always_notify=always_notify,
            weekly=weekly,
        )
        summary: WeeklySummary | None = None
        if weekly:
            summary = build_weekly(
                con,
                hourly=hourly_glob,
                daily=f"s3://{target_bucket}/{s3io.DAILY_PREFIX}/*/*.parquet",
                meter=meter_glob,
                meter_device=primary,
                week_end=day,
                labels=labels,
                rate_usd=rate,
                instrument_days=_instrument_days(previous_state, plan, local_day=day),
            )
    finally:
        con.close()

    for finding in report.findings:
        log.warning("digest_finding", **finding.to_dict())
    log.info(
        "digest_done",
        local_day=day.isoformat(),
        findings=len(report.findings),
        integrity_findings=len(integrity_report.findings),
        compared=report.compared,
        skipped_incomplete=len(report.skipped_incomplete),
        skipped_unbaselined=len(report.skipped_unbaselined),
        skipped_untrusted=len(report.skipped_untrusted),
    )

    delivered: bool | None = None
    title, message = render_push(report, plan, labels=labels, weekly=summary)
    if notify and plan.send:
        from energy_capture import watch

        token = settings.pushover_token.get_secret_value()
        user = settings.pushover_user.get_secret_value()
        if token and user:
            delivered = watch.push_message(
                title=title,
                message=message,
                token=token,
                user=user,
            )
        else:
            delivered = False
            log.error(
                "pushover_not_configured",
                detail="findings were raised but PUSHOVER_* are unset",
            )

    log.info(
        "digest_notify",
        send=plan.send,
        reason=plan.reason,
        alerts=len(plan.alerts),
        also=len(plan.also),
        known_instrument=sorted(plan.known_instrument),
        chars=len(message),
        delivered=delivered,
    )
    # A push that was DUE and did not land must not be recorded as reported, or
    # the retry never happens — the same failing-open rule watch-state uses.
    undelivered = bool(notify and plan.send) and delivered is not True
    if not undelivered:
        _write_digest_state(
            state_path,
            plan,
            local_day=day,
            previous=previous_state,
            now=timeutil.now_utc(),
            notified=bool(delivered),
        )

    result = report.to_dict()
    result["notified"] = delivered
    result["notify_reason"] = plan.reason
    result["push"] = {"title": title, "message": message}
    result["marginal_rate_usd"] = float(rate) if rate else None
    result["integrity"] = integrity_report.to_dict()
    return result
