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

    def line(self) -> str:
        money = f"  (~${self.cost_usd:.2f})" if self.cost_usd else ""
        return f"• {self.headline}{money}\n  {self.detail}"

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

#: The overnight floor: the quietest hour of the small hours, per day.
NIGHT_FLOOR_SQL: Final[str] = """
SELECT
    h.local_hour_start::DATE AS local_day,
    min(nightly.total_w)     AS floor_w
FROM (
    SELECT
        local_hour_start,
        sum(mean) AS total_w
    FROM read_parquet(?, union_by_name := true)
    WHERE metric = 'watts'
      AND channel_id IN ('ct_1_a', 'ct_1_b')
      AND hour(local_hour_start) BETWEEN ? AND ?
    GROUP BY local_hour_start
) nightly
JOIN read_parquet(?, union_by_name := true) h
  ON h.local_hour_start = nightly.local_hour_start
WHERE h.local_hour_start >= ? AND h.local_hour_start < ?
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


def _rows(con: Any, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, list(params))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def _expected_seconds(local_day: date) -> int:
    """Seconds in a LOCAL day — 23, 24 or 25 hours' worth."""
    return len(list(timeutil.iter_local_hours(local_day))) * 3600


def _label(labels: Mapping[tuple[str, str, str], Any], key: tuple[str, str, str]) -> str:
    meta = labels.get(key)
    if isinstance(meta, Mapping):
        for field_name in ("short_label", "label"):
            value = meta.get(field_name)
            if isinstance(value, str) and value.strip():
                return value
    return f"{key[2]}"


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
        if day == local_day:
            today[key] = {**row, "kwh": kwh, "complete": complete}
        elif complete:
            duty_history.setdefault(key, []).append(duty)
            # Incomplete days are excluded from the BASELINE too: a band built
            # from half-watched days sits low, and then the first complete day
            # looks like a spike.
            history.setdefault(key, []).append(kwh)

    for key in sorted(untrusted_keys):
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
        elif kwh < band.low and abs(band.median - kwh) >= MIN_ABSOLUTE_KWH:
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
        if hours >= 20 and drawing >= STUCK_DUTY * hours and normally_cycles:
            usual = statistics.median(past_duty)
            report.findings.append(
                Finding(
                    rule="stuck_load",
                    key=f"stuck_load:{'/'.join(key)}",
                    headline=f"{name} never switched off ({drawing}/{hours} hours drawing)",
                    detail=(
                        f"Above {STUCK_MIN_WATTS:.0f} W in every hour, against a "
                        f"usual {usual:.0%} of the day. A thermostatic load that "
                        "stops cycling is a stuck element, a failed thermostat "
                        "or a leak."
                    ),
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
    # The overnight-floor rule reads the feed CTs directly, and those are
    # exactly the channels that freeze (#180). A stuck feed gives a perfectly
    # flat night floor, which this rule would read as a clean baseline — so an
    # untrusted feed silences it rather than being quietly believed.
    if any(key[2].startswith("ct_1") for key in untrusted_keys):
        report.notes.append(
            "Overnight-floor check skipped: a service-feed channel is untrusted "
            "for this day, and that rule reads the feed CTs directly."
        )
    else:
        _phantom_rule(con, report, hourly=hourly, local_day=local_day, lo=lo, hi=hi)

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


def _phantom_rule(con, report, *, hourly, local_day, lo, hi) -> None:
    """The overnight floor, which is where a small always-on fault hides.

    A 200 W fault is 3% of a busy day and invisible in the total, but it is a
    third of the floor at 03:00 and 4.8 kWh every single day.
    """
    try:
        rows = _rows(
            con, NIGHT_FLOOR_SQL, [hourly, NIGHT_HOURS[0], NIGHT_HOURS[1], hourly, lo, hi]
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("digest_phantom_unavailable", error=f"{type(exc).__name__}: {exc}")
        return
    floors = {r["local_day"]: float(r["floor_w"] or 0.0) for r in rows}
    tonight = floors.pop(local_day, None)
    if tonight is None or len(floors) < MIN_BASELINE_DAYS:
        return
    history = sorted(floors.values())
    median = statistics.median(history)
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
                "rather than something used."
            ),
            kwh=round(daily_kwh, 2),
        )
    )


# ----------------------------------------------------- cross-day notify policy
#
# The digest runs once a day and used to push whenever it found ANYTHING. A
# hardware fault that persists for weeks — a latched CT, say — is a finding every
# single night, so the owner got the same message over and over and learned to
# ignore it, which is exactly how a real new problem then gets missed. So the
# digest now remembers the SIGNATURES it reported last time (the stable
# per-subject identity, not the headline with its nightly numbers) and pushes
# only when something is NEW. Ongoing faults are still in the body and still on
# /healthz; they just stop re-paging on their own.


def _read_digest_state(path: Path) -> dict[str, Any] | None:
    try:
        body = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        # No history means "everything is new", which sends — failing toward a
        # notification, never away from one.
        return None
    return body if isinstance(body, dict) else None


def _decide_push(
    report: DigestReport,
    previous: Mapping[str, Any] | None,
    *,
    always_notify: bool,
) -> tuple[bool, str, list[str], list[str]]:
    """Return ``(send, reason, current_signatures, new_signatures)``.

    Send when a finding appears that was not in the last report; stay quiet when
    every current finding is one already reported. A finding that CLEARED is not
    itself a reason to push — the digest never announced all-clears — but it does
    drop out of the remembered set so its return would page again.
    """
    current = sorted({f.signature() for f in report.findings})
    known = set(previous.get("signatures", [])) if previous else set()
    new = [sig for sig in current if sig not in known]
    if always_notify:
        return True, "always-notify", current, new
    if new:
        return True, ("changed" if known else "first"), current, new
    if current:
        return False, "unchanged", current, new
    return False, "clear", current, new


def _write_digest_state(
    path: Path, signatures: Sequence[str], *, now: Any, notified: bool
) -> None:
    previous = _read_digest_state(path) or {}
    state = {
        "signatures": list(signatures),
        "checked_utc": timeutil.format_utc(now),
        "notified_utc": timeutil.format_utc(now)
        if notified
        else previous.get("notified_utc"),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2) + "\n")
    except OSError as exc:  # pragma: no cover - defensive
        log.warning("digest_state_unwritable", path=str(path), error=str(exc))


# ------------------------------------------------------------------- the run


def run(
    *,
    local_day: date | None = None,
    bucket: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
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
        labels = {
            e.key: {"label": e.label, "short_label": e.short_label} for e in entries
        }
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
            untrusted=integrity_report.untrusted,
        )
    finally:
        con.close()

    report.findings[:0] = integrity_report.findings
    report.notes.extend(integrity_report.notes)

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

    state_path = settings.spool_dir / "digest-state.json"
    previous_state = _read_digest_state(state_path)
    send, reason, current_sigs, new_sigs = _decide_push(
        report, previous_state, always_notify=always_notify
    )

    delivered: bool | None = None
    if notify and send:
        from energy_capture import watch

        token = settings.pushover_token.get_secret_value()
        user = settings.pushover_user.get_secret_value()
        if token and user:
            body = report.body()
            ongoing = len(current_sigs) - len(new_sigs)
            if new_sigs and ongoing:
                body = f"{len(new_sigs)} new, {ongoing} continuing from earlier.\n\n" + body
            delivered = watch.push_message(
                title=report.title(),
                message=body,
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
        send=send,
        reason=reason,
        new=len(new_sigs),
        ongoing=len(current_sigs) - len(new_sigs),
        delivered=delivered,
    )
    # A push that was DUE and did not land must not be recorded as reported, or
    # the retry never happens — the same failing-open rule watch-state uses.
    undelivered = bool(notify and send) and delivered is not True
    if not undelivered:
        _write_digest_state(
            state_path, current_sigs, now=timeutil.now_utc(), notified=bool(delivered)
        )

    result = report.to_dict()
    result["notified"] = delivered
    result["notify_reason"] = reason
    result["marginal_rate_usd"] = float(rate) if rate else None
    result["integrity"] = integrity_report.to_dict()
    return result
