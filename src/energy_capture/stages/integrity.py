"""``energycap check-channels`` — can the instruments be trusted?

Every other check in this project asks *"did we observe it?"* — ``sample_count``,
``observed_seconds``, staleness, failure streaks. All of them pass while a CT
channel returns the same wrong number six hundred times in a row: the row is
present, the value is non-null, the coverage is complete and the number is
plausible. That is DEVIATIONS #180, and it went **six days** undetected before a
human happened to eye a total.

So this module asks the other question, and it is the only thing here that does.

The four checks
---------------

============================  =================================================
finding                        what it catches
============================  =================================================
``frozen_channel``             a channel reporting one value for consecutive
                               whole hours — ``min == max`` in the rollup, which
                               needs no new column and would have fired on
                               2026-08-18, day two of collection
``feed_below_children``        a panel's metered children drawing more than its
                               own feed clamp reports. Physically impossible,
                               and the only check that is INDEPENDENT of the
                               failure mode: it fires on a frozen channel, a
                               stuck-zero channel and a clamp reading low alike
``meter_disagreement``         the panels disagreeing with the utility meter by
                               more than tolerance over a whole day. The ONLY
                               check that sees a clamp reading live but SCALED
                               WRONG — a jaw not fully closed, a clamp round the
                               wrong conductor — because such a channel varies,
                               never freezes, and may stay above its children.
                               Run it after touching any clamp
``negative_reading``           a reversed clamp. Never yet observed in eight
                               days, so this one is unproven rather than
                               calibrated — but it is free, and a backwards
                               clamp is a realistic outcome of re-seating work
============================  =================================================

Why the thresholds are what they are
------------------------------------
Every default in :class:`~energy_capture.config.Settings` was calibrated against
eight days of two real hubs — one healthy, one faulty — and each separates them
with **no false positives**. ``docs/check-channels.md`` carries the tables. Two
are worth restating because they look arbitrary and are not:

* **Two consecutive frozen hours, not one.** The healthy hub froze for a single
  hour twice in eight days (both legs, 04:00, a real steady overnight load) and
  never twice running. The faulty hub produced runs of 2, 3 and 5 hours twelve
  times.
* **5% or 100 W of feed-versus-children tolerance.** Bare "children exceed feed
  at all" fires 18 times on the *healthy* hub, because clamp tolerance against
  the utility meter is ~3.4%. An alert that cries wolf gets muted, and then the
  real one is missed too — which is exactly how #180 survived.

What this module refuses to do
------------------------------
**It never repairs, interpolates, or filters a value out of the archive.** The
Leviton fault is upstream of every line of code here (#180): both transports
report the same wrong number, so there is nothing to fix on this side. Cardinal
rules 1 and 2 are unchanged — a gap stays a gap, and what the API said is what
gets stored. This module only reports, and it is the first stage in the project
that writes nothing at all.

Coverage discipline, inherited from ``digest``
---------------------------------------------
An hour below :attr:`Settings.integrity_min_samples` is **skipped and named**,
never counted as a pass: a half-watched hour has a narrow ``min..max`` for
reasons that have nothing to do with the instrument. A panel with too few
metered circuits reports "not enough coverage to judge" rather than passing. A
day with no overlapping meter data is skipped, not cleared.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final

from energy_capture import timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.stages.digest import Finding

STAGE = "integrity"
log = get_logger(STAGE)

#: Channel ids that are a panel's own service feed. A panel's children are
#: everything else it meters — breakers plus any subpanel CT pair.
FEED_CHANNELS: Final[tuple[str, ...]] = ("ct_1_a", "ct_1_b")

#: Prefix of a subpanel CT pair. These are CHILDREN of the feed, not siblings of
#: it: the HVAC subpanel feeder is downstream of the panel's own service feed.
SUBPANEL_PREFIX: Final[str] = "ct_2"

#: The freeze check applies to CT channels ONLY, and this is a calibration
#: result rather than a preference. Two things came out of running the check
#: against eight real days:
#:
#: * **Breakers report integer watts; CTs report floats.** A breaker on a small
#:   steady load (a porch light at 21 W, a mini-split idling) pins to the same
#:   integer for hours as a matter of course — the healthy hub produced frozen
#:   non-zero breaker runs on ``breaker_p23``, ``p17``, ``p18`` and ``p6``. A CT
#:   reading ``494.76`` and repeating that exact float for hours is a different
#:   kind of claim: the healthy hub's feed CTs averaged 55 distinct values an
#:   hour and produced **zero** runs of two.
#: * A breaker that really does stick is caught by ``feed_below_children``
#:   anyway, which needs no per-channel threshold at all.
CT_PREFIX: Final[str] = "ct_"

#: A panel needs at least this many distinct metered children before
#: feed-vs-children means anything. Below it, an excess is far more likely to be
#: an unmetered circuit than a fault, so the panel is reported as unjudgeable.
MIN_CHILDREN_FOR_FEED_CHECK: Final[int] = 3

#: Findings sort before ``digest``'s consumption findings. "Your CT is lying"
#: outranks "the dryer ran long", because every consumption number under it
#: depends on the instrument being right.
RULES: Final[tuple[str, ...]] = (
    "negative_reading",
    "frozen_channel",
    "feed_below_children",
    "meter_disagreement",
)


# --------------------------------------------------------------------- the SQL

#: Per channel per hour: is it pinned, and was the hour watched? ``min == max``
#: over a complete hour is the freeze signature and needs no new rollup column.
FROZEN_SQL: Final[str] = """
SELECT
    h.source,
    h.device_id,
    h.channel_id,
    h.local_hour_start,
    h.min                       AS lo,
    h.max                       AS hi,
    h.mean                      AS mean_w,
    h.sample_count              AS sample_count
FROM read_parquet(?, union_by_name := true) h
WHERE h.metric = 'watts'
  AND h.local_hour_start >= ? AND h.local_hour_start < ?
ORDER BY h.source, h.device_id, h.channel_id, h.local_hour_start
"""

#: A panel's feed against the sum of everything it meters beneath that feed.
#: ``series`` counts the distinct children so a barely-metered panel can be
#: reported as unjudgeable rather than silently passed.
FEED_SQL: Final[str] = """
WITH feed AS (
    SELECT device_id, local_hour_start,
           sum(mean) AS feed_w, min(sample_count) AS samples, count(*) AS legs
    FROM read_parquet(?, union_by_name := true)
    WHERE metric = 'watts' AND source = 'leviton' AND channel_id IN ('ct_1_a', 'ct_1_b')
      AND local_hour_start >= ? AND local_hour_start < ?
    GROUP BY 1, 2
),
kids AS (
    SELECT device_id, local_hour_start,
           sum(mean) AS children_w, count(*) AS series
    FROM read_parquet(?, union_by_name := true)
    WHERE metric = 'watts' AND source = 'leviton'
      AND (channel_id LIKE 'breaker_%' OR channel_id LIKE 'ct_2%')
      AND local_hour_start >= ? AND local_hour_start < ?
    GROUP BY 1, 2
)
SELECT f.device_id, f.local_hour_start, f.feed_w, f.samples, f.legs,
       k.children_w, k.series
FROM feed f JOIN kids k
  ON f.device_id = k.device_id AND f.local_hour_start = k.local_hour_start
ORDER BY f.device_id, f.local_hour_start
"""

#: Panel energy per local day, from the feed CTs only.
#:
#: ``series`` is load-bearing, not decoration. ``observed_seconds`` is summed
#: across every feed leg on every hub, so a fully watched day reports roughly
#: ``series * seconds_in_the_day`` — four times a day's length on this house.
#: Dividing by ``series`` is what turns that back into a coverage FRACTION. The
#: first version of this query omitted it, the gate could therefore never fire,
#: and the partial first day of collection (08-17, panels watched from 15:22
#: local) was reported as a -60.4% instrument fault. It was a coverage gap.
PANEL_DAILY_SQL: Final[str] = """
SELECT local_hour_start::DATE AS local_day,
       sum(kwh)               AS kwh,
       sum(observed_seconds)  AS observed_seconds,
       count(DISTINCT device_id || '/' || channel_id) AS series
FROM read_parquet(?, union_by_name := true)
WHERE metric = 'watts' AND source = 'leviton' AND channel_id IN ('ct_1_a', 'ct_1_b')
  AND local_hour_start >= ? AND local_hour_start < ?
GROUP BY 1
"""

#: Meter energy per local day for ONE interval series. Never sum two (#169):
#: every UsagePoint publishes the same energy as both 900s and 3600s.
#:
#: ``intervals`` is the meter-side coverage gate, and it is needed for exactly
#: the same reason the panel side is: **Green Button publishes on the UTILITY's
#: lag, not ours**, so the most recent day or two is routinely a partial day of
#: meter data sitting beside a complete day of panel data. Without this gate,
#: 2026-08-23 reported the panels as +59.3% ABOVE the meter — which was a
#: half-published meter day, not an instrument reading high.
METER_DAILY_SQL: Final[str] = """
SELECT ts_local::DATE AS local_day, sum(value) AS kwh, count(*) AS intervals
FROM read_parquet(?)
WHERE metric = 'kwh_interval' AND device_id = ? AND interval_s = ?
  AND ts_local >= ? AND ts_local < ?
GROUP BY 1
"""

#: Any negative reading at all, from any Leviton channel.
NEGATIVE_SQL: Final[str] = """
SELECT source, device_id, channel_id, metric, local_hour_start, min AS lo
FROM read_parquet(?, union_by_name := true)
WHERE source = 'leviton' AND min < 0
  AND local_hour_start >= ? AND local_hour_start < ?
ORDER BY min
"""


@dataclass
class IntegrityReport:
    """What the instruments look like over a local date range."""

    start: date
    end: date
    findings: list[Finding] = field(default_factory=list)
    #: Channels that produced at least one judged hour.
    channels_checked: int = 0
    hours_checked: int = 0
    #: Named, never silently passed.
    skipped_low_samples: list[str] = field(default_factory=list)
    skipped_unjudgeable: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    #: ``(source, device_id, channel_id)`` the caller must not trust for this
    #: range. ``digest`` excludes these from band comparison.
    untrusted: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    def title(self) -> str:
        span = (
            self.start.isoformat()
            if self.start == self.end
            else f"{self.start}..{self.end}"
        )
        if self.ok:
            return f"energycap {span}: instruments look sound"
        n = len(self.findings)
        return f"energycap {span}: {n} instrument problem{'s' if n != 1 else ''}"

    def body(self) -> str:
        lines = [f.line() for f in self.findings]
        if not lines:
            lines = [
                f"{self.channels_checked} channels over {self.hours_checked} "
                "hours: nothing frozen, no panel drawing more than its feed, "
                "no negative reading."
            ]
        tail = [f"({self.hours_checked} channel-hours checked"]
        if self.skipped_low_samples:
            tail.append(f", {len(self.skipped_low_samples)} skipped for coverage")
        if self.skipped_unjudgeable:
            tail.append(f", {len(self.skipped_unjudgeable)} unjudgeable")
        tail.append(")")
        lines.append("".join(tail))
        for note in self.notes:
            lines.append(note)
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "ok": self.ok,
            "findings": [f.to_dict() for f in self.findings],
            "channels_checked": self.channels_checked,
            "hours_checked": self.hours_checked,
            "skipped_low_samples": self.skipped_low_samples,
            "skipped_unjudgeable": self.skipped_unjudgeable,
            "untrusted": ["/".join(k) for k in self.untrusted],
            "notes": self.notes,
            "counts": {
                rule: sum(1 for f in self.findings if f.rule == rule) for rule in RULES
            },
        }


def _rows(con: Any, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    cur = con.execute(sql, list(params))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def _label(labels: Mapping[tuple[str, str, str], Any], key: tuple[str, str, str]) -> str:
    meta = labels.get(key)
    if isinstance(meta, Mapping):
        for name in ("short_label", "label"):
            value = meta.get(name)
            if isinstance(value, str) and value.strip():
                return value
    return f"{key[1][-4:]}/{key[2]}"


def frozen_runs(
    hours: Sequence[Mapping[str, Any]], *, min_samples: int
) -> list[tuple[int, list[Mapping[str, Any]]]]:
    """Maximal runs of CONSECUTIVE frozen hours in one channel's series.

    A frozen hour is ``min == max`` at a **non-zero** value, with enough samples
    to mean anything. Hours that fail the sample gate break a run rather than
    extending it: two frozen hours either side of an unwatched one are not
    evidence of three, because nobody watched the middle.

    **Pinned at exactly zero is not frozen — it is off.** A water heater idle
    for two hours reports ``0.0`` unchanging, and that is the truth; calling it
    a stuck instrument was the first false positive this check produced against
    real data (``breaker_p19``, 340 such hours on the healthy hub). The
    stuck-at-zero failure mode is real but belongs to ``feed_below_children``,
    which catches it without having to guess whether a zero is honest.

    Runs of length **one** are returned. They used to be dropped here, which
    silently made :data:`Settings.integrity_frozen_min_hours` unable to mean
    what it says: the setting accepts ``1``, and at ``1`` nothing changed,
    because a single pinned hour never became a run in the first place.

    A one-hour run is a real signal for a CT, and this filters to CTs. An
    analog clamp under load does not report one value 120 times — the healthy
    hub's feed produced **82 distinct values** in the hour its faulty twin
    produced one. Whether one hour is enough to *act* on is the caller's
    judgement, which is what ``frozen_min`` is for; it is not this function's
    business to decide by discarding the evidence.
    """
    runs: list[tuple[int, list[Mapping[str, Any]]]] = []
    current: list[Mapping[str, Any]] = []
    previous: Any = None
    for row in hours:
        samples = int(row.get("sample_count") or 0)
        lo, hi = row.get("lo"), row.get("hi")
        pinned = samples >= min_samples and lo is not None and lo == hi and lo != 0
        contiguous = previous is None or (
            row["local_hour_start"] - previous == timedelta(hours=1)
        )
        if pinned and (not current or contiguous):
            current.append(row)
        else:
            if current:
                runs.append((len(current), current))
            current = [row] if pinned else []
        previous = row["local_hour_start"]
    if current:
        runs.append((len(current), current))
    return runs


def build_report(
    con: Any,
    *,
    hourly: str,
    meter: str | None = None,
    start: date,
    end: date,
    labels: Mapping[tuple[str, str, str], Any] | None = None,
    meter_device: str | None = None,
    meter_interval_s: int | None = None,
    frozen_min_hours: int | None = None,
    feed_excess_pct: float | None = None,
    feed_excess_min_w: float | None = None,
    meter_disagree_pct: float | None = None,
    min_samples: int | None = None,
) -> IntegrityReport:
    """All four checks over ``start``..``end`` inclusive. Pure apart from ``con``."""
    settings = get_settings()
    labels = labels or {}
    frozen_min = (
        frozen_min_hours
        if frozen_min_hours is not None
        else settings.integrity_frozen_min_hours
    )
    excess_pct = (
        feed_excess_pct
        if feed_excess_pct is not None
        else settings.integrity_feed_excess_pct
    )
    excess_min_w = (
        feed_excess_min_w
        if feed_excess_min_w is not None
        else settings.integrity_feed_excess_min_w
    )
    disagree_pct = (
        meter_disagree_pct
        if meter_disagree_pct is not None
        else settings.integrity_meter_disagree_pct
    )
    samples_floor = (
        min_samples if min_samples is not None else settings.integrity_min_samples
    )

    report = IntegrityReport(start=start, end=end)
    lo = timeutil.local_midnight_naive(start)
    hi = timeutil.local_midnight_naive(end + timedelta(days=1))

    _negative_check(con, report, hourly=hourly, lo=lo, hi=hi, labels=labels)
    _frozen_check(
        con,
        report,
        hourly=hourly,
        lo=lo,
        hi=hi,
        labels=labels,
        frozen_min=frozen_min,
        samples_floor=samples_floor,
    )
    _feed_check(
        con,
        report,
        hourly=hourly,
        lo=lo,
        hi=hi,
        labels=labels,
        excess_pct=excess_pct,
        excess_min_w=excess_min_w,
        samples_floor=samples_floor,
    )
    if meter:
        _meter_check(
            con,
            report,
            hourly=hourly,
            meter=meter,
            lo=lo,
            hi=hi,
            start=start,
            end=end,
            meter_device=meter_device,
            meter_interval_s=meter_interval_s,
            disagree_pct=disagree_pct,
        )
    else:
        report.notes.append(
            "No meter data supplied, so a clamp reading live but scaled wrong "
            "would not be detected — that is the only check which can see one."
        )

    report.findings.sort(key=lambda f: RULES.index(f.rule) if f.rule in RULES else 99)
    if not report.hours_checked:
        report.notes.append(
            "No hour had enough samples to judge. That is a statement about the "
            "archive, not about the instruments."
        )
    return report


# ------------------------------------------------------------------- the rules


def _negative_check(con, report, *, hourly, lo, hi, labels) -> None:
    """A reversed clamp. Unproven — never observed — and free to check."""
    for row in _rows(con, NEGATIVE_SQL, [hourly, lo, hi]):
        key = (row["source"], row["device_id"], row["channel_id"])
        name = _label(labels, key)
        report.findings.append(
            Finding(
                rule="negative_reading",
                key=f"negative_reading:{'/'.join(key)}",
                headline=f"{name} reported {row['lo']:.1f} — a negative reading",
                detail=(
                    f"metric {row['metric']} at "
                    f"{row['local_hour_start']}. A clamp fitted backwards reads "
                    "negative; no Leviton channel here had ever done this before, "
                    "so treat it as a wiring change rather than a known fault."
                ),
            )
        )
        if key not in report.untrusted:
            report.untrusted.append(key)


def _frozen_check(
    con, report, *, hourly, lo, hi, labels, frozen_min, samples_floor
) -> None:
    """``min == max`` for consecutive whole hours: the channel stopped updating."""
    rows = _rows(con, FROZEN_SQL, [hourly, lo, hi])
    by_channel: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        key = (row["source"], row["device_id"], row["channel_id"])
        by_channel.setdefault(key, []).append(row)

    for key, hours in sorted(by_channel.items()):
        name = _label(labels, key)
        if not key[2].startswith(CT_PREFIX):
            # Integer-quantised breaker channels pin on steady loads as a matter
            # of course; see CT_PREFIX. Their faults surface through
            # feed_below_children instead.
            continue
        judged = [h for h in hours if int(h.get("sample_count") or 0) >= samples_floor]
        if not judged:
            report.skipped_low_samples.append(name)
            continue
        report.channels_checked += 1
        report.hours_checked += len(judged)
        for length, run in frozen_runs(hours, min_samples=samples_floor):
            if length < frozen_min:
                continue
            value = run[0]["mean_w"]
            first = run[0]["local_hour_start"]
            last = run[-1]["local_hour_start"]
            span = (
                f"the hour of {first}"
                if length == 1
                else f"{length} consecutive hours"
            )
            window = f"{first}" if length == 1 else f"{first} through {last}"
            report.findings.append(
                Finding(
                    rule="frozen_channel",
                    key=f"frozen_channel:{'/'.join(key)}",
                    headline=f"{name} reported exactly {value:.2f} W for {span}",
                    detail=(
                        f"{window}, {len(run) * 120} samples, one value. A "
                        "channel that stops updating keeps full sample_count "
                        "and a plausible number, so nothing else here notices "
                        "(DEVIATIONS #180). A healthy CT under load does not do "
                        "this: the good hub's feed produced 82 distinct values "
                        "in the hour its faulty twin produced one. Compare it "
                        "against another channel on the same device before "
                        "believing it."
                    ),
                )
            )
            if key not in report.untrusted:
                report.untrusted.append(key)


def _feed_check(
    con, report, *, hourly, lo, hi, labels, excess_pct, excess_min_w, samples_floor
) -> None:
    """A panel's children cannot outdraw its feed — everything passes through it.

    One under-reading feed CT trips this every hour it is loaded, and the old
    code emitted a finding PER HOUR — a single stuck clamp filled a nightly
    digest with five or nine near-identical lines. The fault is the clamp, not
    each hour, so offending hours are now collapsed into one finding per feed per
    LOCAL DAY: the worst hour is named, the rest are counted. Nothing is hidden —
    the count and the worst excess still say how bad and how persistent it was —
    and the same clamp no longer pages once an hour.
    """
    rows = _rows(con, FEED_SQL, [hourly, lo, hi, hourly, lo, hi])
    thin: set[str] = set()
    # (device, local_day) -> the offending hours under it.
    offences: dict[tuple[str, date], list[dict[str, Any]]] = {}
    for row in rows:
        device = row["device_id"]
        if int(row.get("samples") or 0) < samples_floor:
            continue
        if int(row.get("series") or 0) < MIN_CHILDREN_FOR_FEED_CHECK:
            # Too few metered circuits: an excess is far more likely to be an
            # unmetered load than a fault. Say so once, do not pass silently.
            thin.add(device)
            continue
        feed_w = float(row["feed_w"] or 0.0)
        children_w = float(row["children_w"] or 0.0)
        excess = children_w - feed_w
        tolerance = max(excess_pct / 100.0 * feed_w, excess_min_w)
        if excess <= tolerance:
            continue
        hour = row["local_hour_start"]
        day = hour.date() if hasattr(hour, "date") else hour
        offences.setdefault((device, day), []).append(
            {
                "hour": hour,
                "feed_w": feed_w,
                "children_w": children_w,
                "excess": excess,
                "tolerance": tolerance,
            }
        )

    for (device, day), hours in sorted(offences.items()):
        worst = max(hours, key=lambda h: h["excess"])
        n = len(hours)
        key = ("leviton", device, "ct_1_a")
        report.findings.append(
            Finding(
                rule="feed_below_children",
                key=f"feed_below_children:{device}",
                headline=(
                    f"{device[-4:]} feed read less than its own circuits in "
                    f"{n} hour{'s' if n != 1 else ''} on {day}"
                ),
                detail=(
                    f"Worst at {worst['hour']}: the feed clamp reported "
                    f"{worst['feed_w']:.0f} W while {worst['children_w']:.0f} W "
                    f"flowed through it downstream ({worst['excess']:+.0f} W over a "
                    f"{worst['tolerance']:.0f} W tolerance). Every one of those "
                    "circuits passes through that clamp, so this cannot be true — "
                    "the feed CT is under-reporting or has stopped updating. "
                    "Independent of which failure mode is at work."
                ),
            )
        )
        if key not in report.untrusted:
            report.untrusted.append(key)
    for device in sorted(thin):
        report.skipped_unjudgeable.append(
            f"{device[-4:]} feed vs children (fewer than "
            f"{MIN_CHILDREN_FOR_FEED_CHECK} metered circuits)"
        )


def _meter_check(
    con,
    report,
    *,
    hourly,
    meter,
    lo,
    hi,
    start,
    end,
    meter_device,
    meter_interval_s,
    disagree_pct,
) -> None:
    """Panels against the utility meter: the only check that sees a scale error."""
    if not meter_device:
        report.notes.append(
            "No primary meter in the channel map, so panel-vs-meter was not "
            "checked. dim_channel.is_primary is how the house meter is named; "
            "this refuses to guess between the house and the barn (#178)."
        )
        return
    interval = meter_interval_s or 3600
    try:
        meter_rows = _rows(
            con, METER_DAILY_SQL, [meter, meter_device, interval, lo, hi]
        )
    except Exception as exc:  # noqa: BLE001 - a missing meter file is not a fault
        report.notes.append(
            f"Meter data unreadable ({type(exc).__name__}), so panel-vs-meter "
            "was skipped rather than passed."
        )
        return
    panel_rows = _rows(con, PANEL_DAILY_SQL, [hourly, lo, hi])
    panel_by_day = {r["local_day"]: r for r in panel_rows}
    meter_by_day = {r["local_day"]: r for r in meter_rows}

    for day in timeutil.iter_local_dates(start, end):
        p, m = panel_by_day.get(day), meter_by_day.get(day)
        if not p or not m:
            report.skipped_unjudgeable.append(f"{day} panel-vs-meter (no overlap)")
            continue
        # A day the collector half watched has roughly half the kWh. Firing on
        # that reports a collector gap as an instrument fault, which is the
        # single fastest way to make this whole command ignorable.
        series = int(p.get("series") or 0)
        if not series:
            report.skipped_unjudgeable.append(f"{day} panel-vs-meter (no feed series)")
            continue
        expected = len(list(timeutil.iter_local_hours(day))) * 3600 * series
        observed = int(p.get("observed_seconds") or 0)
        coverage = observed / expected if expected else 0.0
        if coverage < 0.9:
            report.skipped_low_samples.append(
                f"{day} panel-vs-meter ({100 * coverage:.0f}% watched)"
            )
            continue
        # The meter needs the same gate as the panels, for a different reason:
        # LG&E publishes on its own lag, so the newest day or two is habitually
        # a PARTIAL meter day beside a complete panel day.
        hours_in_day = len(list(timeutil.iter_local_hours(day)))
        expected_intervals = int(hours_in_day * 3600 / interval)
        got_intervals = int(m.get("intervals") or 0)
        if got_intervals < 0.9 * expected_intervals:
            report.skipped_low_samples.append(
                f"{day} panel-vs-meter (meter published "
                f"{got_intervals}/{expected_intervals} intervals)"
            )
            continue
        meter_kwh = float(m["kwh"] or 0.0)
        panel_kwh = float(p["kwh"] or 0.0)
        if not meter_kwh:
            report.skipped_unjudgeable.append(f"{day} panel-vs-meter (meter 0 kWh)")
            continue
        diff_pct = 100.0 * (panel_kwh - meter_kwh) / meter_kwh
        if abs(diff_pct) <= disagree_pct:
            continue
        report.findings.append(
            Finding(
                rule="meter_disagreement",
                key=f"meter_disagreement:{meter_device or 'panels'}",
                headline=(
                    f"{day}: panels {panel_kwh:.1f} kWh against the meter's "
                    f"{meter_kwh:.1f} kWh ({diff_pct:+.1f}%)"
                ),
                detail=(
                    f"Beyond the {disagree_pct:.0f}% threshold on a day watched "
                    f"{100 * coverage:.0f}% of the way through. Clean "
                    "days here run within a few percent; a persistent one-sided "
                    "gap is a clamp reading live but scaled wrong — not fully "
                    "closed, or around the wrong conductor. This is the only "
                    "check that can see that."
                ),
                kwh=round(panel_kwh - meter_kwh, 2),
            )
        )


def run(
    *,
    start: date | None = None,
    end: date | None = None,
    bucket: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
    map_path: Path | None = None,
    frozen_min_hours: int | None = None,
) -> dict[str, Any]:
    """``energycap check-channels`` — read-only, and pushed to Pushover.

    The wrapper exists so the ``integrity`` status section is written when the
    check **fails**, not only when it succeeds. A section that appears only on
    success cannot be distinguished from a section that has never been written,
    and both read as "no verdict" — which is the shape of every bug this project
    keeps rediscovering.
    """
    try:
        result = _run(
            start=start,
            end=end,
            bucket=bucket,
            notify=notify,
            always_notify=always_notify,
            map_path=map_path,
            frozen_min_hours=frozen_min_hours,
        )
    except Exception as exc:
        try:
            from energy_capture.health import get_status_store

            get_status_store().record_failure("integrity", exc)
        except Exception as status_exc:  # noqa: BLE001 - instrumentation only
            log.warning(
                "integrity_status_unavailable",
                error=f"{type(status_exc).__name__}: {status_exc}",
            )
        raise
    return result


def _run(
    *,
    start: date | None = None,
    end: date | None = None,
    bucket: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
    map_path: Path | None = None,
    frozen_min_hours: int | None = None,
) -> dict[str, Any]:
    """The check proper. See :func:`run` for why the status wrapper is separate."""
    from energy_capture.aws import s3io
    from energy_capture.stages import compare, dim, rollup

    settings = get_settings()
    target_bucket = bucket or settings.require("s3_bucket")
    last = end or (timeutil.local_date_of(timeutil.now_utc()) - timedelta(days=1))
    first = start or last

    labels: dict[tuple[str, str, str], Any] = {}
    try:
        entries = dim.load_channel_map(map_path or compare.DEFAULT_CHANNEL_MAP)
        labels = {e.key: {"label": e.label, "short_label": e.short_label} for e in entries}
    except Exception as exc:  # noqa: BLE001 - labels are a nicety, not the point
        log.warning("integrity_labels_unavailable", error=f"{type(exc).__name__}: {exc}")

    primary = None
    try:
        primary = compare.primary_meter_from_map(map_path)
    except Exception as exc:  # noqa: BLE001 - reported as a note, not a pass
        log.warning("integrity_meter_unknown", error=f"{type(exc).__name__}: {exc}")

    con = rollup.connect(s3=True)
    try:
        report = build_report(
            con,
            hourly=f"s3://{target_bucket}/{s3io.HOURLY_PREFIX}/*/*/rollup-*.parquet",
            meter=f"s3://{target_bucket}/{s3io.METER_PREFIX}/*/*.parquet",
            start=first,
            end=last,
            labels=labels,
            meter_device=primary,
            frozen_min_hours=frozen_min_hours,
        )
    finally:
        con.close()

    for finding in report.findings:
        log.warning("integrity_finding", **finding.to_dict())
    log.info(
        "integrity_done",
        start=first.isoformat(),
        end=last.isoformat(),
        findings=len(report.findings),
        channels_checked=report.channels_checked,
        hours_checked=report.hours_checked,
        **report.to_dict()["counts"],
    )

    delivered: bool | None = None
    if notify and (report.findings or always_notify):
        from energy_capture import watch

        token = settings.pushover_token.get_secret_value()
        user = settings.pushover_user.get_secret_value()
        if token and user:
            delivered = watch.push_message(
                title=report.title(),
                message=report.body(),
                token=token,
                user=user,
            )
        else:
            delivered = False
            log.error(
                "pushover_not_configured",
                detail="integrity findings were raised but PUSHOVER_* are unset",
            )

    # Publish to /healthz so the instrument verdict is pullable, not only
    # pushed. Missing must read as failure rather than a pass -- watch-health's
    # rule, and the bug it exists to stop repeating.
    try:
        from energy_capture.health import get_status_store

        counts = report.to_dict()["counts"]
        get_status_store().record_success(
            "integrity",
            start=first.isoformat(),
            end=last.isoformat(),
            ok=report.ok,
            findings=len(report.findings),
            channels_checked=report.channels_checked,
            hours_checked=report.hours_checked,
            worst=report.findings[0].headline if report.findings else None,
            **counts,
        )
    except Exception as exc:  # noqa: BLE001 - a status write must not fail the check
        log.warning("integrity_status_unavailable", error=f"{type(exc).__name__}: {exc}")

    result = report.to_dict()
    result["notified"] = delivered
    return result
