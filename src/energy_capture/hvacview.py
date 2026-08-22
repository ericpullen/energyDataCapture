"""``/ui/hvac`` — what the Bryant cloud says the HVAC is doing, against what the panel measures.

Why this screen exists
----------------------
Two independent clouds describe one machine. Bryant/Carrier reports the system's
*intent and state* every 30s — mode, compressor capacity percent, blower RPM,
airflow — and Leviton reports the *electricity* every 30s from the panel: the
compressor's own breaker and the CT pair on the HVAC subpanel feeder. Nothing
joins them automatically, and nothing on ``/ui`` puts them side by side. This
does, because the interesting question is not "what is the HVAC doing" but
"**do the two accounts of it agree**".

Three things make that comparison less obvious than it sounds.

**The two sources never share a ``ts_utc``.** Each source stamps its own cycle
(``new_cycle(ts_utc=now_utc())``), so a Bryant row and a Leviton row from the
same half-minute differ by however long the two poll loops happen to be apart.
Joining on equality returns *nothing* — measured: 0 rows out of 722. So every
comparison here is **bucket-aligned**, never key-joined, and the bucket width is
reported alongside the numbers.

**Absence is a reading.** Bryant simply omits ``stage_pct`` while the compressor
is off, and cardinal rule 1 means the pipeline writes no row rather than a zero.
That absence is therefore *information* — it is the cloud saying "not running" —
and this module reports it as its own state rather than as a gap. The same is
not true of a missing Leviton row, which really is a gap.

**Which channels are HVAC is a mapping question, not a code question.** The
channels are selected by ``category == "hvac"`` in ``channel_map.json``, so
installing a second HVAC circuit means editing the map, not this file. Within
that set, ``breaker_*`` channels are equipment circuits and ``ct_*`` channels are
feeders (PLAN.md §6.5's naming), which is the only split this module infers.

What it deliberately does NOT show
---------------------------------
**Bryant's own kWh.** ``fetch-daily`` writes day-grain energy to
``energy/daily`` in S3, never to the spool (rule 6 — day-grain rows would poison
the hourly rollup), and no bucket is configured yet, so **not one Bryant energy
row exists anywhere**. Rather than draw an empty chart, the payload carries an
explicit ``bryant_energy.available = false`` with the reason, because "we cannot
compare kWh yet, and here is exactly why" is the honest answer.

Nothing here raises. Every failure returns a block carrying ``error``.
"""

from __future__ import annotations

import math
import sqlite3
import statistics
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, Final

from energy_capture import model, timeutil

__all__ = [
    "BRYANT_ENERGY_UNAVAILABLE",
    "DEFAULT_WINDOW_S",
    "HVAC_CATEGORY",
    "MAX_WINDOW_S",
    "MIN_WINDOW_S",
    "WINDOW_PRESETS_S",
    "hvac_comparison",
]

#: The category in ``channel_map.json`` that puts a panel channel on this screen.
HVAC_CATEGORY: Final[str] = "hvac"

#: Bryant status metrics this screen reads. ``stage_pct`` is the important one:
#: the outdoor unit's capacity percentage, which is what should track watts.
CAPACITY_METRIC: Final[str] = "stage_pct"
BRYANT_METRICS: Final[tuple[str, ...]] = (
    CAPACITY_METRIC,
    "blower_rpm",
    "cfm",
    "mode",
    "outdoor_temp_f",
    "indoor_temp_f",
    "humidity_pct",
    "setpoint_cool_f",
    "setpoint_heat_f",
)

#: Window defaults. Six hours because that is long enough to hold several
#: capacity changes and short enough to keep 30s buckets meaningful.
DEFAULT_WINDOW_S: Final[int] = 6 * 3600
MIN_WINDOW_S: Final[int] = 300
MAX_WINDOW_S: Final[int] = 7 * 86400
WINDOW_PRESETS_S: Final[tuple[int, ...]] = (3600, 6 * 3600, 86400, 3 * 86400, 7 * 86400)

#: At most this many buckets in a response, so a week-long window stays small.
MAX_BUCKETS: Final[int] = 720

BRYANT_ENERGY_UNAVAILABLE: Final[str] = (
    "Bryant's own kWh cannot be compared yet. `fetch-daily` writes day-grain "
    "energy to energy/daily in S3 and never to the spool (day-grain rows would "
    "poison the hourly rollup), and no S3 bucket is configured — so no Bryant "
    "energy row exists anywhere to compare against. The panel-side kWh below is "
    "observed-time only: mean watts x samples x poll interval, never "
    "extrapolated across a gap."
)

_ALIGNMENT_NOTE: Final[str] = (
    "The two clouds are polled by independent loops and never share a ts_utc, so "
    "nothing here is key-joined: both sides are averaged into the same time "
    "bucket and compared bucket by bucket."
)

#: Rows for one window. Both predicates are indexed by ts_utc (fixed-width ISO
#: text, so string order IS chronological order).
_WINDOW_SQL: Final[str] = """
SELECT ts_utc, source, device_id, channel_id, metric, value
FROM observations
WHERE ts_utc >= ? AND ts_utc < ?
  AND (
      (source = ? AND metric = ?)
      OR (source = ? AND metric IN ({bryant}))
  )
ORDER BY ts_utc
"""


# --------------------------------------------------------------- small helpers


def parse_stored_utc(text: Any) -> datetime | None:
    """Parse a spool ``ts_utc`` (fixed-width ISO-8601 with a ``Z``); ``None`` if not one."""
    if isinstance(text, datetime):
        return timeutil.ensure_utc(text)
    if not isinstance(text, str) or not text.strip():
        return None
    raw = text.strip()
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        return timeutil.ensure_utc(datetime.fromisoformat(raw))
    except ValueError:
        return None


def _pearson(pairs: Sequence[tuple[float, float]]) -> float | None:
    """Pearson r, or ``None`` when it would be meaningless (n < 3, or no spread)."""
    if len(pairs) < 3:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mx = statistics.fmean(xs)
    my = statistics.fmean(ys)
    numerator = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0.0 or dy == 0.0:
        return None
    return round(numerator / (dx * dy), 4)


def _mean(values: Iterable[float]) -> float | None:
    seq = [v for v in values if v is not None]
    return round(statistics.fmean(seq), 3) if seq else None


def bucket_width_s(window_s: int) -> int:
    """The narrowest round bucket that keeps the response under :data:`MAX_BUCKETS`.

    Never narrower than the poll interval: a 30s cadence cannot fill a 10s
    bucket, and a chart of mostly-empty buckets reads as an outage.
    """
    for candidate in (30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400):
        if window_s / candidate <= MAX_BUCKETS:
            return candidate
    return 14400


def parse_window_s(raw: Any) -> tuple[int, bool]:
    """``(window_s, clamped)``. Anything unparseable falls back to the default.

    A bad query string must not 400 this screen: it is a diagnostic page, and
    showing the default window beats showing an error.
    """
    if raw is None or raw == "":
        return DEFAULT_WINDOW_S, False
    try:
        requested = int(float(raw))
    except (TypeError, ValueError):
        return DEFAULT_WINDOW_S, False
    window = min(MAX_WINDOW_S, max(MIN_WINDOW_S, requested))
    return window, window != requested


# ----------------------------------------------------------- channel selection


def _hvac_channels(
    labels: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """``(equipment, feeders)`` — the panel channels this screen compares.

    Selected by ``category == "hvac"`` on a Leviton channel, so a new HVAC
    circuit reaches this screen through ``channel_map.json`` rather than through
    a code change. The split is PLAN.md §6.5's own naming: a ``breaker_*``
    channel is the equipment's circuit, a ``ct_*`` channel is a clamp on a
    feeder — which is a real distinction here, because the feeder carries
    whatever else lives in that subpanel.
    """
    equipment: list[dict[str, Any]] = []
    feeders: list[dict[str, Any]] = []
    for key, meta in sorted(labels.items()):
        source, device_id, channel_id = key
        if source != model.SOURCE_LEVITON:
            continue
        if (meta.get("category") or "").strip().lower() != HVAC_CATEGORY:
            continue
        entry = {
            "key": f"{source}/{device_id}/{channel_id}",
            "source": source,
            "device_id": device_id,
            "channel_id": channel_id,
            "label": meta.get("label") or channel_id,
            "short_label": meta.get("short_label") or meta.get("label") or channel_id,
            "blackstart_device_id": meta.get("blackstart_device_id"),
            "panel": meta.get("panel"),
            "slots": meta.get("slots"),
        }
        (equipment if channel_id.startswith("breaker_") else feeders).append(entry)
    return equipment, feeders


# ------------------------------------------------------------------ the block


def hvac_comparison(
    conn: sqlite3.Connection | None,
    labels: Mapping[tuple[str, str, str], Mapping[str, Any]],
    *,
    now: datetime,
    window_s: int = DEFAULT_WINDOW_S,
    clamped: bool = False,
    poll_interval_s: int = 30,
    bryant_interval_s: int = 30,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    """The whole ``/ui/hvac/data`` payload. Reads only; never raises for data."""
    problems: list[str] = errors if errors is not None else []
    equipment, feeders = _hvac_channels(labels)
    bucket_s = bucket_width_s(window_s)
    end = timeutil.ensure_utc(now)
    start = end - timedelta(seconds=window_s)

    block: dict[str, Any] = {
        "window": {
            "start": _stamp(start),
            "end": _stamp(end),
            "window_s": window_s,
            "clamped": clamped,
            "bucket_s": bucket_s,
            "presets_s": list(WINDOW_PRESETS_S),
            "min_window_s": MIN_WINDOW_S,
            "max_window_s": MAX_WINDOW_S,
        },
        "alignment_note": _ALIGNMENT_NOTE,
        "capacity_metric": CAPACITY_METRIC,
        "equipment": equipment,
        "feeders": feeders,
        "buckets": [],
        "agreement": None,
        "off_state": None,
        "feeder_profile": None,
        "energy": None,
        "bryant_energy": {"available": False, "reason": BRYANT_ENERGY_UNAVAILABLE},
        "present": False,
        "reason": None,
    }

    if not equipment and not feeders:
        block["reason"] = (
            "no Leviton channel is categorised 'hvac' in channel_map.json, so "
            "there is nothing to compare Bryant against"
        )
        return block
    if conn is None:
        block["reason"] = "the spool could not be opened"
        return block

    watt_keys = {(e["device_id"], e["channel_id"]): e for e in equipment}
    feeder_keys = {(f["device_id"], f["channel_id"]): f for f in feeders}
    try:
        rows = conn.execute(
            _WINDOW_SQL.format(bryant=",".join("?" * len(BRYANT_METRICS))),
            (
                timeutil.format_utc(start),
                timeutil.format_utc(end),
                model.SOURCE_LEVITON,
                "watts",
                model.SOURCE_BRYANT,
                *BRYANT_METRICS,
            ),
        ).fetchall()
    except Exception as exc:  # pragma: no cover - defensive
        problems.append(f"hvac window read failed: {type(exc).__name__}: {exc}")
        block["reason"] = "the spool query failed"
        return block

    # ---- fold every row into its bucket -----------------------------------
    buckets: dict[datetime, dict[str, Any]] = {}

    def slot(ts_text: str) -> datetime | None:
        """Floor one stored ts_utc onto the bucket grid, or drop it.

        Bucketing on the UTC epoch rather than on the local wall clock is
        deliberate: ts_utc is canonical (rule 3), and a DST fall-back day would
        otherwise fold two different hours into one bucket.
        """
        parsed = parse_stored_utc(ts_text)
        if parsed is None:
            return None
        epoch = int(parsed.timestamp())
        return datetime.fromtimestamp(epoch - (epoch % bucket_s), tz=timeutil.UTC)

    for ts_text, source, device_id, channel_id, metric, value in rows:
        if value is None:
            continue
        at = slot(ts_text)
        if at is None:
            continue
        bucket = buckets.setdefault(
            at, {"equipment_w": [], "feeder_w": {}, "bryant": {}}
        )
        if source == model.SOURCE_BRYANT:
            bucket["bryant"].setdefault(metric, []).append(float(value))
        elif (device_id, channel_id) in watt_keys:
            bucket["equipment_w"].append(float(value))
        elif (device_id, channel_id) in feeder_keys:
            # Feeder legs SUM (each leg is its own conductor), so they are kept
            # per channel and summed after averaging — averaging first and
            # summing after is the only order that survives a missing leg.
            bucket["feeder_w"].setdefault(channel_id, []).append(float(value))

    expected_leviton = max(1, round(bucket_s / max(1, poll_interval_s)))
    expected_bryant = max(1, round(bucket_s / max(1, bryant_interval_s)))

    series: list[dict[str, Any]] = []
    for at in sorted(buckets):
        raw = buckets[at]
        equip_samples = len(raw["equipment_w"])
        feeder_means = {c: _mean(v) for c, v in raw["feeder_w"].items()}
        feeder_total = (
            round(sum(v for v in feeder_means.values() if v is not None), 3)
            if feeder_means
            else None
        )
        feeder_samples = max((len(v) for v in raw["feeder_w"].values()), default=0)
        bryant = raw["bryant"]
        capacity = _mean(bryant.get(CAPACITY_METRIC, []))
        series.append(
            {
                "at": _stamp(at),
                "equipment_w": _mean(raw["equipment_w"]),
                "equipment_samples": equip_samples,
                "feeder_w": feeder_total,
                "feeder_legs": feeder_means,
                "feeder_samples": feeder_samples,
                "capacity_pct": capacity,
                # An absent capacity is Bryant SAYING the compressor is off, not
                # a gap — but only if the status poller reported at all.
                "capacity_present": capacity is not None,
                "bryant_samples": len(bryant.get("mode", [])),
                "blower_rpm": _mean(bryant.get("blower_rpm", [])),
                "cfm": _mean(bryant.get("cfm", [])),
                "outdoor_temp_f": _mean(bryant.get("outdoor_temp_f", [])),
                "indoor_temp_f": _mean(bryant.get("indoor_temp_f", [])),
                "coverage": {
                    "equipment_pct": round(100.0 * equip_samples / expected_leviton, 1),
                    "feeder_pct": round(100.0 * feeder_samples / expected_leviton, 1),
                    "bryant_pct": round(
                        100.0 * len(bryant.get("mode", [])) / expected_bryant, 1
                    ),
                },
            }
        )

    block["buckets"] = series
    block["present"] = bool(series)
    if not series:
        block["reason"] = "no rows in this window"
        return block

    block["expected_per_bucket"] = {
        "leviton": expected_leviton,
        "bryant": expected_bryant,
    }
    block["agreement"] = _agreement(series)
    block["off_state"] = _off_state(series)
    block["feeder_profile"] = _feeder_profile(series)
    block["energy"] = _energy(series, bucket_s, poll_interval_s)
    block["latest"] = series[-1]
    return block


def _agreement(series: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Does the compressor's measured power track Bryant's reported capacity?

    The headline is ``watts_per_point``: a variable-speed compressor's draw
    should be very nearly proportional to the capacity percentage the control
    reports, so a tight spread here means the two clouds are describing the same
    machine, and ``implied_full_load_w`` extrapolates to a nameplate-checkable
    figure. Reported with ``n`` always visible, because on a young channel the
    correlation is the less interesting half of the answer.
    """
    pairs = [
        (float(b["capacity_pct"]), float(b["equipment_w"]))
        for b in series
        if b["capacity_pct"] is not None and b["equipment_w"] is not None
    ]
    running = [(c, w) for c, w in pairs if c > 0]
    ratios = [w / c for c, w in running]
    bands: dict[int, list[float]] = {}
    for capacity, watts in running:
        bands.setdefault(int(round(capacity / 5.0) * 5), []).append(watts)
    return {
        "n": len(pairs),
        "r": _pearson(pairs),
        "watts_per_point": round(statistics.fmean(ratios), 2) if ratios else None,
        "watts_per_point_sd": (
            round(statistics.pstdev(ratios), 2) if len(ratios) > 1 else None
        ),
        "implied_full_load_w": (
            round(statistics.fmean(ratios) * 100.0) if ratios else None
        ),
        "bands": [
            {
                "capacity_pct": band,
                "n": len(values),
                "mean_w": round(statistics.fmean(values), 1),
                "min_w": round(min(values), 1),
                "max_w": round(max(values), 1),
            }
            for band, values in sorted(bands.items())
        ],
    }


def _off_state(series: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Bryant omits capacity when the compressor is off. Does the panel agree?

    This is the cleanest test on the screen and it needs no correlation: when the
    field is absent the breaker should read zero, and when it is present the
    breaker should not. ``disagreements`` counts buckets that break the rule, so
    the answer is a count and not an adjective.
    """
    absent = [
        b for b in series if not b["capacity_present"] and b["equipment_w"] is not None
    ]
    present = [
        b for b in series if b["capacity_present"] and b["equipment_w"] is not None
    ]
    # A breaker reading a few watts on an idle 240V circuit is instrumentation
    # noise, not a running compressor; anything above this is a real disagreement.
    threshold_w = 25.0
    return {
        "threshold_w": threshold_w,
        "absent_buckets": len(absent),
        "absent_max_w": round(max((b["equipment_w"] for b in absent), default=0.0), 1),
        "absent_mean_w": _mean(b["equipment_w"] for b in absent),
        "present_buckets": len(present),
        "present_min_w": (
            round(min(b["equipment_w"] for b in present), 1) if present else None
        ),
        "present_mean_w": _mean(b["equipment_w"] for b in present),
        "disagreements": sum(1 for b in absent if b["equipment_w"] > threshold_w)
        + sum(1 for b in present if b["equipment_w"] <= threshold_w),
    }


def _feeder_profile(series: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """What the HVAC subpanel feeder is actually carrying.

    The clamps were installed on the assumption that the subpanel is the electric
    heat kit, so in cooling season they should read ~0. Testing that is a matter
    of asking what the feeder's watts correlate with: the compressor's capacity,
    or the blower. Fan power goes as the cube of speed, so ``r_vs_rpm_cubed``
    being the strongest of the three is the fingerprint of a blower rather than a
    resistive element — and it is measured here rather than assumed either way.
    """
    rows = [b for b in series if b["feeder_w"] is not None]
    rpm = [(float(b["blower_rpm"]), float(b["feeder_w"])) for b in rows if b["blower_rpm"] is not None]
    cfm = [(float(b["cfm"]), float(b["feeder_w"])) for b in rows if b["cfm"] is not None]
    capacity = [
        (float(b["capacity_pct"]), float(b["feeder_w"]))
        for b in rows
        if b["capacity_pct"] is not None
    ]
    bands: dict[int, list[float]] = {}
    for speed, watts in rpm:
        bands.setdefault(int(round(speed / 200.0) * 200), []).append(watts)
    return {
        "n": len(rows),
        "r_vs_blower_rpm": _pearson(rpm),
        "r_vs_rpm_cubed": _pearson([(s**3, w) for s, w in rpm]),
        "r_vs_cfm": _pearson(cfm),
        "r_vs_capacity_pct": _pearson(capacity),
        "max_w": round(max((w for _, w in rpm), default=0.0), 1),
        "bands": [
            {
                "blower_rpm": band,
                "n": len(values),
                "mean_w": round(statistics.fmean(values), 1),
                "max_w": round(max(values), 1),
            }
            for band, values in sorted(bands.items())
        ],
    }


def _energy(
    series: Sequence[Mapping[str, Any]], bucket_s: int, poll_interval_s: int
) -> dict[str, Any]:
    """Observed-time kWh for each side (cardinal rule 5).

    Observed seconds come from the **sample count**, never from the window
    length: ``samples * poll_interval_s``. So a gap shrinks the energy figure
    rather than being silently filled, and ``coverage_pct`` is what makes the
    number readable — 2 kWh at 40% coverage is not a period total.
    """

    def total(value_field: str, samples_field: str) -> dict[str, Any]:
        observed_s = 0.0
        watt_seconds = 0.0
        for bucket in series:
            value = bucket[value_field]
            samples = bucket[samples_field]
            if value is None or not samples:
                continue
            # Cap at the bucket width: a bucket cannot observe more wall clock
            # than it spans, however many samples landed in it.
            seconds = min(float(bucket_s), samples * float(poll_interval_s))
            observed_s += seconds
            watt_seconds += value * seconds
        return {
            "kwh": round(watt_seconds / 3.6e6, 4) if observed_s else None,
            "observed_s": round(observed_s),
            "mean_w": round(watt_seconds / observed_s, 1) if observed_s else None,
        }

    equipment = total("equipment_w", "equipment_samples")
    feeder = total("feeder_w", "feeder_samples")
    span_s = bucket_s * len(series)
    total_kwh = None
    if equipment["kwh"] is not None or feeder["kwh"] is not None:
        total_kwh = round((equipment["kwh"] or 0.0) + (feeder["kwh"] or 0.0), 4)
    return {
        "equipment": equipment,
        "feeder": feeder,
        "total_kwh": total_kwh,
        "poll_interval_s": poll_interval_s,
        "coverage_pct": (
            round(100.0 * equipment["observed_s"] / span_s, 1) if span_s else None
        ),
        "note": (
            "Observed-time only: mean watts x observed seconds, where observed "
            "seconds are the sample count times the poll interval. A gap reduces "
            "this figure; it is never extrapolated across one."
        ),
    }


def _stamp(ts: datetime) -> dict[str, str]:
    aware = timeutil.ensure_utc(ts)
    return {
        "utc": timeutil.format_utc(aware),
        "local": timeutil.to_local_naive(aware).isoformat(timespec="seconds"),
    }
