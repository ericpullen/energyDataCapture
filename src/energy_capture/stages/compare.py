"""Meter vs. panels: does the sub-metering add up to the bill?

The question this answers
-------------------------
The Leviton CTs on the two service feeds should, summed, equal what the utility
meter records for the whole house. They will not equal it exactly — CTs have a
tolerance, the meter has its own, and the two devices integrate over different
windows — but the *size and sign* of the disagreement is the single most useful
number in this project. A consistent few percent is instrument error. A large or
drifting gap means a feed is unmetered, a CT is on backwards, or a clamp is on
the wrong conductor.

Why it does not just query S3
-----------------------------
Not because there is no S3 — there is, and has been since 2026-08-19 — but
because the freshest panel data is not in it. The uploader runs hourly, so the
last hour of 30s rows lives only in the spool, and a comparison that queried S3
would silently be comparing an incomplete final hour. This reads the SQLite
spool directly, plus the meter Parquet that ``energycap import-greenbutton``
writes locally, so the comparison is exact to the last completed poll cycle and
works on a laptop with nothing but the collector running.

**It must therefore run inside the container** while the collector holds the
spool::

    container exec energycap energycap compare-meter --start … --end …

Opening the spool from the macOS host while the container writes it corrupts the
database — measured, and the reason the dashboard exists.

The panel side reuses the rollup
--------------------------------
It does not re-implement the kWh math. Spool rows for each local day are handed
to :func:`energy_capture.stages.rollup.rollup_day`, which is the same code and
the same ``rollup.sql`` that produces ``energy/hourly`` — so the comparison
cannot disagree with the warehouse about what an hour of watts is worth. That
also means it inherits the honest bits: ``kwh`` is observed-time-only, and a
partly observed hour reports a smaller ``sample_count`` rather than a guess.

Reading the output honestly
---------------------------
``coverage`` is ``sample_count`` over what a fully observed hour would hold. An
hour at 60% coverage will show ~40% less panel energy than the meter *because
the collector was down*, not because the CTs are wrong. Hours below
``--min-coverage`` are reported but excluded from the totals, and how many were
excluded is printed — a comparison that quietly dropped its inconvenient hours
would be worse than no comparison.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from energy_capture import model, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.spool.sqlite import open_spool
from energy_capture.stages.rollup import rollup_day

STAGE = "compare"
log = get_logger(STAGE)

#: The whole-house feed CTs. ``ct_1_a``/``ct_1_b`` are the two service legs on
#: each Leviton hub, so all four summed is the whole house — ``panel_leg_*`` is
#: *voltage*, not power, and must never be added in here.
DEFAULT_PANEL_CHANNELS: tuple[str, ...] = ("ct_1_a", "ct_1_b")

#: Where the expected feed series come from. Hand-maintained, committed, and
#: the same file the rest of the semantic layer is built from.
DEFAULT_CHANNEL_MAP = Path("config/channel_map.json")


def primary_meter_from_map(
    map_path: Path | str | None = None, *, source: str = model.SOURCE_LGE
) -> str | None:
    """The ``device_id`` marked ``primary: true``, or ``None``.

    The account has two real services and only the house has panel CTs to
    compare against. Which is which is knowledge about the property, so it
    lives in the map — "the bigger one is the house" would be wrong the first
    time the barn's EV charges hard on a mild day.

    Returns ``None`` rather than guessing when the map is unreadable or marks
    no primary; :func:`resolve_meter` then refuses if the meters genuinely
    differ, which is the behaviour worth keeping.
    """
    resolved = Path(map_path) if map_path else DEFAULT_CHANNEL_MAP
    try:
        blob = json.loads(resolved.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    for entry in blob.get("mappings", []):
        if not isinstance(entry, dict) or entry.get("source") != source:
            continue
        # `primary` must be a real bool: dim.py coerced with bool(), which made
        # the string "no" true (review B7).
        if entry.get("primary") is True and isinstance(entry.get("device_id"), str):
            return str(entry["device_id"])
    return None


def expected_feed_series(
    channels: Sequence[str] = DEFAULT_PANEL_CHANNELS,
    *,
    map_path: Path | str | None = None,
    source: str = model.SOURCE_LEVITON,
) -> frozenset[tuple[str, str]]:
    """The ``(device_id, channel_id)`` feed series that SHOULD report.

    Read from ``channel_map.json`` rather than from the measurements, and that
    is the whole point. Deriving the expectation from the data is circular: a
    hub that stopped reporting for the entire hour simply would not appear, so
    its absence could never make the hour incomplete — which is exactly the bug
    this exists to close. ``historyview`` reaches the same conclusion from
    ``dim_channel``; the map is used here because ``compare-meter`` is designed
    to run with nothing but the collector, and the map needs no build step.

    Returns an empty set when the map is missing or unreadable. Callers must
    treat that as "cannot judge completeness" rather than as "nothing expected".
    """
    resolved = Path(map_path) if map_path else DEFAULT_CHANNEL_MAP
    wanted = set(channels)
    try:
        blob = json.loads(resolved.read_text())
    except (OSError, json.JSONDecodeError):
        return frozenset()
    out: set[tuple[str, str]] = set()
    for entry in blob.get("mappings", []):
        if not isinstance(entry, dict):
            continue
        if entry.get("source") != source:
            continue
        device, channel = entry.get("device_id"), entry.get("channel_id")
        if isinstance(device, str) and channel in wanted:
            out.add((device, channel))
    return frozenset(out)

#: An hour below this fraction of its expected samples is shown but not totalled.
DEFAULT_MIN_COVERAGE = 0.9

__all__ = ["HourComparison", "compare_range", "format_report", "run"]


@dataclass(frozen=True)
class HourComparison:
    """One local hour, from both sides."""

    hour_start_utc: datetime
    local_hour_start: datetime
    meter_kwh: float | None
    panel_kwh: float | None
    sample_count: int
    expected_samples: int
    #: Distinct ``(device_id, channel_id)`` feed series that produced a row this
    #: hour, and how many the semantic layer says should have. Zero expected
    #: means the channel map could not be read — completeness is then UNKNOWN.
    series_seen: int = 0
    series_expected: int = 0

    @property
    def coverage(self) -> float:
        if not self.expected_samples:
            return 0.0
        return min(1.0, self.sample_count / self.expected_samples)

    @property
    def series_complete(self) -> bool:
        """Did every feed the map knows about report at all this hour?

        ``sample_count`` cannot answer this. It is the MINIMUM across the
        channels that produced rows, so a hub that was absent for the entire
        hour contributes nothing to the minimum and the remaining hub's full
        count is reported as 100% coverage — while the summed energy is missing
        a whole panel. One hub offline for a day read as "the panels are 50%
        below the meter" at full coverage.
        """
        return bool(self.series_expected) and self.series_seen >= self.series_expected

    @property
    def difference_kwh(self) -> float | None:
        if self.meter_kwh is None or self.panel_kwh is None:
            return None
        return self.panel_kwh - self.meter_kwh

    @property
    def difference_pct(self) -> float | None:
        if self.difference_kwh is None or not self.meter_kwh:
            return None
        return 100.0 * self.difference_kwh / self.meter_kwh

    def to_dict(self) -> dict[str, Any]:
        return {
            "hour_start_utc": timeutil.format_utc(self.hour_start_utc),
            "local_hour_start": self.local_hour_start.isoformat(sep=" "),
            "meter_kwh": self.meter_kwh,
            "panel_kwh": self.panel_kwh,
            "difference_kwh": self.difference_kwh,
            "difference_pct": self.difference_pct,
            "sample_count": self.sample_count,
            "expected_samples": self.expected_samples,
            "series_seen": self.series_seen,
            "series_expected": self.series_expected,
            "series_complete": self.series_complete,
        }


# ------------------------------------------------------------- the two sides


def panel_hours(
    spool: Any,
    local_day: date,
    *,
    channels: Sequence[str],
    poll_interval_s: int,
) -> dict[datetime, tuple[float, int, int]]:
    """``hour_start_utc -> (kWh, min sample_count, distinct series reporting)``.

    The sample count reported is the **minimum** across the contributing
    channels, not the sum: if one of the four feed CTs was missing for half the
    hour, the summed energy is understated by that channel's share, and the
    weakest channel is what says so.

    The minimum cannot see a channel that produced NO rows at all, though — an
    absent hub contributes nothing to a minimum. So the count of distinct
    ``(device_id, channel_id)`` series is returned alongside it, and
    :attr:`HourComparison.series_complete` compares it against what the channel
    map says should exist.
    """
    observations = [
        obs
        for hour in range(24)
        for obs in spool.rows_for_local_hour(local_day, hour)
    ]
    if not observations:
        return {}

    table = model.observations_to_table(observations, dataset=model.Dataset.RAW_30S)
    with tempfile.TemporaryDirectory(prefix="energycap-compare-") as scratch:
        path = Path(scratch) / f"raw-{local_day:%Y%m%d}.parquet"
        pq.write_table(table, path)
        hourly = rollup_day(local_day, [str(path)], poll_interval_s=poll_interval_s)

    wanted = set(channels)
    totals: dict[datetime, tuple[float, int]] = {}
    series: dict[datetime, set[tuple[str, str]]] = {}
    for row in hourly.to_pylist():
        if row["metric"] != "watts" or row["channel_id"] not in wanted:
            continue
        if row["kwh"] is None:
            continue
        bucket = row["hour_start_utc"]
        kwh, samples = totals.get(bucket, (0.0, 0))
        merged_samples = row["sample_count"] if not samples else min(samples, row["sample_count"])
        totals[bucket] = (kwh + row["kwh"], merged_samples)
        # Keyed on the PAIR: both hubs publish a `ct_1_a`, so the channel_id
        # alone cannot tell four reporting series from two.
        series.setdefault(bucket, set()).add((row["device_id"], row["channel_id"]))
    return {
        bucket: (kwh, samples, len(series.get(bucket, ())))
        for bucket, (kwh, samples) in totals.items()
    }


class AmbiguousMeterError(RuntimeError):
    """Several distinct meters are present and none was chosen."""


def resolve_meter(
    tables: Sequence[pa.Table], *, requested: str | None = None
) -> tuple[str | None, str | None]:
    """Decide which ``device_id`` the comparison is about.

    A real LG&E export turns out to carry the **same interval series under
    three UsagePoints** — 1308468, 944401 and 944006, identical to the watt-hour
    for every interval of ten days (measured 2026-08-18). Almost certainly the
    same service through successive meter swaps. Summing them would report three
    times the household's actual consumption and make the panels look like they
    were measuring a third of the house.

    So: one meter, always. If several are present and they are identical, one is
    chosen deterministically and *said so*. If they genuinely differ, this
    refuses and lists them — guessing which meter is the house is not something
    software should do quietly.
    """
    totals: dict[str, float] = {}
    series: dict[str, list[tuple[Any, float]]] = {}
    for table in tables:
        for row in table.to_pylist():
            if row["metric"] != "kwh_interval":
                continue
            device = row["device_id"]
            totals[device] = totals.get(device, 0.0) + float(row["value"])
            series.setdefault(device, []).append((row["ts_utc"], float(row["value"])))

    if not totals:
        return None, None
    if requested is not None:
        if requested not in totals:
            raise AmbiguousMeterError(
                f"no readings for meter {requested!r}; this export has "
                f"{sorted(totals)}"
            )
        return requested, None
    if len(totals) == 1:
        return next(iter(totals)), None

    ordered = {device: sorted(rows) for device, rows in series.items()}
    distinct = {repr(rows) for rows in ordered.values()}
    chosen = sorted(totals)[0]
    if len(distinct) == 1:
        return chosen, (
            f"This export carries {len(totals)} meter ids with an IDENTICAL "
            f"series ({', '.join(sorted(totals))}) — the same service through "
            f"meter changes. Using {chosen}; summing them would treble the "
            "meter reading."
        )
    raise AmbiguousMeterError(
        "this export carries several meters with DIFFERENT readings — pass "
        "--meter to choose one:\n"
        + "\n".join(f"  {device}: {total:.2f} kWh" for device, total in sorted(totals.items()))
    )


def resolve_interval(
    tables: Sequence[pa.Table], *, device_id: str | None = None
) -> tuple[int | None, str | None]:
    """Pick ONE interval series. Summing two resolutions doubles the energy.

    LG&E publishes the same energy twice — a 15-minute series and an hourly one,
    for the same UsagePoint (measured 2026-08-18: they collide at every hour
    boundary). Both are stored, because they are genuinely different observations
    and discarding one at ingest would be filtering the custodian's data. But a
    comparison must choose, and adding them together would report roughly twice
    the household's consumption.

    The finest interval wins: it is the more informative series, and the coarser
    one is a rollup of the same energy.
    """
    lengths: set[int] = set()
    for table in tables:
        for row in table.to_pylist():
            if row["metric"] != "kwh_interval":
                continue
            if device_id is not None and row["device_id"] != device_id:
                continue
            lengths.add(int(row["interval_s"]))
    if not lengths:
        return None, None
    finest = min(lengths)
    if len(lengths) == 1:
        return finest, None
    return finest, (
        f"This meter publishes {len(lengths)} interval series "
        f"({', '.join(f'{v}s' for v in sorted(lengths))}) covering the same "
        f"energy. Using the finest ({finest}s); adding them would roughly "
        f"{'double' if len(lengths) == 2 else 'multiply'} the meter reading."
    )


def meter_hours(
    tables: Sequence[pa.Table],
    start: date,
    end: date,
    *,
    device_id: str | None = None,
    interval_s: int | None = None,
) -> dict[datetime, float]:
    """``hour_start_utc -> kWh``, from interval readings.

    Readings are assigned to the UTC hour containing their **start**. A 15-minute
    interval never straddles an hour boundary and an hourly one aligns with it,
    so no reading is ever split — and if a custodian ever sends something that
    does straddle, it lands whole in the hour it began, which is the same
    convention ``ts_utc``-as-interval-start already implies.
    """
    totals: dict[datetime, float] = {}
    for table in tables:
        for row in table.to_pylist():
            if row["metric"] != "kwh_interval":
                continue
            if device_id is not None and row["device_id"] != device_id:
                continue
            if interval_s is not None and int(row["interval_s"]) != interval_s:
                continue
            stamp = timeutil.ensure_utc(row["ts_utc"])
            if not (start <= timeutil.local_date_of(stamp) <= end):
                continue
            bucket = timeutil.utc_hour_start(stamp)
            totals[bucket] = totals.get(bucket, 0.0) + float(row["value"])
    return totals


def load_meter_tables(
    meter_dir: Path, *, source: str = model.SOURCE_LGE
) -> list[pa.Table]:
    files = sorted(meter_dir.glob(f"{source}-*.parquet")) if meter_dir.is_dir() else []
    return [pq.read_table(path) for path in files]


# --------------------------------------------------------------- the compare


def compare_range(
    *,
    start: date,
    end: date,
    spool: Any,
    meter_tables: Sequence[pa.Table],
    channels: Sequence[str] = DEFAULT_PANEL_CHANNELS,
    poll_interval_s: int = 30,
    device_id: str | None = None,
    interval_s: int | None = None,
    expected_series: int = 0,
) -> list[HourComparison]:
    """Both sides, hour by hour, over ``start``..``end`` inclusive (local dates).

    ``expected_series`` is how many ``(device_id, channel_id)`` feed series
    should report. It is passed IN rather than read from the channel map here:
    this function is the measurement, and where the expectation comes from is
    policy that belongs to the caller (``run`` reads the map). Left at 0 the
    completeness of the feed set is UNKNOWN and no hour is excluded for a
    missing hub — which is the honest default for a caller that has not said.
    """
    meter = meter_hours(
        meter_tables, start, end, device_id=device_id, interval_s=interval_s
    )
    panels: dict[datetime, tuple[float, int, int]] = {}
    expected: dict[datetime, int] = {}

    for local_day in timeutil.iter_local_dates(start, end):
        panels.update(
            panel_hours(
                spool, local_day, channels=channels, poll_interval_s=poll_interval_s
            )
        )
        for hour in timeutil.iter_local_hours(local_day):
            span = (hour.end_utc - hour.start_utc).total_seconds()
            expected[hour.start_utc] = int(span // poll_interval_s)

    rows: list[HourComparison] = []
    for bucket in sorted(set(meter) | set(panels)):
        kwh, samples, seen = panels.get(bucket, (None, 0, 0))
        rows.append(
            HourComparison(
                hour_start_utc=bucket,
                local_hour_start=timeutil.local_hour_start(bucket),
                meter_kwh=meter.get(bucket),
                panel_kwh=kwh,
                sample_count=samples,
                expected_samples=expected.get(bucket, 0),
                series_seen=seen,
                series_expected=expected_series,
            )
        )
    return rows


def format_report(
    rows: Sequence[HourComparison], *, min_coverage: float = DEFAULT_MIN_COVERAGE
) -> str:
    """A plain-text table plus totals, with the excluded hours accounted for."""
    lines = [
        f"{'local hour':<19} {'meter kWh':>10} {'panels kWh':>11} "
        f"{'diff kWh':>9} {'diff %':>8} {'coverage':>9} {'feeds':>7}",
        "-" * 79,
    ]
    meter_total = panel_total = 0.0
    compared = excluded = missing_series = 0
    unknown_expectation = any(not row.series_expected for row in rows)

    for row in rows:
        both = row.meter_kwh is not None and row.panel_kwh is not None
        # A whole absent feed is a DIFFERENT exclusion from thin coverage, and
        # the two must not be conflated: thin coverage understates the hour a
        # little, an absent hub understates it by a whole panel while every
        # surviving channel reports 100%.
        series_ok = row.series_complete or not row.series_expected
        usable = both and row.coverage >= min_coverage and series_ok
        if usable:
            meter_total += row.meter_kwh or 0.0
            panel_total += row.panel_kwh or 0.0
            compared += 1
        elif both:
            excluded += 1
            if not series_ok:
                missing_series += 1
        feeds = (
            f"{row.series_seen}/{row.series_expected}"
            if row.series_expected
            else f"{row.series_seen}/?"
        )
        lines.append(
            f"{row.local_hour_start:%Y-%m-%d %H:%M}    "
            f"{_num(row.meter_kwh):>10} {_num(row.panel_kwh):>11} "
            f"{_num(row.difference_kwh):>9} {_num(row.difference_pct, 1):>8} "
            f"{row.coverage * 100:>8.0f}% {feeds:>7}{'' if usable else '  *'}"
        )

    lines.append("-" * 79)
    if compared:
        diff = panel_total - meter_total
        pct = 100.0 * diff / meter_total if meter_total else float("nan")
        lines += [
            f"{'TOTAL (' + str(compared) + ' hours)':<19} "
            f"{meter_total:>10.3f} {panel_total:>11.3f} {diff:>9.3f} {pct:>8.1f}%",
            "",
            f"The panels read {abs(pct):.1f}% "
            f"{'above' if diff > 0 else 'below'} the meter over these hours.",
        ]
    else:
        lines.append("No hour had both a meter reading and adequate panel coverage.")

    if excluded:
        lines.append(
            f"* {excluded} hour(s) had both sides but did not qualify and are NOT "
            f"in the total: under {min_coverage:.0%} sample coverage, or missing a "
            "whole feed series. Either way the panel figure is understated."
        )
    if missing_series:
        lines.append(
            f"  {missing_series} of those were missing a whole FEED SERIES — a hub "
            "that reported nothing at all. sample_count cannot see this (it is a "
            "minimum over the channels that did report), so such an hour shows "
            "full coverage while the panel total is short by a whole panel."
        )
    if unknown_expectation:
        lines.append(
            "! The channel map could not be read, so how many feed series SHOULD "
            "report is unknown and no hour was excluded for a missing hub. Pass "
            "--channel-map, or run from the repository root."
        )
    lines.append(
        "Rows with a blank on either side are hours only one source covers; "
        "they are never filled in."
    )
    return "\n".join(lines)


def _num(value: float | None, places: int = 3) -> str:
    return "" if value is None else f"{value:.{places}f}"


def run(
    *,
    start: date,
    end: date,
    meter_dir: Path | None = None,
    channels: tuple[str, ...] | None = None,
    source: str = model.SOURCE_LGE,
    meter: str | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    spool_path: Path | None = None,
    channel_map: Path | None = None,
) -> dict[str, Any]:
    """``energycap compare-meter --start … --end …``."""
    settings = get_settings()
    directory = Path(meter_dir) if meter_dir else settings.spool_dir / "meter"
    tables = load_meter_tables(directory, source=source)
    if not tables:
        log.warning("compare_no_meter_data", directory=str(directory), source=source)

    # B6: the documented recipe fetched both meters and then ran with no
    # --meter, which is a guaranteed AmbiguousMeterError. `meterview` already
    # consults the map's `primary` flag; this did not, so the CLI and the /ui
    # card disagreed about whether the question was answerable at all.
    requested = meter or primary_meter_from_map(channel_map)
    device_id, note = resolve_meter(tables, requested=requested)
    if note:
        log.warning("compare_meter_ambiguous", note=note, chosen=device_id)
    interval_s, interval_note = resolve_interval(tables, device_id=device_id)
    if interval_note:
        log.warning(
            "compare_meter_multiple_intervals", note=interval_note, chosen=interval_s
        )

    wanted = channels or DEFAULT_PANEL_CHANNELS
    expected_series = len(expected_feed_series(wanted, map_path=channel_map))
    if not expected_series:
        log.warning(
            "compare_no_channel_map",
            path=str(channel_map or DEFAULT_CHANNEL_MAP),
            detail=(
                "cannot tell how many feed series should report, so an hour "
                "missing a whole hub will not be excluded"
            ),
        )

    with open_spool(spool_path) as spool:
        rows = compare_range(
            start=start,
            end=end,
            spool=spool,
            meter_tables=tables,
            channels=wanted,
            poll_interval_s=settings.poll_interval_s,
            device_id=device_id,
            interval_s=interval_s,
            expected_series=expected_series,
        )

    report = format_report(rows, min_coverage=min_coverage)
    for message in (note, interval_note):
        if message:
            report = f"NOTE: {message}\n\n{report}"
    print(report)  # noqa: T201 - this command's output *is* the report

    both = [r for r in rows if r.meter_kwh is not None and r.panel_kwh is not None]
    return {
        "hours": len(rows),
        "hours_compared": len(both),
        "meter": device_id,
        "interval_s": interval_s,
        "meter_files": len(tables),
        "meter_dir": str(directory),
        "channels": list(wanted),
        "series_expected": expected_series,
        "hours_missing_a_feed": sum(
            1
            for r in both
            if r.series_expected and not r.series_complete
        ),
    }
