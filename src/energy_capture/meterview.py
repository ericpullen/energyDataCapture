"""The utility-meter card for ``/ui`` — house, barn, and how well the CTs track.

Why this is a separate module
-----------------------------
The dashboard reads the **spool**, which holds ``raw_30s`` observations. Meter
intervals never go through the spool: ``import-greenbutton`` and
``fetch-greenbutton`` write straight to ``{SPOOL_DIR}/meter/*.parquet``, because
interval data carries ``interval_s`` and would poison the hourly rollups the same
way day-grain rows do. So the page was blind to the meter entirely, and closing
that needs a second, quite different read path — not another branch inside
``build_snapshot``.

What it shows, and why each part
--------------------------------
* **Per-meter kWh** for yesterday and today-so-far. Two meters here: the house
  and a separately metered barn.
* **Panels vs. meter** for the **last complete local day** — the accuracy check
  that justifies the whole sub-metering exercise, on the page instead of only in
  the CLI.
* **Freshness.** The utility publishes hours behind and the fetch runs once a
  day. A meter number that does not say how old it is invites exactly the wrong
  reading — "today-so-far" is not "now", it is "up to whenever the custodian last
  published".

The comparison is cached
------------------------
The page polls every 5 seconds; a DuckDB rollup of a whole day is not something
to run at that rate. But the inputs barely move — the last complete day changes
at local midnight, and the meter file changes when a fetch lands — so the result
is memoised on exactly those two things. When neither has changed, the cached
answer is not merely fresh enough, it is *identical*.

Nothing here raises. Every failure returns a block carrying ``error`` and the
page renders the rest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from energy_capture import model, timeutil
from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger

log = get_logger("meterview")

#: A meter reading older than this is called out on the page. LG&E runs about
#: six hours behind at best and the fetch is daily, so a day and a half is
#: "normal lag plus one missed run" — beyond it, something is wrong.
STALE_AFTER_S = 36 * 3600

__all__ = ["meter_block", "reset_cache"]


@dataclass
class _Cached:
    key: tuple[Any, ...]
    value: dict[str, Any]


_comparison_cache: _Cached | None = None


def reset_cache() -> None:
    """Drop the memoised comparison (tests, and after a re-import)."""
    global _comparison_cache
    _comparison_cache = None


def _meter_dir(settings: Settings) -> Path:
    return settings.spool_dir / "meter"


def _tables(directory: Path, source: str) -> list[Any]:
    if not directory.is_dir():
        return []
    return [pq.read_table(p) for p in sorted(directory.glob(f"{source}-*.parquet"))]


def _newest_mtime(directory: Path, source: str) -> float | None:
    if not directory.is_dir():
        return None
    stamps = [p.stat().st_mtime for p in directory.glob(f"{source}-*.parquet")]
    return max(stamps) if stamps else None


def meter_block(
    *,
    now: datetime | None = None,
    settings: Settings | None = None,
    spool_path: Path | str | None = None,
    labels: dict[tuple[str, str, str], dict[str, Any]] | None = None,
    channel_map: Path | str | None = None,
) -> dict[str, Any]:
    """The whole card. Never raises; a failure becomes ``{"error": …}``."""
    resolved = settings or get_settings()
    reference = timeutil.ensure_utc(now or timeutil.now_utc())
    today = timeutil.local_date_of(reference)
    yesterday = today - timedelta(days=1)
    directory = _meter_dir(resolved)

    try:
        tables = _tables(directory, model.SOURCE_LGE)
    except Exception as exc:  # pragma: no cover - unreadable parquet
        return {"available": False, "error": f"meter data unreadable: {exc}"}

    if not tables:
        return {
            "available": False,
            "dir": str(directory),
            "hint": (
                "no meter data yet — run `energycap greenbutton-authorize` once, "
                "then `energycap fetch-greenbutton`"
            ),
        }

    meters = _per_meter(tables, today=today, yesterday=yesterday, labels=labels)
    latest = max(
        (m["last_reading_utc"] for m in meters if m["last_reading_utc"]), default=None
    )
    age_s = (reference - latest).total_seconds() if latest else None
    for entry in meters:
        stamp = entry["last_reading_utc"]
        entry["last_reading_utc"] = timeutil.format_utc(stamp) if stamp else None

    return {
        "available": True,
        "meters": meters,
        "last_reading_utc": timeutil.format_utc(latest) if latest else None,
        # The page renders wall-clock text from `local` fields only, never by
        # slicing a UTC string — that is how a dashboard ends up four hours off.
        "last_reading_local": (
            timeutil.to_local_naive(latest).isoformat(sep=" ") if latest else None
        ),
        "age_s": round(age_s) if age_s is not None else None,
        "stale": bool(age_s is not None and age_s > STALE_AFTER_S),
        "stale_after_s": STALE_AFTER_S,
        "note": (
            "The utility publishes hours behind and the fetch runs daily, so "
            "“today” ends at the last published interval, not now."
        ),
        "comparison": _comparison(
            tables=tables,
            local_day=yesterday,
            directory=directory,
            settings=resolved,
            spool_path=spool_path,
            requested=primary_meter(labels, [m["device_id"] for m in meters]),
            channel_map=channel_map,
        ),
    }


def _per_meter(
    tables: list[Any],
    *,
    today: date,
    yesterday: date,
    labels: dict[tuple[str, str, str], dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """One row per meter: yesterday, today-so-far, and where it ends.

    Only the **finest** interval series per meter is totalled. LG&E publishes the
    same energy at 900s and 3600s, and adding both would roughly double every
    number on the card (DEVIATIONS #169).
    """
    finest: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for table in tables:
        rows.extend(r for r in table.to_pylist() if r["metric"] == "kwh_interval")
    for row in rows:
        device = row["device_id"]
        length = int(row["interval_s"])
        finest[device] = min(finest.get(device, length), length)

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        device = row["device_id"]
        if int(row["interval_s"]) != finest[device]:
            continue
        local_day = row["ts_local"].date()
        entry = out.setdefault(
            device,
            {
                "device_id": device,
                "label": _label(labels, device),
                "interval_s": finest[device],
                "yesterday_kwh": 0.0,
                "today_kwh": 0.0,
                "last_reading_utc": None,
            },
        )
        if local_day == yesterday:
            entry["yesterday_kwh"] += float(row["value"])
        elif local_day == today:
            entry["today_kwh"] += float(row["value"])
        stamp = timeutil.ensure_utc(row["ts_utc"])
        if entry["last_reading_utc"] is None or stamp > entry["last_reading_utc"]:
            entry["last_reading_utc"] = stamp

    for entry in out.values():
        entry["yesterday_kwh"] = round(entry["yesterday_kwh"], 3)
        entry["today_kwh"] = round(entry["today_kwh"], 3)

    # Collapse meter ids that carry an identical series. The Download export
    # republishes the house under two retired ids (DEVIATIONS #168); showing
    # three "meters" that are one service would make the card read as though the
    # property had three, and invite someone to add them up.
    signatures: dict[tuple[Any, ...], str] = {}
    collapsed: dict[str, dict[str, Any]] = {}
    for device in sorted(out):
        entry = out[device]
        signature = tuple(
            sorted(
                (r["ts_utc"], round(float(r["value"]), 6))
                for r in rows
                if r["device_id"] == device and int(r["interval_s"]) == finest[device]
            )
        )
        keeper = signatures.get(signature)
        if keeper is None:
            signatures[signature] = device
            entry["aliases"] = []
            collapsed[device] = entry
        else:
            collapsed[keeper]["aliases"].append(device)

    # Biggest first: the house before the barn, without hard-coding either.
    # ``last_reading_utc`` stays a datetime here — the caller needs to compare
    # them before anything is formatted for display.
    return sorted(collapsed.values(), key=lambda e: -e["yesterday_kwh"])


def _label(
    labels: dict[tuple[str, str, str], dict[str, Any]] | None, device: str
) -> str:
    """The dim_channel label for this meter, else the bare id."""
    meta = _meta(labels, device)
    found = meta.get("short_label") or meta.get("label") if meta else None
    return str(found) if found else device


def _meta(
    labels: dict[tuple[str, str, str], dict[str, Any]] | None, device: str
) -> dict[str, Any] | None:
    if not labels:
        return None
    for (source, dev, _channel), meta in labels.items():
        if source == model.SOURCE_LGE and dev == device:
            return meta
    return None


def primary_meter(
    labels: dict[tuple[str, str, str], dict[str, Any]] | None,
    candidates: list[str],
) -> str | None:
    """The meter marked ``primary`` in ``channel_map.json``, if any.

    The account has two real services — the house and the barn — and only the
    house has panel CTs to compare against. Which is which is knowledge about
    the property, so it lives in the map rather than in a heuristic here ("the
    bigger one is the house" would be wrong the first time an EV charges hard on
    a mild day).
    """
    if not labels:
        return None
    for device in candidates:
        meta = _meta(labels, device)
        if meta and meta.get("primary"):
            return device
    return None


def _comparison(
    *,
    tables: list[Any],
    local_day: date,
    directory: Path,
    settings: Settings,
    spool_path: Path | str | None,
    requested: str | None = None,
    channel_map: Path | str | None = None,
) -> dict[str, Any]:
    """Panels vs. meter for one complete local day, memoised on its inputs."""
    global _comparison_cache

    key = (
        local_day,
        _newest_mtime(directory, model.SOURCE_LGE),
        str(spool_path or ""),
        requested,
        str(channel_map or ""),
    )
    if _comparison_cache is not None and _comparison_cache.key == key:
        return _comparison_cache.value

    value = _compute_comparison(
        tables=tables,
        local_day=local_day,
        settings=settings,
        spool_path=spool_path,
        requested=requested,
        channel_map=channel_map,
    )
    _comparison_cache = _Cached(key=key, value=value)
    return value


def _compute_comparison(
    *,
    tables: list[Any],
    local_day: date,
    settings: Settings,
    spool_path: Path | str | None,
    requested: str | None = None,
    channel_map: Path | str | None = None,
) -> dict[str, Any]:
    # Imported here, not at module scope: the comparison pulls in DuckDB and the
    # rollup, and the dashboard must keep rendering if that import ever fails.
    try:
        from energy_capture.spool.sqlite import open_spool
        from energy_capture.stages import compare
    except Exception as exc:  # pragma: no cover - import-time failure only
        return {"available": False, "error": f"comparison unavailable: {exc}"}

    try:
        device_id, _ = compare.resolve_meter(tables, requested=requested)
    except compare.AmbiguousMeterError:
        # Several real meters and nothing in the map saying which is the house.
        # The CLI can ask; a card cannot, so it says so rather than picking one.
        return {
            "available": False,
            "reason": (
                "several meters differ and none is marked `primary` in "
                "channel_map.json — run `energycap compare-meter --meter …`"
            ),
        }
    except Exception as exc:  # pragma: no cover - defensive
        return {"available": False, "error": str(exc)}

    if device_id is None:
        return {"available": False, "reason": "no meter readings"}

    interval_s, _ = compare.resolve_interval(tables, device_id=device_id)
    # How many feed series SHOULD report, from the hand-maintained map. Without
    # it an hour in which a whole hub went silent shows 100% coverage — every
    # channel that reported reported fully — while the panel total is short by a
    # whole panel. `sample_count` is a minimum and cannot see an absent series.
    expected_series = len(compare.expected_feed_series(map_path=channel_map))
    try:
        with open_spool(spool_path) as spool:
            rows = compare.compare_range(
                start=local_day,
                end=local_day,
                spool=spool,
                meter_tables=tables,
                poll_interval_s=settings.poll_interval_s,
                device_id=device_id,
                interval_s=interval_s,
                expected_series=expected_series,
            )
    except Exception as exc:
        log.warning("meterview_comparison_failed", error=f"{type(exc).__name__}: {exc}")
        return {"available": False, "error": f"comparison failed: {exc}"}

    def _both(r: Any) -> bool:
        return r.meter_kwh is not None and r.panel_kwh is not None

    def _qualifies(r: Any) -> bool:
        series_ok = r.series_complete or not r.series_expected
        return r.coverage >= compare.DEFAULT_MIN_COVERAGE and series_ok

    usable = [r for r in rows if _both(r) and _qualifies(r)]
    excluded = sum(1 for r in rows if _both(r) and not _qualifies(r))
    missing_feed = sum(
        1 for r in rows if _both(r) and r.series_expected and not r.series_complete
    )
    if not usable:
        return {
            "available": False,
            "local_day": local_day.isoformat(),
            "meter": device_id,
            "hours_excluded": excluded,
            "hours_missing_a_feed": missing_feed,
            "series_expected": expected_series or None,
            "reason": (
                "no hour of that day had both a meter reading and a complete "
                "panel side (full sample coverage AND every feed series "
                "reporting)"
            ),
        }

    meter_kwh = sum(r.meter_kwh or 0.0 for r in usable)
    panel_kwh = sum(r.panel_kwh or 0.0 for r in usable)
    difference = panel_kwh - meter_kwh
    return {
        "available": True,
        "local_day": local_day.isoformat(),
        "meter": device_id,
        "interval_s": interval_s,
        "hours_compared": len(usable),
        "hours_excluded": excluded,
        "hours_missing_a_feed": missing_feed,
        "series_expected": expected_series or None,
        "meter_kwh": round(meter_kwh, 3),
        "panel_kwh": round(panel_kwh, 3),
        "difference_kwh": round(difference, 3),
        "difference_pct": round(100.0 * difference / meter_kwh, 1) if meter_kwh else None,
        "note": (
            "Only hours with full sample coverage AND every feed series "
            "reporting are totalled. A partly observed hour understates the "
            "panels because the collector was down, not because the CTs are "
            "wrong — and an hour missing a whole hub understates them by a "
            "whole panel while every surviving channel still reads 100%."
        ),
    }
