"""The ``/ui`` dashboard: a read-only window onto the live spool.

Two routes, both served by the existing :class:`~energy_capture.health.HealthServer`
(PLAN.md §11's server — no second port, no web framework, no new dependency):

``GET /ui``
    ``static/dashboard.html``, one self-contained file (vanilla JS, inline SVG,
    no CDN, no build step). Read through :func:`importlib.resources` so it works
    from the installed wheel inside the container image, and cached in memory
    after the first read.

``GET /ui/data``
    The JSON snapshot below, which the page polls every 5s.

    Two optional query parameters move the **overlay watts chart's** time window.
    Nothing else on the page is affected by them: the "Live now" cards and "The
    math" table keep their own fixed windows, and a request with no parameters at
    all returns exactly the document it always did.

    ``window_s``
        Chart window length in seconds. Default :data:`CHART_DEFAULT_WINDOW_S`
        (1800 — the live view). Clamped to
        ``[CHART_MIN_WINDOW_S, CHART_MAX_WINDOW_S]`` = ``[60, 86400]``; a value
        that is not a positive integer is a **400**, never a silent default.
    ``end``
        ISO-8601 instant at the RIGHT edge of the window (``2026-08-16T18:30:00Z``;
        a bare naive value is read as UTC). Omit it to mean "now", which is what
        *live* means here: a request that pins ``end`` is history and its window
        does not move as the page refreshes. Unparseable, or more than
        :data:`CHART_FUTURE_SKEW_S` in the future, is a **400**.

    Unknown parameters are ignored (``/ui/data?since=now`` has always been a 200).
    :func:`handle_ui_data` is the entry point that turns a request target into
    ``(status, document)`` — including the 400s; :func:`build_snapshot` takes the
    already-parsed :class:`ChartRequest`.

Long windows are **bucketed on the server** (:data:`CHART_RAW_MAX_WINDOW_S`: an
hour or less stays raw, so the live view is exactly as responsive as it was).
Bucketing is the place a chart fabricates data, so:

* **an empty bucket is an explicit hole** — ``mean``/``min``/``max`` are ``null``
  and ``sample_count`` is ``0``. Never 0 W, never the previous bucket's value,
  never an interpolation between neighbours. The page breaks the line there, the
  same way it breaks it at a raw gap;
* **a partial bucket keeps its real mean** — the mean of the samples that exist,
  divided by how many exist, never by the expected count (dividing by the
  expected count is `PLAN.md` §2.5's "extrapolate across a gap" wearing a hat).
  Every bucket carries its own ``sample_count`` *and* ``expected`` so the page can
  show it as partial rather than as solid fact;
* **bucket boundaries are computed on ``ts_utc``** (CLAUDE.md rule 3) — aligned to
  epoch multiples of the bucket width, so they are stable across refreshes and the
  DST fall-back day's two 01:00 local hours land in different buckets rather than
  merging into one.

**This module never writes.** The spool is opened on a *separate* connection with
``mode=ro`` and a short ``busy_timeout``, so a dashboard request can neither block
nor corrupt the poll loop that is writing to the same file (WAL means the reader
never takes the write lock at all; ``mode=ro`` means a bug here cannot).

The three cardinal rules this module is most able to violate, and what it does
instead (CLAUDE.md rules 1 and 5):

* **A gap stays a gap.** The series builder emits *only observed samples*. It
  never emits a placeholder value, never zero-fills, and never resamples onto a
  regular grid. Consecutive points more than ``gap_threshold_s``
  (:data:`GAP_INTERVAL_FACTOR` × the poll interval) apart are a gap; those gaps
  are also listed explicitly in ``series.gaps`` so the client does not have to
  infer them, and the page breaks the line at each one. Absence at the *edges* of
  the window — before the first sample, after the last — is not a space between
  two samples and so cannot live in ``gaps``; it travels as
  ``series.leading_gap`` / ``series.trailing_gap`` and the page shades it the
  same way, because to a reader it is the same fact.
* **An absent hour has no row.** :func:`hourly_rollup` mirrors ``stages/rollup.sql``:
  an hour with no samples produces no row at all, never a zero-height bar.
* **``sample_count`` travels with every aggregate**, next to the *expected* count
  for that hour at the current poll interval, so a partial hour is visibly
  partial. Expected counts come from :func:`energy_capture.timeutil.iter_local_hours`,
  so a 23- or 25-hour DST day is right without anything hardcoding 24.

The kWh math is **not reinvented here**: :func:`hourly_rollup` computes
``mean_watts * (sample_count * poll_interval_s) / 3.6e6`` for ``metric='watts'``
and ``None`` for every other metric, which is line for line what ``rollup.sql``
does. See :data:`KWH_FORMULA`. The SQL itself cannot be executed here — it is a
DuckDB query over Parquet files with two registered relations, while this reads
live SQLite rows — so this is a deliberate, documented **mirror** rather than a
second definition. :data:`KWH_FORMULA` is the string a future editor diffs
against ``rollup.sql`` when changing either one.

The ``/ui/data`` document
-------------------------

Every timestamp is an object ``{"utc": "2026-08-17T18:20:51.497894Z", "local":
"2026-08-17T14:20:51"}`` — canonical UTC plus the naive local wall clock, exactly
how the rest of the codebase talks about time (``timeutil``). Series points are
the compact form of the same pair: ``[ts_utc, ts_local, value]``.

``{``
  ``"generated": Stamp, "now": Stamp, "tz": str, "poll_interval_s": {...},``
  ``"process": {...},   # health, ingest mode, WS state, spool depth, poll ages``
  ``"spool": {...},     # path, readability, and the DATA EXTENT (oldest/newest)``
  ``"channels": [...],  # one per (source, device_id, channel_id) in the spool``
  ``"overlay": {...},   # <=3 watt channels for the overlay chart, with slots``
  ``"hvac": {...},      # the Bryant picture, enums decoded to words``
  ``"hourly": {...},    # local hour x channel: mean/min/max, n/expected, kwh``
  ``"errors": [...]     # anything that degraded, so the page never lies``
``}``

Nothing in here is expensive: the spool holds a few hundred to a few thousand
rows at a time and the pipeline is explicitly not performance-sensitive
(CLAUDE.md), so the aggregation is plain Python over one windowed SELECT. That
keeps *all* timezone logic in ``timeutil`` — there is no date arithmetic in SQL.
"""

from __future__ import annotations

import json
import math
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

from energy_capture import model, timeutil
from energy_capture.logging import get_logger

if TYPE_CHECKING:  # imported lazily at runtime; health imports this module back
    from energy_capture.config import Settings
    from energy_capture.health import StatusStore

__all__ = [
    "CHART_DEFAULT_WINDOW_S",
    "CHART_FUTURE_SKEW_S",
    "CHART_MAX_BUCKETS",
    "CHART_MAX_WINDOW_S",
    "CHART_MIN_WINDOW_S",
    "CHART_PRESETS_S",
    "CHART_RAW_MAX_WINDOW_S",
    "GAP_INTERVAL_FACTOR",
    "HOURLY_WINDOW_HOURS",
    "KWH_FORMULA",
    "UI_HVAC_PAGE_PATH",
    "UI_HVAC_DATA_PATH",
    "handle_ui_hvac",
    "render_hvac_page",
    "PAGE_CONTENT_TYPE",
    "SERIES_WINDOW_MINUTES",
    "UI_DATA_PATH",
    "UI_PAGE_PATH",
    "UI_PATHS",
    "ChartParamError",
    "ChartRequest",
    "build_snapshot",
    "chart_bucket_width_s",
    "chart_resolution_label",
    "decode_enum",
    "expected_samples",
    "expected_samples_for_local_day",
    "handle_ui_data",
    "hour_buckets",
    "open_readonly",
    "parse_chart_request",
    "render_page",
    "reset_label_cache",
    "reset_page_cache",
]

log = get_logger("dashboard")

#: The two routes this module owns. ``health.py`` dispatches on these and on
#: nothing else, so the existing ``DEFAULT_HEALTH_PATHS`` keep their behaviour.
UI_PAGE_PATH: str = "/ui"
UI_DATA_PATH: str = "/ui/data"
#: The HVAC cross-check screen: Bryant's account of the system against the
#: panel's. Its own page and payload rather than another card on ``/ui``, because
#: it answers one question and wants the whole width to do it (``hvacview``).
UI_HVAC_PAGE_PATH: str = "/ui/hvac"
UI_HVAC_DATA_PATH: str = "/ui/hvac/data"
UI_PATHS: frozenset[str] = frozenset(
    {UI_PAGE_PATH, UI_DATA_PATH, UI_HVAC_PAGE_PATH, UI_HVAC_DATA_PATH}
)

PAGE_CONTENT_TYPE: str = "text/html; charset=utf-8"

#: Package-relative location of the page. ``src/`` is copied wholesale into the
#: image *and* hatchling's wheel target is the whole ``src/energy_capture``
#: package (pyproject ``[tool.hatch.build.targets.wheel]``), so this file ships
#: in the wheel the container installs — the same way ``stages/rollup.sql`` does.
_PAGE_PACKAGE: str = "energy_capture"
_PAGE_RESOURCE: str = "static/dashboard.html"
_HVAC_PAGE_RESOURCE: str = "static/hvac.html"

#: How far apart two consecutive samples may be before the line BREAKS. 1.5x the
#: poll interval: one missed cycle is already a gap, and half an interval of
#: slack absorbs ordinary jitter without hiding a real outage.
GAP_INTERVAL_FACTOR: float = 1.5

#: "Live now" sparkline window.
SERIES_WINDOW_MINUTES: int = 30

#: "The math" table window.
HOURLY_WINDOW_HOURS: int = 6

# ------------------------------------------------------- the chart's window
#
# The overlay watts chart is the one thing on this page that can be moved
# through time. These are the whole of its contract; the page's preset buttons
# are :data:`CHART_PRESETS_S` and nothing else here knows about them.

#: Default chart window: the live view, unchanged (``SERIES_WINDOW_MINUTES``).
CHART_DEFAULT_WINDOW_S: int = SERIES_WINDOW_MINUTES * 60

#: Shortest and longest window ``?window_s=`` may ask for. 24h is the cap the
#: owner asked for; anything longer is clamped, not rejected, so a hand-typed
#: ``window_s=999999`` still answers with a day rather than an error.
CHART_MIN_WINDOW_S: int = 60
CHART_MAX_WINDOW_S: int = 86400

#: At or below this the chart returns RAW samples — the live view must stay
#: exactly as responsive as it is. Above it the server buckets (an hour at 30s
#: is 120 points per channel, which is still nothing; 24h is 2,880).
CHART_RAW_MAX_WINDOW_S: int = 3600

#: Upper bound on buckets across the window, so the payload stays small and the
#: SVG stays cheap. 24h / 600 ≈ 144s, which :func:`chart_bucket_width_s` rounds
#: up to a whole number of poll intervals (150s = 2.5 min at a 30s cadence).
CHART_MAX_BUCKETS: int = 600

#: A bucket is never narrower than this many poll intervals. At one interval per
#: bucket, ordinary poll jitter would leave real buckets empty and the page would
#: draw holes that are not holes — the opposite failure to fabricating data, and
#: just as much a lie.
CHART_MIN_BUCKET_INTERVALS: int = 2

#: How far into the future ``?end=`` may sit before it is a 400. A browser's
#: clock is not this process's clock; a minute of skew is not a bug.
CHART_FUTURE_SKEW_S: float = 60.0

#: The page's preset buttons: 30m / 1h / 6h / 24h.
CHART_PRESETS_S: tuple[int, ...] = (1800, 3600, 21600, 86400)

#: Query parameter names, in one place so the page and the parser agree.
CHART_PARAM_WINDOW: str = "window_s"
CHART_PARAM_END: str = "end"

#: Quoted verbatim from ``stages/rollup.sql`` / PLAN.md §2.5, so a reader can see
#: the two are the same statement and a future editor changing one notices the
#: other. Observed time only; never extrapolated across a gap.
KWH_FORMULA: str = "kwh = mean_watts * (sample_count * poll_interval_s) / 3.6e6"

#: Which metric a channel's sparkline plots, most interesting first. Enum metrics
#: come last: a line through enum codes is not a measurement, so the page draws
#: those as a step line and labels the word, never an average.
SERIES_METRIC_PRIORITY: tuple[str, ...] = (
    "watts",
    "amps",
    "indoor_temp_f",
    "outdoor_temp_f",
    "stage_pct",
    "humidity_pct",
    "cfm",
    "blower_rpm",
    "volts",
    "hz",
    "setpoint_cool_f",
    "setpoint_heat_f",
    "mode",
    "stage",
    "fan",
)

#: Coverage thresholds for the hourly table, as (floor_pct, status, word).
#: Status colours ship with the number and a word — never colour alone.
COVERAGE_LEVELS: tuple[tuple[float, str, str], ...] = (
    (98.0, "good", "complete"),
    (80.0, "warning", "thin"),
    (50.0, "serious", "sparse"),
    (0.0, "critical", "mostly missing"),
)

#: Freshness of a channel's latest sample, in multiples of its poll interval.
#: 3x is :data:`energy_capture.health.STALE_INTERVAL_MULTIPLIER` — the same
#: budget ``/healthz`` uses, so the page and the probe never disagree.
_LIVE_FACTOR: float = GAP_INTERVAL_FACTOR
_LATE_FACTOR: float = 3.0

_DEFAULT_BUSY_TIMEOUT_S: float = 2.0

# ---------------------------------------------------------------------- SQL
#
# Two statements, both pure SELECTs, both parameterised. There is deliberately no
# date arithmetic here: hour bucketing happens in Python via `timeutil`, because
# `strftime` in SQLite does not know about America/Kentucky/Louisville and
# re-deriving local time in SQL is the one bug this project cannot afford
# (spool/sqlite.py's module docstring makes the same argument).

#: Newest row per (source, device_id, channel_id, metric) — "latest value + age",
#: including channels that have gone silent and are therefore *outside* the
#: window query below. A window function rather than SQLite's bare-column
#: min/max trick, so what it returns is obvious rather than idiomatic.
_LATEST_SQL = """
SELECT source, device_id, channel_id, metric, unit, value, ts_utc, ts_local
FROM (
    SELECT source, device_id, channel_id, metric, unit, value, ts_utc, ts_local,
           ROW_NUMBER() OVER (
               PARTITION BY source, device_id, channel_id, metric
               ORDER BY ts_utc DESC
           ) AS rn
    FROM observations
)
WHERE rn = 1
"""

#: Every sample in [start, end). `ts_utc` is fixed-width ISO-8601 text, so a
#: string comparison IS a chronological comparison (spool/sqlite.py) and the
#: index on ts_utc applies.
_WINDOW_SQL = """
SELECT ts_utc, ts_local, source, device_id, channel_id, metric, unit, value
FROM observations
WHERE ts_utc >= ? AND ts_utc < ?
ORDER BY ts_utc
"""

#: The oldest and newest instants the spool actually holds — the chart's panning
#: limit, so the page can say "no data before 08:12" instead of drawing an empty
#: chart that looks like an outage. Two statements rather than
#: ``SELECT MIN(ts_utc), MAX(ts_utc)``: SQLite only rewrites a *single* bare
#: min/max into an index seek, so the combined form scans the table while these
#: two touch one index entry each.
_OLDEST_SQL = "SELECT ts_utc FROM observations ORDER BY ts_utc LIMIT 1"
_NEWEST_SQL = "SELECT ts_utc FROM observations ORDER BY ts_utc DESC LIMIT 1"

#: Rank the chart's candidate channels over the chart window WITHOUT hauling
#: every row into Python: 24h of watts is ~100k rows and only three channels are
#: drawn. ``MAX(value)`` is the same "highest watts observed in the window" rule
#: the overlay has always used. The leading column of ``ux_observations_dedupe``
#: is ``ts_utc``, so the range predicate is an index seek, not a table scan.
_CHART_RANK_SQL = """
SELECT source, device_id, channel_id,
       MAX(value) AS peak, MIN(value) AS trough, COUNT(*) AS samples, MIN(unit) AS unit
FROM observations
WHERE ts_utc >= ? AND ts_utc < ? AND metric = ?
GROUP BY source, device_id, channel_id
"""

#: Rows for the (at most three) channels the chart draws. The channel predicate
#: is spelled out as OR-ed triples rather than a row-value ``IN`` so it is
#: obvious that only ``ts_utc`` drives the index.
_CHART_POINTS_SQL_HEAD = """
SELECT ts_utc, ts_local, source, device_id, channel_id, metric, unit, value
FROM observations
WHERE ts_utc >= ? AND ts_utc < ? AND metric = ?
"""


def _chart_points_sql(channels: int) -> str:
    """:data:`_CHART_POINTS_SQL_HEAD` restricted to ``channels`` channel keys."""
    clause = " OR ".join(
        ["(source = ? AND device_id = ? AND channel_id = ?)"] * max(1, channels)
    )
    return f"{_CHART_POINTS_SQL_HEAD}  AND ({clause})\nORDER BY ts_utc\n"


# --------------------------------------------------------------- the page


_page_cache: str | None = None
_hvac_page_cache: str | None = None
_page_lock = threading.Lock()


def render_page() -> str:
    """``static/dashboard.html``, read once and cached in memory.

    Uses :func:`importlib.resources`, not ``__file__`` arithmetic, so it resolves
    inside the installed wheel in the container image as well as from a source
    checkout.
    """
    global _page_cache
    with _page_lock:
        if _page_cache is None:
            _page_cache = (
                resources.files(_PAGE_PACKAGE)
                .joinpath(_PAGE_RESOURCE)
                .read_text(encoding="utf-8")
            )
        return _page_cache


def render_hvac_page() -> str:
    """``static/hvac.html``, read once and cached — see :func:`render_page`."""
    global _hvac_page_cache
    with _page_lock:
        if _hvac_page_cache is None:
            _hvac_page_cache = (
                resources.files(_PAGE_PACKAGE)
                .joinpath(_HVAC_PAGE_RESOURCE)
                .read_text(encoding="utf-8")
            )
        return _hvac_page_cache


def reset_page_cache() -> None:
    """Drop the cached pages (tests; and a dev editing the HTML in place)."""
    global _page_cache, _hvac_page_cache
    with _page_lock:
        _page_cache = None
        _hvac_page_cache = None


def handle_ui_hvac(
    store: StatusStore | None = None,
    target: str | None = None,
    *,
    spool_path: Path | str | None = None,
    channel_map_path: Path | str | None = None,
    inventory_path: Path | str | None = None,
    energy_out_dir: Path | str | None = None,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> tuple[int, dict[str, Any]]:
    """``(status, document)`` for ``GET /ui/hvac/data``.

    Always 200: an unparseable ``?window_s=`` falls back to the default rather
    than 400ing, because this is a diagnostic screen and the default window is a
    more useful answer than an error. Failures below the top level land in
    ``errors`` the way ``/ui/data``'s do.
    """
    from energy_capture import hvacview
    from energy_capture.stages import dailystore

    errors: list[str] = []
    reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()

    resolved = settings
    if resolved is None:
        try:
            from energy_capture.config import get_settings

            resolved = get_settings()
        except Exception as exc:  # pragma: no cover - configuration must not break the page
            errors.append(f"settings unavailable: {type(exc).__name__}: {exc}")

    if spool_path is None:
        spool_path = getattr(resolved, "spool_db_path", None)
    if channel_map_path is None:
        channel_map_path = Path("config/channel_map.json")
    if inventory_path is None:
        inventory_path = getattr(resolved, "blackstart_inventory_path", None)

    window_s, clamped = hvacview.parse_window_s(_query_value(target, "window_s"))
    # The day-grain dataset lives beside the spool, not in it (rule 6).
    if energy_out_dir is not None:
        daily_dir: Path | None = Path(energy_out_dir)
    else:
        spool_dir = getattr(resolved, "spool_dir", None)
        daily_dir = Path(spool_dir) / dailystore.LOCAL_SUBDIR if spool_dir else None
    labels = _labels(channel_map_path, inventory_path, errors)
    poll_interval_s = int(getattr(resolved, "poll_interval_s", 30) or 30)
    bryant_interval_s = int(getattr(resolved, "bryant_poll_interval_s", 30) or 30)

    comparison: dict[str, Any]
    if spool_path is None:
        errors.append("no spool path is configured")
        comparison = hvacview.hvac_comparison(
            None, labels, now=reference, window_s=window_s, clamped=clamped, errors=errors
        )
    else:
        try:
            with open_readonly(spool_path) as conn:
                comparison = hvacview.hvac_comparison(
                    conn,
                    labels,
                    now=reference,
                    window_s=window_s,
                    clamped=clamped,
                    poll_interval_s=poll_interval_s,
                    bryant_interval_s=bryant_interval_s,
                    out_dir=daily_dir,
                    errors=errors,
                )
        except Exception as exc:
            errors.append(f"spool unreadable: {type(exc).__name__}: {exc}")
            comparison = hvacview.hvac_comparison(
                None, labels, now=reference, window_s=window_s, clamped=clamped, errors=errors
            )

    document = {
        "generated": _stamp(reference),
        "tz": timeutil.tz_name(),
        "refresh_s": 15,
        "hvac": comparison,
        "errors": errors,
    }
    return (200, document)


def _query_value(target: str | None, name: str) -> str | None:
    """One query parameter out of a raw request target, or ``None``."""
    if not target or "?" not in target:
        return None
    query = target.split("?", 1)[1].split("#", 1)[0]
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key == name:
            return value
    return None


# ------------------------------------------------------------ time helpers


def _stamp(ts: datetime | None) -> dict[str, str] | None:
    """``{"utc": ..., "local": ...}`` — canonical instant + naive wall clock."""
    if ts is None:
        return None
    aware = timeutil.ensure_utc(ts)
    return {
        "utc": timeutil.format_utc(aware),
        "local": timeutil.to_local_naive(aware).isoformat(timespec="seconds"),
    }


def _age_s(now: datetime, ts: datetime | None) -> float | None:
    if ts is None:
        return None
    return round((timeutil.ensure_utc(now) - timeutil.ensure_utc(ts)).total_seconds(), 1)


def _parse_utc(text: Any) -> datetime | None:
    """Parse a stored/serialised UTC timestamp; ``None`` if it is not one."""
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


# ------------------------------------------------------- the chart request


class ChartParamError(ValueError):
    """A ``/ui/data`` chart parameter that must be answered with **400**.

    Garbage is rejected rather than silently replaced with the default: a chart
    that quietly shows a different window than the one asked for is a chart that
    lies about what it is showing.
    """

    def __init__(self, param: str, message: str, value: Any = None) -> None:
        super().__init__(f"{param}: {message}")
        self.param = param
        self.message = message
        self.value = value

    def as_document(self) -> dict[str, Any]:
        """The JSON body for the 400."""
        return {
            "error": "bad chart parameter",
            "parameter": self.param,
            "detail": self.message,
            "value": None if self.value is None else str(self.value),
            "accepts": {
                CHART_PARAM_WINDOW: (
                    f"integer seconds, {CHART_MIN_WINDOW_S}..{CHART_MAX_WINDOW_S} "
                    f"(default {CHART_DEFAULT_WINDOW_S}; longer values are clamped)"
                ),
                CHART_PARAM_END: (
                    "ISO-8601 instant at the right edge of the window, e.g. "
                    "2026-08-16T18:30:00Z (default: now, which means live)"
                ),
            },
        }


@dataclass(frozen=True, slots=True)
class ChartRequest:
    """A validated request for the overlay chart's window.

    ``end is None`` **is** what live means: the window's right edge is resolved
    to the snapshot's own ``now`` at build time and therefore follows it. A
    pinned ``end`` is history and does not move when the page refreshes.
    """

    window_s: int = CHART_DEFAULT_WINDOW_S
    end: datetime | None = None
    #: What the caller literally asked for, before clamping — echoed back so the
    #: page can say "clamped to 24h" instead of silently disagreeing with itself.
    requested_window_s: int | None = None
    clamped: bool = False

    @property
    def live(self) -> bool:
        return self.end is None


def parse_chart_request(
    query: str | Mapping[str, Any] | None, *, now: datetime | None = None
) -> ChartRequest:
    """Parse ``?window_s=&end=`` into a :class:`ChartRequest`.

    Accepts a full request target (``/ui/data?window_s=3600``), a bare query
    string, or an already-parsed mapping. Unknown parameters are **ignored** —
    ``/ui/data?since=now`` has always been a 200 and stays one. Raises
    :class:`ChartParamError` for anything malformed.
    """
    items: list[tuple[str, str]] = []
    if isinstance(query, Mapping):
        items = [(str(k), "" if v is None else str(v)) for k, v in query.items()]
    elif query:
        raw = str(query)
        if "?" in raw:
            raw = raw.split("?", 1)[1]
        raw = raw.split("#", 1)[0].lstrip("?")
        items = parse_qsl(raw, keep_blank_values=True)

    # Last one wins, the way every HTTP stack reads a repeated parameter.
    values = {key: value for key, value in items}

    window_s = CHART_DEFAULT_WINDOW_S
    requested: int | None = None
    clamped = False
    if CHART_PARAM_WINDOW in values:
        text = values[CHART_PARAM_WINDOW].strip()
        if not text.isdigit():
            raise ChartParamError(
                CHART_PARAM_WINDOW,
                "must be a whole number of seconds (a positive integer)",
                values[CHART_PARAM_WINDOW],
            )
        requested = int(text)
        if requested <= 0:
            raise ChartParamError(
                CHART_PARAM_WINDOW, "must be greater than zero seconds", requested
            )
        window_s = min(CHART_MAX_WINDOW_S, max(CHART_MIN_WINDOW_S, requested))
        clamped = window_s != requested

    end: datetime | None = None
    if CHART_PARAM_END in values:
        text = values[CHART_PARAM_END].strip()
        parsed = _parse_utc(text) if text else None
        if parsed is None:
            raise ChartParamError(
                CHART_PARAM_END,
                "must be an ISO-8601 instant, e.g. 2026-08-16T18:30:00Z",
                values[CHART_PARAM_END],
            )
        reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
        if (parsed - reference).total_seconds() > CHART_FUTURE_SKEW_S:
            raise ChartParamError(
                CHART_PARAM_END,
                (
                    "is in the future — there is no data there. Allowed skew is "
                    f"{CHART_FUTURE_SKEW_S:g}s; omit `end` to follow now"
                ),
                text,
            )
        end = parsed

    return ChartRequest(
        window_s=window_s, end=end, requested_window_s=requested, clamped=clamped
    )


def chart_bucket_width_s(
    window_s: int, poll_interval_s: float, *, max_buckets: int = CHART_MAX_BUCKETS
) -> float:
    """Bucket width for a window: a whole number of poll intervals, >= 2 of them.

    Rounding up to a multiple of the poll interval is what makes
    ``expected samples per bucket`` an exact integer, which is what makes
    "partial" mean something. 24h at a 30s cadence gives 150s (2.5 min, 576
    buckets); 6h gives 60s (360 buckets).
    """
    step = float(poll_interval_s) if poll_interval_s > 0 else 30.0
    needed = math.ceil(window_s / (max(1, max_buckets) * step))
    return step * max(CHART_MIN_BUCKET_INTERVALS, needed)


def chart_resolution_label(
    mode: str, bucket_s: float | None, poll_interval_s: float
) -> str:
    """What one mark on the x axis IS — the axis must never imply 30s at 24h."""
    if mode == "raw" or not bucket_s:
        return f"{poll_interval_s:g}s samples"
    if bucket_s < 60:
        return f"{bucket_s:g}-second buckets"
    return f"{bucket_s / 60:g}-minute buckets"


def handle_ui_data(
    store: StatusStore | None = None, target: str | None = None, **kwargs: Any
) -> tuple[int, dict[str, Any]]:
    """``(status, document)`` for ``GET /ui/data`` — the whole route, parsing included.

    This is the entry point a server should call, because it is the only place
    that can answer **400**: :func:`build_snapshot` takes an already-valid
    :class:`ChartRequest` and has no opinion about query strings. ``target`` may
    be the raw request target (``/ui/data?window_s=86400``) or ``None``, which is
    the no-parameter request and returns the document it always did.
    """
    try:
        chart = parse_chart_request(target, now=kwargs.get("now"))
    except ChartParamError as exc:
        return (400, exc.as_document())
    return (200, build_snapshot(store, chart=chart, **kwargs))


# -------------------------------------------------------- expected counts


def expected_samples(seconds: float, poll_interval_s: float) -> int:
    """How many samples a span of ``seconds`` should hold at this cadence.

    A full hour at 30s is 3600/30 = 120. Rounded (not floored) because the poll
    loop's phase relative to the hour boundary is arbitrary, so a partial span
    holds ``span/interval`` samples give or take one; never below 1, because a
    span that has started should have produced something.
    """
    if poll_interval_s <= 0:
        raise ValueError(f"poll_interval_s must be positive, got {poll_interval_s!r}")
    if seconds <= 0:
        return 0
    return max(1, round(seconds / poll_interval_s))


def expected_samples_for_local_day(local_day: date, poll_interval_s: float) -> int:
    """Samples one channel should produce over a whole LOCAL day.

    Derived from :func:`timeutil.iter_local_hours`, which yields the day's real
    hours — 23 on spring-forward, 25 on fall-back — so this is 2,760 / 2,880 /
    3,000 at a 30s cadence without anything here knowing what DST is.
    """
    return sum(
        expected_samples((hour.end_utc - hour.start_utc).total_seconds(), poll_interval_s)
        for hour in timeutil.iter_local_hours(local_day)
    )


def hour_buckets(now: datetime, hours: int) -> list[timeutil.LocalHour]:
    """The last ``hours`` physical local hours, newest last, in-progress included.

    Built by walking :func:`timeutil.iter_local_hours` over the local days the
    window touches — the same construction ``stages/rollup.py`` registers as the
    ``rollup_hours`` relation — so the fall-back day's two 01:00 hours are two
    distinct buckets and the spring-forward day has no 02:00 bucket at all.
    """
    if hours < 1:
        raise ValueError(f"hours must be >= 1, got {hours}")
    end = timeutil.ensure_utc(now)
    start = timeutil.utc_hour_start(end) - timedelta(hours=hours - 1)
    first_day = timeutil.local_date_of(start)
    last_day = timeutil.local_date_of(end)
    out: list[timeutil.LocalHour] = []
    for day in timeutil.iter_local_dates(first_day, last_day):
        for hour in timeutil.iter_local_hours(day):
            if hour.end_utc > start and hour.start_utc <= end:
                out.append(hour)
    return out[-hours:]


def _coverage_status(pct: float | None) -> tuple[str, str]:
    """``(status, word)`` for a coverage percentage. Never colour alone."""
    if pct is None:
        return ("critical", "no samples")
    for floor, status, word in COVERAGE_LEVELS:
        if pct >= floor:
            return (status, word)
    return ("critical", "mostly missing")


# ----------------------------------------------------------- read-only spool


@contextmanager
def open_readonly(
    path: Path | str, *, busy_timeout_s: float = _DEFAULT_BUSY_TIMEOUT_S
) -> Iterator[sqlite3.Connection]:
    """A **read-only** connection to the spool, closed on the way out.

    ``mode=ro`` (a SQLite URI, not a convention) plus ``PRAGMA query_only`` makes
    a write attempt through this handle fail rather than corrupt the poller's
    database — a test asserts exactly that. The busy timeout is short on purpose:
    a dashboard request must give up quickly rather than sit on a lock while the
    poll loop is trying to commit. In WAL mode a reader does not contend with the
    writer at all, so this is the belt to WAL's braces.

    One SQLite constraint worth knowing: a **WAL** database cannot be opened
    read-only unless its ``-shm`` file already exists, which it does exactly when
    a writer has the database open. In the deployed process that is always true
    (this page is served by the same process that runs the poll loop). With the
    poller stopped it raises, and :func:`build_snapshot` turns that into a
    message on the page rather than an exception — deliberately, because
    ``immutable=1`` would make it openable at the price of returning garbage the
    moment a writer came back.
    """
    uri = f"{Path(path).absolute().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=busy_timeout_s, check_same_thread=False)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_s * 1000)}")
        conn.execute("PRAGMA query_only=TRUE")
        yield conn
    finally:
        conn.close()


@dataclass(frozen=True, slots=True)
class _Sample:
    """One spooled row, decoded. Deliberately tiny: there are thousands."""

    ts_utc: datetime
    ts_local: str
    source: str
    device_id: str
    channel_id: str
    metric: str
    unit: str
    value: float

    @property
    def channel_key(self) -> tuple[str, str, str]:
        return (self.source, self.device_id, self.channel_id)


def _row_to_sample(row: sqlite3.Row) -> _Sample:
    return _Sample(
        ts_utc=timeutil.ensure_utc(datetime.fromisoformat(row["ts_utc"])),
        ts_local=str(row["ts_local"]),
        source=row["source"],
        device_id=row["device_id"],
        channel_id=row["channel_id"],
        metric=row["metric"],
        unit=row["unit"],
        value=float(row["value"]),
    )


def _spool_extent(conn: sqlite3.Connection) -> dict[str, Any]:
    """Oldest and newest ``ts_utc`` in the spool — how far the chart can be panned.

    Without this the page cannot tell "the collector was down" from "the spool
    never held that far back", and a user panning past the beginning gets an empty
    chart that looks exactly like an outage. Two single-row index seeks (see
    :data:`_OLDEST_SQL`), so this stays cheap on a spool holding a day of rows.
    """
    oldest_row = conn.execute(_OLDEST_SQL).fetchone()
    newest_row = conn.execute(_NEWEST_SQL).fetchone()
    oldest = _parse_utc(oldest_row[0]) if oldest_row else None
    newest = _parse_utc(newest_row[0]) if newest_row else None
    return {
        "oldest": _stamp(oldest),
        "newest": _stamp(newest),
        "span_s": (
            round((newest - oldest).total_seconds(), 1)
            if oldest is not None and newest is not None
            else None
        ),
    }


def _read_chart_window(
    conn: sqlite3.Connection,
    *,
    start: datetime,
    end: datetime,
    metric: str = model.POWER_METRIC,
    limit: int = 3,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], list[_Sample]]]:
    """``(ranked channels, samples for the top ``limit``)`` over the chart window.

    Two statements on purpose. A 24h window is ~100k rows and the chart draws at
    most three channels, so the ranking — the same "highest watts observed in the
    window" rule the overlay has always used — is done as a GROUP BY in SQLite and
    only the chosen channels' rows are materialised in Python. The upper bound is
    ``end + 1s``, matching the live window query, so a sample landing exactly on
    ``now`` is in the chart rather than one refresh late.
    """
    bounds = (
        timeutil.format_utc(start),
        timeutil.format_utc(end + timedelta(seconds=1)),
        metric,
    )
    ranked: list[dict[str, Any]] = []
    for row in conn.execute(_CHART_RANK_SQL, bounds):
        key = (row["source"], row["device_id"], row["channel_id"])
        ranked.append(
            {
                "key": key,
                "key_str": "{}/{}/{}".format(*key),
                "peak": float(row["peak"]),
                "samples": int(row["samples"]),
                "unit": row["unit"],
            }
        )
    # Same ordering as the overlay has always used: peak descending, then key
    # descending, so the selection is stable for a given window.
    ranked.sort(key=lambda entry: (entry["peak"], entry["key_str"]), reverse=True)

    chosen = ranked[:limit]
    samples: dict[tuple[str, str, str], list[_Sample]] = {entry["key"]: [] for entry in chosen}
    if chosen:
        params: list[Any] = [*bounds]
        for entry in chosen:
            params.extend(entry["key"])
        for row in conn.execute(_chart_points_sql(len(chosen)), params):
            sample = _row_to_sample(row)
            samples[sample.channel_key].append(sample)
    return ranked, samples


# ------------------------------------------------------------------ labels


#: ``(paths+mtimes) -> (labels, errors)``. The semantic layer is two JSON files
#: that change when a human edits them, and this page asks for them every 5s;
#: re-reading (and re-logging) both on every request would be silly. Keyed on
#: mtime, so an edit is picked up on the next refresh without a restart.
_labels_cache: tuple[tuple[Any, ...], dict[tuple[str, str, str], dict[str, Any]], tuple[str, ...]] | None = None
_labels_lock = threading.Lock()


def reset_label_cache() -> None:
    """Forget the cached channel labels (tests; and a hand-edited map)."""
    global _labels_cache
    with _labels_lock:
        _labels_cache = None


def _mtime(path: Path | str | None) -> float | None:
    try:
        return Path(path).stat().st_mtime if path is not None else None
    except OSError:
        return None


def _labels(
    map_path: Path | str | None, inventory_path: Path | str | None, errors: list[str]
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """:func:`_load_labels`, memoised on the two files' mtimes."""
    global _labels_cache
    key = (str(map_path), _mtime(map_path), str(inventory_path), _mtime(inventory_path))
    with _labels_lock:
        cached = _labels_cache
        if cached is not None and cached[0] == key:
            errors.extend(cached[2])
            return cached[1]
    local_errors: list[str] = []
    labels = _load_labels(map_path, inventory_path, local_errors)
    with _labels_lock:
        _labels_cache = (key, labels, tuple(local_errors))
    errors.extend(local_errors)
    return labels


def _load_labels(
    map_path: Path | str | None, inventory_path: Path | str | None, errors: list[str]
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """``(source, device_id, channel_id)`` -> label metadata from the semantic layer.

    Reuses ``stages/dim`` so the page names a channel exactly the way
    ``dim_channel`` does (explicit ``label`` wins; otherwise blackstart supplies
    it — PLAN.md §9). Every failure degrades to "fewer labels", never to an
    error page: an unlabelled channel is still shown, marked, and counted.
    """
    if map_path is None:
        return {}
    labels: dict[tuple[str, str, str], dict[str, Any]] = {}
    try:
        from energy_capture.stages import dim
    except Exception as exc:  # pragma: no cover - import-time failure only
        errors.append(f"channel labels unavailable: {type(exc).__name__}: {exc}")
        return {}

    try:
        entries = dim.load_channel_map(map_path)
    except Exception as exc:
        errors.append(f"channel_map could not be read ({exc.__class__.__name__}); channels are shown unlabelled")
        return {}

    for entry in entries:
        labels[entry.key] = {
            "label": entry.label or entry.short_label,
            "short_label": entry.short_label or entry.label,
            "category": entry.category,
            "panel": entry.panel,
            "blackstart_device_id": entry.blackstart_device_id,
            "label_source": "channel_map" if entry.label or entry.short_label else None,
            "placeholder": entry.placeholder,
            # Which meter a whole-system comparison should use when a source
            # exposes several (the house, not the barn) — read by the meter card.
            "primary": entry.primary,
        }

    # Anything whose label lives in blackstart's inventory: resolve it the way
    # build-dim does. Optional — a missing inventory just means those channels
    # keep the fallback label below.
    needs_inventory = any(e.blackstart_device_id for e in entries)
    if not needs_inventory or inventory_path is None:
        return labels
    try:
        inventory = dim.load_inventory(inventory_path)
        rows = dim.resolve_rows(
            entries, inventory, updated_at=timeutil.now_utc(), subject="channel_map"
        )
    except Exception as exc:
        errors.append(
            f"blackstart inventory not joined ({exc.__class__.__name__}); "
            "channels named only by blackstart_device_id fall back to their channel_id"
        )
        return labels
    for row in rows:
        labels[row.key] = {
            "label": row.label,
            "short_label": row.short_label,
            "category": row.category,
            "panel": row.panel,
            "blackstart_device_id": row.blackstart_device_id,
            "label_source": "blackstart" if row.blackstart_device_id else "channel_map",
            "placeholder": False,
            # `primary` is map-only metadata — it never reaches the dim_channel
            # Parquet, so a resolved row does not carry it. This pass rewrites
            # every key, not only the blackstart-labelled ones, so the flag has
            # to be carried across or it is silently lost.
            "primary": bool(labels.get(row.key, {}).get("primary", False)),
        }
    return labels


def _describe_channel(
    key: tuple[str, str, str], labels: Mapping[tuple[str, str, str], Mapping[str, Any]]
) -> dict[str, Any]:
    """Naming for one channel. An unmapped channel is shown, never hidden."""
    source, device_id, channel_id = key
    entry = labels.get(key)
    if entry is None:
        return {
            "label": channel_id,
            "short_label": channel_id,
            "category": None,
            "panel": None,
            "label_source": "channel_id",
            "unmapped": True,
            "unmapped_note": (
                f"no entry in channel_map.json for {source}/{device_id}/{channel_id} — "
                "run `energycap discover` and add one"
            ),
        }
    label = entry.get("label") or channel_id
    return {
        "label": label,
        "short_label": entry.get("short_label") or label,
        "category": entry.get("category"),
        "panel": entry.get("panel"),
        "label_source": entry.get("label_source") or "channel_id",
        "unmapped": False,
        "unmapped_note": None,
    }


# -------------------------------------------------------------- enum decode


def decode_enum(metric: str, code: float | int | None) -> dict[str, Any]:
    """Decode an enum-metric code to its word via the tables in ``sources/bryant``.

    A human is never shown a bare integer (the whole point of the append-only
    tables). A code that is not in the table is reported as unknown *with* the
    integer — never silently rendered as something plausible.
    """
    result: dict[str, Any] = {"code": None, "word": None, "known": False, "table": None}
    if code is None:
        return result
    try:
        from energy_capture.sources.bryant import ENUM_TABLES
    except Exception:  # pragma: no cover - import-time failure only
        return result
    table = ENUM_TABLES.get(metric)
    result["code"] = int(code)
    if table is None:
        return result
    result["table"] = f"{metric} codes"
    for word, value in table.items():
        if value == int(code):
            result["word"] = word
            result["known"] = True
            break
    return result


# ------------------------------------------------------------------ series


def _series_gaps(
    samples: Sequence[_Sample], gap_threshold_s: float, poll_interval_s: float
) -> list[dict[str, Any]]:
    """Every break longer than ``gap_threshold_s``, as explicit facts.

    The client also detects these from the point deltas; sending them too means
    the page never has to *infer* that the collector was down, and the count of
    missing samples is computed once, here, where the poll interval is known.
    """
    gaps: list[dict[str, Any]] = []
    for previous, current in zip(samples, samples[1:]):
        delta = (current.ts_utc - previous.ts_utc).total_seconds()
        if delta <= gap_threshold_s:
            continue
        gaps.append(
            {
                "after": _stamp(previous.ts_utc),
                "before": _stamp(current.ts_utc),
                "seconds": round(delta, 1),
                "missing_samples": max(0, round(delta / poll_interval_s) - 1),
            }
        )
    return gaps


def _edge_gap(
    edge: str,
    start: datetime,
    end: datetime,
    gap_threshold_s: float,
    poll_interval_s: float,
) -> dict[str, Any] | None:
    """Absence at the **edge** of the window — before the first sample, or after the last.

    :func:`_series_gaps` can only see the space *between* two observed samples, so a
    channel that produced nothing for the first twenty minutes of the window would
    draw as a short line with blank space beside it — visually indistinguishable
    from "the chart simply starts here". These two facts let the page mark that
    space as the absence it is.

    They are deliberately kept **out of** ``gaps``: ``gaps`` means exactly "between
    two observed samples", the count is quoted as such on the page, and an edge span
    is bounded by an arbitrary window edge rather than by a measurement. ``None``
    when the edge is shorter than one gap threshold, i.e. when there is nothing to
    report.
    """
    seconds = round((end - start).total_seconds(), 1)
    if seconds <= gap_threshold_s:
        return None
    return {
        "edge": edge,
        "after": _stamp(start),
        "before": _stamp(end),
        "seconds": seconds,
        # The whole span is unobserved (unlike an interior gap, which is bounded by a
        # sample at each end), so nothing is subtracted here.
        "missing_samples": max(0, round(seconds / poll_interval_s)),
    }


def _build_series(
    samples: Sequence[_Sample],
    *,
    metric: str,
    unit: str,
    poll_interval_s: float,
    window_start: datetime,
    now: datetime,
) -> dict[str, Any]:
    """One channel's recent series — observed samples only, gaps left as gaps."""
    gap_threshold_s = round(poll_interval_s * GAP_INTERVAL_FACTOR, 1)
    points = [[timeutil.format_utc(s.ts_utc), s.ts_local, s.value] for s in samples]
    values = [s.value for s in samples]
    observed = len(samples)
    expected = expected_samples((now - window_start).total_seconds(), poll_interval_s)
    return {
        "metric": metric,
        "unit": unit,
        "is_enum": metric in model.ENUM_METRICS,
        "points": points,
        "point_format": ["ts_utc", "ts_local", "value"],
        "gap_threshold_s": gap_threshold_s,
        "gaps": _series_gaps(samples, gap_threshold_s, poll_interval_s),
        # Absence at the two window edges, which `gaps` cannot express (see _edge_gap).
        "leading_gap": (
            _edge_gap(
                "before_first_sample",
                window_start,
                samples[0].ts_utc,
                gap_threshold_s,
                poll_interval_s,
            )
            if samples
            else None
        ),
        "trailing_gap": (
            _edge_gap(
                "after_last_sample",
                samples[-1].ts_utc,
                now,
                gap_threshold_s,
                poll_interval_s,
            )
            if samples
            else None
        ),
        "window_start": _stamp(window_start),
        "window_end": _stamp(now),
        "sample_count": observed,
        "expected_samples": expected,
        "coverage_pct": round(100.0 * observed / expected, 1) if expected else None,
        "zero_samples": sum(1 for v in values if v == 0.0),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "note": (
            "observed samples only — no interpolation, no zero fill. Consecutive "
            f"points more than {gap_threshold_s}s apart are a gap in collection."
        ),
    }


# ----------------------------------------------------------------- buckets


def _align_to_bucket(ts: datetime, bucket_s: float) -> datetime:
    """Floor an instant to a bucket boundary, **on ``ts_utc``** (CLAUDE.md rule 3).

    Boundaries are epoch multiples of ``bucket_s``, so they do not move when the
    window slides (a refresh only ever changes the last bucket) and they know
    nothing about the local wall clock. That last part is the whole point: on the
    DST fall-back day two different instants render as ``01:30`` local, and a
    bucket keyed on that label would merge an hour of EDT with an hour of EST.
    """
    epoch = timeutil.ensure_utc(ts).timestamp()
    return datetime.fromtimestamp(math.floor(epoch / bucket_s) * bucket_s, tz=timeutil.UTC)


def _bucket_series(
    samples: Sequence[_Sample],
    *,
    metric: str,
    unit: str,
    poll_interval_s: float,
    window_start: datetime,
    window_end: datetime,
    bucket_s: float,
) -> dict[str, Any]:
    """Bucket one channel's samples into fixed spans — holes preserved as holes.

    ``window_start`` must already be bucket-aligned (:func:`_align_to_bucket`).

    Three invariants, each of which is a way this function could fabricate data
    and does not:

    * a bucket with **no** samples emits ``mean``/``min``/``max`` of ``None`` and
      ``sample_count`` 0 — it is a hole, and the page breaks the line there. Not
      0 W, not the previous bucket's value, not an interpolation;
    * the mean is ``sum(values) / len(values)`` — the mean of the samples that
      EXIST. Dividing by ``expected`` would drag every partial bucket toward zero,
      which is exactly the error of extrapolating kWh across a gap (PLAN.md §2.5);
    * ``sample_count`` and ``expected`` ride on every bucket, so a bucket built
      from one sample out of five is visibly not the same fact as a full one.
    """
    start_epoch = timeutil.ensure_utc(window_start).timestamp()
    end_epoch = timeutil.ensure_utc(window_end).timestamp()
    count = max(1, math.ceil((end_epoch - start_epoch) / bucket_s))

    grouped: list[list[float]] = [[] for _ in range(count)]
    for sample in samples:
        index = int((sample.ts_utc.timestamp() - start_epoch) // bucket_s)
        if 0 <= index < count:
            grouped[index].append(sample.value)

    points: list[list[Any]] = []
    holes: list[dict[str, Any]] = []
    run_start: int | None = None
    run_missing = 0
    empty = partial = 0

    def close_run(end_index: int) -> None:
        nonlocal run_start, run_missing
        if run_start is None:
            return
        first = datetime.fromtimestamp(start_epoch + run_start * bucket_s, tz=timeutil.UTC)
        last = datetime.fromtimestamp(
            min(start_epoch + end_index * bucket_s, end_epoch), tz=timeutil.UTC
        )
        holes.append(
            {
                "start": _stamp(first),
                "end": _stamp(last),
                "buckets": end_index - run_start,
                "seconds": round((last - first).total_seconds(), 1),
                "missing_samples": run_missing,
            }
        )
        run_start, run_missing = None, 0

    for index, values in enumerate(grouped):
        bucket_start_epoch = start_epoch + index * bucket_s
        bucket_start = datetime.fromtimestamp(bucket_start_epoch, tz=timeutil.UTC)
        span_s = min(bucket_start_epoch + bucket_s, end_epoch) - bucket_start_epoch
        expected = expected_samples(span_s, poll_interval_s)
        observed = len(values)
        if observed:
            close_run(index)
            if observed < expected:
                partial += 1
            points.append(
                [
                    timeutil.format_utc(bucket_start),
                    timeutil.to_local_naive(bucket_start).isoformat(timespec="seconds"),
                    sum(values) / observed,  # the mean of what EXISTS
                    min(values),
                    max(values),
                    observed,
                    expected,
                ]
            )
        else:
            empty += 1
            if run_start is None:
                run_start = index
            run_missing += expected
            points.append(
                [
                    timeutil.format_utc(bucket_start),
                    timeutil.to_local_naive(bucket_start).isoformat(timespec="seconds"),
                    None,  # an empty bucket is a HOLE, explicitly
                    None,
                    None,
                    0,
                    expected,
                ]
            )
    close_run(count)

    values_seen = [value for bucket in grouped for value in bucket]
    expected_total = sum(point[6] for point in points)
    observed_total = len(values_seen)
    return {
        "metric": metric,
        "unit": unit,
        "is_enum": metric in model.ENUM_METRICS,
        "mode": "bucketed",
        "points": points,
        "point_format": [
            "bucket_start_utc",
            "bucket_start_local",
            "mean",
            "min",
            "max",
            "sample_count",
            "expected",
        ],
        "bucket_s": bucket_s,
        "bucket_count": count,
        "empty_buckets": empty,
        "partial_buckets": partial,
        "holes": holes,
        # The break rule in bucketed mode is `mean is null`, not a time delta; this
        # travels anyway so a client that only knows the raw shape still breaks
        # the line in the right places.
        "gap_threshold_s": round(bucket_s * GAP_INTERVAL_FACTOR, 1),
        "gaps": [],
        "leading_gap": None,
        "trailing_gap": None,
        "window_start": _stamp(window_start),
        "window_end": _stamp(window_end),
        "sample_count": observed_total,
        "expected_samples": expected_total,
        "coverage_pct": (
            round(100.0 * observed_total / expected_total, 1) if expected_total else None
        ),
        "zero_samples": sum(1 for value in values_seen if value == 0.0),
        "min": min(values_seen) if values_seen else None,
        "max": max(values_seen) if values_seen else None,
        "note": (
            f"{bucket_s:g}s buckets aligned on ts_utc. An empty bucket is a HOLE "
            "(mean null, sample_count 0) — never a zero and never interpolated; a "
            "partial bucket carries the mean of the samples that exist and its own "
            "count, never a mean divided by the expected count."
        ),
    }


# ------------------------------------------------------------------ hourly


def hourly_rollup(
    samples: Iterable[_Sample],
    buckets: Sequence[timeutil.LocalHour],
    *,
    now: datetime,
    interval_for_source: Mapping[str, int],
    default_interval_s: int,
) -> list[dict[str, Any]]:
    """Aggregate the spool by local hour x channel x metric — ``rollup.sql`` in Python.

    Mirrors ``stages/rollup.sql`` step for step:

    * bucket on ``hour_start_utc``, **not** the naive local hour, so the two
      fall-back 01:00 hours stay two rows (DEVIATIONS.md #1);
    * carry ``local_hour_start`` along as the human-readable (that day,
      deliberately ambiguous) label;
    * exclude ``model.DAY_GRAIN_METRICS`` (CLAUDE.md rule 6);
    * no gap filling of any kind — an hour with no samples produces **no row**;
    * ``kwh`` only for ``metric='watts'``, ``None`` (never 0) for the rest.

    Two steps of the SQL are structurally unnecessary here and are therefore
    absent rather than reimplemented: the ``deduped`` CTE (the spool's UNIQUE
    index on ``model.DEDUPE_KEY`` makes duplicates impossible) and the
    ``value IS NOT NULL`` filter (the column is ``NOT NULL``).
    """
    starts = [bucket.start_utc for bucket in buckets]
    index: dict[datetime, timeutil.LocalHour] = {b.start_utc: b for b in buckets}
    groups: dict[tuple[datetime, str, str, str, str], list[_Sample]] = {}
    if not starts:
        return []
    lowest, highest = starts[0], buckets[-1].end_utc

    for sample in samples:
        if sample.metric in model.DAY_GRAIN_METRICS:
            continue
        if not (lowest <= sample.ts_utc < highest):
            continue
        bucket_start = timeutil.utc_hour_start(sample.ts_utc)
        if bucket_start not in index:
            continue
        key = (
            bucket_start,
            sample.source,
            sample.device_id,
            sample.channel_id,
            sample.metric,
        )
        groups.setdefault(key, []).append(sample)

    rows: list[dict[str, Any]] = []
    for key in sorted(groups, key=lambda k: (k[0], k[1], k[2], k[3], k[4])):
        bucket_start, source, device_id, channel_id, metric = key
        bucket = index[bucket_start]
        members = groups[key]
        values = [s.value for s in members]
        count = len(values)
        mean = sum(values) / count
        interval = float(interval_for_source.get(source, default_interval_s))

        # Expected count for THIS hour: its real span (always 3600s — every US
        # DST offset is a whole number of hours) clipped to now for the hour that
        # is still in progress, so a live hour is not scored as if it were over.
        span_end = min(bucket.end_utc, timeutil.ensure_utc(now))
        span_s = (span_end - bucket.start_utc).total_seconds()
        expected = expected_samples(span_s, interval)
        in_progress = bucket.end_utc > timeutil.ensure_utc(now)
        coverage = round(100.0 * count / expected, 1) if expected else None
        status, word = ("in_progress", "still filling") if in_progress else _coverage_status(coverage)

        rows.append(
            {
                "hour_start": _stamp(bucket.start_utc),
                "local_hour_start": bucket.local_start.isoformat(timespec="seconds"),
                "ambiguous_local_hour": bucket.ambiguous,
                "source": source,
                "device_id": device_id,
                "channel_id": channel_id,
                "metric": metric,
                "unit": members[0].unit,
                "mean": mean,
                "min": min(values),
                "max": max(values),
                "sample_count": count,
                "expected_samples": expected,
                "coverage_pct": coverage,
                "coverage_status": status,
                "coverage_word": word,
                "in_progress": in_progress,
                "poll_interval_s": interval,
                "first_ts": _stamp(members[0].ts_utc),
                "last_ts": _stamp(members[-1].ts_utc),
                # PLAN.md §2.5 / rollup.sql step 4: observed time only, watts only.
                "kwh": (
                    mean * (count * interval) / 3.6e6
                    if metric == model.POWER_METRIC
                    else None
                ),
            }
        )
    return rows


# ----------------------------------------------------------------- process


def _poller_block(
    doc: Mapping[str, Any],
    section: str,
    label: str,
    interval_s: int,
    now: datetime,
    checks: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    body = doc.get(section) if isinstance(doc.get(section), Mapping) else {}
    last = _parse_utc(body.get("last_success_utc"))
    check = checks.get(section, {})
    return {
        "section": section,
        "label": label,
        "poll_interval_s": interval_s,
        "last_success": _stamp(last),
        "age_s": _age_s(now, last),
        "consecutive_failures": body.get("consecutive_failures", 0),
        "last_error": body.get("last_error"),
        "rows_last_cycle": body.get("rows"),
        "channels_seen": body.get("channels_seen"),
        "stale": bool(check.get("stale")) if check else None,
        "never_succeeded": last is None,
    }


def _process_block(
    doc: Mapping[str, Any],
    health: Mapping[str, Any] | None,
    healthz_status: int | None,
    now: datetime,
    intervals: Mapping[str, int],
) -> dict[str, Any]:
    """Everything ``status.json`` already knows, arranged for one glance.

    Nothing here is recomputed from the spool: the poll loop is the authority on
    its own state and this page is a reader (PLAN.md §11).
    """
    ingest = doc.get("leviton_ingest") if isinstance(doc.get("leviton_ingest"), Mapping) else {}
    ws = doc.get("leviton_ws") if isinstance(doc.get("leviton_ws"), Mapping) else {}
    spool = doc.get("spool") if isinstance(doc.get("spool"), Mapping) else {}
    keepalive = doc.get("leviton_keepalive") if isinstance(doc.get("leviton_keepalive"), Mapping) else {}
    scheduler = doc.get("scheduler") if isinstance(doc.get("scheduler"), Mapping) else {}
    checks = {
        str(c.get("section")): c
        for c in (health or {}).get("checks", [])
        if isinstance(c, Mapping)
    }
    oldest_pending = _parse_utc(spool.get("oldest_pending_utc"))

    return {
        "healthz_ok": (healthz_status == 200) if healthz_status is not None else None,
        "healthz_status": healthz_status,
        "health_checks": list((health or {}).get("checks", [])),
        "stale_after_intervals": (health or {}).get("stale_after_intervals"),
        "started": _stamp(_parse_utc(doc.get("started_utc"))),
        "status_updated": _stamp(_parse_utc(doc.get("updated_utc"))),
        "status_age_s": _age_s(now, _parse_utc(doc.get("updated_utc"))),
        "ingest": {
            "mode": ingest.get("mode"),
            "value_source": ingest.get("value_source"),
            "ws_enabled": ingest.get("ws_enabled"),
            "ws_available": ingest.get("ws_available"),
            "withheld_reason": ingest.get("ws_withheld_reason"),
            "cycles_ws": ingest.get("cycles_ws"),
            "cycles_rest": ingest.get("cycles_rest"),
            "cycles_rest_fallback": ingest.get("cycles_rest_fallback"),
            "cycles_withheld": ingest.get("cycles_withheld"),
            "rest_reconciles": ingest.get("rest_reconciles"),
            "last_reconcile_drift": ingest.get("last_reconcile_drift"),
        },
        "ws": {
            "connected": ws.get("connected"),
            "synced": ws.get("synced"),
            "sync_mode": ws.get("sync_mode"),
            "awaiting_sync": ws.get("awaiting_sync"),
            "subscriptions": ws.get("subscriptions"),
            "subscriptions_active": ws.get("subscriptions_active"),
            "reconnects": ws.get("reconnects"),
            "stalls": ws.get("stalls"),
            "seconds_since_message": ws.get("seconds_since_message"),
            "stall_timeout_s": ws.get("stall_timeout_s"),
            "connection_age_s": ws.get("connection_age_s"),
            "hub_silence_s": ws.get("hub_silence_s") or {},
            "stalled_hubs": ws.get("stalled_hubs") or [],
            "last_message": _stamp(_parse_utc(ws.get("last_message_utc"))),
            "last_error": ws.get("last_error"),
        },
        "keepalive": {
            "connected_hubs": keepalive.get("connected_hubs"),
            "last_success": _stamp(_parse_utc(keepalive.get("last_success_utc"))),
            "age_s": _age_s(now, _parse_utc(keepalive.get("last_success_utc"))),
            "consecutive_failures": keepalive.get("consecutive_failures"),
        },
        "spool": {
            "pending_rows": spool.get("pending_rows"),
            "oldest_pending": _stamp(oldest_pending),
            "oldest_pending_age_s": _age_s(now, oldest_pending),
        },
        "pollers": [
            _poller_block(doc, "leviton", "Leviton", intervals["leviton"], now, checks),
            _poller_block(
                doc, "bryant_status", "Bryant", intervals["bryant_status"], now, checks
            ),
        ],
        "scheduler": {
            "consecutive_failures": scheduler.get("consecutive_failures", 0),
            "last_error": scheduler.get("last_error"),
            "job": scheduler.get("job"),
            "last_success": _stamp(_parse_utc(scheduler.get("last_success_utc"))),
        },
    }


def _health_from_document(
    doc: Mapping[str, Any], intervals: Mapping[str, int], now: datetime
) -> tuple[int, dict[str, Any]]:
    """What ``/healthz`` *would* answer, computed from a status.json on disk.

    Only used when there is no in-process :class:`StatusStore` to ask (the CLI
    and test path); when there is one, its own ``health_report()`` is used
    verbatim. This mirrors that method — same
    :data:`~energy_capture.health.STALE_INTERVAL_MULTIPLIER`, same "measure a
    never-succeeded poller from process start" rule — reading ``started_utc``
    from the document instead of from a live process.
    """
    from energy_capture.health import STALE_INTERVAL_MULTIPLIER

    started = _parse_utc(doc.get("started_utc")) or now
    checks: list[dict[str, Any]] = []
    ok = True
    for section in sorted(intervals):
        interval = intervals[section]
        max_age = interval * STALE_INTERVAL_MULTIPLIER
        body = doc.get(section)
        raw = body.get("last_success_utc") if isinstance(body, Mapping) else None
        last_success = _parse_utc(raw)
        reference = last_success if last_success is not None else started
        age = (now - reference).total_seconds()
        stale = age > max_age
        ok = ok and not stale
        checks.append(
            {
                "section": section,
                "poll_interval_s": interval,
                "max_age_s": max_age,
                "last_success_utc": raw,
                "never_succeeded": last_success is None,
                "age_s": round(age, 3),
                "stale": stale,
            }
        )
    return (
        200 if ok else 503,
        {
            "ok": ok,
            "now_utc": timeutil.format_utc(now),
            "started_utc": timeutil.format_utc(started),
            "stale_after_intervals": STALE_INTERVAL_MULTIPLIER,
            "checks": checks,
        },
    )


def _status_document(
    store: StatusStore | None,
    status_path: Path | str | None,
    errors: list[str],
    *,
    intervals: Mapping[str, int],
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None, int | None, str]:
    """``(document, health_block, http_status, source)``.

    Prefers the **in-process** :class:`~energy_capture.health.StatusStore` — it is
    the authority, it needs no file read, and its ``health_report()`` is literally
    what ``/healthz`` would answer right now. Falls back to reading
    ``status.json`` off disk (the CLI/test path), and finally to nothing at all,
    which the page reports rather than papering over.
    """
    if store is not None:
        try:
            http_status, body = store.health_report()
            document = {k: v for k, v in body.items() if k != "health"}
            return (document, body.get("health"), http_status, "in-process status store")
        except Exception as exc:  # pragma: no cover - defensive
            errors.append(f"status store unreadable: {type(exc).__name__}: {exc}")

    if status_path is not None:
        try:
            document = json.loads(Path(status_path).read_text(encoding="utf-8"))
            if isinstance(document, Mapping):
                doc = dict(document)
                doc.pop("health", None)  # derived per request, never persisted
                http_status, health = _health_from_document(doc, intervals, now)
                return (doc, health, http_status, f"status.json ({status_path})")
        except FileNotFoundError:
            errors.append(f"status.json not found at {status_path} — is `energycap run` up?")
        except Exception as exc:
            errors.append(f"status.json unreadable: {type(exc).__name__}: {exc}")

    return ({}, None, None, "unavailable")


# ------------------------------------------------------------------- hvac


_HVAC_SYSTEM_METRICS: tuple[str, ...] = (
    "outdoor_temp_f",
    "mode",
    "stage",
    "stage_pct",
    "blower_rpm",
    "cfm",
)
_HVAC_ZONE_METRICS: tuple[str, ...] = (
    "indoor_temp_f",
    "humidity_pct",
    "setpoint_heat_f",
    "setpoint_cool_f",
    "fan",
)


def _reading(
    latest: Mapping[str, Mapping[str, Any]], metric: str, now: datetime, absent_reason: str
) -> dict[str, Any]:
    """One HVAC readout — present with a value, or absent with a reason.

    "Absent" and "zero" are different facts and are reported as different shapes,
    never collapsed (CLAUDE.md rule 1). ``0`` is a reading.
    """
    row = latest.get(metric)
    if row is None:
        return {"metric": metric, "present": False, "value": None, "reason": absent_reason}
    ts = row["ts_utc"]
    out: dict[str, Any] = {
        "metric": metric,
        "present": True,
        "value": row["value"],
        "unit": row["unit"],
        "ts": _stamp(ts),
        "age_s": _age_s(now, ts),
    }
    if metric in model.ENUM_METRICS:
        out["enum"] = decode_enum(metric, row["value"])
    return out


def _hvac_block(
    latest_by_channel: Mapping[tuple[str, str, str], Mapping[str, Mapping[str, Any]]],
    labels: Mapping[tuple[str, str, str], Mapping[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    """The Bryant picture: system + zones, every enum decoded to its word."""
    bryant = {
        key: metrics
        for key, metrics in latest_by_channel.items()
        if key[0] == model.SOURCE_BRYANT
    }
    if not bryant:
        return {
            "present": False,
            "reason": "no Bryant rows in the spool — the status poller has not landed a cycle yet",
            "system": None,
            "zones": [],
        }

    device_id = sorted({key[1] for key in bryant})[0]
    system_key = (model.SOURCE_BRYANT, device_id, "system")
    system_latest = bryant.get(system_key, {})

    system = {
        "channel_id": "system",
        "device_id": device_id,
        **_describe_channel(system_key, labels),
        "readings": {
            metric: _reading(
                system_latest,
                metric,
                now,
                _stage_absent_reason(metric, system_latest),
            )
            for metric in _HVAC_SYSTEM_METRICS
        },
    }
    # Which rendering of odu.opstat this system uses (sources/bryant.py: a word
    # on a staged compressor, a 0-100 capacity percentage on a variable one).
    if "stage_pct" in system_latest:
        system["stage_representation"] = "pct"
    elif "stage" in system_latest:
        system["stage_representation"] = "enum"
    else:
        system["stage_representation"] = None

    zones = []
    for key in sorted(bryant):
        if key[2] == "system":
            continue
        zone_latest = bryant[key]
        zones.append(
            {
                "channel_id": key[2],
                "device_id": key[1],
                **_describe_channel(key, labels),
                "readings": {
                    metric: _reading(
                        zone_latest,
                        metric,
                        now,
                        f"{metric} not reported for this zone",
                    )
                    for metric in _HVAC_ZONE_METRICS
                },
            }
        )

    return {"present": True, "reason": None, "system": system, "zones": zones}


def _stage_absent_reason(metric: str, system_latest: Mapping[str, Any]) -> str:
    """Say *plainly* why a stage figure is missing — absent is not zero."""
    if metric == "stage_pct":
        if "stage" in system_latest:
            return (
                "not reported: this outdoor unit reports a stage WORD, not a "
                "capacity percentage, so it has a stage instead of a stage_pct"
            )
        return "no stage_pct sample in the spool — absent, which is not the same as 0%"
    if metric == "stage":
        if "stage_pct" in system_latest:
            return (
                "not reported: this is a variable-capacity compressor, which "
                "reports a capacity percentage (stage_pct) rather than a stage word"
            )
        return "no stage sample in the spool — absent, which is not the same as 'off'"
    return f"no {metric} sample in the spool"


# --------------------------------------------------------------- the snapshot


def build_snapshot(
    store: StatusStore | None = None,
    *,
    spool_path: Path | str | None = None,
    status_path: Path | str | None = None,
    channel_map_path: Path | str | None = None,
    inventory_path: Path | str | None = None,
    now: datetime | None = None,
    window_minutes: int = SERIES_WINDOW_MINUTES,
    hours: int = HOURLY_WINDOW_HOURS,
    chart: ChartRequest | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Build the whole ``/ui/data`` document. Reads only; never raises for data.

    Args:
        store: the process's :class:`~energy_capture.health.StatusStore`, when
            there is one. Its ``health_report()`` is exactly what ``/healthz``
            would answer, so the page and the probe cannot disagree.
        spool_path / status_path / channel_map_path / inventory_path: overrides;
            each defaults to the configured location.
        now: injectable clock (tests).
        window_minutes: the "Live now" sparkline window.
        hours: how many local hours "The math" table covers.
        chart: the overlay chart's window (:func:`parse_chart_request`). ``None``
            is the default live window and produces the document this route
            returned before the window was movable at all.

    Every failure mode below the top level is caught and appended to ``errors``:
    a dashboard that 500s tells the owner nothing, while a dashboard that says
    "the spool is unreadable" tells them exactly what broke.
    """
    errors: list[str] = []
    reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()

    resolved = settings
    if resolved is None:
        try:
            from energy_capture.config import get_settings

            resolved = get_settings()
        except Exception as exc:  # pragma: no cover - configuration must not break the page
            errors.append(f"settings unavailable: {type(exc).__name__}: {exc}")

    poll_interval_s = int(getattr(resolved, "poll_interval_s", 30) or 30)
    bryant_interval_s = int(getattr(resolved, "bryant_poll_interval_s", 30) or 30)
    interval_for_source = {
        model.SOURCE_LEVITON: poll_interval_s,
        model.SOURCE_BRYANT: bryant_interval_s,
    }
    if spool_path is None:
        spool_path = getattr(resolved, "spool_db_path", None)
    if status_path is None:
        status_path = getattr(resolved, "status_path", None)
    if channel_map_path is None:
        channel_map_path = Path("config/channel_map.json")
    if inventory_path is None:
        inventory_path = getattr(resolved, "blackstart_inventory_path", None)

    health_intervals = {"leviton": poll_interval_s, "bryant_status": bryant_interval_s}
    document, health_block, healthz_status, status_source = _status_document(
        store, status_path, errors, intervals=health_intervals, now=reference
    )
    labels = _labels(channel_map_path, inventory_path, errors)

    window_start = reference - timedelta(minutes=window_minutes)
    buckets = hour_buckets(reference, hours)
    read_from = min(window_start, buckets[0].start_utc) if buckets else window_start

    # ------------------------------------------------- the chart's own window
    chart_request = chart if chart is not None else ChartRequest()
    # A within-skew future `end` is honoured as "now": the chart must not open a
    # strip of empty axis to the right that reads as an outage.
    chart_end = (
        min(timeutil.ensure_utc(chart_request.end), reference)
        if chart_request.end
        else reference
    )
    chart_mode = "raw" if chart_request.window_s <= CHART_RAW_MAX_WINDOW_S else "bucketed"
    chart_bucket_s = (
        None
        if chart_mode == "raw"
        else chart_bucket_width_s(chart_request.window_s, poll_interval_s)
    )
    chart_start = chart_end - timedelta(seconds=chart_request.window_s)
    if chart_bucket_s:
        # Widen to the bucket boundary below, so the first bucket is a whole one
        # and boundaries stay put as the window slides.
        chart_start = _align_to_bucket(chart_start, chart_bucket_s)

    latest_rows: list[sqlite3.Row] = []
    window_samples: list[_Sample] = []
    chart_ranked: list[dict[str, Any]] = []
    chart_by_key: dict[tuple[str, str, str], list[_Sample]] = {}
    extent: dict[str, Any] = {"oldest": None, "newest": None, "span_s": None}
    spool_ok = False
    if spool_path is None:
        errors.append("no spool path configured")
    else:
        try:
            with open_readonly(spool_path) as conn:
                latest_rows = list(conn.execute(_LATEST_SQL))
                window_samples = [
                    _row_to_sample(row)
                    for row in conn.execute(
                        _WINDOW_SQL,
                        (
                            timeutil.format_utc(read_from),
                            timeutil.format_utc(reference + timedelta(seconds=1)),
                        ),
                    )
                ]
                extent = _spool_extent(conn)
                chart_ranked, chart_by_key = _read_chart_window(
                    conn, start=chart_start, end=chart_end
                )
            spool_ok = True
        except sqlite3.Error as exc:
            errors.append(
                f"spool at {spool_path} could not be read read-only "
                f"({type(exc).__name__}: {exc}). Two things cause this. A WAL "
                "database needs its -shm file, which exists only while a writer "
                "has the database open — with the poll loop down, this page "
                "cannot show its history. And SQLite's shared-memory locking is "
                "not coherent across a VM or network filesystem boundary, so a "
                "spool being written from inside a container must be read from "
                "inside that container: opening it from the host can report a "
                "malformed image (and risks making one). In the deployed "
                "process this page runs alongside the writer, where neither "
                "applies."
            )

    # latest value per (channel, metric)
    latest_by_channel: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
    for row in latest_rows:
        key = (row["source"], row["device_id"], row["channel_id"])
        latest_by_channel.setdefault(key, {})[row["metric"]] = {
            "value": float(row["value"]),
            "unit": row["unit"],
            "ts_utc": timeutil.ensure_utc(datetime.fromisoformat(row["ts_utc"])),
            "ts_local": row["ts_local"],
        }

    # window samples per (channel, metric), already chronological
    by_channel_metric: dict[tuple[str, str, str], dict[str, list[_Sample]]] = {}
    for sample in window_samples:
        if sample.ts_utc < window_start:
            continue
        by_channel_metric.setdefault(sample.channel_key, {}).setdefault(
            sample.metric, []
        ).append(sample)

    channels: list[dict[str, Any]] = []
    for key in sorted(latest_by_channel):
        source, device_id, channel_id = key
        interval = float(interval_for_source.get(source, poll_interval_s))
        metrics = latest_by_channel[key]
        newest = max(row["ts_utc"] for row in metrics.values())
        age = _age_s(reference, newest) or 0.0
        if age <= interval * _LIVE_FACTOR:
            level, word = "good", "live"
        elif age <= interval * _LATE_FACTOR:
            level, word = "warning", "late"
        else:
            level, word = "critical", "silent"

        available = by_channel_metric.get(key, {})
        series_metric = next(
            (m for m in SERIES_METRIC_PRIORITY if m in available),
            next(iter(sorted(available)), None),
        )
        series = None
        if series_metric is not None:
            members = available[series_metric]
            series = _build_series(
                members,
                metric=series_metric,
                unit=members[0].unit,
                poll_interval_s=interval,
                window_start=window_start,
                now=reference,
            )

        channels.append(
            {
                "key": f"{source}/{device_id}/{channel_id}",
                "source": source,
                "device_id": device_id,
                "channel_id": channel_id,
                **_describe_channel(key, labels),
                "poll_interval_s": interval,
                "latest_ts": _stamp(newest),
                "age_s": age,
                "status": level,
                "status_word": word,
                "metrics": {
                    metric: {
                        "value": row["value"],
                        "unit": row["unit"],
                        "ts": _stamp(row["ts_utc"]),
                        "age_s": _age_s(reference, row["ts_utc"]),
                        "enum": (
                            decode_enum(metric, row["value"])
                            if metric in model.ENUM_METRICS
                            else None
                        ),
                    }
                    for metric, row in sorted(metrics.items())
                },
                "series": series,
            }
        )

    hourly_rows = hourly_rollup(
        window_samples,
        buckets,
        now=reference,
        interval_for_source=interval_for_source,
        default_interval_s=poll_interval_s,
    )
    # Name every rollup row. `channel_id` alone is NOT unique across the house: both
    # load centres expose a `ct_1_a`, so a table keyed on channel_id shows two
    # identical-looking rows with different numbers in them. The naming is applied
    # here rather than inside `hourly_rollup`, which stays a faithful mirror of
    # `stages/rollup.sql` (whose grain is the channel key, not a human label).
    for row in hourly_rows:
        row_key = (row["source"], row["device_id"], row["channel_id"])
        described = _describe_channel(row_key, labels)
        row["key"] = "{}/{}/{}".format(*row_key)
        row["label"] = described["label"]
        row["short_label"] = described["short_label"]
        row["panel"] = described["panel"]
        row["unmapped"] = described["unmapped"]

    today = timeutil.local_date_of(reference)
    hourly = {
        "window_hours": hours,
        "hours": [
            {
                "hour_start": _stamp(bucket.start_utc),
                "local_hour_start": bucket.local_start.isoformat(timespec="seconds"),
                "ambiguous_local_hour": bucket.ambiguous,
                "in_progress": bucket.end_utc > reference,
            }
            for bucket in buckets
        ],
        "rows": hourly_rows,
        "kwh_formula": KWH_FORMULA,
        "local_day": {
            "date": today.isoformat(),
            "hours_in_day": timeutil.local_hours_in_day(today),
            "expected_samples_per_channel": expected_samples_for_local_day(
                today, poll_interval_s
            ),
        },
        "note": (
            "An hour with no samples has NO ROW — it is not a zero. kwh is "
            "observed time only and is NULL for every metric except watts."
        ),
    }

    overlay = _overlay_block(
        request=chart_request,
        window_start=chart_start,
        window_end=chart_end,
        mode=chart_mode,
        bucket_s=chart_bucket_s,
        ranked=chart_ranked,
        samples_by_key=chart_by_key,
        labels=labels,
        interval_for_source=interval_for_source,
        poll_interval_s=poll_interval_s,
    )

    return {
        "generated": _stamp(reference),
        "now": _stamp(reference),
        "tz": timeutil.tz_name(),
        "refresh_s": 5,
        "poll_interval_s": {
            "leviton": poll_interval_s,
            "bryant": bryant_interval_s,
        },
        "gap_interval_factor": GAP_INTERVAL_FACTOR,
        "spool": {
            "path": str(spool_path) if spool_path else None,
            "readable": spool_ok,
            "mode": "read-only (mode=ro, query_only)",
            "channels": len(channels),
            "window_rows": len(window_samples),
            "chart_rows": sum(len(rows) for rows in chart_by_key.values()),
            # How far back the chart can be panned. The spool is not an archive:
            # rows are purged once they are uploaded and past the retention floor,
            # so this is "what is still here", not "what was ever collected".
            "extent": extent,
        },
        "status_source": status_source,
        "process": _process_block(
            document, health_block, healthz_status, reference, health_intervals
        ),
        "channels": channels,
        "overlay": overlay,
        "hvac": _hvac_block(latest_by_channel, labels, reference),
        "hourly": hourly,
        # The utility meter. A second read path entirely — meter intervals live
        # in Parquet, never in the spool — so it is its own module, and every
        # failure inside it comes back as a block the page can render rather
        # than an exception that would take the whole snapshot down.
        "meter": _meter_block(reference, spool_path, labels, errors, resolved),
        "errors": errors,
    }


def _meter_block(
    reference: datetime,
    spool_path: Path | str | None,
    labels: dict[tuple[str, str, str], dict[str, Any]],
    errors: list[str],
    settings: Settings | None,
) -> dict[str, Any]:
    """The meter card, or a reason it is absent. Never raises."""
    try:
        from energy_capture import meterview

        return meterview.meter_block(
            now=reference,
            settings=settings,
            spool_path=spool_path,
            labels=labels,
        )
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        errors.append(f"meter card unavailable: {detail}")
        return {"available": False, "error": detail}


def _overlay_block(
    *,
    request: ChartRequest,
    window_start: datetime,
    window_end: datetime,
    mode: str,
    bucket_s: float | None,
    ranked: Sequence[Mapping[str, Any]],
    samples_by_key: Mapping[tuple[str, str, str], Sequence[_Sample]],
    labels: Mapping[tuple[str, str, str], Mapping[str, Any]],
    interval_for_source: Mapping[str, int],
    poll_interval_s: float,
) -> dict[str, Any]:
    """The <=3 watt channels worth overlaying, most active first, over the chart window.

    Three is the whole categorical palette (slots 1-3); a fourth series is not
    allowed, so the rest are named in ``omitted`` rather than cycled into a
    repeated colour. The page assigns each *entity* a slot and keeps it, so a
    channel that survives a change of selection — including a change caused by
    panning into a different stretch of the day — never changes colour.

    The block carries its own ``series`` rather than pointing the page back at
    ``channels[].series``: the chart's window is movable and the cards' window is
    not, so the two are only the same document when the chart is live at its
    default length (which is exactly when the two agree sample for sample).
    """
    chosen = list(ranked[:3])
    series: list[dict[str, Any]] = []
    for entry in chosen:
        key = entry["key"]
        members = list(samples_by_key.get(key, ()))
        interval = float(interval_for_source.get(key[0], poll_interval_s))
        unit = entry.get("unit") or model.UNIT_WATTS
        if mode == "bucketed" and bucket_s:
            built = _bucket_series(
                members,
                metric=model.POWER_METRIC,
                unit=unit,
                poll_interval_s=interval,
                window_start=window_start,
                window_end=window_end,
                bucket_s=bucket_s,
            )
        else:
            built = _build_series(
                members,
                metric=model.POWER_METRIC,
                unit=unit,
                poll_interval_s=interval,
                window_start=window_start,
                now=window_end,
            )
            built["mode"] = "raw"
        described = _describe_channel(key, labels)
        built["key"] = entry["key_str"]
        built["label"] = described["label"]
        built["short_label"] = described["short_label"]
        built["poll_interval_s"] = interval
        series.append(built)

    return {
        "metric": model.POWER_METRIC,
        "unit": model.UNIT_WATTS,
        "window_start": _stamp(window_start),
        "window_end": _stamp(window_end),
        "selected_by": "highest watts observed in the window",
        "keys": [entry["key_str"] for entry in chosen],
        "omitted": [
            {
                "key": entry["key_str"],
                "label": _describe_channel(entry["key"], labels)["label"],
                "max": entry["peak"],
            }
            for entry in ranked[3:]
        ],
        # ------------------------------------------------ the movable window
        "window_s": request.window_s,
        "live": request.live,
        "mode": mode,
        "bucket_s": bucket_s,
        # Independent of the series, so an empty window still labels its axis.
        "bucket_count": (
            max(
                1,
                math.ceil(
                    (
                        timeutil.ensure_utc(window_end) - timeutil.ensure_utc(window_start)
                    ).total_seconds()
                    / bucket_s
                ),
            )
            if bucket_s
            else None
        ),
        "expected_per_bucket": (
            expected_samples(bucket_s, poll_interval_s) if bucket_s else None
        ),
        "resolution": chart_resolution_label(mode, bucket_s, poll_interval_s),
        "raw_max_window_s": CHART_RAW_MAX_WINDOW_S,
        "max_window_s": CHART_MAX_WINDOW_S,
        "presets_s": list(CHART_PRESETS_S),
        "request": {
            "window_s": request.requested_window_s,
            "end": _stamp(request.end),
            "clamped": request.clamped,
        },
        "series": series,
        "note": (
            "The marks on this chart are "
            + chart_resolution_label(mode, bucket_s, poll_interval_s)
            + (
                ". An empty bucket is a hole: mean null, sample_count 0 — the line "
                "breaks there. A bucket with fewer samples than expected keeps the "
                "mean of the samples it has, and says how many that was."
                if mode == "bucketed"
                else ". Raw observed samples — nothing is bucketed, averaged or filled."
            )
        ),
    }
