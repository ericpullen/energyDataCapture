"""The single home of UTC<->local conversion and local-date partition math.

CLAUDE.md invariant: **no ``zoneinfo`` calls anywhere else in the codebase.**
Every stage that needs a local date, a local hour bucket, or a partition path
comes through this module.

The three rules this module encodes (PLAN.md §2.4, §3, CLAUDE.md rules 3–4):

1. ``ts_utc`` is canonical. All sorting, bucketing and dedupe keys use it.
2. ``ts_local`` is a **timezone-naive wall clock** in ``TZ_LOCAL``
   (America/Kentucky/Louisville). It is *deliberately ambiguous* during the DST
   fall-back hour: 01:30 occurs twice on that day and both instants render as
   ``01:30``. That is by design — it is a human/LLM readability column, never a
   key. Anything that must distinguish the two occurrences uses ``ts_utc``
   (see :func:`utc_hour_start`).
3. Partitioning is on the **LOCAL** date, derived from ``ts_local``.

DST correctness lives here, so it is worth stating what "correct" means for
America/Kentucky/Louisville (US Eastern rules):

* Spring forward (e.g. 2026-03-08): the local day is **23 hours** long and the
  wall-clock hour ``02`` does not exist.
* Fall back (e.g. 2026-11-01): the local day is **25 hours** long and the
  wall-clock hour ``01`` happens twice. :func:`iter_local_hours` yields both,
  keyed by their distinct UTC starts.

Local day boundaries are computed as UTC instants, so a "local day" is the real
span between two local midnights — 23h, 24h or 25h — never a hardcoded 24h.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

__all__ = [
    "UTC",
    "LocalHour",
    "ensure_utc",
    "format_utc",
    "iter_local_dates",
    "iter_local_hours",
    "local_date_of",
    "local_day_bounds_utc",
    "local_hour_bounds_utc",
    "local_hour_key",
    "local_hour_label",
    "local_hour_stamp",
    "local_hour_start",
    "local_hours_in_day",
    "local_midnight_naive",
    "local_midnight_utc",
    "local_naive_to_utc",
    "local_tz",
    "local_wall_hours_of_day",
    "now_utc",
    "parse_local_date",
    "partition_parts",
    "partition_parts_for_local_date",
    "to_local",
    "to_local_naive",
    "tz_name",
    "utc_hour_start",
]

UTC = timezone.utc

_ONE_HOUR = timedelta(hours=1)
_ONE_DAY = timedelta(days=1)


@lru_cache(maxsize=8)
def _zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def tz_name() -> str:
    """Configured local IANA zone name (``TZ_LOCAL``)."""
    from energy_capture.config import get_settings

    return get_settings().tz_local


def local_tz(name: str | None = None) -> ZoneInfo:
    """The local :class:`ZoneInfo` (defaults to the configured ``TZ_LOCAL``)."""
    return _zone(name or tz_name())


# --------------------------------------------------------------------- basics


def now_utc() -> datetime:
    """Current instant as a timezone-aware UTC datetime (microsecond precision)."""
    return datetime.now(UTC)


def ensure_utc(ts: datetime) -> datetime:
    """Normalise ``ts`` to an aware UTC datetime.

    A naive datetime is *assumed to already be UTC* — callers holding wall-clock
    time must go through :func:`local_naive_to_utc` instead.
    """
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def to_local(ts_utc: datetime, *, tz: str | None = None) -> datetime:
    """Convert an instant to an **aware** local datetime."""
    return ensure_utc(ts_utc).astimezone(local_tz(tz))


def to_local_naive(ts_utc: datetime, *, tz: str | None = None) -> datetime:
    """Convert an instant to the naive local wall clock stored as ``ts_local``.

    Ambiguous during fall-back by design (PLAN.md §2.4).
    """
    return to_local(ts_utc, tz=tz).replace(tzinfo=None)


def local_naive_to_utc(
    ts_local: datetime, *, fold: int = 0, tz: str | None = None
) -> datetime:
    """Convert a naive local wall clock back to an aware UTC instant.

    The inverse of :func:`to_local_naive` is not a function: during fall-back a
    wall-clock time maps to two instants (``fold=0`` picks the first, EDT;
    ``fold=1`` the second, EST) and during spring-forward it maps to none (Python
    resolves the nonexistent hour by shifting; check with
    :func:`local_wall_hours_of_day` if that matters). Never use this on data —
    ``ts_utc`` is what is recorded — only on human-supplied local times.
    """
    if ts_local.tzinfo is not None:
        raise ValueError("local_naive_to_utc expects a naive wall-clock datetime")
    return ts_local.replace(tzinfo=local_tz(tz), fold=fold).astimezone(UTC)


# ----------------------------------------------------------- dates and hours


def local_date_of(ts_utc: datetime, *, tz: str | None = None) -> date:
    """The LOCAL calendar date of an instant — the partition date."""
    return to_local(ts_utc, tz=tz).date()


def local_hour_start(ts_utc: datetime, *, tz: str | None = None) -> datetime:
    """Naive local wall-clock hour containing ``ts_utc`` (minutes/seconds zeroed).

    This is the ``local_hour_start`` column of the hourly rollup: readable, and
    ambiguous for the two fall-back 01:00 hours. The rollup's *grouping* key is
    :func:`utc_hour_start`, which keeps those two hours distinct.
    """
    return to_local_naive(ts_utc, tz=tz).replace(minute=0, second=0, microsecond=0)


def utc_hour_start(ts_utc: datetime) -> datetime:
    """Aware UTC hour containing ``ts_utc`` — the canonical hour bucket key.

    Every US DST offset is a whole number of hours, so a UTC hour boundary is
    also a local hour boundary; bucketing on UTC therefore yields exactly the
    local hours of the day (23 / 24 / 25 of them) with the fall-back repeat
    preserved as two distinct buckets.
    """
    return ensure_utc(ts_utc).replace(minute=0, second=0, microsecond=0)


def local_midnight_naive(local_day: date) -> datetime:
    """Naive local midnight of ``local_day`` — the ``ts_local`` of day-grain rows."""
    return datetime.combine(local_day, time.min)


def local_midnight_utc(local_day: date, *, tz: str | None = None) -> datetime:
    """Aware UTC instant of local midnight starting ``local_day``.

    This is ``ts_utc`` for day-grain rows in ``energy/daily`` (PLAN.md §7.2) and
    the lower bound of a local day. Midnight is never skipped or repeated by US
    DST transitions (they happen at 02:00), so this is unambiguous.
    """
    return local_midnight_naive(local_day).replace(tzinfo=local_tz(tz)).astimezone(UTC)


def local_day_bounds_utc(
    local_day: date, *, tz: str | None = None
) -> tuple[datetime, datetime]:
    """``[start, end)`` UTC instants of a local day — 23h, 24h or 25h wide."""
    return (
        local_midnight_utc(local_day, tz=tz),
        local_midnight_utc(local_day + _ONE_DAY, tz=tz),
    )


@dataclass(frozen=True, slots=True)
class LocalHour:
    """One physical hour of a local day.

    Attributes:
        index: position within the day (0-based; 0..22 on spring-forward,
            0..24 on fall-back).
        local_start: naive local wall clock at which the hour starts. Repeats for
            the two fall-back 01:00 hours — see ``ambiguous``.
        start_utc: aware UTC start (inclusive). Unique; use as the bucket key.
        end_utc: aware UTC end (exclusive).
        ambiguous: True when another hour of the same day shares ``local_start``.
    """

    index: int
    local_start: datetime
    start_utc: datetime
    end_utc: datetime
    ambiguous: bool


def iter_local_hours(local_day: date, *, tz: str | None = None) -> Iterator[LocalHour]:
    """Yield the local hours of ``local_day`` in chronological order.

    Yields 23 entries on spring-forward, 25 on fall-back, 24 otherwise. Walking
    UTC between the day's two local midnights is what makes that automatic.
    """
    start, end = local_day_bounds_utc(local_day, tz=tz)

    spans: list[tuple[datetime, datetime, datetime]] = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + _ONE_HOUR, end)
        spans.append((to_local_naive(cursor, tz=tz), cursor, nxt))
        cursor = nxt

    seen: dict[datetime, int] = {}
    for local_start, _, _ in spans:
        seen[local_start] = seen.get(local_start, 0) + 1

    for index, (local_start, start_utc, end_utc) in enumerate(spans):
        yield LocalHour(
            index=index,
            local_start=local_start,
            start_utc=start_utc,
            end_utc=end_utc,
            ambiguous=seen[local_start] > 1,
        )


def local_hours_in_day(local_day: date, *, tz: str | None = None) -> int:
    """Number of local hours in ``local_day``: 23, 24 or 25."""
    return sum(1 for _ in iter_local_hours(local_day, tz=tz))


def local_wall_hours_of_day(local_day: date, *, tz: str | None = None) -> list[int]:
    """Distinct wall-clock hour numbers that exist on ``local_day``.

    Spring-forward drops ``2``; fall-back still lists ``1`` once (it happens
    twice, but it is one wall-clock hour label — and therefore one
    ``part-{YYYYMMDD}T01.parquet`` file holding two hours of rows).
    """
    out: list[int] = []
    for hour in iter_local_hours(local_day, tz=tz):
        value = hour.local_start.hour
        if value not in out:
            out.append(value)
    return out


def local_hour_bounds_utc(
    local_day: date, hour: int, *, tz: str | None = None
) -> tuple[datetime, datetime]:
    """``[start, end)`` UTC bounds of wall-clock ``hour`` on ``local_day``.

    ``hour`` is the wall-clock hour label (0–23), matching the ``HH`` in
    ``part-{YYYYMMDD}T{HH}.parquet``. On the fall-back day, hour ``1`` covers
    *both* occurrences and the returned span is therefore two hours wide — which
    is exactly the set of rows that belong in that part file. Raises
    :class:`ValueError` for the wall-clock hour that does not exist on a
    spring-forward day; use :func:`local_wall_hours_of_day` to enumerate safely.
    """
    matches = [h for h in iter_local_hours(local_day, tz=tz) if h.local_start.hour == hour]
    if not matches:
        raise ValueError(
            f"local hour {hour:02d} does not exist on {local_day.isoformat()} "
            f"in {tz or tz_name()} (DST spring-forward)"
        )
    return matches[0].start_utc, matches[-1].end_utc


def iter_local_dates(start: date, end: date) -> Iterator[date]:
    """Yield local dates from ``start`` to ``end``, both inclusive.

    Every stage takes ``--start/--end`` local dates (PLAN.md §10); this is the
    canonical expansion. An inverted range is a caller bug, not an empty range.
    """
    if end < start:
        raise ValueError(f"end {end.isoformat()} is before start {start.isoformat()}")
    current = start
    while current <= end:
        yield current
        current += _ONE_DAY


def parse_local_date(value: str | date | datetime) -> date:
    """Parse a ``YYYY-MM-DD`` CLI argument into a local :class:`date`."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


# ------------------------------------------------------------- partitioning


def partition_parts_for_local_date(local_day: date) -> tuple[str, str, str]:
    """``(year, month, day)`` partition values as zero-padded strings."""
    return (f"{local_day.year:04d}", f"{local_day.month:02d}", f"{local_day.day:02d}")


def partition_parts(ts_utc: datetime, *, tz: str | None = None) -> tuple[str, str, str]:
    """``(year, month, day)`` partition values for an instant, on its LOCAL date.

    ``year=2026/month=03/day=08`` — matches the Glue partition projection
    (integer, two digits for month/day) in PLAN.md §12.
    """
    return partition_parts_for_local_date(local_date_of(ts_utc, tz=tz))


def local_hour_stamp(ts_utc: datetime, *, tz: str | None = None) -> tuple[str, str]:
    """``("20260816", "14")`` — the stamps in ``part-{YYYYMMDD}T{HH}.parquet``."""
    local = to_local_naive(ts_utc, tz=tz)
    return (f"{local:%Y%m%d}", f"{local:%H}")


def local_hour_label(local_day: date, hour: int) -> str:
    """``"2026-08-16T14"`` from a local date and wall-clock hour.

    The ``last_hour_uploaded`` form of PLAN.md §11. This is a *rendering*, not a
    conversion — the caller already holds local time — but the format string
    lives here so there is exactly one definition of what that label looks like.
    Well defined for the fall-back day's repeated hour ``01``: one label, one
    ``part-{YYYYMMDD}T01.parquet``, two physical hours of rows inside it.
    """
    if not 0 <= int(hour) <= 23:
        raise ValueError(f"hour must be a wall-clock hour 0..23, got {hour!r}")
    return f"{local_day.isoformat()}T{int(hour):02d}"


def local_hour_key(ts_utc: datetime, *, tz: str | None = None) -> str:
    """``"2026-08-16T14"`` for an instant — its local date and wall-clock hour."""
    local = to_local_naive(ts_utc, tz=tz)
    return local_hour_label(local.date(), local.hour)


def format_utc(ts: datetime) -> str:
    """RFC3339 UTC with a ``Z`` suffix, for ``status.json`` and log fields."""
    return ensure_utc(ts).isoformat(timespec="microseconds").replace("+00:00", "Z")
