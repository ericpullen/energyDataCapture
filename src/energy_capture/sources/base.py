"""The ``Source`` contract every cloud poller implements (PLAN.md §5–§7).

A *source* is one third-party cloud we poll on a fixed cadence. Two exist today
(``sources/leviton.py``, ``sources/bryant.py``) and a third is designed for
(``lge``, PLAN.md §13). Everything downstream — ``stages/poller.py``, the
scheduler in ``energycap run``, ``energycap discover`` — talks to sources only
through the names in this module, so a new cloud is a new file, not a new branch
in the pipeline.

What this module provides:

* :class:`Source` — the structural protocol (poll, discover, start, close, plus
  a per-source poll interval and any background tasks the source needs).
* :class:`BaseSource` — an optional ABC with the boring parts already written:
  the poll-interval floor, discovery caching, and a periodic re-discovery task.
* :class:`PollCycle` — the shared stamping helper. PLAN.md §6.5 requires **one
  timestamp per source per poll cycle, taken when the response set is
  complete**; both sources use this so their rows are stamped identically and a
  cycle's rows all share a single ``ts_utc``.
* :class:`Discovery` / :class:`DiscoveredChannel` / :class:`DiscoveredDevice` —
  what ``energycap discover`` prints and what ``build-dim`` checks the
  hand-maintained ``config/channel_map.json`` against (PLAN.md §9).
* :class:`SourceError` and friends — the two failure modes the poll loop must
  tell apart: *transient* (retry within the cycle, then emit nothing and move
  on) and *auth* (re-login, respecting the source's login floor).

Cardinal rules this module enforces mechanically:

* **A gap stays a gap** (CLAUDE.md rule 1). :meth:`PollCycle.add` accepts
  ``None`` and returns ``False`` — a null API field produces *no row*, never a
  zero, and never a repeat of the last value. The count of skipped fields is
  available as :attr:`PollCycle.gaps` for logging/health, but it never becomes
  data.
* **Record what the API said, verbatim** (CLAUDE.md rule 2). Nothing here
  filters, smooths, or clamps a value that is a real finite number — Leviton's
  spurious zeros go through untouched.
* **``ts_utc`` is canonical** (CLAUDE.md rule 3). Sources never compute local
  time; :func:`energy_capture.model.make_observation` derives ``ts_local``.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Any, ClassVar, Protocol, runtime_checkable

from energy_capture.config import MIN_POLL_INTERVAL_S
from energy_capture.model import SOURCES, Observation, make_observation
from energy_capture.timeutil import ensure_utc, now_utc

__all__ = [
    "BackgroundTask",
    "BaseSource",
    "DiscoveredChannel",
    "DiscoveredDevice",
    "Discovery",
    "PollCycle",
    "Source",
    "SourceAuthError",
    "SourceError",
    "SourceTransientError",
]


# --------------------------------------------------------------------- errors


class SourceError(Exception):
    """Base class for every failure a source reports to the poll loop."""


class SourceTransientError(SourceError):
    """A retryable upstream hiccup — 502/504, timeout, connection reset.

    PLAN.md §6.6: Leviton's gateway returns these routinely. The source retries
    inside the cycle; if the cycle still fails it raises this, and the poll loop
    logs once at WARN, **emits no rows**, bumps the consecutive-failure counter
    and moves on. It never crashes the loop and never reuses the last reading.
    """


class SourceAuthError(SourceError):
    """Credentials/token rejected (401 after a refresh attempt).

    The source re-authenticates itself (honouring Leviton's 10-second login
    floor, PLAN.md §6.1); this escapes only when re-authentication also failed,
    so the loop can back off and reflect the condition in ``status.json``.
    """


# ----------------------------------------------------------------- discovery


@dataclass(frozen=True, slots=True)
class DiscoveredDevice:
    """A physical thing a source found: a Leviton hub, the Bryant system.

    ``device_id`` is exactly what lands in the ``device_id`` column — the hub id
    (panel serial) for Leviton, the system serial for Bryant (PLAN.md §6.5, §7.4).
    """

    source: str
    device_id: str
    #: Free-form kind, e.g. ``"hub"``, ``"system"``.
    kind: str
    label: str | None = None
    #: Raw-ish extras worth showing in ``energycap discover`` (firmware, rssi,
    #: connected, model). Never a credential.
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DiscoveredChannel:
    """One live channel: what ``channel_map.json`` must eventually name.

    ``channel_id`` follows the conventions in CLAUDE.md ("Conventions"):
    ``breaker_p{position}`` / ``ct_{channel}_{a,b}`` / ``panel_leg_{a,b}`` for
    Leviton, ``zone_{n}`` / ``system`` for Bryant status, the lowercase
    component name for Bryant daily energy. It is *never* the API's own object
    id — firmware ≥2.2.0 mutates those (PLAN.md §6.5).
    """

    source: str
    device_id: str
    channel_id: str
    #: ``"breaker"``, ``"ct"``, ``"panel_leg"``, ``"zone"``, ``"system"``,
    #: ``"energy_component"`` — descriptive only, for the discover table.
    kind: str
    label: str | None = None
    #: Everything a human needs to map this channel by hand: ``position``,
    #: ``poles``, ``branchType``, ``usageType``, ``model``, ``connected``.
    details: Mapping[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> tuple[str, str, str]:
        """``(source, device_id, channel_id)`` — the ``dim_channel`` join key."""
        return (self.source, self.device_id, self.channel_id)

    def channel_map_entry(self) -> dict[str, Any]:
        """A paste-ready skeleton entry for ``config/channel_map.json`` (§9).

        ``blackstart_device_id`` is left empty on purpose: PLAN.md §9 makes an
        entry with neither a ``label`` nor a ``blackstart_device_id`` a build
        error, so the human filling this in is forced to say what the circuit is.
        """
        return {
            "source": self.source,
            "device_id": self.device_id,
            "channel_id": self.channel_id,
            "label": self.label or "",
            "blackstart_device_id": "",
        }


@dataclass(frozen=True, slots=True)
class Discovery:
    """The result of one discovery pass over a source (PLAN.md §6.2, §9)."""

    source: str
    devices: tuple[DiscoveredDevice, ...] = ()
    channels: tuple[DiscoveredChannel, ...] = ()
    ts_utc: datetime = field(default_factory=now_utc)

    def channel_keys(self) -> set[tuple[str, str, str]]:
        """Every live ``(source, device_id, channel_id)``."""
        return {channel.key for channel in self.channels}

    def unmapped(
        self, known: Iterable[tuple[str, str, str]]
    ) -> tuple[DiscoveredChannel, ...]:
        """Live channels absent from ``known`` (the channel_map keys).

        Feeds both ``energycap discover`` (prints skeletons) and ``build-dim``
        (WARNs) — an unmapped channel is never silently dropped (PLAN.md §9).
        """
        seen = set(known)
        return tuple(channel for channel in self.channels if channel.key not in seen)

    def skeleton(
        self, known: Iterable[tuple[str, str, str]] = ()
    ) -> list[dict[str, Any]]:
        """``channel_map.json`` ``mappings`` entries for everything unmapped."""
        return [channel.channel_map_entry() for channel in self.unmapped(known)]


# ------------------------------------------------------------- background work


@dataclass(frozen=True, slots=True)
class BackgroundTask:
    """A periodic coroutine a source needs running while ``energycap run`` runs.

    This is how the Leviton bandwidth keepalive (PLAN.md §6.4: ``PUT
    {"bandwidth": 1}`` every 50s per connected hub) and periodic re-discovery
    reach the event loop without ``stages/poller.py`` knowing anything about
    Leviton. The runner schedules each task every ``interval_s`` seconds and
    logs/absorbs exceptions — a failing background task must never kill polling.
    """

    name: str
    interval_s: float
    run: Callable[[], Awaitable[None]]
    #: Seconds to wait before the first invocation (0 = run immediately).
    initial_delay_s: float = 0.0


# ------------------------------------------------------------- the poll cycle


@dataclass(frozen=True, slots=True)
class _Pending:
    device_id: str
    channel_id: str
    metric: str
    value: float
    unit: str | None
    interval_s: int | None


class PollCycle:
    """Collects one poll cycle's samples and stamps them with a single instant.

    PLAN.md §6.5: *"``ts_utc`` = one timestamp per source per poll cycle, taken
    when the response set is complete (µs precision). All rows from one poll
    share it."* Both sources use this class so their rows are stamped by the
    same rule — and so a cycle's rows always align on a single bucket boundary.

    Typical use::

        with source.new_cycle() as cycle:          # or PollCycle("leviton")
            payload = await self._fetch_everything()
            for breaker in payload.breakers:
                cycle.add(hub_id, f"breaker_p{breaker.position}", "watts", breaker.power)
        return cycle.observations

    The timestamp is taken by :meth:`finish` — i.e. on ``__exit__``, after the
    last response has been parsed — never at ``__enter__``.

    Gaps: :meth:`add` accepts ``None`` (and NaN/inf, which no schema can hold)
    and simply records nothing, returning ``False``. That is the whole of
    CLAUDE.md rule 1 at the source boundary: a null second-leg CT reading emits
    no row rather than a zero. If the body raises, ``__exit__`` deliberately does
    **not** stamp: a failed cycle contributes zero rows.
    """

    __slots__ = ("source", "_pending", "_observations", "_gaps", "_ts_utc")

    def __init__(self, source: str, *, ts_utc: datetime | None = None) -> None:
        if source not in SOURCES:
            raise ValueError(
                f"unknown source {source!r}; expected one of {sorted(SOURCES)}"
            )
        self.source = source
        self._pending: list[_Pending] = []
        self._observations: list[Observation] | None = None
        self._gaps: list[tuple[str, str, str]] = []
        # A pinned timestamp is for callers that already know the instant the
        # data describes (backfill, day-grain rows at local midnight, replaying
        # a recorded fixture in tests). Live pollers leave it None.
        self._ts_utc: datetime | None = ensure_utc(ts_utc) if ts_utc else None

    # ------------------------------------------------------------- collecting
    def add(
        self,
        device_id: str,
        channel_id: str,
        metric: str,
        value: float | int | None,
        *,
        unit: str | None = None,
        interval_s: int | None = None,
    ) -> bool:
        """Record one sample. Returns ``False`` (and records a gap) for ``None``.

        ``unit`` defaults to the metric's canonical unit; pass it only for a
        metric whose unit is genuinely variable. Values are **not** filtered,
        rounded or clamped — a spurious Leviton zero is a real reading as far as
        this pipeline is concerned (CLAUDE.md rule 2).
        """
        if self._observations is not None:
            raise RuntimeError("PollCycle is already finished; start a new cycle")
        if value is None:
            self._gaps.append((device_id, channel_id, metric))
            return False
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            # Not representable in the schema and not a real measurement: treat
            # exactly like a null field — no row, counted as a gap.
            self._gaps.append((device_id, channel_id, metric))
            return False
        self._pending.append(
            _Pending(
                device_id=device_id,
                channel_id=channel_id,
                metric=metric,
                value=numeric,
                unit=unit,
                interval_s=interval_s,
            )
        )
        return True

    def add_metrics(
        self,
        device_id: str,
        channel_id: str,
        metrics: Mapping[str, float | int | None],
        *,
        interval_s: int | None = None,
    ) -> int:
        """Add several metrics for one channel; returns how many were recorded."""
        return sum(
            1
            for metric, value in metrics.items()
            if self.add(device_id, channel_id, metric, value, interval_s=interval_s)
        )

    # --------------------------------------------------------------- stamping
    def finish(self, ts_utc: datetime | None = None) -> list[Observation]:
        """Stamp every pending sample with one instant and build the rows.

        Called automatically by ``__exit__``. The instant is ``ts_utc`` if given,
        else the timestamp pinned at construction, else :func:`now_utc` sampled
        *here* — which is the moment the response set became complete.
        """
        if self._observations is not None:
            raise RuntimeError("PollCycle.finish() called twice")
        if ts_utc is not None:
            stamp = ensure_utc(ts_utc)
        elif self._ts_utc is not None:
            stamp = self._ts_utc
        else:
            stamp = now_utc()
        self._ts_utc = stamp
        self._observations = [
            make_observation(
                ts_utc=stamp,
                source=self.source,
                device_id=item.device_id,
                channel_id=item.channel_id,
                metric=item.metric,
                value=item.value,
                unit=item.unit,
                interval_s=item.interval_s,
            )
            for item in self._pending
        ]
        return self._observations

    # -------------------------------------------------------------- accessors
    @property
    def observations(self) -> list[Observation]:
        """The stamped rows. Raises if the cycle has not been finished."""
        if self._observations is None:
            raise RuntimeError("PollCycle is not finished; call finish() first")
        return self._observations

    @property
    def ts_utc(self) -> datetime | None:
        """The cycle timestamp — ``None`` until :meth:`finish` (unless pinned)."""
        return self._ts_utc

    @property
    def finished(self) -> bool:
        return self._observations is not None

    @property
    def gaps(self) -> int:
        """How many fields were absent this cycle. A health signal, never data."""
        return len(self._gaps)

    @property
    def gap_keys(self) -> tuple[tuple[str, str, str], ...]:
        """``(device_id, channel_id, metric)`` of every skipped field (DEBUG logs)."""
        return tuple(self._gaps)

    def __len__(self) -> int:
        """Rows collected so far (or produced, once finished)."""
        return len(self._pending)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        state = "finished" if self.finished else "open"
        return f"<PollCycle {self.source} {state} rows={len(self._pending)} gaps={self.gaps}>"

    # -------------------------------------------------------- context manager
    def __enter__(self) -> PollCycle:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        # A cycle that blew up mid-fetch produces nothing: partial data stamped
        # with a completion time we never reached would be a fabrication.
        if exc_type is None and self._observations is None:
            self.finish()
        return False


# -------------------------------------------------------------- the protocol


@runtime_checkable
class Source(Protocol):
    """What ``stages/poller.py`` and ``energycap discover`` require of a source.

    Structural: an implementation does not have to inherit from anything (though
    :class:`BaseSource` saves work). Lifecycle::

        source = LevitonSource(settings)
        await source.start()                 # auth + first discovery
        tasks = source.background_tasks()    # keepalive, re-discovery
        while running:
            rows = await source.poll()       # [] on a failed cycle
        await source.close()

    ``poll()`` never returns fabricated rows and never raises for an ordinary
    upstream failure — it either returns the rows it genuinely observed or
    raises :class:`SourceTransientError` / :class:`SourceAuthError` so the loop
    can count the failure and continue.
    """

    #: ``"leviton"`` / ``"bryant"`` / ``"lge"`` — must be in ``model.SOURCES``.
    name: str
    #: Cadence for this source, seconds; never below ``MIN_POLL_INTERVAL_S``.
    poll_interval_s: int

    async def start(self) -> None:
        """Authenticate and run the first discovery pass (PLAN.md §6.2)."""
        ...

    async def discover(self, *, force: bool = False) -> Discovery:
        """Enumerate live devices/channels; cached unless ``force``."""
        ...

    async def poll(self) -> list[Observation]:
        """One poll cycle. All returned rows share a single ``ts_utc`` (§6.5)."""
        ...

    def background_tasks(self) -> Sequence[BackgroundTask]:
        """Periodic work the runner must schedule (keepalive, re-discovery)."""
        ...

    async def close(self) -> None:
        """Release HTTP sessions and any other resources. Must be idempotent."""
        ...


# ------------------------------------------------------------- optional base


class BaseSource(ABC):
    """Optional base class implementing the parts every source shares.

    Subclasses set :attr:`name`, implement :meth:`poll` and :meth:`discover`, and
    get the poll-interval floor, discovery caching, a periodic re-discovery
    background task, and :meth:`new_cycle` for free.
    """

    #: Overridden by each subclass; must be a member of ``model.SOURCES``.
    name: ClassVar[str] = ""

    def __init__(
        self,
        *,
        poll_interval_s: int,
        discovery_interval_s: int | None = None,
    ) -> None:
        if self.name not in SOURCES:
            raise ValueError(
                f"{type(self).__name__}.name={self.name!r} is not one of {sorted(SOURCES)}"
            )
        # The floor is in code, not only in config: PLAN.md §6.6 / CLAUDE.md
        # "Poll intervals have hard floors in code, regardless of the env var".
        self.poll_interval_s = max(int(poll_interval_s), MIN_POLL_INTERVAL_S)
        self.discovery_interval_s = (
            int(discovery_interval_s) if discovery_interval_s else None
        )
        self._discovery: Discovery | None = None

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Default startup: one discovery pass. Override to add authentication."""
        await self.discover(force=True)

    async def close(self) -> None:
        """No-op; override to close HTTP clients. Must tolerate repeat calls."""
        return None

    def background_tasks(self) -> Sequence[BackgroundTask]:
        """Periodic re-discovery so new hardware appears without a restart (§6.2)."""
        if not self.discovery_interval_s:
            return ()
        return (
            BackgroundTask(
                name=f"{self.name}_discovery",
                interval_s=float(self.discovery_interval_s),
                run=self._rediscover,
                initial_delay_s=float(self.discovery_interval_s),
            ),
        )

    async def _rediscover(self) -> None:
        await self.discover(force=True)

    # ------------------------------------------------------------- discovery
    @property
    def cached_discovery(self) -> Discovery | None:
        """The last successful discovery, or ``None`` before the first pass."""
        return self._discovery

    def _remember(self, discovery: Discovery) -> Discovery:
        """Cache and return a discovery result (call from :meth:`discover`)."""
        self._discovery = discovery
        return discovery

    # ------------------------------------------------------------- polling
    def new_cycle(self, *, ts_utc: datetime | None = None) -> PollCycle:
        """A :class:`PollCycle` bound to this source's name."""
        return PollCycle(self.name, ts_utc=ts_utc)

    @abstractmethod
    async def discover(self, *, force: bool = False) -> Discovery:
        """Enumerate devices/channels; return the cache unless ``force``."""

    @abstractmethod
    async def poll(self) -> list[Observation]:
        """One poll cycle's observations (possibly empty — never fabricated)."""
