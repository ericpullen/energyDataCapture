"""The asyncio poll loop: cloud sources -> SQLite spool (PLAN.md §5, §10).

This is the stage that actually collects data. Everything else in the pipeline
moves bytes that this module wrote.

What it guarantees
------------------

**One transaction per poll cycle** (PLAN.md §10, "Poller"). A cycle's rows are
handed to :meth:`SpoolDB.append` as a single batch, which validates the whole
batch and then commits it inside one ``BEGIN IMMEDIATE`` — so a crash leaves
either the entire cycle or none of it, never half.

**A failed cycle writes zero rows** (CLAUDE.md rule 1). The source raises
:class:`~energy_capture.sources.base.SourceTransientError` or
:class:`~energy_capture.sources.base.SourceAuthError`, this module logs once,
bumps the section's ``consecutive_failures`` in ``status.json`` and moves on. It
never repeats the last reading, never zero-fills, and never turns an empty cycle
into a fabricated one.

**One source's failure never touches another's.** Each source gets its own
:class:`SourcePoller` running its own loop task on its own cadence; a cycle is
wrapped so that *any* exception — including a programming error inside a source
— is contained, logged and counted. The loop keeps running: PLAN.md §6.6 is
explicit that the poll loop never crashes.

**The cadence is the *sampling* cadence, and nothing changes it.** Since
2026-08-16 the Leviton source can keep its values fresh over a WebSocket instead
of re-reading them over REST (``LEVITON_INGEST``; see ``sources/leviton.py`` for
the measurement that motivated it — 10 of 12 channels frozen across 46
consecutive REST reads). That is a *freshness* mechanism and it stops at the
source boundary: ``poll()`` is still called every ``POLL_INTERVAL_S``, still
returns one set of rows carrying one ``ts_utc``, and still returns zero rows when
it does not know a value. Nothing in this module knows a socket exists, and
PLAN.md §2.5's kWh formula and ``sample_count``'s meaning as the gap detector —
both of which assume a fixed cadence — are untouched.

**The 30s floor is in code and is logged once at startup.** ``config.Settings``
clamps ``POLL_INTERVAL_S`` silently (it cannot log from inside its own
validator — DEVIATIONS.md #11) and ``BaseSource`` clamps again. :meth:`Poller.start`
is where the effective cadence becomes visible: it logs one
``poll_interval_effective`` line per source, at WARNING when the configured
value was floored.

Layering
--------

``stages/poller.py`` knows about *sources in general*; it knows nothing about
Leviton or Carrier. It also does not own the process: signal handling, the
health server and the scheduler live in :mod:`energy_capture.runtime`, which
drives the pieces exported here (:class:`Poller`, :func:`run_background_task`,
:func:`wait_or_stop`). The dependency runs one way — ``runtime`` imports
``poller``, never the reverse.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from energy_capture.config import MIN_POLL_INTERVAL_S, Settings, get_settings
from energy_capture.logging import get_logger
from energy_capture.sources.base import (
    BackgroundTask,
    Source,
    SourceAuthError,
    SourceTransientError,
)
from energy_capture.spool.sqlite import SpoolDB, open_spool

__all__ = [
    "SOURCE_FACTORIES",
    "SOURCE_STATUS_SECTIONS",
    "STAGE",
    "CycleResult",
    "Poller",
    "SourcePoller",
    "SourceUnavailable",
    "build_sources",
    "run",
    "run_background_task",
    "wait_or_stop",
]

STAGE = "poller"

#: ``source.name`` -> the ``status.json`` section it owns (PLAN.md §11). The
#: Bryant *status* poller writes ``bryant_status``; ``bryant_daily`` belongs to
#: the once-a-day energy fetch, which is a scheduled job, not a poll loop.
SOURCE_STATUS_SECTIONS: Mapping[str, str] = {
    "leviton": "leviton",
    "bryant": "bryant_status",
}

#: How often the poller refreshes the ``spool`` gauge in ``status.json``.
#: ``SpoolDB.stats()`` scans the table, so it is deliberately not per-cycle; the
#: uploader refreshes it too, on every hourly run.
SPOOL_STATUS_INTERVAL_S: float = 300.0

_log = get_logger(STAGE)


class SourceUnavailable(RuntimeError):
    """A source was explicitly requested but its module has not landed yet."""


# ------------------------------------------------------------ source registry


def _leviton(settings: Settings) -> Source:
    from energy_capture.sources.leviton import LevitonSource

    return LevitonSource(settings)


def _bryant(settings: Settings) -> Source:
    # PLAN.md §7.3 — the 30s status poller. Construction must not need
    # credentials or a network: the source resolves configuration at poll time,
    # so a container with a missing CARRIER_* var boots and reports itself
    # unhealthy rather than refusing to start (and Leviton keeps collecting).
    from energy_capture.sources.bryant import BryantStatusSource

    return BryantStatusSource(settings)


#: ``--source`` name -> factory. Ordered: this is also the order sources are
#: started and reported in.
SOURCE_FACTORIES: Mapping[str, Callable[[Settings], Source]] = {
    "leviton": _leviton,
    "bryant": _bryant,
}


def _is_missing_source_module(exc: ModuleNotFoundError, name: str) -> bool:
    """True when ``exc`` is "the ``sources/{name}.py`` module does not exist".

    A source module that exists but imports a missing third-party package is a
    real deployment bug and must not be disguised as "not implemented yet" —
    same rule the CLI applies to stage modules.
    """
    return exc.name == f"energy_capture.sources.{name}"


def build_sources(
    names: Sequence[str] | None = None,
    *,
    settings: Settings | None = None,
    factories: Mapping[str, Callable[[Settings], Source]] | None = None,
) -> list[Source]:
    """Construct the requested sources.

    Args:
        names: source names (``("leviton",)``). ``None`` means every registered
            source, and then a source whose module has not landed yet is skipped
            with a WARN instead of stopping the process — that is what lets
            ``energycap run`` work today with only Leviton built (PLAN.md §16).
            An *explicitly* named missing source raises
            :class:`SourceUnavailable`, because silently doing nothing when the
            operator asked for something specific is worse than failing.

    Raises:
        ValueError: unknown source name.
        SourceUnavailable: an explicitly requested source is not implemented.
        RuntimeError: nothing could be constructed.
    """
    registry = dict(factories if factories is not None else SOURCE_FACTORIES)
    resolved = get_settings() if settings is None else settings
    explicit = names is not None
    wanted = list(names) if names is not None else list(registry)

    unknown = [name for name in wanted if name not in registry]
    if unknown:
        raise ValueError(
            f"unknown source(s) {sorted(unknown)}; known sources are {sorted(registry)}"
        )

    sources: list[Source] = []
    for name in wanted:
        try:
            sources.append(registry[name](resolved))
        except ModuleNotFoundError as exc:
            if not _is_missing_source_module(exc, name):
                raise
            if explicit:
                raise SourceUnavailable(
                    f"source {name!r} is not implemented yet "
                    f"(no energy_capture.sources.{name})"
                ) from None
            _log.warning(
                "source_not_implemented",
                source=name,
                module=f"energy_capture.sources.{name}",
                detail="skipped; the rest of the pipeline runs without it",
            )
    if not sources:
        raise RuntimeError(
            f"no pollable sources: requested {wanted}, none could be constructed"
        )
    return sources


# ------------------------------------------------------------------ scheduling


async def wait_or_stop(
    stop: asyncio.Event,
    delay: float,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> bool:
    """Sleep ``delay`` seconds or until ``stop`` is set. Returns True if stopped.

    Shutdown must not wait out a 50s keepalive interval, so the sleep races the
    stop event rather than being polled. ``sleep`` is injectable so tests can run
    the loops with no real time passing.
    """
    if stop.is_set():
        return True
    if delay <= 0:
        await sleep(0)
        return stop.is_set()

    sleeper = asyncio.ensure_future(sleep(delay))
    waiter = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({sleeper, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (sleeper, waiter):
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
    return stop.is_set()


async def run_background_task(
    task: BackgroundTask,
    stop: asyncio.Event,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    """Run a source's :class:`BackgroundTask` until ``stop`` is set.

    This is how the Leviton bandwidth keepalive (PLAN.md §6.4) and periodic
    re-discovery (§6.2) reach the event loop. Exceptions are logged and absorbed:
    a failing background task must never take polling down with it, and the
    keepalive manages its own backoff internally, so no scheduler-level backoff
    is stacked on top of it.

    Returns:
        How many times the task body ran.
    """
    log = _log.bind(task=task.name)
    runs = 0
    if task.initial_delay_s > 0 and await wait_or_stop(stop, task.initial_delay_s, sleep):
        return runs
    log.debug("background_task_started", interval_s=task.interval_s)
    while not stop.is_set():
        try:
            await task.run()
            runs += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # never let a background task kill the process
            runs += 1
            log.exception(
                "background_task_failed", error=f"{type(exc).__name__}: {exc}"
            )
        if await wait_or_stop(stop, task.interval_s, sleep):
            break
    log.debug("background_task_stopped", runs=runs)
    return runs


# ------------------------------------------------------------------ one cycle


@dataclass(frozen=True, slots=True)
class CycleResult:
    """The outcome of one poll cycle — what gets logged and counted."""

    source: str
    ok: bool
    #: Rows the source produced. Zero is a legitimate outcome (every field was
    #: null); it is never turned into a fabricated sample.
    rows: int = 0
    #: Rows actually inserted (the spool ignores duplicates of the dedupe key).
    inserted: int = 0
    channels: int = 0
    duration_s: float = 0.0
    #: ``"transient"``, ``"auth"``, ``"spool"`` or ``"unexpected"``.
    failure: str | None = None
    error: str | None = None

    @property
    def duplicates(self) -> int:
        return max(self.rows - self.inserted, 0)


class SourcePoller:
    """Drives exactly one :class:`Source` at its configured cadence.

    Owns the source's ``status.json`` section and its consecutive-failure
    counter. Nothing here knows which cloud it is talking to.
    """

    __slots__ = (
        "_consecutive_failures",
        "_cycles",
        "_last_spool_status",
        "_log",
        "_monotonic",
        "_sleep",
        "_spool",
        "_spool_status_interval_s",
        "_status",
        "source",
        "status_section",
    )

    def __init__(
        self,
        source: Source,
        spool: SpoolDB,
        *,
        status: Any = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        spool_status_interval_s: float = SPOOL_STATUS_INTERVAL_S,
    ) -> None:
        self.source = source
        self.status_section = SOURCE_STATUS_SECTIONS.get(source.name, source.name)
        self._spool = spool
        self._status = status
        self._monotonic = monotonic
        self._sleep = sleep
        self._spool_status_interval_s = float(spool_status_interval_s)
        self._last_spool_status: float | None = None
        self._consecutive_failures = 0
        self._cycles = 0
        self._log = _log.bind(source=source.name)

    # -------------------------------------------------------------- accessors
    @property
    def name(self) -> str:
        return self.source.name

    @property
    def interval_s(self) -> int:
        """Effective cadence — never below :data:`MIN_POLL_INTERVAL_S`."""
        return max(int(self.source.poll_interval_s), MIN_POLL_INTERVAL_S)

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def cycles(self) -> int:
        return self._cycles

    def _store(self) -> Any:
        """The ``StatusStore``, resolved lazily so importing is cheap."""
        if self._status is None:
            from energy_capture.health import get_status_store

            self._status = get_status_store()
        return self._status

    # ------------------------------------------------------------- one cycle
    async def cycle(self) -> CycleResult:
        """Poll once and spool the result. Never raises for an upstream failure."""
        started = self._monotonic()
        self._cycles += 1
        try:
            observations = await self.source.poll()
        except asyncio.CancelledError:
            raise
        except SourceAuthError as exc:
            return self._failed("auth", exc, started, level="error")
        except SourceTransientError as exc:
            return self._failed("transient", exc, started, level="warning")
        except Exception as exc:
            # A bug in a source is still not allowed to stop collection.
            return self._failed("unexpected", exc, started, level="exception")

        rows = list(observations)
        try:
            inserted = self._spool.append(rows)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The rows are gone — that is a gap, and gaps stay gaps. Recording a
            # failure is the honest outcome; inventing a retry buffer here would
            # duplicate the spool's job.
            return self._failed("spool", exc, started, level="exception", rows=len(rows))

        duration_s = round(self._monotonic() - started, 4)
        channels = len({(obs.device_id, obs.channel_id) for obs in rows})
        self._consecutive_failures = 0
        self._record_success(rows=len(rows), inserted=inserted, channels=channels)
        result = CycleResult(
            source=self.name,
            ok=True,
            rows=len(rows),
            inserted=inserted,
            channels=channels,
            duration_s=duration_s,
        )
        self._log.info(
            "poll_cycle_ok",
            rows=result.rows,
            inserted=result.inserted,
            duplicates=result.duplicates,
            channels=result.channels,
            duration_s=result.duration_s,
        )
        return result

    def _failed(
        self,
        failure: str,
        exc: BaseException,
        started: float,
        *,
        level: str,
        rows: int = 0,
    ) -> CycleResult:
        self._consecutive_failures += 1
        duration_s = round(self._monotonic() - started, 4)
        error = f"{type(exc).__name__}: {exc}"
        getattr(self._log, level)(
            "poll_cycle_failed",
            failure=failure,
            error=error,
            consecutive_failures=self._consecutive_failures,
            duration_s=duration_s,
            rows_lost=rows,
        )
        self._record_failure(exc, failure=failure)
        return CycleResult(
            source=self.name,
            ok=False,
            rows=0,
            duration_s=duration_s,
            failure=failure,
            error=error,
        )

    # ------------------------------------------------------------ status.json
    def _record_success(self, *, rows: int, inserted: int, channels: int) -> None:
        fields: dict[str, Any] = {"rows": rows, "inserted": inserted}
        if channels:
            # A cycle in which every field was null must not erase the last known
            # channel count with a 0 — "nothing was readable this cycle" and "the
            # panel has no channels" are different facts.
            fields["channels_seen"] = channels
        self._safe_status("record_success", self.status_section, **fields)
        self._maybe_refresh_spool_gauge()

    def _record_failure(self, exc: BaseException, *, failure: str) -> None:
        self._safe_status("record_failure", self.status_section, exc, failure=failure)

    def _maybe_refresh_spool_gauge(self) -> None:
        now = self._monotonic()
        if (
            self._last_spool_status is not None
            and now - self._last_spool_status < self._spool_status_interval_s
        ):
            return
        self._last_spool_status = now
        try:
            stats = self._spool.stats()
        except Exception as exc:  # telemetry must never break collection
            self._log.warning("spool_stats_failed", error=f"{type(exc).__name__}: {exc}")
            return
        self._safe_status("set", "spool", **stats.to_status_dict())

    def _safe_status(self, method: str, *args: Any, **fields: Any) -> None:
        """Call the status store, swallowing failures.

        ``status.json`` is telemetry about the pipeline; the pipeline outranks
        it. :class:`StatusStore` already absorbs write errors, so this only
        guards against a store that is missing or misconfigured entirely.
        """
        try:
            getattr(self._store(), method)(*args, **fields)
        except Exception as exc:  # pragma: no cover - defensive
            self._log.warning("status_update_failed", error=f"{type(exc).__name__}: {exc}")

    # --------------------------------------------------------------- the loop
    async def run_forever(self, stop: asyncio.Event) -> None:
        """Cycle every :attr:`interval_s` seconds until ``stop`` is set.

        The next deadline is computed from the previous *deadline*, not from the
        time the last cycle finished, so cadence does not drift by the duration
        of each poll. A cycle that overruns its interval resyncs instead of
        firing a burst of catch-up polls at a cloud that is already struggling.
        """
        self._log.info("poll_loop_started", interval_s=self.interval_s)
        next_at = self._monotonic()
        while not stop.is_set():
            await self.cycle()
            if stop.is_set():
                break
            next_at += self.interval_s
            delay = next_at - self._monotonic()
            if delay <= 0:
                self._log.warning("poll_cycle_overran", overrun_s=round(-delay, 3))
                next_at = self._monotonic()
                delay = 0.0
            if await wait_or_stop(stop, delay, self._sleep):
                break
        self._log.info(
            "poll_loop_stopped",
            cycles=self._cycles,
            consecutive_failures=self._consecutive_failures,
        )


# ------------------------------------------------------------------ the poller


class Poller:
    """All the source loops, plus their lifecycle and background tasks.

    ``energycap run`` (via :mod:`energy_capture.runtime`) and ``energycap poll``
    both drive this class; the only difference is whether it runs one cycle or
    forever.
    """

    __slots__ = ("_log", "_pollers", "_sleep", "_spool", "_started", "_status")

    def __init__(
        self,
        sources: Sequence[Source],
        spool: SpoolDB,
        *,
        status: Any = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        spool_status_interval_s: float = SPOOL_STATUS_INTERVAL_S,
    ) -> None:
        if not sources:
            raise ValueError("Poller needs at least one source")
        self._spool = spool
        self._status = status
        self._sleep = sleep
        self._log = _log
        self._started: list[Source] = []
        self._pollers = [
            SourcePoller(
                source,
                spool,
                status=status,
                monotonic=monotonic,
                sleep=sleep,
                spool_status_interval_s=spool_status_interval_s,
            )
            for source in sources
        ]

    # -------------------------------------------------------------- accessors
    @property
    def pollers(self) -> tuple[SourcePoller, ...]:
        return tuple(self._pollers)

    @property
    def sources(self) -> tuple[Source, ...]:
        return tuple(poller.source for poller in self._pollers)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(poller.name for poller in self._pollers)

    def background_tasks(self) -> tuple[BackgroundTask, ...]:
        """Every source's periodic work (keepalive, re-discovery)."""
        tasks: list[BackgroundTask] = []
        for poller in self._pollers:
            try:
                tasks.extend(poller.source.background_tasks())
            except Exception as exc:  # pragma: no cover - defensive
                self._log.warning(
                    "background_tasks_unavailable",
                    source=poller.name,
                    error=f"{type(exc).__name__}: {exc}",
                )
        return tuple(tasks)

    def _store(self) -> Any:
        if self._status is None:
            from energy_capture.health import get_status_store

            self._status = get_status_store()
        return self._status

    # -------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Log the effective cadences, wire health, and start every source.

        A source that fails to start (cloud down at boot, expired credentials) is
        **kept**: ``poll()`` re-authenticates and re-discovers on its own, and a
        container that refuses to boot because a third-party cloud is having a
        bad minute is worse than one that boots degraded and heals.
        """
        self._log_intervals()
        self._wire_health()
        for poller in self._pollers:
            try:
                await poller.source.start()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.error(
                    "source_start_failed",
                    source=poller.name,
                    error=f"{type(exc).__name__}: {exc}",
                    detail="kept in the loop; poll() will retry and re-authenticate",
                )
                poller._record_failure(exc, failure="start")
            self._started.append(poller.source)

    def _log_intervals(self) -> None:
        """One line per source stating the cadence actually in force.

        DEVIATIONS.md #11: the 30s floor is applied silently inside ``Settings``
        (a validator cannot log without recursing into ``get_settings``), so the
        running process is where it becomes visible. The configured value is read
        back from the environment purely to say *whether* it was floored.
        """
        for poller in self._pollers:
            effective = poller.interval_s
            configured = _configured_interval(poller.name)
            floored = configured is not None and configured < effective
            fields: dict[str, Any] = {
                "source": poller.name,
                "poll_interval_s": effective,
                "floor_s": MIN_POLL_INTERVAL_S,
            }
            if configured is not None:
                fields["configured_s"] = configured
            if floored:
                self._log.warning("poll_interval_floored", **fields)
            else:
                self._log.info("poll_interval_effective", **fields)

    def _wire_health(self) -> None:
        """Judge exactly the sources that are running (PLAN.md §11).

        A source that is not running must be *dropped* from the staleness check —
        otherwise ``/healthz`` reports 503 forever for a poller that was never
        supposed to be up (the Bryant status poller, until PLAN.md §7.3 lands).

        Only *pollers* are watched, and the set is exactly one section per
        :class:`SourcePoller`. Sections a source writes for its own conditions —
        ``leviton_keepalive``, ``leviton_auth``, ``leviton_ws``,
        ``leviton_ingest`` — are never registered here, so a dead WebSocket
        cannot fail ``/healthz`` while REST is still landing rows every 30s. That
        is deliberate: liveness is "are observations arriving", not "is every
        mechanism that could produce them healthy".
        """
        try:
            store = self._store()
            running = {poller.status_section for poller in self._pollers}
            for poller in self._pollers:
                store.watch_poller(poller.status_section, poller.interval_s)
            for section in list(store.poll_intervals):
                if section not in running:
                    store.forget_poller(section)
                    self._log.info("health_check_dropped", section=section)
        except Exception as exc:  # pragma: no cover - defensive
            self._log.warning("health_wiring_failed", error=f"{type(exc).__name__}: {exc}")

    async def close(self) -> None:
        """Close every source that was started. Idempotent, never raises."""
        for source in self._started:
            try:
                await source.close()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.warning(
                    "source_close_failed",
                    source=getattr(source, "name", "?"),
                    error=f"{type(exc).__name__}: {exc}",
                )
        self._started.clear()

    # ---------------------------------------------------------------- running
    async def poll_once(self) -> list[CycleResult]:
        """One cycle per source, concurrently. Used by ``energycap poll --once``."""
        return list(
            await asyncio.gather(*(poller.cycle() for poller in self._pollers))
        )

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Run every source's loop until ``stop`` is set.

        Each loop is its own task: a source that stalls on a slow HTTP call
        cannot delay another source's cycle, and a task that somehow dies is
        logged without taking its siblings with it.
        """
        tasks = [
            asyncio.create_task(poller.run_forever(stop), name=f"poll-{poller.name}")
            for poller in self._pollers
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            raise
        for poller, outcome in zip(self._pollers, results):
            if isinstance(outcome, asyncio.CancelledError):
                continue
            if isinstance(outcome, BaseException):  # pragma: no cover - defensive
                self._log.exception(
                    "poll_loop_crashed",
                    source=poller.name,
                    error=f"{type(outcome).__name__}: {outcome}",
                )

    def summary(self) -> dict[str, Any]:
        """Loggable counters for the CLI's ``stage_ok`` line."""
        return {
            "sources": list(self.names),
            "cycles": sum(poller.cycles for poller in self._pollers),
            "failures": sum(
                1 for poller in self._pollers if poller.consecutive_failures
            ),
        }


def _configured_interval(source_name: str) -> int | None:
    """The raw ``*_POLL_INTERVAL_S`` env value, for the floor log line only."""
    var = "BRYANT_POLL_INTERVAL_S" if source_name == "bryant" else "POLL_INTERVAL_S"
    raw = os.environ.get(var)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# --------------------------------------------------------------- CLI entry point


async def run(
    *,
    once: bool = False,
    sources: Sequence[str] | None = None,
    spool: SpoolDB | None = None,
    status: Any = None,
    settings: Settings | None = None,
    stop: asyncio.Event | None = None,
    source_objects: Sequence[Source] | None = None,
    background: bool | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """``energycap poll [--once] [--source …]`` (``cli.STAGE_ENTRYPOINTS``).

    ``--once`` runs exactly one cycle per source and exits — that is the manual
    smoke test in PLAN.md §16's definition of done, and it is idempotent because
    the spool's UNIQUE index over the dedupe key ignores a repeat of the same
    instant.

    Without ``--once`` this runs the loops (and the sources' background tasks,
    so Leviton readings are not stale — PLAN.md §6.4) until the process is
    interrupted. The full production host, with the scheduler and the health
    server, is ``energycap run`` (:mod:`energy_capture.runtime`).
    """
    # Build the sources before touching the spool: a missing credential or an
    # unimplemented source is a startup error, and it should not leave a spool
    # connection open behind it.
    built = (
        list(source_objects)
        if source_objects is not None
        else build_sources(sources, settings=settings)
    )
    owns_spool = spool is None
    resolved_spool = spool if spool is not None else open_spool()
    poller = Poller(
        built,
        resolved_spool,
        status=status,
        sleep=sleep,
        monotonic=monotonic,
    )
    stop_event = stop if stop is not None else asyncio.Event()
    run_background = (not once) if background is None else background

    try:
        await poller.start()
        if once:
            results = await poller.poll_once()
            summary = poller.summary()
            summary.update(
                once=True,
                rows=sum(result.rows for result in results),
                inserted=sum(result.inserted for result in results),
                failed=sum(0 if result.ok else 1 for result in results),
            )
            return summary

        tasks = [asyncio.create_task(poller.run_forever(stop_event), name="poll-loops")]
        if run_background:
            tasks.extend(
                asyncio.create_task(
                    run_background_task(task, stop_event, sleep=sleep),
                    name=f"bg-{task.name}",
                )
                for task in poller.background_tasks()
            )
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            stop_event.set()
            raise
        except (KeyboardInterrupt, SystemExit):  # pragma: no cover - interactive
            stop_event.set()
            raise
        summary = poller.summary()
        summary.update(once=False)
        return summary
    finally:
        stop_event.set()
        await poller.close()
        if owns_spool:
            resolved_spool.close()
