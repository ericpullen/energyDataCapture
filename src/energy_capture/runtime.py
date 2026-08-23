"""``energycap run`` — the long-running process (PLAN.md §5).

One container, one process, five things running inside it:

1. the asyncio poll loops (``stages/poller.py``) writing to the SQLite spool;
2. each source's background tasks — the Leviton bandwidth keepalive every 50s
   (PLAN.md §6.4, unchanged: ``PUT {"bandwidth": 1}``, never 0), periodic
   re-discovery, and — when ``LEVITON_INGEST`` asks for it — the Leviton
   WebSocket supervisor and the periodic REST reconcile;
3. an in-process scheduler firing the jobs of PLAN.md §5;
4. the ``/healthz`` server (PLAN.md §11);
5. signal handling, so ``docker stop`` is a clean shutdown and not data loss.

Freshness vs sampling
---------------------

Measurement on 2026-08-16 showed the Leviton REST endpoint serving a cache —
10 of 12 channels unchanged across 46 consecutive reads, unaffected by the
keepalive — so ``sources/leviton_ws.py`` now keeps an in-memory current-state
store fresh over the push socket. **This host schedules that work; it does not
change the shape of the process.** The poll loops still run at
``POLL_INTERVAL_S``, still produce one set of rows per cycle with one ``ts_utc``,
and still write nothing when they do not know a value. The socket arrives here
as one more :class:`~energy_capture.sources.base.BackgroundTask` among the
keepalive and re-discovery, supervised by ``run_background_task``, which logs and
absorbs anything it raises — so a WebSocket failure can no more take the process
down than a failed keepalive PUT can. ``/healthz`` judges pollers only, so a dead
socket does not turn the container unhealthy while REST is still collecting.

The schedule
------------

============================  ==================================================
job                           when (LOCAL time)
============================  ==================================================
``upload_hourly``             every hour at :05 — every closed hour with rows
``rollup_hourly``             every hour at :20 — the local day(s) touched by HH-1
``daily_maintenance``         01:30 — upload catch-up, compact D-1, re-roll D-1,
                              then purge uploaded spool rows past the retention
                              floor (PLAN.md §10 — nothing else calls ``purge``)
``bryant_daily_energy``       08:30 — Carrier daily energy for day2..day1 (§7.2)
``greenbutton_daily``         09:15 — LG&E meter intervals for D-3..today (§13);
                              skipped without complaint until Connect is authorised
``dim_channel`` rebuild       never — on demand only (``energycap build-dim``)
============================  ==================================================

**Scheduling is in local time and is DST-correct**, which is the whole reason
this module does not do ``sleep(3600)`` arithmetic:

* Hourly jobs are computed from :func:`timeutil.utc_hour_start`. Every US DST
  offset is a whole number of hours, so a UTC hour boundary *is* a local hour
  boundary. On the fall-back day the hourly jobs therefore fire **25** times —
  once for each of the two physical 01:00 hours, both of which contain real data
  — and on the spring-forward day **23** times, never in the 02:00 hour that
  does not exist.
* Daily jobs are resolved by asking :func:`timeutil.local_naive_to_utc` for the
  instant of the next local wall-clock time, so 01:30 and 08:30 stay 01:30 and
  08:30 across a transition even though the UTC instant moves by an hour. On the
  fall-back day the ambiguous local time resolves to its *first* occurrence and
  the job fires exactly once, not twice.
* ``bryant_daily_energy`` is the one job whose date window does **not** come
  from its firing slot. Carrier's ``day1``/``day2`` are relative to the instant
  of the fetch, so the job takes a single fresh clock read and uses it for both
  the window and the stage's dating (it is passed down as ``now=``), and WARNs
  if that read disagrees with the slot's local date. See
  :func:`_job_bryant_daily`.

Failure containment
-------------------

A scheduled job that throws is logged (``job_failed``) and recorded in
``status.json``; the scheduler computes its next run and carries on, and the
other jobs and the poll loops are untouched. The Bryant daily energy job keeps a
defensive path for the case where ``stages/daily.py`` cannot be imported at all
(a partially built image): it logs one WARN per firing and skips, rather than
taking the poll loops down with it. The stage itself has landed (PLAN.md §7.2).

Shutdown
--------

``SIGTERM``/``SIGINT`` set a stop event. The poll loops finish the cycle they
are in (its rows are already committed, or are committed before the loop
exits), stop accepting new ones, and the process then closes the sources —
which is what tears the Leviton WebSocket down cleanly, since the socket belongs
to the source that owns it — flushes the spool gauge into ``status.json``, closes
the spool and stops the health server. Exit is zero: a requested shutdown is not
a failure. Only a genuine startup or task crash exits non-zero.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import signal
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Protocol

from energy_capture import timeutil
from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger
from energy_capture.sources.base import Source
from energy_capture.spool.sqlite import SpoolDB, open_spool
from energy_capture.stages.poller import (
    Poller,
    build_sources,
    run_background_task,
    wait_or_stop,
)

__all__ = [
    "BRYANT_DAILY_AT",
    "DAILY_MAINTENANCE_AT",
    "GREENBUTTON_DAILY_AT",
    "UPLOAD_MINUTE",
    "ROLLUP_MINUTE",
    "DailyAt",
    "HourlyAt",
    "JobOutcome",
    "Runtime",
    "ScheduledJob",
    "Scheduler",
    "default_jobs",
    "run",
]

STAGE = "runtime"

#: Local minute-of-hour for the hourly jobs (PLAN.md §5: "~HH:05" / "~HH:20").
UPLOAD_MINUTE = 5
ROLLUP_MINUTE = 20

#: Local wall-clock times for the daily jobs (PLAN.md §5, §7.2).
DAILY_MAINTENANCE_AT = (1, 30)
BRYANT_DAILY_AT = (8, 30)

#: LG&E publishes overnight and lags several hours; 09:15 local is comfortably
#: after that and does not collide with the 08:30 Carrier fetch.
GREENBUTTON_DAILY_AT = (9, 15)
#: Days of overlap re-read on every run, so a revised interval lands.
GREENBUTTON_LOOKBACK_DAYS = 3

#: How many days back the 01:30 job compacts and re-rolls. PLAN.md §10 specifies
#: D-1; the extra days are idempotent no-ops that heal a night the container
#: spent down (see DEVIATIONS note in the return report).
DAILY_LOOKBACK_DAYS = 3

#: The Bryant daily energy fetch lands ``day1`` (yesterday) and ``day2`` (the
#: day before, as a revision) — PLAN.md §7.2.
BRYANT_DAILY_LOOKBACK_DAYS = 2

#: Longest single sleep the scheduler takes. The delay to the next job is
#: computed exactly; this cap only bounds how long a wall-clock jump (NTP step)
#: can strand the loop.
MAX_SCHEDULER_SLEEP_S = 300.0

#: How long shutdown waits for in-flight work before cancelling it. One poll
#: cycle plus its in-cycle retries (2s + 5s) fits comfortably.
SHUTDOWN_TIMEOUT_S = 30.0

_log = get_logger(STAGE)


# ------------------------------------------------------------------ schedules


class Schedule(Protocol):
    """Anything that can say when a job should next fire."""

    def next_after(self, now_utc: datetime) -> datetime:
        """The first firing instant strictly after ``now_utc`` (aware UTC)."""
        ...

    def describe(self) -> str:
        """Human-readable local-time description, for the startup log line."""
        ...


@dataclass(frozen=True, slots=True)
class HourlyAt:
    """Every hour at ``minute`` past the local hour.

    Computed in UTC on purpose. Every US DST offset is a whole number of hours,
    so ``minute`` past a UTC hour is also ``minute`` past the local hour — and
    the count follows the *physical* hours of the day: 25 firings on the
    fall-back day (both 01:00 hours hold real data and each needs its own upload
    and rollup), 23 on the spring-forward day.
    """

    minute: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.minute < 60:
            raise ValueError(f"minute must be 0..59, got {self.minute}")

    def next_after(self, now_utc: datetime) -> datetime:
        now = timeutil.ensure_utc(now_utc)
        candidate = timeutil.utc_hour_start(now) + timedelta(minutes=self.minute)
        if candidate <= now:
            candidate += timedelta(hours=1)
        return candidate

    def describe(self) -> str:
        return f"hourly at :{self.minute:02d}"


@dataclass(frozen=True, slots=True)
class DailyAt:
    """Once a day at a local wall-clock time.

    Resolved through :func:`timeutil.local_naive_to_utc`, so the job stays at the
    same *local* time across a DST transition while its UTC instant moves.

    Two DST edge cases, both deliberate:

    * **Fall back** — the requested wall clock happens twice. ``fold=0`` picks
      the first occurrence and the second is skipped, so the job runs exactly
      once that day. (The 01:30 maintenance job must not compact D-1 twice.)
    * **Spring forward** — a wall clock inside the missing hour does not exist.
      Python resolves it by shifting, so the job still fires exactly once, an
      hour later in local terms. Neither configured job (01:30, 08:30) is
      affected; the 02:00 hour is empty by construction.
    """

    hour: int
    minute: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.hour < 24:
            raise ValueError(f"hour must be 0..23, got {self.hour}")
        if not 0 <= self.minute < 60:
            raise ValueError(f"minute must be 0..59, got {self.minute}")

    def next_after(self, now_utc: datetime) -> datetime:
        now = timeutil.ensure_utc(now_utc)
        today = timeutil.local_date_of(now)
        # Three candidates is one more than DST can ever need; it also covers a
        # zone whose UTC offset changed by more than an hour.
        for offset in range(3):
            wall = datetime.combine(
                today + timedelta(days=offset), dt_time(self.hour, self.minute)
            )
            candidate = timeutil.local_naive_to_utc(wall)
            if candidate > now:
                return candidate
        raise RuntimeError(  # pragma: no cover - unreachable for real zones
            f"no firing instant for {self.describe()} after {timeutil.format_utc(now)}"
        )

    def describe(self) -> str:
        return f"daily at {self.hour:02d}:{self.minute:02d} local"


# ---------------------------------------------------------------------- jobs

#: A job body. It receives the instant it fired at (aware UTC) so every date
#: window it computes comes from the scheduler's clock, never from a second,
#: possibly-different read of the wall clock.
#:
#: The one job that cannot use the slot instant is ``bryant_daily_energy``: the
#: Carrier cloud's ``day1``/``day2`` are relative to the moment of the *fetch*,
#: not to the slot we were scheduled for, so dating them from the slot would
#: mislabel the response. It takes exactly one fresh read instead and uses it for
#: both halves of the problem — see :func:`_job_bryant_daily`.
JobRunner = Callable[[datetime], Any]


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    """One entry in the schedule of PLAN.md §5."""

    name: str
    schedule: Schedule
    run: JobRunner
    #: What this job is for, in the startup log line.
    description: str = ""


@dataclass(frozen=True, slots=True)
class JobOutcome:
    """The result of one firing."""

    name: str
    ok: bool
    duration_s: float = 0.0
    result: Any = None
    error: str | None = None


class JobStepError(RuntimeError):
    """One or more steps of a multi-step job failed (the rest still ran)."""


def _loggable(value: Any) -> dict[str, Any]:
    """Fold a stage's return value into the ``job_ok`` log line."""
    if isinstance(value, Mapping):
        return {
            key: (list(val) if isinstance(val, (list, tuple, set)) else val)
            for key, val in value.items()
        }
    if isinstance(value, int) and not isinstance(value, bool):
        return {"rows": value}
    return {}


async def _call(func: Callable[..., Any], /, **kwargs: Any) -> Any:
    """Run a stage entry point off the event loop.

    Every stage is blocking (SQLite, boto3, DuckDB). Running one inline would
    stall the poll loops for the length of an upload, so they go to a worker
    thread. ``SpoolDB`` is explicitly one-connection-per-thread, so this is safe.
    """
    return await asyncio.to_thread(func, **kwargs)


async def _job_hourly_upload(now: datetime, *, spool: SpoolDB | None = None) -> Any:
    """Upload every closed local hour that still has un-uploaded spool rows.

    Called with no range on purpose: ``uploader.run()`` with no arguments is the
    true catch-up window, so an outage of any length drains in one firing
    (PLAN.md §10, "Handles multi-hour catch-up after downtime in one
    invocation").

    ``spool`` is the process's own :class:`SpoolDB`. Passing it matters: without
    it the uploader opens a *second* connection set to the same file every hour,
    which is legal under WAL but pointlessly re-runs the ``journal_mode``
    handshake against a database the poller is actively writing.
    """
    from energy_capture.stages import uploader

    return await _call(uploader.run, spool=spool)


async def _job_hourly_rollup(now: datetime) -> Any:
    """Regenerate the rollup for the local day(s) that hour HH-1 belongs to.

    The rollup always rebuilds a whole local day, so there is no partial-hour
    state to manage. Just after local midnight, hour HH-1 is yesterday's last
    hour, so the window spans both days — one extra cheap, idempotent rebuild.
    """
    from energy_capture.stages import rollup

    start = timeutil.local_date_of(now - timedelta(hours=1))
    end = timeutil.local_date_of(now)
    return await _call(rollup.run, start=start, end=end)


async def _job_spool_purge(now: datetime, *, spool: SpoolDB | None = None) -> Any:
    """Delete spool rows that are uploaded **and** past the retention floor.

    PLAN.md §10: "Spool rows are deleted only after their hour is verified
    uploaded, plus a 7-day retention floor (``SPOOL_RETENTION_DAYS=7``) as a
    second safety net." :meth:`SpoolDB.purge` enforces both conditions itself —
    a row that is old but never uploaded is un-landed data and is kept — so this
    job is only the thing that calls it. Nothing else does, and without it
    ``spool.db`` grows for the life of the container.
    """
    if spool is None:  # pragma: no cover - only when no runtime owns the spool
        return {"skipped": "no_spool"}
    deleted = await _call(spool.purge, now=now)
    return {"purged_rows": deleted}


async def _job_daily_maintenance(
    now: datetime,
    *,
    lookback_days: int = DAILY_LOOKBACK_DAYS,
    spool: SpoolDB | None = None,
) -> dict[str, Any]:
    """The 01:30 job: upload catch-up, compact D-1, re-roll D-1, purge the spool.

    Order matters. The compactor must run *after* the uploader has drained D-1,
    or a part landing a minute later forces a second compaction pass. The rollup
    runs last of the S3 stages so it sees the compacted day file. The purge runs
    after all of them because it deletes only rows the uploader has already
    marked, and running it last means a failed upload cannot even in principle
    race it.

    Each step is attempted even if an earlier one failed — a broken rollup must
    not strand a week of parts under ``raw_30s`` — and the job then raises so the
    failure is visible in the log and in ``status.json``.
    """
    from energy_capture.stages import compactor, rollup, uploader

    today = timeutil.local_date_of(now)
    end = today - timedelta(days=1)
    start = today - timedelta(days=max(int(lookback_days), 1))

    steps: tuple[tuple[str, Callable[[], Awaitable[Any]]], ...] = (
        ("upload", lambda: _call(uploader.run, spool=spool)),
        ("compact", lambda: _call(compactor.run, start=start, end=end)),
        ("rollup", lambda: _call(rollup.run, start=start, end=end)),
        ("purge", lambda: _job_spool_purge(now, spool=spool)),
    )

    summary: dict[str, Any] = {"start": start.isoformat(), "end": end.isoformat()}
    failures: list[str] = []
    first: BaseException | None = None
    for label, step in steps:
        try:
            result = await step()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            first = first if first is not None else exc
            failures.append(label)
            summary[label] = f"failed: {type(exc).__name__}: {exc}"
            _log.exception(
                "daily_step_failed", step=label, error=f"{type(exc).__name__}: {exc}"
            )
        else:
            summary[label] = "ok"
            for key, value in _loggable(result).items():
                summary[f"{label}_{key}"] = value
    if failures:
        raise JobStepError(
            f"daily maintenance steps failed: {', '.join(failures)}"
        ) from first
    return summary


async def _job_bryant_daily(
    now: datetime, *, clock: Callable[[], datetime] | None = None
) -> dict[str, Any]:
    """Fetch Carrier daily energy for day2..day1 (PLAN.md §7.2).

    ``stages/daily.py`` has landed, so the normal path calls it. The import is
    still guarded: an image built without the stage should lose this one job, not
    the poll loops, so a genuinely absent module logs one WARN per firing and
    skips. A ``ModuleNotFoundError`` naming anything *else* is a broken
    dependency inside the stage and propagates unchanged.

    **One clock read, used twice.** The Carrier response carries no dates: its
    ``day1``/``day2`` periods are relative to the instant of the *fetch*
    (``stages/daily.py::period_local_date``). So this job cannot date them from
    ``now`` — the scheduler's firing slot — without mislabelling the response,
    and it must not let the stage read the wall clock a *second* time either: if
    the two reads land on different LOCAL dates (a host suspend, an NTP step, a
    very late firing, or simply firing within a second of local midnight), the
    rows are dated from one read and filtered by a window built from the other,
    the freshest day is silently discarded, and the run still reports SUCCESS.

    So exactly one fresh read is taken here, at the moment of the fetch, and it
    is what both the ``--start``/``--end`` window and the stage's own dating are
    derived from — the stage is handed it as ``now=`` rather than being left to
    re-read. A disagreement with the slot's local date is real information about
    the host (it was asleep, or its clock stepped), so it is logged at WARN
    instead of passing silently.

    Args:
        now: the scheduler's firing slot (aware UTC) — used only to detect and
            report clock disagreement, never to date or filter rows.
        clock: the fresh-read clock; defaults to the wall clock. Injected by
            :func:`default_jobs` so the whole host can be driven offline.
    """
    module_name = "energy_capture.stages.daily"
    try:
        # importlib rather than ``from … import daily``: the ``from`` form turns
        # "the submodule does not exist" into a bare ImportError, losing the
        # ``name`` that distinguishes it from a genuinely broken import.
        daily = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise  # a real broken import inside the stage — do not disguise it
        _log.warning(
            "scheduled_job_not_implemented",
            job="bryant_daily_energy",
            module=module_name,
            detail=(
                "Bryant daily energy (PLAN.md §7.2) could not be imported; "
                "skipping this firing"
            ),
        )
        return {"skipped": "not_implemented", "module": module_name}

    entry = getattr(daily, "run", None)
    if not callable(entry):
        _log.warning(
            "scheduled_job_not_implemented",
            job="bryant_daily_energy",
            module=module_name,
            detail=f"{module_name} has no callable run()",
        )
        return {"skipped": "no_entrypoint", "module": module_name}

    # The single fresh read, taken as late as possible — after the import guards,
    # immediately before the fetch is handed to its worker thread.
    fetch_at = timeutil.ensure_utc(clock() if clock is not None else timeutil.now_utc())
    today = timeutil.local_date_of(fetch_at)
    slot_date = timeutil.local_date_of(now)
    if today != slot_date:
        _log.warning(
            "bryant_daily_clock_skew",
            job="bryant_daily_energy",
            scheduled_utc=timeutil.format_utc(timeutil.ensure_utc(now)),
            scheduled_local_date=slot_date.isoformat(),
            fetch_utc=timeutil.format_utc(fetch_at),
            fetch_local_date=today.isoformat(),
            skew_s=round((fetch_at - timeutil.ensure_utc(now)).total_seconds(), 3),
            detail=(
                "the fetch is running on a different LOCAL date than the slot it "
                "was scheduled for (a suspended host, an NTP step, or a firing "
                "across local midnight); day1/day2 are relative to the fetch, so "
                "the window follows the fetch clock"
            ),
        )
    start = today - timedelta(days=BRYANT_DAILY_LOOKBACK_DAYS)
    end = today - timedelta(days=1)
    # ``now=fetch_at`` is what makes the window and the stage's dating one
    # decision instead of two: stages/daily.py dates day1/day2 off this instant
    # and then keeps whichever of them falls inside [start, end].
    return await _call(entry, start=start, end=end, now=fetch_at)


class GreenbuttonAuthorizationRevoked(RuntimeError):
    """LG&E revoked a working authorisation — loud on purpose.

    Distinct from "never authorised", which is a quiet skip. Only a human with a
    browser can fix this, so it has to reach `/healthz` rather than the log floor.
    """


async def _job_greenbutton_daily(now: datetime) -> dict[str, Any]:
    """Fetch the LG&E meter series for the last few local days (PLAN.md §13).

    Skipped, quietly and without an error, when Green Button Connect has not been
    authorised — which is the normal state for anyone who has not clicked through
    the consent flow. An unauthorised deployment must not accumulate a failing job
    every morning; that is noise which teaches an operator to ignore the log.

    The window is deliberately wider than one day. MyMeter publishes recent
    intervals and then revises them, and the month file is rebuilt on the dedupe
    key with the freshest row winning, so re-reading is how a revision lands —
    the same reasoning as the Bryant day1/day2 pair.
    """
    module_name = "energy_capture.stages.greenbutton_fetch"
    try:
        fetch = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        _log.warning(
            "scheduled_job_not_implemented",
            job="greenbutton_daily",
            module=module_name,
        )
        return {"skipped": "not_implemented", "module": module_name}

    settings = get_settings()
    if not settings.lge_client_id or not settings.lge_client_secret.get_secret_value():
        return {"skipped": "not_configured"}

    from energy_capture.sources.lge_auth import LgeTokenCache

    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    if not cache.path.exists():
        # A deployment that never authorised skips QUIETLY -- that is the whole
        # point, and it must stay that way. But an authorisation that was
        # REVOKED is a different event with the same symptom, and treating the
        # two alike is what hid the 2026-08-20 lapse for three days: the job
        # returned "not_authorized" every morning and nobody was meant to care.
        # A revocation now fails loudly, so it lands in job_failed,
        # consecutive_failures and /healthz. DEVIATIONS.md #177.
        revoked = cache.revoked()
        if revoked:
            raise GreenbuttonAuthorizationRevoked(
                "LG&E authorisation was revoked at "
                f"{revoked.get('revoked_at', 'an unknown time')} "
                f"({revoked.get('reason', 'no reason recorded')}). Meter data has "
                "stopped. Re-authorise: `energycap greenbutton-authorize`."
            )
        return {"skipped": "not_authorized"}

    today = timeutil.local_date_of(now)
    start = today - timedelta(days=GREENBUTTON_LOOKBACK_DAYS)
    return await _call(fetch.run, start=start, end=today)


def default_jobs(
    *,
    lookback_days: int = DAILY_LOOKBACK_DAYS,
    spool: SpoolDB | None = None,
    clock: Callable[[], datetime] | None = None,
) -> tuple[ScheduledJob, ...]:
    """The schedule of PLAN.md §5, in firing-time order.

    ``spool`` is the running process's :class:`SpoolDB`. The jobs that touch the
    spool (the uploader, the retention purge) are handed it rather than opening
    their own, so exactly one :class:`SpoolDB` exists per process.

    ``clock`` is the process's clock — the same one the :class:`Scheduler` uses.
    Only ``bryant_daily_energy`` needs it, and only because the Carrier cloud
    dates its response relative to the fetch (see :func:`_job_bryant_daily`);
    every other job takes its window from the firing instant it is handed.
    """

    async def hourly_upload(now: datetime) -> Any:
        return await _job_hourly_upload(now, spool=spool)

    async def daily_maintenance(now: datetime) -> Any:
        return await _job_daily_maintenance(now, lookback_days=lookback_days, spool=spool)

    async def bryant_daily(now: datetime) -> Any:
        return await _job_bryant_daily(now, clock=clock)

    return (
        ScheduledJob(
            name="upload_hourly",
            schedule=HourlyAt(UPLOAD_MINUTE),
            run=hourly_upload,
            description="spool -> part-{YYYYMMDD}T{HH}.parquet for closed hours",
        ),
        ScheduledJob(
            name="rollup_hourly",
            schedule=HourlyAt(ROLLUP_MINUTE),
            run=_job_hourly_rollup,
            description="regenerate rollup-{YYYYMMDD}.parquet for the day(s) of hour HH-1",
        ),
        ScheduledJob(
            name="daily_maintenance",
            schedule=DailyAt(*DAILY_MAINTENANCE_AT),
            run=daily_maintenance,
            description="upload catch-up, compact D-1, re-roll D-1, purge the spool",
        ),
        ScheduledJob(
            name="bryant_daily_energy",
            schedule=DailyAt(*BRYANT_DAILY_AT),
            run=bryant_daily,
            description="Carrier daily energy for day2..day1 -> energy/daily",
        ),
        ScheduledJob(
            name="greenbutton_daily",
            schedule=DailyAt(*GREENBUTTON_DAILY_AT),
            run=_job_greenbutton_daily,
            description="LG&E Green Button meter intervals -> energy/meter",
        ),
    )


# ------------------------------------------------------------------ scheduler


class Scheduler:
    """Fires :class:`ScheduledJob` s at their local times, forever.

    One task, one clock read per iteration, no per-job timers — so there is a
    single place where "what time is it" is decided, and tests can drive a year
    of schedule through it with a fake clock in milliseconds.
    """

    __slots__ = ("_clock", "_failures", "_jobs", "_log", "_next", "_runs", "_sleep", "_status")

    def __init__(
        self,
        jobs: Sequence[ScheduledJob],
        *,
        clock: Callable[[], datetime] = timeutil.now_utc,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        status: Any = None,
    ) -> None:
        self._jobs = list(jobs)
        self._clock = clock
        self._sleep = sleep
        self._status = status
        self._log = get_logger("scheduler")
        self._next: dict[str, datetime] = {}
        self._runs = 0
        self._failures = 0

    # -------------------------------------------------------------- accessors
    @property
    def jobs(self) -> tuple[ScheduledJob, ...]:
        return tuple(self._jobs)

    @property
    def runs(self) -> int:
        return self._runs

    @property
    def failures(self) -> int:
        return self._failures

    @property
    def next_runs(self) -> dict[str, datetime]:
        """Each job's next firing instant (aware UTC)."""
        return dict(self._next)

    # ------------------------------------------------------------- scheduling
    def schedule_all(self, now: datetime | None = None) -> dict[str, datetime]:
        """Compute every job's next firing instant after ``now``."""
        reference = timeutil.ensure_utc(now if now is not None else self._clock())
        self._next = {
            job.name: job.schedule.next_after(reference) for job in self._jobs
        }
        for job in self._jobs:
            when = self._next[job.name]
            self._log.info(
                "job_scheduled",
                job=job.name,
                schedule=job.schedule.describe(),
                next_utc=timeutil.format_utc(when),
                next_local=timeutil.to_local_naive(when).isoformat(),
            )
        return dict(self._next)

    def due(self, now: datetime) -> list[ScheduledJob]:
        """Jobs whose firing instant has arrived, in schedule order."""
        reference = timeutil.ensure_utc(now)
        return [
            job
            for job in self._jobs
            if job.name in self._next and self._next[job.name] <= reference
        ]

    async def fire(self, job: ScheduledJob, now: datetime) -> JobOutcome:
        """Run one job. Never raises: a job's failure is data, not a crash."""
        started = time.monotonic()
        self._log.info(
            "job_start",
            job=job.name,
            scheduled_utc=timeutil.format_utc(now),
            scheduled_local=timeutil.to_local_naive(now).isoformat(),
        )
        try:
            result = job.run(now)
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failures += 1
            duration_s = round(time.monotonic() - started, 3)
            error = f"{type(exc).__name__}: {exc}"
            self._log.exception("job_failed", job=job.name, duration_s=duration_s, error=error)
            self._record_failure(job.name, exc)
            return JobOutcome(name=job.name, ok=False, duration_s=duration_s, error=error)
        self._runs += 1
        duration_s = round(time.monotonic() - started, 3)
        # Merge rather than splat: a stage summary carries its own ``duration_s``
        # (UploadSummary is a Mapping), and splatting it alongside ours raises
        # TypeError *outside* the try above — which would kill the scheduler task
        # and take the process down once an hour. The scheduler's measured wall
        # time wins; the stage's inner timing is kept as ``stage_duration_s``.
        fields = dict(_loggable(result))
        if "duration_s" in fields:
            fields["stage_duration_s"] = fields.pop("duration_s")
        try:
            self._log.info("job_ok", **{**fields, "job": job.name, "duration_s": duration_s})
        except Exception as log_exc:  # pragma: no cover - defensive
            # "Never raises" is the contract; no future stage return value gets
            # to kill the scheduler over a log line.
            self._log.info("job_ok", job=job.name, duration_s=duration_s)
            self._log.warning(
                "job_ok_fields_unloggable",
                job=job.name,
                error=f"{type(log_exc).__name__}: {log_exc}",
            )
        return JobOutcome(name=job.name, ok=True, duration_s=duration_s, result=result)

    def _record_failure(self, job: str, exc: BaseException) -> None:
        try:
            store = self._status
            if store is None:
                from energy_capture.health import get_status_store

                store = self._status = get_status_store()
            store.record_failure("scheduler", exc, job=job)
        except Exception as status_exc:  # pragma: no cover - defensive
            self._log.warning(
                "status_update_failed", error=f"{type(status_exc).__name__}: {status_exc}"
            )

    # ----------------------------------------------------------------- the loop
    async def run_forever(self, stop: asyncio.Event) -> None:
        """Fire jobs until ``stop`` is set."""
        self.schedule_all()
        while not stop.is_set():
            now = timeutil.ensure_utc(self._clock())
            due = self.due(now)
            if due:
                for job in due:
                    if stop.is_set():
                        break
                    await self.fire(job, self._next.get(job.name, now))
                    # Re-read the clock: a long job may have crossed its own next
                    # slot, and firing it again immediately would be a stampede.
                    self._next[job.name] = job.schedule.next_after(
                        timeutil.ensure_utc(self._clock())
                    )
                continue
            if self._next:
                delay = (min(self._next.values()) - now).total_seconds()
            else:  # no jobs at all: idle until asked to stop
                delay = MAX_SCHEDULER_SLEEP_S
            delay = min(MAX_SCHEDULER_SLEEP_S, max(0.0, delay))
            if await wait_or_stop(stop, delay, self._sleep):
                break
        self._log.info("scheduler_stopped", runs=self._runs, failures=self._failures)


# -------------------------------------------------------------------- runtime


class Runtime:
    """The process host: poll loops + background tasks + scheduler + health.

    Everything is injectable so the whole host can be exercised offline with a
    fake clock, fake sources and no sockets::

        runtime = Runtime(
            sources=[FakeSource()], spool=spool, status=store,
            jobs=(), health_enabled=False, install_signal_handlers=False,
            clock=fake_clock, sleep=fake_sleep,
        )
        await runtime.serve()
    """

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        spool: SpoolDB | None = None,
        status: Any = None,
        sources: Sequence[Source] | None = None,
        source_names: Sequence[str] | None = None,
        jobs: Sequence[ScheduledJob] | None = None,
        health_enabled: bool = True,
        health_port: int | None = None,
        install_signal_handlers: bool = True,
        background: bool = True,
        clock: Callable[[], datetime] = timeutil.now_utc,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        shutdown_timeout_s: float = SHUTDOWN_TIMEOUT_S,
    ) -> None:
        self._settings = settings
        self._spool = spool
        self._owns_spool = spool is None
        self._status = status
        self._sources = list(sources) if sources is not None else None
        self._source_names = list(source_names) if source_names is not None else None
        self._jobs = list(jobs) if jobs is not None else None
        self._health_enabled = health_enabled
        self._health_port = health_port
        self._install_signals = install_signal_handlers
        self._background = background
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic
        self._shutdown_timeout_s = float(shutdown_timeout_s)

        self._log = _log
        self._stop = asyncio.Event()
        self._reason: str = "completed"
        self._poller: Poller | None = None
        self._scheduler: Scheduler | None = None
        self._health: Any = None
        self._installed_signals: list[signal.Signals] = []

    # -------------------------------------------------------------- accessors
    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop

    @property
    def poller(self) -> Poller | None:
        return self._poller

    @property
    def scheduler(self) -> Scheduler | None:
        return self._scheduler

    def request_stop(self, reason: str = "requested") -> None:
        """Ask the process to shut down gracefully. Safe from a signal handler."""
        if self._stop.is_set():
            self._log.warning("shutdown_already_requested", reason=reason)
            return
        self._reason = reason
        self._log.info("shutdown_requested", reason=reason)
        self._stop.set()

    # --------------------------------------------------------------- the host
    async def serve(self) -> dict[str, Any]:
        """Start everything and block until stopped. Returns a run summary."""
        settings = self._settings if self._settings is not None else get_settings()
        started_at = timeutil.ensure_utc(self._clock())
        started_monotonic = self._monotonic()

        # Sources first: a missing credential or an unknown --source is a startup
        # error, and there is no reason to have created a spool connection by the
        # time it surfaces.
        sources = (
            self._sources
            if self._sources is not None
            else build_sources(self._source_names, settings=settings)
        )
        spool = self._spool if self._spool is not None else open_spool()
        self._spool = spool
        poller = Poller(
            sources,
            spool,
            status=self._status,
            monotonic=self._monotonic,
            sleep=self._sleep,
        )
        self._poller = poller
        # The spool is already open here, so the scheduled jobs that touch it
        # (uploader catch-up, retention purge) share this process's one
        # connection set instead of opening their own every hour. The clock goes
        # with it: the Bryant daily job's fresh read must come from the same
        # clock the scheduler fires on, not from a second source of time.
        jobs = (
            self._jobs
            if self._jobs is not None
            else list(default_jobs(spool=spool, clock=self._clock))
        )
        scheduler = Scheduler(
            jobs, clock=self._clock, sleep=self._sleep, status=self._status
        )
        self._scheduler = scheduler

        self._log.info(
            "runtime_starting",
            sources=[source.name for source in sources],
            jobs=[job.name for job in jobs],
            spool=str(spool.path),
            started_utc=timeutil.format_utc(started_at),
            # Which mechanism keeps Leviton values fresh is the one thing about
            # this process that changed on 2026-08-16, and it is invisible in the
            # row stream (the schema has no provenance column). Say it out loud
            # once per boot so a log tail is enough to know how the container is
            # collecting; the per-cycle detail lives in status.json's
            # ``leviton_ingest`` section.
            leviton_ingest=settings.leviton_ingest,
        )

        tasks: list[asyncio.Task[Any]] = []
        try:
            await poller.start()
            await self._start_health(settings)
            self._install_signal_handlers()

            tasks.append(
                asyncio.create_task(poller.run_forever(self._stop), name="poll-loops")
            )
            if self._background:
                tasks.extend(self._background_tasks(poller))
            if jobs:
                tasks.append(
                    asyncio.create_task(
                        scheduler.run_forever(self._stop), name="scheduler"
                    )
                )
            self._log.info("runtime_started", tasks=[task.get_name() for task in tasks])
            await self._supervise(tasks)
        finally:
            self._stop.set()
            await self._shutdown(tasks, poller)

        summary = {
            "reason": self._reason,
            "uptime_s": round(self._monotonic() - started_monotonic, 3),
            "sources": list(poller.names),
            "cycles": sum(p.cycles for p in poller.pollers),
            "jobs_run": scheduler.runs,
            "jobs_failed": scheduler.failures,
        }
        self._log.info("runtime_stopped", **summary)
        return summary

    def _background_tasks(self, poller: Poller) -> list[asyncio.Task[Any]]:
        tasks: list[asyncio.Task[Any]] = []
        for task in poller.background_tasks():
            tasks.append(
                asyncio.create_task(
                    run_background_task(task, self._stop, sleep=self._sleep),
                    name=f"bg-{task.name}",
                )
            )
            self._log.info(
                "background_task_scheduled",
                task=task.name,
                interval_s=task.interval_s,
                initial_delay_s=task.initial_delay_s,
            )
        return tasks

    async def _supervise(self, tasks: Sequence[asyncio.Task[Any]]) -> None:
        """Wait for a stop request, or for a task to die unexpectedly."""
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            done, _ = await asyncio.wait(
                {stopper, *tasks}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            if not stopper.done():
                stopper.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stopper
        if self._stop.is_set():
            return
        # A task finished on its own: either it crashed (re-raise, so the
        # container restarts) or it returned early (also unexpected).
        for task in done:
            if task is stopper or not isinstance(task, asyncio.Task):
                continue
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                self._reason = f"task_failed:{task.get_name()}"
                raise exc
            self._reason = f"task_exited:{task.get_name()}"
            self._log.error("runtime_task_exited", task=task.get_name())

    async def _shutdown(
        self, tasks: Sequence[asyncio.Task[Any]], poller: Poller
    ) -> None:
        """Stop cleanly: drain tasks, close sources, flush and close the spool."""
        pending = [task for task in tasks if not task.done()]
        if pending:
            self._log.info(
                "shutdown_draining",
                tasks=[task.get_name() for task in pending],
                timeout_s=self._shutdown_timeout_s,
            )
            done, still_pending = await asyncio.wait(
                pending, timeout=self._shutdown_timeout_s
            )
            for task in still_pending:
                self._log.warning("shutdown_cancelling_task", task=task.get_name())
                task.cancel()
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)
        for task in tasks:
            if task.cancelled():
                continue
            exc = task.exception() if task.done() else None
            if exc is not None and not isinstance(exc, asyncio.CancelledError):
                self._log.error(
                    "shutdown_task_error",
                    task=task.get_name(),
                    error=f"{type(exc).__name__}: {exc}",
                )

        with contextlib.suppress(Exception):
            await poller.close()
        await self._stop_health()
        self._remove_signal_handlers()
        self._flush_spool()

    def _flush_spool(self) -> None:
        """Record the final spool gauge, then close the database.

        The spool commits once per poll cycle, so nothing is buffered in memory
        waiting to be written; "flush" here means (a) make the final pending-row
        count visible in ``status.json`` and (b) close the connections so the WAL
        is checkpointed before the container dies.
        """
        spool = self._spool
        if spool is None:
            return
        try:
            stats = spool.stats()
            self._log.info(
                "spool_final",
                pending_rows=stats.pending_rows,
                uploaded_rows=stats.uploaded_rows,
                total_rows=stats.total_rows,
            )
            store = self._status
            if store is None:
                from energy_capture.health import get_status_store

                store = get_status_store()
            store.set("spool", **stats.to_status_dict())
        except Exception as exc:
            self._log.warning("spool_final_failed", error=f"{type(exc).__name__}: {exc}")
        if self._owns_spool:
            with contextlib.suppress(Exception):
                spool.close()

    # ---------------------------------------------------------------- health
    async def _start_health(self, settings: Settings) -> None:
        if not self._health_enabled:
            return
        from energy_capture.health import HealthServer, get_status_store

        store = self._status if self._status is not None else get_status_store()
        port = self._health_port if self._health_port is not None else settings.health_port
        server = HealthServer(store, port=port)
        try:
            await server.start()
        except OSError as exc:
            # A busy port must not stop data collection; health is telemetry.
            self._log.error(
                "health_server_failed", port=port, error=f"{type(exc).__name__}: {exc}"
            )
            return
        self._health = server

    async def _stop_health(self) -> None:
        server, self._health = self._health, None
        if server is None:
            return
        with contextlib.suppress(Exception):
            await server.aclose()

    # --------------------------------------------------------------- signals
    def _install_signal_handlers(self) -> None:
        if not self._install_signals:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - serve() always has a loop
            return
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, RuntimeError, ValueError, AttributeError):
                # Windows, or a non-main thread: fall back to default handling
                # (SIGINT still raises KeyboardInterrupt, which serve()'s caller
                # turns into exit 130).
                self._log.warning("signal_handler_unavailable", signal=sig.name)
                continue
            self._installed_signals.append(sig)
        if self._installed_signals:
            self._log.info(
                "signal_handlers_installed",
                signals=[sig.name for sig in self._installed_signals],
            )

    @property
    def installed_signals(self) -> tuple[str, ...]:
        """Signals whose handler is currently installed (tests, diagnostics)."""
        return tuple(sig.name for sig in self._installed_signals)

    def _on_signal(self, sig: signal.Signals) -> None:
        self.request_stop(f"signal:{sig.name}")

    def _remove_signal_handlers(self) -> None:
        if not self._installed_signals:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover
            self._installed_signals.clear()
            return
        for sig in self._installed_signals:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)
        self._installed_signals.clear()


# --------------------------------------------------------------- CLI entry point


async def run(**kwargs: Any) -> dict[str, Any]:
    """``energycap run`` (``cli.STAGE_ENTRYPOINTS["run"]``) — blocks until stopped.

    Keyword arguments are the :class:`Runtime` constructor's; the CLI passes
    none. Returns a summary mapping, which the CLI folds into its ``stage_ok``
    line — a clean SIGTERM is a successful run, not a failure.
    """
    return await Runtime(**kwargs).serve()
