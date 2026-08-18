"""The long-running process: poll loops, scheduler, shutdown (PLAN.md §5, §10).

Everything here runs offline, in milliseconds, with no real sleeping: the poll
loops get an instant ``sleep`` and the scheduler gets a fake clock that only
moves when something asks to wait. Nothing opens a socket and nothing touches a
cloud.

What these tests pin down, in the order the task list puts them:

* a source that raises does not kill its own loop or its sibling's;
* one poll cycle is exactly **one** spool transaction, and a failed cycle is
  **zero** transactions and zero rows (CLAUDE.md rule 1);
* the scheduler's job times are correct in LOCAL time on an ordinary day *and*
  on both DST transition days — 25 hourly firings on the fall-back day, 23 on
  spring-forward, and the 01:30 daily job firing exactly once on the day when
  01:30 happens twice;
* ``SIGTERM`` shuts the process down cleanly: the last cycle's rows are on disk,
  sources are closed, and the run is reported as a success, not a failure;
* a scheduled job that throws is contained — the scheduler and its other jobs
  keep going;
* the Bryant daily energy stage, which does not exist yet, produces one WARN per
  firing instead of taking the process down.
"""

from __future__ import annotations

import asyncio
import importlib
import io
import json
import signal
import sys
import types
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from energy_capture import runtime, timeutil
from energy_capture.health import StatusStore
from energy_capture.logging import configure_logging
from energy_capture.model import Observation, make_observation
from energy_capture.runtime import (
    DailyAt,
    HourlyAt,
    Runtime,
    ScheduledJob,
    Scheduler,
    default_jobs,
)
from energy_capture.sources.base import (
    BackgroundTask,
    Discovery,
    SourceAuthError,
    SourceTransientError,
)
from energy_capture.spool.sqlite import SpoolDB
from energy_capture.stages import poller as poller_stage
from energy_capture.stages.poller import Poller, SourcePoller, build_sources
from tests.conftest import utc

# 2026 US DST: spring forward Sunday 2026-03-08, fall back Sunday 2026-11-01.
SPRING_FORWARD = date(2026, 3, 8)
FALL_BACK = date(2026, 11, 1)
ORDINARY_DAY = date(2026, 8, 16)


# ---------------------------------------------------------------- fake clocks


class FakeClock:
    """A UTC clock that only advances when someone sleeps.

    Used for the scheduler, which is the one component whose correctness is a
    function of wall-clock time. Only one sleeper may share a clock, or the two
    would each advance it and the test would stop being deterministic.
    """

    def __init__(self, start: datetime) -> None:
        self.utc = timeutil.ensure_utc(start)
        self.slept: list[float] = []

    def now(self) -> datetime:
        return self.utc

    async def sleep(self, delay: float) -> None:
        self.slept.append(delay)
        self.utc += timedelta(seconds=max(delay, 0.0))
        await asyncio.sleep(0)


async def instant_sleep(delay: float) -> None:
    """A ``sleep`` that only yields — poll loops run at full speed under test."""
    await asyncio.sleep(0)


# --------------------------------------------------------------- fake sources


class FakeSource:
    """A :class:`~energy_capture.sources.base.Source` with no cloud behind it."""

    def __init__(
        self,
        name: str = "leviton",
        *,
        poll_interval_s: int = 30,
        channels: Sequence[str] = ("breaker_p11", "breaker_p13"),
        error: BaseException | None = None,
        stop_after: int | None = None,
        stop_event: asyncio.Event | None = None,
        on_poll: Callable[[FakeSource], None] | None = None,
        background: Sequence[BackgroundTask] = (),
        start_error: BaseException | None = None,
        base_ts: datetime = utc(2026, 8, 16, 18, 0, 0),
    ) -> None:
        self.name = name
        self.poll_interval_s = poll_interval_s
        self.channels = tuple(channels)
        self.error = error
        self.stop_after = stop_after
        self.stop_event = stop_event
        self.on_poll = on_poll
        self.start_error = start_error
        self.base_ts = base_ts
        self._background = tuple(background)
        self.polls = 0
        self.started = 0
        self.closed = 0

    async def start(self) -> None:
        self.started += 1
        if self.start_error is not None:
            raise self.start_error

    async def discover(self, *, force: bool = False) -> Discovery:
        return Discovery(source=self.name)

    def background_tasks(self) -> Sequence[BackgroundTask]:
        return self._background

    async def close(self) -> None:
        self.closed += 1

    async def poll(self) -> list[Observation]:
        self.polls += 1
        await asyncio.sleep(0)
        if self.on_poll is not None:
            self.on_poll(self)
        if (
            self.stop_after is not None
            and self.stop_event is not None
            and self.polls >= self.stop_after
        ):
            self.stop_event.set()
        if self.error is not None:
            raise self.error
        # A distinct instant per cycle, exactly as a real source stamps them:
        # one ts_utc for the whole cycle (PLAN.md §6.5).
        ts = self.base_ts + timedelta(seconds=self.poll_interval_s * self.polls)
        return [
            make_observation(
                ts_utc=ts,
                source=self.name,
                device_id="hub-a",
                channel_id=channel,
                metric="watts",
                value=100.0 + index,
            )
            for index, channel in enumerate(self.channels)
        ]


class CountingSpool(SpoolDB):
    """A spool that counts write transactions (``BEGIN IMMEDIATE`` blocks)."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.transactions = 0

    @contextmanager
    def _write(self):  # type: ignore[override]
        self.transactions += 1
        with SpoolDB._write(self) as conn:
            yield conn


# -------------------------------------------------------------------- fixtures


@pytest.fixture
def spool(tmp_path: Path) -> CountingSpool:
    db = CountingSpool(tmp_path / "spool.db")
    db.connect()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def status(tmp_path: Path) -> StatusStore:
    return StatusStore(
        tmp_path / "status.json",
        poll_intervals={"leviton": 30, "bryant_status": 30},
        load_existing=False,
    )


@pytest.fixture
def log_stream() -> io.StringIO:
    """Capture the structured JSON log lines this module's code emits."""
    buffer = io.StringIO()
    configure_logging("DEBUG", stream=buffer, force=True)
    yield buffer
    configure_logging("INFO", stream=io.StringIO(), force=True)


def log_events(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in stream.getvalue().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("event") == event:
            out.append(record)
    return out


# ======================================================================
# Schedules — local time, and DST
# ======================================================================


def test_hourly_schedule_fires_at_the_configured_minute_past_each_local_hour() -> None:
    schedule = HourlyAt(5)
    fire = schedule.next_after(utc(2026, 8, 16, 18, 2, 30))
    assert timeutil.to_local_naive(fire) == datetime(2026, 8, 16, 14, 5)
    # Exactly on the boundary counts as past: strictly-after, so no double fire.
    assert schedule.next_after(fire) == fire + timedelta(hours=1)


def test_hourly_schedule_is_strictly_in_the_future() -> None:
    schedule = HourlyAt(20)
    now = utc(2026, 8, 16, 18, 20, 0)
    assert schedule.next_after(now) == now + timedelta(hours=1)


@pytest.mark.parametrize("minute", [-1, 60, 100])
def test_hourly_schedule_rejects_an_impossible_minute(minute: int) -> None:
    with pytest.raises(ValueError):
        HourlyAt(minute)


@pytest.mark.parametrize(
    ("hour", "minute"), [(24, 0), (-1, 0), (1, 60), (1, -5)]
)
def test_daily_schedule_rejects_an_impossible_time(hour: int, minute: int) -> None:
    with pytest.raises(ValueError):
        DailyAt(hour, minute)


def _firings(schedule: Any, local_day: date) -> list[datetime]:
    """Every firing of ``schedule`` inside ``local_day``'s real UTC span."""
    start, end = timeutil.local_day_bounds_utc(local_day)
    out: list[datetime] = []
    cursor = start - timedelta(microseconds=1)
    while True:
        cursor = schedule.next_after(cursor)
        if cursor >= end:
            return out
        out.append(cursor)


def test_hourly_jobs_fire_once_per_physical_hour_on_an_ordinary_day() -> None:
    fires = _firings(HourlyAt(5), ORDINARY_DAY)
    assert len(fires) == 24
    assert {timeutil.to_local_naive(f).minute for f in fires} == {5}


def test_hourly_jobs_fire_25_times_on_the_fall_back_day() -> None:
    """Both 01:00 hours hold real data, so both need their upload and rollup."""
    fires = _firings(HourlyAt(5), FALL_BACK)
    assert len(fires) == timeutil.local_hours_in_day(FALL_BACK) == 25
    locals_ = [timeutil.to_local_naive(f) for f in fires]
    # 01:05 appears twice — two distinct instants, one ambiguous wall clock.
    assert locals_.count(datetime(2026, 11, 1, 1, 5)) == 2
    assert len(set(fires)) == 25


def test_hourly_jobs_fire_23_times_on_the_spring_forward_day() -> None:
    fires = _firings(HourlyAt(5), SPRING_FORWARD)
    assert len(fires) == timeutil.local_hours_in_day(SPRING_FORWARD) == 23
    # The 02:00 wall-clock hour does not exist; nothing may be scheduled in it.
    assert all(timeutil.to_local_naive(f).hour != 2 for f in fires)


def test_daily_job_keeps_its_local_time_across_spring_forward() -> None:
    """08:30 local stays 08:30 local; it is the UTC instant that moves."""
    schedule = DailyAt(8, 30)

    before = schedule.next_after(utc(2026, 3, 7, 0, 0))  # EST day
    after = schedule.next_after(utc(2026, 3, 8, 0, 0))  # EDT day

    assert timeutil.to_local_naive(before) == datetime(2026, 3, 7, 8, 30)
    assert timeutil.to_local_naive(after) == datetime(2026, 3, 8, 8, 30)
    assert before == utc(2026, 3, 7, 13, 30)  # UTC-5
    assert after == utc(2026, 3, 8, 12, 30)  # UTC-4
    # Naive arithmetic would have put them 24h apart; the real gap is 23h.
    assert after - before == timedelta(hours=23)


def test_daily_maintenance_fires_exactly_once_on_the_spring_forward_day() -> None:
    fires = _firings(DailyAt(*runtime.DAILY_MAINTENANCE_AT), SPRING_FORWARD)
    assert len(fires) == 1
    assert timeutil.to_local_naive(fires[0]) == datetime(2026, 3, 8, 1, 30)


def test_daily_maintenance_fires_exactly_once_on_the_fall_back_day() -> None:
    """01:30 happens twice on 2026-11-01; compacting D-1 twice is not wanted."""
    fires = _firings(DailyAt(*runtime.DAILY_MAINTENANCE_AT), FALL_BACK)
    assert len(fires) == 1
    assert timeutil.to_local_naive(fires[0]) == datetime(2026, 11, 1, 1, 30)
    # The FIRST occurrence (EDT, UTC-4), not the second (EST, UTC-5).
    assert fires[0] == utc(2026, 11, 1, 5, 30)


def test_bryant_daily_job_fires_once_on_both_transition_days() -> None:
    schedule = DailyAt(*runtime.BRYANT_DAILY_AT)
    for day in (SPRING_FORWARD, FALL_BACK, ORDINARY_DAY):
        fires = _firings(schedule, day)
        assert len(fires) == 1, day
        assert timeutil.to_local_naive(fires[0]).hour == 8
        assert timeutil.to_local_naive(fires[0]).minute == 30


def test_a_year_of_daily_firings_is_one_per_local_day() -> None:
    """The strongest DST statement available: no day is skipped or doubled."""
    schedule = DailyAt(1, 30)
    start = timeutil.local_midnight_utc(date(2026, 1, 1))
    end = timeutil.local_midnight_utc(date(2027, 1, 1))
    fires: list[datetime] = []
    cursor = start - timedelta(microseconds=1)
    while True:
        cursor = schedule.next_after(cursor)
        if cursor >= end:
            break
        fires.append(cursor)
    assert len(fires) == 365
    assert len({timeutil.local_date_of(f) for f in fires}) == 365
    assert {timeutil.to_local_naive(f).time() for f in fires} == {
        datetime(2026, 1, 1, 1, 30).time()
    }


def test_default_jobs_are_exactly_the_schedule_in_plan_section_5() -> None:
    jobs = {job.name: job for job in default_jobs()}
    assert set(jobs) == {
        "upload_hourly",
        "rollup_hourly",
        "daily_maintenance",
        "bryant_daily_energy",
        "greenbutton_daily",
    }
    assert jobs["upload_hourly"].schedule == HourlyAt(runtime.UPLOAD_MINUTE) == HourlyAt(5)
    assert jobs["rollup_hourly"].schedule == HourlyAt(runtime.ROLLUP_MINUTE) == HourlyAt(20)
    assert jobs["daily_maintenance"].schedule == DailyAt(1, 30)
    assert jobs["bryant_daily_energy"].schedule == DailyAt(8, 30)
    # LG&E publishes overnight and lags; 09:15 is after that and clear of 08:30.
    assert jobs["greenbutton_daily"].schedule == DailyAt(9, 15)
    # dim_channel is rebuilt on demand only (PLAN.md §5) — never scheduled.
    assert not any("dim" in name for name in jobs)


# ======================================================================
# The poll loop
# ======================================================================


async def test_one_poll_cycle_is_exactly_one_spool_transaction(
    spool: CountingSpool, status: StatusStore
) -> None:
    source = FakeSource(channels=("a", "b", "c", "d"))
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)

    before = spool.transactions
    result = await poller.cycle()

    assert result.ok and result.rows == 4 and result.inserted == 4
    assert spool.transactions - before == 1
    assert len(spool.read_local_hour(date(2026, 8, 16), 14)) == 4


async def test_a_failed_cycle_writes_zero_rows_and_opens_no_transaction(
    spool: CountingSpool, status: StatusStore
) -> None:
    source = FakeSource(error=SourceTransientError("502 Bad Gateway"))
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)

    before = spool.transactions
    result = await poller.cycle()

    assert not result.ok
    assert result.rows == 0
    assert result.failure == "transient"
    assert spool.transactions == before
    assert spool.stats().total_rows == 0
    section = status.section("leviton")
    assert section["consecutive_failures"] == 1
    assert section["last_success_utc"] is None  # a failure never looks like a success


async def test_consecutive_failures_accumulate_then_reset_on_success(
    spool: CountingSpool, status: StatusStore
) -> None:
    source = FakeSource(error=SourceAuthError("401"))
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)
    for _ in range(3):
        await poller.cycle()
    assert poller.consecutive_failures == 3
    assert status.section("leviton")["consecutive_failures"] == 3

    source.error = None
    result = await poller.cycle()
    assert result.ok
    assert poller.consecutive_failures == 0
    assert status.section("leviton")["consecutive_failures"] == 0
    assert status.section("leviton")["last_success_utc"] is not None


async def test_a_source_raising_does_not_kill_the_loop_or_its_sibling(
    spool: CountingSpool, status: StatusStore
) -> None:
    stop = asyncio.Event()
    broken = FakeSource("bryant", error=SourceTransientError("504"))
    healthy = FakeSource(
        "leviton", stop_after=4, stop_event=stop, channels=("breaker_p11",)
    )
    poller = Poller([broken, healthy], spool, status=status, sleep=instant_sleep)
    await poller.start()

    await asyncio.wait_for(poller.run_forever(stop), timeout=5)

    # The broken source kept being polled (its loop survived every failure)…
    assert broken.polls >= 4
    # …and the healthy one produced a full row set for every one of its cycles.
    assert healthy.polls >= 4
    assert spool.stats().total_rows == healthy.polls
    assert status.section("bryant_status")["consecutive_failures"] == broken.polls
    assert status.section("leviton")["last_success_utc"] is not None


async def test_an_unexpected_source_bug_is_contained_like_any_other_failure(
    spool: CountingSpool, status: StatusStore
) -> None:
    """A ``ValueError`` from a source is a bug, but it still may not stop polling."""
    stop = asyncio.Event()
    source = FakeSource(error=ValueError("bug in the mapper"), stop_after=3, stop_event=stop)
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)

    await asyncio.wait_for(poller.run_forever(stop), timeout=5)

    assert source.polls >= 3
    assert poller.consecutive_failures == source.polls
    assert spool.stats().total_rows == 0


async def test_a_spool_failure_is_recorded_and_does_not_stop_the_loop(
    tmp_path: Path, status: StatusStore
) -> None:
    class BrokenSpool(CountingSpool):
        def append(self, observations):  # type: ignore[override]
            raise OSError("disk is gone")

    broken = BrokenSpool(tmp_path / "spool.db")
    try:
        stop = asyncio.Event()
        source = FakeSource(stop_after=2, stop_event=stop)
        poller = SourcePoller(source, broken, status=status, sleep=instant_sleep)
        await asyncio.wait_for(poller.run_forever(stop), timeout=5)
    finally:
        broken.close()

    assert source.polls >= 2
    assert poller.consecutive_failures == source.polls
    assert status.section("leviton")["consecutive_failures"] == source.polls


async def test_a_cycle_with_no_readable_fields_is_a_success_with_zero_rows(
    spool: CountingSpool, status: StatusStore
) -> None:
    """Every field null is a gap, not a failure — and never a fabricated zero."""
    source = FakeSource(channels=())
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)

    result = await poller.cycle()

    assert result.ok and result.rows == 0
    assert spool.stats().total_rows == 0
    section = status.section("leviton")
    assert section["last_success_utc"] is not None
    # channels_seen is left alone rather than being overwritten with 0.
    assert section["channels_seen"] == 0


async def test_channels_seen_is_not_erased_by_an_empty_cycle(
    spool: CountingSpool, status: StatusStore
) -> None:
    source = FakeSource(channels=("a", "b", "c"))
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)
    await poller.cycle()
    assert status.section("leviton")["channels_seen"] == 3

    source.channels = ()
    await poller.cycle()
    assert status.section("leviton")["channels_seen"] == 3


async def test_duplicate_rows_are_ignored_by_the_spool_not_double_counted(
    spool: CountingSpool, status: StatusStore
) -> None:
    """``energycap poll --once`` twice over the same instant is idempotent."""
    source = FakeSource()
    poller = SourcePoller(source, spool, status=status, sleep=instant_sleep)
    first = await poller.cycle()
    source.polls = 0  # replay the same instant
    second = await poller.cycle()

    assert first.inserted == 2
    assert second.rows == 2 and second.inserted == 0 and second.duplicates == 2
    assert spool.stats().total_rows == 2


async def test_the_poll_interval_floor_is_applied_and_logged_once_at_startup(
    spool: CountingSpool,
    status: StatusStore,
    log_stream: io.StringIO,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DEVIATIONS.md #11: Settings clamps silently; the process must say so."""
    monkeypatch.setenv("POLL_INTERVAL_S", "5")
    source = FakeSource(poll_interval_s=5)
    poller = Poller([source], spool, status=status, sleep=instant_sleep)

    await poller.start()

    assert poller.pollers[0].interval_s == 30
    floored = log_events(log_stream, "poll_interval_floored")
    assert len(floored) == 1
    assert floored[0]["configured_s"] == 5
    assert floored[0]["poll_interval_s"] == 30
    assert floored[0]["floor_s"] == 30
    assert floored[0]["level"] == "WARNING"


async def test_an_honoured_interval_is_logged_without_a_warning(
    spool: CountingSpool, status: StatusStore, log_stream: io.StringIO
) -> None:
    poller = Poller([FakeSource(poll_interval_s=30)], spool, status=status)
    await poller.start()
    assert len(log_events(log_stream, "poll_interval_effective")) == 1
    assert not log_events(log_stream, "poll_interval_floored")


async def test_health_stops_judging_sources_that_are_not_running(
    spool: CountingSpool, status: StatusStore
) -> None:
    """Otherwise /healthz is 503 forever for a poller nobody started (PLAN.md §11)."""
    assert "bryant_status" in status.poll_intervals

    poller = Poller([FakeSource("leviton")], spool, status=status)
    await poller.start()

    assert status.poll_intervals == {"leviton": 30}
    code, _ = status.health_report()
    assert code == 200


async def test_a_source_that_fails_to_start_is_kept_and_retried(
    spool: CountingSpool, status: StatusStore
) -> None:
    """A cloud having a bad minute at boot must not stop the container."""
    source = FakeSource(start_error=SourceAuthError("cloud is down"))
    poller = Poller([source], spool, status=status, sleep=instant_sleep)

    await poller.start()

    assert source.started == 1
    assert status.section("leviton")["consecutive_failures"] == 1
    result = await poller.pollers[0].cycle()
    assert result.ok  # poll() heals on its own


async def test_sources_are_closed_exactly_once(
    spool: CountingSpool, status: StatusStore
) -> None:
    source = FakeSource()
    poller = Poller([source], spool, status=status)
    await poller.start()
    await poller.close()
    await poller.close()
    assert source.closed == 1


async def test_poll_once_runs_one_cycle_per_source(
    spool: CountingSpool, status: StatusStore
) -> None:
    leviton = FakeSource("leviton")
    bryant = FakeSource("bryant", channels=("zone_1",))

    summary = await poller_stage.run(
        once=True,
        source_objects=[leviton, bryant],
        spool=spool,
        status=status,
        sleep=instant_sleep,
    )

    assert leviton.polls == 1 and bryant.polls == 1
    assert summary["once"] is True
    assert summary["rows"] == 3
    assert summary["failed"] == 0
    assert leviton.closed == 1 and bryant.closed == 1


async def test_the_status_section_of_the_bryant_status_poller_is_not_bryant_daily(
    spool: CountingSpool, status: StatusStore
) -> None:
    poller = SourcePoller(FakeSource("bryant"), spool, status=status)
    assert poller.status_section == "bryant_status"
    await poller.cycle()
    assert status.section("bryant_status")["last_success_utc"] is not None
    assert status.section("bryant_daily")["last_success_utc"] is None


# ---------------------------------------------------------------- background


async def test_background_tasks_are_run_and_their_failures_absorbed() -> None:
    stop = asyncio.Event()
    calls: list[int] = []

    async def flaky() -> None:
        calls.append(len(calls))
        if len(calls) == 2:
            raise RuntimeError("keepalive PUT failed")
        if len(calls) >= 4:
            stop.set()

    task = BackgroundTask(name="leviton_keepalive", interval_s=50.0, run=flaky)
    runs = await asyncio.wait_for(
        poller_stage.run_background_task(task, stop, sleep=instant_sleep), timeout=5
    )

    assert runs >= 4  # the exception did not end the task
    assert len(calls) >= 4


async def test_a_background_tasks_initial_delay_is_honoured() -> None:
    stop = asyncio.Event()
    stop.set()

    async def never() -> None:  # pragma: no cover - must not run
        raise AssertionError("ran despite a stop before the initial delay")

    runs = await poller_stage.run_background_task(
        BackgroundTask("t", 50.0, never, initial_delay_s=10.0), stop, sleep=instant_sleep
    )
    assert runs == 0


# ---------------------------------------------------------------- registry


def test_build_sources_builds_every_registered_source() -> None:
    """Both PLAN.md sources have landed: ``energycap run`` polls Leviton (§6) and
    Bryant status (§7.3), in that registration order, with no ``--source``.

    Construction alone must not need credentials or a network — both sources
    resolve configuration at poll time, so a container with a bad ``.env`` boots
    degraded and heals rather than refusing to start.
    """
    assert [source.name for source in build_sources()] == ["leviton", "bryant"]
    assert [source.name for source in build_sources(["bryant"])] == ["bryant"]


def _missing_module_factory(name: str):
    """A factory that fails exactly the way an unlanded ``sources/{name}.py`` does."""

    def factory(settings):  # noqa: ANN001 - matches SOURCE_FACTORIES' signature
        raise ModuleNotFoundError(
            f"No module named 'energy_capture.sources.{name}'",
            name=f"energy_capture.sources.{name}",
        )

    return factory


def test_build_sources_skips_a_source_whose_module_has_not_landed(
    log_stream: io.StringIO,
) -> None:
    """The degraded-boot path, now that no real source exercises it.

    Both PLAN.md §6/§7.3 sources exist, so this drives ``build_sources`` with a
    registry whose second factory raises the same ``ModuleNotFoundError`` an
    absent module raises. The branch stays covered for whatever lands next.
    """
    registry = {
        "leviton": poller_stage.SOURCE_FACTORIES["leviton"],
        "solarish": _missing_module_factory("solarish"),
    }
    sources = build_sources(factories=registry)
    assert [source.name for source in sources] == ["leviton"]
    warned = log_events(log_stream, "source_not_implemented")
    assert warned and warned[0]["source"] == "solarish"


def test_an_explicitly_requested_missing_source_is_an_error() -> None:
    registry = {"solarish": _missing_module_factory("solarish")}
    with pytest.raises(poller_stage.SourceUnavailable):
        build_sources(["solarish"], factories=registry)


def test_a_broken_import_inside_a_source_is_not_disguised_as_not_implemented() -> None:
    """A source module that exists but imports a missing third-party package is a
    deployment bug and must surface, not be swallowed as "not built yet"."""

    def factory(settings):  # noqa: ANN001
        raise ModuleNotFoundError("No module named 'aioleviton'", name="aioleviton")

    with pytest.raises(ModuleNotFoundError, match="aioleviton"):
        build_sources(factories={"leviton": factory})


def test_an_unknown_source_name_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown source"):
        build_sources(["solar"])


# ======================================================================
# The scheduler
# ======================================================================


async def test_a_throwing_scheduled_job_does_not_stop_the_scheduler(
    status: StatusStore,
) -> None:
    clock = FakeClock(utc(2026, 8, 16, 18, 0))
    stop = asyncio.Event()
    good: list[datetime] = []

    def boom(now: datetime) -> None:
        raise RuntimeError("S3 is unreachable")

    def fine(now: datetime) -> dict[str, int]:
        good.append(now)
        if len(good) >= 3:
            stop.set()
        return {"rows": 7}

    scheduler = Scheduler(
        [
            ScheduledJob("bad", HourlyAt(5), boom),
            ScheduledJob("good", HourlyAt(5), fine),
        ],
        clock=clock.now,
        sleep=clock.sleep,
        status=status,
    )

    await asyncio.wait_for(scheduler.run_forever(stop), timeout=5)

    assert len(good) >= 3  # the sibling job kept firing
    assert scheduler.failures >= 3
    assert scheduler.runs >= 3
    section = status.section("scheduler")
    assert section["consecutive_failures"] >= 3
    assert "S3 is unreachable" in section["last_error"]
    # Both jobs are still scheduled for the future, not stuck in the past.
    assert all(when > clock.now() for when in scheduler.next_runs.values())


async def test_jobs_fire_at_their_local_times(status: StatusStore) -> None:
    clock = FakeClock(utc(2026, 8, 16, 18, 0))  # 14:00 local
    stop = asyncio.Event()
    fired: list[tuple[str, datetime]] = []

    def record(name: str) -> Callable[[datetime], None]:
        def _run(now: datetime) -> None:
            fired.append((name, timeutil.to_local_naive(now)))
            if len(fired) >= 4:
                stop.set()

        return _run

    scheduler = Scheduler(
        [
            ScheduledJob("upload", HourlyAt(5), record("upload")),
            ScheduledJob("rollup", HourlyAt(20), record("rollup")),
        ],
        clock=clock.now,
        sleep=clock.sleep,
        status=status,
    )

    await asyncio.wait_for(scheduler.run_forever(stop), timeout=5)

    assert [name for name, _ in fired][:4] == ["upload", "rollup", "upload", "rollup"]
    assert [when.minute for _, when in fired][:4] == [5, 20, 5, 20]
    # 18:00 UTC is 14:00 local (EDT), so the first pair is 14:05 / 14:20.
    assert [when.hour for _, when in fired][:4] == [14, 14, 15, 15]


async def test_a_job_is_not_fired_twice_for_one_slot(status: StatusStore) -> None:
    """A job that overruns its own slot must not stampede."""
    clock = FakeClock(utc(2026, 8, 16, 18, 4, 59))
    stop = asyncio.Event()
    fired: list[datetime] = []

    async def slow(now: datetime) -> None:
        fired.append(now)
        clock.utc += timedelta(minutes=61)  # the job outlives its next slot
        if len(fired) >= 2:
            stop.set()

    scheduler = Scheduler(
        [ScheduledJob("slow", HourlyAt(5), slow)],
        clock=clock.now,
        sleep=clock.sleep,
        status=status,
    )
    await asyncio.wait_for(scheduler.run_forever(stop), timeout=5)

    assert len(fired) == 2
    assert fired[1] - fired[0] >= timedelta(hours=1)


async def test_scheduler_stops_promptly_when_asked(status: StatusStore) -> None:
    clock = FakeClock(utc(2026, 8, 16, 18, 0))
    stop = asyncio.Event()
    stop.set()
    scheduler = Scheduler(
        [ScheduledJob("never", DailyAt(1, 30), lambda now: None)],
        clock=clock.now,
        sleep=clock.sleep,
        status=status,
    )
    await asyncio.wait_for(scheduler.run_forever(stop), timeout=5)
    assert scheduler.runs == 0


# ------------------------------------------------------- the Bryant daily job


def test_the_bryant_daily_stage_has_landed() -> None:
    """PLAN.md §7.2 is built: the 08:30 job resolves a real ``run()``.

    This is the counterpart to the defensive branch below — it is what stops the
    "not implemented" skip from quietly becoming the normal path again.
    """
    daily = importlib.import_module("energy_capture.stages.daily")
    assert callable(daily.run)


async def test_a_missing_daily_module_logs_a_warn_and_does_not_raise(
    monkeypatch: pytest.MonkeyPatch, log_stream: io.StringIO
) -> None:
    """The stage exists now, so the defensive branch is driven directly.

    It stays in the code (and under test) because ``runtime`` must survive a
    partial deployment — an image built without ``stages/daily.py`` should skip
    one job, not take the poll loops down with it.
    """
    module_name = "energy_capture.stages.daily"
    real_import = importlib.import_module

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == module_name:
            raise ModuleNotFoundError(f"No module named '{name}'", name=name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(runtime.importlib, "import_module", fake_import)

    result = await runtime._job_bryant_daily(utc(2026, 8, 16, 12, 30))

    assert result["skipped"] == "not_implemented"
    warned = log_events(log_stream, "scheduled_job_not_implemented")
    assert len(warned) == 1
    assert warned[0]["level"] == "WARNING"
    assert warned[0]["module"] == module_name


async def test_a_broken_import_inside_the_daily_stage_is_not_disguised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing third-party package inside the stage is a deployment bug and
    must propagate, not be reported as "not built yet"."""
    real_import = importlib.import_module

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "energy_capture.stages.daily":
            raise ModuleNotFoundError("No module named 'pyarrow'", name="pyarrow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(runtime.importlib, "import_module", fake_import)

    with pytest.raises(ModuleNotFoundError, match="pyarrow"):
        await runtime._job_bryant_daily(utc(2026, 8, 16, 12, 30))


async def test_the_scheduled_bryant_daily_job_reaches_the_real_stage(
    status: StatusStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fire the job the way the scheduler does and check it lands in §7.2's
    ``run()`` over day2..day1 — no "skipped" result, no failure counted.

    ``run`` is replaced rather than executed: the real one would talk to Carrier,
    and the conftest network guard would (correctly) refuse.
    """
    captured: dict[str, Any] = {}
    daily = importlib.import_module("energy_capture.stages.daily")
    fired = utc(2026, 8, 16, 12, 30)

    def fake_run(*, start: date, end: date, **kwargs: Any) -> dict[str, int]:
        captured.update(start=start, end=end, now=kwargs.get("now"))
        return {"rows": 16, "months": 1}

    monkeypatch.setattr(daily, "run", fake_run)

    scheduler = Scheduler([], status=status)
    # The process's clock, the one the scheduler fires on. The job's window has
    # to come from a read of *this* clock rather than the ambient wall clock,
    # so pinning it is what makes the expected dates below meaningful.
    job = {job.name: job for job in default_jobs(clock=lambda: fired)}[
        "bryant_daily_energy"
    ]
    outcome = await scheduler.fire(job, fired)

    assert outcome.ok
    assert scheduler.failures == 0
    assert captured == {
        "start": date(2026, 8, 14),
        "end": date(2026, 8, 15),
        # The stage is handed the same instant the window was built from, so it
        # never re-reads the clock to date day1/day2.
        "now": fired,
    }
    assert outcome.result == {"rows": 16, "months": 1}
    assert status.section("scheduler").get("consecutive_failures", 0) == 0


async def test_the_bryant_daily_job_calls_the_stage_once_it_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fired = utc(2026, 8, 16, 12, 30)

    def fake_run(*, start: date, end: date, now: datetime) -> dict[str, int]:
        captured.update(start=start, end=end, now=now)
        return {"rows": 16}

    module = types.ModuleType("energy_capture.stages.daily")
    module.run = fake_run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "energy_capture.stages.daily", module)
    from energy_capture import stages

    monkeypatch.setattr(stages, "daily", module, raising=False)

    result = await runtime._job_bryant_daily(fired, clock=lambda: fired)

    # day2 (the revision) through day1, per PLAN.md §7.2 — and the same instant
    # handed down, so the stage dates day1/day2 off the window's own clock read.
    assert captured == {
        "start": date(2026, 8, 14),
        "end": date(2026, 8, 15),
        "now": fired,
    }
    assert result == {"rows": 16}


def _dating_stage(
    captured: dict[str, Any],
) -> Callable[..., dict[str, Any]]:
    """A stand-in for ``stages/daily.py::run`` that dates rows exactly as it does.

    The real stage derives ``today`` from its ``now`` argument *or*, when it is
    not given one, from a fresh read of the wall clock — and then keeps whichever
    of ``day1``/``day2`` falls inside ``[start, end]`` (``daily.run``,
    ``period_local_date``). Reproducing that here rather than only recording the
    kwargs is the point: it is what makes "the rows were dated from one clock and
    filtered by a window built from another" visible as discarded rows.
    """
    daily = importlib.import_module("energy_capture.stages.daily")

    def fake_run(
        *, start: date, end: date, now: datetime | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
        today = timeutil.local_date_of(reference)
        dated = {
            period: daily.period_local_date(period, today=today)
            for period in daily.PERIOD_OFFSET_DAYS
        }
        kept = {p: d for p, d in dated.items() if start <= d <= end}
        captured.update(
            start=start, end=end, now=now, today=today, dated=dated, kept=kept
        )
        return {"rows": len(kept) * 8, "days": [d.isoformat() for d in kept.values()]}

    return fake_run


@pytest.mark.parametrize(
    ("slot_local", "fired_local", "expected_today"),
    [
        # The host was suspended (lid closed, VM paused) and the 08:30 job did
        # not actually fire until the next local day.
        (
            datetime(2026, 8, 16, 8, 30),
            datetime(2026, 8, 17, 9, 12),
            date(2026, 8, 17),
        ),
        # The sub-second version of the same bug: the slot and the fetch land on
        # opposite sides of local midnight. Nothing exotic is required.
        (
            datetime(2026, 8, 16, 23, 59, 59),
            datetime(2026, 8, 17, 0, 0, 0, 400000),
            date(2026, 8, 17),
        ),
    ],
    ids=["suspended_host", "across_local_midnight"],
)
async def test_the_bryant_daily_window_and_the_stages_dating_share_one_clock_read(
    status: StatusStore,
    monkeypatch: pytest.MonkeyPatch,
    log_stream: io.StringIO,
    slot_local: datetime,
    fired_local: datetime,
    expected_today: date,
) -> None:
    """Two clock reads on opposite sides of a local midnight = silent data loss.

    The Carrier cloud dates ``day1``/``day2`` relative to the instant of the
    *fetch*, so the window cannot be built from the scheduler's slot — but it
    must not be built from a *second* read either. If the window comes from one
    read and the stage dates its rows from another, day1 is fetched, dated, and
    then filtered straight back out, and the job still reports SUCCESS: fetched
    energy vanishes with a green ``job_ok`` line behind it (CLAUDE.md rule 1 —
    a gap must be a gap because nothing was measured, never because the
    collector threw a measurement away).

    One read, used for both halves.
    """
    slot = timeutil.local_naive_to_utc(slot_local)
    fired = timeutil.local_naive_to_utc(fired_local)
    captured: dict[str, Any] = {}

    daily = importlib.import_module("energy_capture.stages.daily")
    monkeypatch.setattr(daily, "run", _dating_stage(captured))
    # The wall clock the stage would read if it were left to read one itself.
    monkeypatch.setattr(timeutil, "now_utc", lambda: fired)

    scheduler = Scheduler([], status=status)
    job = {job.name: job for job in default_jobs(clock=lambda: fired)}[
        "bryant_daily_energy"
    ]
    outcome = await scheduler.fire(job, slot)

    assert outcome.ok
    # The window follows the fetch, not the stale slot it was scheduled for.
    assert captured["today"] == expected_today
    assert captured["start"] == expected_today - timedelta(days=2)
    assert captured["end"] == expected_today - timedelta(days=1)
    # ...and the stage dated its rows from that same instant.
    assert captured["now"] == fired
    # Nothing fetched was silently discarded: both day-grain periods the cloud
    # served fall inside the window that filtered them.
    assert captured["kept"] == captured["dated"]
    assert sorted(captured["kept"].values()) == [
        expected_today - timedelta(days=2),
        expected_today - timedelta(days=1),
    ]

    # A suspended or stepped host is operationally interesting, so it is visible.
    warned = log_events(log_stream, "bryant_daily_clock_skew")
    assert len(warned) == 1
    assert warned[0]["level"] == "WARNING"
    assert warned[0]["scheduled_local_date"] == slot_local.date().isoformat()
    assert warned[0]["fetch_local_date"] == expected_today.isoformat()
    assert warned[0]["skew_s"] == pytest.approx((fired - slot).total_seconds())


async def test_an_ordinary_bryant_daily_firing_logs_no_clock_skew_warning(
    status: StatusStore, monkeypatch: pytest.MonkeyPatch, log_stream: io.StringIO
) -> None:
    """The WARN is a real signal, not noise: it must not fire 365 nights a year.

    A few seconds of scheduler latency inside the same local date is normal and
    says nothing about the host.
    """
    slot = timeutil.local_naive_to_utc(datetime(2026, 8, 16, 8, 30))
    fired = slot + timedelta(seconds=4)
    captured: dict[str, Any] = {}

    daily = importlib.import_module("energy_capture.stages.daily")
    monkeypatch.setattr(daily, "run", _dating_stage(captured))
    monkeypatch.setattr(timeutil, "now_utc", lambda: fired)

    scheduler = Scheduler([], status=status)
    job = {job.name: job for job in default_jobs(clock=lambda: fired)}[
        "bryant_daily_energy"
    ]
    outcome = await scheduler.fire(job, slot)

    assert outcome.ok
    assert (captured["start"], captured["end"]) == (date(2026, 8, 14), date(2026, 8, 15))
    assert captured["kept"] == captured["dated"]
    assert log_events(log_stream, "bryant_daily_clock_skew") == []


async def test_the_bryant_daily_clock_read_happens_after_the_import_guards(
    monkeypatch: pytest.MonkeyPatch, log_stream: io.StringIO
) -> None:
    """A firing that never fetches must not report clock skew.

    The fresh read is taken at the moment of the fetch, so a partially built
    image (no ``stages/daily.py``) skips before any clock question arises — and
    the skew WARN keeps meaning "a fetch ran on the wrong local date".
    """
    module_name = "energy_capture.stages.daily"
    real_import = importlib.import_module
    reads = 0

    def counting_clock() -> datetime:
        nonlocal reads
        reads += 1
        return timeutil.local_naive_to_utc(datetime(2026, 8, 17, 9, 12))

    def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == module_name:
            raise ModuleNotFoundError(f"No module named '{name}'", name=name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(runtime.importlib, "import_module", fake_import)

    slot = timeutil.local_naive_to_utc(datetime(2026, 8, 16, 8, 30))
    result = await runtime._job_bryant_daily(slot, clock=counting_clock)

    assert result["skipped"] == "not_implemented"
    assert reads == 0
    assert log_events(log_stream, "bryant_daily_clock_skew") == []


async def test_a_daily_module_without_an_entrypoint_is_skipped_not_crashed(
    monkeypatch: pytest.MonkeyPatch, log_stream: io.StringIO
) -> None:
    module = types.ModuleType("energy_capture.stages.daily")  # no run()
    monkeypatch.setitem(sys.modules, "energy_capture.stages.daily", module)
    from energy_capture import stages

    monkeypatch.setattr(stages, "daily", module, raising=False)

    result = await runtime._job_bryant_daily(utc(2026, 8, 16, 12, 30))

    assert result["skipped"] == "no_entrypoint"
    assert log_events(log_stream, "scheduled_job_not_implemented")


async def test_daily_maintenance_runs_every_step_and_reports_the_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One broken step must not strand the others (PLAN.md §10 ordering)."""
    calls: list[str] = []

    def make(name: str, *, boom: bool = False):
        def _run(**kwargs: Any) -> dict[str, Any]:
            calls.append(name)
            if boom:
                raise RuntimeError(f"{name} exploded")
            return {"rows": 1, **{k: v for k, v in kwargs.items()}}

        return _run

    uploader = types.SimpleNamespace(run=make("upload"))
    compactor = types.SimpleNamespace(run=make("compact", boom=True))
    rollup = types.SimpleNamespace(run=make("rollup"))
    from energy_capture import stages

    for name, module in (
        ("uploader", uploader),
        ("compactor", compactor),
        ("rollup", rollup),
    ):
        monkeypatch.setitem(sys.modules, f"energy_capture.stages.{name}", module)
        monkeypatch.setattr(stages, name, module, raising=False)

    with pytest.raises(runtime.JobStepError, match="compact"):
        await runtime._job_daily_maintenance(utc(2026, 8, 16, 5, 30))

    # Every step was attempted, in the documented order.
    assert calls == ["upload", "compact", "rollup"]


async def test_daily_maintenance_windows_end_at_yesterday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    windows: dict[str, tuple[date, date]] = {}

    def recorder(name: str):
        def _run(**kwargs: Any) -> dict[str, int]:
            if "start" in kwargs:
                windows[name] = (kwargs["start"], kwargs["end"])
            return {"rows": 0}

        return _run

    from energy_capture import stages

    for name in ("uploader", "compactor", "rollup"):
        module = types.SimpleNamespace(run=recorder(name))
        monkeypatch.setitem(sys.modules, f"energy_capture.stages.{name}", module)
        monkeypatch.setattr(stages, name, module, raising=False)

    # 2026-08-16 01:30 local == 05:30 UTC.
    await runtime._job_daily_maintenance(utc(2026, 8, 16, 5, 30), lookback_days=3)

    assert windows["compactor"][1] == date(2026, 8, 15)  # D-1
    assert windows["compactor"][0] == date(2026, 8, 13)  # the healing lookback
    assert windows["rollup"] == windows["compactor"]


async def test_the_hourly_rollup_job_covers_the_day_of_hour_hh_minus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, date] = {}

    def fake_run(*, start: date, end: date, **kwargs: Any) -> dict[str, int]:
        seen.update(start=start, end=end)
        return {"rows": 0}

    module = types.SimpleNamespace(run=fake_run)
    from energy_capture import stages

    monkeypatch.setitem(sys.modules, "energy_capture.stages.rollup", module)
    monkeypatch.setattr(stages, "rollup", module, raising=False)

    # 00:20 local on 2026-08-16 == 04:20 UTC; hour HH-1 is 23:00 on the 15th.
    await runtime._job_hourly_rollup(utc(2026, 8, 16, 4, 20))
    assert seen == {"start": date(2026, 8, 15), "end": date(2026, 8, 16)}

    await runtime._job_hourly_rollup(utc(2026, 8, 16, 18, 20))
    assert seen == {"start": date(2026, 8, 16), "end": date(2026, 8, 16)}


# ======================================================================
# The process host
# ======================================================================


async def test_runtime_runs_poll_loops_and_the_scheduler_then_stops_cleanly(
    spool: CountingSpool, status: StatusStore
) -> None:
    fired: list[str] = []
    host: dict[str, Runtime] = {}

    def job(now: datetime) -> dict[str, int]:
        fired.append("job")
        return {"rows": 1}

    def stop_after_two(source: FakeSource) -> None:
        if source.polls >= 2:
            host["runtime"].request_stop("test")

    source = FakeSource(on_poll=stop_after_two)
    clock = FakeClock(utc(2026, 8, 16, 18, 4, 59))
    host["runtime"] = Runtime(
        sources=[source],
        spool=spool,
        status=status,
        jobs=[ScheduledJob("hourly", HourlyAt(5), job)],
        health_enabled=False,
        install_signal_handlers=False,
        clock=clock.now,
        sleep=instant_sleep,
    )

    summary = await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    assert summary["reason"] == "test"
    assert summary["cycles"] >= 2
    assert summary["sources"] == ["leviton"]
    assert source.closed == 1
    assert spool.stats().total_rows >= 2


async def test_sigterm_shuts_down_cleanly_and_the_spool_keeps_its_rows(
    tmp_path: Path, status: StatusStore
) -> None:
    """``docker stop`` must be a clean stop, not data loss (PLAN.md §5)."""
    db_path = tmp_path / "spool.db"
    spool = SpoolDB(db_path)
    host: dict[str, Runtime] = {}

    def raise_sigterm(source: FakeSource) -> None:
        if source.polls != 1:
            return
        # Only signal once the asyncio handler is definitely installed —
        # otherwise the default disposition would kill the test process.
        assert "SIGTERM" in host["runtime"].installed_signals
        signal.raise_signal(signal.SIGTERM)

    source = FakeSource(on_poll=raise_sigterm)
    host["runtime"] = Runtime(
        sources=[source],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=True,
        sleep=instant_sleep,
    )

    summary = await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    assert summary["reason"] == "signal:SIGTERM"
    assert summary["cycles"] >= 1
    assert source.closed == 1
    assert host["runtime"].installed_signals == ()  # handlers were removed

    # The cycle that was in flight when the signal arrived is durable: a fresh
    # connection to the same file sees its rows.
    reopened = SpoolDB(db_path)
    try:
        assert reopened.stats().total_rows == 2 * source.polls
    finally:
        reopened.close()
    # …and the final pending count reached status.json before shutdown.
    assert status.section("spool")["pending_rows"] == 2 * source.polls


async def test_a_second_signal_does_not_restart_the_shutdown(
    spool: CountingSpool, status: StatusStore, log_stream: io.StringIO
) -> None:
    host = Runtime(
        sources=[FakeSource()],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )
    host.request_stop("signal:SIGTERM")
    host.request_stop("signal:SIGINT")
    assert host.stop_event.is_set()
    assert log_events(log_stream, "shutdown_already_requested")


async def test_a_crashing_task_fails_the_process(
    spool: CountingSpool, status: StatusStore
) -> None:
    """A real bug must exit non-zero so the container restarts."""

    class ExplodingSchedule:
        def next_after(self, now_utc: datetime) -> datetime:
            raise ZeroDivisionError("schedule is broken")

        def describe(self) -> str:
            return "broken"

    host = Runtime(
        sources=[FakeSource(stop_after=None)],
        spool=spool,
        status=status,
        jobs=[ScheduledJob("broken", ExplodingSchedule(), lambda now: None)],
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    with pytest.raises(ZeroDivisionError):
        await asyncio.wait_for(host.serve(), timeout=10)

    # …and it still shut the sources down on the way out.
    assert host.poller is not None
    assert all(source.closed == 1 for source in host.poller.sources)  # type: ignore[attr-defined]


async def test_runtime_schedules_every_source_background_task(
    spool: CountingSpool, status: StatusStore
) -> None:
    """The Leviton keepalive must start with polling, not after it (§6.4)."""
    keepalives: list[int] = []
    host: dict[str, Runtime] = {}

    async def keepalive() -> None:
        keepalives.append(1)
        if len(keepalives) >= 2:
            host["runtime"].request_stop("test")

    source = FakeSource(
        background=[BackgroundTask("leviton_keepalive", 50.0, keepalive, 0.0)]
    )
    host["runtime"] = Runtime(
        sources=[source],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    assert len(keepalives) >= 2


async def test_the_health_server_serves_status_and_is_stopped_on_shutdown(
    spool: CountingSpool, status: StatusStore
) -> None:
    host: dict[str, Runtime] = {}
    seen: dict[str, Any] = {}

    def probe(source: FakeSource) -> None:
        if source.polls != 1:
            return
        seen["port"] = host["runtime"]._health.port  # noqa: SLF001
        host["runtime"].request_stop("test")

    host["runtime"] = Runtime(
        sources=[FakeSource(on_poll=probe)],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=True,
        health_port=0,  # ephemeral
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    assert seen["port"] > 0
    assert host["runtime"]._health is None  # noqa: SLF001


# ======================================================================
# The Leviton WebSocket, as this host sees it
# ======================================================================
#
# The host does not know a socket exists — it schedules background tasks and
# closes sources. What is pinned here is that those two generic guarantees are
# enough for the freshness layer: a socket that is broken forever cannot stop
# collection, cannot stop the process, cannot turn the container unhealthy, and
# is torn down on the way out.


class FakeSocketSource(FakeSource):
    """A source that owns something socket-shaped, like ``LevitonSource`` does.

    ``ticks`` counts supervisor runs; ``torn_down`` records that ``close()``
    reached the socket. The real teardown is pinned in ``tests/test_leviton.py``
    — what matters here is that the *host* gets the source closed at all.
    """

    def __init__(self, *, tick_error: BaseException | None = None, **kwargs: Any) -> None:
        self.ticks = 0
        self.torn_down = 0
        self.tick_error = tick_error
        super().__init__(**kwargs)
        self._background = (
            BackgroundTask("leviton_keepalive", 50.0, self._keepalive, 0.0),
            BackgroundTask("leviton_ws", 15.0, self._tick, 0.0),
        )

    async def _keepalive(self) -> None:
        return None

    async def _tick(self) -> None:
        self.ticks += 1
        if self.tick_error is not None:
            raise self.tick_error

    async def close(self) -> None:
        self.torn_down += 1
        await super().close()


async def test_a_websocket_task_that_always_fails_never_stops_the_poll_loops(
    spool: CountingSpool, status: StatusStore, log_stream: io.StringIO
) -> None:
    """The freshness layer is an optimisation; collection outranks it.

    ``LevitonWebSocketIngester.tick`` already absorbs everything, so this drives
    the pathological case it is supposed to make impossible — a supervisor that
    raises on every single run — and asserts the loops, the process and the
    shutdown are all unaffected.
    """
    host: dict[str, Runtime] = {}

    def stop_after_three(source: FakeSource) -> None:
        if source.polls >= 3:
            host["runtime"].request_stop("test")

    source = FakeSocketSource(
        tick_error=RuntimeError("the socket layer is on fire"),
        on_poll=stop_after_three,
    )
    host["runtime"] = Runtime(
        sources=[source],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    summary = await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    assert summary["reason"] == "test"  # not a crash
    assert summary["cycles"] >= 3  # the loops never noticed
    assert spool.stats().total_rows >= 6
    assert source.ticks >= 2  # and the task kept being retried, not abandoned
    failures = log_events(log_stream, "background_task_failed")
    assert failures and all(f["task"] == "leviton_ws" for f in failures)


async def test_the_websocket_supervisor_is_scheduled_and_torn_down_on_shutdown(
    spool: CountingSpool, status: StatusStore, log_stream: io.StringIO
) -> None:
    """``docker stop`` closes the source, and the source is what owns the socket."""
    host: dict[str, Runtime] = {}

    def stop_after_two(source: FakeSource) -> None:
        if source.polls >= 2:
            host["runtime"].request_stop("test")

    source = FakeSocketSource(on_poll=stop_after_two)
    host["runtime"] = Runtime(
        sources=[source],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    scheduled = {e["task"] for e in log_events(log_stream, "background_task_scheduled")}
    assert {"leviton_keepalive", "leviton_ws"} <= scheduled
    assert source.ticks >= 1
    assert source.torn_down == 1


async def test_the_startup_log_names_the_active_ingestion_mode(
    spool: CountingSpool, status: StatusStore, log_stream: io.StringIO
) -> None:
    """Which mechanism keeps values fresh is invisible in the rows; say it once.

    The schema has no provenance column (PLAN.md §3), so a log tail has to be
    enough to know how a container is collecting.
    """
    host: dict[str, Runtime] = {}

    def stop_now(source: FakeSource) -> None:
        host["runtime"].request_stop("test")

    host["runtime"] = Runtime(
        sources=[FakeSource(on_poll=stop_now)],
        spool=spool,
        status=status,
        jobs=(),
        health_enabled=False,
        install_signal_handlers=False,
        sleep=instant_sleep,
    )

    await asyncio.wait_for(host["runtime"].serve(), timeout=10)

    starting = log_events(log_stream, "runtime_starting")
    assert starting and starting[0]["leviton_ingest"] == "hybrid"


async def test_a_dead_websocket_does_not_make_healthz_fail_while_rest_collects(
    spool: CountingSpool, status: StatusStore
) -> None:
    """PLAN.md §11 liveness is "are observations arriving", not "is every
    mechanism that could produce them healthy".

    A socket that is down while REST is still landing rows every 30s is a
    degraded collector, not a dead one — and a container that restarts itself
    over it would lose the spool's in-flight cycle for nothing.
    """
    poller = Poller([FakeSource()], spool, status=status, sleep=instant_sleep)
    await poller.start()
    try:
        # Only the poller sections are judged; the source's own sections are not.
        assert set(status.poll_intervals) == {"leviton"}

        await poller.poll_once()
        status.record_failure("leviton_ws", RuntimeError("socket down"), connected=False)
        status.record_failure("leviton_keepalive", RuntimeError("502"))

        code, body = status.health_report()
        assert code == 200
        assert body["health"]["ok"] is True
        assert [c["section"] for c in body["health"]["checks"]] == ["leviton"]
        # …and the condition is still visible to a human reading the document.
        assert body["leviton_ws"]["consecutive_failures"] == 1
    finally:
        await poller.close()


def test_the_cli_points_run_at_this_module() -> None:
    from energy_capture import cli

    assert cli.STAGE_ENTRYPOINTS["run"] == ("energy_capture.runtime", "run")
    assert cli.STAGE_ENTRYPOINTS["poll"] == ("energy_capture.stages.poller", "run")
    assert callable(runtime.run)


# ======================================================================
# Regression: a stage summary must not be able to kill the scheduler
# ======================================================================


async def test_job_ok_survives_a_result_that_carries_its_own_duration_s(
    log_stream: io.StringIO,
) -> None:
    """A stage summary sharing a keyword with the scheduler's own log fields
    must not raise out of ``fire()``.

    ``UploadSummary`` is a Mapping whose fields include ``duration_s``. Splatting
    it alongside the scheduler's ``duration_s=`` raised ``TypeError`` *outside*
    ``fire()``'s try/except, killing the scheduler task and taking the process
    down at HH:05 every hour — an hourly crash loop the rest of the suite could
    not see, because no test fired a real job with a real stage summary.
    """
    from energy_capture.stages.uploader import UploadSummary

    summary = UploadSummary([], duration_s=0.42)
    assert "duration_s" in dict(summary)  # the trap this test exists for

    scheduler = runtime.Scheduler(jobs=())
    job = runtime.ScheduledJob(
        name="upload_hourly",
        schedule=runtime.HourlyAt(minute=5),
        run=lambda now: summary,
    )

    outcome = await scheduler.fire(job, utc(2026, 8, 16, 14, 5))

    assert outcome.ok is True
    assert outcome.result is summary
    assert scheduler.failures == 0

    (line,) = log_events(log_stream, "job_ok")
    assert line["job"] == "upload_hourly"
    # The scheduler's measured wall time wins; the stage's own timing is kept
    # under a distinct key rather than colliding or being silently dropped.
    assert line["duration_s"] == outcome.duration_s
    assert line["stage_duration_s"] == 0.42


async def test_job_ok_never_raises_even_for_an_unloggable_result(
    log_stream: io.StringIO,
) -> None:
    """``fire()`` documents "never raises". Belt and braces for the general case:
    no future stage return value gets to kill the scheduler over a log line."""

    class Exploding(dict):
        def keys(self):  # noqa: D102 - splatting this raises
            raise RuntimeError("boom")

    scheduler = runtime.Scheduler(jobs=())
    job = runtime.ScheduledJob(
        name="rollup_hourly",
        schedule=runtime.HourlyAt(minute=20),
        run=lambda now: Exploding(),
    )

    outcome = await scheduler.fire(job, utc(2026, 8, 16, 14, 20))

    assert outcome.ok is True
    assert scheduler.failures == 0
    assert log_events(log_stream, "job_ok")


# ======================================================================
# The Green Button daily job
# ======================================================================


def test_greenbutton_daily_skips_quietly_when_connect_is_not_configured(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """An unauthorised deployment must not fail a job every morning.

    A scheduled job that errors daily on a perfectly normal configuration is
    noise, and noise is what teaches an operator to stop reading the log. Not
    configured and not authorised are both ordinary states — they report
    ``skipped``, not a failure.
    """
    from energy_capture.config import Settings

    monkeypatch.setattr(
        runtime, "get_settings", lambda: Settings(_env_file=None, spool_dir=tmp_path)
    )
    result = asyncio.run(runtime._job_greenbutton_daily(timeutil.now_utc()))
    assert result == {"skipped": "not_configured"}


def test_greenbutton_daily_skips_when_configured_but_never_authorised(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    from energy_capture.config import Settings

    monkeypatch.setattr(
        runtime,
        "get_settings",
        lambda: Settings(
            _env_file=None,
            spool_dir=tmp_path,
            lge_client_id="gbc_test",
            lge_client_secret="s3cret-value",
        ),
    )
    result = asyncio.run(runtime._job_greenbutton_daily(timeutil.now_utc()))
    assert result == {"skipped": "not_authorized"}


def test_greenbutton_daily_reads_back_far_enough_to_catch_a_revision(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """MyMeter revises recent intervals, so one day's window would miss them."""
    from energy_capture.config import Settings

    tokens = tmp_path / "tokens"
    tokens.mkdir(parents=True)
    (tokens / "lge.json").write_text("{}")
    monkeypatch.setattr(
        runtime,
        "get_settings",
        lambda: Settings(
            _env_file=None,
            spool_dir=tmp_path,
            lge_client_id="gbc_test",
            lge_client_secret="s3cret-value",
        ),
    )

    captured: dict = {}

    class FakeStage:
        @staticmethod
        def run(**kwargs):
            captured.update(kwargs)
            return {"rows": 0}

    monkeypatch.setattr(runtime.importlib, "import_module", lambda name: FakeStage)

    now = timeutil.now_utc()
    asyncio.run(runtime._job_greenbutton_daily(now))
    today = timeutil.local_date_of(now)
    assert captured["end"] == today
    assert captured["start"] == today - timedelta(days=runtime.GREENBUTTON_LOOKBACK_DAYS)
    assert runtime.GREENBUTTON_LOOKBACK_DAYS >= 2, "one day cannot catch a revision"
