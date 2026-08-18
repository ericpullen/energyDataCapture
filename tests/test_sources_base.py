"""Tests for :mod:`energy_capture.sources.base`.

This module is the source-side chokepoint for two cardinal rules: *one timestamp
per source per poll cycle* (PLAN.md §6.5) and *a gap stays a gap* (CLAUDE.md
rule 1). Every source depends on ``PollCycle`` for both, so the guarantees are
pinned here rather than re-tested in each source's file.
"""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timedelta, timezone

import pytest

from energy_capture import model, timeutil
from energy_capture.config import MIN_POLL_INTERVAL_S
from energy_capture.model import Observation
from energy_capture.sources.base import (
    BackgroundTask,
    BaseSource,
    DiscoveredChannel,
    DiscoveredDevice,
    Discovery,
    PollCycle,
    Source,
    SourceAuthError,
    SourceError,
    SourceTransientError,
)
from tests.conftest import utc

# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


def test_both_failure_modes_are_source_errors_but_distinguishable() -> None:
    """The poll loop tells transient apart from auth; both are catchable as one."""
    assert issubclass(SourceTransientError, SourceError)
    assert issubclass(SourceAuthError, SourceError)
    assert not issubclass(SourceAuthError, SourceTransientError)
    assert not issubclass(SourceTransientError, SourceAuthError)


# ---------------------------------------------------------------------------
# PollCycle: construction
# ---------------------------------------------------------------------------


def test_cycle_rejects_an_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown source"):
        PollCycle("solaredge")


@pytest.mark.parametrize("source", sorted(model.SOURCES))
def test_cycle_accepts_every_declared_source(source: str) -> None:
    assert PollCycle(source).source == source


def test_a_fresh_cycle_is_open_empty_and_unstamped() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    assert cycle.finished is False
    assert cycle.ts_utc is None
    assert len(cycle) == 0
    assert cycle.gaps == 0
    assert cycle.gap_keys == ()
    with pytest.raises(RuntimeError, match="not finished"):
        _ = cycle.observations


# ---------------------------------------------------------------------------
# PollCycle: the single-timestamp guarantee (PLAN.md §6.5)
# ---------------------------------------------------------------------------


def test_every_row_of_a_cycle_shares_one_ts_utc_to_the_microsecond() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    for position in range(1, 25):
        cycle.add("hub-a", f"breaker_p{position}", "watts", float(position))
        cycle.add("hub-a", f"breaker_p{position}", "amps", position / 10)
    rows = cycle.finish()

    assert len(rows) == 48
    stamps = {row.ts_utc for row in rows}
    assert len(stamps) == 1
    stamp = stamps.pop()
    assert stamp == cycle.ts_utc
    # µs precision must survive: the Arrow type is timestamp[us].
    assert stamp.tzinfo is not None
    assert stamp.utcoffset() == timedelta(0)


def test_two_cycles_of_the_same_source_get_different_timestamps() -> None:
    first = PollCycle(model.SOURCE_LEVITON)
    first.add("hub-a", "breaker_p1", "watts", 1.0)
    first.finish()
    second = PollCycle(model.SOURCE_LEVITON)
    second.add("hub-a", "breaker_p1", "watts", 2.0)
    second.finish()
    assert second.ts_utc >= first.ts_utc
    # Not the same object smuggled across cycles.
    assert second.observations[0] is not first.observations[0]


def test_the_stamp_is_taken_at_finish_not_at_construction() -> None:
    """§6.5: the instant is 'when the response set is complete'."""
    before = timeutil.now_utc()
    cycle = PollCycle(model.SOURCE_BRYANT)
    cycle.add("SYS1", "zone_1", "indoor_temp_f", 71.5)
    assert cycle.ts_utc is None  # nothing stamped while the fetch is in flight
    cycle.finish()
    after = timeutil.now_utc()
    assert before <= cycle.ts_utc <= after


def test_explicit_finish_timestamp_wins_and_is_normalised_to_utc() -> None:
    eastern_ish = datetime(2026, 8, 16, 14, 0, 30, 123456, tzinfo=timezone(timedelta(hours=-4)))
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.add("hub-a", "breaker_p1", "watts", 42.0)
    rows = cycle.finish(eastern_ish)
    assert rows[0].ts_utc == utc(2026, 8, 16, 18, 0, 30, 123456)
    assert rows[0].ts_utc.tzinfo is timeutil.UTC


def test_a_pinned_timestamp_is_visible_before_finish_and_used_by_it() -> None:
    """Backfill/day-grain callers already know the instant the data describes."""
    pinned = utc(2026, 8, 16, 4, 0)
    cycle = PollCycle(model.SOURCE_BRYANT, ts_utc=pinned)
    assert cycle.ts_utc == pinned
    assert cycle.finished is False
    cycle.add("SYS1", "hpheat", "kwh_day", 12.5)
    rows = cycle.finish()
    assert rows[0].ts_utc == pinned


def test_finish_argument_overrides_a_pinned_timestamp() -> None:
    cycle = PollCycle(model.SOURCE_BRYANT, ts_utc=utc(2026, 8, 16))
    cycle.add("SYS1", "hpheat", "kwh_day", 1.0)
    rows = cycle.finish(utc(2026, 8, 17))
    assert rows[0].ts_utc == utc(2026, 8, 17)
    assert cycle.ts_utc == utc(2026, 8, 17)


def test_a_pinned_naive_timestamp_is_read_as_utc_never_as_wall_clock() -> None:
    """``ensure_utc`` assumes naive means UTC; local wall clock has its own door."""
    cycle = PollCycle(model.SOURCE_BRYANT, ts_utc=datetime(2026, 8, 16, 4, 0))
    cycle.add("SYS1", "hpheat", "kwh_day", 1.0)
    row = cycle.finish()[0]
    assert row.ts_utc == utc(2026, 8, 16, 4, 0)
    assert row.ts_local == datetime(2026, 8, 16, 0, 0)


def test_a_falsy_pinned_timestamp_is_impossible_to_express() -> None:
    """Only ``None`` means 'stamp me at finish' — every datetime is truthy."""
    assert PollCycle(model.SOURCE_BRYANT).ts_utc is None


# ---------------------------------------------------------------------------
# PollCycle: gaps stay gaps (CLAUDE.md rule 1)
# ---------------------------------------------------------------------------


def test_a_null_field_emits_no_row_and_is_counted_as_a_gap() -> None:
    """A null second-leg CT reading must not become a zero."""
    cycle = PollCycle(model.SOURCE_LEVITON)
    assert cycle.add("hub-a", "ct_3_a", "watts", 812.0) is True
    assert cycle.add("hub-a", "ct_3_b", "watts", None) is False
    rows = cycle.finish()

    assert [row.channel_id for row in rows] == ["ct_3_a"]
    assert cycle.gaps == 1
    assert cycle.gap_keys == (("hub-a", "ct_3_b", "watts"),)
    assert all(row.value != 0.0 for row in rows)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_are_refused_exactly_like_a_null(bad: float) -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    assert cycle.add("hub-a", "breaker_p1", "watts", bad) is False
    assert len(cycle) == 0
    assert cycle.gaps == 1
    assert cycle.finish() == []


def test_a_real_zero_is_data_and_passes_through_verbatim() -> None:
    """CLAUDE.md rule 2: Leviton's spurious zeros are recorded, not filtered."""
    cycle = PollCycle(model.SOURCE_LEVITON)
    assert cycle.add("hub-a", "breaker_p7", "watts", 0) is True
    assert cycle.add("hub-a", "breaker_p8", "watts", -3.25) is True
    rows = cycle.finish()
    assert [row.value for row in rows] == [0.0, -3.25]
    assert cycle.gaps == 0


def test_integer_values_are_widened_to_float_without_rounding() -> None:
    cycle = PollCycle(model.SOURCE_BRYANT)
    cycle.add("SYS1", "zone_1", "indoor_temp_f", 71)
    cycle.add("SYS1", "zone_1", "humidity_pct", 43.7)
    rows = cycle.finish()
    assert [type(row.value) for row in rows] == [float, float]
    assert [row.value for row in rows] == [71.0, 43.7]


def test_gap_count_is_a_health_signal_and_never_becomes_a_row() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    for position in range(1, 5):
        cycle.add("hub-a", f"breaker_p{position}", "watts", None)
    rows = cycle.finish()
    assert rows == []
    assert cycle.gaps == 4
    assert len(cycle.gap_keys) == 4


def test_add_metrics_records_only_the_present_fields() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    recorded = cycle.add_metrics(
        "hub-a",
        "breaker_p11",
        {"watts": 100.0, "amps": None, "volts": 241.3, "hz": math.nan},
    )
    assert recorded == 2
    rows = cycle.finish()
    assert {row.metric for row in rows} == {"watts", "volts"}
    assert cycle.gaps == 2


def test_add_metrics_propagates_interval_s_to_meter_rows() -> None:
    cycle = PollCycle(model.SOURCE_LGE)
    cycle.add_metrics("meter-1", "main", {"kwh_interval": 0.42}, interval_s=900)
    row = cycle.finish()[0]
    assert isinstance(row, model.MeterObservation)
    assert row.interval_s == 900


# ---------------------------------------------------------------------------
# PollCycle: a failed cycle emits nothing (CLAUDE.md rule 1)
# ---------------------------------------------------------------------------


def test_a_cycle_that_raises_mid_fetch_emits_zero_rows_not_partial_ones() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    with pytest.raises(SourceTransientError):
        with cycle:
            cycle.add("hub-a", "breaker_p1", "watts", 100.0)
            cycle.add("hub-a", "breaker_p2", "watts", 200.0)
            raise SourceTransientError("502 from the gateway")

    assert cycle.finished is False
    assert cycle.ts_utc is None
    with pytest.raises(RuntimeError, match="not finished"):
        _ = cycle.observations


def test_the_context_manager_does_not_swallow_the_exception() -> None:
    with pytest.raises(SourceAuthError):
        with PollCycle(model.SOURCE_BRYANT):
            raise SourceAuthError("401 after refresh")


def test_a_clean_context_exit_stamps_exactly_once() -> None:
    with PollCycle(model.SOURCE_LEVITON) as cycle:
        assert isinstance(cycle, PollCycle)
        cycle.add("hub-a", "panel_leg_a", "volts", 241.1)
    assert cycle.finished is True
    assert len(cycle.observations) == 1
    stamp = cycle.ts_utc
    assert stamp is not None
    # Re-entering must not re-stamp the rows.
    with cycle:
        pass
    assert cycle.ts_utc == stamp


def test_explicit_finish_inside_the_block_is_not_repeated_on_exit() -> None:
    with PollCycle(model.SOURCE_LEVITON) as cycle:
        cycle.add("hub-a", "breaker_p1", "watts", 5.0)
        rows = cycle.finish(utc(2026, 8, 16, 18))
    assert cycle.observations is rows
    assert cycle.ts_utc == utc(2026, 8, 16, 18)


def test_an_empty_successful_cycle_is_legal_and_yields_no_rows() -> None:
    """A source that genuinely saw nothing reports nothing — not a zero row."""
    with PollCycle(model.SOURCE_BRYANT) as cycle:
        pass
    assert cycle.observations == []
    assert cycle.finished is True


# ---------------------------------------------------------------------------
# PollCycle: finished cycles are sealed
# ---------------------------------------------------------------------------


def test_adding_to_a_finished_cycle_raises() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.finish()
    with pytest.raises(RuntimeError, match="already finished"):
        cycle.add("hub-a", "breaker_p1", "watts", 1.0)


def test_finishing_twice_raises() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.finish()
    with pytest.raises(RuntimeError, match="twice"):
        cycle.finish()


def test_len_counts_collected_samples_only() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.add("hub-a", "breaker_p1", "watts", 1.0)
    cycle.add("hub-a", "breaker_p2", "watts", None)
    assert len(cycle) == 1
    cycle.finish()
    assert len(cycle) == len(cycle.observations) == 1


# ---------------------------------------------------------------------------
# PollCycle: row construction goes through model.make_observation
# ---------------------------------------------------------------------------


def test_rows_carry_the_cycle_source_and_the_metrics_canonical_unit() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.add("hub-a", "breaker_p11", "watts", 100.0)
    cycle.add("hub-a", "panel_leg_a", "volts", 241.3)
    rows = cycle.finish()
    assert [row.source for row in rows] == [model.SOURCE_LEVITON] * 2
    assert [row.unit for row in rows] == [model.UNIT_WATTS, model.UNIT_VOLTS]
    assert isinstance(rows[0], Observation)


def test_an_explicit_unit_overrides_the_metric_default() -> None:
    cycle = PollCycle(model.SOURCE_BRYANT)
    cycle.add("SYS1", "system", "mode", 2, unit=model.UNIT_ENUM)
    assert cycle.finish()[0].unit == model.UNIT_ENUM


def test_an_unknown_unit_is_rejected_at_finish() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    cycle.add("hub-a", "breaker_p1", "watts", 1.0, unit="furlongs")
    with pytest.raises(ValueError, match="unknown unit"):
        cycle.finish()


def test_interval_s_produces_a_meter_observation() -> None:
    cycle = PollCycle(model.SOURCE_LGE)
    cycle.add("meter-1", "main", "kwh_interval", 0.25, interval_s=900)
    cycle.add("meter-1", "main", "ccf_interval", 0.1)
    rows = cycle.finish()
    assert isinstance(rows[0], model.MeterObservation)
    assert rows[0].interval_s == 900
    assert not isinstance(rows[1], model.MeterObservation)


def test_insertion_order_is_preserved() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON)
    channels = ["breaker_p3", "breaker_p1", "ct_2_a", "panel_leg_b"]
    for channel in channels:
        cycle.add("hub-a", channel, "watts", 1.0)
    assert [row.channel_id for row in cycle.finish()] == channels


# ---------------------------------------------------------------------------
# PollCycle: ts_local is always the local wall clock of ts_utc
# ---------------------------------------------------------------------------


def test_ts_local_is_the_naive_local_wall_clock_of_ts_utc() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON, ts_utc=utc(2026, 8, 16, 18, 0, 30, 123456))
    cycle.add("hub-a", "breaker_p1", "watts", 1.0)
    row = cycle.finish()[0]
    assert row.ts_local == datetime(2026, 8, 16, 14, 0, 30, 123456)  # EDT, UTC-4
    assert row.ts_local.tzinfo is None
    assert row.ts_local == timeutil.to_local_naive(row.ts_utc)


def test_ts_local_derivation_holds_across_the_spring_forward_gap() -> None:
    """2026-03-08: 02:00 local never happens; 07:00Z is 03:00 EDT."""
    cycle = PollCycle(model.SOURCE_LEVITON, ts_utc=utc(2026, 3, 8, 7, 0, 0))
    cycle.add("hub-a", "breaker_p1", "watts", 1.0)
    row = cycle.finish()[0]
    assert row.ts_local == datetime(2026, 3, 8, 3, 0, 0)
    assert timeutil.local_date_of(row.ts_utc) == date(2026, 3, 8)


def test_the_two_fall_back_hours_share_a_wall_clock_but_not_a_ts_utc() -> None:
    """CLAUDE.md rule 3: ts_local is deliberately ambiguous on fall-back."""
    first = PollCycle(model.SOURCE_LEVITON, ts_utc=utc(2026, 11, 1, 5, 30))  # 01:30 EDT
    first.add("hub-a", "breaker_p1", "watts", 100.0)
    second = PollCycle(model.SOURCE_LEVITON, ts_utc=utc(2026, 11, 1, 6, 30))  # 01:30 EST
    second.add("hub-a", "breaker_p1", "watts", 200.0)

    early, late = first.finish()[0], second.finish()[0]
    assert early.ts_local == late.ts_local == datetime(2026, 11, 1, 1, 30)
    assert early.ts_utc != late.ts_utc
    # ts_utc is canonical, so the two survive dedupe as distinct rows.
    assert len(model.dedupe_observations([early, late])) == 2


def test_a_whole_cycle_spanning_the_fall_back_instant_stays_on_one_local_date() -> None:
    cycle = PollCycle(model.SOURCE_LEVITON, ts_utc=utc(2026, 11, 1, 6, 0))
    for position in range(1, 4):
        cycle.add("hub-a", f"breaker_p{position}", "watts", float(position))
    rows = cycle.finish()
    assert {row.ts_local for row in rows} == {datetime(2026, 11, 1, 1, 0)}
    assert {timeutil.local_date_of(row.ts_utc) for row in rows} == {date(2026, 11, 1)}


# ---------------------------------------------------------------------------
# discovery dataclasses
# ---------------------------------------------------------------------------


def _channel(channel_id: str, **kwargs: object) -> DiscoveredChannel:
    return DiscoveredChannel(
        source=model.SOURCE_LEVITON,
        device_id="hub-a",
        channel_id=channel_id,
        kind=str(kwargs.pop("kind", "breaker")),
        **kwargs,  # type: ignore[arg-type]
    )


def test_discovered_channel_key_is_the_dim_channel_join_key() -> None:
    channel = _channel("breaker_p11", label="Kitchen")
    assert channel.key == (model.SOURCE_LEVITON, "hub-a", "breaker_p11")


def test_discovered_dataclasses_are_frozen() -> None:
    channel = _channel("breaker_p11")
    device = DiscoveredDevice(source=model.SOURCE_LEVITON, device_id="hub-a", kind="hub")
    with pytest.raises(Exception):
        channel.channel_id = "breaker_p12"  # type: ignore[misc]
    with pytest.raises(Exception):
        device.device_id = "hub-b"  # type: ignore[misc]


def test_channel_map_entry_leaves_the_human_fields_blank_on_purpose() -> None:
    """§9 makes an entry with neither label nor blackstart id a build error."""
    entry = _channel("breaker_p11").channel_map_entry()
    assert entry == {
        "source": model.SOURCE_LEVITON,
        "device_id": "hub-a",
        "channel_id": "breaker_p11",
        "label": "",
        "blackstart_device_id": "",
    }
    assert _channel("breaker_p12", label="Kitchen").channel_map_entry()["label"] == "Kitchen"


def test_discovery_reports_unmapped_channels_and_never_drops_one_silently() -> None:
    known = _channel("breaker_p1")
    fresh = _channel("breaker_p2")
    discovery = Discovery(source=model.SOURCE_LEVITON, channels=(known, fresh))

    assert discovery.channel_keys() == {known.key, fresh.key}
    assert discovery.unmapped([known.key]) == (fresh,)
    assert discovery.unmapped(()) == (known, fresh)
    assert [entry["channel_id"] for entry in discovery.skeleton([known.key])] == ["breaker_p2"]
    assert discovery.skeleton([known.key, fresh.key]) == []


def test_discovery_defaults_are_empty_and_carry_a_timestamp() -> None:
    discovery = Discovery(source=model.SOURCE_BRYANT)
    assert discovery.devices == () and discovery.channels == ()
    assert discovery.channel_keys() == set()
    assert discovery.ts_utc.tzinfo is not None


# ---------------------------------------------------------------------------
# BackgroundTask
# ---------------------------------------------------------------------------


def test_background_task_defaults_to_running_immediately() -> None:
    async def _noop() -> None:
        return None

    task = BackgroundTask(name="keepalive", interval_s=50.0, run=_noop)
    assert task.initial_delay_s == 0.0
    assert asyncio.run(task.run()) is None


# ---------------------------------------------------------------------------
# BaseSource
# ---------------------------------------------------------------------------


class _FakeSource(BaseSource):
    name = model.SOURCE_LEVITON

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.discover_calls = 0
        self.closed = 0

    async def discover(self, *, force: bool = False) -> Discovery:
        if not force and self.cached_discovery is not None:
            return self.cached_discovery
        self.discover_calls += 1
        return self._remember(
            Discovery(
                source=self.name,
                channels=(_channel(f"breaker_p{self.discover_calls}"),),
            )
        )

    async def poll(self) -> list[Observation]:
        with self.new_cycle() as cycle:
            cycle.add("hub-a", "breaker_p1", "watts", 100.0)
            cycle.add("hub-a", "breaker_p2", "watts", None)
        return cycle.observations


def test_base_source_rejects_a_name_outside_the_vocabulary() -> None:
    class _Bogus(_FakeSource):
        name = "solaredge"

    with pytest.raises(ValueError, match="not one of"):
        _Bogus(poll_interval_s=30)

    class _Unset(_FakeSource):
        name = ""

    with pytest.raises(ValueError, match="not one of"):
        _Unset(poll_interval_s=30)


@pytest.mark.parametrize(
    ("requested", "expected"),
    [(1, MIN_POLL_INTERVAL_S), (29, MIN_POLL_INTERVAL_S), (30, 30), (300, 300)],
)
def test_the_poll_interval_floor_lives_in_code_not_only_in_config(
    requested: int, expected: int
) -> None:
    assert _FakeSource(poll_interval_s=requested).poll_interval_s == expected


def test_discovery_interval_is_optional_and_zero_means_none() -> None:
    assert _FakeSource(poll_interval_s=30).discovery_interval_s is None
    assert _FakeSource(poll_interval_s=30, discovery_interval_s=0).discovery_interval_s is None
    assert _FakeSource(poll_interval_s=30, discovery_interval_s=3600).discovery_interval_s == 3600


def test_start_forces_the_first_discovery_and_caches_it() -> None:
    source = _FakeSource(poll_interval_s=30)
    assert source.cached_discovery is None
    asyncio.run(source.start())
    assert source.discover_calls == 1
    assert source.cached_discovery is not None
    # A non-forced call is served from the cache.
    asyncio.run(source.discover())
    assert source.discover_calls == 1
    asyncio.run(source.discover(force=True))
    assert source.discover_calls == 2


def test_close_is_a_tolerant_no_op_by_default() -> None:
    source = _FakeSource(poll_interval_s=30)
    assert asyncio.run(source.close()) is None
    assert asyncio.run(source.close()) is None


def test_background_tasks_are_empty_without_a_discovery_interval() -> None:
    assert _FakeSource(poll_interval_s=30).background_tasks() == ()


def test_the_rediscovery_task_is_scheduled_and_forces_a_refresh() -> None:
    source = _FakeSource(poll_interval_s=30, discovery_interval_s=3600)
    (task,) = source.background_tasks()
    assert task.name == f"{model.SOURCE_LEVITON}_discovery"
    assert task.interval_s == 3600.0
    # Never immediately: start() has just discovered.
    assert task.initial_delay_s == 3600.0

    asyncio.run(source.start())
    assert source.discover_calls == 1
    asyncio.run(task.run())
    assert source.discover_calls == 2
    assert source.cached_discovery.channels[0].channel_id == "breaker_p2"


def test_new_cycle_binds_the_sources_name_and_accepts_a_pinned_stamp() -> None:
    source = _FakeSource(poll_interval_s=30)
    assert source.new_cycle().source == model.SOURCE_LEVITON
    pinned = source.new_cycle(ts_utc=utc(2026, 8, 16, 18))
    assert pinned.ts_utc == utc(2026, 8, 16, 18)


def test_a_base_source_poll_returns_stamped_rows_with_gaps_omitted() -> None:
    rows = asyncio.run(_FakeSource(poll_interval_s=30).poll())
    assert [row.channel_id for row in rows] == ["breaker_p1"]
    assert len({row.ts_utc for row in rows}) == 1


def test_base_source_cannot_be_instantiated_without_poll_and_discover() -> None:
    class _Incomplete(BaseSource):
        name = model.SOURCE_BRYANT

    with pytest.raises(TypeError):
        _Incomplete(poll_interval_s=30)  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# the Source protocol
# ---------------------------------------------------------------------------


def test_a_base_source_subclass_satisfies_the_structural_protocol() -> None:
    assert isinstance(_FakeSource(poll_interval_s=30), Source)


def test_an_object_missing_a_protocol_member_does_not_satisfy_it() -> None:
    class _NotASource:
        name = model.SOURCE_LEVITON
        poll_interval_s = 30

        async def poll(self) -> list[Observation]:
            return []

    assert not isinstance(_NotASource(), Source)
