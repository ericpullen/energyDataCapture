"""DST and partition-date correctness — PLAN.md §15.3, CLAUDE.md rules 3 and 4.

This is the correctness contract for time in this project. Everything else
(partition paths, part-file names, rollup buckets, the daily ``ts_utc``) is
derived from these functions, so a regression here silently misfiles data rather
than crashing.

The zone is ``America/Kentucky/Louisville`` (US Eastern rules). The reference
transitions used throughout:

===========  ===========================  =========================
local day    what happens at 02:00 local  length of the local day
===========  ===========================  =========================
2026-03-08   EST -> EDT, 02:00 skipped     23 h (05:00Z -> 04:00Z+1d)
2026-11-01   EDT -> EST, 01:00 repeats     25 h (04:00Z -> 05:00Z+1d)
===========  ===========================  =========================

Other years are parametrised in as well, so a hardcoded 2026 constant anywhere
in ``timeutil`` would fail the suite.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from energy_capture import timeutil
from energy_capture.timeutil import UTC
from tests.conftest import LOCAL_TZ, naive, utc

# --------------------------------------------------------------------------
# Reference days
# --------------------------------------------------------------------------

SPRING_FORWARD = date(2026, 3, 8)  # 23 local hours; wall hour 02 does not exist
FALL_BACK = date(2026, 11, 1)  # 25 local hours; wall hour 01 happens twice
NORMAL_SUMMER = date(2026, 8, 16)  # EDT, UTC-4
NORMAL_WINTER = date(2026, 1, 15)  # EST, UTC-5

#: Nothing may be hardcoded to 2026.
OTHER_SPRING_FORWARD = [date(2025, 3, 9), date(2027, 3, 14), date(2028, 3, 12)]
OTHER_FALL_BACK = [date(2025, 11, 2), date(2027, 11, 7), date(2028, 11, 5)]

#: The two instants that render as the same wall clock on the fall-back day.
FIRST_0130 = utc(2026, 11, 1, 5, 30)  # 01:30 EDT (UTC-4), the first pass
SECOND_0130 = utc(2026, 11, 1, 6, 30)  # 01:30 EST (UTC-5), one hour later


# --------------------------------------------------------------------------
# The configured zone
# --------------------------------------------------------------------------


def test_tz_name_comes_from_settings() -> None:
    assert timeutil.tz_name() == LOCAL_TZ
    assert str(timeutil.local_tz()) == LOCAL_TZ


def test_local_tz_argument_overrides_settings() -> None:
    """Every helper takes ``tz=`` so tests (and future multi-site use) can pin it."""
    assert str(timeutil.local_tz("UTC")) == "UTC"
    assert timeutil.local_date_of(utc(2026, 1, 15, 4, 30), tz="UTC") == date(2026, 1, 15)
    assert timeutil.local_date_of(utc(2026, 1, 15, 4, 30)) == date(2026, 1, 14)


# --------------------------------------------------------------------------
# §15.3 — 23-hour and 25-hour local days
# --------------------------------------------------------------------------


def test_spring_forward_day_has_23_local_hours() -> None:
    assert timeutil.local_hours_in_day(SPRING_FORWARD) == 23
    assert len(list(timeutil.iter_local_hours(SPRING_FORWARD))) == 23


def test_fall_back_day_has_25_local_hours() -> None:
    assert timeutil.local_hours_in_day(FALL_BACK) == 25
    assert len(list(timeutil.iter_local_hours(FALL_BACK))) == 25


@pytest.mark.parametrize("local_day", [NORMAL_SUMMER, NORMAL_WINTER, date(2026, 3, 7), date(2026, 11, 2)])
def test_ordinary_day_has_24_local_hours(local_day: date) -> None:
    assert timeutil.local_hours_in_day(local_day) == 24


@pytest.mark.parametrize("local_day", OTHER_SPRING_FORWARD)
def test_spring_forward_is_not_hardcoded_to_2026(local_day: date) -> None:
    assert timeutil.local_hours_in_day(local_day) == 23
    assert 2 not in timeutil.local_wall_hours_of_day(local_day)


@pytest.mark.parametrize("local_day", OTHER_FALL_BACK)
def test_fall_back_is_not_hardcoded_to_2026(local_day: date) -> None:
    assert timeutil.local_hours_in_day(local_day) == 25
    ambiguous = [h for h in timeutil.iter_local_hours(local_day) if h.ambiguous]
    assert len(ambiguous) == 2
    assert {h.local_start.hour for h in ambiguous} == {1}


@pytest.mark.parametrize(
    ("tz", "local_day", "expected_hours"),
    [
        ("UTC", SPRING_FORWARD, 24),  # no DST at all
        ("UTC", FALL_BACK, 24),
        ("America/Denver", SPRING_FORWARD, 23),  # same US rules, different offset
        ("America/Denver", FALL_BACK, 25),
    ],
)
def test_hour_count_follows_the_requested_zone(tz: str, local_day: date, expected_hours: int) -> None:
    assert timeutil.local_hours_in_day(local_day, tz=tz) == expected_hours


def test_iter_local_hours_is_contiguous_and_indexed() -> None:
    """The hours of a day tile ``[midnight, midnight)`` exactly, with no overlap."""
    for local_day in (SPRING_FORWARD, FALL_BACK, NORMAL_SUMMER):
        hours = list(timeutil.iter_local_hours(local_day))
        start, end = timeutil.local_day_bounds_utc(local_day)

        assert [h.index for h in hours] == list(range(len(hours)))
        assert hours[0].start_utc == start
        assert hours[-1].end_utc == end
        # Deliberately not strict=True: pairing a list with its own tail always
        # leaves one element unmatched. Every adjacent pair is still checked.
        for previous, current in zip(hours, hours[1:]):
            assert previous.end_utc == current.start_utc
        # Every US DST shift is a whole hour, so every bucket is exactly 1h.
        assert all(h.end_utc - h.start_utc == timedelta(hours=1) for h in hours)
        # UTC starts are unique even when local starts are not.
        assert len({h.start_utc for h in hours}) == len(hours)


def test_utc_hour_start_is_the_bucket_key_for_every_local_hour() -> None:
    """Bucketing on UTC yields exactly the local hours — the rollup's premise."""
    for local_day in (SPRING_FORWARD, FALL_BACK, NORMAL_WINTER):
        for hour in timeutil.iter_local_hours(local_day):
            assert timeutil.utc_hour_start(hour.start_utc) == hour.start_utc
            midway = hour.start_utc + timedelta(minutes=37, seconds=12, microseconds=9)
            assert timeutil.utc_hour_start(midway) == hour.start_utc


# --------------------------------------------------------------------------
# §15.3 — the wall-clock hour that does not exist / the one that repeats
# --------------------------------------------------------------------------


def test_spring_forward_drops_wall_hour_02() -> None:
    wall_hours = timeutil.local_wall_hours_of_day(SPRING_FORWARD)
    assert wall_hours == [0, 1] + list(range(3, 24))
    assert len(wall_hours) == 23
    with pytest.raises(ValueError, match="does not exist"):
        timeutil.local_hour_bounds_utc(SPRING_FORWARD, 2)


def test_spring_forward_hours_either_side_of_the_gap() -> None:
    assert timeutil.local_hour_bounds_utc(SPRING_FORWARD, 1) == (
        utc(2026, 3, 8, 6),
        utc(2026, 3, 8, 7),
    )
    assert timeutil.local_hour_bounds_utc(SPRING_FORWARD, 3) == (
        utc(2026, 3, 8, 7),
        utc(2026, 3, 8, 8),
    )


def test_fall_back_lists_24_wall_hours_but_hour_01_is_two_hours_wide() -> None:
    """One wall-hour label ``01`` -> one ``part-20261101T01.parquet`` holding 2 h."""
    assert timeutil.local_wall_hours_of_day(FALL_BACK) == list(range(24))
    start, end = timeutil.local_hour_bounds_utc(FALL_BACK, 1)
    assert (start, end) == (utc(2026, 11, 1, 5), utc(2026, 11, 1, 7))
    assert end - start == timedelta(hours=2)
    # Both ambiguous instants fall inside that single part file's span.
    assert start <= FIRST_0130 < end
    assert start <= SECOND_0130 < end


@pytest.mark.parametrize("wall_hour", [0, 2, 3, 23])
def test_fall_back_other_hours_are_one_hour_wide(wall_hour: int) -> None:
    start, end = timeutil.local_hour_bounds_utc(FALL_BACK, wall_hour)
    assert end - start == timedelta(hours=1)


# --------------------------------------------------------------------------
# §15.3 — the ambiguous hour: same ts_local, distinct ts_utc
# --------------------------------------------------------------------------


def test_two_distinct_instants_share_one_naive_local_wall_clock() -> None:
    """PLAN.md §2.4: ``ts_local`` is deliberately ambiguous during fall-back."""
    assert FIRST_0130 != SECOND_0130
    assert SECOND_0130 - FIRST_0130 == timedelta(hours=1)

    assert timeutil.to_local_naive(FIRST_0130) == naive(2026, 11, 1, 1, 30)
    assert timeutil.to_local_naive(SECOND_0130) == naive(2026, 11, 1, 1, 30)
    assert timeutil.to_local_naive(FIRST_0130) == timeutil.to_local_naive(SECOND_0130)

    # The aware forms differ — the offset is what distinguishes them.
    assert timeutil.to_local(FIRST_0130).utcoffset() == timedelta(hours=-4)  # EDT
    assert timeutil.to_local(SECOND_0130).utcoffset() == timedelta(hours=-5)  # EST


def test_ambiguous_hour_buckets_by_ts_utc_without_loss() -> None:
    """CLAUDE.md rule 3: bucketing on ``ts_utc`` keeps the repeat distinct."""
    instants = [FIRST_0130, SECOND_0130]

    by_utc_hour = {timeutil.utc_hour_start(ts) for ts in instants}
    assert by_utc_hour == {utc(2026, 11, 1, 5), utc(2026, 11, 1, 6)}
    assert len(by_utc_hour) == 2, "grouping on ts_utc must not merge the two 01:00 hours"

    # ...whereas grouping on the readable local hour would lose one of them.
    # This is exactly why HOURLY_SORT_KEY leads with hour_start_utc.
    by_local_hour = {timeutil.local_hour_start(ts) for ts in instants}
    assert by_local_hour == {naive(2026, 11, 1, 1, 0)}
    assert len(by_local_hour) == 1


def test_iter_local_hours_marks_both_01_hours_ambiguous() -> None:
    hours = list(timeutil.iter_local_hours(FALL_BACK))
    ambiguous = [h for h in hours if h.ambiguous]

    assert len(ambiguous) == 2
    assert [h.local_start for h in ambiguous] == [naive(2026, 11, 1, 1), naive(2026, 11, 1, 1)]
    assert [h.start_utc for h in ambiguous] == [utc(2026, 11, 1, 5), utc(2026, 11, 1, 6)]
    assert ambiguous[0].end_utc == ambiguous[1].start_utc
    # Every other hour of the day is unambiguous.
    assert sum(1 for h in hours if not h.ambiguous) == 23


def test_no_hour_is_ambiguous_on_ordinary_or_spring_forward_days() -> None:
    for local_day in (NORMAL_SUMMER, NORMAL_WINTER, SPRING_FORWARD):
        assert not any(h.ambiguous for h in timeutil.iter_local_hours(local_day))


def test_local_naive_to_utc_fold_resolves_the_ambiguity() -> None:
    """The inverse is not a function; ``fold`` picks which occurrence you mean."""
    wall = naive(2026, 11, 1, 1, 30)
    assert timeutil.local_naive_to_utc(wall, fold=0) == FIRST_0130
    assert timeutil.local_naive_to_utc(wall, fold=1) == SECOND_0130


def test_local_naive_to_utc_rejects_an_aware_datetime() -> None:
    with pytest.raises(ValueError, match="naive"):
        timeutil.local_naive_to_utc(utc(2026, 11, 1, 5, 30))


def test_ambiguous_instants_share_one_part_file_and_one_partition() -> None:
    """Both occurrences land in ``.../day=01/part-20261101T01.parquet``."""
    for instant in (FIRST_0130, SECOND_0130):
        assert timeutil.partition_parts(instant) == ("2026", "11", "01")
        assert timeutil.local_hour_stamp(instant) == ("20261101", "01")
        assert timeutil.local_hour_key(instant) == "2026-11-01T01"


# --------------------------------------------------------------------------
# §15.3 — partition date assignment around the 02:00 transitions
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("instant", "expected_local", "expected_date"),
    [
        # Spring forward: 06:59:59.999999Z is the last instant of 01:59 EST;
        # one microsecond later the wall clock jumps straight to 03:00 EDT.
        (utc(2026, 3, 8, 6, 59, 59, 999999), naive(2026, 3, 8, 1, 59, 59, 999999), SPRING_FORWARD),
        (utc(2026, 3, 8, 7, 0, 0), naive(2026, 3, 8, 3, 0), SPRING_FORWARD),
        # Fall back: 05:59:59.999999Z ends the first 01:59 (EDT); the next
        # instant is 01:00 again (EST).
        (utc(2026, 11, 1, 5, 59, 59, 999999), naive(2026, 11, 1, 1, 59, 59, 999999), FALL_BACK),
        (utc(2026, 11, 1, 6, 0, 0), naive(2026, 11, 1, 1, 0), FALL_BACK),
    ],
)
def test_partition_date_across_the_02_00_transitions(
    instant: datetime, expected_local: datetime, expected_date: date
) -> None:
    assert timeutil.to_local_naive(instant) == expected_local
    assert timeutil.local_date_of(instant) == expected_date
    year, month, day = timeutil.partition_parts(instant)
    assert (year, month, day) == (
        f"{expected_date.year:04d}",
        f"{expected_date.month:02d}",
        f"{expected_date.day:02d}",
    )


def test_transition_instants_stay_within_their_local_day_bounds() -> None:
    for local_day in (SPRING_FORWARD, FALL_BACK):
        start, end = timeutil.local_day_bounds_utc(local_day)
        assert timeutil.local_date_of(start) == local_day
        assert timeutil.local_date_of(end - timedelta(microseconds=1)) == local_day
        assert timeutil.local_date_of(end) == local_day + timedelta(days=1)
        assert timeutil.local_date_of(start - timedelta(microseconds=1)) == local_day - timedelta(days=1)


@pytest.mark.parametrize(
    ("instant", "expected_date", "why"),
    [
        # EST (UTC-5): 04:30Z is 23:30 the PREVIOUS local day.
        (utc(2026, 1, 15, 4, 30), date(2026, 1, 14), "winter 04:30Z is still yesterday"),
        (utc(2026, 1, 15, 5, 0), date(2026, 1, 15), "winter local midnight is 05:00Z"),
        # EDT (UTC-4): 04:30Z is 00:30 the SAME local day, but 03:30Z is not.
        (utc(2026, 7, 15, 4, 30), date(2026, 7, 15), "summer 04:30Z is already today"),
        (utc(2026, 7, 15, 3, 30), date(2026, 7, 14), "summer 03:30Z is still yesterday"),
        # The day before spring forward is EST; the day after fall back is EST.
        (utc(2026, 3, 8, 4, 30), date(2026, 3, 7), "04:30Z on spring-forward day is D-1"),
        (utc(2026, 11, 2, 4, 30), date(2026, 11, 1), "04:30Z after fall back is D-1"),
        # ...but 04:30Z on the fall-back day itself (still EDT) is that day.
        (utc(2026, 11, 1, 4, 30), FALL_BACK, "04:30Z on fall-back day is already today"),
    ],
)
def test_partition_date_around_local_midnight(instant: datetime, expected_date: date, why: str) -> None:
    assert timeutil.local_date_of(instant) == expected_date, why


def test_no_sample_on_the_spring_forward_day_ever_reports_wall_hour_02() -> None:
    """A 30s sweep of the real day: 23 h x 120 samples, and hour 02 never appears."""
    start, end = timeutil.local_day_bounds_utc(SPRING_FORWARD)
    samples = []
    cursor = start
    while cursor < end:
        samples.append(cursor)
        cursor += timedelta(seconds=30)

    assert len(samples) == 23 * 120
    assert 2 not in {timeutil.to_local_naive(ts).hour for ts in samples}
    # Every sample partitions to the one local date, and to 23 hour buckets —
    # by UTC and by local label alike, since nothing repeats on this day.
    assert {timeutil.local_date_of(ts) for ts in samples} == {SPRING_FORWARD}
    assert len({timeutil.utc_hour_start(ts) for ts in samples}) == 23
    assert len({timeutil.local_hour_start(ts) for ts in samples}) == 23


def test_fall_back_day_sweep_has_25_utc_buckets_but_24_local_hour_labels() -> None:
    """The heart of §15.3: 25 rollup buckets, 24 readable labels, no rows lost."""
    start, end = timeutil.local_day_bounds_utc(FALL_BACK)
    samples = []
    cursor = start
    while cursor < end:
        samples.append(cursor)
        cursor += timedelta(seconds=30)

    assert len(samples) == 25 * 120
    assert {timeutil.local_date_of(ts) for ts in samples} == {FALL_BACK}
    assert len({timeutil.utc_hour_start(ts) for ts in samples}) == 25
    assert len({timeutil.local_hour_start(ts) for ts in samples}) == 24
    # 240 samples share the ambiguous 01:00 label — two full hours of data that
    # a local-hour GROUP BY would have merged into one bucket.
    ambiguous = [ts for ts in samples if timeutil.local_hour_start(ts) == naive(2026, 11, 1, 1)]
    assert len(ambiguous) == 240
    assert len({timeutil.utc_hour_start(ts) for ts in ambiguous}) == 2


# --------------------------------------------------------------------------
# §15.3 — local midnight -> UTC on both transition days
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("local_day", "expected_midnight_utc"),
    [
        (SPRING_FORWARD, utc(2026, 3, 8, 5)),  # EST, UTC-5, before the 02:00 shift
        (SPRING_FORWARD + timedelta(days=1), utc(2026, 3, 9, 4)),  # EDT, UTC-4
        (FALL_BACK, utc(2026, 11, 1, 4)),  # EDT, UTC-4, before the 02:00 shift
        (FALL_BACK + timedelta(days=1), utc(2026, 11, 2, 5)),  # EST, UTC-5
        (NORMAL_SUMMER, utc(2026, 8, 16, 4)),
        (NORMAL_WINTER, utc(2026, 1, 15, 5)),
        (date(2025, 3, 9), utc(2025, 3, 9, 5)),
        (date(2027, 11, 7), utc(2027, 11, 7, 4)),
    ],
)
def test_local_midnight_to_utc(local_day: date, expected_midnight_utc: datetime) -> None:
    """``ts_utc`` of an ``energy/daily`` row (PLAN.md §7.2). The offset differs."""
    midnight = timeutil.local_midnight_utc(local_day)
    assert midnight == expected_midnight_utc
    assert midnight.tzinfo is not None
    # Round-trips: local midnight is never skipped or repeated (DST is at 02:00).
    assert timeutil.to_local_naive(midnight) == timeutil.local_midnight_naive(local_day)
    assert timeutil.local_date_of(midnight) == local_day


def test_local_midnight_naive_is_tz_naive() -> None:
    ts_local = timeutil.local_midnight_naive(FALL_BACK)
    assert ts_local == naive(2026, 11, 1, 0, 0)
    assert ts_local.tzinfo is None


@pytest.mark.parametrize(
    ("local_day", "expected_hours"),
    [(SPRING_FORWARD, 23), (FALL_BACK, 25), (NORMAL_SUMMER, 24), (NORMAL_WINTER, 24)],
)
def test_local_day_bounds_span_the_real_day_length(local_day: date, expected_hours: int) -> None:
    start, end = timeutil.local_day_bounds_utc(local_day)
    assert start == timeutil.local_midnight_utc(local_day)
    assert end == timeutil.local_midnight_utc(local_day + timedelta(days=1))
    assert end - start == timedelta(hours=expected_hours)


# --------------------------------------------------------------------------
# Microsecond precision
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "instant",
    [
        utc(2026, 8, 16, 18, 0, 30, 123456),  # ordinary EDT
        utc(2026, 1, 15, 18, 0, 30, 1),  # ordinary EST, 1 microsecond
        utc(2026, 3, 8, 7, 0, 0, 999999),  # first instant of 03:00 EDT
        FIRST_0130.replace(microsecond=500000),  # inside the ambiguous hour
        SECOND_0130.replace(microsecond=500000),
    ],
)
def test_microseconds_survive_every_conversion(instant: datetime) -> None:
    assert timeutil.ensure_utc(instant).microsecond == instant.microsecond
    assert timeutil.to_local(instant).microsecond == instant.microsecond

    ts_local = timeutil.to_local_naive(instant)
    assert ts_local.microsecond == instant.microsecond
    assert ts_local.tzinfo is None

    # ts_local -> ts_utc round-trips (choosing the right fold for the repeat).
    fold = 1 if instant == SECOND_0130.replace(microsecond=500000) else 0
    assert timeutil.local_naive_to_utc(ts_local, fold=fold) == instant


def test_format_utc_keeps_microseconds_and_uses_a_z_suffix() -> None:
    assert timeutil.format_utc(utc(2026, 8, 16, 18, 0, 30, 123456)) == "2026-08-16T18:00:30.123456Z"
    # A whole second still prints six digits — a stable width for log parsing.
    assert timeutil.format_utc(utc(2026, 8, 16, 18, 0, 30)) == "2026-08-16T18:00:30.000000Z"
    # A naive datetime is assumed to already be UTC.
    assert timeutil.format_utc(datetime(2026, 8, 16, 18, 0, 30, 7)) == "2026-08-16T18:00:30.000007Z"


def test_ensure_utc_normalises_naive_and_offset_aware_input() -> None:
    naive_ts = datetime(2026, 8, 16, 18, 0, 30, 123456)
    assert timeutil.ensure_utc(naive_ts) == utc(2026, 8, 16, 18, 0, 30, 123456)
    assert timeutil.ensure_utc(naive_ts).tzinfo is UTC

    eastern = timeutil.to_local(utc(2026, 8, 16, 18, 0, 30))
    assert timeutil.ensure_utc(eastern) == utc(2026, 8, 16, 18, 0, 30)
    assert timeutil.ensure_utc(eastern).utcoffset() == timedelta(0)


def test_local_hour_start_truncates_below_the_hour_only() -> None:
    ts = utc(2026, 8, 16, 18, 47, 12, 999999)
    assert timeutil.local_hour_start(ts) == naive(2026, 8, 16, 14, 0)
    assert timeutil.utc_hour_start(ts) == utc(2026, 8, 16, 18, 0)


# --------------------------------------------------------------------------
# Partition / file-name stamps and date-range expansion
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        (utc(2026, 3, 8, 7, 0), ("2026", "03", "08")),  # zero padded
        (utc(2026, 11, 1, 6, 30), ("2026", "11", "01")),
        (utc(2026, 1, 15, 4, 30), ("2026", "01", "14")),  # previous local day
    ],
)
def test_partition_parts_are_zero_padded_local_dates(
    instant: datetime, expected: tuple[str, str, str]
) -> None:
    assert timeutil.partition_parts(instant) == expected
    assert timeutil.partition_parts_for_local_date(timeutil.local_date_of(instant)) == expected


def test_local_hour_stamp_and_key_use_local_wall_clock() -> None:
    ts = utc(2026, 8, 16, 18, 47, 12)
    assert timeutil.local_hour_stamp(ts) == ("20260816", "14")
    assert timeutil.local_hour_key(ts) == "2026-08-16T14"
    # An instant just after local midnight in winter belongs to the previous day.
    assert timeutil.local_hour_stamp(utc(2026, 1, 15, 4, 30)) == ("20260114", "23")


def test_iter_local_dates_is_inclusive_and_covers_transition_days() -> None:
    days = list(timeutil.iter_local_dates(date(2026, 3, 7), date(2026, 3, 9)))
    assert days == [date(2026, 3, 7), SPRING_FORWARD, date(2026, 3, 9)]
    assert list(timeutil.iter_local_dates(FALL_BACK, FALL_BACK)) == [FALL_BACK]
    # A 23-hour day is still exactly one day in a range expansion.
    assert len(list(timeutil.iter_local_dates(date(2026, 3, 1), date(2026, 3, 31)))) == 31


def test_iter_local_dates_rejects_an_inverted_range() -> None:
    with pytest.raises(ValueError, match="before start"):
        list(timeutil.iter_local_dates(date(2026, 3, 9), date(2026, 3, 7)))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-11-01", FALL_BACK),
        ("  2026-11-01  ", FALL_BACK),
        (FALL_BACK, FALL_BACK),
        (datetime(2026, 11, 1, 13, 45), FALL_BACK),
    ],
)
def test_parse_local_date(value: object, expected: date) -> None:
    assert timeutil.parse_local_date(value) == expected  # type: ignore[arg-type]


def test_parse_local_date_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        timeutil.parse_local_date("11/01/2026")


def test_now_utc_is_aware_and_utc() -> None:
    now = timeutil.now_utc()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)
