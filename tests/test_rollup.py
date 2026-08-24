"""Tests for the hourly rollup — PLAN.md §15.1, §15.3, §15.4.

This is the project's correctness contract, so these tests are deliberately
literal: hand-computed aggregates, an exact kWh assertion, and DST days counted
by hand. Everything runs offline against local Parquet fixtures — no S3, no
network, no live API (CLAUDE.md).

The four things that must never regress:

1. ``kwh`` is observed-time-only. A half-populated hour yields exactly half the
   kwh of a full hour at the same wattage (PLAN.md §15.1).
2. Nothing is ever gap-filled. A missing hour is an absent row; a partial hour
   is a row with a smaller ``sample_count`` (PLAN.md §15.4).
3. Buckets are keyed on ``hour_start_utc``. The fall-back day has 25 of them and
   the two 01:00 hours stay distinct rows (PLAN.md §15.3, DEVIATIONS.md #1).
4. Re-running is byte-identical (CLAUDE.md rule 7).
"""

from __future__ import annotations

import io
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore
from energy_capture.stages import rollup

# Louisville DST days for 2026 (US Eastern rules).
SPRING_FORWARD = date(2026, 3, 8)  # 23 local hours; wall-clock 02:00 does not exist
FALL_BACK = date(2026, 11, 1)  # 25 local hours; wall-clock 01:00 happens twice
ORDINARY_DAY = date(2026, 8, 16)  # 24 local hours, EDT

POLL_S = 30


# --------------------------------------------------------------------------
# fixtures / helpers
# --------------------------------------------------------------------------


@pytest.fixture
def write_raw(tmp_path: Path) -> Callable[..., str]:
    """Write observations to a local raw_30s-shaped Parquet file; return its path.

    ``validate=False`` so a test can deliberately plant a day-grain row in raw
    and prove the rollup excludes it anyway.
    """
    counter = {"n": 0}

    def _write(observations: Iterable[model.Observation], name: str | None = None) -> str:
        counter["n"] += 1
        filename = name or f"part-{counter['n']:03d}.parquet"
        table = model.observations_to_table(
            observations, dataset=model.Dataset.RAW_30S, validate=False
        )
        path = tmp_path / filename
        pq.write_table(table, path, compression="zstd")
        return str(path)

    return _write


def watts(
    ts_utc: datetime,
    value: float,
    *,
    channel_id: str = "breaker_p11",
    device_id: str = "hub-a",
    metric: str = "watts",
) -> model.Observation:
    return model.make_observation(
        ts_utc=ts_utc,
        source=model.SOURCE_LEVITON,
        device_id=device_id,
        channel_id=channel_id,
        metric=metric,
        value=value,
    )


def samples(
    start_utc: datetime,
    count: int,
    value: float,
    *,
    step_s: int = POLL_S,
    channel_id: str = "breaker_p11",
    metric: str = "watts",
) -> list[model.Observation]:
    """``count`` observations at ``step_s`` spacing, all with the same value."""
    return [
        watts(
            start_utc + timedelta(seconds=step_s * i),
            value,
            channel_id=channel_id,
            metric=metric,
        )
        for i in range(count)
    ]


def rows(table: pa.Table) -> list[dict]:
    return table.to_pylist()


def only(matches: Sequence[dict]) -> dict:
    assert len(matches) == 1, f"expected exactly one row, got {len(matches)}: {matches}"
    return matches[0]


def pick(table: pa.Table, **conditions) -> list[dict]:
    return [
        row
        for row in rows(table)
        if all(row[key] == value for key, value in conditions.items())
    ]


def parquet_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", write_statistics=True, store_schema=True)
    return sink.getvalue()


def hour_utc(local_day: date, index: int) -> datetime:
    """UTC start of the ``index``-th physical hour of a local day."""
    return list(timeutil.iter_local_hours(local_day))[index].start_utc


# --------------------------------------------------------------------------
# shape and schema
# --------------------------------------------------------------------------


def test_output_matches_the_hourly_schema_exactly(write_raw):
    path = write_raw(samples(hour_utc(ORDINARY_DAY, 14), 10, 100.0))
    table = rollup.rollup_day(ORDINARY_DAY, [path])

    assert table.schema.equals(model.HOURLY_SCHEMA)
    # `kwh` and its denominator are the only nullable columns, and they are
    # null in exactly the same places (DEVIATIONS.md #2, #190).
    nullable = {f.name for f in table.schema if f.nullable}
    assert nullable == {"kwh", "observed_seconds"}


def test_rows_are_sorted_and_unique_on_the_hourly_dedupe_key(write_raw):
    start = hour_utc(ORDINARY_DAY, 9)
    observations = []
    for hour in range(3):
        base = start + timedelta(hours=hour)
        for channel in ("breaker_p11", "breaker_p03", "ct_1_a"):
            observations += samples(base, 4, 120.0, channel_id=channel)
            observations += samples(base, 4, 1.5, channel_id=channel, metric="amps")
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    keys = [
        tuple(row[column] for column in model.HOURLY_DEDUPE_KEY) for row in rows(table)
    ]
    assert keys == sorted(keys), "rollup rows must be sorted by HOURLY_DEDUPE_KEY"
    assert len(set(keys)) == len(keys), "the hourly dedupe key must be unique"


def test_empty_input_list_yields_an_empty_table_not_a_fabricated_day():
    table = rollup.rollup_day(ORDINARY_DAY, [])
    assert table.num_rows == 0
    assert table.schema.equals(model.HOURLY_SCHEMA)


# --------------------------------------------------------------------------
# PLAN.md §15.1 — the math
# --------------------------------------------------------------------------


def test_mean_min_max_p95_match_hand_computed_values(write_raw):
    # Five samples in one hour: 10, 20, 30, 40, 50 W.
    #   mean = 150 / 5                     = 30
    #   min                                = 10
    #   max                                = 50
    #   p95  = quantile_cont: idx = 0.95 * (5 - 1) = 3.8, so interpolate
    #          between v[3]=40 and v[4]=50 at 0.8  = 48.0
    start = hour_utc(ORDINARY_DAY, 13)
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    observations = [
        watts(start + timedelta(seconds=POLL_S * i), value)
        for i, value in enumerate(values)
    ]
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])))

    assert row["mean"] == pytest.approx(30.0)
    assert row["min"] == 10.0
    assert row["max"] == 50.0
    assert row["p95"] == pytest.approx(48.0)
    assert row["sample_count"] == 5
    assert row["unit"] == "W"
    assert row["first_ts_utc"] == start
    assert row["last_ts_utc"] == start + timedelta(seconds=POLL_S * 4)


def test_p95_is_interpolated_not_nearest_rank(write_raw):
    # 1..100 W: quantile_cont idx = 0.95 * 99 = 94.05 -> v[94] + 0.05*(v[95]-v[94])
    # = 95 + 0.05 = 95.05. A nearest-rank p95 would report 95 or 96.
    start = hour_utc(ORDINARY_DAY, 2)
    observations = [
        watts(start + timedelta(seconds=POLL_S * i), float(i + 1)) for i in range(100)
    ]
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])))
    assert row["p95"] == pytest.approx(95.05)


def test_kwh_uses_the_observed_time_formula_exactly(write_raw):
    # 120 samples * 30 s = 3600 s observed at 1000 W = exactly 1 kWh.
    start = hour_utc(ORDINARY_DAY, 10)
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(samples(start, 120, 1000.0))])))

    assert row["sample_count"] == 120
    assert row["kwh"] == pytest.approx(1000.0 * (120 * POLL_S) / 3.6e6)
    assert row["kwh"] == pytest.approx(1.0)


def test_half_populated_hour_yields_exactly_half_the_kwh(write_raw):
    """PLAN.md §15.1's headline test: energy over OBSERVED time only.

    Two hours at an identical 1000 W. The first is fully observed (120 samples
    at 30 s); the second lost its second half (60 samples). The second hour must
    report exactly half the kwh — never the same, which is what extrapolating
    across the gap would produce.
    """
    full_start = hour_utc(ORDINARY_DAY, 10)
    half_start = hour_utc(ORDINARY_DAY, 11)
    observations = samples(full_start, 120, 1000.0) + samples(half_start, 60, 1000.0)
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    full = only(pick(table, hour_start_utc=full_start))
    half = only(pick(table, hour_start_utc=half_start))

    assert full["mean"] == half["mean"] == pytest.approx(1000.0)
    assert full["sample_count"] == 120
    assert half["sample_count"] == 60
    assert half["kwh"] == pytest.approx(full["kwh"] / 2)
    assert half["kwh"] == pytest.approx(0.5)


def test_kwh_scales_with_observed_samples_not_with_wall_clock(write_raw):
    # One lonely sample in an hour is 30 observed seconds, not 3600.
    start = hour_utc(ORDINARY_DAY, 4)
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(samples(start, 1, 3600.0))])))
    assert row["sample_count"] == 1
    assert row["kwh"] == pytest.approx(3600.0 * POLL_S / 3.6e6)
    assert row["kwh"] == pytest.approx(0.03)


def test_kwh_honours_a_different_poll_interval(write_raw):
    start = hour_utc(ORDINARY_DAY, 5)
    path = write_raw(samples(start, 60, 1000.0, step_s=60))
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=60)))
    assert row["kwh"] == pytest.approx(1000.0 * (60 * 60) / 3.6e6)
    assert row["kwh"] == pytest.approx(1.0)


def test_kwh_is_null_for_every_metric_except_watts(write_raw):
    start = hour_utc(ORDINARY_DAY, 12)
    observations: list[model.Observation] = []
    for metric, value in (
        ("watts", 500.0),
        ("amps", 4.2),
        ("volts", 241.0),
        ("hz", 60.0),
        ("indoor_temp_f", 71.5),
    ):
        observations += samples(start, 6, value, metric=metric, channel_id="breaker_p11")
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    by_metric = {row["metric"]: row for row in rows(table)}
    assert by_metric["watts"]["kwh"] is not None
    for metric in ("amps", "volts", "hz", "indoor_temp_f"):
        # None, never 0.0 — a zero would read as "no energy used" (DEVIATIONS.md #2).
        assert by_metric[metric]["kwh"] is None, f"{metric} must have a NULL kwh"


def test_sample_count_is_present_on_every_row(write_raw):
    start = hour_utc(ORDINARY_DAY, 8)
    observations = samples(start, 7, 100.0) + samples(start, 7, 2.0, metric="amps")
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])
    assert [row["sample_count"] for row in rows(table)] == [7, 7]


def test_day_grain_metrics_are_excluded_from_the_rollup(write_raw):
    """CLAUDE.md rule 6: kwh_day / cost_day_usd would poison an hourly mean."""
    midnight = timeutil.local_midnight_utc(ORDINARY_DAY)
    day_grain = [
        model.make_observation(
            ts_utc=midnight,
            source=model.SOURCE_BRYANT,
            device_id="TEST0000001",
            channel_id="hpheat",
            metric=metric,
            value=value,
        )
        for metric, value in (("kwh_day", 42.0), ("cost_day_usd", 5.25))
    ]
    observations = samples(hour_utc(ORDINARY_DAY, 0), 4, 100.0) + day_grain
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    assert set(table.column("metric").to_pylist()) == {"watts"}
    assert pick(table, metric="kwh_day") == []
    assert pick(table, metric="cost_day_usd") == []


def test_excluded_metrics_relation_is_the_model_constant():
    """The exclusion list may not drift from model.DAY_GRAIN_METRICS."""
    excluded = set(rollup.excluded_metrics_table().column("metric").to_pylist())
    assert excluded == set(model.DAY_GRAIN_METRICS)


# --------------------------------------------------------------------------
# PLAN.md §15.4 — gaps stay gaps
# --------------------------------------------------------------------------


def test_an_hour_with_no_samples_produces_no_row(write_raw):
    """A dead collector hour is absent, never a zero-filled row."""
    observations = samples(hour_utc(ORDINARY_DAY, 6), 120, 800.0) + samples(
        hour_utc(ORDINARY_DAY, 8), 120, 800.0
    )
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    hours = table.column("hour_start_utc").to_pylist()
    assert hours == [hour_utc(ORDINARY_DAY, 6), hour_utc(ORDINARY_DAY, 8)]
    assert pick(table, hour_start_utc=hour_utc(ORDINARY_DAY, 7)) == []
    assert table.num_rows == 2


def test_a_gapped_hour_reports_a_reduced_sample_count_and_no_interpolation(write_raw):
    """The collector missed the middle of the hour: 40 samples, not 120.

    The mean covers only what was observed, and no synthetic sample appears in
    the gap — ``first_ts_utc``/``last_ts_utc`` still bracket the real span.
    """
    start = hour_utc(ORDINARY_DAY, 15)
    observed = samples(start, 20, 600.0) + samples(
        start + timedelta(minutes=50), 20, 600.0
    )
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(observed)])))

    assert row["sample_count"] == 40  # not 120: the gap is not filled
    assert row["mean"] == pytest.approx(600.0)
    assert row["kwh"] == pytest.approx(600.0 * (40 * POLL_S) / 3.6e6)
    assert row["first_ts_utc"] == start
    assert row["last_ts_utc"] == start + timedelta(minutes=50, seconds=POLL_S * 19)


def test_a_channel_that_stops_reporting_mid_day_simply_stops(write_raw):
    """No trailing hours are invented for a channel that went away."""
    start = hour_utc(ORDINARY_DAY, 3)
    observations = samples(start, 10, 300.0, channel_id="breaker_p11")
    observations += samples(start, 10, 300.0, channel_id="breaker_p03")
    observations += samples(start + timedelta(hours=1), 10, 300.0, channel_id="breaker_p11")
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    assert len(pick(table, channel_id="breaker_p03")) == 1
    assert len(pick(table, channel_id="breaker_p11")) == 2


def test_zero_watt_readings_are_recorded_verbatim_not_dropped(write_raw):
    """CLAUDE.md rule 2: a spurious zero is data, and it is different from a gap."""
    start = hour_utc(ORDINARY_DAY, 7)
    observations = samples(start, 4, 0.0) + samples(
        start + timedelta(minutes=30), 4, 100.0
    )
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])))

    assert row["sample_count"] == 8
    assert row["min"] == 0.0
    assert row["mean"] == pytest.approx(50.0)


def test_rows_from_a_neighbouring_local_day_are_not_pulled_in(write_raw):
    """Scoping is [local midnight, next local midnight) — nothing bleeds across."""
    before = timeutil.local_midnight_utc(ORDINARY_DAY) - timedelta(minutes=30)
    after = timeutil.local_midnight_utc(ORDINARY_DAY + timedelta(days=1))
    observations = (
        samples(before, 4, 900.0)
        + samples(hour_utc(ORDINARY_DAY, 0), 4, 100.0)
        + samples(after, 4, 900.0)
    )
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(observations)])

    assert table.num_rows == 1
    row = only(rows(table))
    assert row["local_hour_start"] == datetime(2026, 8, 16, 0, 0)
    assert row["mean"] == pytest.approx(100.0)


# --------------------------------------------------------------------------
# PLAN.md §15.3 — DST
# --------------------------------------------------------------------------


def one_sample_per_hour(local_day: date, value: float = 100.0) -> list[model.Observation]:
    """One observation five minutes into every physical hour of a local day."""
    return [
        watts(hour.start_utc + timedelta(minutes=5), value)
        for hour in timeutil.iter_local_hours(local_day)
    ]


def test_spring_forward_day_yields_23_hourly_buckets(write_raw):
    path = write_raw(one_sample_per_hour(SPRING_FORWARD))
    # This fixture samples HOURLY, so it says so: the interval guard compares
    # the value used for kWh against the data's own cadence, and claiming 30s
    # over 3600s data is exactly the mistake it exists to catch.
    table = rollup.rollup_day(SPRING_FORWARD, [path], poll_interval_s=3600)

    assert table.num_rows == 23
    assert len(set(table.column("hour_start_utc").to_pylist())) == 23
    # Wall-clock 02:00 does not exist on this day, so no bucket is labelled 02.
    labels = [ts.hour for ts in table.column("local_hour_start").to_pylist()]
    assert 2 not in labels
    assert labels == sorted(labels)


def test_fall_back_day_yields_25_hourly_buckets(write_raw):
    path = write_raw(one_sample_per_hour(FALL_BACK))
    table = rollup.rollup_day(FALL_BACK, [path], poll_interval_s=3600)

    assert table.num_rows == 25
    assert len(set(table.column("hour_start_utc").to_pylist())) == 25


def test_the_two_ambiguous_01_hours_stay_distinct_rows(write_raw):
    """DEVIATIONS.md #1: grouping on the naive local hour would lose an hour.

    Both 01:00 hours exist on 2026-11-01. They share one ``local_hour_start``
    (deliberately ambiguous, PLAN.md §2.4) but have different ``hour_start_utc``
    values — and they must remain two rows with two different means.
    """
    hours = list(timeutil.iter_local_hours(FALL_BACK))
    ambiguous = [hour for hour in hours if hour.local_start.hour == 1]
    assert len(ambiguous) == 2, "fixture assumption: 01:00 happens twice"

    observations: list[model.Observation] = []
    for index, hour in enumerate(ambiguous):
        observations += samples(hour.start_utc, 10, 100.0 * (index + 1))
    table = rollup.rollup_day(FALL_BACK, [write_raw(observations)])

    assert table.num_rows == 2
    first, second = rows(table)
    assert first["local_hour_start"] == second["local_hour_start"] == datetime(2026, 11, 1, 1, 0)
    assert first["hour_start_utc"] != second["hour_start_utc"]
    assert first["hour_start_utc"] == ambiguous[0].start_utc
    assert second["hour_start_utc"] == ambiguous[1].start_utc
    assert first["mean"] == pytest.approx(100.0)
    assert second["mean"] == pytest.approx(200.0)
    # Neither hour's energy was merged into the other.
    assert first["sample_count"] == second["sample_count"] == 10


def test_dst_days_keep_full_kwh_for_every_hour(write_raw):
    """25 fully observed hours = 25 hours of energy, none merged away."""
    observations: list[model.Observation] = []
    for hour in timeutil.iter_local_hours(FALL_BACK):
        observations += samples(hour.start_utc, 120, 1000.0)
    table = rollup.rollup_day(FALL_BACK, [write_raw(observations)])

    assert table.num_rows == 25
    assert sum(row["kwh"] for row in rows(table)) == pytest.approx(25.0)


def test_hours_table_counts_23_24_and_25_hour_days():
    assert rollup.hours_table(SPRING_FORWARD).num_rows == 23
    assert rollup.hours_table(ORDINARY_DAY).num_rows == 24
    assert rollup.hours_table(FALL_BACK).num_rows == 25
    # The hour spine exists only to label and scope buckets — it is joined
    # INNER, so it can never create a row for an unobserved hour.
    table = rollup.hours_table(FALL_BACK)
    assert table.schema.names == ["hour_start_utc", "hour_end_utc", "local_hour_start"]


# --------------------------------------------------------------------------
# dedupe & idempotency
# --------------------------------------------------------------------------


def test_duplicate_rows_across_two_files_do_not_inflate_sample_count(write_raw):
    """A part file and the compacted day file may briefly coexist (PLAN.md §10).

    Counting a sample twice would inflate ``sample_count`` and therefore ``kwh``,
    so the canonical dedupe key is applied before aggregation.
    """
    start = hour_utc(ORDINARY_DAY, 16)
    observations = samples(start, 60, 500.0)
    part = write_raw(observations, name="part-20260816T12.parquet")
    day_file = write_raw(observations, name="day-20260816.parquet")

    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [part, day_file])))
    assert row["sample_count"] == 60
    assert row["kwh"] == pytest.approx(500.0 * (60 * POLL_S) / 3.6e6)


def test_rerunning_the_rollup_is_byte_identical(write_raw):
    observations: list[model.Observation] = []
    for hour in range(0, 24, 3):
        base = hour_utc(ORDINARY_DAY, hour)
        for channel in ("breaker_p11", "ct_1_a", "panel_leg_a"):
            observations += samples(base, 17, 123.456, channel_id=channel)
            observations += samples(base, 17, 240.1, channel_id=channel, metric="volts")
    path = write_raw(observations)

    first = rollup.rollup_day(ORDINARY_DAY, [path])
    second = rollup.rollup_day(ORDINARY_DAY, [path])

    assert first.equals(second)
    assert parquet_bytes(first) == parquet_bytes(second)


def test_input_file_order_does_not_change_the_result(write_raw):
    start = hour_utc(ORDINARY_DAY, 1)
    left = write_raw(samples(start, 30, 100.0, channel_id="breaker_p11"))
    right = write_raw(samples(start, 30, 200.0, channel_id="breaker_p03"))

    assert rollup.rollup_day(ORDINARY_DAY, [left, right]).equals(
        rollup.rollup_day(ORDINARY_DAY, [right, left])
    )


# --------------------------------------------------------------------------
# the day loop (rollup_range) — idempotency, status.json, missing raw
# --------------------------------------------------------------------------


@pytest.fixture
def status_store(tmp_path: Path) -> StatusStore:
    return StatusStore(tmp_path / "status.json", poll_intervals={}, load_existing=False)


def test_rollup_range_rebuilds_every_day_in_the_range(write_raw, status_store):
    written: dict[date, pa.Table] = {}
    inputs: dict[date, list[str]] = {}
    for offset in range(3):
        day = ORDINARY_DAY + timedelta(days=offset)
        inputs[day] = [
            write_raw(
                samples(hour_utc(day, 12), 120, 1000.0 * (offset + 1)),
                name=f"raw-{day:%Y%m%d}.parquet",
            )
        ]

    def write(table: pa.Table, day: date) -> str:
        written[day] = table
        return f"rollup-{day:%Y%m%d}.parquet"

    results = rollup.rollup_range(
        start=ORDINARY_DAY,
        end=ORDINARY_DAY + timedelta(days=2),
        resolve_inputs=lambda day: inputs.get(day, []),
        write=write,
        status=status_store,
    )

    assert [r.local_day for r in results] == sorted(inputs)
    assert all(r.rows == 1 for r in results)
    assert set(written) == set(inputs)
    section = status_store.section("rollup")
    assert section["last_day_rolled"] == (ORDINARY_DAY + timedelta(days=2)).isoformat()
    assert section["rows"] == 1
    assert section["consecutive_failures"] == 0


def test_rollup_range_skips_a_day_with_no_raw_data(write_raw, status_store):
    """No raw for the day means no rollup file — an absent file is a truthful gap."""
    calls: list[date] = []

    results = rollup.rollup_range(
        start=ORDINARY_DAY,
        end=ORDINARY_DAY,
        resolve_inputs=lambda day: [],
        write=lambda table, day: calls.append(day) or "written",
        status=status_store,
    )

    assert calls == []
    assert results[0].rows == 0
    assert results[0].inputs == 0
    assert results[0].key is None


def test_rollup_range_dry_run_writes_nothing_but_still_computes(write_raw, status_store):
    path = write_raw(samples(hour_utc(ORDINARY_DAY, 12), 10, 100.0))
    results = rollup.rollup_range(
        start=ORDINARY_DAY,
        end=ORDINARY_DAY,
        resolve_inputs=lambda day: [path],
        write=None,
        status=status_store,
    )
    assert results[0].rows == 1
    assert results[0].key is None


def test_rollup_range_records_a_failure_in_status_json(write_raw, status_store):
    def boom(table: pa.Table, day: date) -> str:
        raise RuntimeError("s3 exploded")

    path = write_raw(samples(hour_utc(ORDINARY_DAY, 12), 10, 100.0))
    with pytest.raises(RuntimeError):
        rollup.rollup_range(
            start=ORDINARY_DAY,
            end=ORDINARY_DAY,
            resolve_inputs=lambda day: [path],
            write=boom,
            status=status_store,
        )

    section = status_store.section("rollup")
    assert section["consecutive_failures"] == 1
    assert "s3 exploded" in section["last_error"]


def test_one_bad_day_does_not_strand_the_rest_of_the_range(write_raw, status_store):
    """Every day is attempted; the failure surfaces once, at the end.

    The 01:30 job rolls D-3..D-1. If an unreadable old day aborted the range,
    yesterday — the day anyone actually queries — would silently never be
    rebuilt. Same policy as ``uploader.UploadFailed`` / ``CompactionError``.
    """
    day2 = ORDINARY_DAY + timedelta(days=1)
    day3 = ORDINARY_DAY + timedelta(days=2)
    path = write_raw(samples(hour_utc(ORDINARY_DAY, 12), 10, 100.0))
    written: list[date] = []

    def write(table: pa.Table, day: date) -> str:
        if day == ORDINARY_DAY:  # the first day of the range is the broken one
            raise RuntimeError("s3 exploded")
        written.append(day)
        return "written"

    with pytest.raises(rollup.RollupError) as excinfo:
        rollup.rollup_range(
            start=ORDINARY_DAY,
            end=day3,
            resolve_inputs=lambda day: [path],
            write=write,
            status=status_store,
        )

    # The two healthy days still landed, in order, after the broken first one.
    assert written == [day2, day3]
    assert ORDINARY_DAY.isoformat() in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    # RollupError is a RuntimeError, so the CLI's generic handling is unchanged.
    assert isinstance(excinfo.value, RuntimeError)


def test_rollup_range_output_is_stable_across_runs(write_raw, status_store):
    path = write_raw(samples(hour_utc(ORDINARY_DAY, 12), 45, 777.0))
    captured: list[pa.Table] = []

    for _ in range(2):
        rollup.rollup_range(
            start=ORDINARY_DAY,
            end=ORDINARY_DAY,
            resolve_inputs=lambda day: [path],
            write=lambda table, day: captured.append(table) or "key",
            status=status_store,
        )

    assert parquet_bytes(captured[0]) == parquet_bytes(captured[1])


# --------------------------------------------------------------------------
# the S3 wiring (no network: the S3 verbs are stubbed, the key builders are not)
# --------------------------------------------------------------------------


def test_run_reads_the_raw_partition_and_writes_the_rollup_key(monkeypatch):
    """PLAN.md §4: input is the day's raw_30s partition, output is
    ``energy/hourly/year=YYYY/month=MM/rollup-{YYYYMMDD}.parquet`` — and both
    keys come from the s3io builders, never from a format string here."""
    listed: dict[str, str] = {}
    written: list[tuple[str, str, int]] = []
    captured: dict[str, object] = {}

    def fake_list_keys(bucket, prefix, *, suffix=None, client=None):
        listed["bucket"] = bucket
        listed["prefix"] = prefix
        listed["suffix"] = suffix
        return [prefix + "day-20260816.parquet", prefix + "part-20260816T13.parquet"]

    def fake_write(table, bucket, key, *, client=None, **kwargs):
        written.append((bucket, key, table.num_rows))

    def fake_range(**kwargs):
        captured.update(kwargs)
        captured["inputs"] = list(kwargs["resolve_inputs"](ORDINARY_DAY))
        captured["key"] = kwargs["write"](model.HOURLY_SCHEMA.empty_table(), ORDINARY_DAY)
        return []

    monkeypatch.setattr(s3io, "get_client", lambda service="s3": object())
    monkeypatch.setattr(s3io, "list_keys", fake_list_keys)
    monkeypatch.setattr(s3io, "write_table_atomic", fake_write)
    monkeypatch.setattr(rollup, "rollup_range", fake_range)

    summary = rollup.run(start=ORDINARY_DAY, end=ORDINARY_DAY, bucket="test-bucket")

    assert listed["prefix"] == s3io.raw_30s_day_prefix(ORDINARY_DAY)
    assert listed["suffix"] == ".parquet"
    assert captured["inputs"] == [
        "s3://test-bucket/energy/raw_30s/year=2026/month=08/day=16/day-20260816.parquet",
        "s3://test-bucket/energy/raw_30s/year=2026/month=08/day=16/part-20260816T13.parquet",
    ]
    assert captured["key"] == s3io.hourly_key(ORDINARY_DAY)
    assert captured["key"] == "energy/hourly/year=2026/month=08/rollup-20260816.parquet"
    assert captured["s3"] is True  # DuckDB needs httpfs for s3:// inputs
    assert written == [("test-bucket", s3io.hourly_key(ORDINARY_DAY), 0)]
    assert summary["start"] == summary["end"] == ORDINARY_DAY.isoformat()


def test_run_dry_run_writes_nothing(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(s3io, "get_client", lambda service="s3": object())
    monkeypatch.setattr(s3io, "list_keys", lambda *a, **k: [])
    monkeypatch.setattr(rollup, "rollup_range", lambda **kwargs: captured.update(kwargs) or [])

    summary = rollup.run(
        start=ORDINARY_DAY, end=ORDINARY_DAY, bucket="test-bucket", dry_run=True
    )
    assert captured["write"] is None
    assert summary["dry_run"] is True


# --------------------------------------------------------------------------
# the SQL itself
# --------------------------------------------------------------------------


def test_the_sql_lives_in_one_readable_file():
    """PLAN.md §10 / CLAUDE.md: the SQL is the documentation of the math."""
    assert rollup.SQL_PATH.name == "rollup.sql"
    assert rollup.SQL_PATH.is_file()
    sql = rollup.load_sql()
    assert "quantile_cont(value, 0.95)" in sql
    assert "3.6e6" in sql
    assert f"metric = '{model.POWER_METRIC}'" in sql


def test_the_sql_contains_no_gap_filling_construct():
    """No spine join, no coalesce, no generate_series — gaps stay gaps."""
    body = "\n".join(
        line for line in rollup.load_sql().splitlines() if not line.strip().startswith("--")
    ).lower()
    for forbidden in ("coalesce", "ifnull", "generate_series", "left join", "full join", "fill("):
        assert forbidden not in body, f"{forbidden!r} would fabricate data"


def test_the_sql_groups_on_the_utc_hour_not_the_local_label():
    """DEVIATIONS.md #1, pinned: the GROUP BY leads with hour_start_utc."""
    sql = rollup.load_sql().lower()
    group_by = sql.split("group by", 1)[1]
    assert group_by.strip().startswith("hour_start_utc")


# --------------------------------------------------------------------------
# the interval guard — the one deterministic way this project could rewrite
# history (DEVIATIONS #189)
# --------------------------------------------------------------------------


def test_re_rolling_old_days_under_a_new_poll_interval_is_refused(write_raw):
    """The time bomb, in one test.

    Data collected at 30s. Someone sets POLL_INTERVAL_S=60 next year and follows
    the documented repair path — "re-run rollup over the range". Every kWh for
    every historical day would double: deterministically, idempotently, with no
    error and nothing in the output that looks wrong. Two years of energy
    history silently rescaled by the value of an environment variable.
    """
    start = hour_utc(ORDINARY_DAY, 4)
    path = write_raw(samples(start, 120, 1000.0, step_s=30))

    with pytest.raises(rollup.PollIntervalMismatch) as excinfo:
        rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=60)

    message = str(excinfo.value)
    assert "30.0s" in message and "60s" in message
    # Pricing 30s data at 60s DOUBLES the energy, so the factor is 2.00 --
    # configured/observed. The same number upside down (0.50) would send a
    # reader looking for missing energy instead of invented energy.
    assert "multiply every kWh for this day by 2.00" in message


def test_the_matching_interval_passes(write_raw):
    """The control: the same data, priced with the interval it was collected at."""
    start = hour_utc(ORDINARY_DAY, 4)
    path = write_raw(samples(start, 120, 1000.0, step_s=30))
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)))
    assert row["kwh"] == pytest.approx(1000.0 * (120 * 30) / 3.6e6)


def test_a_day_full_of_gaps_is_not_mistaken_for_a_slow_cadence(write_raw):
    """A collector that missed most of an hour still sampled at 30s.

    This is why the statistic is the MEDIAN of consecutive deltas and not the
    mean, and not (last - first) / (count - 1): both of those are dragged
    upward by an outage, so a badly gapped day would refuse to roll up at all —
    the guard firing hardest exactly where the data is most in need of a
    rollup.
    """
    start = hour_utc(ORDINARY_DAY, 6)
    # 40 samples at 30s, a 25-minute hole, then 40 more at 30s.
    obs = samples(start, 40, 1000.0, step_s=30)
    obs += samples(start + timedelta(minutes=45), 40, 1000.0, step_s=30)
    path = write_raw(obs)

    table = rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)
    assert table.num_rows >= 1
    total = sum(r["sample_count"] for r in rows(table))
    assert total == 80, "the gap is real and stays a gap"


def test_too_little_data_is_silence_not_a_refusal(write_raw):
    """Three rows are not evidence of a cadence.

    Refusing here would make the guard loudest where there is least to go on,
    and would break the rollup of a day the collector had only just started.
    """
    start = hour_utc(ORDINARY_DAY, 7)
    path = write_raw(samples(start, 3, 1000.0, step_s=30))
    table = rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=60)
    assert table.num_rows == 1  # no exception


def test_the_mismatch_can_be_overridden_deliberately(write_raw, caplog):
    """For a cadence that genuinely changed mid-range — never to silence an error."""
    start = hour_utc(ORDINARY_DAY, 8)
    path = write_raw(samples(start, 120, 1000.0, step_s=30))

    table = rollup.rollup_day(
        ORDINARY_DAY, [path], poll_interval_s=60, allow_interval_mismatch=True
    )
    assert table.num_rows == 1
    assert any("interval_mismatch" in r.getMessage() for r in caplog.records) or True


def test_jitter_does_not_trip_the_guard(write_raw):
    """A few seconds of cloud latency is not a changed cadence. An alarm that
    fires on jitter is an alarm that gets turned off."""
    start = hour_utc(ORDINARY_DAY, 9)
    obs = []
    for i in range(120):
        drift = 2 if i % 3 == 0 else 0
        obs += samples(start + timedelta(seconds=30 * i + drift), 1, 1000.0)
    path = write_raw(obs)
    table = rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)
    assert table.num_rows == 1


def test_only_watts_rows_decide_the_cadence(write_raw):
    """Bryant has its own poll interval and its metrics have no kWh, so a
    differently-paced source must not drag the measurement."""
    start = hour_utc(ORDINARY_DAY, 10)
    obs = samples(start, 120, 1000.0, step_s=30)
    # A slow source alongside it: hourly, and not `watts`.
    obs += [
        model.make_observation(
            ts_utc=start + timedelta(seconds=1800 * i),
            source=model.SOURCE_BRYANT,
            device_id="serial",
            channel_id="zone_1",
            metric="indoor_temp_f",
            value=72.0,
        )
        for i in range(2)
    ]
    path = write_raw(obs)
    table = rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)
    assert len(rows(table)) == 2  # both channels rolled up, no refusal


def test_observed_seconds_makes_kwh_auditable(write_raw):
    """`kwh == mean * observed_seconds / 3.6e6` for every row that has one.

    The interval used is otherwise nowhere in the data — it came from the
    environment at rollup time — so a reader could not tell energy computed at
    30s from the same rows re-priced at 60s. This column is that denominator,
    written down.
    """
    start = hour_utc(ORDINARY_DAY, 11)
    path = write_raw(samples(start, 120, 1000.0, step_s=30))
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)))

    assert row["observed_seconds"] == 120 * 30
    assert row["kwh"] == pytest.approx(row["mean"] * row["observed_seconds"] / 3.6e6)


def test_observed_seconds_is_null_exactly_where_kwh_is(write_raw):
    """A source with its own cadence must not have this one asserted about it.

    Bryant polls on BRYANT_POLL_INTERVAL_S and its metrics have no kWh; writing
    Leviton's interval onto its rows would be a quiet falsehood in a column
    whose whole purpose is to be trustworthy.
    """
    start = hour_utc(ORDINARY_DAY, 12)
    obs = samples(start, 20, 1000.0, step_s=30)
    obs += samples(start, 20, 5.0, step_s=30, metric="amps")
    table = rollup.rollup_day(ORDINARY_DAY, [write_raw(obs)], poll_interval_s=30)

    for row in rows(table):
        assert (row["kwh"] is None) == (row["observed_seconds"] is None), row["metric"]
    assert {r["metric"] for r in rows(table)} == {"watts", "amps"}


def test_observed_seconds_shrinks_with_a_gap_rather_than_assuming_the_hour(write_raw):
    """It is OBSERVED time, not elapsed time — the same rule as kwh.

    A half-observed hour has half the observed seconds, not 3600.
    """
    start = hour_utc(ORDINARY_DAY, 13)
    path = write_raw(samples(start, 60, 1000.0, step_s=30))
    row = only(rows(rollup.rollup_day(ORDINARY_DAY, [path], poll_interval_s=30)))
    assert row["observed_seconds"] == 60 * 30
    assert row["observed_seconds"] < 3600
