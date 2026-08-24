"""The canonical row, the schemas, and the sort/dedupe contract — PLAN.md §3, §15.2.

What is pinned here:

* ``Observation`` -> :class:`pyarrow.Table` -> ``Observation`` round-trips with the
  exact types PLAN.md §3 specifies (``ts_utc`` tz-aware UTC, ``ts_local`` naive,
  ``value`` double) at microsecond precision.
* Rows come out sorted by :data:`SORT_KEY` and deduped on :data:`DEDUPE_KEY`,
  deterministically, so a re-run of any stage overwrites byte-identically.
* The ``hourly`` (§10) and ``meter`` (§13) schema variants carry the columns
  those sections require, without disturbing the canonical eight.
* ``UNIT_FOR_METRIC`` says what §3 says, for every metric §3 lists.
* Cardinal rule 1 at the constructor boundary: a missing sample raises rather
  than becoming a zero, while a genuine zero from the API is recorded verbatim.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from energy_capture import model, timeutil
from energy_capture.model import (
    CANONICAL_COLUMNS,
    DEDUPE_KEY,
    HOURLY_DEDUPE_KEY,
    HOURLY_SORT_KEY,
    SORT_KEY,
    Dataset,
    MeterObservation,
    Observation,
)
from tests.conftest import ObservationFactory, naive, utc

# The two instants of the DST fall-back 01:30 — identical ``ts_local``, distinct
# ``ts_utc``. Used to prove the keys really are UTC-based.
FIRST_0130 = utc(2026, 11, 1, 5, 30)
SECOND_0130 = utc(2026, 11, 1, 6, 30)


# --------------------------------------------------------------------------
# PLAN.md §3 — the metric/unit vocabulary
# --------------------------------------------------------------------------

#: Transcribed from the PLAN.md §3 schema table (``metric`` and ``unit`` rows).
#: If a future edit changes a unit here, that is a data-model change and this
#: test is where it must be argued.
PLAN_SECTION_3_UNITS: dict[str, str] = {
    "watts": "W",
    "amps": "A",
    "volts": "V",
    "hz": "Hz",
    "indoor_temp_f": "degF",
    "outdoor_temp_f": "degF",
    "setpoint_cool_f": "degF",
    "setpoint_heat_f": "degF",
    "stage": "enum",
    "mode": "enum",
    "fan": "enum",
    "humidity_pct": "pct",
    "kwh_day": "kWh",
    "cost_day_usd": "USD",
}


@pytest.mark.parametrize(("metric", "unit"), sorted(PLAN_SECTION_3_UNITS.items()))
def test_unit_for_metric_matches_plan_section_3(metric: str, unit: str) -> None:
    assert model.UNIT_FOR_METRIC[metric] == unit
    assert model.unit_for_metric(metric) == unit


def test_every_metric_has_a_unit_from_the_canonical_vocabulary() -> None:
    assert model.METRICS == frozenset(model.UNIT_FOR_METRIC)
    assert set(model.UNIT_FOR_METRIC.values()) <= model.UNITS
    # §3's unit list, plus §13's CCF and §7.3's native units.
    assert {"W", "A", "V", "Hz", "degF", "kWh", "USD", "pct", "enum"} <= model.UNITS


def test_unit_for_metric_rejects_an_unknown_metric() -> None:
    with pytest.raises(ValueError, match="unknown metric"):
        model.unit_for_metric("kilowatts")


def test_enum_metrics_carry_the_enum_unit() -> None:
    """§7.3: enums are a small integer in ``value`` with ``unit='enum'``."""
    # Six as of 2026-08-22: the three original, plus the per-unit state strings
    # mapped when the live capture proved they are populated.
    assert model.ENUM_METRICS == frozenset(
        {"mode", "stage", "fan", "op_status", "odu_mode", "idu_status"}
    )
    for metric in model.ENUM_METRICS:
        assert model.unit_for_metric(metric) == model.UNIT_ENUM


def test_day_grain_metrics_are_exactly_the_daily_dataset_metrics() -> None:
    assert model.DAY_GRAIN_METRICS == frozenset({"kwh_day", "cost_day_usd"})
    assert all(model.is_day_grain(m) for m in model.DAY_GRAIN_METRICS)
    assert not model.is_day_grain("watts")
    assert not model.is_day_grain("kwh_interval")  # §13 meter rows are interval, not day


def test_sources_vocabulary() -> None:
    assert model.SOURCES == frozenset({"leviton", "bryant", "lge"})


# --------------------------------------------------------------------------
# PLAN.md §3 — Observation <-> Table round-trip
# --------------------------------------------------------------------------


def test_table_types_match_the_canonical_schema(make_obs: ObservationFactory) -> None:
    table = model.observations_to_table([make_obs()])

    assert table.schema == model.RAW_30S_SCHEMA
    assert table.column_names == list(CANONICAL_COLUMNS)
    assert table.schema.field("ts_utc").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("ts_local").type == pa.timestamp("us")
    assert table.schema.field("ts_local").type.tz is None
    assert table.schema.field("value").type == pa.float64()
    for name in CANONICAL_COLUMNS:
        assert not table.schema.field(name).nullable, f"{name} must be non-nullable"


def test_round_trip_preserves_awareness_and_microseconds(make_obs: ObservationFactory) -> None:
    obs = make_obs(utc(2026, 8, 16, 18, 0, 30, 123456), value=1234.5)
    assert obs.ts_utc == utc(2026, 8, 16, 18, 0, 30, 123456)
    assert obs.ts_local == naive(2026, 8, 16, 14, 0, 30, 123456)

    table = model.observations_to_table([obs])
    row = table.to_pylist()[0]

    assert row["ts_utc"] == obs.ts_utc
    assert row["ts_utc"].tzinfo is not None
    assert row["ts_utc"].utcoffset() == timedelta(0)
    assert row["ts_utc"].microsecond == 123456

    assert row["ts_local"] == obs.ts_local
    assert row["ts_local"].tzinfo is None, "ts_local is a naive wall clock (PLAN §2.4)"
    assert row["ts_local"].microsecond == 123456

    assert isinstance(row["value"], float)
    assert row["value"] == 1234.5

    assert model.table_to_observations(table) == [obs]


def test_round_trip_of_a_mixed_batch(make_obs: ObservationFactory) -> None:
    observations = [
        make_obs(utc(2026, 8, 16, 18, 0, 30), metric="watts", value=0.0),
        make_obs(utc(2026, 8, 16, 18, 0, 30), metric="amps", value=1.75),
        make_obs(
            utc(2026, 8, 16, 18, 0, 30),
            source=model.SOURCE_BRYANT,
            device_id="TEST0000001",
            channel_id="zone_1",
            metric="indoor_temp_f",
            value=71.5,
        ),
    ]
    table = model.observations_to_table(observations)

    assert table.num_rows == 3
    assert model.table_to_observations(table) == model.sort_observations(observations)
    assert all(isinstance(o, Observation) for o in model.table_to_observations(table))


def test_integer_values_become_doubles(make_obs: ObservationFactory) -> None:
    obs = make_obs(value=240)
    assert isinstance(obs.value, float)
    table = model.observations_to_table([obs])
    assert table.schema.field("value").type == pa.float64()
    assert table.column("value").to_pylist() == [240.0]


def test_empty_input_yields_an_empty_table_not_a_fabricated_row() -> None:
    """A gap is a real answer: zero rows, right schema (CLAUDE.md rule 1)."""
    table = model.observations_to_table([])
    assert table.num_rows == 0
    assert table.schema == model.RAW_30S_SCHEMA
    assert model.empty_table().num_rows == 0
    assert model.empty_table(Dataset.HOURLY).schema == model.HOURLY_SCHEMA


# --------------------------------------------------------------------------
# PLAN.md §3 — sort order
# --------------------------------------------------------------------------


def test_table_is_sorted_by_sort_key(make_obs: ObservationFactory) -> None:
    assert SORT_KEY == ("ts_utc", "source", "device_id", "channel_id")

    later = utc(2026, 8, 16, 18, 1, 0)
    earlier = utc(2026, 8, 16, 18, 0, 30)
    shuffled = [
        make_obs(later, channel_id="breaker_p11"),
        make_obs(earlier, channel_id="breaker_p9"),
        make_obs(earlier, source=model.SOURCE_BRYANT, device_id="TEST1", channel_id="system",
                 metric="outdoor_temp_f", value=88.0),
        make_obs(earlier, channel_id="breaker_p11"),
        make_obs(earlier, device_id="hub-b", channel_id="ct_1_a"),
    ]

    table = model.observations_to_table(shuffled)
    keys = [tuple(row[k] for k in SORT_KEY) for row in table.select(list(SORT_KEY)).to_pylist()]

    assert keys == sorted(keys)
    assert keys[0][0] == earlier and keys[-1][0] == later
    # bryant sorts before leviton at the same instant; hub-a before hub-b.
    assert keys[0][1] == "bryant"
    assert [k[2] for k in keys[1:4]] == ["hub-a", "hub-a", "hub-b"]


def test_sort_is_stable_for_rows_differing_only_in_metric(make_obs: ObservationFactory) -> None:
    """``metric`` is not in SORT_KEY, so ties keep input order — deterministically."""
    ts = utc(2026, 8, 16, 18, 0, 30)
    observations = [
        make_obs(ts, metric="watts", value=900.0),
        make_obs(ts, metric="amps", value=3.75),
        make_obs(ts, metric="volts", value=241.0),
    ]
    table = model.observations_to_table(observations)
    assert table.column("metric").to_pylist() == ["watts", "amps", "volts"]


def test_sort_table_breaks_ties_by_original_row_order(make_obs: ObservationFactory) -> None:
    ts = utc(2026, 8, 16, 18, 0, 30)
    unsorted = model.observations_to_table(
        [make_obs(ts, metric="volts", value=241.0), make_obs(ts, metric="watts", value=900.0)],
        sort=False,
    )
    assert model.sort_table(unsorted).column("metric").to_pylist() == ["volts", "watts"]


def test_ambiguous_dst_hour_sorts_by_ts_utc_despite_equal_ts_local(
    make_obs: ObservationFactory,
) -> None:
    """Two rows with the same readable wall clock stay ordered by the real instant."""
    observations = [make_obs(SECOND_0130, value=200.0), make_obs(FIRST_0130, value=100.0)]
    table = model.observations_to_table(observations)

    assert table.column("ts_utc").to_pylist() == [FIRST_0130, SECOND_0130]
    assert table.column("value").to_pylist() == [100.0, 200.0]
    assert table.column("ts_local").to_pylist() == [naive(2026, 11, 1, 1, 30)] * 2


# --------------------------------------------------------------------------
# PLAN.md §15.2 — dedupe
# --------------------------------------------------------------------------


def test_dedupe_key_is_the_documented_tuple() -> None:
    assert DEDUPE_KEY == ("ts_utc", "source", "device_id", "channel_id", "metric")


def test_rows_sharing_the_dedupe_key_collapse_to_exactly_one(
    make_obs: ObservationFactory,
) -> None:
    ts = utc(2026, 8, 16, 18, 0, 30)
    duplicates = [make_obs(ts, value=100.0), make_obs(ts, value=999.0), make_obs(ts, value=42.0)]

    table = model.observations_to_table(duplicates)

    assert table.num_rows == 1
    assert table.column("value").to_pylist() == [100.0], "first occurrence wins (precedence = input order)"


def test_input_order_expresses_precedence(make_obs: ObservationFactory) -> None:
    """Backfill relies on this: DynamoDB rows first, so they beat the legacy JSON."""
    ts = utc(2026, 8, 16, 18, 0, 30)
    preferred = make_obs(ts, value=100.0)
    fallback = make_obs(ts, value=999.0)

    assert model.observations_to_table([preferred, fallback]).column("value").to_pylist() == [100.0]
    assert model.observations_to_table([fallback, preferred]).column("value").to_pylist() == [999.0]


def test_rows_differing_only_in_metric_are_both_kept(make_obs: ObservationFactory) -> None:
    ts = utc(2026, 8, 16, 18, 0, 30)
    table = model.observations_to_table(
        [make_obs(ts, metric="watts", value=900.0), make_obs(ts, metric="amps", value=3.75)]
    )

    assert table.num_rows == 2
    assert sorted(table.column("metric").to_pylist()) == ["amps", "watts"]


@pytest.mark.parametrize(
    ("field", "other_value"),
    [
        ("source", model.SOURCE_BRYANT),
        ("device_id", "hub-b"),
        ("channel_id", "breaker_p9"),
        ("metric", "amps"),
    ],
)
def test_rows_differing_in_any_key_component_are_both_kept(
    make_obs: ObservationFactory, field: str, other_value: str
) -> None:
    ts = utc(2026, 8, 16, 18, 0, 30)
    first = make_obs(ts)
    second = make_obs(ts, **{field: other_value})
    assert model.observations_to_table([first, second]).num_rows == 2


def test_ambiguous_dst_rows_are_not_deduped_away(make_obs: ObservationFactory) -> None:
    """§15.3: the repeated local hour must not lose a row to dedupe."""
    rows = [make_obs(FIRST_0130, value=100.0), make_obs(SECOND_0130, value=200.0)]
    assert len(model.dedupe_observations(rows)) == 2
    assert model.observations_to_table(rows).num_rows == 2


def test_dedupe_table_keeps_the_first_row_in_table_order(make_obs: ObservationFactory) -> None:
    ts = utc(2026, 8, 16, 18, 0, 30)
    raw = model.observations_to_table(
        [make_obs(ts, value=100.0), make_obs(ts, value=999.0)], dedupe=False, sort=False
    )
    assert raw.num_rows == 2

    deduped = model.dedupe_table(raw)
    assert deduped.num_rows == 1
    assert deduped.column("value").to_pylist() == [100.0]
    assert deduped.schema == raw.schema


def test_dedupe_table_on_an_empty_table_is_a_no_op() -> None:
    empty = model.empty_table()
    assert model.dedupe_table(empty).num_rows == 0


def test_repeated_conversion_is_byte_identical(make_obs: ObservationFactory) -> None:
    """Idempotent stages depend on this: same input -> same Parquet bytes."""
    observations = [
        make_obs(utc(2026, 8, 16, 18, 1, 0), channel_id="breaker_p11", value=900.0),
        make_obs(utc(2026, 8, 16, 18, 0, 30), channel_id="ct_1_a", value=12.0),
        make_obs(utc(2026, 8, 16, 18, 0, 30), channel_id="ct_1_a", value=77.0),  # duplicate
        make_obs(utc(2026, 8, 16, 18, 0, 30), channel_id="breaker_p11", value=880.0),
    ]

    first = model.observations_to_table(observations)
    second = model.observations_to_table(list(observations))
    assert first.equals(second)

    def parquet_bytes(table: pa.Table) -> bytes:
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        return sink.getvalue().to_pybytes()

    assert parquet_bytes(first) == parquet_bytes(second)


def test_parquet_file_round_trip_preserves_the_schema(make_obs: ObservationFactory, tmp_path) -> None:
    table = model.observations_to_table([make_obs(), make_obs(metric="volts", value=241.3)])
    path = tmp_path / "part-20260816T14.parquet"
    pq.write_table(table, path, compression="zstd")

    read_back = pq.read_table(path)
    assert read_back.schema == model.RAW_30S_SCHEMA
    assert read_back.equals(table)
    assert pq.read_metadata(path).num_rows == table.num_rows  # what the uploader verifies


# --------------------------------------------------------------------------
# PLAN.md §10 — the hourly rollup schema variant
# --------------------------------------------------------------------------


def test_hourly_schema_has_every_column_section_10_requires() -> None:
    names = model.HOURLY_SCHEMA.names

    # Grouping keys (§10) — the hour bucket plus the channel/metric identity.
    for column in ("source", "device_id", "channel_id", "metric"):
        assert column in names
    assert "hour_start_utc" in names, "bucket key must be UTC so fall-back stays 25 hours"
    assert "local_hour_start" in names, "readable local rendering of the bucket"
    # Aggregates (§10).
    for column in ("mean", "min", "max", "p95", "sample_count", "first_ts_utc", "last_ts_utc", "kwh"):
        assert column in names


def test_hourly_schema_types() -> None:
    schema = model.HOURLY_SCHEMA
    assert schema.field("hour_start_utc").type == pa.timestamp("us", tz="UTC")
    assert schema.field("local_hour_start").type == pa.timestamp("us")
    assert schema.field("local_hour_start").type.tz is None
    for column in ("mean", "min", "max", "p95", "kwh"):
        assert schema.field(column).type == pa.float64()
    assert schema.field("sample_count").type == pa.int64()
    for column in ("first_ts_utc", "last_ts_utc"):
        assert schema.field(column).type == pa.timestamp("us", tz="UTC")


def test_hourly_kwh_and_its_denominator_are_the_only_nullable_columns() -> None:
    """``kwh`` is NULL for every metric but ``watts`` — never 0 (§2.5).

    ``observed_seconds`` is null in exactly the same places: it is kwh's
    denominator, and asserting one source's poll interval onto another source's
    rows would put a quiet falsehood in the column whose entire purpose is to
    make kwh auditable (#190).
    """
    nullable = {f.name for f in model.HOURLY_SCHEMA if f.nullable}
    assert nullable == {"kwh", "observed_seconds"}
    assert model.POWER_METRIC == "watts"


def test_hourly_keys_lead_with_the_utc_bucket() -> None:
    assert HOURLY_SORT_KEY[0] == "hour_start_utc"
    assert HOURLY_SORT_KEY == ("hour_start_utc", "source", "device_id", "channel_id")
    assert HOURLY_DEDUPE_KEY == HOURLY_SORT_KEY + ("metric",)
    assert set(HOURLY_DEDUPE_KEY) <= set(model.HOURLY_SCHEMA.names)


def test_hourly_is_not_an_observation_dataset() -> None:
    assert model.schema_for("hourly") is model.HOURLY_SCHEMA
    with pytest.raises(ValueError, match="not an observation dataset"):
        model.row_type_for(Dataset.HOURLY)


def test_hourly_bucket_keys_come_from_timeutil() -> None:
    """A rollup row's two hour columns for the ambiguous hour, end to end."""
    buckets = {
        (timeutil.utc_hour_start(ts), timeutil.local_hour_start(ts))
        for ts in (FIRST_0130, SECOND_0130)
    }
    assert buckets == {
        (utc(2026, 11, 1, 5), naive(2026, 11, 1, 1)),
        (utc(2026, 11, 1, 6), naive(2026, 11, 1, 1)),
    }


# --------------------------------------------------------------------------
# PLAN.md §13 — the meter schema variant
# --------------------------------------------------------------------------


def test_meter_schema_is_the_canonical_schema_plus_interval_s() -> None:
    assert model.METER_SCHEMA.names == list(CANONICAL_COLUMNS) + ["interval_s"]
    for name in CANONICAL_COLUMNS:
        assert model.METER_SCHEMA.field(name) == model.RAW_30S_SCHEMA.field(name)
    assert model.METER_SCHEMA.field("interval_s").type == pa.int32()
    assert not model.METER_SCHEMA.field("interval_s").nullable


def test_observation_schema_composes_variants_without_patching() -> None:
    extended = model.observation_schema(pa.field("interval_s", pa.int32(), nullable=False))
    assert extended == model.METER_SCHEMA
    assert model.observation_schema() == model.RAW_30S_SCHEMA


def test_meter_observation_round_trip(make_obs: ObservationFactory) -> None:
    """``ts_utc`` is the interval START; ``interval_s`` is its length (§13)."""
    interval_start = utc(2026, 8, 16, 18, 0, 0)
    obs = make_obs(
        interval_start,
        source=model.SOURCE_LGE,
        device_id="meter-1",
        channel_id="electric_main",
        metric="kwh_interval",
        value=0.37,
        interval_s=900,
    )
    assert isinstance(obs, MeterObservation)
    assert obs.interval_s == 900
    assert obs.unit == model.UNIT_KWH
    assert obs.ts_utc == interval_start

    table = model.observations_to_table([obs], dataset=Dataset.METER)
    assert table.schema == model.METER_SCHEMA
    assert table.column("interval_s").to_pylist() == [900]

    restored = model.table_to_observations(table)
    assert restored == [obs]
    assert isinstance(restored[0], MeterObservation)


def test_gas_meter_interval_uses_ccf(make_obs: ObservationFactory) -> None:
    obs = make_obs(
        source=model.SOURCE_LGE,
        device_id="meter-2",
        channel_id="gas_main",
        metric="ccf_interval",
        value=1.2,
        interval_s=86400,
    )
    assert obs.unit == model.UNIT_CCF
    assert isinstance(obs, MeterObservation)


def test_meter_row_type_and_schema_lookup() -> None:
    assert model.row_type_for(Dataset.METER) is MeterObservation
    assert model.row_type_for("raw_30s") is Observation
    assert model.row_type_for("daily") is Observation
    assert model.schema_for(Dataset.RAW_30S) is model.RAW_30S_SCHEMA
    assert model.schema_for("daily") is model.DAILY_SCHEMA
    assert model.schema_for("meter") is model.METER_SCHEMA


@pytest.mark.parametrize("bad", [0, -1, -900])
def test_non_positive_interval_is_rejected(make_obs: ObservationFactory, bad: int) -> None:
    with pytest.raises(ValueError, match="interval_s"):
        make_obs(metric="kwh_interval", source=model.SOURCE_LGE, value=1.0, interval_s=bad)


# --------------------------------------------------------------------------
# CLAUDE.md rule 6 — day-grain rows never enter raw_30s
# --------------------------------------------------------------------------


def test_day_grain_metrics_are_rejected_by_raw_30s(day_grain_obs) -> None:
    rows = [day_grain_obs(date(2026, 8, 15))]
    with pytest.raises(ValueError, match="may not be written to raw_30s"):
        model.observations_to_table(rows, dataset=Dataset.RAW_30S)


def test_daily_dataset_rejects_non_day_grain_metrics(make_obs: ObservationFactory) -> None:
    with pytest.raises(ValueError, match="may not be written to energy/daily"):
        model.observations_to_table([make_obs()], dataset=Dataset.DAILY)


def test_daily_rows_are_stamped_at_local_midnight(day_grain_obs) -> None:
    """§7.2: ``ts_utc`` = local midnight of the measured day, ``ts_local`` = midnight."""
    for local_day, expected_utc in (
        (date(2026, 3, 8), utc(2026, 3, 8, 5)),  # spring-forward day, EST offset
        (date(2026, 11, 1), utc(2026, 11, 1, 4)),  # fall-back day, EDT offset
        (date(2026, 8, 15), utc(2026, 8, 15, 4)),
    ):
        obs = day_grain_obs(local_day)
        assert obs.ts_utc == expected_utc
        assert obs.ts_local == datetime.combine(local_day, datetime.min.time())

    table = model.observations_to_table(
        [day_grain_obs(date(2026, 8, 15)), day_grain_obs(date(2026, 8, 15), metric="cost_day_usd", value=1.42)],
        dataset=Dataset.DAILY,
    )
    assert table.schema == model.DAILY_SCHEMA
    assert table.num_rows == 2


def test_daily_day2_revision_collapses_onto_day1(day_grain_obs) -> None:
    """§15.2: a day2 re-read of the same date+component is one row, not two."""
    day1 = day_grain_obs(date(2026, 8, 15), value=12.5)
    day2 = day_grain_obs(date(2026, 8, 15), value=12.9)

    table = model.observations_to_table([day2, day1], dataset=Dataset.DAILY)
    assert table.num_rows == 1
    assert table.column("value").to_pylist() == [12.9], "input order decides which revision wins"


# --------------------------------------------------------------------------
# CLAUDE.md rule 1 — gaps stay gaps at the constructor boundary
# --------------------------------------------------------------------------


def test_a_missing_sample_raises_rather_than_becoming_a_zero(make_obs: ObservationFactory) -> None:
    with pytest.raises(ValueError, match="emit no row"):
        make_obs(value=None)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_are_rejected(make_obs: ObservationFactory, bad: float) -> None:
    with pytest.raises(ValueError, match="non-finite"):
        make_obs(value=bad)


def test_a_genuine_zero_is_recorded_verbatim(make_obs: ObservationFactory) -> None:
    """PLAN §2.3: Leviton fw v2 spurious zeros go into raw unfiltered."""
    table = model.observations_to_table([make_obs(value=0.0)])
    assert table.column("value").to_pylist() == [0.0]
    assert table.num_rows == 1


def test_negative_values_are_allowed(make_obs: ObservationFactory) -> None:
    """Export/backfeed is real data, not an error."""
    assert make_obs(value=-450.5).value == -450.5


def test_unknown_source_and_unit_are_rejected(make_obs: ObservationFactory) -> None:
    with pytest.raises(ValueError, match="unknown source"):
        make_obs(source="sense")
    with pytest.raises(ValueError, match="unknown unit"):
        make_obs(unit="kilowatts")
    with pytest.raises(ValueError, match="unknown metric"):
        make_obs(metric="power")


def test_make_observation_derives_ts_local_and_unit(make_obs: ObservationFactory) -> None:
    instant = utc(2026, 11, 1, 6, 30)
    obs = make_obs(instant, metric="volts", value=241.3)

    assert obs.ts_local == timeutil.to_local_naive(instant)
    assert obs.ts_local.tzinfo is None
    assert obs.unit == "V"
    assert obs.ts_utc.tzinfo is not None


def test_make_observation_normalises_a_naive_ts_utc_as_utc() -> None:
    obs = model.make_observation(
        ts_utc=datetime(2026, 8, 16, 18, 0, 30),
        source=model.SOURCE_LEVITON,
        device_id="hub-a",
        channel_id="breaker_p11",
        metric="watts",
        value=900.0,
    )
    assert obs.ts_utc == utc(2026, 8, 16, 18, 0, 30)
    assert obs.ts_local == naive(2026, 8, 16, 14, 0, 30)


def test_observation_is_frozen(make_obs: ObservationFactory) -> None:
    obs = make_obs()
    with pytest.raises(Exception):
        obs.value = 1.0  # type: ignore[misc]


def test_tz_aware_ts_local_is_rejected_when_building_a_table(make_obs: ObservationFactory) -> None:
    obs = make_obs()
    broken = Observation(
        ts_utc=obs.ts_utc,
        ts_local=obs.ts_utc,  # aware — wrong for this column
        source=obs.source,
        device_id=obs.device_id,
        channel_id=obs.channel_id,
        metric=obs.metric,
        value=obs.value,
        unit=obs.unit,
    )
    with pytest.raises(ValueError, match="naive local wall clock"):
        model.observations_to_table([broken])
