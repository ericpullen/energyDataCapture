"""Tests for the S3 layout and the atomic-write / verify gate (PLAN.md §4, §10).

Everything here runs offline against ``moto`` — no real AWS, no credentials.

The path-builder tests pin the **literal** keys from PLAN.md §4. They are meant
to fail loudly if a filename convention drifts: those names are the contract that
makes re-runs idempotent (a deterministic name overwrites instead of duplicating)
and that the Glue partition projection templates depend on.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from tenacity import wait_none

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from tests.conftest import BUCKET

UTC = timezone.utc

# The moto-backed ``s3`` fixture and ``BUCKET`` live in tests/conftest.py — three
# byte-identical copies of that fixture used to live in this file, test_uploader
# and test_compactor. :func:`test_get_client_is_lazy_and_cached` covers the
# default (non-injected) client path separately.


def _observations(
    start: datetime, count: int, *, channels: tuple[str, ...] = ("breaker_p11",)
) -> list[model.Observation]:
    out: list[model.Observation] = []
    for step in range(count):
        ts = start + timedelta(seconds=30 * step)
        for channel in channels:
            out.append(
                model.make_observation(
                    ts_utc=ts,
                    source=model.SOURCE_LEVITON,
                    device_id="4C45565275C6",
                    channel_id=channel,
                    metric="watts",
                    value=100.0 + step,
                )
            )
    return out


def _table(count: int = 6, *, start: datetime | None = None) -> pa.Table:
    start = start or datetime(2026, 8, 16, 18, 0, tzinfo=UTC)
    return model.observations_to_table(
        _observations(start, count, channels=("breaker_p11", "ct_1_a"))
    )


def _all_keys(client) -> list[str]:
    return s3io.list_keys(BUCKET, "", client=client)


# ------------------------------------------------------------ path builders


def test_path_builders_pin_the_plan_section_4_layout():
    day = date(2026, 8, 16)

    assert s3io.raw_30s_day_prefix(day) == "energy/raw_30s/year=2026/month=08/day=16/"
    assert (
        s3io.raw_30s_part_key(day, 14)
        == "energy/raw_30s/year=2026/month=08/day=16/part-20260816T14.parquet"
    )
    assert (
        s3io.raw_30s_part_key(day, 0)
        == "energy/raw_30s/year=2026/month=08/day=16/part-20260816T00.parquet"
    )
    assert (
        s3io.raw_30s_day_key(day)
        == "energy/raw_30s/year=2026/month=08/day=16/day-20260816.parquet"
    )
    assert (
        s3io.raw_30s_archive_day_prefix(day)
        == "energy/raw_30s_parts_archive/year=2026/month=08/day=16/"
    )
    assert (
        s3io.raw_30s_archive_key(day, s3io.raw_30s_part_key(day, 14))
        == "energy/raw_30s_parts_archive/year=2026/month=08/day=16/part-20260816T14.parquet"
    )
    assert s3io.hourly_month_prefix(day) == "energy/hourly/year=2026/month=08/"
    assert s3io.hourly_key(day) == "energy/hourly/year=2026/month=08/rollup-20260816.parquet"
    assert s3io.daily_year_prefix(day) == "energy/daily/year=2026/"
    assert s3io.daily_key(day) == "energy/daily/year=2026/bryant-202608.parquet"
    assert s3io.daily_key(date(2026, 1, 3)) == "energy/daily/year=2026/bryant-202601.parquet"
    assert s3io.dim_channel_key() == "energy/dim_channel/dim_channel.parquet"
    assert s3io.meter_year_prefix(day) == "energy/meter/year=2026/"
    assert s3io.meter_key(date(2026, 1, 3)) == "energy/meter/year=2026/lge-202601.parquet"
    assert (
        s3io.raw_30s_day_glob_uri(BUCKET, day)
        == f"s3://{BUCKET}/energy/raw_30s/year=2026/month=08/day=16/*.parquet"
    )


def test_part_key_for_ts_partitions_on_the_local_date():
    # 2026-08-17T02:30Z is still 2026-08-16 22:30 local -> yesterday's partition.
    ts = datetime(2026, 8, 17, 2, 30, tzinfo=UTC)
    assert (
        s3io.raw_30s_part_key_for_ts(ts)
        == "energy/raw_30s/year=2026/month=08/day=16/part-20260816T22.parquet"
    )


def test_part_key_rejects_an_impossible_hour():
    with pytest.raises(ValueError):
        s3io.raw_30s_part_key(date(2026, 8, 16), 24)


def test_dst_days_produce_the_expected_part_files():
    spring = date(2026, 3, 8)  # 23-hour local day: wall-clock hour 02 never happens
    fall = date(2026, 11, 1)  # 25-hour local day: wall-clock hour 01 happens twice

    spring_keys = [s3io.raw_30s_part_key(spring, h) for h in timeutil.local_wall_hours_of_day(spring)]
    assert len(spring_keys) == 23
    assert "energy/raw_30s/year=2026/month=03/day=08/part-20260308T02.parquet" not in spring_keys

    fall_keys = [s3io.raw_30s_part_key(fall, h) for h in timeutil.local_wall_hours_of_day(fall)]
    # 25 physical hours, but 24 wall-clock labels: hour 01 is ONE part file
    # holding both occurrences, kept distinct inside it by ts_utc.
    assert len(fall_keys) == 24
    assert len(set(fall_keys)) == 24
    both_01 = {
        s3io.raw_30s_part_key_for_ts(datetime(2026, 11, 1, 5, 30, tzinfo=UTC)),  # 01:30 EDT
        s3io.raw_30s_part_key_for_ts(datetime(2026, 11, 1, 6, 30, tzinfo=UTC)),  # 01:30 EST
    }
    assert both_01 == {"energy/raw_30s/year=2026/month=11/day=01/part-20261101T01.parquet"}


def test_temp_keys_live_outside_every_tabled_prefix_and_are_unique():
    final = s3io.raw_30s_part_key(date(2026, 8, 16), 14)
    first = s3io.temp_key(final)
    second = s3io.temp_key(final)

    assert first.startswith("energy/_tmp/")
    assert first.endswith("part-20260816T14.parquet")
    assert first != second
    for tabled in (s3io.RAW_30S_PREFIX, s3io.HOURLY_PREFIX, s3io.DAILY_PREFIX, s3io.METER_PREFIX):
        assert not first.startswith(tabled)


def test_s3_uri_roundtrip():
    key = s3io.hourly_key(date(2026, 8, 16))
    assert s3io.parse_s3_uri(s3io.s3_uri(BUCKET, key)) == (BUCKET, key)
    with pytest.raises(ValueError):
        s3io.parse_s3_uri("/energy/hourly")


# ------------------------------------------------------------ atomic writing


def test_write_table_atomic_leaves_nothing_at_the_temp_key(s3):
    table = _table()
    key = s3io.raw_30s_part_key(date(2026, 8, 16), 14)

    result = s3io.write_table_atomic(table, BUCKET, key, client=s3)

    assert result.rows == table.num_rows
    assert result.verified is True
    assert _all_keys(s3) == [key]  # exactly one object: no temp, no debris
    assert s3io.list_keys(BUCKET, s3io.TMP_PREFIX, client=s3) == []
    assert not s3io.key_exists(BUCKET, result.temp_key, client=s3)

    written = s3io.read_table(BUCKET, key, client=s3)
    assert written.equals(table)


def test_write_table_atomic_uses_zstd(s3):
    key = s3io.raw_30s_part_key(date(2026, 8, 16), 15)
    s3io.write_table_atomic(_table(), BUCKET, key, client=s3)

    metadata = s3io.read_parquet_metadata(BUCKET, key, client=s3)
    compressions = {
        metadata.row_group(g).column(c).compression
        for g in range(metadata.num_row_groups)
        for c in range(metadata.num_columns)
    }
    assert compressions == {"ZSTD"}


def test_write_table_atomic_rerun_is_byte_identical(s3):
    key = s3io.raw_30s_day_key(date(2026, 8, 16))
    table = _table()

    s3io.write_table_atomic(table, BUCKET, key, client=s3)
    first = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    s3io.write_table_atomic(table, BUCKET, key, client=s3)
    second = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    assert first == second
    assert _all_keys(s3) == [key]


def test_write_table_atomic_refuses_unsorted_rows_and_writes_nothing(s3):
    table = _table()
    reversed_table = table.take(pa.array(list(reversed(range(table.num_rows)))))
    key = s3io.raw_30s_part_key(date(2026, 8, 16), 16)

    with pytest.raises(s3io.UnsortedRowsError):
        s3io.write_table_atomic(reversed_table, BUCKET, key, client=s3)

    assert _all_keys(s3) == []


def test_write_table_atomic_checks_the_hourly_sort_key(s3):
    hour = datetime(2026, 8, 16, 18, tzinfo=UTC)
    rows = [
        {
            "hour_start_utc": hour + timedelta(hours=offset),
            "local_hour_start": timeutil.local_hour_start(hour + timedelta(hours=offset)),
            "source": model.SOURCE_LEVITON,
            "device_id": "4C45565275C6",
            "channel_id": "breaker_p11",
            "metric": "watts",
            "unit": "W",
            "mean": 100.0,
            "min": 90.0,
            "max": 110.0,
            "p95": 109.0,
            "sample_count": 120,
            "first_ts_utc": hour + timedelta(hours=offset),
            "last_ts_utc": hour + timedelta(hours=offset, minutes=59),
            "kwh": 0.2,
        }
        for offset in (0, 1)
    ]
    ordered = pa.Table.from_pylist(rows, schema=model.HOURLY_SCHEMA)
    key = s3io.hourly_key(date(2026, 8, 16))

    s3io.write_table_atomic(ordered, BUCKET, key, client=s3)
    assert s3io.verify_row_count(BUCKET, key, 2, client=s3)

    with pytest.raises(s3io.UnsortedRowsError):
        s3io.write_table_atomic(
            pa.Table.from_pylist(list(reversed(rows)), schema=model.HOURLY_SCHEMA),
            BUCKET,
            s3io.hourly_key(date(2026, 8, 17)),
            client=s3,
        )


def test_write_table_atomic_skips_the_sort_check_for_dim_channel(s3):
    dim = pa.table(
        {
            "source": ["leviton", "bryant"],
            "device_id": ["4C45565275C6", "4022W200213"],
            "channel_id": ["breaker_p11", "hpheat"],
            "label": ["Dryer", "Heat pump - heating"],
        }
    )
    key = s3io.dim_channel_key()

    result = s3io.write_table_atomic(dim, BUCKET, key, client=s3)

    assert result.rows == 2
    assert _all_keys(s3) == [key]


# ------------------------------------------------------------------- verify


def test_verify_row_count_agrees_with_what_was_written(s3):
    table = _table(count=10)
    key = s3io.raw_30s_part_key(date(2026, 8, 16), 17)
    result = s3io.write_table_atomic(table, BUCKET, key, client=s3)

    assert result.rows == 20  # 10 timestamps x 2 channels
    assert s3io.verify_row_count(BUCKET, key, 20, client=s3) is True
    assert s3io.verify_row_count(BUCKET, key, 19, client=s3) is False
    assert s3io.require_row_count(BUCKET, key, 20, client=s3) == 20
    with pytest.raises(s3io.RowCountMismatch):
        s3io.require_row_count(BUCKET, key, 21, client=s3)


def test_verify_row_count_of_a_missing_object_is_false_not_an_exception(s3):
    missing = s3io.raw_30s_day_key(date(2001, 1, 1))
    assert s3io.verify_row_count(BUCKET, missing, 1, client=s3) is False


def test_verify_reads_only_the_parquet_footer(s3):
    # Random doubles defeat compression, so the object is comfortably larger than
    # the footer the reader needs. If verify ever downloads whole objects, this
    # is the test that notices.
    count = 60_000
    rng = random.Random(7)
    base = datetime(2026, 8, 16, 4, 0, tzinfo=UTC)
    stamps = [base + timedelta(seconds=i) for i in range(count)]
    table = pa.Table.from_pydict(
        {
            "ts_utc": stamps,
            "ts_local": [timeutil.to_local_naive(t) for t in stamps],
            "source": ["leviton"] * count,
            "device_id": ["4C45565275C6"] * count,
            "channel_id": ["breaker_p11"] * count,
            "metric": ["watts"] * count,
            "value": [rng.random() * 3000.0 for _ in range(count)],
            "unit": ["W"] * count,
        },
        schema=model.RAW_30S_SCHEMA,
    )
    key = s3io.raw_30s_day_key(date(2026, 8, 16))
    s3io.write_table_atomic(table, BUCKET, key, client=s3)

    reader = s3io.S3RangeReader(BUCKET, key, client=s3)
    try:
        assert pq.ParquetFile(reader).metadata.num_rows == count
        assert reader.size() > 200_000
        assert reader.bytes_fetched < reader.size() / 2
    finally:
        reader.close()

    assert s3io.verify_row_count(BUCKET, key, count, client=s3) is True


# ---------------------------------------------------------- listing / verbs


def test_list_raw_30s_parts_ignores_the_day_file(s3):
    day = date(2026, 8, 16)
    for hour in (13, 14):
        s3io.write_table_atomic(_table(), BUCKET, s3io.raw_30s_part_key(day, hour), client=s3)
    s3io.write_table_atomic(_table(), BUCKET, s3io.raw_30s_day_key(day), client=s3)

    parts = s3io.list_raw_30s_parts(BUCKET, day, client=s3)

    assert parts == [s3io.raw_30s_part_key(day, 13), s3io.raw_30s_part_key(day, 14)]
    assert len(s3io.list_keys(BUCKET, s3io.raw_30s_day_prefix(day), client=s3)) == 3


def test_key_exists_copy_and_delete(s3):
    day = date(2026, 8, 16)
    src = s3io.raw_30s_part_key(day, 9)
    dst = s3io.raw_30s_archive_key(day, src)
    s3io.write_table_atomic(_table(), BUCKET, src, client=s3)

    assert s3io.key_exists(BUCKET, src, client=s3)
    assert not s3io.key_exists(BUCKET, dst, client=s3)

    s3io.copy_key(BUCKET, src, dst, client=s3)
    assert s3io.key_exists(BUCKET, dst, client=s3)

    s3io.delete_key(BUCKET, src, client=s3)
    assert not s3io.key_exists(BUCKET, src, client=s3)
    s3io.delete_key(BUCKET, src, client=s3)  # deleting twice is not an error


def test_read_table_roundtrips_observations(s3):
    table = _table(count=3)
    key = s3io.daily_key(date(2026, 8, 16))
    s3io.write_table_atomic(table, BUCKET, key, client=s3)

    back = s3io.read_table(BUCKET, key, client=s3)
    assert back.schema.equals(model.RAW_30S_SCHEMA)
    assert model.table_to_observations(back) == model.table_to_observations(table)


# -------------------------------------------------------------------- moves


def _write_parts(s3, day: date, hours: tuple[int, ...]) -> list[str]:
    keys = []
    for hour in hours:
        key = s3io.raw_30s_part_key(day, hour)
        s3io.write_table_atomic(_table(count=hour + 1), BUCKET, key, client=s3)
        keys.append(key)
    return keys


def test_move_keys_archives_parts(s3):
    day = date(2026, 8, 16)
    parts = _write_parts(s3, day, (10, 11, 12))
    archive = s3io.raw_30s_archive_day_prefix(day)

    result = s3io.move_keys(BUCKET, parts, archive, client=s3)

    assert len(result.moved) == 3
    assert result.already_moved == []
    assert result.complete
    assert result.destinations == [s3io.raw_30s_archive_key(day, p) for p in parts]
    assert s3io.list_raw_30s_parts(BUCKET, day, client=s3) == []
    assert s3io.list_keys(BUCKET, archive, client=s3) == sorted(result.destinations)


def test_move_keys_is_idempotent_after_a_partial_move(s3):
    day = date(2026, 8, 16)
    parts = _write_parts(s3, day, (10, 11, 12))
    archive = s3io.raw_30s_archive_day_prefix(day)

    # Simulated partial failure #1: the run died after moving only the first key.
    s3io.move_keys(BUCKET, parts[:1], archive, client=s3)
    # Simulated partial failure #2: a copy succeeded but its delete never ran, so
    # the second key currently exists at BOTH ends.
    s3io.copy_key(BUCKET, parts[1], s3io.raw_30s_archive_key(day, parts[1]), client=s3)

    result = s3io.move_keys(BUCKET, parts, archive, client=s3)

    assert [src for src, _ in result.already_moved] == parts[:2]
    assert [src for src, _ in result.moved] == parts[2:]
    assert result.missing == []
    assert s3io.list_raw_30s_parts(BUCKET, day, client=s3) == []
    assert s3io.list_keys(BUCKET, archive, client=s3) == sorted(
        s3io.raw_30s_archive_key(day, p) for p in parts
    )

    # And a third, fully redundant run changes nothing at all.
    again = s3io.move_keys(BUCKET, parts, archive, client=s3)
    assert len(again.already_moved) == 3
    assert again.moved == []
    assert s3io.list_keys(BUCKET, archive, client=s3) == sorted(
        s3io.raw_30s_archive_key(day, p) for p in parts
    )


def test_move_keys_reports_a_key_missing_at_both_ends(s3):
    day = date(2026, 8, 16)
    ghost = s3io.raw_30s_part_key(day, 23)

    result = s3io.move_keys(BUCKET, [ghost], s3io.raw_30s_archive_day_prefix(day), client=s3)

    assert result.missing == [ghost]
    assert result.moved == []
    assert not result.complete


def test_move_keys_rejects_a_destination_equal_to_the_source(s3):
    day = date(2026, 8, 16)
    part = s3io.raw_30s_part_key(day, 8)
    with pytest.raises(ValueError):
        s3io.move_keys(BUCKET, [part], s3io.raw_30s_day_prefix(day), client=s3)


# ------------------------------------------------------------------ retries


class _FlakyClient:
    """Fails ``fails`` times with a 5xx, then succeeds; 404s on head_object."""

    def __init__(self, fails: int) -> None:
        self.fails = fails
        self.attempts = 0
        self.heads = 0

    def put_object(self, **_kwargs):
        self.attempts += 1
        if self.attempts <= self.fails:
            raise ClientError(
                {
                    "Error": {"Code": "InternalError", "Message": "transient"},
                    "ResponseMetadata": {"HTTPStatusCode": 500},
                },
                "PutObject",
            )
        return {}

    def head_object(self, **_kwargs):
        self.heads += 1
        raise ClientError(
            {
                "Error": {"Code": "404", "Message": "not found"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            "HeadObject",
        )


@pytest.fixture
def no_retry_sleep(monkeypatch):
    monkeypatch.setattr(s3io._put_object.retry, "wait", wait_none())
    monkeypatch.setattr(s3io._head_object.retry, "wait", wait_none())


def test_transient_errors_are_retried(no_retry_sleep):
    flaky = _FlakyClient(fails=2)
    s3io._put_object(flaky, BUCKET, "energy/_tmp/x.parquet", b"payload")
    assert flaky.attempts == 3


def test_transient_errors_eventually_give_up(no_retry_sleep):
    hopeless = _FlakyClient(fails=99)
    with pytest.raises(ClientError):
        s3io._put_object(hopeless, BUCKET, "energy/_tmp/x.parquet", b"payload")
    assert hopeless.attempts == s3io.S3_RETRY_ATTEMPTS


def test_a_404_is_not_retried(no_retry_sleep):
    """A missing object is an answer, not a failure — retrying it wastes minutes."""
    flaky = _FlakyClient(fails=0)
    assert s3io.key_exists(BUCKET, "energy/nope.parquet", client=flaky) is False
    assert flaky.heads == 1


# ------------------------------------------------------------------ clients


def test_get_client_is_lazy_and_cached(s3):
    first = s3io.get_client()
    assert first is s3io.get_client()
    s3io.reset_clients()
    assert s3io.get_client() is not first
