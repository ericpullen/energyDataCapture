"""Tests for the daily compactor (PLAN.md §10, §15.2, §15.7).

Everything here runs offline against ``moto`` — no AWS, no network.

The property these tests exist to protect is the one PLAN.md §10 spends a
paragraph resolving: **parts and the day file must never coexist under the
queried ``raw_30s`` prefix**, because the day file is built from the parts and a
query that saw both would double-count every row of that day. The compactor
buys that property with a strict order — write, verify, *then* archive — so the
tests are mostly about what happens when a step fails or is interrupted:

* verify fails            -> nothing archived, nothing deleted (§15.7)
* crash mid-move          -> re-run converges, never loses an object
* overlapping parts       -> collapse on the canonical dedupe key (§15.2)
* after compaction        -> exactly one authoritative set under ``raw_30s``
* archived parts          -> survive the 7-day window, then are deleted
* already-compacted day   -> no-op that still verifies
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pyarrow as pa
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore
from energy_capture.stages import compactor
from tests.conftest import BUCKET


#: A plain summer local day: 24 hours, EDT (UTC-4).
DAY = date(2026, 8, 16)

DEVICE = "4C45565275C6"


# --------------------------------------------------------------------- setup


# The moto-backed ``s3`` fixture and ``BUCKET`` now live in tests/conftest.py.


@pytest.fixture
def store(tmp_path) -> StatusStore:
    """A StatusStore in a temp dir, so tests never touch a shared status.json."""
    return StatusStore(path=tmp_path / "status.json", load_existing=False)


# ------------------------------------------------------------------ helpers


def obs(
    hour: int,
    minute: int = 0,
    second: int = 0,
    *,
    day: date = DAY,
    channel: str = "breaker_p11",
    metric: str = "watts",
    value: float = 100.0,
) -> model.Observation:
    """One observation inside local wall-clock ``hour`` of ``day``."""
    start, _ = timeutil.local_hour_bounds_utc(day, hour)
    return model.make_observation(
        ts_utc=start + timedelta(minutes=minute, seconds=second),
        source=model.SOURCE_LEVITON,
        device_id=DEVICE,
        channel_id=channel,
        metric=metric,
        value=value,
    )


def write_part(s3, hour: int, rows, *, day: date = DAY) -> str:
    """Write ``rows`` as the hourly part the uploader would have produced."""
    key = s3io.raw_30s_part_key(day, hour)
    s3io.write_table_atomic(model.observations_to_table(rows), BUCKET, key, client=s3)
    return key


def raw_keys(s3, day: date = DAY) -> list[str]:
    return s3io.list_keys(BUCKET, s3io.raw_30s_day_prefix(day), client=s3)


def archive_keys(s3, day: date = DAY) -> list[str]:
    return s3io.list_keys(BUCKET, s3io.raw_30s_archive_day_prefix(day), client=s3)


def basenames(keys) -> list[str]:
    return sorted(key.rsplit("/", 1)[-1] for key in keys)


def at_0130_local(day: date) -> datetime:
    """The instant the ~01:30 scheduled compaction fires on ``day``."""
    return timeutil.local_naive_to_utc(datetime.combine(day, time(1, 30)))


def day_table(s3, day: date = DAY) -> pa.Table:
    return s3io.read_table(BUCKET, s3io.raw_30s_day_key(day), client=s3)


def two_parts(s3) -> None:
    """The normal input: two hourly parts, two rows each, no overlap."""
    write_part(s3, 0, [obs(0, 0, value=100.0), obs(0, 30, value=110.0)])
    write_part(s3, 1, [obs(1, 0, value=120.0), obs(1, 30, value=130.0)])


def compact(s3, store, *, day: date = DAY, now: datetime | None = None, **kwargs):
    return compactor.run(
        start=kwargs.pop("start", day),
        end=kwargs.pop("end", day),
        bucket=BUCKET,
        client=s3,
        store=store,
        now=now if now is not None else at_0130_local(day + timedelta(days=1)),
        **kwargs,
    )


# ------------------------------------------------------------ the happy path


def test_compaction_writes_one_day_file_and_archives_the_parts(s3, store):
    two_parts(s3)

    summary = compact(s3, store)

    day_key = s3io.raw_30s_day_key(DAY)
    assert raw_keys(s3) == [day_key], "raw_30s must hold the day file and nothing else"
    assert basenames(archive_keys(s3)) == [
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
    ]
    assert s3io.parquet_row_count(BUCKET, day_key, client=s3) == 4
    assert summary["rows"] == 4
    assert summary["compacted"] == 1
    assert summary["parts_archived"] == 2
    assert summary["failed"] == 0


def test_raw_prefix_holds_exactly_one_authoritative_set(s3, store):
    """The double-count trap: summing every raw_30s object must not overcount."""
    two_parts(s3)
    compact(s3, store)

    everything = s3io.list_keys(BUCKET, f"{s3io.RAW_30S_PREFIX}/", client=s3)
    assert everything == [s3io.raw_30s_day_key(DAY)]

    # What a query engine scanning the prefix would see, row for row.
    scanned = sum(s3io.parquet_row_count(BUCKET, key, client=s3) for key in everything)
    assert scanned == 4  # not 8

    # And the archive is a sibling prefix, outside the table's location.
    assert all(key.startswith(f"{s3io.ARCHIVE_PREFIX}/") for key in archive_keys(s3))


def test_day_file_is_sorted_and_deduped_on_the_canonical_keys(s3, store):
    write_part(s3, 1, [obs(1, 30, value=130.0), obs(1, 0, value=120.0)])
    write_part(s3, 0, [obs(0, 30, value=110.0), obs(0, 0, value=100.0)])

    compact(s3, store)

    table = day_table(s3)
    stamps = table.column("ts_utc").to_pylist()
    assert stamps == sorted(stamps)
    assert table.column("value").to_pylist() == [100.0, 110.0, 120.0, 130.0]
    assert table.schema.equals(model.RAW_30S_SCHEMA)


# ------------------------------------------------------------------ dedupe


def test_overlapping_parts_collapse_to_one_row_each(s3, store):
    """PLAN.md §15.2: identical dedupe keys collapse; the first part wins."""
    shared = obs(0, 30, value=110.0)
    # A re-uploaded/overlapping part carrying the same (ts, source, device,
    # channel, metric) key with a different value.
    conflicting = obs(0, 30, value=999.0)

    write_part(s3, 0, [obs(0, 0, value=100.0), shared])
    write_part(s3, 1, [conflicting, obs(1, 0, value=120.0)])

    summary = compact(s3, store)

    table = day_table(s3)
    assert table.num_rows == 3
    assert summary["rows"] == 3
    keys = list(
        zip(
            table.column("ts_utc").to_pylist(),
            table.column("channel_id").to_pylist(),
            table.column("metric").to_pylist(),
            strict=True,
        )
    )
    assert len(set(keys)) == 3
    # Parts are read in key order, so part-…T00 supplies the surviving row.
    assert table.column("value").to_pylist() == [100.0, 110.0, 120.0]


def test_existing_day_file_and_a_late_part_merge(s3, store):
    two_parts(s3)
    compact(s3, store)

    # A late upload lands after the day was compacted.
    write_part(s3, 2, [obs(2, 0, value=140.0)])
    summary = compact(s3, store)

    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert day_table(s3).num_rows == 5
    assert summary["rows"] == 5
    assert basenames(archive_keys(s3)) == [
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
        "part-20260816T02.parquet",
    ]


def test_late_part_that_adds_nothing_is_still_archived(s3, store):
    two_parts(s3)
    compact(s3, store)

    # Same rows re-uploaded: the day file is already correct, but the part must
    # not be left beside it.
    write_part(s3, 0, [obs(0, 0, value=100.0), obs(0, 30, value=110.0)])
    summary = compact(s3, store)

    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert summary["rows"] == 4
    assert summary["parts_archived"] == 1


# ---------------------------------------------------------------- idempotency


def test_recompacting_a_compacted_day_is_a_noop_that_still_verifies(s3, store, monkeypatch):
    two_parts(s3)
    compact(s3, store)

    day_key = s3io.raw_30s_day_key(DAY)
    etag_before = s3.head_object(Bucket=BUCKET, Key=day_key)["ETag"]

    writes: list[str] = []
    real_write = s3io.write_table_atomic

    def spy_write(table, bucket, key, **kwargs):
        writes.append(key)
        return real_write(table, bucket, key, **kwargs)

    verifies: list[tuple[str, int]] = []
    real_verify = s3io.verify_row_count

    def spy_verify(bucket, key, expected, **kwargs):
        verifies.append((key, expected))
        return real_verify(bucket, key, expected, **kwargs)

    monkeypatch.setattr(s3io, "write_table_atomic", spy_write)
    monkeypatch.setattr(s3io, "verify_row_count", spy_verify)

    summary = compact(s3, store)

    assert writes == [], "an already-compacted day must not be rewritten"
    assert (day_key, 4) in verifies, "…but it must still be verified"
    assert s3.head_object(Bucket=BUCKET, Key=day_key)["ETag"] == etag_before
    assert summary["rows"] == 4
    assert summary["parts_archived"] == 0
    assert raw_keys(s3) == [day_key]


def test_a_day_with_no_parts_and_no_day_file_is_a_harmless_noop(s3, store):
    summary = compact(s3, store)

    assert summary["nothing_to_do"] == 1
    assert summary["compacted"] == 0
    assert s3io.list_keys(BUCKET, "energy/", client=s3) == []


def test_default_window_is_yesterday(s3, store):
    two_parts(s3)

    summary = compactor.run(
        bucket=BUCKET,
        client=s3,
        store=store,
        now=at_0130_local(DAY + timedelta(days=1)),
    )

    assert summary["start"] == DAY.isoformat()
    assert summary["end"] == DAY.isoformat()
    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]


def test_a_range_compacts_every_day_in_it(s3, store):
    other = DAY + timedelta(days=1)
    write_part(s3, 0, [obs(0, 0, value=100.0)])
    write_part(s3, 0, [obs(0, 0, day=other, value=200.0)], day=other)

    summary = compactor.run(
        start=DAY,
        end=other,
        bucket=BUCKET,
        client=s3,
        store=store,
        now=at_0130_local(other + timedelta(days=1)),
    )

    assert summary["days"] == 2
    assert summary["compacted"] == 2
    assert raw_keys(s3, DAY) == [s3io.raw_30s_day_key(DAY)]
    assert raw_keys(s3, other) == [s3io.raw_30s_day_key(other)]


# ------------------------------------------------------------- verify safety


def test_parts_are_not_archived_when_the_staged_verify_fails(s3, store, monkeypatch):
    """§15.7: a bad row count means nothing lands and nothing is archived."""
    two_parts(s3)

    monkeypatch.setattr(s3io, "parquet_row_count", lambda *a, **k: 999_999)

    with pytest.raises(compactor.CompactionError):
        compact(s3, store)

    assert basenames(raw_keys(s3)) == [
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
    ], "parts must stay where queries can still see them"
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_day_key(DAY), client=s3)
    assert archive_keys(s3) == []
    assert store.section("compactor")["consecutive_failures"] == 1


def test_parts_are_not_archived_when_the_final_verify_fails(s3, store, monkeypatch):
    """The day file may exist, but an unverified one never blesses an archive."""
    two_parts(s3)

    monkeypatch.setattr(s3io, "verify_row_count", lambda *a, **k: False)

    with pytest.raises(compactor.CompactionError):
        compact(s3, store)

    assert basenames(raw_keys(s3)) == [
        "day-20260816.parquet",
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
    ]
    assert archive_keys(s3) == []


def test_a_verify_failure_recovers_on_the_next_run(s3, store, monkeypatch):
    two_parts(s3)
    broken = {"yet": True}
    real_verify = s3io.verify_row_count

    def flaky_verify(bucket, key, expected, **kwargs):
        if broken["yet"]:
            return False
        return real_verify(bucket, key, expected, **kwargs)

    monkeypatch.setattr(s3io, "verify_row_count", flaky_verify)

    with pytest.raises(compactor.CompactionError):
        compact(s3, store)

    broken["yet"] = False
    summary = compact(s3, store)

    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert summary["rows"] == 4
    assert store.section("compactor")["consecutive_failures"] == 0


def test_a_foreign_object_in_the_partition_stops_the_day(s3, store):
    two_parts(s3)
    bogus = pa.table({"nonsense": pa.array([1, 2, 3])})
    s3io.write_table_atomic(
        bogus,
        BUCKET,
        s3io.raw_30s_day_prefix(DAY) + "part-20260816T09.parquet",
        sort_key=(),
        client=s3,
    )

    with pytest.raises(compactor.CompactionError):
        compact(s3, store)

    assert not s3io.key_exists(BUCKET, s3io.raw_30s_day_key(DAY), client=s3)
    assert archive_keys(s3) == []


def test_one_failed_day_does_not_strand_the_others(s3, store):
    other = DAY + timedelta(days=1)
    write_part(s3, 0, [obs(0, 0, value=100.0)])
    write_part(s3, 0, [obs(0, 0, day=other, value=200.0)], day=other)
    s3io.write_table_atomic(
        pa.table({"nonsense": pa.array([1])}),
        BUCKET,
        s3io.raw_30s_day_prefix(DAY) + "part-20260816T09.parquet",
        sort_key=(),
        client=s3,
    )

    with pytest.raises(compactor.CompactionError):
        compactor.run(
            start=DAY,
            end=other,
            bucket=BUCKET,
            client=s3,
            store=store,
            now=at_0130_local(other + timedelta(days=1)),
        )

    assert raw_keys(s3, other) == [s3io.raw_30s_day_key(other)]
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_day_key(DAY), client=s3)


# ----------------------------------------------------------- crash recovery


def test_rerun_after_a_crash_mid_move_converges(s3, store, monkeypatch):
    """A crash between the copy and the delete leaves a duplicate, never a loss."""
    two_parts(s3)
    part0 = s3io.raw_30s_part_key(DAY, 0)

    crashed = {"yet": False}
    real_move = s3io.move_keys

    def flaky_move(bucket, src_keys, dst_prefix, *, client=None):
        keys = list(src_keys)
        if not crashed["yet"]:
            crashed["yet"] = True
            first = keys[0]
            s3io.copy_key(
                bucket,
                first,
                dst_prefix + first.rsplit("/", 1)[-1],
                client=client,
            )
            raise ConnectionError("simulated crash after copy, before delete")
        return real_move(bucket, src_keys, dst_prefix, client=client)

    monkeypatch.setattr(s3io, "move_keys", flaky_move)

    with pytest.raises(compactor.CompactionError):
        compact(s3, store)

    # Mid-crash state: the day file is written, the first part exists at BOTH
    # ends (duplicated, not lost), and both parts are still under raw_30s.
    assert s3io.key_exists(BUCKET, part0, client=s3)
    assert basenames(archive_keys(s3)) == ["part-20260816T00.parquet"]
    assert basenames(raw_keys(s3)) == [
        "day-20260816.parquet",
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
    ]

    summary = compact(s3, store)

    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert basenames(archive_keys(s3)) == [
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
    ]
    assert summary["rows"] == 4
    assert day_table(s3).num_rows == 4


def test_parts_reappearing_mid_run_trigger_another_pass(s3, store, monkeypatch):
    """A racing uploader must never leave a part beside the day file."""
    two_parts(s3)

    raced = {"yet": False}
    real_move = s3io.move_keys

    def racing_move(bucket, src_keys, dst_prefix, *, client=None):
        result = real_move(bucket, src_keys, dst_prefix, client=client)
        if not raced["yet"]:
            raced["yet"] = True
            write_part(s3, 5, [obs(5, 0, value=150.0)])
        return result

    monkeypatch.setattr(s3io, "move_keys", racing_move)

    summary = compact(s3, store)

    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert summary["rows"] == 5
    assert basenames(archive_keys(s3)) == [
        "part-20260816T00.parquet",
        "part-20260816T01.parquet",
        "part-20260816T05.parquet",
    ]


def test_endless_reappearing_parts_fail_loudly(s3, store, monkeypatch):
    two_parts(s3)
    counter = {"n": 0}
    real_move = s3io.move_keys

    def always_racing(bucket, src_keys, dst_prefix, *, client=None):
        result = real_move(bucket, src_keys, dst_prefix, client=client)
        counter["n"] += 1
        write_part(s3, 5 + counter["n"], [obs(5 + counter["n"], 0, value=150.0)])
        return result

    monkeypatch.setattr(s3io, "move_keys", always_racing)

    with pytest.raises(compactor.CompactionError) as excinfo:
        compact(s3, store)

    # The run-level error names the day; its cause is the per-day give-up.
    assert "failed to compact" in str(excinfo.value)
    assert "compaction passes" in str(excinfo.value.__cause__)


# --------------------------------------------------------- archive retention


def test_archived_parts_survive_the_window_and_are_then_deleted(s3, store):
    two_parts(s3)
    compact(s3, store)
    assert len(archive_keys(s3)) == 2

    # Same day, three days later: still inside the 7-day safety window.
    summary = compact(s3, store, now=at_0130_local(DAY + timedelta(days=3)))
    assert summary["archived_parts_deleted"] == 0
    assert len(archive_keys(s3)) == 2

    # Seven days old: the day file exists and still passes the count check.
    summary = compact(s3, store, now=at_0130_local(DAY + timedelta(days=7)))
    assert summary["archived_parts_deleted"] == 2
    assert archive_keys(s3) == []
    assert raw_keys(s3) == [s3io.raw_30s_day_key(DAY)]
    assert day_table(s3).num_rows == 4
    assert store.section("compactor")["archived_parts_deleted"] == 2


def test_parts_archived_this_run_get_their_full_soak(s3, store):
    """A day compacted late must not lose its safety window in the same run."""
    two_parts(s3)

    summary = compact(s3, store, now=at_0130_local(DAY + timedelta(days=30)))

    assert summary["parts_archived"] == 2
    assert summary["archived_parts_deleted"] == 0
    assert len(archive_keys(s3)) == 2

    # The next run (nothing new to archive) is free to sweep it.
    summary = compact(s3, store, now=at_0130_local(DAY + timedelta(days=30)))
    assert summary["archived_parts_deleted"] == 2


def test_archive_is_kept_when_the_day_file_is_missing_rows(s3, store):
    two_parts(s3)
    compact(s3, store)

    # Corrupt the day file: drop a row that only the archive still holds.
    truncated = model.observations_to_table([obs(0, 0, value=100.0)])
    s3io.write_table_atomic(truncated, BUCKET, s3io.raw_30s_day_key(DAY), client=s3)

    summary = compact(
        s3,
        store,
        start=DAY + timedelta(days=1),  # do not recompact DAY; sweep only
        end=DAY + timedelta(days=1),
        now=at_0130_local(DAY + timedelta(days=10)),
    )

    assert summary["archived_parts_deleted"] == 0
    assert len(archive_keys(s3)) == 2, "the archive is the only copy of those rows"


def test_archive_is_kept_when_the_day_file_is_gone(s3, store):
    two_parts(s3)
    compact(s3, store)
    s3io.delete_key(BUCKET, s3io.raw_30s_day_key(DAY), client=s3)

    summary = compact(
        s3,
        store,
        start=DAY + timedelta(days=1),
        end=DAY + timedelta(days=1),
        now=at_0130_local(DAY + timedelta(days=10)),
    )

    assert summary["archived_parts_deleted"] == 0
    assert len(archive_keys(s3)) == 2


def test_sweep_can_be_disabled(s3, store):
    two_parts(s3)
    compact(s3, store)

    summary = compact(s3, store, now=at_0130_local(DAY + timedelta(days=10)), sweep=False)

    assert summary["archived_parts_deleted"] == 0
    assert len(archive_keys(s3)) == 2


def test_sweep_ignores_keys_it_does_not_understand(s3, store):
    two_parts(s3)
    compact(s3, store)
    stray = f"{s3io.ARCHIVE_PREFIX}/loose-note.parquet"
    s3.put_object(Bucket=BUCKET, Key=stray, Body=b"not parquet")

    compact(s3, store, now=at_0130_local(DAY + timedelta(days=10)))

    assert archive_keys(s3) == []
    assert s3io.key_exists(BUCKET, stray, client=s3), "an unparsable key is left alone"


# ------------------------------------------------------------------ status


def test_status_json_records_the_compaction(s3, store):
    two_parts(s3)
    compact(s3, store)

    section = store.section("compactor")
    assert section["last_day_compacted"] == "2026-08-16"
    assert section["rows"] == 4
    assert section["parts_archived"] == 2
    assert section["consecutive_failures"] == 0
    assert section["last_success_utc"] is not None
