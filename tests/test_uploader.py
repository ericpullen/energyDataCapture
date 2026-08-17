"""Tests for the hourly uploader (PLAN.md §10 "Uploader", §4, §15.6).

Everything here runs offline against ``moto`` — no real AWS, no credentials, no
network. Time is supplied explicitly (``now=``) rather than frozen, matching the
rest of the suite.

The properties these tests pin are the uploader's whole reason to exist:

* a single invocation catches up an arbitrary number of closed local hours;
* spool rows are marked uploaded **only after** the object's Parquet footer row
  count has been read back from S3 (PLAN.md §15.6) — a failed verify leaves them
  pending and the next run retries;
* the part file is rewritten from **every** row of the hour, not just the pending
  ones, so a partial failure can never replace a complete part with a shorter one;
* rows the poller appends mid-upload stay pending (max-rowid marking);
* the DST fall-back day's wall-clock hour 01 is one part file holding two
  physical hours of rows, kept distinct by ``ts_utc``;
* a re-run is byte-identical.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pyarrow.parquet as pq
import pyarrow as pa
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore, reset_status_store
from energy_capture.spool.sqlite import SpoolDB, open_spool
from energy_capture.stages import uploader
from tests.conftest import BUCKET


#: A boring summer local day (EDT, UTC-4).
DAY = date(2026, 8, 16)
#: DST boundaries in America/Kentucky/Louisville.
FALL_BACK = date(2026, 11, 1)
SPRING_FORWARD = date(2026, 3, 8)


# --------------------------------------------------------------------- setup


# The moto-backed ``s3`` fixture and ``BUCKET`` now live in tests/conftest.py.


@pytest.fixture
def spool(spool_dir) -> SpoolDB:
    """An empty spool in the per-test SPOOL_DIR."""
    db = open_spool(spool_dir / "spool.db")
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def status(spool_dir) -> StatusStore:
    """A per-test ``status.json`` installed as the process-wide store."""
    store = StatusStore(spool_dir / "status.json", load_existing=False)
    reset_status_store(store)
    try:
        yield store
    finally:
        reset_status_store(None)


# ------------------------------------------------------------------ helpers


def seed_hour(
    spool: SpoolDB,
    local_day: date,
    hour: int,
    *,
    samples: int = 4,
    channels: tuple[str, ...] = ("breaker_p11", "breaker_p12"),
    step: timedelta = timedelta(minutes=10),
    offset: timedelta = timedelta(0),
    base: float = 100.0,
) -> list[model.Observation]:
    """Append ``samples`` polls of ``channels`` into one local hour of the spool."""
    start_utc, _ = timeutil.local_hour_bounds_utc(local_day, hour)
    rows: list[model.Observation] = []
    for index in range(samples):
        ts = start_utc + offset + step * index
        for channel in channels:
            rows.append(
                model.make_observation(
                    ts_utc=ts,
                    source=model.SOURCE_LEVITON,
                    device_id="4C45565275C6",
                    channel_id=channel,
                    metric="watts",
                    value=base + index,
                )
            )
    spool.append(rows)
    return rows


def after(local_day: date, hour: int) -> datetime:
    """An instant just after wall-clock ``hour`` on ``local_day`` has closed."""
    _, end_utc = timeutil.local_hour_bounds_utc(local_day, hour)
    return end_utc + timedelta(seconds=1)


def read_part(s3, local_day: date, hour: int) -> pa.Table:
    key = s3io.raw_30s_part_key(local_day, hour)
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pq.read_table(pa.BufferReader(body))


def object_bytes(s3, key: str) -> bytes:
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def listed_keys(s3, prefix: str = "energy/") -> list[str]:
    return s3io.list_keys(BUCKET, prefix, client=s3)


def run(spool, s3, **kwargs):
    kwargs.setdefault("bucket", BUCKET)
    kwargs.setdefault("client", s3)
    return uploader.run(spool=spool, **kwargs)


# ------------------------------------------------------------ multi-hour catch-up


def test_multi_hour_catch_up_in_one_invocation(spool, s3, status):
    """Four hours of downtime is drained by one run; the open hour is left alone."""
    for hour in (10, 11, 12, 13):
        seed_hour(spool, DAY, hour, samples=3)
    # 14:30 local — hour 14 is still open and must not be uploaded.
    now = timeutil.local_naive_to_utc(datetime(2026, 8, 16, 14, 30))
    seed_hour(spool, DAY, 14, samples=1)

    summary = run(spool, s3, now=now)

    assert [r.label for r in summary.uploaded] == [
        "2026-08-16T10",
        "2026-08-16T11",
        "2026-08-16T12",
        "2026-08-16T13",
    ]
    assert summary.rows == 4 * 3 * 2
    assert summary.marked == summary.rows
    assert summary.ok

    keys = listed_keys(s3, s3io.raw_30s_day_prefix(DAY))
    assert keys == [
        s3io.raw_30s_part_key(DAY, hour) for hour in (10, 11, 12, 13)
    ]
    assert read_part(s3, DAY, 10).num_rows == 6

    # Only the open hour is still pending, and no part exists for it.
    assert spool.stats().pending_rows == 2
    assert spool.pending_rows_for_hour(DAY, 14) == 2
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 14), client=s3)


def test_part_lands_in_the_local_date_partition_with_the_plan_filename(spool, s3, status):
    """PLAN.md §4: part-{YYYYMMDD}T{HH}.parquet under the LOCAL date."""
    # 2026-08-16 21:00 local == 2026-08-17 01:00 UTC: the UTC date is the next
    # day, but partitioning is on the LOCAL date (CLAUDE.md rule 4).
    seed_hour(spool, DAY, 21, samples=2)
    run(spool, s3, now=after(DAY, 21))

    key = "energy/raw_30s/year=2026/month=08/day=16/part-20260816T21.parquet"
    assert s3io.key_exists(BUCKET, key, client=s3)
    table = read_part(s3, DAY, 21)
    assert table.column("ts_utc").to_pylist()[0].astimezone(timeutil.UTC).day == 17


def test_rows_are_deduped_and_sorted_in_the_part(spool, s3, status):
    seed_hour(spool, DAY, 9, samples=3, channels=("breaker_p12", "breaker_p11"))
    # A repeated poll of the same instant: the spool's UNIQUE index drops it, and
    # observations_to_table would too.
    seed_hour(spool, DAY, 9, samples=3, channels=("breaker_p12", "breaker_p11"))

    run(spool, s3, now=after(DAY, 9))
    table = read_part(s3, DAY, 9)

    assert table.num_rows == 6
    order = list(
        zip(
            table.column("ts_utc").to_pylist(),
            table.column("source").to_pylist(),
            table.column("device_id").to_pylist(),
            table.column("channel_id").to_pylist(),
        )
    )
    assert order == sorted(order)
    assert table.schema.equals(model.RAW_30S_SCHEMA)


def test_start_and_end_narrow_the_hours_considered(spool, s3, status):
    seed_hour(spool, DAY - timedelta(days=1), 22, samples=2)
    seed_hour(spool, DAY, 1, samples=2)
    now = after(DAY, 1)

    summary = run(spool, s3, start=DAY, end=DAY, now=now)

    assert [r.label for r in summary.uploaded] == ["2026-08-16T01"]
    # Yesterday's hour is untouched and still pending.
    assert spool.pending_rows_for_hour(DAY - timedelta(days=1), 22) == 4

    # A run with no range at all drains it too (the default catch-up window).
    summary = run(spool, s3, now=now)
    assert [r.label for r in summary.uploaded] == ["2026-08-15T22"]


def test_only_start_given_means_that_single_local_day(spool, s3, status):
    seed_hour(spool, DAY, 3, samples=1)
    summary = run(spool, s3, start=DAY, now=after(DAY, 3))
    assert [r.label for r in summary.uploaded] == ["2026-08-16T03"]


def test_inverted_range_is_rejected(spool, s3, status):
    with pytest.raises(ValueError, match="before start"):
        run(spool, s3, start=DAY, end=DAY - timedelta(days=1))


# ------------------------------------------------------ verify gate (§15.6)


def test_verify_failure_leaves_rows_pending_and_the_next_run_retries(
    spool, s3, status
):
    """Nothing is marked until the footer row count matches (PLAN.md §15.6)."""
    seed_hour(spool, DAY, 12, samples=3)
    now = after(DAY, 12)

    # A scoped context, not the shared `monkeypatch` fixture: undoing that one
    # would also undo the environment the s3 fixture and conftest installed.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(s3io, "verify_row_count", lambda *a, **k: False)
        with pytest.raises(uploader.UploadFailed) as excinfo:
            run(spool, s3, now=now)

        summary = excinfo.value.summary
        assert [r.label for r in summary.failed] == ["2026-08-16T12"]
        assert summary.uploaded == []
        assert summary.rows == 0
        # The object was written, but NOT a single spool row was marked.
        assert s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 12), client=s3)
        assert spool.pending_rows_for_hour(DAY, 12) == 6
        assert status.section("uploader")["consecutive_failures"] == 1

    summary = run(spool, s3, now=now)

    assert [r.label for r in summary.uploaded] == ["2026-08-16T12"]
    assert summary.marked == 6
    assert spool.pending_rows_for_hour(DAY, 12) == 0
    assert read_part(s3, DAY, 12).num_rows == 6
    assert status.section("uploader")["consecutive_failures"] == 0


def test_a_write_error_fails_only_its_own_hour(spool, s3, status):
    """One bad hour must not strand the rest of a catch-up."""
    for hour in (5, 6, 7):
        seed_hour(spool, DAY, hour, samples=2)

    real_write = s3io.write_table_atomic

    def flaky(table, bucket, key, **kwargs):
        if key.endswith("T06.parquet"):
            raise s3io.S3IOError("boom")
        return real_write(table, bucket, key, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(s3io, "write_table_atomic", flaky)
        with pytest.raises(uploader.UploadFailed) as excinfo:
            run(spool, s3, now=after(DAY, 7))

    summary = excinfo.value.summary
    assert [r.label for r in summary.uploaded] == ["2026-08-16T05", "2026-08-16T07"]
    assert [r.label for r in summary.failed] == ["2026-08-16T06"]
    assert spool.pending_rows_for_hour(DAY, 6) == 4
    assert spool.pending_rows_for_hour(DAY, 5) == 0
    assert spool.pending_rows_for_hour(DAY, 7) == 0


# -------------------------------------------- the whole hour is always rewritten


def test_late_rows_rewrite_the_whole_hour_not_just_the_pending_ones(spool, s3, status):
    """The deterministic filename means the write OVERWRITES.

    So the second upload of an hour must contain the already-uploaded rows too —
    otherwise a complete part is replaced by a shorter one (silent data loss).
    """
    seed_hour(spool, DAY, 8, samples=4)  # 8 rows
    now = after(DAY, 8)
    run(spool, s3, now=now)
    assert read_part(s3, DAY, 8).num_rows == 8

    # A late poll lands two more rows in that (now closed) hour.
    seed_hour(spool, DAY, 8, samples=1, offset=timedelta(minutes=55), base=999.0)
    assert spool.pending_rows_for_hour(DAY, 8) == 2

    summary = run(spool, s3, now=now)

    assert [r.label for r in summary.uploaded] == ["2026-08-16T08"]
    assert summary.rows == 10, "the part must be rewritten from ALL rows of the hour"
    assert summary.marked == 2, "only the two newly pending rows needed marking"
    table = read_part(s3, DAY, 8)
    assert table.num_rows == 10
    assert 999.0 in table.column("value").to_pylist()


def test_crash_between_write_and_mark_converges_on_the_next_run(spool, s3, status):
    """PLAN.md §15.6: rows written before a crash are uploaded after restart."""
    seed_hour(spool, DAY, 15, samples=3)
    now = after(DAY, 15)

    def die(*args, **kwargs):
        raise RuntimeError("power loss between verify and mark")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(spool, "mark_uploaded", die)
        with pytest.raises(RuntimeError, match="power loss"):
            run(spool, s3, now=now)

        key = s3io.raw_30s_part_key(DAY, 15)
        crashed_bytes = object_bytes(s3, key)
        assert spool.pending_rows_for_hour(DAY, 15) == 6, "nothing may be marked"

    summary = run(spool, s3, now=now)

    assert summary.rows == 6
    assert summary.marked == 6
    assert spool.pending_rows_for_hour(DAY, 15) == 0
    assert object_bytes(s3, key) == crashed_bytes, "convergent AND byte-identical"


def test_rows_appended_during_the_upload_stay_pending(spool, s3, status):
    """Only rows up to the max rowid actually read are marked."""
    seed_hour(spool, DAY, 16, samples=2)  # 4 rows, ids 1..4
    now = after(DAY, 16)

    real_write = s3io.write_table_atomic

    def write_then_poll(table, bucket, key, **kwargs):
        result = real_write(table, bucket, key, **kwargs)
        # The poll loop appends a row for the same local hour mid-upload.
        seed_hour(spool, DAY, 16, samples=1, offset=timedelta(minutes=59), base=42.0)
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(s3io, "write_table_atomic", write_then_poll)
        summary = run(spool, s3, now=now)

    assert summary.rows == 4, "the object holds what was read, not the late row"
    assert summary.marked == 4
    assert spool.pending_rows_for_hour(DAY, 16) == 2, "the late rows stay pending"

    # The next run heals it: the whole hour is rewritten with all 6 rows.
    summary = run(spool, s3, now=now)
    assert summary.rows == 6
    assert read_part(s3, DAY, 16).num_rows == 6


# ------------------------------------------------------------- idempotency


def test_rerun_is_a_no_op_and_force_rewrites_byte_identical_output(spool, s3, status):
    seed_hour(spool, DAY, 4, samples=5, channels=("breaker_p11", "ct_1_a", "ct_1_b"))
    now = after(DAY, 4)
    key = s3io.raw_30s_part_key(DAY, 4)

    first = run(spool, s3, now=now)
    original = object_bytes(s3, key)
    assert first.rows == 15

    # Nothing is pending, so a plain re-run does nothing at all.
    second = run(spool, s3, now=now)
    assert second.uploaded == []
    assert second.results == []
    assert object_bytes(s3, key) == original

    # An explicit forced rewrite reproduces the object byte for byte.
    third = run(spool, s3, start=DAY, end=DAY, now=now, force=True)
    assert [r.label for r in third.uploaded] == ["2026-08-16T04"]
    assert third.rows == 15
    assert third.marked == 0, "there was nothing left to mark"
    assert object_bytes(s3, key) == original

    # ...and no stray temp objects were left behind under energy/_tmp.
    assert listed_keys(s3, s3io.TMP_PREFIX) == []


def test_force_requires_an_explicit_range(spool, s3, status):
    with pytest.raises(ValueError, match="explicit --start/--end"):
        run(spool, s3, force=True)


def test_force_skips_hours_with_no_rows(spool, s3, status):
    """A gap stays a gap: an hour with no spool rows produces no object."""
    seed_hour(spool, DAY, 2, samples=1)
    summary = run(spool, s3, start=DAY, end=DAY, now=after(DAY, 23), force=True)

    assert [r.label for r in summary.uploaded] == ["2026-08-16T02"]
    assert len(summary.skipped) == 23
    assert listed_keys(s3, s3io.raw_30s_day_prefix(DAY)) == [
        s3io.raw_30s_part_key(DAY, 2)
    ]


# ---------------------------------------------------------------------- DST


def test_fall_back_hour_01_is_one_part_holding_two_physical_hours(spool, s3, status):
    """2026-11-01: wall-clock 01:00 happens twice; both belong in ...T01.parquet."""
    edt = datetime(2026, 11, 1, 5, 30, tzinfo=timeutil.UTC)  # 01:30 EDT
    est = datetime(2026, 11, 1, 6, 30, tzinfo=timeutil.UTC)  # 01:30 EST
    spool.append(
        [
            model.make_observation(
                ts_utc=ts,
                source=model.SOURCE_LEVITON,
                device_id="4C45565275C6",
                channel_id="breaker_p11",
                metric="watts",
                value=value,
            )
            for ts, value in ((edt, 111.0), (est, 222.0))
        ]
    )
    # Both instants really are the same wall clock.
    assert timeutil.to_local_naive(edt) == timeutil.to_local_naive(est)

    # Still open one minute before the SECOND occurrence ends.
    summary = run(spool, s3, now=datetime(2026, 11, 1, 6, 59, tzinfo=timeutil.UTC))
    assert summary.results == []

    summary = run(spool, s3, now=after(FALL_BACK, 1))
    assert [r.label for r in summary.uploaded] == ["2026-11-01T01"]

    keys = listed_keys(s3, s3io.raw_30s_day_prefix(FALL_BACK))
    assert keys == ["energy/raw_30s/year=2026/month=11/day=01/part-20261101T01.parquet"]

    table = read_part(s3, FALL_BACK, 1)
    assert table.num_rows == 2
    assert table.column("value").to_pylist() == [111.0, 222.0]
    # ts_local is ambiguous by design; ts_utc keeps the rows distinct.
    assert table.column("ts_local").to_pylist() == [
        datetime(2026, 11, 1, 1, 30),
        datetime(2026, 11, 1, 1, 30),
    ]
    assert len(set(table.column("ts_utc").to_pylist())) == 2
    assert spool.stats().pending_rows == 0


def test_spring_forward_day_has_no_hour_02_part(spool, s3, status):
    """2026-03-08: wall-clock 02 does not exist, so no part is ever named T02."""
    assert 2 not in timeutil.local_wall_hours_of_day(SPRING_FORWARD)
    seed_hour(spool, SPRING_FORWARD, 1, samples=2)
    seed_hour(spool, SPRING_FORWARD, 3, samples=2)

    summary = run(
        spool,
        s3,
        start=SPRING_FORWARD,
        end=SPRING_FORWARD,
        now=after(SPRING_FORWARD, 23),
        force=True,
    )

    assert [r.label for r in summary.uploaded] == ["2026-03-08T01", "2026-03-08T03"]
    keys = listed_keys(s3, s3io.raw_30s_day_prefix(SPRING_FORWARD))
    assert keys == [
        "energy/raw_30s/year=2026/month=03/day=08/part-20260308T01.parquet",
        "energy/raw_30s/year=2026/month=03/day=08/part-20260308T03.parquet",
    ]
    assert all("T02" not in key for key in keys)


# ---------------------------------------------------- rule 6 and empty hours


def test_day_grain_rows_are_refused_before_they_can_reach_raw_30s(spool, s3, status):
    """CLAUDE.md rule 6. The spool rejects them; the uploader asserts it anyway."""
    day_row = model.make_observation(
        ts_utc=timeutil.local_midnight_utc(DAY),
        source=model.SOURCE_BRYANT,
        device_id="TEST0000001",
        channel_id="hpheat",
        metric="kwh_day",
        value=12.5,
    )
    with pytest.raises(ValueError, match="day-grain"):
        spool.append([day_row])

    # Force one in behind the spool's back to prove the uploader's own gate.
    conn = spool.connect()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO observations (ts_utc, ts_local, source, device_id, channel_id,"
        " metric, value, unit, local_date, local_hour, uploaded_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        (
            timeutil.format_utc(day_row.ts_utc),
            day_row.ts_local.isoformat(sep="T", timespec="microseconds"),
            day_row.source,
            day_row.device_id,
            day_row.channel_id,
            day_row.metric,
            day_row.value,
            day_row.unit,
            DAY.isoformat(),
            0,
        ),
    )
    conn.commit()

    with pytest.raises(ValueError, match="day-grain metrics"):
        run(spool, s3, now=after(DAY, 0))
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 0), client=s3)


def test_an_hour_with_no_rows_writes_no_object(spool, s3, status):
    result = uploader.upload_hour(spool, DAY, 11, bucket=BUCKET, client=s3)
    assert result.skipped == "no_rows"
    assert result.rows == 0
    assert not result.uploaded
    assert listed_keys(s3) == []


def test_nothing_pending_is_a_clean_no_op(spool, s3, status):
    summary = run(spool, s3, now=after(DAY, 23))
    assert summary.ok
    assert summary.results == []
    assert summary.rows == 0
    assert summary.last_hour_uploaded is None
    assert listed_keys(s3) == []


# ------------------------------------------------------------- status.json


def test_status_json_carries_the_uploader_section(spool, s3, status):
    seed_hour(spool, DAY, 13, samples=3)
    run(spool, s3, now=after(DAY, 13))

    section = status.section("uploader")
    assert section["last_hour_uploaded"] == "2026-08-16T13"
    assert section["rows"] == 6
    assert section["last_success_utc"] is not None
    assert section["consecutive_failures"] == 0
    # The spool gauge is refreshed in the same pass (PLAN.md §11).
    assert status.section("spool")["pending_rows"] == 0
    assert status.path.exists()


def test_a_no_op_run_does_not_blank_the_last_uploaded_hour(spool, s3, status):
    seed_hour(spool, DAY, 13, samples=3)
    run(spool, s3, now=after(DAY, 13))
    first = status.section("uploader")["last_success_utc"]

    run(spool, s3, now=after(DAY, 14))
    section = status.section("uploader")

    assert section["last_hour_uploaded"] == "2026-08-16T13"
    assert section["rows"] == 6
    assert section["last_success_utc"] >= first


# ------------------------------------------------------------------ plumbing


def test_summary_is_a_mapping_of_loggable_fields(spool, s3, status):
    seed_hour(spool, DAY, 20, samples=2)
    summary = run(spool, s3, now=after(DAY, 20))

    assert dict(summary) == {
        "hours_uploaded": 1,
        "hours_failed": 0,
        "hours_skipped": 0,
        "rows": 4,
        "marked": 4,
        "last_hour_uploaded": "2026-08-16T20",
        "duration_s": summary["duration_s"],
    }
    assert summary.keys_written == (s3io.raw_30s_part_key(DAY, 20),)


def test_run_opens_and_closes_its_own_spool_when_none_is_given(spool_dir, s3, status):
    """The CLI calls run(start=..., end=...) with no spool of its own."""
    with open_spool(spool_dir / "spool.db") as db:
        seed_hour(db, DAY, 18, samples=2)

    summary = uploader.run(
        start=DAY, end=DAY, bucket=BUCKET, client=s3, now=after(DAY, 18)
    )
    assert summary.rows == 4

    with open_spool(spool_dir / "spool.db") as db:
        assert db.stats().pending_rows == 0


# --------------------------------------- spool failures stay inside their hour


def test_a_spool_mark_failure_fails_only_its_own_hour(spool, s3, status):
    """A disk-full spool must not strand the rest of a catch-up.

    ``mark_uploaded`` runs after the part is written and verified, and it used to
    sit outside the per-hour containment: an ``OperationalError`` there escaped
    ``run()`` as itself (not ``UploadFailed``), abandoned the remaining hours of
    a catch-up, and skipped the ``status.json`` update entirely — so the operator
    saw a stale ``last_success_utc`` instead of a failure. No rows were lost (the
    UPDATE is atomic), which is why this is a stranding bug rather than a
    corruption one.
    """
    for hour in (5, 6, 7):
        seed_hour(spool, DAY, hour, samples=2)

    real_mark = spool.mark_uploaded

    def flaky_mark(local_date, hour, max_rowid, **kwargs):
        if hour == 6:
            raise OSError("database or disk is full")
        return real_mark(local_date, hour, max_rowid, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(spool, "mark_uploaded", flaky_mark)
        with pytest.raises(uploader.UploadFailed) as excinfo:
            run(spool, s3, now=after(DAY, 7))

    summary = excinfo.value.summary
    # Every hour was attempted, and the failure is data rather than a crash.
    assert [r.label for r in summary.uploaded] == ["2026-08-16T05", "2026-08-16T07"]
    assert [r.label for r in summary.failed] == ["2026-08-16T06"]
    assert "mark_uploaded" in summary.failed[0].error

    # Hour 6's object landed and verified; only the bookkeeping failed, so its
    # rows stay pending and the next run rewrites a byte-identical part.
    assert s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 6), client=s3)
    assert spool.pending_rows_for_hour(DAY, 6) == 4
    assert spool.pending_rows_for_hour(DAY, 5) == 0

    # The operator can see it: the failure reached status.json.
    assert status.section("uploader")["consecutive_failures"] == 1

    summary = run(spool, s3, now=after(DAY, 7))
    assert [r.label for r in summary.uploaded] == ["2026-08-16T06"]
    assert spool.pending_rows_for_hour(DAY, 6) == 0


def test_a_spool_read_failure_fails_only_its_own_hour(spool, s3, status):
    """The other uncontained spool call: a read failure strands nothing either."""
    for hour in (5, 6, 7):
        seed_hour(spool, DAY, hour, samples=2)

    real_read = spool.read_local_hour

    def flaky_read(local_date, hour, **kwargs):
        if hour == 6:
            raise OSError("disk I/O error")
        return real_read(local_date, hour, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(spool, "read_local_hour", flaky_read)
        with pytest.raises(uploader.UploadFailed) as excinfo:
            run(spool, s3, now=after(DAY, 7))

    summary = excinfo.value.summary
    assert [r.label for r in summary.uploaded] == ["2026-08-16T05", "2026-08-16T07"]
    assert [r.label for r in summary.failed] == ["2026-08-16T06"]
    # Nothing was written for the unreadable hour — no half-part at its key.
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 6), client=s3)
    assert spool.pending_rows_for_hour(DAY, 6) == 4


def test_a_day_grain_row_in_the_spool_still_aborts_the_run(spool, s3, status):
    """The containment above must not soften the CLAUDE.md rule 6 guard.

    Day-grain rows in ``raw_30s`` would poison every hourly rollup, so that guard
    aborts the whole run rather than being recorded as one hour's error. The
    spool refuses to store such a row, so the only way in is a corrupted read —
    which is exactly the boundary the guard exists to defend.
    """
    from energy_capture.spool.sqlite import SpoolRow

    seed_hour(spool, DAY, 12, samples=2)
    start_utc, _ = timeutil.local_hour_bounds_utc(DAY, 12)
    poisoned = SpoolRow(
        rowid=999,
        observation=model.make_observation(
            ts_utc=start_utc,
            source=model.SOURCE_BRYANT,
            device_id="4022W200213",
            channel_id="hpheat",
            metric="kwh_day",
            value=12.5,
        ),
        local_date=DAY,
        local_hour=12,
        uploaded_at=None,
    )

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(spool, "read_local_hour", lambda *a, **k: [poisoned])
        with pytest.raises(ValueError, match="day-grain"):
            run(spool, s3, now=after(DAY, 12))

    # It aborted instead of writing a poisoned part.
    assert not s3io.key_exists(BUCKET, s3io.raw_30s_part_key(DAY, 12), client=s3)
