"""Tests for the SQLite spool (PLAN.md §15.6, §15.3, §10).

The contracts pinned here:

* append/read round-trips values and timestamps exactly (microsecond precision);
* local-date/hour bucketing is right across a DST fall-back hour — both 01:00
  hours land in the one bucket that becomes ``part-20261101T01.parquet``, and
  stay distinguishable by ``ts_utc``;
* ``purge`` requires **both** "uploaded" and "older than retention" — never one;
* ``pending_local_hours`` never returns the currently-open hour;
* rows committed before a hard process kill are still there after restart.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import energy_capture
from energy_capture.model import Observation, make_observation
from energy_capture.spool import SpoolDB, SpoolRow

from tests.conftest import LOCAL_TZ

UTC = timezone.utc
TZ = LOCAL_TZ

DEVICE = "4C45565275C6"

# ``tests/conftest.py`` pins TZ_LOCAL, SPOOL_DIR and SPOOL_RETENTION_DAYS=7 for
# every test and disables ``.env`` loading, so a developer's real configuration
# can never reach the DST assertions below.


@pytest.fixture
def spool(tmp_path: Path):
    db = SpoolDB(tmp_path / "spool.db")
    try:
        yield db
    finally:
        db.close()


def obs(
    ts: datetime,
    *,
    channel: str = "breaker_p11",
    metric: str = "watts",
    value: float = 1234.5,
    source: str = "leviton",
    device: str = DEVICE,
):
    return make_observation(
        ts_utc=ts,
        source=source,
        device_id=device,
        channel_id=channel,
        metric=metric,
        value=value,
    )


# ------------------------------------------------------------- round tripping


def test_append_read_round_trip_is_exact(spool: SpoolDB) -> None:
    ts = datetime(2026, 8, 16, 18, 0, 30, 123456, tzinfo=UTC)
    written = [
        obs(ts, channel="breaker_p11", value=1234.5678901234),
        obs(ts, channel="breaker_p11", metric="amps", value=-5.125),
        obs(ts, channel="ct_1_a", value=0.0),  # spurious fw-v2 zero, verbatim
    ]

    assert spool.append(written) == 3

    read = spool.rows_for_local_hour(date(2026, 8, 16), 14)
    assert len(read) == 3

    by_key = {(o.channel_id, o.metric): o for o in read}
    for original in written:
        got = by_key[(original.channel_id, original.metric)]
        assert got.ts_utc == original.ts_utc
        assert got.ts_utc.microsecond == 123456
        assert got.ts_local == original.ts_local
        assert got.ts_local.tzinfo is None
        assert got.value == original.value  # exact, not approx
        assert got.unit == original.unit
        assert got.source == original.source
        assert got.device_id == original.device_id
        assert got == original


def test_empty_append_is_a_noop(spool: SpoolDB) -> None:
    assert spool.append([]) == 0
    assert spool.stats().total_rows == 0


def test_duplicate_dedupe_key_is_ignored(spool: SpoolDB) -> None:
    """Re-running `poll --once` over the same instant must not double-insert."""
    ts = datetime(2026, 8, 16, 18, 0, 30, tzinfo=UTC)
    assert spool.append([obs(ts, value=100.0)]) == 1
    assert spool.append([obs(ts, value=999.0)]) == 0  # first occurrence wins

    rows = spool.rows_for_local_hour(date(2026, 8, 16), 14)
    assert [r.value for r in rows] == [100.0]


def test_rows_are_returned_in_sort_key_order(spool: SpoolDB) -> None:
    base = datetime(2026, 8, 16, 18, 0, 0, tzinfo=UTC)
    spool.append(
        [
            obs(base + timedelta(seconds=60), channel="ct_1_a"),
            obs(base, channel="breaker_p9"),
            obs(base, channel="breaker_p11"),
        ]
    )
    rows = spool.rows_for_local_hour(date(2026, 8, 16), 14)
    assert [(r.ts_utc, r.channel_id) for r in rows] == [
        (base, "breaker_p11"),
        (base, "breaker_p9"),
        (base + timedelta(seconds=60), "ct_1_a"),
    ]


def test_day_grain_and_meter_rows_are_rejected(spool: SpoolDB) -> None:
    """The spool feeds raw_30s; poisoning it is a programming error, not a warning."""
    midnight = datetime(2026, 8, 16, 4, 0, 0, tzinfo=UTC)
    day_grain = make_observation(
        ts_utc=midnight,
        source="bryant",
        device_id="4022W200213",
        channel_id="hpheat",
        metric="kwh_day",
        value=12.5,
    )
    with pytest.raises(ValueError, match="day-grain"):
        spool.append([day_grain])

    meter = make_observation(
        ts_utc=midnight,
        source="lge",
        device_id="meter1",
        channel_id="electric_main",
        metric="kwh_interval",
        value=0.5,
        interval_s=900,
    )
    with pytest.raises(ValueError, match="MeterObservation"):
        spool.append([meter])

    # ...and neither left a partial transaction behind.
    assert spool.stats().total_rows == 0


def test_ts_local_that_disagrees_with_ts_utc_is_rejected(spool: SpoolDB) -> None:
    """The partition column and the readability column must describe one instant."""
    ts = datetime(2026, 8, 16, 18, 0, tzinfo=UTC)  # local 14:00
    bad = Observation(
        ts_utc=ts,
        ts_local=datetime(2026, 8, 16, 18, 0),  # someone stored UTC in ts_local
        source="leviton",
        device_id=DEVICE,
        channel_id="breaker_p11",
        metric="watts",
        value=1.0,
        unit="W",
    )
    with pytest.raises(ValueError, match="local wall clock"):
        spool.append([bad])
    assert spool.stats().total_rows == 0

    aware = Observation(
        ts_utc=ts,
        ts_local=datetime(2026, 8, 16, 14, 0, tzinfo=UTC),
        source="leviton",
        device_id=DEVICE,
        channel_id="breaker_p11",
        metric="watts",
        value=1.0,
        unit="W",
    )
    with pytest.raises(ValueError, match="naive local wall clock"):
        spool.append([aware])


# ------------------------------------------------------- local-date bucketing


def test_local_date_partition_is_local_not_utc(spool: SpoolDB) -> None:
    """23:30 local on the 15th is 03:30Z on the 16th — it partitions to the 15th."""
    spool.append(
        [
            obs(datetime(2026, 8, 16, 3, 30, tzinfo=UTC), channel="breaker_p1"),
            obs(datetime(2026, 8, 16, 4, 30, tzinfo=UTC), channel="breaker_p1"),
        ]
    )
    late = spool.rows_for_local_hour(date(2026, 8, 15), 23)
    early = spool.rows_for_local_hour(date(2026, 8, 16), 0)
    assert len(late) == 1 and len(early) == 1
    assert late[0].ts_local == datetime(2026, 8, 15, 23, 30)
    assert early[0].ts_local == datetime(2026, 8, 16, 0, 30)


def test_dst_fall_back_both_01_hours_bucket_together_and_stay_distinct(
    spool: SpoolDB,
) -> None:
    """2026-11-01: local 01:00 happens twice (05:xxZ EDT, then 06:xxZ EST).

    Both belong in the single ``part-20261101T01.parquet``, so both must land in
    ``(local_date=2026-11-01, local_hour=1)`` — and ``ts_utc`` keeps them apart.
    """
    first_01 = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)  # 01:30 EDT
    second_01 = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)  # 01:30 EST
    hour_00 = datetime(2026, 11, 1, 4, 30, tzinfo=UTC)  # 00:30 EDT
    hour_02 = datetime(2026, 11, 1, 7, 30, tzinfo=UTC)  # 02:30 EST

    assert spool.append([obs(t) for t in (hour_00, first_01, second_01, hour_02)]) == 4

    bucket_01 = spool.rows_for_local_hour(date(2026, 11, 1), 1)
    assert len(bucket_01) == 2, "the repeated 01:00 hour must not lose a row"
    assert [r.ts_utc for r in bucket_01] == [first_01, second_01]
    # ts_local is deliberately ambiguous — identical for both (PLAN.md §2.4).
    assert {r.ts_local for r in bucket_01} == {datetime(2026, 11, 1, 1, 30)}

    assert len(spool.rows_for_local_hour(date(2026, 11, 1), 0)) == 1
    assert len(spool.rows_for_local_hour(date(2026, 11, 1), 2)) == 1

    # One pending hour entry for 01, not two.
    pending = spool.pending_local_hours(now=datetime(2026, 11, 1, 12, 0, tzinfo=UTC))
    assert pending == [
        (date(2026, 11, 1), 0),
        (date(2026, 11, 1), 1),
        (date(2026, 11, 1), 2),
    ]


def test_dst_spring_forward_skips_the_nonexistent_hour(spool: SpoolDB) -> None:
    """2026-03-08: local 02:xx does not exist; 06:59Z is 01:59 and 07:00Z is 03:00."""
    before = datetime(2026, 3, 8, 6, 59, tzinfo=UTC)
    after = datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
    spool.append([obs(before), obs(after)])

    assert [r.ts_local for r in spool.rows_for_local_hour(date(2026, 3, 8), 1)] == [
        datetime(2026, 3, 8, 1, 59)
    ]
    assert spool.rows_for_local_hour(date(2026, 3, 8), 2) == []
    assert [r.ts_local for r in spool.rows_for_local_hour(date(2026, 3, 8), 3)] == [
        datetime(2026, 3, 8, 3, 0)
    ]

    hours = [h.hour for h in spool.pending_local_hours(
        now=datetime(2026, 3, 9, 12, 0, tzinfo=UTC)
    )]
    assert hours == [1, 3]


# ------------------------------------------------------------- pending hours


def test_pending_local_hours_excludes_the_open_hour(spool: SpoolDB) -> None:
    closed = datetime(2026, 8, 16, 17, 30, tzinfo=UTC)  # local 13:30, hour 13
    open_hour = datetime(2026, 8, 16, 18, 30, tzinfo=UTC)  # local 14:30, hour 14
    spool.append([obs(closed), obs(open_hour)])

    now = datetime(2026, 8, 16, 18, 45, tzinfo=UTC)  # local 14:45 — hour 14 still open
    assert spool.pending_local_hours(now=now) == [(date(2026, 8, 16), 13)]

    # Exactly at the boundary the hour is closed (bounds are [start, end)).
    at_close = datetime(2026, 8, 16, 19, 0, tzinfo=UTC)
    assert spool.pending_local_hours(now=at_close) == [
        (date(2026, 8, 16), 13),
        (date(2026, 8, 16), 14),
    ]


def test_pending_fall_back_hour_closes_only_after_both_occurrences(
    spool: SpoolDB,
) -> None:
    spool.append([obs(datetime(2026, 11, 1, 5, 30, tzinfo=UTC))])  # first 01:30

    # 06:30Z is the *second* 01:xx local hour — the 01 bucket is still open.
    mid = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    assert spool.pending_local_hours(now=mid) == []

    done = datetime(2026, 11, 1, 7, 0, tzinfo=UTC)
    assert spool.pending_local_hours(now=done) == [(date(2026, 11, 1), 1)]


def test_pending_local_hours_ignores_uploaded_hours(spool: SpoolDB) -> None:
    ts = datetime(2026, 8, 16, 17, 30, tzinfo=UTC)
    spool.append([obs(ts)])
    now = datetime(2026, 8, 16, 20, 0, tzinfo=UTC)
    assert spool.pending_local_hours(now=now) == [(date(2026, 8, 16), 13)]

    spool.mark_uploaded(date(2026, 8, 16), 13)
    assert spool.pending_local_hours(now=now) == []


# ----------------------------------------------------------- marking uploaded


def test_mark_uploaded_variants(spool: SpoolDB) -> None:
    base = datetime(2026, 8, 16, 17, 0, tzinfo=UTC)
    spool.append([obs(base + timedelta(seconds=30 * i), channel=f"breaker_p{i}")
                  for i in range(4)])

    rows: list[SpoolRow] = spool.read_local_hour(date(2026, 8, 16), 13)
    assert [r.uploaded for r in rows] == [False] * 4
    ids = sorted(r.rowid for r in rows)

    # Explicit id list.
    assert spool.mark_uploaded(date(2026, 8, 16), 13, [ids[0]]) == 1
    # Everything up to and including a captured max rowid.
    assert spool.mark_uploaded(date(2026, 8, 16), 13, ids[2]) == 2
    assert spool.pending_rows_for_hour(date(2026, 8, 16), 13) == 1
    # The catch-all form.
    assert spool.mark_uploaded(date(2026, 8, 16), 13) == 1
    assert spool.pending_rows_for_hour(date(2026, 8, 16), 13) == 0

    # Idempotent: re-marking changes nothing.
    assert spool.mark_uploaded(date(2026, 8, 16), 13) == 0


def test_mark_uploaded_preserves_the_original_stamp(spool: SpoolDB) -> None:
    ts = datetime(2026, 8, 16, 17, 30, tzinfo=UTC)
    spool.append([obs(ts)])
    first = datetime(2026, 8, 16, 19, 5, tzinfo=UTC)
    spool.mark_uploaded(date(2026, 8, 16), 13, uploaded_at=first)
    spool.mark_uploaded(
        date(2026, 8, 16), 13, uploaded_at=datetime(2026, 8, 16, 20, 5, tzinfo=UTC)
    )
    assert spool.read_local_hour(date(2026, 8, 16), 13)[0].uploaded_at == first


def test_uploaded_rows_are_still_returned_for_the_hour(spool: SpoolDB) -> None:
    """A re-upload overwrites the part file, so it must see every row for the hour."""
    base = datetime(2026, 8, 16, 17, 0, tzinfo=UTC)
    spool.append([obs(base, channel="breaker_p1")])
    spool.mark_uploaded(date(2026, 8, 16), 13)
    spool.append([obs(base + timedelta(seconds=30), channel="breaker_p1")])

    assert len(spool.rows_for_local_hour(date(2026, 8, 16), 13)) == 2
    assert len(spool.rows_for_local_hour(date(2026, 8, 16), 13, pending_only=True)) == 1


def test_max_rowid_for_local_hour(spool: SpoolDB) -> None:
    base = datetime(2026, 8, 16, 17, 0, tzinfo=UTC)
    assert spool.max_rowid_for_local_hour(date(2026, 8, 16), 13) is None
    spool.append([obs(base, channel="breaker_p1"), obs(base, channel="breaker_p2")])
    highest = spool.max_rowid_for_local_hour(date(2026, 8, 16), 13)
    assert highest == max(r.rowid for r in spool.read_local_hour(date(2026, 8, 16), 13))


# -------------------------------------------------------------------- purge


def test_purge_requires_both_uploaded_and_aged(spool: SpoolDB) -> None:
    now = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    old = now - timedelta(days=10)
    recent = now - timedelta(days=1)

    spool.append(
        [
            obs(old, channel="old_uploaded"),
            obs(old, channel="old_pending"),
            obs(recent, channel="recent_uploaded"),
            obs(recent, channel="recent_pending"),
        ]
    )

    def mark(channel: str) -> None:
        rows = [r for r in _all_rows(spool) if r.observation.channel_id == channel]
        assert len(rows) == 1
        spool.mark_uploaded(rows[0].local_date, rows[0].local_hour, [rows[0].rowid])

    mark("old_uploaded")
    mark("recent_uploaded")

    deleted = spool.purge(7, now=now)
    assert deleted == 1

    remaining = {
        r.observation.channel_id
        for r in _all_rows(spool)
    }
    assert remaining == {"old_pending", "recent_uploaded", "recent_pending"}, (
        "purge must delete only rows that are BOTH uploaded AND past retention"
    )


def test_purge_never_deletes_unuploaded_rows_however_old(spool: SpoolDB) -> None:
    now = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    spool.append([obs(now - timedelta(days=365), channel="ancient")])
    assert spool.purge(1, now=now) == 0
    assert spool.stats().pending_rows == 1


def test_purge_rejects_a_zero_retention_floor(spool: SpoolDB) -> None:
    with pytest.raises(ValueError, match="retention_days"):
        spool.purge(0)


def test_purge_defaults_to_the_configured_retention(spool: SpoolDB) -> None:
    """SPOOL_RETENTION_DAYS=7 from the fixture; a 3-day-old uploaded row survives."""
    now = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    ts = now - timedelta(days=3)
    spool.append([obs(ts)])
    rows = _all_rows(spool)
    spool.mark_uploaded(rows[0].local_date, rows[0].local_hour)
    assert spool.purge(now=now) == 0
    assert spool.stats().total_rows == 1


# -------------------------------------------------------------------- stats


def test_stats_reports_pending_rows_and_oldest_pending(spool: SpoolDB) -> None:
    assert spool.stats() == (0, None, 0, 0)

    oldest = datetime(2026, 8, 16, 16, 0, 0, 500000, tzinfo=UTC)
    newer = datetime(2026, 8, 16, 17, 0, tzinfo=UTC)
    spool.append([obs(oldest, channel="a"), obs(newer, channel="b")])

    stats = spool.stats()
    assert stats.pending_rows == 2
    assert stats.oldest_pending_utc == oldest
    assert stats.total_rows == 2
    assert stats.uploaded_rows == 0
    assert stats.to_status_dict() == {
        "pending_rows": 2,
        "oldest_pending_utc": "2026-08-16T16:00:00.500000Z",
    }

    spool.mark_uploaded(date(2026, 8, 16), 12)  # the `oldest` row's local hour
    stats = spool.stats()
    assert stats.pending_rows == 1
    assert stats.uploaded_rows == 1
    assert stats.oldest_pending_utc == newer


# --------------------------------------------------------------- durability


def test_rows_survive_a_hard_process_kill(tmp_path: Path) -> None:
    """PLAN.md §15.6: a committed poll cycle must be there after a crash+restart.

    The child appends and then ``os._exit``s — no close, no flush, no atexit —
    which is as close to `docker kill` as a test can get.
    """
    db_path = tmp_path / "spool.db"
    src_dir = str(Path(energy_capture.__file__).resolve().parent.parent)
    script = tmp_path / "crash.py"
    script.write_text(
        textwrap.dedent(
            f"""
            import os, sys
            sys.path.insert(0, {src_dir!r})
            from datetime import datetime, timezone
            from energy_capture.model import make_observation
            from energy_capture.spool import SpoolDB

            ts = datetime(2026, 8, 16, 18, 0, 30, 123456, tzinfo=timezone.utc)
            rows = [
                make_observation(
                    ts_utc=ts, source="leviton", device_id={DEVICE!r},
                    channel_id=f"breaker_p{{i}}", metric="watts", value=100.0 + i,
                )
                for i in range(5)
            ]
            spool = SpoolDB({str(db_path)!r})
            spool.append(rows)
            os._exit(1)          # hard kill: no close(), no cleanup
            """
        )
    )

    env = dict(os.environ, TZ_LOCAL=TZ, SPOOL_DIR=str(tmp_path))
    env.pop("PYTEST_CURRENT_TEST", None)
    result = subprocess.run(
        [sys.executable, str(script)], cwd=tmp_path, env=env, capture_output=True
    )
    assert result.returncode == 1, result.stderr.decode()

    survivor = SpoolDB(db_path)
    try:
        rows = survivor.rows_for_local_hour(date(2026, 8, 16), 14)
        assert len(rows) == 5
        assert sorted(r.value for r in rows) == [100.0, 101.0, 102.0, 103.0, 104.0]
        assert survivor.stats().pending_rows == 5, "crash-survivors must still upload"
    finally:
        survivor.close()


def test_reopening_sees_previously_committed_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "spool.db"
    first = SpoolDB(db_path)
    first.append([obs(datetime(2026, 8, 16, 17, 30, tzinfo=UTC))])
    first.close()

    second = SpoolDB(db_path)
    try:
        assert second.stats().pending_rows == 1
        assert second.pending_local_hours(
            now=datetime(2026, 8, 16, 20, 0, tzinfo=UTC)
        ) == [(date(2026, 8, 16), 13)]
    finally:
        second.close()


def test_concurrent_writer_and_reader_threads(tmp_path: Path) -> None:
    """The poller writes while the uploader reads — neither may fail."""
    import threading

    db_path = tmp_path / "spool.db"
    spool = SpoolDB(db_path, busy_timeout_s=10.0)
    base = datetime(2026, 8, 16, 17, 0, tzinfo=UTC)
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            for i in range(50):
                spool.append([obs(base + timedelta(seconds=i), channel="breaker_p1")])
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(50):
                spool.stats()
                spool.rows_for_local_hour(date(2026, 8, 16), 13)
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    spool.close()

    assert not errors, errors
    reopened = SpoolDB(db_path)
    try:
        assert reopened.stats().total_rows == 50
    finally:
        reopened.close()


# ------------------------------------------------------------------ helpers


def _all_rows(spool: SpoolDB) -> list[SpoolRow]:
    conn = spool.connect()
    buckets = conn.execute(
        "SELECT DISTINCT local_date, local_hour FROM observations "
        "ORDER BY local_date, local_hour"
    ).fetchall()
    out: list[SpoolRow] = []
    for row in buckets:
        out.extend(
            spool.read_local_hour(date.fromisoformat(row["local_date"]), int(row["local_hour"]))
        )
    return out
