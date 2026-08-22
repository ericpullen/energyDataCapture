"""``stages/dailystore`` — where day-grain Bryant energy lands.

The reason this module exists is a deployment fact: ``fetch-daily`` and
``backfill`` were S3-only, no bucket is configured, so Bryant's energy was never
recorded anywhere at all. The behaviour worth pinning is therefore not the
Parquet plumbing but the *destination policy*:

* local always, so the stage works with no AWS at all;
* S3 as a mirror when a bucket exists, at the key PLAN.md §4 specifies, so
  turning S3 on later adds a destination rather than changing a behaviour;
* an S3 object written before the local file existed is absorbed into the merge,
  not shadowed by it;
* and none of it goes anywhere near the spool (rule 6).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.stages import dailystore
from tests.conftest import BUCKET

MONTH = date(2026, 8, 1)


def observation(local_day: date, *, channel_id: str, value: float) -> model.Observation:
    """One day-grain row, stamped at LOCAL midnight (PLAN.md §7.2)."""
    ts = timeutil.local_midnight_utc(local_day)
    return model.Observation(
        ts_utc=ts,
        ts_local=timeutil.to_local_naive(ts),
        source=model.SOURCE_BRYANT,
        device_id="4022W200213",
        channel_id=channel_id,
        metric="kwh_day",
        value=value,
        unit="kWh",
    )


def rows_of(path: Path) -> dict[tuple[str, str], float]:
    table = pq.read_table(path)
    out: dict[tuple[str, str], float] = {}
    for row in table.to_pylist():
        # ts_local is a naive timestamp column, not text.
        out[(row["channel_id"], row["ts_local"].date().isoformat())] = row["value"]
    return out


# ------------------------------------------------------------------ local only


def test_a_month_is_written_locally_with_no_bucket_at_all(tmp_path: Path) -> None:
    """The whole point: no AWS, and the data still lands."""
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    assert destination.mirrors_to_s3 is False
    assert destination.path == tmp_path / "bryant-202608.parquet"

    table = dailystore.build_month_table([observation(date(2026, 8, 21), channel_id="cooling", value=12.0)])
    outcome = dailystore.write_month_table(table, destination)

    assert outcome["written"] is True
    assert "no bucket" in outcome["s3"]
    assert rows_of(destination.path) == {("cooling", "2026-08-21"): 12.0}


def test_the_local_basename_matches_the_s3_object(tmp_path: Path) -> None:
    """Same dataset, two destinations: a file copied between them needs no rename."""
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket="b")
    assert destination.path.name == destination.key.rsplit("/", 1)[-1]
    assert destination.key.startswith("energy/daily/")


def test_the_dataset_lives_beside_the_spool_and_not_inside_it(tmp_path: Path) -> None:
    """Rule 6: day-grain rows never enter the spool. They get their own dataset."""
    assert dailystore.LOCAL_SUBDIR == "daily"
    path = dailystore.local_month_path(MONTH, out_dir=tmp_path / "data" / "daily")
    assert path.suffix == ".parquet"
    assert "spool.db" not in str(path)


def test_a_dry_run_writes_nothing(tmp_path: Path) -> None:
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    table = dailystore.build_month_table([observation(date(2026, 8, 21), channel_id="fan", value=3.0)])
    outcome = dailystore.write_month_table(table, destination, dry_run=True)
    assert outcome["written"] is False
    assert not destination.path.exists()


# --------------------------------------------------------------- the merge


def test_a_rerun_merges_over_what_the_month_already_held(tmp_path: Path) -> None:
    """Fetched rows win; everything else is carried through untouched."""
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    first = [
        observation(date(2026, 8, 20), channel_id="cooling", value=10.0),
        observation(date(2026, 8, 21), channel_id="cooling", value=11.0),
    ]
    dailystore.write_month_table(dailystore.build_month_table(first), destination)

    # A later run re-reports the 21st (a day2 revision) and adds the 22nd.
    revision = [
        observation(date(2026, 8, 21), channel_id="cooling", value=99.0),
        observation(date(2026, 8, 22), channel_id="cooling", value=12.0),
    ]
    existing = dailystore.existing_rows(destination)
    dailystore.write_month_table(
        dailystore.build_month_table(revision, existing), destination
    )

    assert rows_of(destination.path) == {
        ("cooling", "2026-08-20"): 10.0,  # untouched
        ("cooling", "2026-08-21"): 99.0,  # revised
        ("cooling", "2026-08-22"): 12.0,  # new
    }


def test_writing_the_same_month_twice_is_byte_identical(tmp_path: Path) -> None:
    """Idempotent over a date range, which is what makes a re-run safe."""
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    rows = [observation(date(2026, 8, 21), channel_id="hpheat", value=21.0)]
    dailystore.write_month_table(dailystore.build_month_table(rows), destination)
    first = destination.path.read_bytes()
    dailystore.write_month_table(
        dailystore.build_month_table(rows, dailystore.existing_rows(destination)),
        destination,
    )
    assert destination.path.read_bytes() == first


# ------------------------------------------------------------- the S3 mirror


def test_a_bucket_gets_a_mirror_at_the_planned_key(tmp_path: Path, s3: Any) -> None:
    bucket = BUCKET
    destination = dailystore.MonthDestination(
        MONTH, out_dir=tmp_path, bucket=bucket, client=s3
    )
    table = dailystore.build_month_table(
        [observation(date(2026, 8, 21), channel_id="eheat", value=4.0)]
    )
    outcome = dailystore.write_month_table(table, destination)

    assert outcome["s3"] == f"s3://{bucket}/{destination.key}"
    assert destination.path.exists(), "local is written even when a mirror exists"
    mirrored = s3io.read_table(bucket, destination.key, client=s3)
    assert mirrored.num_rows == table.num_rows


def test_history_already_in_s3_is_absorbed_not_shadowed(tmp_path: Path, s3: Any) -> None:
    """Switching to local-first must not lose what a bucket already held.

    An earlier S3-only run is part of the month's history, so it belongs in the
    merge. Otherwise the first local write would rewrite the month from this
    run's rows alone and silently drop the rest.
    """
    bucket = BUCKET
    destination = dailystore.MonthDestination(
        MONTH, out_dir=tmp_path, bucket=bucket, client=s3
    )
    old = dailystore.build_month_table(
        [observation(date(2026, 8, 1), channel_id="cooling", value=7.0)]
    )
    s3io.write_table_atomic(old, bucket, destination.key, client=s3)
    assert not destination.path.exists()

    fresh = [observation(date(2026, 8, 21), channel_id="cooling", value=12.0)]
    existing = dailystore.existing_rows(destination)
    dailystore.write_month_table(
        dailystore.build_month_table(fresh, existing), destination
    )

    assert rows_of(destination.path) == {
        ("cooling", "2026-08-01"): 7.0,
        ("cooling", "2026-08-21"): 12.0,
    }


def test_an_unreadable_month_raises_instead_of_silently_rewriting_it(
    tmp_path: Path,
) -> None:
    """Treating a corrupt month as empty would delete history on the next write."""
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    destination.path.parent.mkdir(parents=True, exist_ok=True)
    destination.path.write_bytes(b"this is not parquet")
    with pytest.raises(Exception):
        dailystore.existing_rows(destination)


def test_a_failed_write_leaves_the_previous_month_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bug that ate a month, pinned.

    Writing Parquet straight to the destination removes the old file before
    opening the new one, so a failed write leaves NOTHING — and the next run
    then merges over an empty month and writes a fraction of the history in its
    place. Observed for real on 2026-08-22. The write must be atomic: either the
    old month survives or the new one replaces it whole.
    """
    destination = dailystore.MonthDestination(MONTH, out_dir=tmp_path, bucket=None)
    good = [observation(date(2026, 8, d), channel_id="cooling", value=float(d)) for d in range(1, 22)]
    dailystore.write_month_table(dailystore.build_month_table(good), destination)
    before = destination.path.read_bytes()
    assert len(rows_of(destination.path)) == 21

    def explode(*args: Any, **kwargs: Any) -> None:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(dailystore.pq, "write_table", explode)
    with pytest.raises(PermissionError):
        dailystore.write_month_table(
            dailystore.build_month_table(
                [observation(date(2026, 8, 22), channel_id="cooling", value=99.0)]
            ),
            destination,
        )

    # The month is still there, unchanged, and readable.
    assert destination.path.exists(), "a failed write deleted the month"
    assert destination.path.read_bytes() == before
    assert len(rows_of(destination.path)) == 21
    # ...and no half-written temp file was left behind.
    assert [p.name for p in destination.path.parent.glob("*.tmp")] == []


# ------------------------------------------------------- the bucket accessor


def test_an_unset_bucket_is_a_state_not_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """``configured_bucket()`` is the whole reason the stage can run without AWS."""
    from energy_capture import config

    for value in ("", "   ", None):
        monkeypatch.setattr(
            s3io, "get_settings", lambda value=value: type("S", (), {"s3_bucket": value})()
        )
        assert s3io.configured_bucket() is None

    monkeypatch.setattr(
        s3io, "get_settings", lambda: type("S", (), {"s3_bucket": " my-bucket "})()
    )
    assert s3io.configured_bucket() == "my-bucket"
    assert config is not None  # the import is the point: settings still resolve
