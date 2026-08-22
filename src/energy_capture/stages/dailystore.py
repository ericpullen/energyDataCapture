"""Where day-grain Bryant energy lands: a local Parquet month, and S3 when there is one.

Why this module exists
----------------------
``fetch-daily`` and ``backfill`` write the *same* monthly objects — that is
deliberate (see either module's docstring), and it is what lets a backfill and a
nightly fetch converge on one file instead of fighting over it. But both were
written S3-only, against ``energy/daily/year=YYYY/bryant-YYYYMM.parquet``, and
**no bucket is configured in this deployment**. The consequence was not a
degraded feature, it was no feature at all: ``bryant_daily_energy`` failed every
night with ``S3_BUCKET is not configured``, and not one Bryant energy row exists
anywhere.

So this module owns the destination, once, for both stages.

Local is the default, S3 is a mirror
------------------------------------
``{SPOOL_DIR}/daily/bryant-YYYYMM.parquet``, always. When a bucket *is*
configured the same table is also written to the S3 key PLAN.md §4 specifies, so
turning S3 on later changes nothing about how the stage behaves — it only adds a
second destination.

This follows the Green Button precedent exactly (``stages/greenbutton.py``
defaults to ``{SPOOL_DIR}/meter`` and treats S3 as opt-in) with one deliberate
difference: for a *scheduled* stage the mirror is automatic when a bucket exists,
rather than requiring ``--bucket`` every night. An import of a file a human just
downloaded is a manual act and should not fan out by surprise; a nightly job
landing in the archive it was designed for is not a surprise.

**This is not the spool.** Day-grain rows still never enter ``raw_30s`` — that is
cardinal rule 6, and it is why they need a destination of their own at all.
``{SPOOL_DIR}`` is just the writable volume both destinations share; the
``daily`` subdirectory is a separate dataset, the way ``meter`` is.

Merge order is the precedence policy
------------------------------------
``fetched`` first, then whatever the local month already held, then whatever S3
held. The dedupe inside :func:`~energy_capture.model.observations_to_table`
keeps the first occurrence of each :data:`~energy_capture.model.DEDUPE_KEY`, so:

* a ``day2`` revision replaces the ``day1`` value written for that date;
* a backfilled history is carried through untouched by a nightly fetch;
* and an S3 object written before the local file existed is absorbed rather than
  shadowed, so switching destinations loses nothing.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import BaseClient

from energy_capture import model
from energy_capture.aws import s3io
from energy_capture.config import get_settings
from energy_capture.logging import get_logger

__all__ = [
    "LOCAL_SUBDIR",
    "MonthDestination",
    "default_out_dir",
    "existing_rows",
    "local_month_path",
    "month_start_of",
    "write_month_table",
    "write_table_atomic",
]

log = get_logger("dailystore")

#: Subdirectory of ``SPOOL_DIR`` that holds the day-grain dataset. Sibling of
#: ``meter/`` and deliberately NOT inside the spool database.
LOCAL_SUBDIR: Final[str] = "daily"


def month_start_of(local_day: date) -> date:
    """First LOCAL day of the month — what the file and the S3 key are keyed on."""
    return local_day.replace(day=1)


def default_out_dir() -> Path:
    """``{SPOOL_DIR}/daily``."""
    return Path(get_settings().spool_dir) / LOCAL_SUBDIR


def local_month_path(
    month_start: date,
    *,
    out_dir: str | Path | None = None,
    source: str = model.SOURCE_BRYANT,
) -> Path:
    """``{out_dir}/bryant-YYYYMM.parquet`` for the month ``month_start`` opens.

    The basename matches the S3 object's (PLAN.md §4's ``bryant-{YYYYMM}``), so
    the two destinations are obviously the same dataset and a file copied from
    one to the other needs no renaming.
    """
    directory = Path(out_dir) if out_dir is not None else default_out_dir()
    return directory / f"{source}-{month_start:%Y%m}.parquet"


class MonthDestination:
    """The destinations one month's rows are written to, and what they held.

    Constructed per month by both stages so that "where does this go" is answered
    in one place rather than twice.
    """

    def __init__(
        self,
        month_start: date,
        *,
        out_dir: str | Path | None = None,
        bucket: str | None = None,
        client: BaseClient | None = None,
        source: str = model.SOURCE_BRYANT,
    ) -> None:
        self.month_start = month_start
        self.source = source
        self.path = local_month_path(month_start, out_dir=out_dir, source=source)
        self.bucket = bucket or None
        self.client = client
        self.key = s3io.daily_key(month_start, source=source)

    @property
    def mirrors_to_s3(self) -> bool:
        return self.bucket is not None

    def describe(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "bucket": self.bucket,
            "key": self.key if self.mirrors_to_s3 else None,
        }


def existing_rows(destination: MonthDestination) -> list[model.Observation]:
    """Rows both destinations already hold, local first (see the merge order).

    A read failure on either side is fatal on purpose: silently treating an
    unreadable month as empty would rewrite it from this run's rows alone and
    quietly delete history.
    """
    rows: list[model.Observation] = []
    if destination.path.exists():
        table = pq.read_table(destination.path)
        rows.extend(model.table_to_observations(table, dataset=model.Dataset.DAILY))
    if destination.mirrors_to_s3 and s3io.key_exists(
        destination.bucket, destination.key, client=destination.client
    ):
        table = s3io.read_table(
            destination.bucket, destination.key, client=destination.client
        )
        rows.extend(model.table_to_observations(table, dataset=model.Dataset.DAILY))
    return rows


def build_month_table(
    fetched: Sequence[model.Observation],
    existing: Sequence[model.Observation] = (),
) -> pa.Table:
    """Merge freshly-fetched rows over what the month already held.

    Concatenation order **is** the precedence policy — see the module docstring.
    The sort that follows the dedupe is deterministic, so a re-run writes
    byte-identical bytes and both stages are idempotent over a date range.
    """
    return model.observations_to_table(
        list(fetched) + list(existing), dataset=model.Dataset.DAILY
    )


def write_table_atomic(table: pa.Table, path: Path) -> None:
    """Write ``table`` to ``path`` so that a failure cannot destroy what is there.

    **This is not a nicety.** ``pyarrow.parquet.write_table`` straight to the
    destination removes the existing file before it opens the new one, so a write
    that then fails — a permission error, a full disk — leaves **no file at all**.
    Observed on 2026-08-22: a month whose 336 rows had just been read successfully
    was deleted by the failed write, and the next run merged over nothing and
    wrote 28 rows in its place. Silent history loss, from a stage whose entire
    contract is "merge over what the month already held".

    So: a temp file in the same directory (same filesystem, so the rename is
    atomic), fsync, then :func:`os.replace`. Either the old month survives intact
    or the new one replaces it whole — never neither. This is the same guarantee
    ``aws/s3io.write_table_atomic`` gives the mirror, which is where the idea
    came from.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.stem}-", suffix=".parquet.tmp"
    )
    os.close(handle)
    temp_path = Path(temp_name)
    try:
        pq.write_table(table, temp_path)
        with open(temp_path, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(temp_path, path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise


def write_month_table(
    table: pa.Table, destination: MonthDestination, *, dry_run: bool = False
) -> dict[str, Any]:
    """Write one month to every configured destination. Raises on any failure.

    Local first: it is the destination that always exists, so if the S3 mirror
    fails the data is already durable and the next run merges over it rather than
    starting from nothing.
    """
    written: dict[str, Any] = {"path": str(destination.path), "written": False}
    if dry_run:
        log.info(
            "dailystore_would_write",
            rows=table.num_rows,
            **destination.describe(),
        )
        written["s3"] = "dry run"
        return written

    write_table_atomic(table, destination.path)
    written["written"] = True
    log.info("dailystore_wrote", rows=table.num_rows, path=str(destination.path))

    if destination.mirrors_to_s3:
        s3io.write_table_atomic(
            table, destination.bucket, destination.key, client=destination.client
        )
        written["s3"] = f"s3://{destination.bucket}/{destination.key}"
        log.info(
            "dailystore_mirrored",
            rows=table.num_rows,
            bucket=destination.bucket,
            key=destination.key,
        )
    else:
        written["s3"] = "no bucket configured (local only)"
    return written
