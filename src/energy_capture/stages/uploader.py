"""The hourly uploader: SQLite spool -> ``part-{YYYYMMDD}T{HH}.parquet`` in S3.

PLAN.md §10 ("Uploader"), §4 (naming), §15.6 (spool durability). One closed local
hour becomes exactly one Parquet object in the **LOCAL-date** partition::

    energy/raw_30s/year=YYYY/month=MM/day=DD/part-{YYYYMMDD}T{HH}.parquet

The order of operations is the whole point of this module, and it is not
negotiable (PLAN.md §15.6 — "uploaded-marking only after verify")::

    read EVERY spool row for the hour  ->  dedupe + sort (model helpers)
      ->  write the part atomically (staged under energy/_tmp, then copied)
      ->  read the FINAL object's Parquet footer back from S3 and check num_rows
      ->  only then mark the spool rows uploaded

Traps this module exists to avoid
---------------------------------

**Read all rows for the hour, not just the pending ones.** The filename is
deterministic, so writing is an *overwrite*. After a partial failure (part
written, marking crashed) some rows for the hour are already flagged uploaded; an
uploader that read only the pending ones would replace a complete part with an
incomplete one. :meth:`SpoolDB.read_local_hour` defaults to ``pending_only=False``
for exactly this reason and we keep that default.

**Mark only up to the max rowid actually read.** The poller may append rows for
the same local hour while the upload is in flight (a late Leviton response, or a
catch-up run overlapping the live hour). Those rows are not in the object we just
wrote, so they must stay pending; :meth:`SpoolDB.mark_uploaded` takes the max
rowid we read and marks nothing newer. The next run re-writes the hour with them
included.

**DST.** Neither of the two DST cases is re-derived here — :mod:`timeutil` and the
spool already model them. On the fall-back day the wall-clock hour ``01`` happens
twice and both physical hours carry ``local_hour=1``, so they land in the single
``part-YYYYMMDDT01.parquet`` (two hours of rows, kept distinct by ``ts_utc``, the
canonical key). On the spring-forward day the wall-clock hour ``02`` does not
exist and :func:`timeutil.local_wall_hours_of_day` simply never offers it.

**Already-compacted days.** Only hours with *un-uploaded* rows are written by
default. That is what keeps the uploader from re-creating a part next to a
``day-*.parquet`` the compactor has already published (which would double-count
in ``energy_raw_30s``). Pass ``force=True`` to deliberately rewrite an
already-uploaded hour — sound while the day is still uncompacted, and the reason
it is not the default.

Idempotency
-----------

:func:`model.observations_to_table` dedupes on
``(ts_utc, source, device_id, channel_id, metric)`` and then sorts
deterministically, and the Parquet writer options in :mod:`energy_capture.aws.s3io`
are fixed, so the same spool rows always produce a byte-identical object at the
same key. A re-run over the same range is therefore a no-op (nothing pending) or
an identical rewrite (``force=True``) — never a duplicate.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from botocore.client import BaseClient

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore, get_status_store
from energy_capture.logging import get_logger
from energy_capture.spool.sqlite import SpoolDB, SpoolRow, open_spool

__all__ = [
    "STAGE",
    "STATUS_SECTION",
    "HourResult",
    "UploadFailed",
    "UploadSummary",
    "hour_key",
    "run",
    "upload_hour",
]

#: Log ``stage`` field and ``status.json`` section (PLAN.md §11).
STAGE = "uploader"
STATUS_SECTION = "uploader"

log = get_logger(STAGE)


class UploadFailed(RuntimeError):
    """At least one hour in the run did not verify.

    Carries the :class:`UploadSummary` of the whole run on :attr:`summary`, so a
    caller can see which hours *did* land. Raised only after every hour has been
    attempted — one bad hour must not strand a multi-hour catch-up.
    """

    def __init__(self, message: str, summary: UploadSummary) -> None:
        super().__init__(message)
        self.summary = summary


def hour_key(local_date: date, hour: int) -> str:
    """``"2026-08-16T14"`` — the ``last_hour_uploaded`` form of PLAN.md §11.

    Thin alias for :func:`timeutil.local_hour_label`, which owns the format so
    the same label cannot drift between here and ``status.json``'s other writer.
    """
    return timeutil.local_hour_label(local_date, hour)


# ------------------------------------------------------------------- results


@dataclass(frozen=True, slots=True)
class HourResult:
    """What happened to one local hour."""

    local_date: date
    hour: int
    key: str
    rows: int = 0
    bytes_written: int = 0
    marked: int = 0
    verified: bool = False
    #: Set when the hour was deliberately not written (``"no_rows"``).
    skipped: str | None = None
    #: Set when the hour was attempted and failed (verify mismatch, S3 error).
    error: str | None = None

    @property
    def uploaded(self) -> bool:
        return self.verified and self.skipped is None and self.error is None

    @property
    def label(self) -> str:
        return hour_key(self.local_date, self.hour)


class UploadSummary(Mapping):
    """Run summary. Also a :class:`~collections.abc.Mapping`, so the CLI folds its
    fields straight into the ``stage_ok`` log line."""

    __slots__ = ("results", "duration_s")

    def __init__(self, results: list[HourResult], duration_s: float = 0.0) -> None:
        self.results = results
        self.duration_s = duration_s

    # -- derived counters ---------------------------------------------------
    @property
    def uploaded(self) -> list[HourResult]:
        return [r for r in self.results if r.uploaded]

    @property
    def failed(self) -> list[HourResult]:
        return [r for r in self.results if r.error is not None]

    @property
    def skipped(self) -> list[HourResult]:
        return [r for r in self.results if r.skipped is not None]

    @property
    def rows(self) -> int:
        """Rows written across every verified part in this run."""
        return sum(r.rows for r in self.uploaded)

    @property
    def marked(self) -> int:
        return sum(r.marked for r in self.uploaded)

    @property
    def hours(self) -> tuple[str, ...]:
        return tuple(r.label for r in self.uploaded)

    @property
    def keys_written(self) -> tuple[str, ...]:
        return tuple(r.key for r in self.uploaded)

    @property
    def last_hour_uploaded(self) -> str | None:
        hours = self.hours
        return hours[-1] if hours else None

    @property
    def ok(self) -> bool:
        return not self.failed

    # -- Mapping ------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "hours_uploaded": len(self.uploaded),
            "hours_failed": len(self.failed),
            "hours_skipped": len(self.skipped),
            "rows": self.rows,
            "marked": self.marked,
            "last_hour_uploaded": self.last_hour_uploaded,
            "duration_s": round(self.duration_s, 3),
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"UploadSummary({self.to_dict()!r})"


# --------------------------------------------------------------- hour picking


def _closed(local_date: date, hour: int, reference: datetime) -> bool:
    """Has wall-clock ``hour`` on ``local_date`` finished at ``reference``?

    The bound comes from :func:`timeutil.local_hour_bounds_utc`, so on the
    fall-back day hour ``01`` is closed only once *both* of its occurrences are
    over — the part file holds both.
    """
    try:
        _, end_utc = timeutil.local_hour_bounds_utc(local_date, hour)
    except ValueError:
        # Wall-clock hour that DST says never happened (spring forward).
        return False
    return end_utc <= reference


def _candidate_hours(
    spool: SpoolDB,
    start: date | None,
    end: date | None,
    *,
    reference: datetime,
    force: bool,
) -> list[tuple[date, int]]:
    """The closed local hours this run will (re)write, chronologically.

    Default: every closed local hour that still holds un-uploaded spool rows —
    which is what makes a single invocation catch up on an arbitrary number of
    hours after downtime. ``--start/--end`` narrow that set; they do not widen it,
    because rewriting an hour whose rows are already uploaded could re-create a
    part beside a ``day-*.parquet`` the compactor already published. ``force``
    opts into that rewrite explicitly.
    """
    if force:
        if start is None or end is None:
            raise ValueError("force=True requires an explicit --start/--end range")
        out: list[tuple[date, int]] = []
        for day in timeutil.iter_local_dates(start, end):
            for hour in timeutil.local_wall_hours_of_day(day):
                if _closed(day, hour, reference):
                    out.append((day, hour))
        return out

    pending = spool.pending_local_hours(now=reference)
    return [
        (p.local_date, p.hour)
        for p in pending
        if (start is None or p.local_date >= start) and (end is None or p.local_date <= end)
    ]


# ---------------------------------------------------------------- one hour


def _assert_not_day_grain(rows: list[SpoolRow], local_date: date, hour: int) -> None:
    """CLAUDE.md rule 6: day-grain rows may never reach ``raw_30s``.

    :meth:`SpoolDB.append` already rejects them and
    :func:`model.observations_to_table` would too, but this is the boundary where
    they would actually poison an hourly rollup, so it is checked here as well
    and names the offending rows.
    """
    offenders = sorted({r.observation.metric for r in rows if model.is_day_grain(r.observation.metric)})
    if offenders:
        raise ValueError(
            f"day-grain metrics {offenders} found in spool hour "
            f"{hour_key(local_date, hour)}: they belong in energy/daily and would "
            "poison the hourly rollup if written to raw_30s (CLAUDE.md rule 6)"
        )


def upload_hour(
    spool: SpoolDB,
    local_date: date,
    hour: int,
    *,
    bucket: str,
    client: BaseClient | None = None,
    mark: bool = True,
) -> HourResult:
    """Write one local hour's part file, verify it, then mark the spool rows.

    Reads **every** row for the hour (uploaded ones included) because the write
    overwrites the deterministic key; marks only rows up to the max rowid it
    actually read, so anything the poller appends mid-upload stays pending.

    Returns a :class:`HourResult`. Never raises for an upload failure — the error
    is recorded on the result and the spool rows are left pending so the next run
    retries them. Programming errors (a day-grain row in the spool, an unsorted
    table) still raise.
    """
    key = s3io.raw_30s_part_key(local_date, hour)
    label = hour_key(local_date, hour)
    started = time.monotonic()

    try:
        rows = spool.read_local_hour(local_date, hour)
    except Exception as exc:
        # A spool read failure strands the rest of a catch-up if it escapes.
        # Nothing has been written yet, so reporting it as this hour's error is
        # safe and keeps the range stage's attempt-every-unit contract.
        log.error(
            "upload_hour_read_failed",
            hour=label,
            key=key,
            error=f"{type(exc).__name__}: {exc}",
        )
        return HourResult(
            local_date, hour, key, error=f"spool read failed: {type(exc).__name__}: {exc}"
        )
    if not rows:
        # A gap is a real answer: write no object rather than an empty one, which
        # would overwrite a good part with nothing (CLAUDE.md rule 1).
        log.info("upload_hour_empty", hour=label, key=key, rows=0)
        return HourResult(local_date, hour, key, skipped="no_rows")

    _assert_not_day_grain(rows, local_date, hour)

    max_rowid = max(r.rowid for r in rows)
    pending_before = sum(1 for r in rows if not r.uploaded)

    # dedupe (first occurrence wins) then deterministic sort -> byte-identical
    # output for identical input.
    table = model.observations_to_table(
        (r.observation for r in rows), dataset=model.Dataset.RAW_30S
    )
    expected = table.num_rows

    try:
        # verify=False: s3io still verifies the *staged* object before publishing
        # it, and the final-object footer check is done explicitly below so that
        # the "verify, then and only then mark" gate lives in this module.
        written = s3io.write_table_atomic(
            table, bucket, key, client=client, verify=False
        )
        verified = s3io.verify_row_count(bucket, key, expected, client=client)
    except Exception as exc:
        log.error(
            "upload_hour_failed",
            hour=label,
            key=key,
            rows=expected,
            error=f"{type(exc).__name__}: {exc}",
        )
        return HourResult(
            local_date,
            hour,
            key,
            rows=expected,
            error=f"{type(exc).__name__}: {exc}",
        )

    if not verified:
        # Nothing is marked. The rows stay pending and the next run retries the
        # whole hour (PLAN.md §15.6).
        log.error("upload_hour_unverified", hour=label, key=key, rows=expected)
        return HourResult(
            local_date,
            hour,
            key,
            rows=expected,
            bytes_written=written.bytes_written,
            error=f"row count verification failed for {s3io.s3_uri(bucket, key)}",
        )

    try:
        marked = spool.mark_uploaded(local_date, hour, max_rowid) if mark else 0
    except Exception as exc:
        # The object landed and verified; only the spool bookkeeping failed. The
        # UPDATE is atomic, so the rows stay pending and the next run rewrites a
        # byte-identical part and re-marks. Report it as this hour's failure so
        # the rest of a catch-up still runs and status.json sees it.
        log.error(
            "upload_hour_mark_failed",
            hour=label,
            key=key,
            rows=expected,
            max_rowid=max_rowid,
            error=f"{type(exc).__name__}: {exc}",
        )
        return HourResult(
            local_date,
            hour,
            key,
            rows=expected,
            bytes_written=written.bytes_written,
            error=f"spool mark_uploaded failed: {type(exc).__name__}: {exc}",
        )

    log.info(
        "upload_hour_ok",
        hour=label,
        key=key,
        rows=expected,
        spool_rows=len(rows),
        pending_before=pending_before,
        marked=marked,
        max_rowid=max_rowid,
        bytes=written.bytes_written,
        duration_s=round(time.monotonic() - started, 3),
    )
    return HourResult(
        local_date,
        hour,
        key,
        rows=expected,
        bytes_written=written.bytes_written,
        marked=marked,
        verified=True,
    )


# -------------------------------------------------------------------- stage


def run(
    start: date | None = None,
    end: date | None = None,
    *,
    spool: SpoolDB | None = None,
    bucket: str | None = None,
    client: BaseClient | None = None,
    now: datetime | None = None,
    status: StatusStore | None = None,
    force: bool = False,
) -> UploadSummary:
    """Upload every closed local hour that still has un-uploaded spool rows.

    Args:
        start: first LOCAL date to consider, inclusive. ``None`` (with ``end``
            ``None``) means "every closed hour with pending rows", which is how a
            multi-hour catch-up after downtime happens in one invocation.
        end: last LOCAL date, inclusive. Defaults to ``start`` when only ``start``
            is given, and vice versa.
        spool: an open spool; one is opened (and closed) here when omitted.
        bucket: destination bucket; defaults to ``S3_BUCKET``.
        client: boto3 S3 client; defaults to the module-level cached one.
        now: reference instant for "is this hour closed?" (tests).
        status: ``status.json`` writer; defaults to the process-wide store.
        force: also rewrite hours whose rows are already marked uploaded.
            Requires an explicit range. Use it to repair a part on a day that has
            not been compacted yet — see the module docstring.

    Returns:
        An :class:`UploadSummary` (also a mapping of loggable fields).

    Raises:
        UploadFailed: if any hour failed to verify. Every hour is attempted
            first, so the healthy hours in a catch-up still land.
    """
    started = time.monotonic()
    reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()

    if start is not None and end is None:
        end = start
    elif end is not None and start is None:
        start = end
    if start is not None and end is not None and end < start:
        raise ValueError(
            f"end {end.isoformat()} is before start {start.isoformat()}"
        )

    owns_spool = spool is None
    spool = spool if spool is not None else open_spool()
    store = status if status is not None else get_status_store()
    target_bucket = bucket if bucket is not None else s3io.default_bucket()

    try:
        candidates = _candidate_hours(
            spool, start, end, reference=reference, force=force
        )
        log.info(
            "upload_start",
            start=start.isoformat() if start else None,
            end=end.isoformat() if end else None,
            bucket=target_bucket,
            hours=len(candidates),
            force=force,
        )

        results = [
            upload_hour(spool, day, hour, bucket=target_bucket, client=client)
            for day, hour in candidates
        ]
        summary = UploadSummary(results, duration_s=time.monotonic() - started)
        _record_status(store, spool, summary)

        log.info("upload_done", bucket=target_bucket, **summary.to_dict())
        if summary.failed:
            raise UploadFailed(
                f"{len(summary.failed)} of {len(results)} hour(s) failed to upload: "
                + ", ".join(f"{r.label} ({r.error})" for r in summary.failed),
                summary,
            )
        return summary
    finally:
        if owns_spool:
            spool.close()


def _record_status(store: StatusStore, spool: SpoolDB, summary: UploadSummary) -> None:
    """Reflect the run in ``status.json`` (PLAN.md §11).

    ``last_hour_uploaded``/``rows`` are only rewritten when something was actually
    uploaded — a run with nothing to do must not blank the last real numbers with
    zeros, because "no pending hours" and "uploaded an empty hour" are different
    facts.
    """
    try:
        if summary.failed:
            partial: dict[str, Any] = {}
            if summary.uploaded:
                # A catch-up can land some hours and fail others; record what did
                # land so the failure does not hide it.
                partial = {
                    "last_hour_uploaded": summary.last_hour_uploaded,
                    "rows": summary.rows,
                }
            store.record_failure(
                STATUS_SECTION,
                summary.failed[0].error,
                hours_failed=len(summary.failed),
                hours_uploaded=len(summary.uploaded),
                **partial,
            )
        elif summary.uploaded:
            store.record_success(
                STATUS_SECTION,
                last_hour_uploaded=summary.last_hour_uploaded,
                rows=summary.rows,
                hours=len(summary.uploaded),
            )
        else:
            store.record_success(STATUS_SECTION)
        store.set("spool", **spool.stats().to_status_dict())
    except Exception as exc:  # pragma: no cover - telemetry must not break the stage
        log.warning("status_update_failed", error=f"{type(exc).__name__}: {exc}")
