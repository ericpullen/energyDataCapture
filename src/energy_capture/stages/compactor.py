"""Daily compactor: hourly parts -> ``day-{YYYYMMDD}.parquet`` (PLAN.md §10).

Runs at ~01:30 local for D-1, and is also ``energycap compact-daily --start/--end``
over any range of LOCAL dates.

The double-count trap
---------------------

PLAN.md §10 works through this and lands on a layout resolution; it is the whole
reason this module exists in the shape it does. For one local day D::

    energy/raw_30s/year=/month=/day=/part-{YYYYMMDD}T{HH}.parquet   (uploader)
    energy/raw_30s/year=/month=/day=/day-{YYYYMMDD}.parquet         (this stage)

Both live under the prefix the ``energy_raw_30s`` Glue table points at. The day
file is built *from* the parts, so if the two ever coexist there, **every row of
that day is counted twice** by Athena, DuckDB and the rollup alike. Query-time
dedupe would be the alternative, and it is worse: it makes every query the user
(or the LLM) writes silently wrong unless they remember it.

So the compactor keeps exactly one authoritative set per day under ``raw_30s``
— hourly parts for recent days, the day file for compacted days — by moving the
parts to the **sibling, non-tabled** prefix::

    energy/raw_30s_parts_archive/year=/month=/day=/part-…parquet

where they live out a 7-day safety window before being deleted. The archive is
outside every Glue table location and every partition-projection template, so an
archived part is invisible to queries but still recoverable by hand.

Order of operations, per day
----------------------------

1. Read **all** ``part-*.parquet`` for D **and** any existing ``day-{D}.parquet``.
   Re-reading the day file is what makes a re-run after late data converge
   instead of dropping the rows already compacted.
2. Dedupe on :data:`energy_capture.model.DEDUPE_KEY`, then sort on
   :data:`~energy_capture.model.SORT_KEY`. Parts are placed **before** the
   existing day file so that a re-uploaded (corrected) part wins the tie — the
   "latest write wins at the file level" rule of §10. Rows are identical in the
   normal case, so this only matters after a fix-and-re-upload.
3. Write ``day-{D}.parquet`` (atomically: temp key -> verify -> copy -> delete),
   then **verify the S3 footer row count equals the deduped count**.
4. **Only if that verify passes**, move the parts to the archive prefix
   (copy-then-delete, via :func:`energy_capture.aws.s3io.move_keys`).

If anything in 1–3 fails, nothing is archived and nothing is deleted: the parts
stay exactly where the queries can still see them, and the next run tries again
(PLAN.md §15.7). Failure of one day never stops the other days in the range;
the run reports the failure at the end so the exit code is non-zero.

Archive retention
-----------------

Archived parts are deleted only when **all three** hold (§10):

* day D is at least :data:`ARCHIVE_RETENTION_DAYS` days old,
* ``day-{D}.parquet`` exists, and
* it passes the count check *again* — here the strong form: every row in the
  archived parts is still present in the day file, and the day file's Parquet
  footer agrees with what was read back.

Because the scheduled run only ever asks for D-1 — which is never 7 days old —
the deletion pass is a **sweep** over whatever the archive prefix actually holds,
not something driven by ``--start/--end``. That is what makes the nightly job
self-maintaining. Days whose parts were archived *during this same run* are
skipped by the sweep so that a late first compaction still gets its full soak.

Idempotency and convergence
---------------------------

* Deterministic output name, deterministic row order -> a re-run overwrites with
  byte-identical content instead of duplicating.
* Already compacted (no parts left, day file present): the day file is read,
  checked and **not rewritten** — a no-op that still verifies.
* Crash mid-move: :func:`~energy_capture.aws.s3io.move_keys` classifies each key
  before touching it, so a part that was copied but not deleted is simply
  finished off. A crash can leave a duplicate, never a lost object.
* A part that reappears under ``raw_30s`` after the day file was written (an
  uploader run racing the compactor) is caught by re-listing after the move and
  triggering another pass — the invariant "no parts beside a day file" is
  checked, not assumed.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pyarrow as pa
from botocore.client import BaseClient

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore, get_status_store
from energy_capture.logging import get_logger

__all__ = [
    "ARCHIVE_RETENTION_DAYS",
    "MAX_PASSES",
    "STATUS_SECTION",
    "CompactionError",
    "DayResult",
    "compact_day",
    "run",
    "sweep_archive",
]

log = get_logger("compactor")

#: ``status.json`` section this stage owns (PLAN.md §11).
STATUS_SECTION = "compactor"

#: How old a local day must be before its archived parts may be deleted
#: (PLAN.md §10: "once D is ≥7 days old"). Deliberately **not** wired to
#: ``SPOOL_RETENTION_DAYS`` — that knob governs the SQLite spool, and coupling
#: two unrelated safety windows to one env var would make either one surprising
#: to change.
ARCHIVE_RETENTION_DAYS = 7

#: How many times a single day is compacted before giving up. A second pass only
#: happens when parts reappear under ``raw_30s`` after the day file was written
#: (a concurrent uploader); a third means something is writing parts faster than
#: we can compact them, which is a bug worth failing loudly on.
MAX_PASSES = 3

#: ``year=YYYY/month=MM/day=DD/`` inside an archive key. Parsing only — every key
#: is *formatted* by :mod:`energy_capture.aws.s3io`, and each parse below is
#: validated by regenerating the prefix with the s3io builder.
_ARCHIVE_DAY_RE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")

_ONE_DAY = timedelta(days=1)


class CompactionError(RuntimeError):
    """A day could not be compacted, or the run left parts beside a day file."""


@dataclass(frozen=True, slots=True)
class DayResult:
    """What happened to one local day."""

    local_day: date
    #: Rows in the authoritative ``day-{D}.parquet`` after this run.
    rows: int
    #: Part files read (and therefore archived) this run.
    parts: int
    #: Parts now living under the archive prefix because of this run.
    parts_archived: int
    #: False when the existing day file already matched the computed table.
    wrote_day_file: bool
    #: Nothing to compact: neither parts nor a day file exist for this date.
    nothing_to_do: bool = False
    #: Compaction passes used (>1 means parts reappeared mid-run).
    passes: int = 1


# --------------------------------------------------------------- table helpers


def _normalised(table: pa.Table, key: str) -> pa.Table:
    """Coerce a table read back from S3 to the canonical ``raw_30s`` schema.

    Guards against a foreign object landing in the partition: a file without the
    canonical columns is a hard error, never something to be silently patched up
    or dropped.
    """
    missing = [name for name in model.CANONICAL_COLUMNS if name not in table.column_names]
    if missing:
        raise CompactionError(
            f"{key} is not a raw_30s file: missing columns {missing}. "
            "Refusing to compact it (nothing is archived or deleted)."
        )
    return table.select(list(model.CANONICAL_COLUMNS)).cast(model.RAW_30S_SCHEMA)


def _read_normalised(
    bucket: str, key: str, *, client: BaseClient | None = None
) -> pa.Table:
    return _normalised(s3io.read_table(bucket, key, client=client), key)


def _combine(tables: Sequence[pa.Table]) -> pa.Table:
    """Concat -> dedupe -> sort, in that order.

    Dedupe **before** the sort so precedence follows input order (parts before
    the old day file), and sort afterwards so the output is deterministic — the
    same input always produces a byte-identical object (PLAN.md §10).
    """
    if not tables:
        return model.empty_table(model.Dataset.RAW_30S)
    combined = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    return model.sort_table(model.dedupe_table(combined))


# ------------------------------------------------------------------ one day


def compact_day(
    local_day: date,
    *,
    bucket: str | None = None,
    client: BaseClient | None = None,
    max_passes: int = MAX_PASSES,
) -> DayResult:
    """Compact one local day and archive its parts. Raises on any failure.

    Nothing is archived unless the day file's S3 row count matches the deduped
    row count; a raise therefore always leaves the parts queryable where they are
    (PLAN.md §15.7).
    """
    bucket = bucket or s3io.default_bucket()
    day_key = s3io.raw_30s_day_key(local_day)
    archive_prefix = s3io.raw_30s_archive_day_prefix(local_day)
    archived_total = 0

    for attempt in range(1, max_passes + 1):
        part_keys = s3io.list_raw_30s_parts(bucket, local_day, client=client)
        existing: pa.Table | None = None
        if s3io.key_exists(bucket, day_key, client=client):
            existing = _read_normalised(bucket, day_key, client=client)

        if not part_keys and existing is None:
            log.info(
                "compact_nothing_to_do",
                local_day=local_day.isoformat(),
                prefix=s3io.raw_30s_day_prefix(local_day),
            )
            return DayResult(
                local_day=local_day,
                rows=0,
                parts=0,
                parts_archived=archived_total,
                wrote_day_file=False,
                nothing_to_do=True,
                passes=attempt,
            )

        # Parts first: a re-uploaded part beats the day file built from the old
        # one ("latest write wins at the file level", PLAN.md §10). In the normal
        # case the rows are identical and the order is irrelevant.
        tables = [_read_normalised(bucket, key, client=client) for key in part_keys]
        input_rows = sum(table.num_rows for table in tables)
        if existing is not None:
            tables.append(existing)
            input_rows += existing.num_rows

        final = _combine(tables)
        rows = final.num_rows

        unchanged = existing is not None and final.equals(existing)
        if unchanged:
            # Already-compacted day (or a re-run that adds nothing): do not
            # rewrite, but still put the day file through the verify gate below.
            log.info(
                "compact_day_file_unchanged",
                local_day=local_day.isoformat(),
                key=day_key,
                rows=rows,
                parts=len(part_keys),
            )
        else:
            s3io.write_table_atomic(final, bucket, day_key, client=client)

        # The gate. Re-read the footer of the object that is actually in S3 —
        # even when we skipped the write — and only then touch the parts.
        if not s3io.verify_row_count(bucket, day_key, rows, client=client):
            raise s3io.RowCountMismatch(
                f"{s3io.s3_uri(bucket, day_key)} does not hold the expected {rows} "
                f"deduped rows for {local_day.isoformat()}; parts left in place"
            )

        log.info(
            "compact_day_verified",
            local_day=local_day.isoformat(),
            key=day_key,
            rows=rows,
            input_rows=input_rows,
            duplicates_dropped=input_rows - rows,
            parts=len(part_keys),
            rewrote=not unchanged,
            compaction_pass=attempt,
        )

        if part_keys:
            move = s3io.move_keys(bucket, part_keys, archive_prefix, client=client)
            archived_total += len(move.moved) + len(move.already_moved)
            if move.missing:
                # move_keys already logged each one; a part that vanished from
                # both ends is odd but not a reason to fail — its rows are in the
                # day file, which is the thing queries read.
                log.warning(
                    "compact_parts_missing_during_archive",
                    local_day=local_day.isoformat(),
                    keys=move.missing,
                )

        remaining = s3io.list_raw_30s_parts(bucket, local_day, client=client)
        if not remaining:
            return DayResult(
                local_day=local_day,
                rows=rows,
                parts=len(part_keys),
                parts_archived=archived_total,
                wrote_day_file=not unchanged,
                passes=attempt,
            )

        # A part appeared beside the day file after we listed. Left alone it
        # would double-count every row it holds, so fold it in and go again.
        log.warning(
            "compact_parts_reappeared",
            local_day=local_day.isoformat(),
            keys=remaining,
            compaction_pass=attempt,
        )

    raise CompactionError(
        f"parts are still present beside {day_key} after {max_passes} compaction "
        f"passes for {local_day.isoformat()}; refusing to leave raw_30s in a "
        "double-counting state without saying so"
    )


# ------------------------------------------------------------ archive sweep


def _archived_days(bucket: str, *, client: BaseClient | None = None) -> list[date]:
    """Local dates that currently have objects under the archive prefix."""
    keys = s3io.list_keys(
        bucket, f"{s3io.ARCHIVE_PREFIX}/", suffix=".parquet", client=client
    )
    days: set[date] = set()
    for key in keys:
        match = _ARCHIVE_DAY_RE.search(key)
        parsed: date | None = None
        if match:
            try:
                parsed = date(int(match[1]), int(match[2]), int(match[3]))
            except ValueError:
                parsed = None
        # Round-trip through the s3io builder so the layout stays owned by one
        # module: if the key does not sit exactly where s3io would put it, we do
        # not understand it and we certainly do not delete it.
        if parsed is None or not key.startswith(s3io.raw_30s_archive_day_prefix(parsed)):
            log.warning("archive_key_unrecognised", key=key)
            continue
        days.add(parsed)
    return sorted(days)


def _sweep_one_day(
    bucket: str,
    local_day: date,
    *,
    client: BaseClient | None = None,
) -> int:
    """Delete one day's archived parts if the day file provably supersedes them.

    Returns the number of objects deleted (0 when the checks say "keep").
    """
    prefix = s3io.raw_30s_archive_day_prefix(local_day)
    archived = s3io.list_keys(bucket, prefix, suffix=".parquet", client=client)
    if not archived:
        return 0

    day_key = s3io.raw_30s_day_key(local_day)
    if not s3io.key_exists(bucket, day_key, client=client):
        log.error(
            "archive_kept_no_day_file",
            local_day=local_day.isoformat(),
            day_key=day_key,
            archived=len(archived),
        )
        return 0

    stray = s3io.list_raw_30s_parts(bucket, local_day, client=client)
    if stray:
        # Not fatal for the sweep (the day file still covers the archived rows),
        # but it means this day is not in its final state.
        log.warning(
            "archive_sweep_parts_present",
            local_day=local_day.isoformat(),
            keys=stray,
        )

    day_table = _read_normalised(bucket, day_key, client=client)
    day_rows = day_table.num_rows

    # The count check, again, in its strong form: fold the archived parts back in
    # and require that they add nothing. If they do add something, the day file
    # is not a superset and these parts are the only copy of those rows.
    merged = _combine(
        [day_table] + [_read_normalised(bucket, key, client=client) for key in archived]
    )
    if merged.num_rows != day_rows:
        log.error(
            "archive_kept_rows_missing_from_day_file",
            local_day=local_day.isoformat(),
            day_key=day_key,
            day_rows=day_rows,
            merged_rows=merged.num_rows,
            missing=merged.num_rows - day_rows,
            archived=len(archived),
        )
        return 0

    if not s3io.verify_row_count(bucket, day_key, day_rows, client=client):
        log.error(
            "archive_kept_day_file_unverified",
            local_day=local_day.isoformat(),
            day_key=day_key,
            expected=day_rows,
        )
        return 0

    for key in archived:
        s3io.delete_key(bucket, key, client=client)
    log.info(
        "archive_parts_deleted",
        local_day=local_day.isoformat(),
        deleted=len(archived),
        day_key=day_key,
        day_rows=day_rows,
    )
    return len(archived)


def sweep_archive(
    *,
    bucket: str | None = None,
    client: BaseClient | None = None,
    today: date | None = None,
    retention_days: int = ARCHIVE_RETENTION_DAYS,
    skip: frozenset[date] | set[date] | None = None,
) -> int:
    """Delete archived parts for every day that is old enough and superseded.

    ``today`` is the local date the age is measured against (defaults to now).
    ``skip`` holds days that must not be swept yet — the run passes the days
    whose parts it archived moments ago, so a late first compaction still gets
    its full safety window.
    """
    bucket = bucket or s3io.default_bucket()
    today = today if today is not None else timeutil.local_date_of(timeutil.now_utc())
    skip = set(skip or ())
    deleted = 0
    considered = 0

    for local_day in _archived_days(bucket, client=client):
        age_days = (today - local_day).days
        if age_days < retention_days:
            log.debug(
                "archive_within_window",
                local_day=local_day.isoformat(),
                age_days=age_days,
                retention_days=retention_days,
            )
            continue
        if local_day in skip:
            log.info(
                "archive_soak_started_this_run",
                local_day=local_day.isoformat(),
                age_days=age_days,
            )
            continue
        considered += 1
        deleted += _sweep_one_day(bucket, local_day, client=client)

    log.info(
        "archive_sweep_done",
        today=today.isoformat(),
        retention_days=retention_days,
        days_considered=considered,
        objects_deleted=deleted,
    )
    return deleted


# ----------------------------------------------------------------- the stage


def _resolve_range(
    start: date | None, end: date | None, *, today: date
) -> tuple[date, date]:
    """Default window: D-1, the ~01:30 local schedule of PLAN.md §5."""
    if start is None and end is None:
        yesterday = today - _ONE_DAY
        return yesterday, yesterday
    if start is None:
        assert end is not None
        return end, end
    if end is None:
        return start, start
    return start, end


def run(
    *,
    start: date | None = None,
    end: date | None = None,
    bucket: str | None = None,
    client: BaseClient | None = None,
    store: StatusStore | None = None,
    now: datetime | None = None,
    retention_days: int = ARCHIVE_RETENTION_DAYS,
    sweep: bool = True,
) -> dict[str, Any]:
    """Compact each local day in ``[start, end]``, then sweep the archive.

    This is the ``compact-daily`` entry point (``cli.STAGE_ENTRYPOINTS``). With
    no range it does yesterday, matching the scheduled ~01:30 run for D-1.

    Every day is attempted even if an earlier one failed — a single bad day must
    not strand a week of parts — but the run raises :class:`CompactionError` at
    the end if any day failed, so the CLI exits non-zero and ``status.json``
    carries the failure.
    """
    bucket = bucket or s3io.default_bucket()
    now_utc = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
    today = timeutil.local_date_of(now_utc)
    start, end = _resolve_range(start, end, today=today)
    days = list(timeutil.iter_local_dates(start, end))
    store = store if store is not None else get_status_store()

    log.info(
        "compact_start",
        start=start.isoformat(),
        end=end.isoformat(),
        days=len(days),
        bucket=bucket,
    )

    results: list[DayResult] = []
    failures: list[tuple[date, BaseException]] = []

    for local_day in days:
        try:
            result = compact_day(local_day, bucket=bucket, client=client)
        except Exception as exc:
            log.error(
                "compact_day_failed",
                local_day=local_day.isoformat(),
                error=f"{type(exc).__name__}: {exc}",
            )
            store.record_failure(STATUS_SECTION, exc, last_day_attempted=local_day)
            failures.append((local_day, exc))
            continue

        results.append(result)
        if result.nothing_to_do:
            continue
        log.info(
            "compact_day_ok",
            local_day=local_day.isoformat(),
            rows=result.rows,
            parts=result.parts,
            parts_archived=result.parts_archived,
            rewrote=result.wrote_day_file,
            passes=result.passes,
        )
        store.record_success(
            STATUS_SECTION,
            last_day_compacted=local_day,
            rows=result.rows,
            parts_archived=result.parts_archived,
        )

    deleted = 0
    if sweep:
        # Days whose parts were archived seconds ago have not had a safety
        # window yet; give them one even if the date itself is already old.
        just_archived = {r.local_day for r in results if r.parts_archived > 0}
        deleted = sweep_archive(
            bucket=bucket,
            client=client,
            today=today,
            retention_days=retention_days,
            skip=just_archived,
        )
        store.set(STATUS_SECTION, archived_parts_deleted=deleted)

    compacted = [r for r in results if not r.nothing_to_do]
    summary: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": len(days),
        "compacted": len(compacted),
        "nothing_to_do": len(results) - len(compacted),
        "rows": sum(r.rows for r in compacted),
        "parts_archived": sum(r.parts_archived for r in results),
        "archived_parts_deleted": deleted,
        "failed": len(failures),
    }
    log.info("compact_done", **summary)

    if failures:
        listed = ", ".join(f"{day.isoformat()} ({type(exc).__name__})" for day, exc in failures)
        raise CompactionError(
            f"{len(failures)} of {len(days)} day(s) failed to compact: {listed}. "
            "Their parts were left in place under raw_30s and nothing was deleted."
        ) from failures[0][1]

    return summary
