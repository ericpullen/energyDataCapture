"""The SQLite spool — the crash-durable buffer between the poller and S3.

PLAN.md §5 and §10 (Poller / Uploader bullets). The poll loop appends
:class:`~energy_capture.model.Observation` rows here every 30s; the hourly
uploader reads a closed local hour back out, writes
``part-{YYYYMMDD}T{HH}.parquet``, **verifies** the object in S3, and only then
calls :meth:`SpoolDB.mark_uploaded`. Rows are deleted by :meth:`SpoolDB.purge`
only when they are *both* marked uploaded *and* older than
``SPOOL_RETENTION_DAYS`` — never one condition alone.

Design notes worth knowing before you touch this file
-----------------------------------------------------

**Local date/hour are computed once, at insert, via :mod:`energy_capture.timeutil`.**
They are stored as indexed columns (``local_date``, ``local_hour``) rather than
derived at query time from the stored timestamp text. Deriving them in SQL would
mean re-implementing DST in SQLite's ``strftime`` — the exact bug this project
cannot afford. On the fall-back day both 01:00 hours land in
``local_date='2026-11-01', local_hour=1``, which is precisely the set of rows
that belongs in the single ``part-20261101T01.parquet``; they stay distinguishable
by ``ts_utc``, the canonical key (CLAUDE.md rule 3).

**Timestamps are stored as fixed-width ISO-8601 text.** ``ts_utc`` as
``2026-08-16T18:00:30.123456Z`` (via :func:`timeutil.format_utc`) and ``ts_local``
as the naive ``2026-08-16T14:00:30.123456``. Both are fixed width with explicit
microseconds, so lexicographic text ordering is chronological ordering and the
round-trip is exact — no float epoch rounding, no ``sqlite3`` timestamp adapters
(deprecated in 3.12 anyway).

**Durability** (PLAN.md §15.6): WAL journalling plus ``synchronous=FULL``, one
commit per poll cycle. In WAL mode ``FULL`` fsyncs the write-ahead log at every
commit, so a committed poll cycle survives an OS crash or power loss, not merely
a process crash. This costs one fsync per 30s for a few hundred tiny rows —
irrelevant here (CLAUDE.md: "nothing here is performance-sensitive"), and the
alternative (``NORMAL``) can silently lose the last few commits on power loss.

**Concurrency**: the asyncio poller writes while the scheduler's uploader reads,
potentially from different threads. ``sqlite3`` connections must not be shared
across threads, so :class:`SpoolDB` keeps **one connection per thread**, created
lazily and tracked so :meth:`close` can tear them all down. Every connection sets
``busy_timeout`` (default 30s) and writes take an explicit ``BEGIN IMMEDIATE``,
so a concurrent writer waits for the lock instead of failing with
``SQLITE_BUSY`` halfway through a transaction. WAL means readers never block the
writer and vice versa.

**Idempotency**: a ``UNIQUE`` index over the canonical dedupe key
``(ts_utc, source, device_id, channel_id, metric)`` (:data:`model.DEDUPE_KEY`)
makes ``append`` naturally idempotent — re-running ``energycap poll --once`` over
the same instant cannot double-insert, matching the first-occurrence-wins
semantics of :func:`model.dedupe_observations`.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import NamedTuple

from energy_capture import timeutil
from energy_capture.logging import get_logger
from energy_capture.model import DEDUPE_KEY, MeterObservation, Observation, is_day_grain

__all__ = [
    "SCHEMA_VERSION",
    "PendingHour",
    "SpoolDB",
    "SpoolRow",
    "SpoolStats",
    "open_spool",
]

#: Bumped only when the on-disk table shape changes in a way old rows can't satisfy.
SCHEMA_VERSION = 1

_DEFAULT_BUSY_TIMEOUT_S = 30.0

#: Durability levels this spool will accept. ``OFF`` is deliberately absent — the
#: spool is the only copy of data that has not reached S3.
_SYNCHRONOUS_MODES = frozenset({"FULL", "NORMAL", "EXTRA"})

_log = get_logger("spool")


# --------------------------------------------------------------------- schema

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS observations (
    -- Monotonic arrival order. INTEGER PRIMARY KEY AUTOINCREMENT guarantees ids
    -- are never reused after a delete, so an uploader that captured "everything
    -- up to id N" can safely mark that range later (see mark_uploaded).
    id          INTEGER PRIMARY KEY AUTOINCREMENT,

    -- The 8 canonical schema columns (PLAN.md §3), timestamps as fixed-width
    -- ISO-8601 text so text ordering == chronological ordering.
    ts_utc      TEXT    NOT NULL,   -- '2026-08-16T18:00:30.123456Z' (canonical)
    ts_local    TEXT    NOT NULL,   -- '2026-08-16T14:00:30.123456' (naive wall clock)
    source      TEXT    NOT NULL,
    device_id   TEXT    NOT NULL,
    channel_id  TEXT    NOT NULL,
    metric      TEXT    NOT NULL,
    value       REAL    NOT NULL,
    unit        TEXT    NOT NULL,

    -- Partition/bucket keys, computed at INSERT via timeutil (never in SQL).
    local_date  TEXT    NOT NULL,   -- 'YYYY-MM-DD' LOCAL date == S3 partition
    local_hour  INTEGER NOT NULL,   -- 0..23 LOCAL wall-clock hour == part-file HH

    -- NULL until the uploader has VERIFIED the part file in S3 (PLAN.md §10).
    uploaded_at TEXT
)
"""

# The canonical dedupe key (model.DEDUPE_KEY). Makes `append` idempotent.
_CREATE_UNIQUE = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_observations_dedupe
    ON observations (ts_utc, source, device_id, channel_id, metric)
"""

_CREATE_INDEXES = (
    # "all un-uploaded rows for local hour H" — the uploader's hot path.
    """
    CREATE INDEX IF NOT EXISTS ix_observations_pending_hour
        ON observations (local_date, local_hour)
        WHERE uploaded_at IS NULL
    """,
    # oldest_pending_utc for status.json (PLAN.md §11).
    """
    CREATE INDEX IF NOT EXISTS ix_observations_pending_ts
        ON observations (ts_utc)
        WHERE uploaded_at IS NULL
    """,
    # purge(): uploaded AND older than the retention floor.
    """
    CREATE INDEX IF NOT EXISTS ix_observations_purgeable
        ON observations (ts_utc)
        WHERE uploaded_at IS NOT NULL
    """,
    # Full (uploaded + pending) read of one local hour — the uploader rewrites the
    # WHOLE part file, so it reads every row for the hour, not just pending ones.
    """
    CREATE INDEX IF NOT EXISTS ix_observations_hour
        ON observations (local_date, local_hour, id)
    """,
)

_INSERT = """
INSERT OR IGNORE INTO observations
    (ts_utc, ts_local, source, device_id, channel_id, metric, value, unit,
     local_date, local_hour, uploaded_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
"""

_SELECT_COLUMNS = (
    "id, ts_utc, ts_local, source, device_id, channel_id, metric, value, unit, "
    "local_date, local_hour, uploaded_at"
)

# Deterministic, and identical to model.SORT_KEY so the uploader's table is
# already in file order before it sorts. `id` breaks any residual tie.
_ROW_ORDER = "ORDER BY ts_utc, source, device_id, channel_id, metric, id"


# ----------------------------------------------------------------- row types


class PendingHour(NamedTuple):
    """A closed local hour that still has un-uploaded rows.

    A two-field :class:`NamedTuple`, so ``for local_date, hour in
    spool.pending_local_hours():`` reads exactly as PLAN.md §10 describes.
    """

    local_date: date
    hour: int


@dataclass(frozen=True, slots=True)
class SpoolRow:
    """A spooled observation plus its spool bookkeeping.

    ``rowid`` is what :meth:`SpoolDB.mark_uploaded` accepts, so an uploader can
    mark *exactly* the rows it wrote and nothing the poller appended in the
    meantime.
    """

    rowid: int
    observation: Observation
    local_date: date
    local_hour: int
    uploaded_at: datetime | None

    @property
    def uploaded(self) -> bool:
        return self.uploaded_at is not None


class SpoolStats(NamedTuple):
    """Spool counters for ``status.json`` (PLAN.md §11)."""

    pending_rows: int
    oldest_pending_utc: datetime | None
    uploaded_rows: int
    total_rows: int

    def to_status_dict(self) -> dict[str, object]:
        """The exact ``"spool"`` object of ``status.json``."""
        return {
            "pending_rows": self.pending_rows,
            "oldest_pending_utc": (
                timeutil.format_utc(self.oldest_pending_utc)
                if self.oldest_pending_utc is not None
                else None
            ),
        }


# ------------------------------------------------------------ codec helpers


def _encode_utc(ts: datetime) -> str:
    return timeutil.format_utc(ts)


def _decode_utc(text: str) -> datetime:
    return timeutil.ensure_utc(datetime.fromisoformat(text))


def _encode_local(ts: datetime) -> str:
    if ts.tzinfo is not None:
        raise ValueError(
            "ts_local must be a naive local wall clock (PLAN.md §2.4); "
            f"got tz-aware {ts!r}"
        )
    return ts.isoformat(sep="T", timespec="microseconds")


def _decode_local(text: str) -> datetime:
    return datetime.fromisoformat(text)


# ------------------------------------------------------------------ the spool


class SpoolDB:
    """The SQLite spool at ``{SPOOL_DIR}/spool.db``.

    Thread-safe: each thread gets its own connection (see the module docstring).
    Cheap to construct; the file and schema are created on first use.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        busy_timeout_s: float = _DEFAULT_BUSY_TIMEOUT_S,
        synchronous: str = "FULL",
    ) -> None:
        """Open (or create) the spool.

        Args:
            path: database file. Defaults to ``Settings.spool_db_path``.
            busy_timeout_s: how long a writer waits for the write lock before
                raising ``sqlite3.OperationalError``. The poller and uploader
                overlap by design, so this must comfortably exceed one upload.
            synchronous: ``FULL`` (default) fsyncs the WAL at every commit, so a
                committed poll cycle survives power loss. ``NORMAL`` is faster
                and strictly less durable; there is no reason to use it here.
        """
        if path is None:
            from energy_capture.config import get_settings

            path = get_settings().spool_db_path
        self._path = Path(path)
        self._busy_timeout_s = float(busy_timeout_s)
        # Interpolated into a PRAGMA (pragmas take no bind parameters), so it is
        # whitelisted rather than trusted.
        self._synchronous = synchronous.upper()
        if self._synchronous not in _SYNCHRONOUS_MODES:
            raise ValueError(
                f"synchronous={synchronous!r} is not one of {sorted(_SYNCHRONOUS_MODES)}"
            )
        self._connections: dict[int, sqlite3.Connection] = {}
        self._lock = threading.Lock()
        self._initialised = False

    # -------------------------------------------------------------- plumbing

    @property
    def path(self) -> Path:
        """Filesystem path of the spool database."""
        return self._path

    def connect(self) -> sqlite3.Connection:
        """This thread's connection, created on first use.

        ``check_same_thread=False`` is set only so :meth:`close` can shut down
        connections belonging to threads that have already exited — each
        connection is still used by exactly one thread.
        """
        ident = threading.get_ident()
        with self._lock:
            conn = self._connections.get(ident)
            if conn is not None:
                return conn

        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self._path,
            timeout=self._busy_timeout_s,
            isolation_level=None,  # explicit BEGIN IMMEDIATE; no implicit txns
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        # busy_timeout first: everything below may contend with a live poll cycle.
        conn.execute(f"PRAGMA busy_timeout={int(self._busy_timeout_s * 1000)}")
        self._ensure_wal(conn)
        conn.execute(f"PRAGMA synchronous={self._synchronous}")

        with self._lock:
            existing = self._connections.get(ident)
            if existing is not None:  # pragma: no cover - lost a benign race
                conn.close()
                return existing
            self._connections[ident] = conn

        self._ensure_schema(conn)
        return conn

    def _ensure_wal(self, conn: sqlite3.Connection) -> None:
        """Put the database in WAL mode, tolerating a concurrent opener.

        ``PRAGMA journal_mode=WAL`` needs a database-wide lock and — unlike
        ordinary statements — SQLite returns ``SQLITE_BUSY`` for it *without*
        consulting the busy handler. A second connection opened while the poller
        is mid-commit would therefore blow up at construction time. WAL is a
        persistent property of the file, so the common case is "already WAL,
        nothing to do"; only the very first opener has to set it, and it retries
        if it loses the race.
        """
        deadline = time.monotonic() + max(self._busy_timeout_s, 1.0)
        while True:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if str(mode).lower() == "wal":
                return
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError:  # pragma: no cover - timing
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        if self._initialised:
            return
        with self._lock:
            if self._initialised:
                return
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(_CREATE_TABLE)
                conn.execute(_CREATE_UNIQUE)
                for statement in _CREATE_INDEXES:
                    conn.execute(statement)
                conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            except BaseException:
                conn.rollback()
                raise
            conn.commit()
            self._initialised = True
        _log.debug(
            "spool_open",
            path=str(self._path),
            synchronous=self._synchronous,
            schema_version=SCHEMA_VERSION,
        )

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """One explicit write transaction.

        ``BEGIN IMMEDIATE`` takes the write lock up front, so a concurrent writer
        waits out ``busy_timeout`` instead of discovering the conflict at COMMIT.
        """
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        conn.commit()

    def close(self) -> None:
        """Close every connection this spool opened."""
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        for conn in connections:
            try:
                conn.close()
            except sqlite3.Error:  # pragma: no cover - closing a broken handle
                pass

    def __enter__(self) -> SpoolDB:
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"SpoolDB(path={str(self._path)!r})"

    # ---------------------------------------------------------------- writes

    def append(self, observations: Iterable[Observation]) -> int:
        """Append a poll cycle's observations in **one transaction** (PLAN.md §10).

        The whole batch is validated and encoded before the transaction opens, so
        a bad row can never leave a half-written cycle behind.

        Duplicates of the canonical dedupe key already in the spool are ignored
        (first occurrence wins, matching
        :func:`~energy_capture.model.dedupe_observations`), which is what makes a
        repeated ``energycap poll --once`` idempotent.

        Returns:
            The number of rows actually inserted (excludes ignored duplicates).
        """
        rows: list[tuple[object, ...]] = []
        # A poll cycle shares one ts_utc across all its rows (PLAN.md §6.5), so
        # memoise the zone conversion per distinct instant.
        buckets: dict[datetime, tuple[datetime, str, int]] = {}

        for obs in observations:
            if isinstance(obs, MeterObservation):
                raise ValueError(
                    "MeterObservation rows are not spooled: the spool feeds "
                    "energy/raw_30s, while meter intervals are imported straight "
                    "to energy/meter by `energycap import-greenbutton` "
                    "(PLAN.md §13). Storing one here would silently drop "
                    "interval_s."
                )
            if is_day_grain(obs.metric):
                raise ValueError(
                    f"day-grain metric {obs.metric!r} may not be spooled: the "
                    "spool feeds energy/raw_30s and day-grain rows would poison "
                    "the hourly rollups (CLAUDE.md rule 6). Bryant daily energy "
                    "is written straight to energy/daily."
                )

            ts_utc = timeutil.ensure_utc(obs.ts_utc)
            bucket = buckets.get(ts_utc)
            if bucket is None:
                # Computed here, once, via timeutil — never re-derived in SQL.
                local_naive = timeutil.to_local_naive(ts_utc)
                bucket = (local_naive, local_naive.date().isoformat(), local_naive.hour)
                buckets[ts_utc] = bucket
            local_naive, local_date, local_hour = bucket
            ts_local_text = _encode_local(obs.ts_local)  # rejects a tz-aware ts_local

            if obs.ts_local != local_naive:
                # The partition column and the human-readable column must describe
                # the same instant, or a query "on 2026-11-01" and the file it
                # reads disagree. make_observation() guarantees this; a
                # hand-built Observation might not.
                raise ValueError(
                    f"ts_local {obs.ts_local.isoformat()} is not the local wall "
                    f"clock of ts_utc {timeutil.format_utc(ts_utc)} "
                    f"(expected {local_naive.isoformat()} in {timeutil.tz_name()}); "
                    "build rows with model.make_observation()"
                )

            rows.append(
                (
                    _encode_utc(ts_utc),
                    ts_local_text,
                    obs.source,
                    obs.device_id,
                    obs.channel_id,
                    obs.metric,
                    float(obs.value),
                    obs.unit,
                    local_date,
                    local_hour,
                )
            )

        if not rows:
            return 0

        with self._write() as conn:
            before = conn.total_changes
            conn.executemany(_INSERT, rows)
            inserted = conn.total_changes - before

        skipped = len(rows) - inserted
        _log.debug("spool_append", rows=inserted, duplicates_ignored=skipped)
        return inserted

    def mark_uploaded(
        self,
        local_date: date,
        hour: int,
        rows: int | Iterable[int] | None = None,
        *,
        uploaded_at: datetime | None = None,
    ) -> int:
        """Mark spool rows for a local hour as uploaded.

        **Call this only after the uploader has verified the object in S3**
        (row count read back from the Parquet footer, PLAN.md §10). Nothing in
        this module ever marks a row on its own.

        Args:
            local_date: LOCAL partition date.
            hour: LOCAL wall-clock hour (the ``HH`` in the part filename). On a
                fall-back day, ``hour=1`` covers both 01:00 hours — the same rows
                the single part file holds.
            rows: what to mark. ``None`` marks every currently-pending row for
                the hour; an ``int`` marks pending rows with ``id <= rows``
                (pass the max rowid you read, so rows the poller appended during
                the upload stay pending); an iterable marks exactly those ids.
            uploaded_at: instant to record; defaults to now.

        Returns:
            Number of rows newly marked. Already-marked rows keep their original
            ``uploaded_at`` — re-running an upload is a no-op, not a rewrite.
        """
        stamp = _encode_utc(uploaded_at if uploaded_at is not None else timeutil.now_utc())
        where = "local_date = ? AND local_hour = ? AND uploaded_at IS NULL"
        params: list[object] = [stamp, local_date.isoformat(), int(hour)]

        if rows is None:
            pass
        elif isinstance(rows, int) and not isinstance(rows, bool):
            where += " AND id <= ?"
            params.append(int(rows))
        else:
            ids = [int(r) for r in rows]
            if not ids:
                return 0
            where += f" AND id IN ({','.join('?' * len(ids))})"
            params.extend(ids)

        with self._write() as conn:
            cursor = conn.execute(
                f"UPDATE observations SET uploaded_at = ? WHERE {where}", params
            )
            marked = cursor.rowcount

        _log.info(
            "spool_marked_uploaded",
            local_date=local_date.isoformat(),
            hour=f"{int(hour):02d}",
            rows=marked,
        )
        return marked

    def purge(
        self,
        retention_days: int | None = None,
        *,
        now: datetime | None = None,
        vacuum: bool = False,
    ) -> int:
        """Delete rows that are uploaded **and** past the retention floor.

        Both conditions, always (PLAN.md §10): the uploaded flag is the primary
        safety interlock and ``SPOOL_RETENTION_DAYS`` is the second one. A row
        that is old but never uploaded is *kept* — it is un-landed data. A row
        uploaded five minutes ago is *kept* — the retention window is what lets a
        human notice a bad upload and re-run it from the spool.

        Age is measured on ``ts_utc`` (when the sample was observed), not on
        ``uploaded_at``.

        Returns:
            Number of rows deleted.
        """
        if retention_days is None:
            from energy_capture.config import get_settings

            retention_days = get_settings().spool_retention_days
        retention_days = int(retention_days)
        if retention_days < 1:
            raise ValueError(
                f"retention_days must be >= 1, got {retention_days}: the spool is "
                "the only copy of data that has not landed in S3"
            )

        reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
        cutoff = reference - timedelta(days=retention_days)

        with self._write() as conn:
            cursor = conn.execute(
                "DELETE FROM observations "
                "WHERE uploaded_at IS NOT NULL AND ts_utc < ?",
                (_encode_utc(cutoff),),
            )
            deleted = cursor.rowcount

        if deleted:
            _log.info(
                "spool_purged",
                rows=deleted,
                retention_days=retention_days,
                cutoff_utc=timeutil.format_utc(cutoff),
            )
        if vacuum:
            # Outside a transaction by definition.
            self.connect().execute("VACUUM")
        return deleted

    # ----------------------------------------------------------------- reads

    def pending_local_hours(self, *, now: datetime | None = None) -> list[PendingHour]:
        """Closed local hours that still hold un-uploaded rows.

        "Closed" means the hour's UTC end has passed, so the currently-open hour
        is never returned — uploading it would produce a part file the next
        upload has to overwrite with more rows. The bound comes from
        :func:`timeutil.local_hour_bounds_utc`, so on a fall-back day the
        repeated 01:00 hour is closed only once *both* occurrences are over.

        Returns:
            ``(local_date, hour)`` pairs in chronological order.
        """
        reference = timeutil.ensure_utc(now) if now is not None else timeutil.now_utc()
        conn = self.connect()
        candidates = conn.execute(
            "SELECT local_date, local_hour FROM observations "
            "WHERE uploaded_at IS NULL "
            "GROUP BY local_date, local_hour "
            "ORDER BY local_date, local_hour"
        ).fetchall()

        out: list[PendingHour] = []
        for row in candidates:
            local_date = date.fromisoformat(row["local_date"])
            hour = int(row["local_hour"])
            try:
                _, end_utc = timeutil.local_hour_bounds_utc(local_date, hour)
            except ValueError:
                # A wall-clock hour that DST says cannot exist. It should be
                # unreachable (local_hour came from a real instant), but stranding
                # real rows forever would be worse than uploading them, so treat
                # it as closed and make the anomaly loud.
                _log.warning(
                    "spool_pending_hour_nonexistent",
                    local_date=row["local_date"],
                    hour=f"{hour:02d}",
                )
                out.append(PendingHour(local_date, hour))
                continue
            if end_utc <= reference:
                out.append(PendingHour(local_date, hour))
        return out

    def read_local_hour(
        self, local_date: date, hour: int, *, pending_only: bool = False
    ) -> list[SpoolRow]:
        """Spool rows for one local hour, with their rowids and upload state.

        ``pending_only`` defaults to **False** on purpose. Part filenames are
        deterministic (``part-{YYYYMMDD}T{HH}.parquet``), so an upload
        *overwrites*: it must therefore contain every row for the hour, not just
        the ones that are still pending. Uploading only the pending rows after a
        partial failure would replace a complete part file with an incomplete
        one — silent data loss.
        """
        conn = self.connect()
        sql = (
            f"SELECT {_SELECT_COLUMNS} FROM observations "
            "WHERE local_date = ? AND local_hour = ?"
        )
        if pending_only:
            sql += " AND uploaded_at IS NULL"
        rows = conn.execute(
            f"{sql} {_ROW_ORDER}", (local_date.isoformat(), int(hour))
        ).fetchall()
        return [_to_spool_row(r) for r in rows]

    def rows_for_local_hour(
        self, local_date: date, hour: int, *, pending_only: bool = False
    ) -> list[Observation]:
        """Observations for one local hour, in :data:`model.SORT_KEY` order.

        See :meth:`read_local_hour` for why ``pending_only`` defaults to False.
        """
        return [row.observation for row in self.read_local_hour(
            local_date, hour, pending_only=pending_only
        )]

    def max_rowid_for_local_hour(
        self, local_date: date, hour: int, *, pending_only: bool = False
    ) -> int | None:
        """Highest rowid currently in a local hour, or ``None`` if it is empty.

        Capture this alongside the rows you upload and pass it back to
        :meth:`mark_uploaded`, so rows the poller appended mid-upload stay
        pending and get picked up by the next run.
        """
        conn = self.connect()
        sql = "SELECT MAX(id) AS max_id FROM observations WHERE local_date = ? AND local_hour = ?"
        if pending_only:
            sql += " AND uploaded_at IS NULL"
        row = conn.execute(sql, (local_date.isoformat(), int(hour))).fetchone()
        return None if row is None or row["max_id"] is None else int(row["max_id"])

    def pending_rows_for_hour(self, local_date: date, hour: int) -> int:
        """Count of un-uploaded rows in a local hour (for logging/health)."""
        conn = self.connect()
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM observations "
            "WHERE local_date = ? AND local_hour = ? AND uploaded_at IS NULL",
            (local_date.isoformat(), int(hour)),
        ).fetchone()
        return int(row["n"])

    def stats(self) -> SpoolStats:
        """Counters for the ``"spool"`` block of ``status.json`` (PLAN.md §11)."""
        conn = self.connect()
        row = conn.execute(
            "SELECT "
            "  COUNT(*) AS total, "
            "  SUM(CASE WHEN uploaded_at IS NULL THEN 1 ELSE 0 END) AS pending, "
            "  MIN(CASE WHEN uploaded_at IS NULL THEN ts_utc END) AS oldest "
            "FROM observations"
        ).fetchone()
        total = int(row["total"])
        pending = int(row["pending"] or 0)
        oldest = _decode_utc(row["oldest"]) if row["oldest"] else None
        return SpoolStats(
            pending_rows=pending,
            oldest_pending_utc=oldest,
            uploaded_rows=total - pending,
            total_rows=total,
        )


def _to_spool_row(row: sqlite3.Row) -> SpoolRow:
    observation = Observation(
        ts_utc=_decode_utc(row["ts_utc"]),
        ts_local=_decode_local(row["ts_local"]),
        source=row["source"],
        device_id=row["device_id"],
        channel_id=row["channel_id"],
        metric=row["metric"],
        value=float(row["value"]),
        unit=row["unit"],
    )
    return SpoolRow(
        rowid=int(row["id"]),
        observation=observation,
        local_date=date.fromisoformat(row["local_date"]),
        local_hour=int(row["local_hour"]),
        uploaded_at=_decode_utc(row["uploaded_at"]) if row["uploaded_at"] else None,
    )


def open_spool(path: str | Path | None = None, **kwargs: object) -> SpoolDB:
    """Open the configured spool (``{SPOOL_DIR}/spool.db``) and create its schema."""
    spool = SpoolDB(path, **kwargs)  # type: ignore[arg-type]
    spool.connect()
    return spool


# Keep the module honest: the UNIQUE index above must stay the canonical key.
_EXPECTED_DEDUPE_KEY: Sequence[str] = (
    "ts_utc",
    "source",
    "device_id",
    "channel_id",
    "metric",
)
if tuple(DEDUPE_KEY) != tuple(_EXPECTED_DEDUPE_KEY):  # pragma: no cover - guard
    raise RuntimeError(
        "model.DEDUPE_KEY changed; update ux_observations_dedupe in spool/sqlite.py"
    )
