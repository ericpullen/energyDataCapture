"""S3 object I/O and the single source of truth for the S3 key layout.

Two jobs, both of them safety-critical:

**1. The key layout (PLAN.md §4).** Every key this pipeline ever touches is
built by a function in this module. Nothing else may format an S3 key by hand —
if a filename convention changes, it changes here and the tests that pin the
literal keys fail loudly::

    energy/raw_30s/year=YYYY/month=MM/day=DD/part-{YYYYMMDD}T{HH}.parquet
    energy/raw_30s/year=YYYY/month=MM/day=DD/day-{YYYYMMDD}.parquet
    energy/raw_30s_parts_archive/year=YYYY/month=MM/day=DD/<part file>   (non-tabled)
    energy/hourly/year=YYYY/month=MM/rollup-{YYYYMMDD}.parquet
    energy/daily/year=YYYY/bryant-{YYYYMM}.parquet
    energy/dim_channel/dim_channel.parquet
    energy/meter/year=YYYY/{source}-{YYYYMM}.parquet                     (future, §13)

All partition values are the **LOCAL** date (CLAUDE.md rule 4) — callers pass a
``datetime.date`` that is already local, or an aware ``ts_utc`` and let
:mod:`energy_capture.timeutil` do the conversion.

**2. Atomic writes and the verify gate (PLAN.md §10).** A partial or corrupt
object must never appear at a final key, because the Glue tables point straight
at those prefixes. So :func:`write_table_atomic` does::

    write Parquet (ZSTD) to energy/_tmp/<uuid>-<name>
      -> read the TEMP object's Parquet footer, check num_rows
      -> copy temp -> final key   (S3 copies are atomic: all-or-nothing)
      -> delete temp
      -> read the FINAL object's footer and check num_rows again

Verifying the temp *before* the copy is what guarantees no bad bytes ever reach
a final key. :func:`verify_row_count` is the same footer check exposed on its
own: the uploader calls it before marking spool rows uploaded and the compactor
calls it before archiving parts (PLAN.md §10, §15.6, §15.7). It reads only the
Parquet footer via ranged GETs — never the whole object.

Sortedness is **checked, not fixed**: rows are expected to arrive already sorted
by :data:`energy_capture.model.SORT_KEY` (that is what
``model.observations_to_table`` produces, deterministically). An unsorted table
raises :class:`UnsortedRowsError` rather than being silently re-sorted, because
silent re-sorting would hide a caller that skipped the deterministic builder and
would break byte-identical idempotent re-runs. Pass ``sort_key=()`` to skip the
check for a table with no time columns (``dim_channel``).
"""

from __future__ import annotations

import io
import os
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import BaseClient
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from energy_capture import model, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger

__all__ = [
    "ARCHIVE_PREFIX",
    "DAILY_PREFIX",
    "DIM_CHANNEL_PREFIX",
    "HOURLY_PREFIX",
    "METER_PREFIX",
    "PARQUET_COMPRESSION",
    "PART_FILE_PREFIX",
    "RAW_30S_PREFIX",
    "ROOT_PREFIX",
    "TMP_PREFIX",
    "MoveResult",
    "RowCountMismatch",
    "S3IOError",
    "S3RangeReader",
    "UnsortedRowsError",
    "WriteResult",
    "copy_key",
    "daily_key",
    "daily_year_prefix",
    "configured_bucket",
    "default_bucket",
    "delete_key",
    "dim_channel_key",
    "get_client",
    "get_session",
    "hourly_key",
    "hourly_month_prefix",
    "key_exists",
    "list_keys",
    "list_raw_30s_parts",
    "meter_key",
    "meter_year_prefix",
    "move_keys",
    "object_size",
    "parquet_row_count",
    "parse_s3_uri",
    "raw_30s_archive_day_prefix",
    "raw_30s_archive_key",
    "raw_30s_day_glob_uri",
    "raw_30s_day_key",
    "raw_30s_day_prefix",
    "raw_30s_part_key",
    "raw_30s_part_key_for_ts",
    "read_parquet_metadata",
    "read_table",
    "require_row_count",
    "reset_clients",
    "s3_uri",
    "temp_key",
    "verify_row_count",
    "write_table_atomic",
]

log = get_logger("s3io")

# --------------------------------------------------------------------- layout

#: Everything this pipeline writes lives under this one prefix (PLAN.md §4).
ROOT_PREFIX = "energy"

#: 30s observations. Tabled as ``energy_raw_30s``; contains exactly one
#: authoritative set per local day (hourly parts, or the compacted day file).
RAW_30S_PREFIX = f"{ROOT_PREFIX}/raw_30s"

#: Where the compactor parks parts after a verified day file exists. Deliberately
#: a **sibling** of ``raw_30s`` and deliberately **not** a Glue table, so parts and
#: the day file can never be double-counted by a query (PLAN.md §10).
ARCHIVE_PREFIX = f"{ROOT_PREFIX}/raw_30s_parts_archive"

#: Derived hourly rollup — fully disposable/regenerable.
HOURLY_PREFIX = f"{ROOT_PREFIX}/hourly"

#: Bryant daily energy (day grain, ``ts_utc`` = local midnight).
DAILY_PREFIX = f"{ROOT_PREFIX}/daily"

#: Semantic layer: one file, overwritten atomically.
DIM_CHANNEL_PREFIX = f"{ROOT_PREFIX}/dim_channel"

#: Future LG&E Green Button interval data (PLAN.md §13).
METER_PREFIX = f"{ROOT_PREFIX}/meter"

#: Staging area for atomic writes. Outside every Glue table location and outside
#: every partition-projection template, so a half-written object is invisible to
#: Athena/DuckDB even in the instant before it is copied to its final key.
TMP_PREFIX = f"{ROOT_PREFIX}/_tmp"

#: Filename prefixes, used to tell parts from day files when listing a partition.
PART_FILE_PREFIX = "part-"
DAY_FILE_PREFIX = "day-"

PARQUET_SUFFIX = ".parquet"

#: PLAN.md §3: ZSTD everywhere.
PARQUET_COMPRESSION = "zstd"

#: Retry budget for transient S3 failures (on top of botocore's own retries).
S3_RETRY_ATTEMPTS = 4


# ------------------------------------------------------------------ exceptions


class S3IOError(RuntimeError):
    """Base class for the failures this module raises deliberately."""


class RowCountMismatch(S3IOError):
    """A written object's Parquet footer disagrees with the expected row count.

    This is the gate the uploader and compactor depend on: nothing is marked
    uploaded and no part is archived until the row count matches.
    """


class UnsortedRowsError(ValueError):
    """A table reached the writer without being sorted by the dataset's sort key."""


# --------------------------------------------------------------- key builders


def s3_uri(bucket: str, key: str) -> str:
    """``s3://bucket/key`` — the form DuckDB ``httpfs`` and Athena want."""
    return f"s3://{bucket}/{key.lstrip('/')}"


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri!r}")
    remainder = uri[len("s3://") :]
    bucket, _, key = remainder.partition("/")
    if not bucket:
        raise ValueError(f"not an s3 uri: {uri!r}")
    return bucket, key


def _partition_dir(local_day: date, *, depth: int = 3) -> str:
    """``year=YYYY/month=MM/day=DD`` (or fewer levels) for a LOCAL date."""
    year, month, day = timeutil.partition_parts_for_local_date(local_day)
    parts = [f"year={year}", f"month={month}", f"day={day}"][:depth]
    return "/".join(parts)


def _stamp_day(local_day: date) -> str:
    return f"{local_day:%Y%m%d}"


def _stamp_month(local_day: date) -> str:
    return f"{local_day:%Y%m}"


def raw_30s_day_prefix(local_day: date) -> str:
    """``energy/raw_30s/year=YYYY/month=MM/day=DD/`` for a LOCAL date."""
    return f"{RAW_30S_PREFIX}/{_partition_dir(local_day)}/"


def raw_30s_part_key(local_day: date, hour: int) -> str:
    """Hourly part written by the uploader: ``part-{YYYYMMDD}T{HH}.parquet``.

    ``hour`` is the local **wall-clock** hour label. On a DST fall-back day the
    wall-clock hour ``01`` occurs twice and both occurrences belong in the single
    ``...T01.parquet`` part (they stay distinct rows, keyed by ``ts_utc``); use
    :func:`energy_capture.timeutil.local_wall_hours_of_day` to enumerate the
    hours that exist on a given day.
    """
    if not 0 <= int(hour) <= 23:
        raise ValueError(f"local wall-clock hour must be 0..23, got {hour!r}")
    name = f"{PART_FILE_PREFIX}{_stamp_day(local_day)}T{int(hour):02d}{PARQUET_SUFFIX}"
    return raw_30s_day_prefix(local_day) + name


def raw_30s_part_key_for_ts(ts_utc: datetime) -> str:
    """The part key an observation at ``ts_utc`` belongs in (LOCAL-date partition)."""
    local_day = timeutil.local_date_of(ts_utc)
    _, hour = timeutil.local_hour_stamp(ts_utc)
    return raw_30s_part_key(local_day, int(hour))


def raw_30s_day_key(local_day: date) -> str:
    """Compacted day file: ``day-{YYYYMMDD}.parquet`` (PLAN.md §4)."""
    name = f"{DAY_FILE_PREFIX}{_stamp_day(local_day)}{PARQUET_SUFFIX}"
    return raw_30s_day_prefix(local_day) + name


def raw_30s_day_glob_uri(bucket: str, local_day: date) -> str:
    """``s3://…/energy/raw_30s/year=…/day=DD/*.parquet`` for DuckDB scans."""
    return s3_uri(bucket, raw_30s_day_prefix(local_day) + "*" + PARQUET_SUFFIX)


def raw_30s_archive_day_prefix(local_day: date) -> str:
    """``energy/raw_30s_parts_archive/year=YYYY/month=MM/day=DD/``."""
    return f"{ARCHIVE_PREFIX}/{_partition_dir(local_day)}/"


def raw_30s_archive_key(local_day: date, filename: str) -> str:
    """Archive destination for one part file (same basename, sibling prefix)."""
    return raw_30s_archive_day_prefix(local_day) + filename.rsplit("/", 1)[-1]


def hourly_month_prefix(local_day: date) -> str:
    """``energy/hourly/year=YYYY/month=MM/`` — the rollup is month-partitioned."""
    return f"{HOURLY_PREFIX}/{_partition_dir(local_day, depth=2)}/"


def hourly_key(local_day: date) -> str:
    """One rollup file per local day: ``rollup-{YYYYMMDD}.parquet`` (PLAN.md §4)."""
    return hourly_month_prefix(local_day) + f"rollup-{_stamp_day(local_day)}{PARQUET_SUFFIX}"


def daily_year_prefix(local_day: date) -> str:
    """``energy/daily/year=YYYY/`` — day-grain rows, partitioned by year only."""
    return f"{DAILY_PREFIX}/{_partition_dir(local_day, depth=1)}/"


def daily_key(local_day: date, *, source: str = model.SOURCE_BRYANT) -> str:
    """Bryant daily energy: ``bryant-{YYYYMM}.parquet``, regenerated per month."""
    return daily_year_prefix(local_day) + f"{source}-{_stamp_month(local_day)}{PARQUET_SUFFIX}"


def dim_channel_key() -> str:
    """``energy/dim_channel/dim_channel.parquet`` — single file, overwritten."""
    return f"{DIM_CHANNEL_PREFIX}/dim_channel{PARQUET_SUFFIX}"


def meter_year_prefix(local_day: date) -> str:
    """``energy/meter/year=YYYY/`` (future LG&E Green Button, PLAN.md §13)."""
    return f"{METER_PREFIX}/{_partition_dir(local_day, depth=1)}/"


def meter_key(local_day: date, *, source: str = model.SOURCE_LGE) -> str:
    """``energy/meter/year=YYYY/{source}-{YYYYMM}.parquet``.

    PLAN.md §4 names the ``meter`` prefix but not its filename (§13 is
    "design now, build later"). We mirror the ``energy/daily`` convention —
    ``{source}-{YYYYMM}.parquet``, one regenerable file per month touched — so an
    idempotent Green Button import overwrites rather than accumulating files.
    """
    return meter_year_prefix(local_day) + f"{source}-{_stamp_month(local_day)}{PARQUET_SUFFIX}"


def temp_key(final_key: str) -> str:
    """A unique staging key for ``final_key`` under :data:`TMP_PREFIX`.

    The uuid makes concurrent writers (a scheduled stage and a manual CLI re-run)
    unable to clobber each other's staging object; the basename is kept so a
    stranded temp object is diagnosable at a glance.
    """
    basename = final_key.rsplit("/", 1)[-1] or "object"
    return f"{TMP_PREFIX}/{uuid.uuid4().hex}-{basename}"


def default_bucket() -> str:
    """``S3_BUCKET`` from settings, raising a named error when unset."""
    return get_settings().require("s3_bucket")


def configured_bucket() -> str | None:
    """``S3_BUCKET``, or ``None`` when there is not one.

    The counterpart to :func:`default_bucket` for stages where S3 is a *mirror*
    rather than the destination: ``fetch-daily`` and ``backfill`` write a local
    Parquet month whether or not a bucket exists (``stages/dailystore``), so for
    them an unset bucket is a configuration state, not an error.
    """
    value = getattr(get_settings(), "s3_bucket", None)
    text = str(value).strip() if value is not None else ""
    return text or None


# ------------------------------------------------------------ client plumbing

_session: boto3.session.Session | None = None
_clients: dict[str, BaseClient] = {}


def _boto_config() -> BotoConfig:
    # botocore's own retry layer handles the common 5xx/throttle case; the
    # tenacity layer below covers what it gives up on. Nothing here is
    # latency-sensitive, so the budget is generous.
    return BotoConfig(
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=10,
        read_timeout=60,
    )


def _drop_empty_aws_profile() -> bool:
    """Remove a set-but-empty ``AWS_PROFILE`` from the environment.

    ``AWS_PROFILE=`` is not "no profile" to botocore — it is a profile *named*
    empty string, and every client construction then raises
    ``ProfileNotFound: The config profile () could not be found``. Passing no
    ``profile_name`` does not help: botocore re-reads the variable itself when it
    builds its config store, so the only fix is for the variable not to be there.

    It is an easy trap to fall into because ``env_file`` in compose forwards a
    bare ``AWS_PROFILE=`` line verbatim, and ``.env.example`` used to ship one
    (DEVIATIONS.md #176). It cost the first S3 deployment a cycle: the container
    had valid static keys and still could not build a client.

    Returns whether anything was removed, so the caller can say so once.
    """
    if "AWS_PROFILE" in os.environ and not os.environ["AWS_PROFILE"].strip():
        del os.environ["AWS_PROFILE"]
        return True
    return False


def get_session() -> boto3.session.Session:
    """Process-wide boto3 session honouring ``AWS_PROFILE`` / ``AWS_REGION``.

    Built lazily: importing this module must never require credentials (the
    pure-logic tests import it to exercise the key builders).
    """
    global _session
    if _session is None:
        if _drop_empty_aws_profile():
            log.warning("aws_profile_empty_ignored")
        settings = get_settings()
        kwargs: dict[str, str] = {}
        if settings.aws_profile:
            kwargs["profile_name"] = settings.aws_profile
        if settings.aws_region:
            kwargs["region_name"] = settings.aws_region
        _session = boto3.session.Session(**kwargs)
    return _session


def get_client(service: str = "s3") -> BaseClient:
    """Cached boto3 client for ``service`` (default ``s3``)."""
    client = _clients.get(service)
    if client is None:
        client = get_session().client(service, config=_boto_config())
        _clients[service] = client
    return client


def reset_clients() -> None:
    """Drop the cached session/clients.

    Tests call this inside a ``moto`` mock so the client is constructed against
    the mocked endpoint; ``reset_settings_cache`` callers should call it too.
    """
    global _session
    _session = None
    _clients.clear()


def _resolve(client: BaseClient | None) -> BaseClient:
    return client if client is not None else get_client("s3")


# ----------------------------------------------------------------- retrying

_TRANSIENT_ERROR_CODES = frozenset(
    {
        "InternalError",
        "InternalServerError",
        "ServiceUnavailable",
        "SlowDown",
        "ThrottlingException",
        "Throttling",
        "RequestTimeout",
        "RequestTimeTooSkewed",
        "RequestThrottled",
        "TooManyRequests",
        "503",
        "500",
    }
)


def _is_transient(exc: BaseException) -> bool:
    """True for the failures worth retrying — never for 4xx (a real bug)."""
    if isinstance(
        exc,
        (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError, ConnectTimeoutError),
    ):
        return True
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        if str(error.get("Code", "")) in _TRANSIENT_ERROR_CODES:
            return True
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return isinstance(status, int) and status >= 500
    return False


def _log_retry(state: RetryCallState) -> None:
    outcome = state.outcome
    error = outcome.exception() if outcome is not None else None
    log.warning(
        "s3_retry",
        operation=getattr(state.fn, "__name__", "?"),
        attempt=state.attempt_number,
        error=type(error).__name__ if error else None,
        detail=str(error) if error else None,
    )


_transient_retry = retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(S3_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    before_sleep=_log_retry,
    reraise=True,
)


# --------------------------------------------------------- primitive S3 verbs


@_transient_retry
def _put_object(client: BaseClient, bucket: str, key: str, body: bytes) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body)


@_transient_retry
def _get_object_bytes(client: BaseClient, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


@_transient_retry
def _get_range(client: BaseClient, bucket: str, key: str, start: int, end: int) -> bytes:
    """Inclusive byte range ``[start, end]``."""
    response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    return response["Body"].read()


@_transient_retry
def _head_object(client: BaseClient, bucket: str, key: str) -> dict:
    return client.head_object(Bucket=bucket, Key=key)


@_transient_retry
def _copy_object(
    client: BaseClient, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str
) -> None:
    client.copy_object(
        Bucket=dst_bucket,
        Key=dst_key,
        CopySource={"Bucket": src_bucket, "Key": src_key},
    )


@_transient_retry
def _delete_object(client: BaseClient, bucket: str, key: str) -> None:
    client.delete_object(Bucket=bucket, Key=key)


def _is_not_found(exc: ClientError) -> bool:
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


# ------------------------------------------------------------ public S3 verbs


def key_exists(bucket: str, key: str, *, client: BaseClient | None = None) -> bool:
    """True when the object exists. A missing object is not an error."""
    try:
        _head_object(_resolve(client), bucket, key)
    except ClientError as exc:
        if _is_not_found(exc):
            return False
        raise
    return True


def object_size(bucket: str, key: str, *, client: BaseClient | None = None) -> int | None:
    """Object size in bytes, or ``None`` when it does not exist."""
    try:
        return int(_head_object(_resolve(client), bucket, key)["ContentLength"])
    except ClientError as exc:
        if _is_not_found(exc):
            return None
        raise


def list_keys(
    bucket: str,
    prefix: str,
    *,
    suffix: str | None = None,
    client: BaseClient | None = None,
) -> list[str]:
    """Every key under ``prefix``, sorted — deterministic input for the compactor."""
    s3 = _resolve(client)
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", ()):
            key = item["Key"]
            if key.endswith("/"):  # directory marker, not an object we wrote
                continue
            if suffix is not None and not key.endswith(suffix):
                continue
            keys.append(key)
    keys.sort()
    return keys


def list_raw_30s_parts(
    bucket: str, local_day: date, *, client: BaseClient | None = None
) -> list[str]:
    """The hourly ``part-*.parquet`` keys for a local day (never the day file)."""
    prefix = raw_30s_day_prefix(local_day)
    return [
        key
        for key in list_keys(bucket, prefix, suffix=PARQUET_SUFFIX, client=client)
        if key.rsplit("/", 1)[-1].startswith(PART_FILE_PREFIX)
    ]


def copy_key(
    bucket: str,
    src_key: str,
    dst_key: str,
    *,
    src_bucket: str | None = None,
    client: BaseClient | None = None,
) -> None:
    """Server-side copy within (or into) ``bucket``. Overwrites ``dst_key``."""
    _copy_object(_resolve(client), src_bucket or bucket, src_key, bucket, dst_key)


def delete_key(bucket: str, key: str, *, client: BaseClient | None = None) -> None:
    """Delete an object. Deleting a key that is already gone is a no-op."""
    _delete_object(_resolve(client), bucket, key)


def read_table(bucket: str, key: str, *, client: BaseClient | None = None) -> pa.Table:
    """Read a whole Parquet object into an Arrow table (files are a few MB at most)."""
    data = _get_object_bytes(_resolve(client), bucket, key)
    return pq.read_table(pa.BufferReader(data))


# ----------------------------------------------- footer-only metadata reading


class S3RangeReader(io.RawIOBase):
    """A seekable read-only file over ranged S3 GETs.

    Handing this to :class:`pyarrow.parquet.ParquetFile` makes the reader fetch
    only the bytes it actually needs — for a metadata-only open that is the
    footer (one or two GETs), not the whole object. :attr:`bytes_fetched` records
    what was transferred, which is what the "reads only the footer" test asserts.
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        client: BaseClient | None = None,
        size: int | None = None,
    ) -> None:
        super().__init__()
        self._client = _resolve(client)
        self._bucket = bucket
        self._key = key
        self._size = (
            int(size)
            if size is not None
            else int(_head_object(self._client, bucket, key)["ContentLength"])
        )
        self._pos = 0
        #: Total bytes actually pulled from S3 by this reader.
        self.bytes_fetched = 0

    # -- io plumbing -------------------------------------------------------
    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def size(self) -> int:
        return self._size

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._pos + offset
        elif whence == io.SEEK_END:
            target = self._size + offset
        else:  # pragma: no cover - io contract
            raise ValueError(f"invalid whence {whence!r}")
        self._pos = max(0, min(int(target), self._size))
        return self._pos

    def readinto(self, buffer) -> int:  # type: ignore[override]
        want = len(buffer)
        if want == 0 or self._pos >= self._size:
            return 0
        last = min(self._pos + want, self._size) - 1
        chunk = _get_range(self._client, self._bucket, self._key, self._pos, last)
        self.bytes_fetched += len(chunk)
        buffer[: len(chunk)] = chunk
        self._pos += len(chunk)
        return len(chunk)


def read_parquet_metadata(
    bucket: str, key: str, *, client: BaseClient | None = None
) -> pq.FileMetaData:
    """Read **only** the Parquet footer of an S3 object and return its metadata."""
    reader = S3RangeReader(bucket, key, client=client)
    try:
        parquet_file = pq.ParquetFile(reader)
        metadata = parquet_file.metadata
        log.debug(
            "parquet_footer_read",
            key=key,
            bytes_fetched=reader.bytes_fetched,
            object_bytes=reader.size(),
            rows=metadata.num_rows,
        )
        return metadata
    finally:
        reader.close()


def parquet_row_count(bucket: str, key: str, *, client: BaseClient | None = None) -> int:
    """Row count from the Parquet footer, without downloading the data pages."""
    return int(read_parquet_metadata(bucket, key, client=client).num_rows)


def verify_row_count(
    bucket: str,
    key: str,
    expected: int,
    *,
    client: BaseClient | None = None,
) -> bool:
    """Footer row count of ``key`` equals ``expected``?

    **This is the gate.** The uploader must not mark spool rows uploaded and the
    compactor must not archive parts until this returns ``True`` (PLAN.md §10).
    A missing object or a mismatch returns ``False`` and logs; it never raises,
    so a caller can degrade to "leave the data where it is and retry next hour".
    Use :func:`require_row_count` when the caller wants the exception.
    """
    try:
        actual = parquet_row_count(bucket, key, client=client)
    except ClientError as exc:
        if _is_not_found(exc):
            log.error("verify_missing", bucket=bucket, key=key, expected=expected)
            return False
        raise
    if actual != int(expected):
        log.error(
            "verify_row_count_mismatch",
            bucket=bucket,
            key=key,
            expected=int(expected),
            actual=actual,
        )
        return False
    log.debug("verify_ok", bucket=bucket, key=key, rows=actual)
    return True


def require_row_count(
    bucket: str, key: str, expected: int, *, client: BaseClient | None = None
) -> int:
    """:func:`verify_row_count`, but raising :class:`RowCountMismatch` on failure."""
    if not verify_row_count(bucket, key, expected, client=client):
        raise RowCountMismatch(
            f"{s3_uri(bucket, key)} does not contain the expected {int(expected)} rows"
        )
    return int(expected)


# ------------------------------------------------------------- atomic writing


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of an atomic write."""

    bucket: str
    key: str
    rows: int
    bytes_written: int
    temp_key: str
    verified: bool

    @property
    def uri(self) -> str:
        return s3_uri(self.bucket, self.key)


def _sort_key_for_table(table: pa.Table) -> tuple[str, ...]:
    """The sort key a table is expected to already be in."""
    names = set(table.column_names)
    if set(model.HOURLY_SORT_KEY) <= names:
        return model.HOURLY_SORT_KEY
    if set(model.SORT_KEY) <= names:
        return model.SORT_KEY
    return ()


def _assert_sorted(table: pa.Table, keys: Sequence[str], key: str) -> None:
    if not keys or table.num_rows <= 1:
        return
    columns = [table.column(name).to_pylist() for name in keys]
    previous: tuple | None = None
    for index, row in enumerate(zip(*columns, strict=True)):
        if previous is not None and row < previous:
            raise UnsortedRowsError(
                f"rows for {key} are not sorted by {tuple(keys)}: row {index} "
                f"{row!r} precedes {previous!r}. Build tables with "
                "model.observations_to_table() (it sorts deterministically) "
                "rather than sorting here — silent re-sorting would hide the bug "
                "and break byte-identical re-runs."
            )
        previous = row


def _table_to_parquet_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        compression=PARQUET_COMPRESSION,
        # Deterministic output: same input table -> byte-identical object, which
        # is what makes an idempotent re-run a no-op rather than a new file.
        write_statistics=True,
        store_schema=True,
    )
    return sink.getvalue()


def write_table_atomic(
    table: pa.Table,
    bucket: str,
    key: str,
    *,
    sort_key: Sequence[str] | None = None,
    verify: bool = True,
    client: BaseClient | None = None,
) -> WriteResult:
    """Write ``table`` as ZSTD Parquet to ``key``, atomically (PLAN.md §10).

    Sequence: stage under :data:`TMP_PREFIX` -> verify the *temp* object's footer
    row count -> copy to ``key`` (an S3 copy is all-or-nothing) -> delete the temp
    -> verify the final object. If the temp fails verification it is deleted and
    :class:`RowCountMismatch` is raised, so nothing bad ever lands at ``key``.

    Rows must already be sorted by :data:`energy_capture.model.SORT_KEY` (or
    ``HOURLY_SORT_KEY`` for rollup tables — detected from the columns). An
    unsorted table raises :class:`UnsortedRowsError`; pass ``sort_key=()`` to skip
    the check for a table with no ordering contract (``dim_channel``).
    """
    s3 = _resolve(client)
    keys = tuple(sort_key) if sort_key is not None else _sort_key_for_table(table)
    _assert_sorted(table, keys, key)

    rows = table.num_rows
    if rows == 0:
        # Legal (a genuinely empty hour), but worth noticing: gaps stay gaps and
        # an empty file is not the same thing as no file.
        log.warning("write_empty_table", bucket=bucket, key=key)

    payload = _table_to_parquet_bytes(table)
    staging = temp_key(key)

    _put_object(s3, bucket, staging, payload)
    try:
        staged_rows = parquet_row_count(bucket, staging, client=s3)
        if staged_rows != rows:
            raise RowCountMismatch(
                f"staged object {s3_uri(bucket, staging)} has {staged_rows} rows, "
                f"expected {rows}; refusing to publish it to {key}"
            )
        _copy_object(s3, bucket, staging, bucket, key)
    except BaseException:
        # Never leave a stranded staging object behind on the failure path.
        try:
            _delete_object(s3, bucket, staging)
        except ClientError:  # pragma: no cover - best effort cleanup
            log.warning("temp_cleanup_failed", bucket=bucket, key=staging)
        raise

    _delete_object(s3, bucket, staging)

    verified = True
    if verify:
        verified = verify_row_count(bucket, key, rows, client=s3)
        if not verified:
            raise RowCountMismatch(
                f"{s3_uri(bucket, key)} failed post-write verification "
                f"(expected {rows} rows)"
            )

    log.info(
        "write_ok",
        bucket=bucket,
        key=key,
        rows=rows,
        bytes=len(payload),
        compression=PARQUET_COMPRESSION,
        verified=verified,
    )
    return WriteResult(
        bucket=bucket,
        key=key,
        rows=rows,
        bytes_written=len(payload),
        temp_key=staging,
        verified=verified,
    )


# ------------------------------------------------------------------- moving


@dataclass(frozen=True, slots=True)
class MoveResult:
    """Outcome of :func:`move_keys` — enough detail to make a re-run explainable."""

    #: ``(src, dst)`` pairs this call actually copied.
    moved: list[tuple[str, str]] = field(default_factory=list)
    #: ``(src, dst)`` pairs already at the destination when this call started.
    already_moved: list[tuple[str, str]] = field(default_factory=list)
    #: Sources that exist at neither end — nothing to do, but worth surfacing.
    missing: list[str] = field(default_factory=list)

    @property
    def destinations(self) -> list[str]:
        return [dst for _, dst in self.moved] + [dst for _, dst in self.already_moved]

    @property
    def complete(self) -> bool:
        """True when every requested source now lives at its destination."""
        return not self.missing


def move_keys(
    bucket: str,
    src_keys: Iterable[str],
    dst_prefix: str,
    *,
    client: BaseClient | None = None,
) -> MoveResult:
    """Copy-then-delete each key into ``dst_prefix``, keeping its basename.

    This is how the compactor archives verified parts to
    ``energy/raw_30s_parts_archive/`` (PLAN.md §10). It is safe to re-run after a
    partial failure, because each key is classified before it is touched:

    * source present, destination absent  -> copy, size-check, delete source
    * source present, destination present -> re-copy only if the sizes differ,
      then delete the source (this is the "copy succeeded, delete didn't" case)
    * source absent, destination present  -> already moved, nothing to do
    * absent at both ends                 -> reported in ``missing``, no exception

    The source is deleted only after the destination is confirmed present and the
    same size, so a crash can leave a duplicate but never a lost object.
    """
    s3 = _resolve(client)
    prefix = dst_prefix if dst_prefix.endswith("/") else dst_prefix + "/"
    result = MoveResult()

    for src_key in src_keys:
        dst_key = prefix + src_key.rsplit("/", 1)[-1]
        if dst_key == src_key:
            raise ValueError(f"move_keys destination equals source: {src_key}")

        src_size = object_size(bucket, src_key, client=s3)
        dst_size = object_size(bucket, dst_key, client=s3)

        if src_size is None:
            if dst_size is None:
                log.warning("move_missing", bucket=bucket, key=src_key, dst=dst_key)
                result.missing.append(src_key)
            else:
                log.debug("move_already_done", bucket=bucket, key=src_key, dst=dst_key)
                result.already_moved.append((src_key, dst_key))
            continue

        if dst_size == src_size:
            # A previous run copied it but died before the delete. Finish the job.
            _delete_object(s3, bucket, src_key)
            log.info("move_completed_partial", bucket=bucket, key=src_key, dst=dst_key)
            result.already_moved.append((src_key, dst_key))
            continue

        _copy_object(s3, bucket, src_key, bucket, dst_key)
        copied = object_size(bucket, dst_key, client=s3)
        if copied != src_size:
            raise S3IOError(
                f"copy of {s3_uri(bucket, src_key)} to {dst_key} is {copied} bytes, "
                f"expected {src_size}; source left in place"
            )
        _delete_object(s3, bucket, src_key)
        log.info("move_ok", bucket=bucket, key=src_key, dst=dst_key, bytes=src_size)
        result.moved.append((src_key, dst_key))

    log.info(
        "move_keys_done",
        bucket=bucket,
        dst_prefix=prefix,
        moved=len(result.moved),
        already_moved=len(result.already_moved),
        missing=len(result.missing),
    )
    return result
