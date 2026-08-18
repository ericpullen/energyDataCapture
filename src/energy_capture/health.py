"""``status.json`` writer + the ``/healthz`` HTTP server (PLAN.md §11).

Two things live here:

:class:`StatusStore`
    Owns ``{SPOOL_DIR}/status.json``. Every stage action calls
    :meth:`StatusStore.record_success`, :meth:`StatusStore.record_failure` or
    :meth:`StatusStore.set`, and the whole document is rewritten **atomically**
    (temp file in the same directory, ``fsync``, ``os.replace``). A crash at any
    point leaves either the previous complete document or the new one — never a
    truncated file. Updates are serialised by a lock so the asyncio poll loop and
    the scheduler jobs can both write without interleaving.

:class:`HealthServer`
    A stdlib-asyncio HTTP server (no web framework — PLAN.md keeps the dependency
    list lean) serving ``GET /healthz``. It returns the status document plus a
    ``health`` block, with status **503** when a poller's last success is older
    than :data:`STALE_INTERVAL_MULTIPLIER` × its poll interval (PLAN.md §11).

Document shape — the seven sections of PLAN.md §11::

    {
      "leviton":       {"last_success_utc": ..., "consecutive_failures": 0, "channels_seen": 0},
      "bryant_status": {"last_success_utc": ..., "consecutive_failures": 0},
      "bryant_daily":  {"last_success_utc": ...},
      "uploader":      {"last_success_utc": ..., "last_hour_uploaded": ..., "rows": 0},
      "compactor":     {"last_day_compacted": ..., "rows": 0},
      "rollup":        {"last_day_rolled": ..., "rows": 0},
      "spool":         {"pending_rows": 0, "oldest_pending_utc": ...}
    }

Additions beyond that example, all strict supersets of it:

* top-level ``started_utc`` / ``updated_utc`` — when this process came up and
  when the document was last rewritten (the second one is what tells you the
  writer itself is alive, not merely that a poller is);
* ``consecutive_failures`` / ``last_failure_utc`` / ``last_error`` appear on any
  section that has ever failed, because :meth:`record_failure` is generic and
  PLAN.md §6.4 and §7.3 both require non-poller conditions (Leviton keepalive
  backoff, an API-imposed effective cadence) to be recorded here;
* sections not in the list above may be created on demand (``leviton_keepalive``
  is the intended one) — nothing else about the file changes.

CLAUDE.md cardinal rule 8 applies to this file exactly as it does to logs: every
value written goes through :func:`energy_capture.logging.scrub` first, so a
stage that passes a token or a URL with credentials in it cannot land one in
``status.json``.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import os
import tempfile
import threading
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from energy_capture.logging import get_logger, scrub
from energy_capture.timeutil import ensure_utc, format_utc, now_utc

__all__ = [
    "DEFAULT_HEALTH_PATHS",
    "HEALTH_SECTION",
    "STALE_INTERVAL_MULTIPLIER",
    "STATUS_SECTIONS",
    "HealthServer",
    "StatusStore",
    "default_status_document",
    "get_status_store",
    "reset_status_store",
    "serve_health",
]

#: ``/healthz`` fails once a poller's last success is older than this many poll
#: intervals (PLAN.md §11). Three intervals = two missed polls plus slack.
STALE_INTERVAL_MULTIPLIER: int = 3

#: The sections of PLAN.md §11, in document order.
STATUS_SECTIONS: tuple[str, ...] = (
    "leviton",
    "bryant_status",
    "bryant_daily",
    "uploader",
    "compactor",
    "rollup",
    "spool",
)

#: Top-level key holding the computed liveness verdict in the ``/healthz`` body.
#: It is *not* persisted to ``status.json`` — it is derived on every request.
HEALTH_SECTION = "health"

#: URL paths that serve the status document. Everything else is a 404.
DEFAULT_HEALTH_PATHS: frozenset[str] = frozenset({"/healthz", "/health", "/", "/status.json"})

#: Sections whose staleness ``/healthz`` judges, mapped to the setting holding
#: their poll interval. A source that is not running should be dropped with
#: :meth:`StatusStore.forget_poller` so it cannot fail the check forever.
_POLLER_INTERVAL_SETTINGS: tuple[tuple[str, str], ...] = (
    ("leviton", "poll_interval_s"),
    ("bryant_status", "bryant_poll_interval_s"),
)

#: Truncation limit for the ``last_error`` string: status.json is a dashboard,
#: not a log. Full tracebacks belong in the JSON log stream.
_MAX_ERROR_CHARS = 500

_TEMP_PREFIX = ".status-"
_TEMP_SUFFIX = ".json.tmp"


def default_status_document() -> dict[str, Any]:
    """A fresh document with every PLAN.md §11 section present and empty.

    Every key is present from the first write so that a reader (or an LLM) never
    has to guess whether a missing key means "no data yet" or "not implemented".
    ``None`` means "has not happened"; it is never a zero — a zero row count and
    "never ran" are different facts (CLAUDE.md cardinal rule 1, in spirit).
    """
    return {
        "started_utc": None,
        "updated_utc": None,
        "leviton": {
            "last_success_utc": None,
            "consecutive_failures": 0,
            "channels_seen": 0,
        },
        "bryant_status": {"last_success_utc": None, "consecutive_failures": 0},
        "bryant_daily": {"last_success_utc": None, "consecutive_failures": 0},
        "uploader": {
            "last_success_utc": None,
            "last_hour_uploaded": None,
            "rows": 0,
            "consecutive_failures": 0,
        },
        "compactor": {
            "last_success_utc": None,
            "last_day_compacted": None,
            "rows": 0,
            "consecutive_failures": 0,
        },
        "rollup": {
            "last_success_utc": None,
            "last_day_rolled": None,
            "rows": 0,
            "consecutive_failures": 0,
        },
        "spool": {"pending_rows": 0, "oldest_pending_utc": None},
    }


def _new_section() -> dict[str, Any]:
    """Default body for a section created on demand (e.g. ``leviton_keepalive``)."""
    return {"last_success_utc": None, "consecutive_failures": 0}


def _jsonable(value: Any) -> Any:
    """Coerce a scrubbed value into something :func:`json.dump` accepts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(v) for v in value)
    return str(value)


def _clean_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Scrub credentials out of ``fields`` and make the result JSON-safe.

    ``scrub`` applied to the whole mapping redacts both by key name (``token``,
    ``password``, …) and by literal value (anything registered via
    ``logging.register_secret``), so a stage cannot leak a credential into
    ``status.json`` by accident.
    """
    scrubbed = scrub(dict(fields))
    return {str(key): _jsonable(value) for key, value in scrubbed.items()}


def _parse_utc(value: Any) -> datetime | None:
    """Parse a ``status.json`` timestamp back into an aware UTC datetime."""
    if isinstance(value, datetime):
        return ensure_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return ensure_utc(parsed)


def _default_poll_intervals() -> dict[str, int]:
    """Poller staleness budgets from configuration (falls back to the 30s floor)."""
    try:
        from energy_capture.config import MIN_POLL_INTERVAL_S, get_settings

        settings = get_settings()
        return {
            section: max(int(getattr(settings, attr)), MIN_POLL_INTERVAL_S)
            for section, attr in _POLLER_INTERVAL_SETTINGS
        }
    except Exception:  # pragma: no cover - configuration must never break health
        return {section: 30 for section, _ in _POLLER_INTERVAL_SETTINGS}


def _default_status_path() -> Path:
    try:
        from energy_capture.config import get_settings

        return get_settings().status_path
    except Exception:  # pragma: no cover - configuration must never break health
        return Path("status.json")


class StatusStore:
    """Owner of ``status.json``: read-modify-write the whole doc, atomically.

    Thread- and asyncio-safe. Every mutator takes a lock, updates the in-memory
    document and rewrites the file before returning, so a caller that returns
    from ``record_success`` knows the fact is on disk. The critical section is a
    JSON dump of a few hundred bytes — calling it from the poll loop is fine (the
    pipeline is not performance-sensitive; CLAUDE.md).

    Args:
        path: destination file. Defaults to ``Settings.status_path``.
        poll_intervals: sections whose staleness ``/healthz`` judges, mapped to
            their poll interval in seconds. Defaults to the configured Leviton
            and Bryant-status intervals.
        clock: injectable ``now`` for tests.
        load_existing: merge a status.json left by a previous run (default), so
            counters and last-success times survive a container restart.
    """

    __slots__ = ("_clock", "_doc", "_lock", "_log", "_path", "_poll_intervals", "_started")

    def __init__(
        self,
        path: Path | str | None = None,
        *,
        poll_intervals: Mapping[str, int] | None = None,
        clock: Callable[[], datetime] = now_utc,
        load_existing: bool = True,
    ) -> None:
        self._path = Path(path) if path is not None else _default_status_path()
        self._clock = clock
        self._lock = threading.RLock()
        self._log = get_logger("health")
        self._poll_intervals = dict(
            poll_intervals if poll_intervals is not None else _default_poll_intervals()
        )
        self._doc = default_status_document()
        if load_existing:
            self._merge_existing()
        self._started = ensure_utc(clock())
        self._doc["started_utc"] = format_utc(self._started)

    # ------------------------------------------------------------------ paths
    @property
    def path(self) -> Path:
        """Location of ``status.json``."""
        return self._path

    @property
    def started_utc(self) -> datetime:
        """When this store (i.e. this process) came up — the staleness baseline."""
        return self._started

    # --------------------------------------------------------------- mutators
    def record_success(self, section: str, **fields: Any) -> dict[str, Any]:
        """Stamp ``section`` as succeeding now; zero its failure counter.

        Extra keyword fields are merged into the section (``rows=4212``,
        ``last_hour_uploaded="2026-08-16T13"``, …) after scrubbing.
        """
        with self._lock:
            body = self._section(section)
            body["last_success_utc"] = format_utc(self._clock())
            body["consecutive_failures"] = 0
            body.pop("last_error", None)
            body.pop("last_failure_utc", None)
            body.update(_clean_fields(fields))
            return self._flush(section)

    def record_failure(
        self, section: str, error: BaseException | str | None = None, **fields: Any
    ) -> dict[str, Any]:
        """Increment ``section``'s consecutive-failure count and record the error.

        ``last_success_utc`` is deliberately left alone: it is the timestamp
        ``/healthz`` measures staleness against, and a failure must not look like
        a success that merely aged.
        """
        with self._lock:
            body = self._section(section)
            previous = body.get("consecutive_failures")
            body["consecutive_failures"] = (
                previous + 1 if isinstance(previous, int) and not isinstance(previous, bool) else 1
            )
            body["last_failure_utc"] = format_utc(self._clock())
            body["last_error"] = _format_error(error)
            body.update(_clean_fields(fields))
            return self._flush(section)

    def set(self, section: str, **fields: Any) -> dict[str, Any]:  # noqa: A003 - PLAN.md API
        """Merge ``fields`` into ``section`` without touching success/failure state.

        Used for gauges: ``store.set("spool", pending_rows=1240,
        oldest_pending_utc=ts)``.
        """
        with self._lock:
            body = self._section(section)
            body.update(_clean_fields(fields))
            return self._flush(section)

    def reset_failures(self, section: str) -> dict[str, Any]:
        """Zero a failure counter without claiming a success (e.g. after a manual fix)."""
        with self._lock:
            body = self._section(section)
            body["consecutive_failures"] = 0
            body.pop("last_error", None)
            body.pop("last_failure_utc", None)
            return self._flush(section)

    # ----------------------------------------------------------- poller wiring
    def watch_poller(self, section: str, interval_s: int) -> None:
        """Judge ``section``'s staleness against ``interval_s`` on ``/healthz``.

        Also the hook for PLAN.md §7.3: if Carrier throttles us to a slower
        cadence, call this with the *effective* interval so health reflects
        reality instead of flapping.
        """
        if interval_s <= 0:
            raise ValueError(f"interval_s must be positive, got {interval_s!r}")
        with self._lock:
            self._poll_intervals[section] = int(interval_s)

    def forget_poller(self, section: str) -> None:
        """Stop judging ``section`` (a source that is not configured/enabled)."""
        with self._lock:
            self._poll_intervals.pop(section, None)

    @property
    def poll_intervals(self) -> dict[str, int]:
        with self._lock:
            return dict(self._poll_intervals)

    # ----------------------------------------------------------------- readers
    def snapshot(self) -> dict[str, Any]:
        """A deep copy of the persisted document (no ``health`` block)."""
        with self._lock:
            return copy.deepcopy(self._doc)

    def section(self, name: str) -> dict[str, Any]:
        """A deep copy of one section (empty dict if it does not exist yet)."""
        with self._lock:
            return copy.deepcopy(self._doc.get(name, {}))

    def health_report(self) -> tuple[int, dict[str, Any]]:
        """``(http_status, body)`` for ``/healthz``.

        503 when any watched poller's last success is **older than**
        :data:`STALE_INTERVAL_MULTIPLIER` × its interval. A poller that has never
        succeeded is measured from process start, so a fresh container is healthy
        for its first three intervals instead of failing its own readiness check.
        """
        with self._lock:
            doc = copy.deepcopy(self._doc)
            intervals = dict(self._poll_intervals)
            started = self._started
        now = ensure_utc(self._clock())

        checks: list[dict[str, Any]] = []
        ok = True
        for name in sorted(intervals):
            interval = intervals[name]
            max_age = interval * STALE_INTERVAL_MULTIPLIER
            raw = doc.get(name, {}).get("last_success_utc") if isinstance(doc.get(name), dict) else None
            last_success = _parse_utc(raw)
            reference = last_success if last_success is not None else started
            age = (now - reference).total_seconds()
            stale = age > max_age
            ok = ok and not stale
            checks.append(
                {
                    "section": name,
                    "poll_interval_s": interval,
                    "max_age_s": max_age,
                    "last_success_utc": raw,
                    "never_succeeded": last_success is None,
                    "age_s": round(age, 3),
                    "stale": stale,
                }
            )

        doc[HEALTH_SECTION] = {
            "ok": ok,
            "now_utc": format_utc(now),
            "started_utc": format_utc(started),
            "stale_after_intervals": STALE_INTERVAL_MULTIPLIER,
            "checks": checks,
        }
        return (200 if ok else 503, doc)

    # ---------------------------------------------------------------- internals
    def _section(self, name: str) -> dict[str, Any]:
        """Return the mutable section body, creating it if this is a new section."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("status section name must be a non-empty string")
        body = self._doc.get(name)
        if not isinstance(body, dict):
            body = _new_section()
            self._doc[name] = body
        return body

    def _flush(self, section: str) -> dict[str, Any]:
        """Rewrite the whole document atomically; return a copy of ``section``."""
        self._doc["updated_utc"] = format_utc(self._clock())
        try:
            write_json_atomic(self._path, self._doc)
        except OSError as exc:
            # A failed status write must never take a stage down with it — the
            # data pipeline is the point, this file is telemetry about it.
            self._log.error(
                "status_write_failed", path=str(self._path), error=f"{type(exc).__name__}: {exc}"
            )
        return copy.deepcopy(self._doc[section])

    def _merge_existing(self) -> None:
        """Load a status.json from a previous run over the defaults, if readable."""
        try:
            raw = self._path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return
        try:
            existing = json.loads(raw)
        except ValueError:
            self._log.warning("status_file_unreadable", path=str(self._path))
            return
        if not isinstance(existing, dict):
            return
        for key, value in existing.items():
            if key == HEALTH_SECTION:
                continue  # derived per-request, never persisted
            if isinstance(value, dict) and isinstance(self._doc.get(key), dict):
                self._doc[key].update(value)
            else:
                self._doc[key] = value


def _format_error(error: BaseException | str | None) -> str | None:
    """Render an error for ``status.json``: scrubbed, one line, bounded length."""
    if error is None:
        return None
    if isinstance(error, BaseException):
        text = f"{type(error).__name__}: {error}"
    else:
        text = str(error)
    text = " ".join(text.split())
    if len(text) > _MAX_ERROR_CHARS:
        text = text[: _MAX_ERROR_CHARS - 1] + "…"
    cleaned = scrub(text)
    return cleaned if isinstance(cleaned, str) else str(cleaned)


def write_json_atomic(path: Path, document: Mapping[str, Any]) -> None:
    """Serialise ``document`` to ``path`` atomically.

    Serialise first (a bad payload therefore never creates a file at all), then
    write a temp file **in the same directory** — ``os.replace`` is only atomic
    within a filesystem — ``fsync`` it, rename over the target, and ``fsync`` the
    directory so the rename itself survives a power cut. A crash at any point
    leaves the previous complete document in place; the temp file is removed on
    any failure so no partial file is left behind.
    """
    text = json.dumps(document, indent=2, ensure_ascii=False, default=_jsonable) + "\n"
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(prefix=_TEMP_PREFIX, suffix=_TEMP_SUFFIX, dir=str(directory))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise
    _fsync_dir(directory)


def _fsync_dir(directory: Path) -> None:
    """Make the rename durable. Best-effort: not every filesystem allows it."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:  # pragma: no cover - platform dependent
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - platform dependent
        pass
    finally:
        os.close(fd)


# --------------------------------------------------------------------- server


_HTTP_REASONS = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed", 500: "Internal Server Error", 503: "Service Unavailable"}

#: Guard rails for a socket that may be anything at all. The health endpoint is
#: unauthenticated on the container network; keep it impossible to wedge.
_REQUEST_LINE_LIMIT = 8192
_MAX_HEADER_LINES = 100
_READ_TIMEOUT_S = 10.0


class HealthServer:
    """``GET /healthz`` over stdlib asyncio — no web framework (PLAN.md §5).

    It also serves the two read-only dashboard routes owned by
    :mod:`energy_capture.dashboard` — ``GET /ui`` (the HTML page) and
    ``GET /ui/data`` (the JSON snapshot it polls) — on the same port, so the
    container still exposes exactly one. They are dispatched *before* the status
    paths and share nothing with them: :data:`DEFAULT_HEALTH_PATHS` behave
    exactly as they did.

    Usage::

        server = HealthServer(store)
        await server.start()
        try:
            ...
        finally:
            await server.aclose()

    or ``async with HealthServer(store) as server: await server.serve_forever()``.
    Bind to port 0 to get an ephemeral port and read :attr:`port` back (tests).
    """

    __slots__ = ("_host", "_log", "_port", "_server", "_store")

    def __init__(
        self,
        store: StatusStore | None = None,
        *,
        host: str = "0.0.0.0",
        port: int | None = None,
    ) -> None:
        self._store = store if store is not None else get_status_store()
        self._host = host
        self._port = port if port is not None else _default_health_port()
        self._server: asyncio.Server | None = None
        self._log = get_logger("health")

    @property
    def port(self) -> int:
        """The bound port (resolved after :meth:`start` when 0 was requested)."""
        return self._port

    @property
    def store(self) -> StatusStore:
        return self._store

    async def start(self) -> int:
        """Bind and begin accepting; returns the actual port."""
        if self._server is not None:
            return self._port
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        sockets = self._server.sockets or ()
        if sockets:
            self._port = sockets[0].getsockname()[1]
        self._log.info("health_server_started", host=self._host, port=self._port)
        return self._port

    async def serve_forever(self) -> None:
        """Start (if needed) and serve until cancelled."""
        await self.start()
        assert self._server is not None
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            raise
        finally:
            await self.aclose()

    async def aclose(self) -> None:
        """Stop accepting and wait for the listener to close."""
        server, self._server = self._server, None
        if server is None:
            return
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()
        self._log.info("health_server_stopped", port=self._port)

    async def __aenter__(self) -> HealthServer:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ---------------------------------------------------------------- internals
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            method, target = await self._read_request(reader)
            ui = await self._respond_ui(method, target)
            if ui is not None:
                status, payload, content_type = ui
                await self._send_bytes(
                    writer, status, payload, content_type, head_only=method == "HEAD"
                )
                return
            status, body = self._respond(method, target)
            await self._send(writer, status, body, head_only=method == "HEAD")
        except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:  # pragma: no cover - defensive; never kill the loop
            self._log.warning("health_request_failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _read_request(self, reader: asyncio.StreamReader) -> tuple[str, str]:
        """Parse the request line and drain headers. Returns ``(method, target)``."""
        raw = await asyncio.wait_for(reader.readline(), _READ_TIMEOUT_S)
        if not raw:
            raise ConnectionError("empty request")
        if len(raw) > _REQUEST_LINE_LIMIT:
            return ("", "")
        parts = raw.decode("latin-1").split()
        method = parts[0].upper() if parts else ""
        target = parts[1] if len(parts) > 1 else "/"
        for _ in range(_MAX_HEADER_LINES):
            line = await asyncio.wait_for(reader.readline(), _READ_TIMEOUT_S)
            if line in (b"\r\n", b"\n", b""):
                break
        return (method, target)

    async def _respond_ui(self, method: str, target: str) -> tuple[int, bytes, str] | None:
        """The two dashboard routes, or ``None`` when this is not one of them.

        ``GET /ui`` serves ``static/dashboard.html``; ``GET /ui/data`` serves the
        JSON snapshot the page polls. Everything else — including every path in
        :data:`DEFAULT_HEALTH_PATHS` — falls through to :meth:`_respond`
        untouched. Imported lazily so the health server keeps starting even if
        the dashboard module cannot be imported, and every failure inside it
        becomes a JSON 500 rather than a dead socket: ``/healthz`` must not be
        able to break because a browser tab asked for a chart.

        The **whole request target** goes to
        :func:`~energy_capture.dashboard.handle_ui_data`, not just the store:
        ``/ui/data`` takes the chart window's ``?window_s=`` / ``?end=``, and
        that function is the only thing that can answer **400** for a malformed
        one. Dropping the target here would silently answer every request with
        the default live window — a chart showing something other than what was
        asked for, which is the one thing this page must never do. A target with
        no query string still yields ``(200, the document this route has always
        returned)``.

        ``build_snapshot`` runs in a **worker thread**. It is synchronous SQLite
        plus a scan of the spool, and this event loop is also running the poll
        loops, the keepalive and the WebSocket reader (PLAN.md §5: one process).
        A browser refreshing every 5s must not be able to stall data collection —
        and the spool grows without bound whenever the uploader is failing, which
        is exactly when someone is watching this page. The snapshot builder opens
        its own connection with ``check_same_thread=False`` and closes it before
        returning, so it is safe off-loop.
        """
        if method not in {"GET", "HEAD"}:
            return None
        path = target.split("?", 1)[0].split("#", 1)[0] or "/"
        if len(path) > 1 and path.endswith("/"):
            path = path.rstrip("/") or "/"

        # Checked before the dashboard, and with its own import, so that a
        # dashboard that fails to import cannot take down the one route with a
        # hard deadline attached: an authorization code expires in minutes and
        # the customer is standing in front of the browser.
        greenbutton = await self._respond_greenbutton(path, target)
        if greenbutton is not None:
            return greenbutton

        try:
            from energy_capture import dashboard
        except Exception as exc:  # pragma: no cover - import-time failure only
            self._log.warning("dashboard_unavailable", error=f"{type(exc).__name__}: {exc}")
            return None

        if path == dashboard.UI_PAGE_PATH:
            try:
                return (200, dashboard.render_page().encode("utf-8"), dashboard.PAGE_CONTENT_TYPE)
            except Exception as exc:
                return self._ui_error("dashboard page could not be read", exc)
        if path == dashboard.UI_DATA_PATH:
            try:
                status, snapshot = await asyncio.to_thread(
                    dashboard.handle_ui_data, self._store, target
                )
            except Exception as exc:
                return self._ui_error("dashboard snapshot failed", exc)
            payload = (
                json.dumps(snapshot, ensure_ascii=False, default=_jsonable) + "\n"
            ).encode("utf-8")
            return (status, payload, "application/json")
        return None

    async def _respond_greenbutton(
        self, path: str, target: str
    ) -> tuple[int, bytes, str] | None:
        """``GET /greenbutton/callback`` — finish a Green Button authorization.

        The published redirect page is static (GitHub Pages, no server), so its
        hand-off button points at this port instead. The authorization code
        therefore travels utility → the operator's browser → localhost, and never
        reaches a host we run.

        The exchange is a blocking HTTPS round trip to the utility, so it goes to
        a worker thread: this loop is also running the poll loops, the keepalive
        and the WebSocket reader, and collection must not pause because someone
        clicked a button.
        """
        if path != "/greenbutton/callback":
            return None
        try:
            from energy_capture.stages import greenbutton_auth
        except Exception as exc:  # pragma: no cover - import-time failure only
            self._log.warning(
                "greenbutton_callback_unavailable", error=f"{type(exc).__name__}: {exc}"
            )
            return None
        try:
            return await asyncio.to_thread(greenbutton_auth.handle_callback, target)
        except Exception as exc:  # pragma: no cover - defensive
            return self._ui_error("green button callback failed", exc)

    def _ui_error(self, message: str, exc: BaseException) -> tuple[int, bytes, str]:
        """A dashboard failure, logged and returned as JSON the page can display."""
        detail = _format_error(exc)
        self._log.warning("dashboard_request_failed", detail=message, error=detail)
        body = json.dumps({"error": message, "detail": detail}, ensure_ascii=False) + "\n"
        return (500, body.encode("utf-8"), "application/json")

    def _respond(self, method: str, target: str) -> tuple[int, dict[str, Any]]:
        if method not in {"GET", "HEAD"}:
            if not method:
                return (400, {"error": "malformed request"})
            return (405, {"error": "method not allowed", "allow": "GET, HEAD"})
        path = target.split("?", 1)[0].split("#", 1)[0] or "/"
        if len(path) > 1 and path.endswith("/"):
            path = path.rstrip("/") or "/"
        if path not in DEFAULT_HEALTH_PATHS:
            return (404, {"error": "not found", "paths": sorted(DEFAULT_HEALTH_PATHS)})
        return self._store.health_report()

    async def _send(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: Mapping[str, Any],
        *,
        head_only: bool = False,
    ) -> None:
        payload = (json.dumps(body, indent=2, ensure_ascii=False, default=_jsonable) + "\n").encode(
            "utf-8"
        )
        await self._send_bytes(
            writer, status, payload, "application/json", head_only=head_only
        )

    async def _send_bytes(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        payload: bytes,
        content_type: str,
        *,
        head_only: bool = False,
    ) -> None:
        """Write one response. The only place a response head is constructed."""
        reason = _HTTP_REASONS.get(status, "OK" if status < 400 else "Error")
        head = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(payload)}\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        writer.write(head if head_only else head + payload)
        await writer.drain()


def _default_health_port() -> int:
    try:
        from energy_capture.config import get_settings

        return int(get_settings().health_port)
    except Exception:  # pragma: no cover - configuration must never break health
        return 8080


@contextlib.asynccontextmanager
async def serve_health(store: StatusStore | None = None, *, host: str = "0.0.0.0", port: int | None = None):
    """Async context manager running the health server for its duration."""
    server = HealthServer(store, host=host, port=port)
    await server.start()
    try:
        yield server
    finally:
        await server.aclose()


# ------------------------------------------------------------ process default

_default_store: StatusStore | None = None
_default_store_lock = threading.Lock()


def get_status_store() -> StatusStore:
    """The process-wide :class:`StatusStore` (created on first use).

    Stages call this rather than threading a store through every constructor;
    tests use :func:`reset_status_store` to point it somewhere temporary.
    """
    global _default_store
    with _default_store_lock:
        if _default_store is None:
            _default_store = StatusStore()
        return _default_store


def reset_status_store(store: StatusStore | None = None) -> StatusStore | None:
    """Replace (or clear, with ``None``) the process-wide store. Tests only."""
    global _default_store
    with _default_store_lock:
        _default_store = store
        return _default_store
