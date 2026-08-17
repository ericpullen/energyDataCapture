"""Structured JSON logging to stdout, with a credential scrubber (PLAN.md §11).

One JSON object per line::

    {"ts":"2026-08-16T14:05:00.123Z","level":"INFO","stage":"uploader",
     "event":"upload_ok","hour":"2026-08-16T13","rows":4212,"duration_s":1.9}

Usage::

    from energy_capture.logging import get_logger

    log = get_logger("uploader")
    log.info("upload_ok", hour="2026-08-16T13", rows=4212, duration_s=1.9)
    log.warning("leviton_poll_failed", consecutive_failures=3)

CLAUDE.md cardinal rule 8 — *no credentials or tokens in logs* — is enforced by
:class:`ScrubbingFilter` (installed on the stdout handler) plus a final
:func:`scrub_text` pass over the serialised line. Three layers of defence:

1. **Literal values.** Every secret in :class:`~energy_capture.config.Settings`
   (see ``SECRET_SETTING_FIELDS``) plus anything handed to :func:`register_secret`
   at runtime — call it with the Leviton login token and the Carrier
   access/refresh tokens as soon as they are obtained.
2. **Key names.** Anything whose key normalises into :data:`SECRET_KEY_NAMES`
   (``password``, ``token``, ``access_token``, ``refresh_token``,
   ``authorization``, ``api_key``, ``secret``, …) is redacted at any depth of a
   nested dict/list, as is ``id`` when the surrounding mapping looks like a
   Leviton login response (whose ``id`` *is* the bearer token, PLAN.md §6.1).
3. **Text patterns.** ``Authorization: <token>``, ``Bearer <token>`` and
   ``password=<value>`` style substrings are rewritten inside free text, and
   ``SecretStr`` values never render as anything but ``***REDACTED***``.

Scrubbing must never cost a line: ``%``-args are redacted and expanded *before*
the message is scrubbed as text (see :func:`_scrubbed_message`), because text
scrubbing changes how many ``%`` placeholders a message has and a mismatch makes
stdlib logging drop the record. Every record that would have been emitted is
still emitted, redacted, as exactly one JSON object.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import threading
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, IO

from pydantic import SecretStr

__all__ = [
    "REDACTED",
    "SECRET_KEY_NAMES",
    "JsonFormatter",
    "ScrubbingFilter",
    "StageLogger",
    "configure_logging",
    "get_logger",
    "refresh_config_secrets",
    "register_secret",
    "scrub",
    "scrub_text",
]

#: Replacement token. Distinctive enough to grep for in a log stream.
REDACTED = "***REDACTED***"

#: Root logger for the whole package; stages are children of it.
ROOT_LOGGER_NAME = "energy_capture"

#: Key names (normalised: lowercased, non-alphanumerics stripped) whose values
#: are always redacted, at any nesting depth.
SECRET_KEY_NAMES: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "pwd",
        "pass",
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "authorization",
        "auth",
        "auth_token",
        "api_key",
        "apikey",
        "secret",
        "client_secret",
        "secret_key",
        "session_token",
        "aws_secret_access_key",
        "aws_session_token",
        "private_key",
        "cookie",
        "set_cookie",
        "credentials",
    }
)

#: Shortest literal we are willing to search-and-replace. Guards against a
#: one-character password turning every log line into confetti.
_MIN_LITERAL_SECRET_LEN = 4

_MAX_SCRUB_DEPTH = 12


def _normalise_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


_SECRET_KEYS_NORMALISED: frozenset[str] = frozenset(
    _normalise_key(k) for k in SECRET_KEY_NAMES
)

# A Leviton login response looks like {"id": <token>, "userId": ..., "ttl": ...,
# "created": ...}: the opaque `id` IS the credential (PLAN.md §6.1). Redact `id`
# only in that shape, so ordinary object ids stay legible.
_LEVITON_LOGIN_SIBLINGS = frozenset({"userid", "ttl", "created"})

_BEARER_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/=]{4,}")

_KV_RE = re.compile(
    r"""(?ix)
    \b(password|passwd|pwd|token|access[_-]?token|refresh[_-]?token|id[_-]?token
      |authorization|api[_-]?key|secret|client[_-]?secret|session[_-]?token)\b
    (["']?\s*[:=]\s*["']?)
    # The value stops at a quote or a backslash so that redacting inside an
    # already-serialised JSON string cannot swallow an escape and break the line.
    ([^\s"'\\,;&}\)\]]+)
    """
)

_SECRETSTR_RE = re.compile(r"SecretStr\((['\"]).*?\1\)")

_runtime_secrets: set[str] = set()
_secrets_lock = threading.Lock()
_config_secrets_loaded = False


def register_secret(value: str | SecretStr | None) -> None:
    """Register a live credential so it is scrubbed from all future log output.

    Call this the moment a token is obtained (Leviton login ``id``, Carrier
    ``access_token`` / ``refresh_token``). Values shorter than four characters
    and non-strings are ignored.
    """
    if isinstance(value, SecretStr):
        value = value.get_secret_value()
    if not isinstance(value, str):
        return
    value = value.strip()
    if len(value) < _MIN_LITERAL_SECRET_LEN:
        return
    with _secrets_lock:
        _runtime_secrets.add(value)


def refresh_config_secrets() -> None:
    """Re-read secrets from :class:`Settings` on next use (after a config reload)."""
    global _config_secrets_loaded
    with _secrets_lock:
        _config_secrets_loaded = False


def _load_config_secrets() -> None:
    global _config_secrets_loaded
    with _secrets_lock:
        if _config_secrets_loaded:
            return
        _config_secrets_loaded = True
    try:
        from energy_capture.config import get_settings

        values = get_settings().secret_values()
    except Exception:  # pragma: no cover - config must never break logging
        return
    for value in values:
        register_secret(value)


def _literal_secrets() -> tuple[str, ...]:
    _load_config_secrets()
    with _secrets_lock:
        # Longest first: redacting a superstring before its substring keeps the
        # output from containing a stray tail of a longer credential.
        return tuple(sorted(_runtime_secrets, key=len, reverse=True))


def _kv_replacement(match: re.Match[str]) -> str:
    value = match.group(3)
    # `Authorization: Bearer <token>`: _BEARER_RE already redacted the token, so
    # leave the scheme word alone rather than redacting the word "Bearer".
    if value.lower() in {"bearer", "basic"} or value.startswith(REDACTED[:3]):
        return match.group(0)
    return f"{match.group(1)}{match.group(2)}{REDACTED}"


def scrub_text(text: str) -> str:
    """Redact known literal secrets and credential-shaped substrings in ``text``."""
    if not text:
        return text
    for secret in _literal_secrets():
        if secret in text:
            text = text.replace(secret, REDACTED)
    text = _BEARER_RE.sub(lambda m: f"{m.group(1)} {REDACTED}", text)
    text = _KV_RE.sub(_kv_replacement, text)
    text = _SECRETSTR_RE.sub(f"SecretStr('{REDACTED}')", text)
    return text


def _is_secret_key(key: Any) -> bool:
    return _normalise_key(key) in _SECRET_KEYS_NORMALISED


def _looks_like_leviton_login(mapping: Mapping[Any, Any]) -> bool:
    keys = {_normalise_key(k) for k in mapping.keys()}
    return "id" in keys and bool(keys & _LEVITON_LOGIN_SIBLINGS)


def scrub(value: Any, _depth: int = 0) -> Any:
    """Recursively redact credentials in an arbitrary log payload.

    Handles nested mappings and sequences, :class:`SecretStr`, and free text.
    Structure is preserved so the JSON stays readable — only the values change.
    """
    if _depth > _MAX_SCRUB_DEPTH:
        return REDACTED
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, str):
        return scrub_text(value)
    if isinstance(value, bytes):
        return REDACTED
    if isinstance(value, Mapping):
        redact_id = _looks_like_leviton_login(value)
        out: dict[Any, Any] = {}
        for key, item in value.items():
            if _is_secret_key(key) or (redact_id and _normalise_key(key) == "id"):
                out[key] = REDACTED
            else:
                out[key] = scrub(item, _depth + 1)
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        scrubbed = [scrub(item, _depth + 1) for item in value]
        if isinstance(value, tuple):
            return tuple(scrubbed)
        if isinstance(value, (set, frozenset)):
            return type(value)(scrubbed)
        return scrubbed
    if isinstance(value, (int, float, bool, type(None), datetime, date, Decimal, Path)):
        return value
    # Unknown object: render it now so its repr can be scrubbed as text rather
    # than reaching the formatter unexamined.
    try:
        return scrub_text(repr(value))
    except Exception:  # pragma: no cover - hostile __repr__
        return REDACTED


# LogRecord attributes we must not treat as user payload.
_RESERVED_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


def _scrubbed_message(msg: Any, args: Any) -> tuple[Any, Any]:
    """Return a ``(msg, args)`` pair that is scrubbed *and* still formats.

    Scrubbing free text changes the number of ``%`` placeholders in it. Both
    directions happen in practice:

    * the format string itself is credential-shaped —
      ``log.warning("password=%s", value)`` — and :data:`_KV_RE` eats the ``%s``;
    * a redacted literal secret contained a ``%``, which disappears with it.

    Either way ``record.getMessage()`` then raises ``TypeError`` inside the
    handler, logging's own error handler dumps a traceback to stderr (including
    ``Arguments:`` — the unredacted args) and **the line never reaches stdout**.
    A log line that silently vanishes is how an incident hides, and PLAN.md §11
    wants every line emitted as exactly one JSON object.

    So the order is: redact the *arguments* structurally first (key-name
    redaction in :func:`scrub` only works on the real objects, before ``%s``
    flattens a dict into a string), expand them against the still-untouched
    format string, and only then scrub the result as text. What comes back
    carries no args, so nothing downstream can re-format it and no later pass —
    including :meth:`JsonFormatter.format`'s belt-and-braces one — can break it.
    """
    if not args:
        # Nothing to format. `msg` may be a dict/list/SecretStr/arbitrary object
        # and scrub() keeps its structure; the formatter stringifies it later.
        return scrub(msg), args
    scrubbed_args = scrub(args)
    template = msg if isinstance(msg, str) else str(scrub(msg))
    try:
        expanded = template % scrubbed_args
    except Exception:
        # The caller's own %-formatting bug (wrong arity, wrong conversion).
        # stdlib logging would drop the record here; keep it instead — a broken
        # log call is worth seeing, and the args are already redacted.
        expanded = f"{template} [unformattable log args: {scrubbed_args!r}]"
    return scrub_text(expanded), None


class ScrubbingFilter(logging.Filter):
    """Mutates every record in place so no credential can reach any handler."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 - stdlib API
        record.msg, record.args = _scrubbed_message(record.msg, record.args)
        if record.exc_text:
            record.exc_text = scrub_text(record.exc_text)
        for key, value in list(record.__dict__.items()):
            if key in _RESERVED_RECORD_ATTRS:
                continue
            record.__dict__[key] = REDACTED if _is_secret_key(key) else scrub(value)
        return True


def _json_default(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(str(item) for item in value)
    if isinstance(value, Iterable):
        return list(value)
    return repr(value)


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ``ts``/``level``/``stage``/``event`` + extras."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003 - stdlib API
        message = record.getMessage()
        event = getattr(record, "event", None) or message
        stage = getattr(record, "stage", None)
        if not stage:
            name = record.name
            stage = name.split(".", 1)[1] if name.startswith(ROOT_LOGGER_NAME + ".") else name

        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "stage": stage,
            "event": event,
        }
        if message and message != event:
            payload["message"] = message

        fields = getattr(record, "energy_fields", None)
        if isinstance(fields, Mapping):
            for key, value in fields.items():
                if key not in payload:
                    payload[key] = value
        # Anything attached via `extra=` that we did not put there ourselves.
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_ATTRS or key in {"stage", "event", "energy_fields"}:
                continue
            if key.startswith("_") or key in payload:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exc_info"] = scrub_text(self.formatException(record.exc_info))
        elif record.exc_text:
            payload["exc_info"] = record.exc_text
        if record.stack_info:
            payload["stack_info"] = scrub_text(self.formatStack(record.stack_info))

        line = json.dumps(payload, default=_json_default, ensure_ascii=False)
        # Final belt-and-braces pass: even a payload that reached us around the
        # filter cannot leave this function with a credential in it.
        scrubbed = scrub_text(line)
        if scrubbed == line:
            return line
        try:
            json.loads(scrubbed)
        except ValueError:
            # Text-level redaction damaged the encoding (a secret straddling an
            # escape). Fall back to redacting values structurally, which cannot.
            scrubbed = json.dumps(scrub(payload), default=_json_default, ensure_ascii=False)
        return scrubbed


_configure_lock = threading.Lock()
_configured = False


def configure_logging(
    level: str | int | None = None,
    *,
    stream: IO[str] | None = None,
    force: bool = False,
) -> logging.Logger:
    """Install the JSON handler + scrubbing filter on the package root logger.

    Idempotent: repeated calls only adjust the level unless ``force=True``
    (which replaces the handler — used by tests that capture a stream).
    """
    global _configured
    with _configure_lock:
        root = logging.getLogger(ROOT_LOGGER_NAME)
        if level is None:
            try:
                from energy_capture.config import get_settings

                level = get_settings().log_level
            except Exception:  # pragma: no cover - never let config break logging
                level = "INFO"
        root.setLevel(level)
        root.propagate = False
        if _configured and not force:
            return root
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler.addFilter(ScrubbingFilter())
        root.addHandler(handler)
        _configured = True
        return root


class StageLogger:
    """Thin structured wrapper: ``log.info("event_name", rows=42)``."""

    __slots__ = ("_logger", "_fields", "stage")

    def __init__(
        self,
        stage: str,
        logger: logging.Logger | None = None,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        self.stage = stage
        self._logger = logger or logging.getLogger(f"{ROOT_LOGGER_NAME}.{stage}")
        self._fields: dict[str, Any] = dict(fields or {})

    def bind(self, **fields: Any) -> StageLogger:
        """Return a child logger carrying ``fields`` on every subsequent record."""
        merged = dict(self._fields)
        merged.update(fields)
        return StageLogger(self.stage, self._logger, merged)

    def _log(
        self,
        level: int,
        event: str,
        fields: Mapping[str, Any],
        *,
        exc_info: Any = None,
    ) -> None:
        if not self._logger.isEnabledFor(level):
            return
        merged = dict(self._fields)
        merged.update(fields)
        self._logger.log(
            level,
            event,
            extra={"stage": self.stage, "event": event, "energy_fields": merged},
            exc_info=exc_info,
        )

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, fields)

    def critical(self, event: str, **fields: Any) -> None:
        self._log(logging.CRITICAL, event, fields)

    def exception(self, event: str, **fields: Any) -> None:
        """Log at ERROR with the active exception's (scrubbed) traceback."""
        self._log(logging.ERROR, event, fields, exc_info=True)

    def log(self, level: int, event: str, **fields: Any) -> None:
        self._log(level, event, fields)


def get_logger(stage: str) -> StageLogger:
    """Return the structured logger for ``stage`` (``poller``, ``uploader``, …).

    Configures package logging on first use, so importing and logging is enough —
    no bootstrap call required.
    """
    if not _configured:
        configure_logging()
    return StageLogger(stage)
