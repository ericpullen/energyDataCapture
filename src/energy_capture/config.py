"""Configuration — every knob is an environment variable (PLAN.md §14).

Rules encoded here:

* Secrets are :class:`pydantic.SecretStr` so that ``str(settings)``,
  ``repr(settings)`` and ``settings.model_dump()`` can never leak them into a log
  line (CLAUDE.md cardinal rule 8). Call ``.get_secret_value()`` at the single
  point of use.
* Poll intervals have a **hard 30s floor in code**, applied after validation,
  regardless of what the environment says (PLAN.md §6.6).
* ``LEVITON_INGEST`` selects how Leviton readings are kept *fresh* — it never
  changes how they are *sampled*. Every mode emits one set of rows per
  ``POLL_INTERVAL_S`` cycle with a single ``ts_utc`` (PLAN.md §6.5), because
  §2.5's kWh formula and ``sample_count``'s meaning as the gap detector both
  assume a fixed cadence. ``rest`` reproduces the original REST-only behaviour
  and stays selectable without a code change.
* Nothing in this module raises just because credentials are absent — pure-logic
  tests must be able to call :func:`get_settings` with an empty environment. Use
  :meth:`Settings.require` at the point where a value is actually needed so the
  failure names the missing variable.
"""

from __future__ import annotations

import logging as _stdlib_logging
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "DEFAULT_LEVITON_WS_URL",
    "DEFAULT_TZ_LOCAL",
    "LEVITON_INGEST_MODES",
    "LEVITON_WS_SERVER_KILL_S",
    "MIN_POLL_INTERVAL_S",
    "SECRET_SETTING_FIELDS",
    "Settings",
    "describe_env_source",
    "get_settings",
    "reset_settings_cache",
]

#: Hard floor for every poll loop, in seconds. Applied even when the env var is
#: lower — Leviton and Carrier are both third-party clouds we must not hammer.
MIN_POLL_INTERVAL_S: int = 30

#: Valid ``LEVITON_INGEST`` values.
#:
#: * ``hybrid`` — WebSocket keeps an in-memory current-state store fresh; REST
#:   still does discovery (§6.2) and a periodic full re-read to reconcile
#:   against that store. The default: it is the only mode that both gets live
#:   values and keeps a REST cross-check.
#: * ``ws`` — WebSocket for values, REST for discovery only. No periodic
#:   reconcile.
#: * ``rest`` — the original PLAN.md §2.8 behaviour: every value read over REST
#:   each cycle, no socket. Kept fully working so the owner can fall back
#:   without a code change if the socket misbehaves in production.
LEVITON_INGEST_MODES: tuple[str, ...] = ("hybrid", "ws", "rest")

#: aioleviton's endpoint (``aioleviton.const.WEBSOCKET_URL``), restated here so a
#: change of endpoint is configuration rather than a code change.
DEFAULT_LEVITON_WS_URL: str = "wss://socket.cloud.leviton.com/"

#: The Leviton server hard-kills a WebSocket at exactly 60 minutes (PLAN.md
#: §6.4). ``LEVITON_WS_RECONNECT_S`` must land inside this window so *we* choose
#: the reconnect moment instead of discovering it as a mid-cycle drop.
LEVITON_WS_SERVER_KILL_S: int = 3600

#: The house is in Louisville; Kentucky is split across two zones, so be explicit.
DEFAULT_TZ_LOCAL: str = "America/Kentucky/Louisville"

#: Field names whose values are secrets. :mod:`energy_capture.logging` reads this
#: to build its literal-scrub list, so keep it in sync when adding a credential.
SECRET_SETTING_FIELDS: tuple[str, ...] = (
    "leviton_password",
    "carrier_password",
    "lge_client_secret",
    "lge_registration_access_token",
)

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


def _discover_env_file() -> Path | str:
    """The nearest ``.env`` at or above the working directory.

    ``env_file=".env"`` is resolved by pydantic-settings against the **process
    working directory**, which made the whole configuration silently depend on
    where you happened to be standing. Running a command from ``data/`` — which
    is exactly what an operator does, since that is where the exports live —
    produced a ``Settings`` with every credential blank and an error message that
    said "not configured" without saying why. Measured 2026-08-18.

    Walking up matches how ``git`` and ``direnv`` behave, so "anywhere inside the
    repository" now works and nothing outside it changes. The container is
    unaffected either way: it gets real environment variables from
    ``--env-file`` and has no ``.env`` on disk at all (deliberately — the image
    must never contain one).
    """
    try:
        here = Path.cwd().resolve()
    except OSError:  # pragma: no cover - deleted cwd
        return ".env"
    for directory in (here, *here.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return ".env"


class Settings(BaseSettings):
    """Runtime configuration, populated from the environment and ``.env``."""

    model_config = SettingsConfigDict(
        env_file=_discover_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        # `FOO=` in .env means "not set", not "set to empty string".
        env_ignore_empty=True,
    )

    # ------------------------------------------------------------- AWS / storage
    s3_bucket: str = ""
    aws_region: str = "us-east-1"
    aws_profile: str | None = None
    glue_database: str = "energy"

    # ----------------------------------------------------------------- Leviton
    leviton_username: str = ""
    leviton_password: SecretStr = SecretStr("")

    # ------------------------------------------------------- Leviton ingestion
    # How readings are kept fresh, never how they are sampled: whatever the mode,
    # one set of rows per POLL_INTERVAL_S cycle with one ts_utc (PLAN.md §6.5).
    #: One of :data:`LEVITON_INGEST_MODES`; ``rest`` is the documented fallback.
    leviton_ingest: str = "hybrid"
    #: WebSocket endpoint. Config, not code, so a moved endpoint is a .env edit.
    leviton_ws_url: str = DEFAULT_LEVITON_WS_URL
    #: Proactive reconnect, comfortably inside the server's 60-minute hard kill
    #: (:data:`LEVITON_WS_SERVER_KILL_S`). Validated to stay under it.
    leviton_ws_reconnect_s: int = Field(default=3300, ge=60)
    #: Quiet window after which an *open* socket is treated as dead. Three poll
    #: intervals at the default cadence; validated to exceed POLL_INTERVAL_S,
    #: since a shorter value would call a stall on every ordinary quiet moment.
    leviton_ws_stall_timeout_s: int = Field(default=90, ge=5)
    #: Full REST re-read while the socket is up, reconciled against the WS store.
    #: 600s is the reference integration's own fallback-polling cadence.
    leviton_rest_reconcile_s: int = Field(default=600, ge=60)

    # --------------------------------------------------------- Bryant / Carrier
    carrier_username: str = ""
    carrier_password: SecretStr = SecretStr("")
    #: System serial number; doubles as the Bryant ``device_id`` (PLAN.md §7.4).
    carrier_serial: str = "4022W200213"

    # ------------------------------------------- LG&E Green Button Connect
    # Issued by LG&E on approval of the third-party registration (2026-08-18) —
    # docs/lge-greenbutton.md. Endpoints are config rather than constants because
    # a custodian that moves one should be a .env edit, not a release; they carry
    # the real values as defaults since they are published to us, not secret.
    #
    # Two credentials here, and they are NOT interchangeable:
    #  * ``lge_client_secret`` authenticates the app when exchanging codes and
    #    refreshing tokens (``client_secret_basic``).
    #  * ``lge_registration_access_token`` authenticates changes to *the
    #    registration itself* at ``lge_registration_client_uri`` — it can read
    #    back and rotate the client credentials, so treat it as the more
    #    dangerous of the two and never use it for data calls.
    lge_client_id: str = ""
    lge_client_secret: SecretStr = SecretStr("")
    lge_authorize_url: str = "https://mymeter.lge-ku.com/OAuthServer/authorize"
    lge_token_url: str = "https://mymeter.lge-ku.com/OAuthServer/token"
    #: ESPI resource base. UsagePoints, MeterReadings and IntervalBlocks hang
    #: off this; the trailing slash is stripped so joins are predictable.
    lge_resource_uri: str = "https://services.mymeter.co/resourceapi/238/GBC/espi/1_1/resource"
    #: Bulk endpoint for the whole subscription. The ``*`` is LG&E's own
    #: wildcard for "every authorised UsagePoint".
    lge_bulk_uri: str = (
        "https://services.mymeter.co/resourceapi/238/GBC/espi/1_1/resource/Batch/Bulk/*"
    )
    #: Where the customer is sent back to. Registered with LG&E as an exact
    #: string; a mismatch here is a rejected authorization (docs §2).
    lge_redirect_uri: str = "https://energycap.ericpullen.com/greenbutton/callback/"
    #: The ESPI scope, read back verbatim from LG&E's own stored
    #: ``ApplicationInformation`` (docs §3c) — so this is what they registered,
    #: not what we think we asked for. No commas or spaces: some OAuth libraries
    #: split a scope on both.
    lge_scope: str = (
        "FB=1_3_4_5;IntervalDuration=900_3600;BlockDuration=Daily;"
        "HistoryLength=63072000;SubscriptionFrequency=Daily"
    )
    #: Dynamic-registration management endpoint for this client.
    lge_registration_client_uri: str = ""
    lge_registration_access_token: SecretStr = SecretStr("")

    # ------------------------------------------------------------ Backfill only
    dynamodb_table: str = "bryant-energy-data"
    #: The old collector's JSON exports — source B of PLAN.md §8. A directory
    #: (scanned for ``energy_*.json``) or a single file. Unset means the default
    #: location on the Mac, ``~/code/bryantDataCollector/energy_data``; the
    #: backfill stage owns that default because the path is meaningless in the
    #: container, where this import never runs.
    bryant_legacy_json_path: Path | None = None

    # ----------------------------------------------------------------- Runtime
    tz_local: str = DEFAULT_TZ_LOCAL
    poll_interval_s: int = 30
    bryant_poll_interval_s: int = 30
    leviton_discovery_interval_s: int = Field(default=3600, ge=60)
    spool_dir: Path = Path("/data")
    spool_retention_days: int = Field(default=7, ge=1)
    health_port: int = Field(default=8080, ge=1, le=65535)
    log_level: str = "INFO"

    # ---------------------------------------------------------- Semantic layer
    blackstart_inventory_path: Path | None = None

    # ------------------------------------------------------------- validation
    @field_validator("poll_interval_s", "bryant_poll_interval_s", mode="after")
    @classmethod
    def _enforce_poll_floor(cls, value: int) -> int:
        """Clamp poll intervals up to :data:`MIN_POLL_INTERVAL_S` (PLAN.md §6.6)."""
        return max(int(value), MIN_POLL_INTERVAL_S)

    @field_validator("tz_local", mode="after")
    @classmethod
    def _validate_tz(cls, value: str) -> str:
        """Fail fast on an unresolvable zone: every partition date depends on it."""
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:  # pragma: no cover - env dependent
            raise ValueError(
                f"TZ_LOCAL={value!r} does not resolve. Is the `tzdata` package installed?"
            ) from exc
        return value

    @field_validator("leviton_ingest", mode="after")
    @classmethod
    def _validate_leviton_ingest(cls, value: str) -> str:
        """Reject an unknown ingestion mode instead of silently picking one."""
        mode = str(value).strip().lower()
        if mode not in LEVITON_INGEST_MODES:
            raise ValueError(
                f"LEVITON_INGEST={value!r} is not one of {list(LEVITON_INGEST_MODES)}"
            )
        return mode

    @field_validator("leviton_ws_url", mode="after")
    @classmethod
    def _validate_ws_url(cls, value: str) -> str:
        """A WebSocket URL, not an https:// one pasted in by mistake."""
        url = str(value).strip()
        if not url.startswith(("ws://", "wss://")):
            raise ValueError(
                f"LEVITON_WS_URL={value!r} must start with ws:// or wss:// "
                f"(default {DEFAULT_LEVITON_WS_URL})."
            )
        return url

    @field_validator("leviton_ws_reconnect_s", mode="after")
    @classmethod
    def _validate_ws_reconnect(cls, value: int) -> int:
        """Keep the proactive reconnect inside the server's 60-minute kill."""
        seconds = int(value)
        if seconds >= LEVITON_WS_SERVER_KILL_S:
            raise ValueError(
                f"LEVITON_WS_RECONNECT_S={value} must be less than "
                f"{LEVITON_WS_SERVER_KILL_S} — Leviton hard-kills the socket at "
                "exactly 60 minutes (PLAN.md §6.4), so at or beyond that we would "
                "never reconnect on our own terms."
            )
        return seconds

    @field_validator("log_level", mode="after")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        level = str(value).strip().upper()
        if level not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"LOG_LEVEL={value!r} is not one of {sorted(_VALID_LOG_LEVELS)}"
            )
        return level

    @model_validator(mode="after")
    def _validate_ws_timings_against_poll(self) -> Settings:
        """Cross-check the WebSocket windows against the sampling cadence.

        Runs after the field validators, so ``poll_interval_s`` is already
        clamped to :data:`MIN_POLL_INTERVAL_S` and these comparisons see the
        interval the loop will actually use.
        """
        if self.leviton_ws_stall_timeout_s <= self.poll_interval_s:
            raise ValueError(
                f"LEVITON_WS_STALL_TIMEOUT_S={self.leviton_ws_stall_timeout_s} "
                f"must be greater than the effective POLL_INTERVAL_S="
                f"{self.poll_interval_s}. A steady load genuinely sends no "
                "updates, so a shorter window would declare the socket dead "
                "during every quiet moment and gap perfectly good data."
            )
        if self.leviton_rest_reconcile_s < self.poll_interval_s:
            raise ValueError(
                f"LEVITON_REST_RECONCILE_S={self.leviton_rest_reconcile_s} must "
                f"be at least the effective POLL_INTERVAL_S={self.poll_interval_s}. "
                "Reconciling more often than we sample only adds REST load that "
                "no row can consume."
            )
        return self

    # ------------------------------------------------------- Leviton ingestion
    @property
    def leviton_ws_enabled(self) -> bool:
        """True when a WebSocket subscriber should run (``hybrid`` or ``ws``)."""
        return self.leviton_ingest in ("hybrid", "ws")

    @property
    def leviton_rest_reconcile_enabled(self) -> bool:
        """True when REST should periodically re-read full state (``hybrid``).

        In ``ws`` mode REST is used for discovery only; in ``rest`` mode every
        cycle is already a full REST read, so there is nothing to reconcile.
        """
        return self.leviton_ingest == "hybrid"

    # ---------------------------------------------------------- derived paths
    @property
    def spool_db_path(self) -> Path:
        """SQLite spool database on the mounted volume."""
        return self.spool_dir / "spool.db"

    @property
    def token_dir(self) -> Path:
        """Directory for cached auth tokens (files must be written mode 600)."""
        return self.spool_dir / "tokens"

    @property
    def leviton_token_path(self) -> Path:
        return self.token_dir / "leviton.json"

    @property
    def carrier_token_path(self) -> Path:
        return self.token_dir / "carrier.json"

    @property
    def status_path(self) -> Path:
        """``status.json``, rewritten atomically after every stage action."""
        return self.spool_dir / "status.json"

    @property
    def log_level_number(self) -> int:
        return _stdlib_logging.getLevelNamesMapping()[self.log_level]

    # --------------------------------------------------------------- accessors
    def require(self, field: str) -> str:
        """Return a non-empty string setting, or raise naming the env var.

        Use at the point of need (``settings.require("s3_bucket")``) so that a
        missing credential is a clear startup error rather than an obscure
        downstream failure — and so pure-logic tests never need an environment.
        """
        if field not in type(self).model_fields:
            raise KeyError(f"unknown setting {field!r}")
        value = getattr(self, field)
        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        if value is None or (isinstance(value, str) and not value.strip()):
            raise RuntimeError(
                f"Required setting {field.upper()} is not configured "
                "(set it in the environment or .env; see .env.example)."
            )
        return str(value)

    def secret_values(self) -> tuple[str, ...]:
        """Plaintext values of every secret field, for the log scrubber only.

        Empty secrets are omitted so the scrubber never tries to redact ``""``.
        """
        out: list[str] = []
        for name in SECRET_SETTING_FIELDS:
            secret = getattr(self, name, None)
            if isinstance(secret, SecretStr):
                value = secret.get_secret_value()
                if value:
                    out.append(value)
        return tuple(out)


def describe_env_source() -> str:
    """Where configuration came from, for an error message to point at.

    Diagnostics only, and never a value — only the path, or the fact that there
    was none.
    """
    configured = Settings.model_config.get("env_file")
    if not configured:
        return "the process environment only (no .env file)"
    path = Path(configured)
    if path.is_file():
        return f"{path} plus the process environment"
    return (
        f"the process environment only — no .env was found at or above "
        f"{Path.cwd()}"
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings`, constructed once."""
    return Settings()


def reset_settings_cache() -> None:
    """Drop the cached :class:`Settings` (tests that mutate the environment)."""
    get_settings.cache_clear()
    # The log scrubber memoises the plaintext secrets it pulled from Settings;
    # imported lazily to keep config.py free of intra-package import cycles.
    from energy_capture.logging import refresh_config_secrets

    refresh_config_secrets()
