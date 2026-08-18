"""Carrier/Bryant Okta auth + the GraphQL transport (PLAN.md §7.1).

Both Bryant paths — the 30s status poller (§7.3) and the ~08:30 daily energy
fetch (§7.2) — talk to ``dataservice.infinity.iot.carrier.com`` and both need a
live OAuth2 access token. This module is the only place that knows how either is
obtained, so ``sources/bryant.py`` and the daily stage contain no auth code at
all.

Layout, outside-in:

* :class:`CarrierGraphQLClient` — what callers use. ``await client.query(...)``
  returns the GraphQL ``data`` object or raises. It owns the retry policy, the
  429 backoff and the 401 recovery ladder.
* :class:`CarrierAuth` — the token manager. Password grant, refresh grant, the
  on-disk cache, proactive renewal, and the ``asyncio.Lock`` that makes N
  concurrent callers cause exactly **one** token request.
* :class:`CarrierToken` / :class:`CarrierTokenCache` — the credential and its
  ``{SPOOL_DIR}/tokens/carrier.json`` (mode ``0600``) persistence.
* :class:`_HttpSender` — one long-lived ``httpx.AsyncClient``, tenacity retries
  for transient 5xx/network failures, and ``Retry-After`` handling.

What this module exists to get right
------------------------------------

**Refresh, don't re-authenticate.** The old collector
(``~/code/bryantDataCollector/carrier_auth.py``, ported here — never imported)
sent the *password* to Okta on every run and threw the refresh token away. That
is defensible once a day and indefensible at 30 seconds. Here the password grant
runs only when there is no usable cached token and only when a refresh grant has
already failed. Note that Okta **rotates the refresh token** on every refresh, so
the cache is rewritten on every renewal — losing that write burns the token.

**Never spin** — and not merely within one call. One ``query()`` performs at
most one refresh grant and one password grant, in that order, each followed by
exactly one retry. Bounding a single call is not enough at 30 seconds, though,
so four floors bound the *sequence* of calls as well:

* :data:`MIN_RENEW_INTERVAL_S` — between expiry-driven renewals, so clock skew
  or a bogus ``exp`` cannot make every cycle mint a token;
* :data:`GRANT_FAILURE_BACKOFF_S` — after Okta *rejects* the password, so a
  wrong ``CARRIER_PASSWORD`` degrades to one attempt a minute rather than one
  per poll (the failed grant leaves no token, so the first floor cannot see it);
* :data:`MIN_PASSWORD_GRANT_INTERVAL_S` — between rejection-driven password
  grants, because a 401 a brand-new token did not fix will not be fixed by a
  newer one;
* :data:`AUTH_LADDER_BACKOFF_S` — after the whole ladder is exhausted, so a
  persistent 401/403 costs one attempt per cycle instead of a full re-climb.

A 429 whose ``Retry-After`` is longer than :data:`MAX_INLINE_RETRY_AFTER_S`
opens its own backoff window on top of that: calls during it fail immediately
(a gap — correct) instead of queueing against a throttled endpoint.

**Tokens never reach a log line, a traceback, or ``status.json``.** Both tokens
go to :func:`~energy_capture.logging.register_secret` the instant they are
parsed — on the login path *and* on the refresh path. :class:`CarrierToken` has
a redacting ``__repr__``, exception messages are built from status codes and
OAuth error *codes* (never bodies) and are passed through
:func:`~energy_capture.logging.scrub_text` on the way out, and
:meth:`CarrierGraphQLClient.status_fields` returns only counters and timestamps.

**Callers keep polling.** Everything raised here is either
:class:`CarrierAuthError` (a :class:`~energy_capture.sources.base.SourceAuthError`)
or :class:`CarrierTransientError` (a
:class:`~energy_capture.sources.base.SourceTransientError`), which is exactly
the pair ``stages/poller.py`` already handles: count it, emit no rows, move on.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Final

import httpx
from tenacity import AsyncRetrying, retry_if_exception, stop_after_attempt

from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger, register_secret, scrub_text
from energy_capture.sources.base import SourceAuthError, SourceError, SourceTransientError
from energy_capture.timeutil import UTC, now_utc

__all__ = [
    "AUTH_LADDER_BACKOFF_S",
    "DEFAULT_RETRY_AFTER_S",
    "GRANT_FAILURE_BACKOFF_S",
    "GRAPHQL_URL",
    "MAX_INLINE_RETRY_AFTER_S",
    "MIN_PASSWORD_GRANT_INTERVAL_S",
    "MIN_RENEW_INTERVAL_S",
    "OKTA_CLIENT_ID",
    "OKTA_TOKEN_URL",
    "OAUTH_SCOPE",
    "REFRESH_MARGIN_S",
    "RETRY_WAITS_S",
    "SPA_ORIGIN",
    "UNKNOWN_TOKEN_LIFETIME_S",
    "CarrierAuth",
    "CarrierAuthError",
    "CarrierError",
    "CarrierGraphQLClient",
    "CarrierGraphQLError",
    "CarrierRateLimitError",
    "CarrierToken",
    "CarrierTokenCache",
    "CarrierTransientError",
    "ThrottleState",
    "auth_from_settings",
    "carrier_stack_from_settings",
    "decode_jwt_exp",
    "graphql_client_from_settings",
    "parse_retry_after",
]


# --------------------------------------------------------------------- endpoints

#: Carrier fronts its identity with Okta; the app uses the ``default`` auth server.
OKTA_BASE_URL: Final[str] = "https://sso.carrier.com"
OKTA_AUTH_SERVER: Final[str] = "default"

#: The public SPA client id. Hardcoding is fine and deliberate (PLAN.md §7.1):
#: it is a *public* client, it is visible in the web app's bundle, and it is the
#: same id the old collector and ``dahlb/carrier-api`` both use.
OKTA_CLIENT_ID: Final[str] = "0oa1ce7hwjuZbfOMB4x7"

OKTA_TOKEN_URL: Final[str] = f"{OKTA_BASE_URL}/oauth2/{OKTA_AUTH_SERVER}/v1/token"

#: ``offline_access`` is what buys the refresh token; without it this module's
#: whole reason for existing evaporates. ``openid`` matches the grant that has
#: been working against this account for years.
OAUTH_SCOPE: Final[str] = "openid offline_access"

GRAPHQL_URL: Final[str] = "https://dataservice.infinity.iot.carrier.com/graphql"

#: Spoofed SPA headers (PLAN.md §7.1). ``dahlb/carrier-api`` omits all three and
#: works, so they are probably not load-bearing — but the old repo's working code
#: sends them, they are free, and "probably" is not a reason to drop them from a
#: collector that must not silently stop.
SPA_ORIGIN: Final[str] = "https://my.carrier.com"
SPA_REFERER: Final[str] = "https://my.carrier.com/"
MOBILE_APP_BRAND: Final[str] = "carrier"


# --------------------------------------------------------------------- policy

#: Renew this many seconds *before* the token actually expires. A 30s poll and a
#: daily job must never be the ones to discover expiry by getting a 401.
REFRESH_MARGIN_S: Final[float] = 300.0

#: Assumed lifetime when the token endpoint sends neither ``expires_in`` nor a
#: decodable JWT ``exp``. Deliberately shorter than any plausible real lifetime:
#: renewing too often is cheap, using a dead token is a gap.
UNKNOWN_TOKEN_LIFETIME_S: Final[float] = 900.0

#: Floor between *expiry-driven* renewals. Anti-spin insurance against clock skew
#: or a nonsense ``exp``: without it a token that always looks expired would mean
#: a token request every poll. A 401 bypasses this floor — that is real evidence.
#:
#: It guards a token we still **hold**, and deliberately nothing else. A grant
#: that *failed* leaves no token to hold, so the case below needs its own floor.
MIN_RENEW_INTERVAL_S: Final[float] = 30.0

#: Backoff after Okta **rejects** a password grant — a wrong ``CARRIER_PASSWORD``,
#: a locked account, a revoked client. PLAN.md §6.6's rule for the equivalent
#: Leviton condition ("if login fails, back off 60s and keep trying"), and the
#: same number as ``sources/leviton.py``'s ``LOGIN_FAILURE_BACKOFF_S``.
#:
#: :data:`MIN_RENEW_INTERVAL_S` cannot cover this and was never meant to: a
#: failed grant path clears the in-memory token, so ``token()``'s floor — which
#: only applies when a token exists — is skipped on every subsequent call.
#: Without this constant nothing at all rate-limits the password grant, and a
#: mis-credentialed container sends the password to Okta once per poll cycle,
#: 2,880 times a day, forever.
GRANT_FAILURE_BACKOFF_S: Final[float] = 60.0

#: Floor between password grants that were driven by a **rejection** rather than
#: by expiry (PLAN.md §7.1: "the old collector re-authenticated with the password
#: on every run … at 30s cadence that is unacceptable").
#:
#: A 401/403 that survives a brand-new token is not a stale-token problem, so
#: re-sending the password every cycle cannot fix it — it only points a 30s loop
#: at an identity provider. Fifteen minutes keeps the retry alive and bounded
#: (96 grants a day in the worst case) instead of hot. Expiry-driven and
#: cold-start grants are **not** floored by this: they are the normal path.
MIN_PASSWORD_GRANT_INTERVAL_S: Final[float] = 900.0

#: How long :class:`CarrierGraphQLClient` stops climbing the whole auth ladder
#: after a token it minted seconds ago was itself rejected. Inside the window a
#: call makes exactly **one** attempt with the token it already holds and fails —
#: a gap, which is the honest outcome under CLAUDE.md rule 1 — rather than buying
#: two fresh tokens per cycle to be rejected with.
AUTH_LADDER_BACKOFF_S: Final[float] = 900.0

#: In-call retry schedule for transient 5xx / network failures.
RETRY_WAITS_S: Final[tuple[float, ...]] = (2.0, 5.0)

#: A 429 asking for no more than this is honoured inline (sleep and retry).
#: Anything longer opens a backoff window and fails the call instead — a poll
#: cycle blocked for two minutes is worse than a cycle that records a gap.
MAX_INLINE_RETRY_AFTER_S: Final[float] = 5.0

#: Assumed pause when a 429 arrives with no ``Retry-After`` at all.
DEFAULT_RETRY_AFTER_S: Final[float] = 60.0

#: Never honour an absurd ``Retry-After``; cap the window at an hour.
MAX_RETRY_AFTER_S: Final[float] = 3600.0

#: Status codes that mean "your token is no good" (``carrier-api`` treats 403 the
#: same way, and Carrier's gateway does return it for an expired token).
AUTH_HTTP_STATUSES: Final[frozenset[int]] = frozenset({401, 403})

#: GraphQL ``extensions.code`` values that mean the same thing.
GRAPHQL_AUTH_CODES: Final[frozenset[str]] = frozenset(
    {"UNAUTHENTICATED", "UNAUTHORIZED", "FORBIDDEN", "ACCESS_DENIED", "INVALID_TOKEN"}
)

_GRAPHQL_AUTH_PHRASES: Final[tuple[str, ...]] = (
    "unauthenticated",
    "unauthorized",
    "not authorized",
    "access denied",
    "invalid token",
    "token expired",
    "expired token",
    "jwt expired",
)

#: Connect/read timeouts. Long enough for a sleepy cloud, short enough that a
#: hung socket cannot swallow a poll cycle.
DEFAULT_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(20.0, connect=10.0)

#: Longest error detail copied out of an upstream response into an exception.
_MAX_ERROR_DETAIL: Final[int] = 240


# --------------------------------------------------------------------- errors


class CarrierError(SourceError):
    """Base for every failure this module reports."""


class CarrierAuthError(CarrierError, SourceAuthError):
    """Okta or the GraphQL gateway rejected our credentials or token.

    A :class:`~energy_capture.sources.base.SourceAuthError`, so ``stages/poller.py``
    already knows what to do with it. ``invalid_grant`` is flagged separately
    because it is the specific signal that the *refresh token* is dead and the
    password grant is the only way forward.

    :attr:`errors` carries the GraphQL ``errors`` array when the rejection
    arrived inside a ``200 OK`` body, and is **empty for a transport-level
    401/403**. That distinction is load-bearing rather than decorative: an
    ``errors`` entry naming a field ("not authorized to access field
    ``infinityStatus``") means the gateway rejected *that field*, not our token,
    and a caller may legitimately respond by asking a different question — see
    ``sources/bryant.py::_fetch_status``. A bare 401 means the token, and only
    the auth ladder can fix it.
    """

    def __init__(
        self,
        message: str,
        *,
        invalid_grant: bool = False,
        status_code: int | None = None,
        errors: Sequence[Any] = (),
    ) -> None:
        super().__init__(scrub_text(message))
        self.invalid_grant = invalid_grant
        self.status_code = status_code
        self.errors = list(errors)


class CarrierTransientError(CarrierError, SourceTransientError):
    """A retryable upstream hiccup: 5xx, a timeout, a reset, an unparseable body."""


class CarrierRateLimitError(CarrierTransientError):
    """HTTP 429. ``retry_after_s`` is the pause the server asked for (or our default).

    Transient by inheritance: the poll loop counts it, writes no rows and carries
    on, which is exactly right — a throttled cycle is a gap, not a failure worth
    crashing over. :attr:`retry_after_s` is what callers put in ``status.json``.
    """

    def __init__(self, message: str, *, retry_after_s: float | None = None) -> None:
        super().__init__(scrub_text(message))
        self.retry_after_s = retry_after_s


class CarrierGraphQLError(CarrierTransientError):
    """HTTP 200 with a non-empty ``errors`` array — a failure, never data.

    Carrier answers a malformed or unauthorised query with ``200 OK`` and an
    ``errors`` array, sometimes alongside a partial ``data`` object. Returning
    that partial object would let half-populated rows into the archive, so the
    whole response is rejected. :attr:`errors` keeps the raw array for logging.
    """

    def __init__(self, message: str, *, errors: Sequence[Any] = ()) -> None:
        super().__init__(scrub_text(message))
        self.errors = list(errors)


# ------------------------------------------------------------------ JWT helper


def decode_jwt_exp(token: str | None) -> float | None:
    """Return a JWT's ``exp`` claim as a POSIX timestamp, or ``None``.

    The fallback for a token response that omits ``expires_in`` (PLAN.md §7.1).
    A JWT payload is just base64url of JSON, so **no dependency is added** and
    **no signature is verified** — we are reading our own token's expiry hint,
    not trusting a third party's assertion.

    Defensive by construction: a non-JWT, a truncated segment, missing base64
    padding, non-UTF-8 bytes, a payload that is not a JSON object, a missing or
    non-numeric ``exp`` — every one of these returns ``None`` rather than
    raising. The caller is a 30s poll loop; it must never die on a malformed
    string, and it must never see the token in a traceback either.
    """
    if not isinstance(token, str):
        return None
    parts = token.strip().split(".")
    if len(parts) < 2:
        return None
    segment = parts[1]
    if not segment:
        return None
    # base64url without padding is legal in a JWS; restore it before decoding.
    padded = segment + "=" * (-len(segment) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError, UnicodeEncodeError):
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    exp = payload.get("exp")
    if isinstance(exp, bool) or not isinstance(exp, (int, float, str)):
        return None
    try:
        value = float(exp)
    except (TypeError, ValueError):
        return None
    if value != value or value in (float("inf"), float("-inf")) or value <= 0:
        return None
    return value


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """``Retry-After`` as seconds. Accepts the delta-seconds and HTTP-date forms.

    Returns ``None`` for an absent or unparseable header (the caller then uses
    :data:`DEFAULT_RETRY_AFTER_S`), and never more than :data:`MAX_RETRY_AFTER_S`
    — an upstream bug must not park the collector for a week.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    seconds: float
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - (now or now_utc())).total_seconds()
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        return None
    return max(0.0, min(seconds, MAX_RETRY_AFTER_S))


# ------------------------------------------------------------------- the token


@dataclass(frozen=True, slots=True, repr=False)
class CarrierToken:
    """One Okta token response, plus when it stops being usable.

    ``__repr__`` is overridden to redact both tokens: this object ends up in
    local variables all over an async stack, and a traceback rendering it would
    put a live credential in the logs (CLAUDE.md rule 8).
    """

    access_token: str
    refresh_token: str | None = None
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    obtained_at: datetime | None = None
    scope: str | None = None
    username: str | None = None
    #: ``"expires_in"`` | ``"jwt_exp"`` | ``"assumed"`` | ``"cache"`` — how
    #: :attr:`expires_at` was established. Useful in logs, never a secret.
    expiry_source: str = "expires_in"

    # ------------------------------------------------------------- accessors
    @property
    def authorization(self) -> str:
        """The ``Authorization`` header value (``Bearer <token>``)."""
        return f"{self.token_type or 'Bearer'} {self.access_token}"

    def seconds_remaining(self, now: datetime) -> float | None:
        """Seconds until expiry (negative if already expired); ``None`` if unknown."""
        if self.expires_at is None:
            return None
        return (self.expires_at - now).total_seconds()

    def effective_margin(self, margin: float = REFRESH_MARGIN_S) -> float:
        """``margin``, capped at half this token's total lifetime.

        Okta issues hour-long tokens today, so the five-minute margin is a
        rounding error. If it ever issues six-minute ones, an uncapped margin
        would make every token "about to expire" the moment it arrives and turn
        the 30s poller into a token-endpoint client. Half the lifetime is the
        most aggressive renewal that still leaves the token usable.
        """
        if self.expires_at is None or self.obtained_at is None:
            return margin
        lifetime = (self.expires_at - self.obtained_at).total_seconds()
        if lifetime <= 0:
            return margin
        return min(margin, lifetime / 2.0)

    def is_fresh(self, now: datetime, *, margin: float = REFRESH_MARGIN_S) -> bool:
        """True when the token is good for at least ``margin`` more seconds.

        An unknown expiry is treated as **not** fresh: guessing that a token we
        cannot date is still valid is how a poller ends up hammering 401s.
        """
        remaining = self.seconds_remaining(now)
        if remaining is None:
            return False
        return remaining > self.effective_margin(margin)

    # ------------------------------------------------------------ persistence
    def to_payload(self) -> dict[str, Any]:
        """The JSON written to ``{SPOOL_DIR}/tokens/carrier.json``."""
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_at": _iso(self.expires_at),
            "obtained_at": _iso(self.obtained_at),
            "scope": self.scope,
            "username": self.username,
            "expiry_source": self.expiry_source,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> CarrierToken | None:
        """Rebuild from cache JSON, or ``None`` if the file is not usable.

        A cache with no ``access_token`` but a usable ``refresh_token`` is still
        worth having — the refresh grant avoids sending the password — so the
        access token is allowed to be empty here and simply reads as expired.
        """
        access = payload.get("access_token")
        refresh = payload.get("refresh_token")
        if not isinstance(access, str):
            access = ""
        if not isinstance(refresh, str) or not refresh:
            refresh = None
        if not access and not refresh:
            return None
        return cls(
            access_token=access,
            refresh_token=refresh,
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_at=_parse_iso(payload.get("expires_at")),
            obtained_at=_parse_iso(payload.get("obtained_at")),
            scope=payload.get("scope") if isinstance(payload.get("scope"), str) else None,
            username=payload.get("username") if isinstance(payload.get("username"), str) else None,
            expiry_source=str(payload.get("expiry_source") or "cache"),
        )

    def __repr__(self) -> str:
        return (
            "CarrierToken(access_token=***REDACTED***, "
            f"refresh_token={'***REDACTED***' if self.refresh_token else None}, "
            f"token_type={self.token_type!r}, expires_at={_iso(self.expires_at)!r}, "
            f"expiry_source={self.expiry_source!r})"
        )


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


# ------------------------------------------------------------------ the cache


@dataclass(frozen=True, slots=True)
class CarrierTokenCache:
    """``{SPOOL_DIR}/tokens/carrier.json``, mode ``0600`` (PLAN.md §7.1).

    Rewritten on **every** renewal, because Okta rotates the refresh token on
    each refresh grant: skipping the write would leave the old, now-invalid
    refresh token on disk and force a password grant on the next start.

    ``username`` is stored and checked on load so that changing
    ``CARRIER_USERNAME`` cannot silently reuse the previous account's token.
    """

    path: Path

    def load(self, *, username: str | None = None) -> CarrierToken | None:
        """Return the cached token, or ``None`` if absent/corrupt/foreign."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except (FileNotFoundError, NotADirectoryError, IsADirectoryError, PermissionError, OSError):
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        token = CarrierToken.from_payload(payload)
        if token is None:
            return None
        if username and token.username and token.username != username:
            return None
        # A cache restored from a backup (or written by an older run) may be
        # world-readable; tighten it on the way in rather than trusting it.
        self._chmod()
        return token

    def save(self, token: CarrierToken) -> None:
        """Write the token atomically, mode ``0600``, creating ``tokens/`` if needed."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".carrier-", suffix=".json.tmp", dir=str(self.path.parent)
        )
        tmp = Path(tmp_name)
        try:
            # chmod *before* anything is written: the secret must never exist on
            # disk, even for an instant, under the process umask.
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(token.to_payload(), handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        self._chmod()

    def clear(self) -> None:
        """Drop the cache. A token the server rejected is worse than none."""
        self.path.unlink(missing_ok=True)

    def _chmod(self) -> None:
        try:
            os.chmod(self.path, 0o600)
        except OSError:  # pragma: no cover - unusual filesystem
            pass


# --------------------------------------------------------------- throttling


@dataclass(slots=True)
class ThrottleState:
    """What this endpoint has told us about rate limiting (PLAN.md §7.3).

    §7.3 says the throttling behaviour at 30s is unknown and asks for the
    *effective* cadence to be recorded, so every field here is designed to be
    dropped straight into ``status.json`` — counters and timestamps only, never
    a credential.
    """

    #: How many 429s this process has seen from this endpoint.
    events: int = 0
    #: The pause the server most recently asked for, in seconds.
    last_retry_after_s: float | None = None
    #: When that happened (UTC).
    last_event_utc: datetime | None = None
    #: Monotonic deadline before which calls fail fast. ``None`` when open.
    blocked_until: float | None = None

    def record(
        self, *, retry_after_s: float, now: datetime, monotonic: float, block: bool
    ) -> None:
        self.events += 1
        self.last_retry_after_s = retry_after_s
        self.last_event_utc = now
        if block:
            self.blocked_until = monotonic + retry_after_s

    def remaining(self, monotonic: float) -> float:
        """Seconds left in the backoff window (0.0 when not throttled)."""
        if self.blocked_until is None:
            return 0.0
        remaining = self.blocked_until - monotonic
        if remaining <= 0:
            self.blocked_until = None
            return 0.0
        return remaining

    def clear(self) -> None:
        self.blocked_until = None

    def status_fields(self, monotonic: float) -> dict[str, Any]:
        """The ``status.json`` view. Safe to log: no secrets can appear here."""
        remaining = self.remaining(monotonic)
        return {
            "throttle_events": self.events,
            "retry_after_s": self.last_retry_after_s,
            "last_throttle_utc": _iso(self.last_event_utc),
            "backoff_remaining_s": round(remaining, 3),
            "throttled": remaining > 0,
        }


# ------------------------------------------------------------------ http layer


class _HttpSender:
    """One long-lived ``httpx.AsyncClient`` plus the retry/backoff policy.

    ``dahlb/carrier-api`` builds a fresh transport and GraphQL client for every
    query — ~2,880 TLS handshakes a day at our cadence. This class exists so
    that both the token endpoint and the GraphQL endpoint reuse one pool.

    Classification is deliberately narrow: 5xx and network errors become
    :class:`CarrierTransientError` (retried), 429 becomes
    :class:`CarrierRateLimitError`, and **every other status is returned to the
    caller** — the token endpoint and the GraphQL endpoint disagree about what a
    400 or a 200 means, and that judgement belongs to them, not here.
    """

    def __init__(
        self,
        *,
        name: str,
        client: httpx.AsyncClient | None = None,
        owns_client: bool | None = None,
        retry_waits: Sequence[float] = RETRY_WAITS_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = now_utc,
        max_inline_retry_after_s: float = MAX_INLINE_RETRY_AFTER_S,
        log: Any = None,
    ) -> None:
        self._name = name
        self._client = client
        self._owns_client = (client is None) if owns_client is None else bool(owns_client)
        self._retry_waits = tuple(float(w) for w in retry_waits)
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now
        self._max_inline_retry_after_s = float(max_inline_retry_after_s)
        self._log = log if log is not None else get_logger("carrier")
        self.throttle = ThrottleState()

    # ---------------------------------------------------------- housekeeping
    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
            self._owns_client = True
        return self._client

    async def close(self) -> None:
        """Release the pool if we created it. Idempotent."""
        client, self._client = self._client, None
        if client is not None and self._owns_client:
            await client.aclose()

    # ------------------------------------------------------------- sending
    async def send(
        self,
        method: str,
        url: str,
        *,
        op: str,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
        json_body: Any = None,
    ) -> httpx.Response:
        """Send one request, retrying transient failures. Returns 2xx **and 4xx**.

        Raises :class:`CarrierRateLimitError` when a backoff window is open or a
        429 asks for longer than we are willing to wait inline, and
        :class:`CarrierTransientError` when 5xx/network failures survive
        :data:`RETRY_WAITS_S`.
        """
        remaining = self.throttle.remaining(self._monotonic())
        if remaining > 0:
            # Fail fast rather than queue against a throttled endpoint. The
            # caller records a gap for this cycle, which is the honest outcome.
            raise CarrierRateLimitError(
                f"carrier {op}: rate limited, {remaining:.1f}s of backoff remaining",
                retry_after_s=remaining,
            )

        client = self._ensure_client()
        waits = self._retry_waits
        attempts = len(waits) + 1

        async def attempt() -> httpx.Response:
            try:
                response = await client.request(
                    method, url, headers=dict(headers or {}), data=data, json=json_body
                )
            except httpx.TimeoutException as exc:
                raise CarrierTransientError(f"carrier {op}: timeout") from exc
            except httpx.TransportError as exc:
                raise CarrierTransientError(f"carrier {op}: {type(exc).__name__}") from exc
            if response.status_code == 429:
                raise self._rate_limited(response, op=op)
            if response.status_code >= 500:
                raise CarrierTransientError(
                    f"carrier {op}: HTTP {response.status_code}"
                )
            # 2xx and 4xx both come back untouched; the caller decides.
            self.throttle.clear()
            return response

        def wait(retry_state: Any) -> float:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            if isinstance(exc, CarrierRateLimitError) and exc.retry_after_s is not None:
                return min(exc.retry_after_s, self._max_inline_retry_after_s)
            index = min(retry_state.attempt_number, len(waits)) - 1
            return waits[index] if waits else 0.0

        def before_sleep(retry_state: Any) -> None:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            self._log.debug(
                "carrier_retry",
                endpoint=self._name,
                op=op,
                attempt=retry_state.attempt_number,
                error=str(exc) if exc else None,
            )

        retrying = AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait,
            retry=retry_if_exception(self._retryable),
            before_sleep=before_sleep,
            sleep=self._sleep,
            reraise=True,
        )
        return await retrying(attempt)

    def _retryable(self, exc: BaseException) -> bool:
        """5xx/network yes; a 429 only when the requested pause is short."""
        if isinstance(exc, CarrierRateLimitError):
            return (
                exc.retry_after_s is not None
                and exc.retry_after_s <= self._max_inline_retry_after_s
            )
        return isinstance(exc, CarrierTransientError)

    def _rate_limited(self, response: httpx.Response, *, op: str) -> CarrierRateLimitError:
        now = self._now()
        retry_after = parse_retry_after(response.headers.get("retry-after"), now=now)
        if retry_after is None:
            retry_after = DEFAULT_RETRY_AFTER_S
        # Only a pause we are *not* going to sit through opens the window; a
        # short one is absorbed inline and must not block the next call.
        block = retry_after > self._max_inline_retry_after_s
        self.throttle.record(
            retry_after_s=retry_after,
            now=now,
            monotonic=self._monotonic(),
            block=block,
        )
        self._log.warning(
            "carrier_rate_limited",
            endpoint=self._name,
            op=op,
            retry_after_s=retry_after,
            inline_retry=not block,
            events=self.throttle.events,
        )
        return CarrierRateLimitError(
            f"carrier {op}: HTTP 429, retry after {retry_after:.0f}s",
            retry_after_s=retry_after,
        )


def _error_detail(response: httpx.Response) -> str:
    """A short, scrubbed description of a failed response.

    OAuth error *codes* (``invalid_grant``, ``invalid_client``) are the useful
    part and carry no secret. The body is never copied wholesale: a token
    endpoint response can contain a credential, and this string ends up in an
    exception message that reaches ``status.json``.
    """
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        code = payload.get("error") or payload.get("errorCode")
        description = payload.get("error_description") or payload.get("errorSummary")
        parts = [str(p) for p in (code, description) if isinstance(p, (str, int))]
        if parts:
            return scrub_text(": ".join(parts))[:_MAX_ERROR_DETAIL]
    return ""


def _oauth_error_code(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return ""
    if isinstance(payload, dict):
        code = payload.get("error")
        if isinstance(code, str):
            return code.strip().lower()
    return ""


# ------------------------------------------------------------------ the auth


class CarrierAuth:
    """The Carrier token manager (PLAN.md §7.1).

    ``await auth.token()`` always returns a token that is either freshly
    validated by expiry or newly obtained. Renewal order is fixed:

    1. the in-memory token, if it has more than ``refresh_margin_s`` left;
    2. the on-disk cache (loaded once per process), same test;
    3. ``grant_type=refresh_token``;
    4. ``grant_type=password`` — **only** after (3) failed or was impossible.

    Concurrency: every path that can issue a token request holds
    :attr:`_lock`, and re-checks freshness *after* acquiring it. Eight coroutines
    arriving at an expired token therefore produce exactly one HTTP request —
    the 30s poller and the 08:30 daily job cannot stampede Okta.

    Injection points exist only for tests: ``client`` (an ``httpx.AsyncClient``,
    typically over ``httpx.MockTransport``), ``now``, ``sleep``, ``monotonic``
    and ``retry_waits``. Production passes none of them.
    """

    def __init__(
        self,
        *,
        username: str,
        password: str,
        token_path: Path,
        client: httpx.AsyncClient | None = None,
        owns_client: bool | None = None,
        refresh_margin_s: float = REFRESH_MARGIN_S,
        min_renew_interval_s: float = MIN_RENEW_INTERVAL_S,
        grant_failure_backoff_s: float = GRANT_FAILURE_BACKOFF_S,
        min_password_grant_interval_s: float = MIN_PASSWORD_GRANT_INTERVAL_S,
        retry_waits: Sequence[float] = RETRY_WAITS_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = now_utc,
        token_url: str = OKTA_TOKEN_URL,
        client_id: str = OKTA_CLIENT_ID,
        scope: str = OAUTH_SCOPE,
    ) -> None:
        self._username = username
        self._password = password
        self._cache = CarrierTokenCache(Path(token_path))
        self._log = get_logger("carrier")
        self._http = _HttpSender(
            name="okta",
            client=client,
            owns_client=owns_client,
            retry_waits=retry_waits,
            sleep=sleep,
            monotonic=monotonic,
            now=now,
            log=self._log,
        )
        self._refresh_margin_s = float(refresh_margin_s)
        self._min_renew_interval_s = float(min_renew_interval_s)
        self._grant_failure_backoff_s = float(grant_failure_backoff_s)
        self._min_password_grant_interval_s = float(min_password_grant_interval_s)
        self._now = now
        self._monotonic = monotonic
        self._token_url = token_url
        self._client_id = client_id
        self._scope = scope

        self._lock = asyncio.Lock()
        self._token: CarrierToken | None = None
        self._cache_loaded = False
        self._last_renew_monotonic: float | None = None
        #: Monotonic deadline before which **no** grant is attempted, armed when
        #: Okta rejects the password. ``None`` when the door is open.
        self._grant_retry_not_before: float | None = None
        #: When the password was last sent, floor for rejection-driven grants.
        self._last_password_grant_monotonic: float | None = None
        self._password_grants = 0
        self._refresh_grants = 0

    # ------------------------------------------------------------- accessors
    @property
    def token_path(self) -> Path:
        return self._cache.path

    @property
    def password_grants(self) -> int:
        """How many times the *password* has been sent. Should stay tiny (§7.1)."""
        return self._password_grants

    @property
    def refresh_grants(self) -> int:
        return self._refresh_grants

    @property
    def throttle(self) -> ThrottleState:
        """Rate-limit state of the **token endpoint** (Okta throttles too)."""
        return self._http.throttle

    def status_fields(self) -> dict[str, Any]:
        """Auth counters for ``status.json``. Contains no credential, by design."""
        token = self._token
        fields: dict[str, Any] = {
            "password_grants": self._password_grants,
            "refresh_grants": self._refresh_grants,
            "token_expires_at": _iso(token.expires_at) if token else None,
            "token_expiry_source": token.expiry_source if token else None,
        }
        fields.update(
            {f"okta_{k}": v for k, v in self._http.throttle.status_fields(self._monotonic()).items()}
        )
        return fields

    async def close(self) -> None:
        await self._http.close()

    # ------------------------------------------------------------- the token
    async def token(self) -> CarrierToken:
        """A token with life left in it, obtaining one only if necessary."""
        current = self._token
        if current is not None and current.is_fresh(self._now(), margin=self._refresh_margin_s):
            return current

        async with self._lock:
            # Re-check under the lock: while we waited, another coroutine may
            # have done the renewal we queued up for. This is the whole of the
            # "concurrent callers cause exactly one refresh" guarantee.
            current = self._token
            if current is not None and current.is_fresh(
                self._now(), margin=self._refresh_margin_s
            ):
                return current
            self._load_cache_locked()
            current = self._token
            if current is not None and current.is_fresh(
                self._now(), margin=self._refresh_margin_s
            ):
                self._log.debug(
                    "carrier_token_cached",
                    expires_at=_iso(current.expires_at),
                    source=current.expiry_source,
                )
                return current
            if current is not None and not self._renew_floor_open():
                # Anti-spin: the token looks stale but we renewed moments ago,
                # so the staleness is more likely clock skew than reality. Use
                # what we have; a genuine rejection arrives as a 401, which
                # bypasses this floor.
                self._log.warning(
                    "carrier_renew_floored",
                    min_interval_s=self._min_renew_interval_s,
                    expires_at=_iso(current.expires_at),
                )
                return current
            return await self._renew_locked(reason="expiry", allow_refresh=True)

    async def access_token(self) -> str:
        """Convenience: just the bearer string."""
        return (await self.token()).access_token

    async def reauthenticate(
        self, *, stale_token: str | None = None, allow_refresh: bool = True, reason: str = "unauthorized"
    ) -> CarrierToken:
        """Force a renewal after a rejection. Bypasses the expiry floor.

        ``stale_token`` makes this idempotent under concurrency: if the token has
        already been replaced by another coroutine (because two in-flight
        requests both got a 401 on the same dead token), the new one is returned
        without a second round-trip to Okta.

        ``allow_refresh=False`` skips straight to the password grant — the last
        rung of the ladder, used when a refreshed token was *also* rejected.

        Every call here is by definition **rejection-driven**, so the password
        grant it may reach is floored by :data:`MIN_PASSWORD_GRANT_INTERVAL_S`.
        Bypassing the expiry floor is deliberate (a 401 is evidence); bypassing
        every floor is what turned a persistent rejection into 2,880 password
        grants a day.
        """
        async with self._lock:
            current = self._token
            if (
                stale_token is not None
                and current is not None
                and current.access_token != stale_token
            ):
                self._log.debug("carrier_token_already_renewed", reason=reason)
                return current
            return await self._renew_locked(
                reason=reason, allow_refresh=allow_refresh, rejection_driven=True
            )

    # ------------------------------------------------------------- internals
    def _renew_floor_open(self) -> bool:
        if self._last_renew_monotonic is None:
            return True
        return (self._monotonic() - self._last_renew_monotonic) >= self._min_renew_interval_s

    def _grant_backoff_remaining(self) -> float:
        """Seconds until a grant may be attempted again (0.0 when open).

        Armed only by a password grant Okta *rejected* — never by a transient
        error and never by a 429, which have their own policies
        (:class:`CarrierTransientError` retries, :class:`ThrottleState`).
        """
        if self._grant_retry_not_before is None:
            return 0.0
        remaining = self._grant_retry_not_before - self._monotonic()
        if remaining <= 0:
            self._grant_retry_not_before = None
            return 0.0
        return remaining

    def _password_floor_remaining(self) -> float:
        """Seconds until a *rejection-driven* password grant is allowed again."""
        if self._last_password_grant_monotonic is None:
            return 0.0
        elapsed = self._monotonic() - self._last_password_grant_monotonic
        return max(0.0, self._min_password_grant_interval_s - elapsed)

    def _load_cache_locked(self) -> None:
        """Read ``carrier.json`` once per process (call with the lock held)."""
        if self._cache_loaded:
            return
        self._cache_loaded = True
        token = self._cache.load(username=self._username)
        if token is None:
            return
        # Registered before anything can log it — including this method's own
        # log line and any traceback raised further down (CLAUDE.md rule 8).
        register_secret(token.access_token)
        register_secret(token.refresh_token)
        self._token = token
        self._log.info(
            "carrier_token_restored",
            path=str(self._cache.path),
            expires_at=_iso(token.expires_at),
            has_refresh_token=token.refresh_token is not None,
        )

    async def _renew_locked(
        self, *, reason: str, allow_refresh: bool, rejection_driven: bool = False
    ) -> CarrierToken:
        """Refresh grant, then password grant. Exactly one fallback, no loop.

        Two floors bound the password grant, because the expiry floor in
        :meth:`token` cannot reach either case:

        * a grant Okta already rejected opens a
          :data:`GRANT_FAILURE_BACKOFF_S` window in which *no* grant is
          attempted at all — the wrong-password case, where the previous
          attempt cleared the in-memory token and so slipped past every guard
          that keys on holding one;
        * a ``rejection_driven`` grant (one the GraphQL ladder asked for after a
          401/403) is additionally floored by
          :data:`MIN_PASSWORD_GRANT_INTERVAL_S`, because a rejection a brand-new
          token did not fix will not be fixed by a newer one either.

        Both raise :class:`CarrierAuthError`, which the poll loop already knows
        how to count: no rows, no crash, a gap for that cycle.
        """
        blocked = self._grant_backoff_remaining()
        if blocked > 0:
            self._log.warning(
                "carrier_grant_backoff",
                reason=reason,
                remaining_s=round(blocked, 1),
                backoff_s=self._grant_failure_backoff_s,
                password_grants=self._password_grants,
                detail="Okta rejected our credentials; not retrying yet",
            )
            raise CarrierAuthError(
                f"carrier {reason}: credentials were rejected, "
                f"{blocked:.0f}s of backoff remaining"
            )

        self._load_cache_locked()
        current = self._token
        refresh_token = current.refresh_token if current is not None else None

        if allow_refresh and refresh_token:
            try:
                return await self._refresh_grant(refresh_token, reason=reason)
            except CarrierAuthError as exc:
                # The refresh token is dead (rotated out, revoked, expired).
                # Falling back to the password is the documented escalation —
                # once, right here, never in a loop.
                self._log.warning(
                    "carrier_refresh_failed",
                    reason=reason,
                    invalid_grant=exc.invalid_grant,
                    status_code=exc.status_code,
                    error=str(exc),
                )
                self._cache.clear()
                self._token = None

        if rejection_driven:
            floored = self._password_floor_remaining()
            if floored > 0:
                self._log.warning(
                    "carrier_password_grant_floored",
                    reason=reason,
                    remaining_s=round(floored, 1),
                    min_interval_s=self._min_password_grant_interval_s,
                    password_grants=self._password_grants,
                    detail=(
                        "a freshly issued token was rejected; a newer one would "
                        "be too. Not re-sending the password this cycle"
                    ),
                )
                raise CarrierAuthError(
                    f"carrier {reason}: password grant floored for another "
                    f"{floored:.0f}s after a rejected token"
                )

        try:
            return await self._password_grant(reason=reason)
        except CarrierAuthError as exc:
            # Nothing about our credentials will change in the next 30 seconds,
            # so the next cycle must not ask again (PLAN.md §6.6).
            self._grant_retry_not_before = self._monotonic() + self._grant_failure_backoff_s
            self._log.error(
                "carrier_password_grant_rejected",
                reason=reason,
                invalid_grant=exc.invalid_grant,
                status_code=exc.status_code,
                backoff_s=self._grant_failure_backoff_s,
                password_grants=self._password_grants,
                error=str(exc),
            )
            raise

    async def _refresh_grant(self, refresh_token: str, *, reason: str) -> CarrierToken:
        self._refresh_grants += 1
        payload = await self._token_request(
            {
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "refresh_token": refresh_token,
                "scope": self._scope,
            },
            op="refresh",
        )
        return self._store(payload, grant="refresh_token", reason=reason, previous_refresh=refresh_token)

    async def _password_grant(self, *, reason: str) -> CarrierToken:
        self._password_grants += 1
        # Stamped before the request, not after it: a grant that fails must
        # still close the floor, or a rejected credential would be re-sent every
        # cycle by way of the failure path.
        self._last_password_grant_monotonic = self._monotonic()
        payload = await self._token_request(
            {
                "grant_type": "password",
                "client_id": self._client_id,
                "username": self._username,
                "password": self._password,
                "scope": self._scope,
            },
            op="password",
        )
        return self._store(payload, grant="password", reason=reason, previous_refresh=None)

    async def _token_request(self, form: Mapping[str, str], *, op: str) -> Mapping[str, Any]:
        response = await self._http.send(
            "POST",
            self._token_url,
            op=op,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data=dict(form),
        )
        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                raise CarrierTransientError(
                    f"carrier {op}: token endpoint returned a non-JSON body"
                ) from exc
            if not isinstance(payload, Mapping):
                raise CarrierTransientError(
                    f"carrier {op}: token endpoint returned {type(payload).__name__}, not an object"
                )
            return payload

        code = _oauth_error_code(response)
        detail = _error_detail(response)
        message = f"carrier {op}: HTTP {response.status_code}"
        if detail:
            message = f"{message} ({detail})"
        raise CarrierAuthError(
            message,
            invalid_grant=(code == "invalid_grant"),
            status_code=response.status_code,
        )

    def _store(
        self,
        payload: Mapping[str, Any],
        *,
        grant: str,
        reason: str,
        previous_refresh: str | None,
    ) -> CarrierToken:
        """Register, date and persist a token response."""
        access = payload.get("access_token")
        if not isinstance(access, str) or not access.strip():
            raise CarrierAuthError(
                f"carrier {grant}: token response contained no access_token"
            )
        # Both secrets registered the instant they exist — before the log line
        # below, before the cache write, before any exception can be raised.
        register_secret(access)
        refresh = payload.get("refresh_token")
        if isinstance(refresh, str) and refresh.strip():
            register_secret(refresh)
        else:
            # Okta normally rotates it on every refresh; if it ever does not,
            # keeping the previous one is what lets the next renewal work.
            refresh = previous_refresh

        now = self._now()
        expires_at, source = self._expiry(access, payload.get("expires_in"), now=now)
        token = CarrierToken(
            access_token=access,
            refresh_token=refresh,
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_at=expires_at,
            obtained_at=now,
            scope=payload.get("scope") if isinstance(payload.get("scope"), str) else None,
            username=self._username,
            expiry_source=source,
        )
        self._token = token
        self._cache_loaded = True
        self._last_renew_monotonic = self._monotonic()
        # Whatever was wrong with our credentials is no longer wrong.
        self._grant_retry_not_before = None
        try:
            # Okta rotates the refresh token on every refresh: if this write is
            # skipped the cached token is already dead.
            self._cache.save(token)
        except OSError as exc:
            # A read-only /data is a real problem, but not one worth failing the
            # poll over — the in-memory token is perfectly usable.
            self._log.warning(
                "carrier_token_cache_write_failed",
                path=str(self._cache.path),
                error=str(exc),
            )
        self._log.info(
            "carrier_token_ok",
            grant=grant,
            reason=reason,
            expires_at=_iso(expires_at),
            expiry_source=source,
            refresh_rotated=bool(refresh and refresh != previous_refresh),
            password_grants=self._password_grants,
            refresh_grants=self._refresh_grants,
        )
        return token

    def _expiry(
        self, access_token: str, expires_in: Any, *, now: datetime
    ) -> tuple[datetime, str]:
        """``expires_in`` → JWT ``exp`` → an assumed short lifetime (PLAN.md §7.1)."""
        seconds: float | None = None
        if isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool):
            seconds = float(expires_in)
        elif isinstance(expires_in, str):
            try:
                seconds = float(expires_in.strip())
            except ValueError:
                seconds = None
        if seconds is not None and seconds > 0:
            return now + timedelta(seconds=seconds), "expires_in"

        exp = decode_jwt_exp(access_token)
        if exp is not None:
            return datetime.fromtimestamp(exp, tz=UTC), "jwt_exp"

        self._log.warning(
            "carrier_token_expiry_unknown",
            assumed_lifetime_s=UNKNOWN_TOKEN_LIFETIME_S,
        )
        return now + timedelta(seconds=UNKNOWN_TOKEN_LIFETIME_S), "assumed"


# ------------------------------------------------------------- graphql client


class CarrierGraphQLClient:
    """``POST https://dataservice.infinity.iot.carrier.com/graphql`` (PLAN.md §7.1).

    ``query()`` returns the GraphQL ``data`` object and raises on everything
    else. In particular a ``200 OK`` carrying an ``errors`` array is a failure,
    not data — Carrier answers an unauthorised or malformed query that way, and
    letting a partial ``data`` through would put half-populated rows in the
    archive.

    The 401 ladder, which is where "never spin" is enforced: the call is
    attempted with the current token; a rejection triggers one **refresh** grant
    and one retry; a second rejection triggers one **password** grant and one
    final retry; a third gives up. At most two token requests and three HTTP
    attempts per ``query()``, always.

    Bounded *per call* is not enough at 30 seconds, though. A rejection that a
    brand-new token does not fix — a revoked entitlement, an account that can no
    longer see this system — would otherwise cost that refresh grant and that
    password grant on **every cycle, forever**: 2,880 password grants a day
    against Carrier's Okta, which is precisely the behaviour PLAN.md §7.1 calls
    unacceptable. So exhausting the ladder opens an
    :data:`AUTH_LADDER_BACKOFF_S` window during which a call makes exactly one
    attempt with the token it already holds and then fails. Any success closes
    the window immediately.
    """

    def __init__(
        self,
        auth: CarrierAuth,
        *,
        client: httpx.AsyncClient | None = None,
        owns_client: bool | None = None,
        url: str = GRAPHQL_URL,
        retry_waits: Sequence[float] = RETRY_WAITS_S,
        auth_ladder_backoff_s: float = AUTH_LADDER_BACKOFF_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = now_utc,
    ) -> None:
        self._auth = auth
        self._url = url
        self._log = get_logger("carrier")
        self._monotonic = monotonic
        self._auth_ladder_backoff_s = float(auth_ladder_backoff_s)
        self._auth_retry_not_before: float | None = None
        self._http = _HttpSender(
            name="graphql",
            client=client,
            owns_client=owns_client,
            retry_waits=retry_waits,
            sleep=sleep,
            monotonic=monotonic,
            now=now,
            log=self._log,
        )

    # ------------------------------------------------------------- accessors
    @property
    def auth(self) -> CarrierAuth:
        return self._auth

    @property
    def throttle(self) -> ThrottleState:
        """Rate-limit state of the GraphQL endpoint (PLAN.md §7.3)."""
        return self._http.throttle

    def status_fields(self) -> dict[str, Any]:
        """Everything a caller should put in ``status.json`` for Bryant.

        Counters, timestamps and the effective backoff — never a credential, so
        this can be logged or serialised without further thought.
        """
        fields = self._http.throttle.status_fields(self._monotonic())
        fields.update(self._auth.status_fields())
        return fields

    async def close(self) -> None:
        """Close both pools. Idempotent."""
        await self._http.close()
        await self._auth.close()

    # ---------------------------------------------------------------- query
    async def query(
        self,
        query: str,
        *,
        variables: Mapping[str, Any] | None = None,
        operation_name: str | None = None,
        op: str | None = None,
    ) -> dict[str, Any]:
        """Execute one GraphQL operation and return its ``data`` object.

        Raises :class:`CarrierGraphQLError` for an ``errors`` payload,
        :class:`CarrierAuthError` when re-authentication cannot fix a rejection,
        :class:`CarrierRateLimitError` when throttled, and
        :class:`CarrierTransientError` for 5xx/network failures that survive the
        in-call retries. Callers catch ``SourceTransientError`` /
        ``SourceAuthError`` and emit no rows — never a fabricated one.
        """
        label = op or operation_name or "graphql"
        body: dict[str, Any] = {"query": query}
        if variables is not None:
            body["variables"] = dict(variables)
        if operation_name:
            body["operationName"] = operation_name

        # None = use the token we have; then one refresh; then one password
        # grant. A fixed-length ladder is what makes spinning impossible.
        ladder: tuple[str | None, ...] = (None, "refresh", "password")
        blocked = self._auth_backoff_remaining()
        if blocked > 0:
            # A token minted seconds ago was rejected, and nothing has changed
            # since. Climbing again would buy two more tokens for this cycle to
            # be rejected with; one attempt and a gap is the honest answer.
            ladder = (None,)
            self._log.warning(
                "carrier_auth_ladder_backoff",
                op=label,
                remaining_s=round(blocked, 1),
                backoff_s=self._auth_ladder_backoff_s,
                detail="a freshly issued token was rejected; not re-authenticating",
            )

        token = await self._auth.token()
        last: CarrierAuthError | None = None

        for step in ladder:
            if step is not None:
                self._log.info("carrier_reauth", op=label, step=step)
                try:
                    token = await self._auth.reauthenticate(
                        stale_token=token.access_token,
                        allow_refresh=(step == "refresh"),
                        reason=f"{label}_{step}",
                    )
                except CarrierAuthError as exc:
                    # The grant was rejected, or one of its anti-spin floors is
                    # closed. Either way there is no further rung to climb.
                    last = exc
                    break
            try:
                data = await self._attempt(body, token, op=label)
            except CarrierAuthError as exc:
                last = exc
                continue
            self._auth_retry_not_before = None
            return data

        # Unreachable unless the ladder ran out, which only happens via
        # CarrierAuthError — every other exception left the loop already.
        if last is None:  # pragma: no cover - defensive
            raise CarrierAuthError(f"carrier {label}: authentication ladder exhausted")
        climbed = len(ladder) > 1
        if climbed:
            # We minted a token moments ago and it was rejected too. Stop
            # climbing until the window closes; re-arming on every subsequent
            # failure would mean never climbing again.
            self._auth_retry_not_before = self._monotonic() + self._auth_ladder_backoff_s
        self._log.error(
            "carrier_auth_exhausted",
            op=label,
            error=str(last),
            climbed=climbed,
            backoff_s=self._auth_ladder_backoff_s if climbed else 0.0,
        )
        raise last

    def _auth_backoff_remaining(self) -> float:
        """Seconds left before the full auth ladder may be climbed again."""
        if self._auth_retry_not_before is None:
            return 0.0
        remaining = self._auth_retry_not_before - self._monotonic()
        if remaining <= 0:
            self._auth_retry_not_before = None
            return 0.0
        return remaining

    async def _attempt(
        self, body: Mapping[str, Any], token: CarrierToken, *, op: str
    ) -> dict[str, Any]:
        response = await self._http.send(
            "POST",
            self._url,
            op=op,
            headers={
                "Authorization": token.authorization,
                "Content-Type": "application/json",
                "Accept": "application/json",
                # PLAN.md §7.1: the old repo's working code spoofs the SPA.
                "Origin": SPA_ORIGIN,
                "Referer": SPA_REFERER,
                "Mobile-App-Brand": MOBILE_APP_BRAND,
            },
            json_body=dict(body),
        )

        if response.status_code in AUTH_HTTP_STATUSES:
            detail = _error_detail(response)
            raise CarrierAuthError(
                f"carrier {op}: HTTP {response.status_code}" + (f" ({detail})" if detail else ""),
                status_code=response.status_code,
            )
        if response.status_code != 200:
            detail = _error_detail(response)
            raise CarrierTransientError(
                f"carrier {op}: HTTP {response.status_code}" + (f" ({detail})" if detail else "")
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise CarrierTransientError(f"carrier {op}: response was not JSON") from exc
        if not isinstance(payload, Mapping):
            raise CarrierTransientError(
                f"carrier {op}: response was {type(payload).__name__}, not an object"
            )

        errors = payload.get("errors")
        if errors:
            error_list = list(errors) if isinstance(errors, (list, tuple)) else [errors]
            if _errors_are_auth(error_list):
                # 200-with-UNAUTHENTICATED: same meaning as a 401, so it enters
                # the same ladder rather than being reported as a data error.
                # The array travels with the exception because "not authorized
                # to access field X" and "your token is dead" are the same
                # exception type but not the same problem, and only the caller
                # knows whether a different query would work.
                raise CarrierAuthError(
                    f"carrier {op}: GraphQL auth error ({_errors_summary(error_list)})",
                    errors=error_list,
                )
            self._log.warning(
                "carrier_graphql_errors",
                op=op,
                count=len(error_list),
                error=_errors_summary(error_list),
                # A partial `data` is discarded on purpose (CLAUDE.md rule 1).
                had_partial_data=payload.get("data") is not None,
            )
            raise CarrierGraphQLError(
                f"carrier {op}: GraphQL errors ({_errors_summary(error_list)})",
                errors=error_list,
            )

        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise CarrierGraphQLError(
                f"carrier {op}: response had no data object"
            )
        return dict(data)


def _errors_summary(errors: Sequence[Any]) -> str:
    """A short, scrubbed rendering of a GraphQL ``errors`` array."""
    messages: list[str] = []
    for item in errors:
        if isinstance(item, Mapping):
            message = item.get("message")
            messages.append(str(message) if message is not None else json.dumps(dict(item))[:80])
        else:
            messages.append(str(item))
    return scrub_text("; ".join(messages))[:_MAX_ERROR_DETAIL]


def _errors_are_auth(errors: Sequence[Any]) -> bool:
    """True when a 200-with-``errors`` payload actually means "your token is bad"."""
    for item in errors:
        if not isinstance(item, Mapping):
            continue
        extensions = item.get("extensions")
        if isinstance(extensions, Mapping):
            code = extensions.get("code")
            if isinstance(code, str) and code.strip().upper() in GRAPHQL_AUTH_CODES:
                return True
        message = item.get("message")
        if isinstance(message, str):
            lowered = message.lower()
            if any(phrase in lowered for phrase in _GRAPHQL_AUTH_PHRASES):
                return True
    return False


# ------------------------------------------------------------------ factories


def auth_from_settings(
    settings: Settings | None = None, *, client: httpx.AsyncClient | None = None
) -> CarrierAuth:
    """Build :class:`CarrierAuth` from the environment.

    Credentials are demanded here, at the point of use, so importing this module
    never requires an environment (``Settings.require`` names the missing var).
    """
    resolved = settings if settings is not None else get_settings()
    return CarrierAuth(
        username=resolved.require("carrier_username"),
        password=resolved.require("carrier_password"),
        token_path=resolved.carrier_token_path,
        client=client,
        owns_client=False if client is not None else None,
    )


def carrier_stack_from_settings(
    settings: Settings | None = None,
) -> tuple[CarrierAuth, CarrierGraphQLClient]:
    """One ``httpx.AsyncClient`` shared by the token and GraphQL endpoints.

    The GraphQL client owns the pool; ``await client.close()`` releases both it
    and the auth manager. Returning the pair lets the daily energy stage and the
    status poller share a single token manager if they want to.
    """
    shared = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
    auth = auth_from_settings(settings, client=shared)
    client = CarrierGraphQLClient(auth, client=shared, owns_client=True)
    return auth, client


def graphql_client_from_settings(settings: Settings | None = None) -> CarrierGraphQLClient:
    """The one-liner both Bryant paths use: a ready GraphQL client."""
    return carrier_stack_from_settings(settings)[1]
