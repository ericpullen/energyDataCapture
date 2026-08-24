"""OAuth2 for LG&E Green Button Connect (NAESB REQ.21 ESPI).

Registered and approved 2026-08-18; every endpoint and credential here is
configuration (``docs/lge-greenbutton.md`` §3c). Deliberately much smaller than
:mod:`energy_capture.sources.carrier_auth`, because the shape of the problem is
different:

* **There is no password grant.** A human authorises once, in a browser, on the
  utility's site. If the refresh token dies the fix is a person clicking a link
  — never a credential this process holds. So there is no re-authentication
  ladder to get wrong, and no way for a bug here to hammer the utility with
  logins.
* **The access token is short-lived and the refresh token is the real asset.**
  Losing the refresh token costs a manual re-authorisation, so the cache is
  written on *every* renewal, before the new access token is used, on the
  assumption the custodian may rotate it.

Three things this will not do:

* **Log a token.** Every token obtained is handed to
  :func:`~energy_capture.logging.register_secret`, and both token fields are
  redacted from ``__repr__`` — this object lives in local variables that a
  traceback would otherwise render (CLAUDE.md rule 8).
* **Refresh forever against a rejection.** A refresh grant the server rejects
  raises :class:`LgeAuthError` rather than being retried; retrying a token the
  custodian has revoked is how an integration gets its registration disabled.
  Whether the cache is *cleared* depends on the RFC 6749 §5.2 error code:
  ``invalid_grant``/``invalid_scope`` mean the customer's authorisation is over
  and the token is dropped, while ``invalid_client`` and its neighbours mean our
  own credentials or request are wrong and the token is KEPT — see
  :data:`REAUTH_OAUTH_ERRORS` and :data:`CONFIG_OAUTH_ERRORS`.
* **Guess the resource base.** ESPI's token response carries ``resourceURI`` —
  the Subscription the customer actually authorised. That is stored with the
  token and preferred over the configured base, because only the custodian knows
  which subscription id belongs to this authorisation.
"""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlencode

import httpx

from energy_capture.config import describe_env_source, get_settings
from energy_capture.logging import get_logger, register_secret
from energy_capture.sources.base import SourceAuthError, SourceTransientError

log = get_logger("lge_auth")

#: Renew this far before the stated expiry. The fetch runs once a day, so the
#: only cost of being early is a refresh grant nobody notices.
REFRESH_MARGIN_S: Final[float] = 300.0

#: Refresh once the token is inside its final third, not its final five minutes.
#:
#: LG&E issues a **24-hour** access token, and `greenbutton_daily` fires once a
#: day. With a flat 300s margin the job essentially never lands inside the
#: refresh window, so it finds the token already dead and refreshes *reactively*
#: -- which means the refresh token sits unused for nearly two days at a stretch.
#: The 2026-08-20 lapse is consistent with that: the grant was rejected and
#: `lge_auth` correctly dropped it (see `LgeTokenCache.clear`), and the most
#: likely reason a refresh token goes stale is not being exercised.
#:
#: A fraction of the token's own lifetime scales to whatever the custodian
#: issues: 8h for a 24h token (so a daily job always refreshes), still 300s for
#: anything under ~15 minutes. Refreshing early is cheap and idempotent; the
#: rotation is persisted before use either way.
REFRESH_FRACTION: Final[float] = 1.0 / 3.0

#: Assumed lifetime when the custodian states none. Short on purpose: guessing
#: long means using a dead token and reporting a confusing 401.
UNKNOWN_TOKEN_LIFETIME_S: Final[float] = 900.0

DEFAULT_TIMEOUT: Final[httpx.Timeout] = httpx.Timeout(30.0, connect=10.0)

#: Statuses that mean "this credential is finished", not "try again later".
AUTH_HTTP_STATUSES: Final[frozenset[int]] = frozenset({400, 401, 403})

#: RFC 6749 §5.2 error codes on a REFRESH grant that mean the authorisation
#: itself is over. Only these clear the token cache.
#:
#: ``invalid_grant`` — the refresh token is expired, revoked, or was already
#: used. ``invalid_scope`` — the scope we ask for is no longer covered by what
#: the resource owner granted, which is what LG&E returned on 2026-08-24
#: ("The requested scope does not match the scope granted by the resource
#: owner"). Both need a human and a browser; keeping the token would only
#: re-present a credential the custodian has finished with.
REAUTH_OAUTH_ERRORS: Final[frozenset[str]] = frozenset({"invalid_grant", "invalid_scope"})

#: RFC 6749 §5.2 codes that are about OUR request or OUR client credentials,
#: not about the customer's authorisation.
#:
#: These must NOT clear the cache, and that is the whole point of classifying at
#: all. Every 400/401/403 used to be treated as a revocation, so a mistyped
#: ``LGE_CLIENT_SECRET`` or a registration the custodian had disabled on their
#: side destroyed a perfectly good refresh token — turning a config error that
#: an operator could fix in ten seconds into a trip to a browser to re-consent.
#: The failure is still raised loudly; only the destruction is withheld.
CONFIG_OAUTH_ERRORS: Final[frozenset[str]] = frozenset(
    {
        "invalid_client",
        "unauthorized_client",
        "unsupported_grant_type",
        "invalid_request",
    }
)

__all__ = [
    "CONFIG_OAUTH_ERRORS",
    "REAUTH_OAUTH_ERRORS",
    "LgeAuth",
    "LgeAuthError",
    "LgeToken",
    "LgeTokenCache",
    "LgeTransientError",
    "authorization_url",
    "new_state",
]


class LgeError(Exception):
    """Base for Green Button Connect failures."""


class LgeAuthError(LgeError, SourceAuthError):
    """The credential is finished — a human must re-authorise."""


class LgeTransientError(LgeError, SourceTransientError):
    """The custodian was unreachable or unwell. Try again later."""


# ------------------------------------------------------------------- token


@dataclass
class LgeToken:
    """One token response from ``/OAuthServer/token``."""

    access_token: str
    refresh_token: str | None = None
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    obtained_at: datetime | None = None
    scope: str | None = None
    #: ESPI ``resourceURI`` — the authorised Subscription. Not a secret.
    resource_uri: str | None = None
    #: ESPI ``authorizationURI`` — where this authorisation is described.
    authorization_uri: str | None = None
    #: Which client obtained it, so rotating ``LGE_CLIENT_ID`` cannot silently
    #: reuse another registration's token.
    client_id: str | None = None

    @property
    def authorization(self) -> str:
        return f"{self.token_type or 'Bearer'} {self.access_token}"

    def seconds_remaining(self, now: datetime) -> float | None:
        if self.expires_at is None:
            return None
        return (self.expires_at - now).total_seconds()

    def lifetime_s(self) -> float | None:
        """Total issued lifetime, or ``None`` when the custodian stated neither end."""
        if self.expires_at is None or self.obtained_at is None:
            return None
        span = (self.expires_at - self.obtained_at).total_seconds()
        return span if span > 0 else None

    def refresh_threshold_s(self) -> float:
        """Seconds-remaining at or below which this token should be refreshed.

        :data:`REFRESH_FRACTION` of the issued lifetime, floored at
        :data:`REFRESH_MARGIN_S` so a very short token is not refreshed on every
        single call.
        """
        lifetime = self.lifetime_s()
        if lifetime is None:
            return REFRESH_MARGIN_S
        return max(REFRESH_MARGIN_S, lifetime * REFRESH_FRACTION)

    def is_fresh(self, now: datetime, *, margin: float | None = None) -> bool:
        """Whether the token is good enough to use without refreshing first.

        ``margin`` overrides the computed threshold; tests use it to pin exact
        boundaries. Left unset -- which is every production caller -- the
        threshold scales with the token's own lifetime
        (:meth:`refresh_threshold_s`).
        """
        remaining = self.seconds_remaining(now)
        if remaining is None:
            return True
        threshold = self.refresh_threshold_s() if margin is None else margin
        return remaining > threshold

    def to_payload(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_at": _iso(self.expires_at),
            "obtained_at": _iso(self.obtained_at),
            "scope": self.scope,
            "resource_uri": self.resource_uri,
            "authorization_uri": self.authorization_uri,
            "client_id": self.client_id,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> LgeToken | None:
        access = payload.get("access_token")
        if not isinstance(access, str) or not access:
            return None
        return cls(
            access_token=access,
            refresh_token=payload.get("refresh_token") or None,
            token_type=payload.get("token_type") or "Bearer",
            expires_at=_parse_iso(payload.get("expires_at")),
            obtained_at=_parse_iso(payload.get("obtained_at")),
            scope=payload.get("scope"),
            resource_uri=payload.get("resource_uri"),
            authorization_uri=payload.get("authorization_uri"),
            client_id=payload.get("client_id"),
        )

    @classmethod
    def from_response(
        cls, payload: dict[str, Any], *, client_id: str, now: datetime
    ) -> LgeToken:
        """Build from a token endpoint response, tolerating ESPI's casing.

        ESPI spells the two resource fields ``resourceURI`` and
        ``authorizationURI``; OAuth spells everything else snake_case, and
        implementations mix them. Both spellings are accepted rather than
        insisting on one and silently dropping the Subscription URI — which is
        the only thing that says *what* to fetch.
        """
        expires_in = payload.get("expires_in")
        try:
            lifetime = float(expires_in) if expires_in is not None else UNKNOWN_TOKEN_LIFETIME_S
        except (TypeError, ValueError):
            lifetime = UNKNOWN_TOKEN_LIFETIME_S
        token = cls(
            access_token=str(payload["access_token"]),
            refresh_token=payload.get("refresh_token") or None,
            token_type=payload.get("token_type") or "Bearer",
            expires_at=now + timedelta(seconds=lifetime),
            obtained_at=now,
            scope=payload.get("scope"),
            resource_uri=_first(payload, "resourceURI", "resource_uri", "ResourceURI"),
            authorization_uri=_first(
                payload, "authorizationURI", "authorization_uri", "AuthorizationURI"
            ),
            client_id=client_id,
        )
        register_secret(token.access_token)
        register_secret(token.refresh_token)
        return token

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"LgeToken(access_token=<redacted>, refresh_token="
            f"{'<redacted>' if self.refresh_token else 'None'}, "
            f"expires_at={_iso(self.expires_at)!r}, scope={self.scope!r})"
        )


def _first(payload: dict[str, Any], *names: str) -> str | None:
    for name in names:
        value = payload.get(name)
        if isinstance(value, str) and value:
            return value
    return None


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


# ------------------------------------------------------------------- cache


@dataclass
class LgeTokenCache:
    """``{SPOOL_DIR}/tokens/lge.json``, mode ``0600``.

    Also holds the pending ``state`` between building an authorisation URL and
    the customer coming back with a code, so the callback can reject a code that
    did not originate here.
    """

    path: Path

    def load(self, *, client_id: str | None = None) -> LgeToken | None:
        payload = self._read()
        if payload is None:
            return None
        token = LgeToken.from_payload(payload.get("token") or {})
        if token is None:
            return None
        if client_id and token.client_id and token.client_id != client_id:
            log.warning("lge_token_client_mismatch", cached=token.client_id)
            return None
        register_secret(token.access_token)
        register_secret(token.refresh_token)
        self._chmod()
        return token

    def save(self, token: LgeToken, *, state: str | None = None) -> None:
        payload = self._read() or {}
        payload["token"] = token.to_payload()
        if state is not None:
            payload["pending_state"] = state
        self._write(payload)
        # Re-authorising (or a successful refresh) is the cure, so it retires the
        # breadcrumb. Otherwise a single old revocation would shout forever.
        self.revoked_path.unlink(missing_ok=True)

    def save_state(self, state: str) -> None:
        payload = self._read() or {}
        payload["pending_state"] = state
        self._write(payload)

    def take_state(self) -> str | None:
        """Read and clear the pending state — it is single-use."""
        payload = self._read()
        if not payload:
            return None
        state = payload.pop("pending_state", None)
        self._write(payload)
        return state if isinstance(state, str) else None

    @property
    def revoked_path(self) -> Path:
        """Breadcrumb marking that a credential was REVOKED, not merely absent.

        Sits beside the token cache and deliberately outlives it.
        """
        return self.path.with_name(f"{self.path.stem}-revoked.json")

    def clear(self, reason: str = "the custodian rejected the credential") -> None:
        """A token the custodian rejected is worse than no token at all.

        Deleting the token is right, but deleting it *silently* made a revoked
        authorisation indistinguishable from one that was never set up — and
        ``_job_greenbutton_daily`` skips the never-set-up case quietly on purpose,
        so a revocation vanished into that same silence for three days
        (DEVIATIONS.md #177). So the deletion leaves a breadcrumb.

        The marker holds **no credential material** — just when and why.
        """
        existed = self.path.exists()
        self.path.unlink(missing_ok=True)
        if not existed:
            return
        try:
            self.revoked_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(
                prefix=".lge-revoked-", suffix=".json.tmp", dir=str(self.revoked_path.parent)
            )
            tmp = Path(tmp_name)
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(
                    {"revoked_at": _iso(datetime.now(UTC)), "reason": reason},
                    handle,
                    sort_keys=True,
                )
            os.replace(tmp, self.revoked_path)
        except OSError as exc:
            # Losing the breadcrumb must never turn a handled rejection into a
            # crash; the raised LgeAuthError still carries the detail.
            log.warning("lge_revocation_marker_unwritable", error=str(exc))
        log.error("lge_authorization_revoked", reason=reason, marker=str(self.revoked_path))

    def revoked(self) -> dict[str, Any] | None:
        """The revocation breadcrumb, if this deployment ever had one."""
        try:
            payload = json.loads(self.revoked_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    # ------------------------------------------------------------- internals
    def _read(self) -> dict[str, Any] | None:
        try:
            raw = self.path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def _write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".lge-", suffix=".json.tmp", dir=str(self.path.parent)
        )
        tmp = Path(tmp_name)
        try:
            # chmod before writing: the refresh token must never exist on disk,
            # even for an instant, under the process umask.
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        self._chmod()

    def _chmod(self) -> None:
        try:
            os.chmod(self.path, 0o600)
        except OSError:  # pragma: no cover - unusual filesystem
            pass


# -------------------------------------------------------- the authorisation


def new_state() -> str:
    """A single-use CSRF state for the authorisation round trip."""
    return secrets.token_urlsafe(24)


def authorization_url(*, state: str, settings: Any | None = None) -> str:
    """Where to send the customer to authorise this application.

    ``redirect_uri`` is included explicitly even though the custodian has it on
    file: it is compared by exact string match, and sending it makes a mismatch
    fail loudly here rather than silently redirecting somewhere unexpected.
    """
    s = settings or get_settings()
    if not s.lge_client_id:
        raise LgeAuthError("LGE_CLIENT_ID is not configured")
    query = urlencode(
        {
            "client_id": s.lge_client_id,
            "redirect_uri": s.lge_redirect_uri,
            "response_type": "code",
            "scope": s.lge_scope,
            "state": state,
        }
    )
    return f"{s.lge_authorize_url}?{query}"


# --------------------------------------------------------------- the client


@dataclass
class LgeAuth:
    """Obtains and renews the customer's access token."""

    settings: Any = field(default_factory=get_settings)
    cache: LgeTokenCache | None = None
    client: httpx.Client | None = None

    def __post_init__(self) -> None:
        if self.cache is None:
            self.cache = LgeTokenCache(self.settings.spool_dir / "tokens" / "lge.json")

    # ----------------------------------------------------------- public API
    def start(self) -> tuple[str, str]:
        """Return ``(url, state)`` and remember the state for the callback."""
        state = new_state()
        assert self.cache is not None
        self.cache.save_state(state)
        return authorization_url(state=state, settings=self.settings), state

    def exchange_code(self, code: str, *, state: str | None = None) -> LgeToken:
        """Trade an authorisation code for tokens, and cache them.

        The state check is deliberately a *warning* rather than a refusal. The
        expected state lives in a cache file that a container restart between
        building the URL and clicking through will have replaced; refusing then
        would strand a code that expires in minutes, and the code itself is
        single-use, bound to this client and delivered over TLS. A mismatch is
        worth seeing, not worth failing on.
        """
        assert self.cache is not None
        expected = self.cache.take_state()
        if state and expected and state != expected:
            log.warning("lge_state_mismatch")

        token = self._token_request(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.settings.lge_redirect_uri,
            },
            what="authorization_code",
        )
        self.cache.save(token)
        log.info(
            "lge_authorized",
            scope=token.scope,
            resource_uri=token.resource_uri,
            expires_at=_iso(token.expires_at),
            has_refresh_token=bool(token.refresh_token),
        )
        return token

    def access_token(self, *, now: datetime | None = None) -> LgeToken:
        """A usable token, refreshing first if it is close to expiry."""
        assert self.cache is not None
        moment = now or datetime.now(UTC)
        token = self.cache.load(client_id=self.settings.lge_client_id)
        if token is None:
            raise LgeAuthError(
                "no LG&E token cached — authorise first: "
                "`energycap greenbutton-authorize`"
            )
        if token.is_fresh(moment):
            return token
        if not token.refresh_token:
            raise LgeAuthError(
                "the cached LG&E token has expired and carries no refresh token "
                "— re-authorise with `energycap greenbutton-authorize`"
            )
        return self.refresh(token, now=moment)

    def refresh(self, token: LgeToken, *, now: datetime | None = None) -> LgeToken:
        """Exchange the refresh token. Cached before use, rotation assumed.

        **`scope` is sent, and must be.** RFC 6749 6 makes it optional and says
        an omitted scope means "the originally granted scope" -- so this sent
        nothing, which is correct against the spec and does not work here. This
        custodian appears to compare the *client registration's* scope instead,
        and answers a scope-less refresh with:

            400 invalid_scope -- "The requested scope does not match the scope
            granted by the resource owner."

        Measured 2026-08-24 against the live endpoint on a token four minutes
        old, whose granted scope was string-identical to the configured one:
        without `scope`, 400; with it, 200 and a rotated refresh token. So every
        refresh this integration has ever attempted failed, the cache was
        cleared each time, and the only thing that ever produced a working token
        was a human in a browser. That is the whole of "the tokens do not
        auto-update".

        The token's OWN granted scope is sent, not the configured one: they are
        the same today, and if they ever diverge the grant is the truth and
        `LGE_SCOPE` is a stale local guess.
        """
        assert self.cache is not None
        data = {
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token or "",
        }
        scope = token.scope or self.settings.lge_scope
        if scope:
            data["scope"] = scope
        refreshed = self._token_request(data, what="refresh_token")
        # The custodian may or may not rotate; carry the old one forward only if
        # it did not send a new one, so a rotation is never lost.
        if not refreshed.refresh_token:
            refreshed.refresh_token = token.refresh_token
        if not refreshed.resource_uri:
            refreshed.resource_uri = token.resource_uri
        self.cache.save(refreshed)
        log.info(
            "lge_token_refreshed",
            expires_at=_iso(refreshed.expires_at),
            rotated=refreshed.refresh_token != token.refresh_token,
        )
        return refreshed

    def resource_base(self, token: LgeToken) -> str:
        """Where to fetch from: the authorised Subscription, else the config."""
        return (token.resource_uri or self.settings.lge_resource_uri).rstrip("/")

    # ---------------------------------------------------------- the request
    def _token_request(self, data: dict[str, str], *, what: str) -> LgeToken:
        s = self.settings
        secret = s.lge_client_secret.get_secret_value()
        if not s.lge_client_id or not secret:
            # Naming *which* one is missing and where the settings came from:
            # the first version of this said only "not configured", and the
            # actual cause was a working directory with no `.env` above it,
            # which that message gave an operator no way to guess.
            missing = [
                name
                for name, value in (
                    ("LGE_CLIENT_ID", s.lge_client_id),
                    ("LGE_CLIENT_SECRET", secret),
                )
                if not value
            ]
            raise LgeAuthError(
                f"{' and '.join(missing)} not set. Settings were read from "
                f"{describe_env_source()}. Run this from inside the repository "
                "(or the container), where the .env holding the LG&E "
                "credentials can be found."
            )

        client = self.client or httpx.Client(timeout=DEFAULT_TIMEOUT)
        owned = self.client is None
        try:
            response = client.post(
                s.lge_token_url,
                data=data,
                # `client_secret_basic`, which the custodian enforces: sending
                # the credentials in the body instead gets a bare 401.
                auth=httpx.BasicAuth(s.lge_client_id, secret),
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise LgeTransientError(f"LG&E token endpoint unreachable: {exc}") from exc
        finally:
            if owned:
                client.close()

        if response.status_code in AUTH_HTTP_STATUSES:
            code = _oauth_error(response)
            # Not every rejection is a revocation, and treating them alike is
            # destructive in one direction only. `invalid_grant`/`invalid_scope`
            # mean the customer's authorisation is over and the token is so much
            # dead weight. `invalid_client` and friends mean OUR request or OUR
            # credentials are wrong while their authorisation is untouched — and
            # clearing the cache there throws away a working refresh token over
            # a typo, which no amount of fixing the typo gets back.
            if what == "refresh_token" and code not in CONFIG_OAUTH_ERRORS:
                assert self.cache is not None
                if code in REAUTH_OAUTH_ERRORS or code is None:
                    self.cache.clear(
                        f"LG&E rejected the refresh_token grant with HTTP "
                        f"{response.status_code}"
                        + (f" ({code})" if code else "")
                    )
            if code in CONFIG_OAUTH_ERRORS:
                raise LgeAuthError(
                    f"LG&E rejected the {what} grant ({response.status_code} "
                    f"{code}): {_detail(response)}. This is about THIS client, "
                    "not the customer's consent — check LGE_CLIENT_ID / "
                    "LGE_CLIENT_SECRET and that the registration is still "
                    "active. The cached token was KEPT: it is probably still "
                    "good, and re-authorising would not fix a credential "
                    "problem."
                )
            raise LgeAuthError(
                f"LG&E rejected the {what} grant "
                f"({response.status_code}{f' {code}' if code else ''}): "
                f"{_detail(response)}. "
                "Re-authorise with `energycap greenbutton-authorize`."
            )
        if response.status_code >= 500:
            raise LgeTransientError(
                f"LG&E token endpoint returned {response.status_code}"
            )
        if response.status_code != 200:
            raise LgeTransientError(
                f"unexpected {response.status_code} from the LG&E token endpoint"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise LgeTransientError("LG&E token response was not JSON") from exc
        if not isinstance(payload, dict) or not payload.get("access_token"):
            raise LgeAuthError("LG&E token response carried no access_token")
        return LgeToken.from_response(
            payload, client_id=s.lge_client_id, now=datetime.now(UTC)
        )


def _oauth_error(response: httpx.Response) -> str | None:
    """The RFC 6749 §5.2 ``error`` code, or ``None`` if the body does not give one.

    ``None`` is deliberately treated as "assume the authorisation is over" by the
    caller: an unrecognised rejection from the custodian is more likely to be a
    dead grant than a config slip, and the recovery (re-authorise) is available
    where the alternative (a silently dead feed) is not.
    """
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    # RFC 6749 5.2 puts the code in `error`. This custodian answers in RFC 9457
    # problem+json and puts it in `title`:
    #
    #   {"type": "https://tools.ietf.org/html/rfc9110#section-15.5.1",
    #    "title": "invalid_scope", "status": 400, "detail": "...", "traceId": ...}
    #
    # Reading only `error` meant the classification added for DEVIATIONS #194
    # never saw a code from LG&E at all -- every rejection fell through to the
    # unrecognised branch and cleared the cache, including the `invalid_client`
    # case that classification exists to protect. Checked against the live
    # endpoint 2026-08-24.
    for key in ("error", "title"):
        code = payload.get(key)
        if isinstance(code, str) and code.strip():
            return code.strip()
    return None


def _detail(response: httpx.Response) -> str:
    """A short, scrubbed excerpt of an error body for the log line."""
    from energy_capture.logging import scrub_text

    return scrub_text((response.text or "").strip().replace("\n", " ")[:240]) or "<empty>"
