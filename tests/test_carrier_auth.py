"""Carrier Okta auth + GraphQL transport (PLAN.md §7.1).

Everything here runs offline against ``httpx.MockTransport``: no socket is
opened, no credential is real, and the clock is injected so a test that exercises
a 429 backoff finishes instantly.

The properties these tests exist to pin, in the order PLAN.md §7.1 states them:

* a cached, still-valid token is reused **without any HTTP call** — the whole
  point of the module (the old collector re-authenticated every run);
* an expired token renews with ``grant_type=refresh_token``, and the password
  grant runs **only** after a refresh has actually failed, exactly once;
* concurrent callers (the 30s poller and the 08:30 daily job) cause exactly one
  token request, never a stampede;
* the token file is ``0600`` and is rewritten on every renewal, because Okta
  rotates the refresh token;
* a GraphQL ``200 OK`` carrying an ``errors`` array is a failure, not data;
* a 401 costs one refresh and one retry, then one password grant and one retry,
  then stops — it never spins;
* ``Retry-After`` is honoured and the effective backoff is exposed for
  ``status.json``;
* JWT ``exp`` decoding survives a missing ``expires_in``, bad padding and
  outright garbage without raising;
* **no token string ever reaches a log line, an exception message or
  ``status.json``.**
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from energy_capture import logging as ec_logging
from energy_capture.config import Settings
from energy_capture.sources import carrier_auth as ca
from energy_capture.sources.base import SourceAuthError, SourceTransientError
from energy_capture.timeutil import UTC

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

USERNAME = "carrier-user@example.invalid"
PASSWORD = "not-a-real-carrier-password"
NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)

#: Every test uses its own token literals. ``register_secret`` is process-global
#: and never forgets, so a shared literal would start getting redacted out of a
#: later test's assertions.
def tok(name: str) -> str:
    return f"carrier-{name}-0123456789abcdef"


# --------------------------------------------------------------------------
# Scripted endpoints
# --------------------------------------------------------------------------


Responder = Callable[[httpx.Request], httpx.Response]


@dataclass
class Endpoint:
    """One scripted host: a queue of responders and the requests it received.

    The **last** queued responder is sticky — it answers every further request —
    so a test only has to script the transitions it cares about.
    """

    name: str
    queue: list[Responder] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    def push(self, *responders: Responder) -> None:
        self.queue.extend(responders)

    def answer(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self.queue:
            raise AssertionError(f"unscripted {self.name} request: {request.url}")
        responder = self.queue.pop(0) if len(self.queue) > 1 else self.queue[0]
        return responder(request)

    @property
    def count(self) -> int:
        return len(self.requests)

    def form(self, index: int = -1) -> dict[str, str]:
        """The urlencoded body of a recorded request, flattened."""
        raw = self.requests[index].content.decode("utf-8")
        return {k: v[0] for k, v in parse_qs(raw).items()}

    def body(self, index: int = -1) -> dict[str, Any]:
        return json.loads(self.requests[index].content.decode("utf-8"))

    def grants(self) -> list[str]:
        return [self.form(i).get("grant_type", "") for i in range(self.count)]


class FakeCarrier:
    """Okta + GraphQL, both over one ``httpx.MockTransport`` handler."""

    def __init__(self) -> None:
        self.okta = Endpoint("okta")
        self.graphql = Endpoint("graphql")

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "sso.carrier.com":
            return self.okta.answer(request)
        if request.url.host == "dataservice.infinity.iot.carrier.com":
            return self.graphql.answer(request)
        raise AssertionError(f"unexpected host {request.url.host}")

    @property
    def total_requests(self) -> int:
        return self.okta.count + self.graphql.count


# ------------------------------------------------------------------ responders


def token_ok(
    access: str,
    *,
    refresh: str | None = "refresh-token-aaaaaaaaaaaa",
    expires_in: Any = 3600,
    token_type: str = "Bearer",
    scope: str = ca.OAUTH_SCOPE,
) -> Responder:
    payload: dict[str, Any] = {"access_token": access, "token_type": token_type, "scope": scope}
    if refresh is not None:
        payload["refresh_token"] = refresh
    if expires_in is not None:
        payload["expires_in"] = expires_in

    def build(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=dict(payload))

    return build


def oauth_error(status: int = 400, error: str = "invalid_grant", description: str = "bad grant") -> Responder:
    def build(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": error, "error_description": description})

    return build


def status_only(status: int, *, headers: dict[str, str] | None = None, text: str = "") -> Responder:
    def build(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, headers=headers or {}, text=text)

    return build


def gql_ok(data: dict[str, Any]) -> Responder:
    def build(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": dict(data)})

    return build


def gql_errors(*errors: dict[str, Any], data: Any = None) -> Responder:
    def build(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": data, "errors": [dict(e) for e in errors]})

    return build


def network_error(message: str = "connection reset") -> Responder:
    def build(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(message, request=request)

    return build


# --------------------------------------------------------------------------
# Injected clock
# --------------------------------------------------------------------------


class Clock:
    """Wall clock + monotonic + ``sleep``, all fake and all in lockstep."""

    def __init__(self, start: datetime = NOW) -> None:
        self.utc = start
        self.mono = 1_000.0
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.utc

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.utc += timedelta(seconds=seconds)
        self.mono += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)


# --------------------------------------------------------------------------
# Fixtures / builders
# --------------------------------------------------------------------------


@pytest.fixture
def carrier() -> FakeCarrier:
    return FakeCarrier()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
async def http_clients() -> Any:
    created: list[httpx.AsyncClient] = []
    yield created
    for client in created:
        await client.aclose()


@pytest.fixture
def token_path(spool_dir: Path) -> Path:
    return spool_dir / "tokens" / "carrier.json"


def _http(carrier: FakeCarrier, registry: list[httpx.AsyncClient]) -> httpx.AsyncClient:
    client = httpx.AsyncClient(transport=httpx.MockTransport(carrier.handler))
    registry.append(client)
    return client


def make_auth(
    carrier: FakeCarrier,
    clock: Clock,
    token_path: Path,
    registry: list[httpx.AsyncClient],
    **kwargs: Any,
) -> ca.CarrierAuth:
    return ca.CarrierAuth(
        username=kwargs.pop("username", USERNAME),
        password=kwargs.pop("password", PASSWORD),
        token_path=token_path,
        client=_http(carrier, registry),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        **kwargs,
    )


def make_client(
    carrier: FakeCarrier,
    clock: Clock,
    token_path: Path,
    registry: list[httpx.AsyncClient],
    **kwargs: Any,
) -> ca.CarrierGraphQLClient:
    """Auth + GraphQL client sharing one mocked pool, exactly like production."""
    shared = _http(carrier, registry)
    auth = ca.CarrierAuth(
        username=USERNAME,
        password=PASSWORD,
        token_path=token_path,
        client=shared,
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        **kwargs,
    )
    return ca.CarrierGraphQLClient(
        auth,
        client=shared,
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


def seed_cache(
    token_path: Path,
    *,
    access: str,
    refresh: str | None = "cached-refresh-token-xxxxxxxx",
    expires_in: float = 3600.0,
    lifetime: float = 3600.0,
    username: str = USERNAME,
    now: datetime = NOW,
) -> ca.CarrierToken:
    """Write a token cache through the production writer.

    ``expires_in`` is seconds from *now*; ``lifetime`` is how long the token was
    minted for (it backdates ``obtained_at``, which is what caps the renewal
    margin).
    """
    expires_at = now + timedelta(seconds=expires_in)
    token = ca.CarrierToken(
        access_token=access,
        refresh_token=refresh,
        expires_at=expires_at,
        obtained_at=expires_at - timedelta(seconds=lifetime),
        username=username,
        expiry_source="expires_in",
    )
    ca.CarrierTokenCache(token_path).save(token)
    return token


QUERY = "query getInfinityStatus($serial: String!) { infinityStatus(serial: $serial) { oat } }"


# ==========================================================================
# Token reuse, refresh, and the password fallback
# ==========================================================================


async def test_cached_valid_token_is_reused_with_no_http_at_all(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The headline requirement: a warm cache means no token endpoint traffic."""
    access = tok("cached-valid")
    seed_cache(token_path, access=access)
    auth = make_auth(carrier, clock, token_path, http_clients)

    for _ in range(5):
        assert await auth.access_token() == access

    assert carrier.total_requests == 0
    assert auth.password_grants == 0
    assert auth.refresh_grants == 0


async def test_expired_token_uses_the_refresh_grant_not_the_password(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    fresh = tok("from-refresh")
    seed_cache(token_path, access=tok("stale-1"), refresh="rt-alpha-000000000000", expires_in=-10)
    carrier.okta.push(token_ok(fresh, refresh="rt-beta-1111111111111"))
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == fresh

    assert carrier.okta.count == 1
    form = carrier.okta.form()
    assert form["grant_type"] == "refresh_token"
    assert form["refresh_token"] == "rt-alpha-000000000000"
    assert form["client_id"] == ca.OKTA_CLIENT_ID
    assert form["scope"] == ca.OAUTH_SCOPE
    assert "password" not in form
    assert auth.password_grants == 0
    assert auth.refresh_grants == 1


async def test_token_renews_proactively_before_expiry(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """A token with 60s left is renewed now, not when a poll gets a 401."""
    seed_cache(token_path, access=tok("nearly-expired"), expires_in=60)
    carrier.okta.push(token_ok(tok("renewed-early")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == tok("renewed-early")
    assert carrier.okta.grants() == ["refresh_token"]


async def test_a_short_lived_token_is_not_renewed_on_every_call(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The renewal margin never exceeds half the token's own lifetime.

    A 240s token with a flat 300s margin would look expired on arrival, and the
    30s poller would ask Okta for a new one every cycle.
    """
    carrier.okta.push(token_ok(tok("short-lived"), expires_in=240))
    auth = make_auth(carrier, clock, token_path, http_clients)
    token = await auth.token()

    assert token.effective_margin() == 120.0
    assert token.is_fresh(clock.now())
    clock.advance(100)
    assert await auth.access_token() == tok("short-lived")
    assert carrier.okta.count == 1


async def test_failed_refresh_falls_back_to_the_password_grant_exactly_once(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("stale-2"), refresh="rt-dead-2222222222", expires_in=-1)
    carrier.okta.push(
        oauth_error(400, "invalid_grant"),
        token_ok(tok("from-password")),
    )
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == tok("from-password")

    assert carrier.okta.grants() == ["refresh_token", "password"]
    password_form = carrier.okta.form(1)
    assert password_form["username"] == USERNAME
    assert password_form["password"] == PASSWORD
    assert password_form["client_id"] == ca.OKTA_CLIENT_ID
    assert password_form["scope"] == ca.OAUTH_SCOPE
    assert (
        carrier.okta.requests[1].headers["content-type"] == "application/x-www-form-urlencoded"
    )
    assert auth.password_grants == 1
    assert auth.refresh_grants == 1


async def test_no_cached_refresh_token_goes_straight_to_the_password_grant(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(token_ok(tok("cold-start")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == tok("cold-start")
    assert carrier.okta.grants() == ["password"]


async def test_concurrent_callers_trigger_exactly_one_refresh(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The 30s poller and the daily job must not stampede the token endpoint."""
    seed_cache(token_path, access=tok("stale-3"), refresh="rt-shared-33333333", expires_in=-5)
    carrier.okta.push(token_ok(tok("single-renewal")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    results = await asyncio.gather(*(auth.access_token() for _ in range(12)))

    assert results == [tok("single-renewal")] * 12
    assert carrier.okta.count == 1
    assert auth.refresh_grants == 1
    assert auth.password_grants == 0


async def test_concurrent_reauthentication_after_a_shared_401_renews_once(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Two in-flight requests rejected on the same token cost one renewal."""
    stale = tok("stale-4")
    seed_cache(token_path, access=stale, refresh="rt-once-44444444444", expires_in=3600)
    carrier.okta.push(token_ok(tok("renewed-once")))
    auth = make_auth(carrier, clock, token_path, http_clients)
    await auth.token()  # warm: the cache is valid, so still zero HTTP so far
    assert carrier.okta.count == 0

    tokens = await asyncio.gather(
        *(auth.reauthenticate(stale_token=stale, reason="test") for _ in range(4))
    )

    assert {t.access_token for t in tokens} == {tok("renewed-once")}
    assert carrier.okta.count == 1


async def test_expiry_floor_stops_a_bogus_exp_from_spinning(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """A token that always looks expired must not mean a grant per poll."""
    carrier.okta.push(
        token_ok(tok("already-stale"), expires_in=1),
        token_ok(tok("after-floor")),
    )
    auth = make_auth(carrier, clock, token_path, http_clients)

    first = await auth.access_token()
    second = await auth.access_token()

    assert first == second == tok("already-stale")
    assert carrier.okta.count == 1  # the second call was floored, not repeated

    clock.advance(ca.MIN_RENEW_INTERVAL_S + 1)
    assert await auth.access_token() == tok("after-floor")
    assert carrier.okta.count == 2


async def test_a_rejected_password_grant_backs_off_instead_of_retrying_every_poll(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """A wrong ``CARRIER_PASSWORD`` must not mean a grant per poll cycle.

    ``MIN_RENEW_INTERVAL_S`` cannot cover this and is not meant to: a failed
    grant leaves ``self._token is None``, so ``token()``'s floor — which only
    guards a token we still hold — is skipped on every later call. Without a
    separate credential-rejection backoff nothing rate-limits the password grant
    at all, and a mis-credentialed container sends the password to Okta 2,880
    times a day (PLAN.md §6.6: "back off 60s and keep trying").
    """
    carrier.okta.push(oauth_error(400, "invalid_grant", "wrong password"))
    auth = make_auth(carrier, clock, token_path, http_clients)

    for _ in range(10):  # ten 30s poll cycles = five minutes
        with pytest.raises(ca.CarrierAuthError):
            await auth.access_token()
        clock.advance(30)

    assert ca.GRANT_FAILURE_BACKOFF_S == 60.0
    # 300s of polling at a 60s floor: five attempts, not ten.
    assert auth.password_grants == 5
    assert carrier.okta.count == 5


async def test_a_rejection_driven_password_grant_is_floored_even_when_okta_says_yes(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The second half of the anti-spin design, isolated from the ladder backoff.

    ``GRANT_FAILURE_BACKOFF_S`` only arms when Okta *rejects* us. The nastier
    case is the one where Okta is perfectly happy — it mints a token every time —
    and the *gateway* is what rejects it (a revoked entitlement, an account that
    can no longer see this system). Every rejection then drives a
    ``reauthenticate()``, every ``reauthenticate()`` reaches the password rung,
    and every password grant succeeds, so no failure-based backoff ever arms:
    the password goes to Okta on every cycle, forever.

    ``MIN_PASSWORD_GRANT_INTERVAL_S`` is the only thing standing in the way, so
    it is pinned here through ``reauthenticate()`` — the public API a rejection
    goes through — rather than through ``CarrierGraphQLClient``, whose own
    ladder backoff would otherwise mask it entirely.
    """
    carrier.okta.push(token_ok(tok("minted-fine"), expires_in=86_400))  # sticky: always yes
    auth = make_auth(carrier, clock, token_path, http_clients)

    # First rejection: we buy a token, as we should.
    await auth.reauthenticate(allow_refresh=False, reason="gateway_403")
    assert auth.password_grants == 1

    # The gateway keeps rejecting. A newer token cannot fix what a token minted
    # seconds ago could not, so the password must not be re-sent.
    for _ in range(120):  # one hour of 30s polls
        clock.advance(30)
        with pytest.raises(ca.CarrierAuthError) as excinfo:
            await auth.reauthenticate(allow_refresh=False, reason="gateway_403")
        assert PASSWORD not in str(excinfo.value)
        if clock.monotonic() >= ca.MIN_PASSWORD_GRANT_INTERVAL_S:
            break
    else:  # pragma: no cover - the floor never opened inside the hour
        raise AssertionError("the password floor never expired")

    assert ca.MIN_PASSWORD_GRANT_INTERVAL_S == 900.0
    assert auth.password_grants == 1  # 15 minutes of rejections, one grant
    assert carrier.okta.count == 1

    # ...and it is a floor, not a wall: the next attempt after the window is
    # allowed through, so a restored entitlement heals without a restart.
    clock.advance(ca.MIN_PASSWORD_GRANT_INTERVAL_S)
    await auth.reauthenticate(allow_refresh=False, reason="gateway_403")
    assert auth.password_grants == 2


async def test_a_floored_grant_fails_without_touching_the_network(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Inside the backoff the call is refused locally — a gap, not a request."""
    carrier.okta.push(oauth_error(400, "invalid_grant"), token_ok(tok("password-fixed")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError):
        await auth.access_token()
    assert carrier.okta.count == 1

    clock.advance(1)
    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await auth.access_token()

    assert carrier.okta.count == 1  # refused locally: no second request
    assert isinstance(excinfo.value, SourceAuthError)
    assert PASSWORD not in str(excinfo.value)

    # ...and the door reopens on its own, so a corrected password heals the
    # container without a restart.
    clock.advance(ca.GRANT_FAILURE_BACKOFF_S)
    assert await auth.access_token() == tok("password-fixed")
    assert carrier.okta.count == 2


# ==========================================================================
# The token cache on disk
# ==========================================================================


async def test_token_file_is_written_0600(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(token_ok(tok("persisted")))
    auth = make_auth(carrier, clock, token_path, http_clients)
    await auth.access_token()

    assert token_path.exists()
    assert stat.S_IMODE(os.stat(token_path).st_mode) == 0o600
    payload = json.loads(token_path.read_text())
    assert payload["access_token"] == tok("persisted")
    assert payload["username"] == USERNAME
    assert payload["expires_at"].endswith("Z")


async def test_token_cache_creates_the_tokens_directory(
    carrier: FakeCarrier, clock: Clock, spool_dir: Path, http_clients: list
) -> None:
    nested = spool_dir / "brand" / "new" / "carrier.json"
    carrier.okta.push(token_ok(tok("nested-dir")))
    auth = make_auth(carrier, clock, nested, http_clients)
    await auth.access_token()

    assert nested.exists()
    assert stat.S_IMODE(os.stat(nested).st_mode) == 0o600


async def test_rotated_refresh_token_is_persisted(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Okta rotates the refresh token; failing to rewrite the file burns it."""
    seed_cache(token_path, access=tok("stale-5"), refresh="rt-old-5555555555", expires_in=-1)
    carrier.okta.push(token_ok(tok("rotated"), refresh="rt-new-666666666666"))
    auth = make_auth(carrier, clock, token_path, http_clients)
    await auth.access_token()

    payload = json.loads(token_path.read_text())
    assert payload["refresh_token"] == "rt-new-666666666666"

    # A second process starting from that file refreshes with the NEW token.
    carrier.okta.push(token_ok(tok("second-process")))
    clock.advance(7200)
    second = make_auth(carrier, clock, token_path, http_clients)
    await second.access_token()
    assert carrier.okta.form()["refresh_token"] == "rt-new-666666666666"


async def test_refresh_response_without_a_new_refresh_token_keeps_the_old_one(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("stale-6"), refresh="rt-keep-7777777777", expires_in=-1)
    carrier.okta.push(token_ok(tok("no-rotation"), refresh=None))
    auth = make_auth(carrier, clock, token_path, http_clients)
    await auth.access_token()

    assert json.loads(token_path.read_text())["refresh_token"] == "rt-keep-7777777777"


async def test_cache_for_a_different_username_is_ignored(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("someone-else"), username="other@example.invalid")
    carrier.okta.push(token_ok(tok("mine")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == tok("mine")
    assert carrier.okta.grants() == ["password"]


@pytest.mark.parametrize(
    "contents",
    ["", "not json at all", "[1, 2, 3]", "{}", '{"token_type": "Bearer"}'],
    ids=["empty", "garbage", "array", "empty-object", "no-tokens"],
)
async def test_unusable_cache_file_falls_back_to_the_password_grant(
    contents: str,
    carrier: FakeCarrier,
    clock: Clock,
    token_path: Path,
    http_clients: list,
) -> None:
    token_path.write_text(contents)
    carrier.okta.push(token_ok(tok("recovered")))
    auth = make_auth(carrier, clock, token_path, http_clients)

    assert await auth.access_token() == tok("recovered")
    assert carrier.okta.grants() == ["password"]


async def test_a_missing_cache_file_is_not_an_error(
    carrier: FakeCarrier, clock: Clock, spool_dir: Path
) -> None:
    cache = ca.CarrierTokenCache(spool_dir / "tokens" / "nope.json")
    assert cache.load() is None
    cache.clear()  # also a no-op, and must not raise


async def test_dead_refresh_token_is_dropped_from_the_cache(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("stale-7"), refresh="rt-revoked-888888", expires_in=-1)
    carrier.okta.push(oauth_error(400, "invalid_grant"), token_ok(tok("after-revocation")))
    auth = make_auth(carrier, clock, token_path, http_clients)
    await auth.access_token()

    payload = json.loads(token_path.read_text())
    assert payload["refresh_token"] != "rt-revoked-888888"
    assert payload["access_token"] == tok("after-revocation")


# ==========================================================================
# Expiry: expires_in, JWT exp, and garbage
# ==========================================================================


def jwt_with(payload: dict[str, Any], *, pad: bool = True) -> str:
    """A syntactically real, cryptographically meaningless JWT."""
    raw = json.dumps(payload).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).decode("ascii")
    if not pad:
        body = body.rstrip("=")
    return f"header.{body}.signature"


def test_decode_jwt_exp_reads_the_claim_with_and_without_padding() -> None:
    assert ca.decode_jwt_exp(jwt_with({"exp": 1789000000})) == 1789000000.0
    assert ca.decode_jwt_exp(jwt_with({"exp": 1789000000}, pad=False)) == 1789000000.0
    assert ca.decode_jwt_exp(jwt_with({"exp": "1789000000"})) == 1789000000.0


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "   ",
        "not-a-jwt",
        "only.two",  # payload segment is not base64
        "a..c",  # empty payload segment
        "a.!!!!.c",  # not base64 at all
        "a.eyJhIjo.c",  # truncated JSON after decoding
        "a." + base64.urlsafe_b64encode(b"[1,2,3]").decode() + ".c",  # not an object
        "a." + base64.urlsafe_b64encode(b'{"sub":"x"}').decode() + ".c",  # no exp
        "a." + base64.urlsafe_b64encode(b'{"exp":"soon"}').decode() + ".c",  # non-numeric
        "a." + base64.urlsafe_b64encode(b'{"exp":true}').decode() + ".c",  # bool
        "a." + base64.urlsafe_b64encode(b'{"exp":-5}').decode() + ".c",  # nonsense
        "a." + base64.urlsafe_b64encode(b"\xff\xfe\xfd").decode() + ".c",  # not UTF-8
        12345,  # not even a string
    ],
    ids=[
        "none", "empty", "blank", "plain", "two-parts", "empty-payload", "not-base64",
        "truncated-json", "not-object", "no-exp", "non-numeric-exp", "bool-exp",
        "negative-exp", "not-utf8", "not-a-string",
    ],
)
def test_decode_jwt_exp_never_raises(value: Any) -> None:
    assert ca.decode_jwt_exp(value) is None


async def test_jwt_exp_is_used_when_expires_in_is_absent(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    exp = int((NOW + timedelta(hours=2)).timestamp())
    access = jwt_with({"exp": exp, "sub": "carrier"})
    carrier.okta.push(token_ok(access, expires_in=None))
    auth = make_auth(carrier, clock, token_path, http_clients)

    token = await auth.token()

    assert token.expiry_source == "jwt_exp"
    assert token.expires_at == datetime.fromtimestamp(exp, tz=UTC)
    assert token.is_fresh(clock.now())


async def test_unknown_expiry_falls_back_to_a_short_assumed_lifetime(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Neither expires_in nor a decodable exp: assume little, keep working."""
    carrier.okta.push(token_ok(tok("opaque"), expires_in=None))
    auth = make_auth(carrier, clock, token_path, http_clients)

    token = await auth.token()

    assert token.expiry_source == "assumed"
    assert token.expires_at == NOW + timedelta(seconds=ca.UNKNOWN_TOKEN_LIFETIME_S)


@pytest.mark.parametrize(
    ("expires_in", "expected"),
    [(3600, 3600.0), ("1800", 1800.0), (900.5, 900.5)],
    ids=["int", "numeric-string", "float"],
)
async def test_expires_in_is_accepted_in_every_shape_okta_might_send(
    expires_in: Any,
    expected: float,
    carrier: FakeCarrier,
    clock: Clock,
    token_path: Path,
    http_clients: list,
) -> None:
    carrier.okta.push(token_ok(tok(f"exp-{expected}"), expires_in=expires_in))
    auth = make_auth(carrier, clock, token_path, http_clients)

    token = await auth.token()

    assert token.expiry_source == "expires_in"
    assert token.expires_at == NOW + timedelta(seconds=expected)


async def test_a_token_response_without_an_access_token_is_an_auth_error(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(lambda _: httpx.Response(200, json={"token_type": "Bearer"}))
    auth = make_auth(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError):
        await auth.access_token()


async def test_a_non_json_token_response_is_transient_not_auth(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(lambda _: httpx.Response(200, text="<html>maintenance</html>"))
    auth = make_auth(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierTransientError):
        await auth.access_token()


# ==========================================================================
# The GraphQL transport
# ==========================================================================


async def test_graphql_request_shape_and_spoofed_spa_headers(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    access = tok("gql-headers")
    seed_cache(token_path, access=access)
    carrier.graphql.push(gql_ok({"infinityStatus": {"oat": "30"}}))
    client = make_client(carrier, clock, token_path, http_clients)

    data = await client.query(
        QUERY, variables={"serial": "TEST0000001"}, operation_name="getInfinityStatus"
    )

    assert data == {"infinityStatus": {"oat": "30"}}
    request = carrier.graphql.requests[-1]
    assert str(request.url) == ca.GRAPHQL_URL
    assert request.method == "POST"
    assert request.headers["authorization"] == f"Bearer {access}"
    assert request.headers["origin"] == ca.SPA_ORIGIN
    assert request.headers["referer"] == ca.SPA_REFERER
    assert request.headers["mobile-app-brand"] == ca.MOBILE_APP_BRAND
    assert request.headers["content-type"] == "application/json"
    assert carrier.graphql.body() == {
        "query": QUERY,
        "variables": {"serial": "TEST0000001"},
        "operationName": "getInfinityStatus",
    }
    assert carrier.okta.count == 0  # a warm cache still means no token traffic


async def test_graphql_errors_payload_raises_instead_of_returning_junk(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """HTTP 200 + ``errors`` is a failure; partial ``data`` is discarded."""
    seed_cache(token_path, access=tok("gql-errors"))
    carrier.graphql.push(
        gql_errors(
            {"message": "Cannot query field 'nope' on type 'InfinityStatus'"},
            data={"infinityStatus": {"oat": "30"}},
        )
    )
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierGraphQLError) as excinfo:
        await client.query(QUERY, operation_name="getInfinityStatus")

    assert "Cannot query field" in str(excinfo.value)
    assert excinfo.value.errors[0]["message"].startswith("Cannot query field")
    # It is transient by inheritance, so the poll loop counts it and continues.
    assert isinstance(excinfo.value, SourceTransientError)
    assert carrier.okta.count == 0  # a data error is not an auth error


async def test_graphql_response_without_data_raises(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-nodata"))
    carrier.graphql.push(lambda _: httpx.Response(200, json={"data": None}))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierGraphQLError):
        await client.query(QUERY)


async def test_graphql_non_json_response_is_transient(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-html"))
    carrier.graphql.push(lambda _: httpx.Response(200, text="<html>oops</html>"))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierTransientError):
        await client.query(QUERY)


async def test_401_refreshes_once_and_retries_successfully(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-401-old"), refresh="rt-401-999999999", expires_in=3600)
    carrier.okta.push(token_ok(tok("gql-401-new")))
    carrier.graphql.push(status_only(401), gql_ok({"infinityStatus": {"oat": "31"}}))
    client = make_client(carrier, clock, token_path, http_clients)

    data = await client.query(QUERY, operation_name="getInfinityStatus")

    assert data == {"infinityStatus": {"oat": "31"}}
    assert carrier.okta.grants() == ["refresh_token"]
    assert carrier.graphql.count == 2
    assert carrier.graphql.requests[0].headers["authorization"] == f"Bearer {tok('gql-401-old')}"
    assert carrier.graphql.requests[1].headers["authorization"] == f"Bearer {tok('gql-401-new')}"


async def test_403_is_treated_as_an_auth_failure_too(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-403"), refresh="rt-403-101010101", expires_in=3600)
    carrier.okta.push(token_ok(tok("gql-403-new")))
    carrier.graphql.push(status_only(403), gql_ok({"ok": 1}))
    client = make_client(carrier, clock, token_path, http_clients)

    assert await client.query(QUERY) == {"ok": 1}
    assert carrier.okta.count == 1


async def test_a_200_unauthenticated_errors_payload_enters_the_auth_ladder(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Carrier reports a dead token as 200 + ``UNAUTHENTICATED`` as well as 401."""
    seed_cache(token_path, access=tok("gql-gqlauth"), refresh="rt-gql-111111111", expires_in=3600)
    carrier.okta.push(token_ok(tok("gql-gqlauth-new")))
    carrier.graphql.push(
        gql_errors({"message": "Unauthorized", "extensions": {"code": "UNAUTHENTICATED"}}),
        gql_ok({"ok": 2}),
    )
    client = make_client(carrier, clock, token_path, http_clients)

    assert await client.query(QUERY) == {"ok": 2}
    assert carrier.okta.count == 1


async def test_401_after_refresh_falls_back_to_the_password_grant(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("ladder-0"), refresh="rt-ladder-1212121", expires_in=3600)
    carrier.okta.push(token_ok(tok("ladder-1")), token_ok(tok("ladder-2")))
    carrier.graphql.push(status_only(401), status_only(401), gql_ok({"ok": 3}))
    client = make_client(carrier, clock, token_path, http_clients)

    assert await client.query(QUERY) == {"ok": 3}

    assert carrier.okta.grants() == ["refresh_token", "password"]
    assert carrier.graphql.count == 3


async def test_persistent_401_gives_up_without_spinning(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("hopeless"), refresh="rt-hopeless-1313", expires_in=3600)
    carrier.okta.push(token_ok(tok("hopeless-new")))
    carrier.graphql.push(status_only(401))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await client.query(QUERY, operation_name="getInfinityStatus")

    assert isinstance(excinfo.value, SourceAuthError)
    # Bounded, always: three GraphQL attempts and at most two token requests.
    assert carrier.graphql.count == 3
    assert carrier.okta.count <= 2


async def test_a_persistent_403_does_not_re_climb_the_ladder_every_poll(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The failure PLAN.md §7.1 was written to prevent, at the transport level.

    A *persistent* rejection — a revoked entitlement, an account that can no
    longer see this system — is not fixed by a new token, so climbing the whole
    ladder on every cycle buys one refresh grant **and one password grant every
    30 seconds**: 2,880 password grants a day against Carrier's Okta, which is
    exactly what the old collector did and exactly what this module exists to
    stop. The retry must degrade to slow, loud and bounded.
    """
    seed_cache(
        token_path,
        access=tok("perma-403"),
        refresh="rt-perma-1616161616",
        expires_in=86_400,
        lifetime=86_400,
    )
    carrier.okta.push(token_ok(tok("perma-403-new"), expires_in=86_400))  # sticky
    carrier.graphql.push(status_only(403))  # sticky: nothing will ever work
    client = make_client(carrier, clock, token_path, http_clients)

    for _ in range(120):  # one hour of 30s polls
        with pytest.raises(ca.CarrierAuthError):
            await client.query(QUERY, operation_name="getInfinityStatus")
        clock.advance(30)

    # An hour of hard failure costs a handful of grants, not 120 of each.
    assert 1 <= client.auth.password_grants <= 5
    assert 1 <= client.auth.refresh_grants <= 5
    assert carrier.okta.count <= 10


async def test_inside_the_auth_backoff_a_call_costs_one_attempt_and_no_grant(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Once a freshly minted token is rejected, stop buying more of them."""
    seed_cache(
        token_path,
        access=tok("fail-fast"),
        refresh="rt-failfast-171717",
        expires_in=86_400,
        lifetime=86_400,
    )
    carrier.okta.push(token_ok(tok("fail-fast-new"), expires_in=86_400))
    carrier.graphql.push(status_only(401))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError):
        await client.query(QUERY)
    assert carrier.graphql.count == 3  # the full ladder, once
    assert carrier.okta.count == 2

    clock.advance(30)
    with pytest.raises(ca.CarrierAuthError):
        await client.query(QUERY)

    assert carrier.graphql.count == 4  # one attempt, not another three
    assert carrier.okta.count == 2  # and not one new token


async def test_a_success_clears_the_auth_backoff(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Backing off must not disarm the ladder for a transport that recovers."""
    seed_cache(
        token_path,
        access=tok("recovers"),
        refresh="rt-recovers-181818",
        expires_in=86_400,
        lifetime=86_400,
    )
    carrier.okta.push(token_ok(tok("recovers-new"), expires_in=86_400))
    carrier.graphql.push(
        status_only(401), status_only(401), status_only(401),  # ladder exhausted
        gql_ok({"ok": 10}),  # then the gateway comes back
        status_only(401), status_only(401), status_only(401),  # and breaks again
    )
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError):
        await client.query(QUERY)
    assert carrier.graphql.count == 3
    assert carrier.okta.count == 2

    clock.advance(30)
    assert await client.query(QUERY) == {"ok": 10}
    assert carrier.graphql.count == 4  # one attempt: the backoff was still open
    assert carrier.okta.count == 2  # and the recovery cost nothing

    # The ladder is armed again: a later rejection gets the full treatment.
    clock.advance(ca.MIN_PASSWORD_GRANT_INTERVAL_S + 1)
    with pytest.raises(ca.CarrierAuthError):
        await client.query(QUERY)
    assert carrier.graphql.count == 7
    assert carrier.okta.count == 4


async def test_a_graphql_auth_error_carries_the_errors_array(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """A 200-with-``errors`` rejection is distinguishable from a dead token.

    ``_errors_are_auth`` reclassifies "not authorized" as a
    :class:`CarrierAuthError`, which is right for a dead token and wrong for a
    permission-gated *field*. The array is carried so a caller
    (``sources/bryant.py::_fetch_status``) can tell the two apart.
    """
    seed_cache(token_path, access=tok("field-denied"), refresh="rt-denied-191919")
    carrier.okta.push(token_ok(tok("field-denied-new")))
    carrier.graphql.push(
        gql_errors(
            {
                "message": "Not authorized to access field 'infinityStatus'",
                "path": ["infinityStatus"],
                "extensions": {"code": "FORBIDDEN"},
            }
        )
    )
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await client.query(QUERY, operation_name="getInfinityStatus")

    assert excinfo.value.errors
    assert excinfo.value.errors[0]["path"] == ["infinityStatus"]
    assert excinfo.value.errors[0]["extensions"]["code"] == "FORBIDDEN"


async def test_a_transport_401_carries_no_errors_array(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """The other half of the distinction: a bare 401 is about our token."""
    seed_cache(token_path, access=tok("bare-401"), refresh="rt-bare-202020")
    carrier.okta.push(token_ok(tok("bare-401-new")))
    carrier.graphql.push(status_only(401))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await client.query(QUERY)

    assert excinfo.value.errors == []


async def test_a_failing_password_grant_surfaces_as_an_auth_error(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(oauth_error(401, "invalid_client", "bad credentials"))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await client.query(QUERY)

    assert "invalid_client" in str(excinfo.value)
    assert carrier.graphql.count == 0


# ==========================================================================
# Transient failures, retries and rate limiting
# ==========================================================================


async def test_transient_5xx_is_retried_then_succeeds(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-5xx"))
    carrier.graphql.push(status_only(502), status_only(504), gql_ok({"ok": 4}))
    client = make_client(carrier, clock, token_path, http_clients)

    assert await client.query(QUERY) == {"ok": 4}
    assert carrier.graphql.count == 3
    assert clock.sleeps == list(ca.RETRY_WAITS_S)


async def test_5xx_that_survives_the_retries_is_transient(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-5xx-hard"))
    carrier.graphql.push(status_only(503))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierTransientError) as excinfo:
        await client.query(QUERY)

    assert not isinstance(excinfo.value, SourceAuthError)
    assert carrier.graphql.count == len(ca.RETRY_WAITS_S) + 1


async def test_network_failure_is_transient_not_a_crash(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-netfail"))
    carrier.graphql.push(network_error())
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierTransientError):
        await client.query(QUERY)


async def test_429_with_a_short_retry_after_is_honoured_inline(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-429-short"))
    carrier.graphql.push(
        status_only(429, headers={"Retry-After": "3"}),
        gql_ok({"ok": 5}),
    )
    client = make_client(carrier, clock, token_path, http_clients)

    assert await client.query(QUERY) == {"ok": 5}

    assert clock.sleeps == [3.0]  # we waited exactly what we were told
    assert client.throttle.events == 1
    assert client.throttle.last_retry_after_s == 3.0
    assert client.status_fields()["throttled"] is False


async def test_429_with_a_long_retry_after_backs_off_and_is_exposed(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """§7.3: honour Retry-After and let the caller record the effective backoff."""
    seed_cache(token_path, access=tok("gql-429-long"))
    carrier.graphql.push(
        status_only(429, headers={"Retry-After": "120"}),
        gql_ok({"ok": 6}),
    )
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierRateLimitError) as excinfo:
        await client.query(QUERY)

    assert excinfo.value.retry_after_s == 120.0
    assert isinstance(excinfo.value, SourceTransientError)
    assert clock.sleeps == []  # nothing slept: the poll cycle is not blocked
    assert carrier.graphql.count == 1  # and nothing retried into the throttle

    fields = client.status_fields()
    assert fields["throttled"] is True
    assert fields["retry_after_s"] == 120.0
    assert fields["throttle_events"] == 1
    assert fields["backoff_remaining_s"] == pytest.approx(120.0)
    assert fields["last_throttle_utc"] == "2026-08-16T12:00:00Z"

    # A call inside the window fails fast without touching the network...
    with pytest.raises(ca.CarrierRateLimitError):
        await client.query(QUERY)
    assert carrier.graphql.count == 1

    # ...and the window closes on its own.
    clock.advance(121)
    assert await client.query(QUERY) == {"ok": 6}
    assert client.status_fields()["throttled"] is False


async def test_429_without_a_retry_after_header_uses_the_default_pause(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    seed_cache(token_path, access=tok("gql-429-bare"))
    carrier.graphql.push(status_only(429))
    client = make_client(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierRateLimitError) as excinfo:
        await client.query(QUERY)

    assert excinfo.value.retry_after_s == ca.DEFAULT_RETRY_AFTER_S


async def test_429_on_the_token_endpoint_is_surfaced_too(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Okta throttles as well; a token request must not hammer it either."""
    carrier.okta.push(status_only(429, headers={"Retry-After": "90"}))
    auth = make_auth(carrier, clock, token_path, http_clients)

    with pytest.raises(ca.CarrierRateLimitError):
        await auth.access_token()

    assert auth.status_fields()["okta_throttled"] is True
    assert auth.status_fields()["okta_retry_after_s"] == 90.0


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("30", 30.0),
        ("0", 0.0),
        ("Sun, 16 Aug 2026 12:02:00 GMT", 120.0),
        ("not-a-date", None),
        ("", None),
        (None, None),
        ("999999999", ca.MAX_RETRY_AFTER_S),
    ],
    ids=["seconds", "zero", "http-date", "garbage", "empty", "absent", "capped"],
)
def test_parse_retry_after(header: str | None, expected: float | None) -> None:
    assert ca.parse_retry_after(header, now=NOW) == expected


# ==========================================================================
# Secrets never escape
# ==========================================================================


def _capture_logs() -> tuple[io.StringIO, Callable[[], None]]:
    buffer = io.StringIO()
    ec_logging.configure_logging("DEBUG", stream=buffer, force=True)

    def restore() -> None:
        ec_logging.configure_logging(force=True)

    return buffer, restore


async def test_no_token_string_ever_reaches_the_log_output(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """Exercise every path that logs, then grep the whole stream for secrets."""
    cached_access = tok("log-cached")
    cached_refresh = "rt-log-cached-1414141414"
    fresh_access = tok("log-fresh")
    fresh_refresh = "rt-log-fresh-1515151515"
    seed_cache(token_path, access=cached_access, refresh=cached_refresh, expires_in=-1)

    carrier.okta.push(
        oauth_error(400, "invalid_grant"),
        token_ok(fresh_access, refresh=fresh_refresh),
    )
    # Three 500s (the retry budget), then a data error, then a throttle.
    carrier.graphql.push(
        *(status_only(500) for _ in range(len(ca.RETRY_WAITS_S) + 1)),
        gql_errors({"message": "boom"}),
        status_only(429, headers={"Retry-After": "600"}),
    )

    buffer, restore = _capture_logs()
    try:
        client = make_client(carrier, clock, token_path, http_clients)
        with pytest.raises(ca.CarrierTransientError):
            await client.query(QUERY, operation_name="getInfinityStatus")
        with pytest.raises(ca.CarrierGraphQLError):
            await client.query(QUERY, operation_name="getInfinityStatus")
        with pytest.raises(ca.CarrierRateLimitError):
            await client.query(QUERY, operation_name="getInfinityStatus")
    finally:
        output = buffer.getvalue()
        restore()

    assert output.strip(), "expected the module to log something at all"
    for secret in (cached_access, cached_refresh, fresh_access, fresh_refresh, PASSWORD):
        assert secret not in output
    # Every line is still valid JSON — scrubbing must not corrupt the stream.
    for line in output.splitlines():
        json.loads(line)


async def test_secrets_do_not_reach_exception_messages_or_status_fields(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    """An upstream that echoes a token back must not leak it onward.

    Exception strings land in ``status.json`` via ``StatusStore.record_failure``,
    so they are held to the same standard as a log line.
    """
    access = tok("echoed")
    carrier.okta.push(
        lambda _: httpx.Response(
            400,
            json={
                "error": "invalid_grant",
                "error_description": f"token {access} is dead",
            },
        )
    )
    auth = make_auth(carrier, clock, token_path, http_clients)
    ec_logging.register_secret(access)

    with pytest.raises(ca.CarrierAuthError) as excinfo:
        await auth.access_token()

    assert access not in str(excinfo.value)
    assert access not in repr(excinfo.value)
    assert "invalid_grant" in str(excinfo.value)
    assert access not in json.dumps(auth.status_fields())


def test_carrier_token_repr_redacts_both_tokens() -> None:
    token = ca.CarrierToken(
        access_token="super-secret-access",
        refresh_token="super-secret-refresh",
        expires_at=NOW,
    )
    rendered = repr(token)
    assert "super-secret-access" not in rendered
    assert "super-secret-refresh" not in rendered
    assert "REDACTED" in rendered
    assert str(token) == rendered


async def test_status_fields_are_json_serialisable_and_credential_free(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    carrier.okta.push(token_ok(tok("status-fields")))
    carrier.graphql.push(gql_ok({"ok": 7}))
    client = make_client(carrier, clock, token_path, http_clients)
    await client.query(QUERY)

    fields = client.status_fields()
    rendered = json.dumps(fields)

    assert tok("status-fields") not in rendered
    assert PASSWORD not in rendered
    assert fields["password_grants"] == 1
    assert fields["refresh_grants"] == 0
    assert fields["token_expiry_source"] == "expires_in"
    assert fields["throttle_events"] == 0


# ==========================================================================
# Typing, wiring and lifecycle
# ==========================================================================


def test_errors_are_the_types_the_poll_loop_already_handles() -> None:
    assert issubclass(ca.CarrierAuthError, SourceAuthError)
    assert issubclass(ca.CarrierTransientError, SourceTransientError)
    assert issubclass(ca.CarrierRateLimitError, ca.CarrierTransientError)
    assert issubclass(ca.CarrierGraphQLError, ca.CarrierTransientError)
    # Nothing here is an auth error by accident: a data error must not trigger
    # a re-login ladder in a caller that catches SourceAuthError.
    assert not issubclass(ca.CarrierGraphQLError, SourceAuthError)


def test_the_okta_endpoint_matches_plan_7_1() -> None:
    assert ca.OKTA_TOKEN_URL == "https://sso.carrier.com/oauth2/default/v1/token"
    assert ca.OKTA_CLIENT_ID == "0oa1ce7hwjuZbfOMB4x7"
    assert ca.OAUTH_SCOPE == "openid offline_access"
    assert ca.GRAPHQL_URL == "https://dataservice.infinity.iot.carrier.com/graphql"


def test_factories_wire_the_configured_paths_and_credentials(settings: Settings) -> None:
    auth = ca.auth_from_settings(settings)
    assert auth.token_path == settings.carrier_token_path
    assert auth.token_path.name == "carrier.json"
    assert auth.token_path.parent == settings.spool_dir / "tokens"


async def test_carrier_stack_shares_one_pool_and_closes_cleanly(settings: Settings) -> None:
    auth, client = ca.carrier_stack_from_settings(settings)
    assert client.auth is auth
    await client.close()
    await client.close()  # idempotent


async def test_missing_credentials_fail_by_name_not_obscurely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from energy_capture.config import get_settings, reset_settings_cache

    monkeypatch.delenv("CARRIER_PASSWORD", raising=False)
    reset_settings_cache()
    try:
        with pytest.raises(RuntimeError, match="CARRIER_PASSWORD"):
            ca.auth_from_settings(get_settings())
    finally:
        reset_settings_cache()


async def test_close_releases_only_a_pool_we_own(
    carrier: FakeCarrier, clock: Clock, token_path: Path, http_clients: list
) -> None:
    shared = _http(carrier, http_clients)
    auth = ca.CarrierAuth(
        username=USERNAME,
        password=PASSWORD,
        token_path=token_path,
        client=shared,
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )
    await auth.close()
    assert not shared.is_closed  # borrowed pools are never closed by the borrower
