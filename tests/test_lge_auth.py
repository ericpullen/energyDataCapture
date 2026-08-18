"""Green Button Connect OAuth (``sources/lge_auth.py``, ``stages/greenbutton_*``).

The credential model here is unlike the other two sources, and the tests follow
that. Leviton and Carrier hold a username and password, so a bug can re-login in
a loop. This holds **no way to re-authenticate**: a person clicks through a
browser once. That makes the refresh token the only asset, and losing it a
human-visible outage rather than a retry — so most of what is pinned here is
about not losing it, not presenting a rejected one, and not logging either.

Everything runs against ``httpx.MockTransport``. Nothing reaches the utility.
"""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from energy_capture import logging as ec_logging
from energy_capture.config import Settings
from energy_capture.sources.lge_auth import (
    LgeAuth,
    LgeAuthError,
    LgeToken,
    LgeTokenCache,
    LgeTransientError,
    authorization_url,
)
from energy_capture.stages import greenbutton_auth

ACCESS = "access-token-aaaaaaaaaaaaaaaaaaaa"
REFRESH = "refresh-token-bbbbbbbbbbbbbbbbbbbb"
ROTATED = "refresh-token-cccccccccccccccccccc"
RESOURCE = "https://services.example.com/espi/1_1/resource/Subscription/5"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        lge_client_id="gbc_test",
        lge_client_secret="s3cret-value",
        spool_dir=tmp_path,
    )


def token_response(
    *, refresh: str | None = REFRESH, expires_in: int | None = 3600, **extra
) -> dict:
    payload = {
        "access_token": ACCESS,
        "token_type": "Bearer",
        "scope": "FB=1_3_4_5",
        "resourceURI": RESOURCE,
        **extra,
    }
    if refresh is not None:
        payload["refresh_token"] = refresh
    if expires_in is not None:
        payload["expires_in"] = expires_in
    return payload


def make_auth(settings: Settings, handler) -> tuple[LgeAuth, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    client = httpx.Client(transport=httpx.MockTransport(record))
    return LgeAuth(settings=settings, client=client), seen


# ------------------------------------------------------------ the URL


def test_the_authorization_url_carries_what_the_custodian_registered(
    settings: Settings,
) -> None:
    url = authorization_url(state="abc123", settings=settings)
    assert url.startswith(settings.lge_authorize_url + "?")
    # redirect_uri is compared by exact string match at the custodian, so it is
    # sent explicitly rather than relying on the one they have on file.
    assert "redirect_uri=https%3A%2F%2Fenergycap.ericpullen.com%2Fgreenbutton%2Fcallback%2F" in url
    assert "response_type=code" in url
    assert "client_id=gbc_test" in url
    assert "state=abc123" in url
    # The ESPI scope must survive urlencoding intact — no commas, no spaces.
    assert "FB%3D1_3_4_5" in url


def test_no_client_id_is_a_clear_error_not_a_broken_url() -> None:
    with pytest.raises(LgeAuthError, match="LGE_CLIENT_ID"):
        authorization_url(state="x", settings=Settings(_env_file=None))  # type: ignore[call-arg]


# ------------------------------------------------------- the code exchange


def test_the_code_exchange_uses_basic_auth_because_the_custodian_demands_it(
    settings: Settings,
) -> None:
    """Measured 2026-08-18: credentials in the body get a bare 401."""
    auth, seen = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    token = auth.exchange_code("the-code")

    assert token.access_token == ACCESS
    assert token.refresh_token == REFRESH
    assert token.resource_uri == RESOURCE
    request = seen[0]
    assert request.headers["authorization"].startswith("Basic ")
    body = request.content.decode()
    assert "grant_type=authorization_code" in body
    assert "code=the-code" in body
    # The secret must never travel in the body when Basic is in use.
    assert "client_secret" not in body


def test_espi_camel_case_resource_uri_is_not_dropped(settings: Settings) -> None:
    """``resourceURI`` says *what* to fetch. Losing it silently is fatal later."""
    for spelling in ("resourceURI", "resource_uri", "ResourceURI"):
        payload = {k: v for k, v in token_response().items() if k != "resourceURI"}
        payload[spelling] = RESOURCE
        auth, _ = make_auth(settings, lambda r, p=payload: httpx.Response(200, json=p))
        assert auth.exchange_code("c").resource_uri == RESOURCE, spelling


def test_a_rejected_code_is_an_auth_error_naming_the_fix(settings: Settings) -> None:
    auth, _ = make_auth(
        settings,
        lambda r: httpx.Response(400, json={"error": "invalid_grant"}),
    )
    with pytest.raises(LgeAuthError, match="greenbutton-authorize"):
        auth.exchange_code("stale-code")


def test_a_server_error_is_transient_not_an_auth_failure(settings: Settings) -> None:
    """A 503 must not read as "re-authorise" — that sends a human to a browser."""
    auth, _ = make_auth(settings, lambda r: httpx.Response(503))
    with pytest.raises(LgeTransientError):
        auth.exchange_code("c")


def test_an_unreachable_custodian_is_transient(settings: Settings) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    auth, _ = make_auth(settings, boom)
    with pytest.raises(LgeTransientError, match="unreachable"):
        auth.exchange_code("c")


# --------------------------------------------------------------- refreshing


def test_a_fresh_token_is_returned_without_touching_the_network(
    settings: Settings,
) -> None:
    # The transport 500s on purpose: reaching it at all is the failure here.
    auth, seen = make_auth(settings, lambda r: httpx.Response(500))
    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    cache.save(
        LgeToken(
            access_token=ACCESS,
            refresh_token=REFRESH,
            expires_at=datetime.now(UTC) + timedelta(hours=2),
            client_id="gbc_test",
        )
    )
    assert auth.access_token().access_token == ACCESS
    assert seen == []


def test_a_near_expiry_token_is_refreshed(settings: Settings) -> None:
    auth, seen = make_auth(
        settings, lambda r: httpx.Response(200, json=token_response(refresh=ROTATED))
    )
    LgeTokenCache(settings.spool_dir / "tokens" / "lge.json").save(
        LgeToken(
            access_token="old",
            refresh_token=REFRESH,
            expires_at=datetime.now(UTC) + timedelta(seconds=30),
            client_id="gbc_test",
        )
    )
    token = auth.access_token()

    assert token.access_token == ACCESS
    assert "grant_type=refresh_token" in seen[0].content.decode()
    # A rotation must land on disk, or the next start presents a dead token.
    reloaded = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json").load()
    assert reloaded is not None and reloaded.refresh_token == ROTATED


def test_a_custodian_that_does_not_rotate_keeps_the_old_refresh_token(
    settings: Settings,
) -> None:
    """Dropping it because the response omitted it would strand the next run."""
    auth, _ = make_auth(
        settings, lambda r: httpx.Response(200, json=token_response(refresh=None))
    )
    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    cache.save(
        LgeToken(
            access_token="old",
            refresh_token=REFRESH,
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
            client_id="gbc_test",
        )
    )
    assert auth.access_token().refresh_token == REFRESH
    assert cache.load().refresh_token == REFRESH


def test_a_rejected_refresh_clears_the_cache(settings: Settings) -> None:
    """Presenting a revoked credential repeatedly is how a registration dies."""
    auth, _ = make_auth(settings, lambda r: httpx.Response(400, json={"error": "invalid_grant"}))
    path = settings.spool_dir / "tokens" / "lge.json"
    LgeTokenCache(path).save(
        LgeToken(
            access_token="old",
            refresh_token=REFRESH,
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
            client_id="gbc_test",
        )
    )
    with pytest.raises(LgeAuthError):
        auth.access_token()
    assert not path.exists()


def test_no_cached_token_says_how_to_get_one(settings: Settings) -> None:
    auth, _ = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    with pytest.raises(LgeAuthError, match="greenbutton-authorize"):
        auth.access_token()


def test_a_token_from_another_client_id_is_not_reused(settings: Settings) -> None:
    """Rotating LGE_CLIENT_ID must not silently reuse the old registration."""
    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    cache.save(LgeToken(access_token=ACCESS, client_id="some-other-client"))
    assert cache.load(client_id="gbc_test") is None


# ------------------------------------------------------------- the cache


def test_the_token_cache_is_owner_only(settings: Settings) -> None:
    path = settings.spool_dir / "tokens" / "lge.json"
    LgeTokenCache(path).save(LgeToken(access_token=ACCESS, refresh_token=REFRESH))
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_the_pending_state_is_single_use(settings: Settings) -> None:
    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    cache.save_state("abc")
    assert cache.take_state() == "abc"
    assert cache.take_state() is None


def test_saving_a_token_does_not_lose_the_pending_state_file(settings: Settings) -> None:
    cache = LgeTokenCache(settings.spool_dir / "tokens" / "lge.json")
    cache.save_state("abc")
    cache.save(LgeToken(access_token=ACCESS))
    assert json.loads((settings.spool_dir / "tokens" / "lge.json").read_text())["token"]


def test_a_corrupt_cache_is_ignored_rather_than_raising(settings: Settings) -> None:
    path = settings.spool_dir / "tokens" / "lge.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json")
    assert LgeTokenCache(path).load() is None


# ------------------------------------------------------------- no leaking


def test_tokens_are_registered_with_the_log_scrubber(settings: Settings) -> None:
    auth, _ = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    auth.exchange_code("c")
    scrubbed = ec_logging.scrub_text(f"got {ACCESS} and {REFRESH}")
    assert ACCESS not in scrubbed
    assert REFRESH not in scrubbed


def test_the_token_repr_redacts_both_tokens() -> None:
    text = repr(LgeToken(access_token=ACCESS, refresh_token=REFRESH))
    assert ACCESS not in text
    assert REFRESH not in text
    assert "<redacted>" in text


# --------------------------------------------------- the localhost callback


def test_the_callback_completes_the_exchange(settings: Settings) -> None:
    auth, _ = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    status, body, content_type = greenbutton_auth.handle_callback(
        "/greenbutton/callback?code=abc&state=xyz", auth=auth
    )
    assert status == 200
    assert content_type.startswith("text/html")
    assert b"Authorized" in body


def test_the_callback_never_echoes_the_code_back(settings: Settings) -> None:
    auth, _ = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    _, body, _ = greenbutton_auth.handle_callback(
        "/greenbutton/callback?code=SECRETCODE123&state=x", auth=auth
    )
    assert b"SECRETCODE123" not in body


def test_the_callback_reports_a_utility_error_without_exchanging(
    settings: Settings,
) -> None:
    auth, seen = make_auth(settings, lambda r: httpx.Response(200, json=token_response()))
    status, body, _ = greenbutton_auth.handle_callback(
        "/greenbutton/callback?error=access_denied&error_description=nope", auth=auth
    )
    assert status == 400
    assert b"access_denied" in body
    assert seen == [], "an errored callback must not call the token endpoint"


def test_the_callback_with_no_code_explains_itself(settings: Settings) -> None:
    status, body, _ = greenbutton_auth.handle_callback("/greenbutton/callback")
    assert status == 400
    assert b"authorization" in body.lower()


def test_a_failed_exchange_is_a_502_page_not_a_traceback(settings: Settings) -> None:
    auth, _ = make_auth(settings, lambda r: httpx.Response(400, json={"error": "invalid_grant"}))
    status, body, _ = greenbutton_auth.handle_callback(
        "/greenbutton/callback?code=stale", auth=auth
    )
    assert status == 502
    assert b"expire" in body.lower()


def test_the_callback_path_matches_the_button_on_the_published_page() -> None:
    """The static page's hand-off button targets this exact path."""
    page = (
        Path(__file__).resolve().parent.parent
        / "site" / "greenbutton" / "callback" / "index.html"
    ).read_text(encoding="utf-8")
    assert greenbutton_auth.CALLBACK_PATH in page
