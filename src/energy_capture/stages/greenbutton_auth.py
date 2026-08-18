"""``energycap greenbutton-authorize`` — the once-per-authorisation human step.

Green Button Connect has exactly one step software cannot do: a person logs into
MyMeter and consents. This command is both halves of that round trip.

With no ``--code`` it prints the authorisation URL and remembers the ``state``.
With ``--code`` it exchanges the code for tokens and caches them at
``{SPOOL_DIR}/tokens/lge.json``, mode 600.

**The invocation is a published contract.** The callback page at
``https://energycap.ericpullen.com/greenbutton/callback/`` prints
``uv run energycap greenbutton-authorize --code … --state …`` with the real code
filled in, and that page is registered with the utility as this application's
redirect URI. Renaming this command or its options breaks a page that is already
live in front of a customer, so ``tests/test_site.py`` pins the name.

Authorisation codes expire in minutes, so this prints what it did rather than
staying quiet: an operator who does not see ``authorized`` needs to know
immediately, while the code is still worth retrying.
"""

from __future__ import annotations

import html
from typing import Any
from urllib.parse import parse_qs, urlsplit

from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.sources.lge_auth import LgeAuth, LgeError

STAGE = "greenbutton_auth"
log = get_logger(STAGE)

#: Where the published callback page's "hand off to the collector" button points.
#: Also a published contract — see the module docstring.
CALLBACK_PATH = "/greenbutton/callback"

__all__ = ["CALLBACK_PATH", "handle_callback", "run"]


def run(
    *,
    code: str | None = None,
    state: str | None = None,
    auth: LgeAuth | None = None,
) -> dict[str, Any]:
    """Print the authorisation URL, or exchange a code for tokens."""
    resolved = auth or LgeAuth()

    if code is None:
        url, issued = resolved.start()
        settings = get_settings()
        print(  # noqa: T201 - the URL *is* this command's output
            "\nOpen this in a browser and sign in to MyMeter with your LOCAL "
            "account\n(the one whose email differs from your My Account "
            f"login):\n\n  {url}\n\n"
            "MyMeter will send you back to\n"
            f"  {settings.lge_redirect_uri}\n"
            "which hands the authorization to this collector. If the hand-off "
            "does not\nwork, that page also prints the command to paste back "
            "here.\n\n"
            "The code expires within minutes, so finish in one sitting.\n"
        )
        return {"action": "authorize_url", "state": issued}

    token = resolved.exchange_code(code, state=state)
    print(  # noqa: T201
        "\nAuthorized. Tokens cached (mode 600).\n"
        f"  scope:        {token.scope}\n"
        f"  resource:     {token.resource_uri or '(using LGE_RESOURCE_URI)'}\n"
        f"  expires:      {token.expires_at.isoformat() if token.expires_at else 'unknown'}\n"
        f"  refreshable:  {'yes' if token.refresh_token else 'NO — will need re-authorising'}\n\n"
        "Now fetch the meter data:\n"
        "  energycap fetch-greenbutton --start <YYYY-MM-DD> --end <YYYY-MM-DD>\n"
    )
    return {
        "action": "authorized",
        "scope": token.scope,
        "resource_uri": token.resource_uri,
        "has_refresh_token": bool(token.refresh_token),
    }


# ------------------------------------------------------- the local hand-off


def _page(title: str, body: str) -> bytes:
    """A tiny self-contained page. No styles from anywhere, no scripts."""
    return (
        "<!doctype html><html lang=en><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width, initial-scale=1'>"
        f"<title>{html.escape(title)}</title>"
        "<body style=\"font:16px/1.6 -apple-system,BlinkMacSystemFont,"
        "'Segoe UI',Roboto,sans-serif;max-width:34rem;margin:3rem auto;"
        "padding:0 1.25rem;color:#0f172a;background:#f8fafc\">"
        f"<h1 style='font-size:1.3rem'>{html.escape(title)}</h1>{body}"
        "</body></html>"
    ).encode("utf-8")


def handle_callback(target: str, *, auth: LgeAuth | None = None) -> tuple[int, bytes, str]:
    """Complete the authorisation from the browser's redirect.

    The published callback page is static — it cannot exchange a code, because it
    is served from GitHub Pages with no server behind it. Its "hand off to the
    collector" button points here instead, at the collector's own health port,
    which *can*. That keeps the authorisation code inside the operator's own
    machine: it goes from the utility, to their browser, to localhost, and never
    to any host we control.

    Returns ``(status, body, content_type)`` rather than touching a socket, so it
    is testable without a server. Never echoes the code back: it is single-use
    and by this point spent.
    """
    query = parse_qs(urlsplit(target).query)
    code = (query.get("code") or [""])[0].strip()
    state = (query.get("state") or [""])[0].strip() or None
    failure = (query.get("error") or [""])[0].strip()

    if failure:
        detail = (query.get("error_description") or [""])[0].strip()
        log.warning("greenbutton_callback_error", error=failure)
        return (
            400,
            _page(
                "The utility reported an error",
                f"<p><code>{html.escape(failure)}</code></p>"
                + (f"<p>{html.escape(detail)}</p>" if detail else "")
                + "<p>Nothing was authorized. Start again from MyMeter.</p>",
            ),
            "text/html; charset=utf-8",
        )

    if not code:
        return (
            400,
            _page(
                "No authorization code",
                "<p>This endpoint completes a Green Button Connect authorization. "
                "It does something only when the utility's callback page hands it "
                "a code.</p>",
            ),
            "text/html; charset=utf-8",
        )

    try:
        token = (auth or LgeAuth()).exchange_code(code, state=state)
    except LgeError as exc:
        # Scrubbed on the way out like every other log line; the message names
        # what failed without repeating the credential.
        log.warning("greenbutton_callback_failed", error=f"{type(exc).__name__}")
        return (
            502,
            _page(
                "The exchange failed",
                f"<p>{html.escape(str(exc))}</p>"
                "<p>Authorization codes expire within minutes — if that is what "
                "happened, start again from MyMeter.</p>",
            ),
            "text/html; charset=utf-8",
        )

    expires = token.expires_at.isoformat() if token.expires_at else "unknown"
    refreshable = "yes" if token.refresh_token else "NO — this will need re-authorising"
    return (
        200,
        _page(
            "Authorized",
            "<p>The collector has the tokens and cached them with owner-only "
            "permissions. You can close this tab.</p>"
            "<ul>"
            f"<li>scope: <code>{html.escape(token.scope or '(none returned)')}</code></li>"
            f"<li>expires: <code>{html.escape(expires)}</code></li>"
            f"<li>refreshable: <code>{html.escape(refreshable)}</code></li>"
            "</ul>"
            "<p>Fetch the meter data with "
            "<code>energycap fetch-greenbutton</code>.</p>",
        ),
        "text/html; charset=utf-8",
    )
