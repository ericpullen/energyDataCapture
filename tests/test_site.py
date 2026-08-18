"""The public site is load-bearing, so it is pinned like the README and the plist.

``site/`` is not marketing. Six required fields of the LG&E Green Button Connect
registration point at these URLs (``docs/lge-greenbutton.md`` §3), the registration
is **reviewed by a human once, for all customers, forever**, and the redirect page
is what hands an OAuth authorization code to the collector. Three failure modes are
worth a test:

1. **A registered URI stops resolving.** Renaming a directory under ``site/`` silently
   breaks an approved registration, and the symptom appears at authorization time.
2. **A trailing slash goes missing.** ``redirect_uri`` is compared by exact string
   match and GitHub Pages 301s ``/x`` to ``/x/``; the un-slashed form invites a
   mismatch (DEVIATIONS #166 / docs §2).
3. **A third-party host creeps into the callback page.** That page holds an
   authorization code in the visitor's browser. Its promise — "nothing loaded from
   any other host" — has to stay true, and a CDN font would quietly break it.

No network here: everything is read off disk.
"""

from __future__ import annotations

import re
import struct
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SITE = REPO / "site"
DOC = REPO / "docs" / "lge-greenbutton.md"
WORKFLOW = REPO / ".github" / "workflows" / "pages.yml"

#: The hostname claimed in ``site/CNAME`` and registered with the utility.
HOST = "energycap.ericpullen.com"

#: Registered URI path -> the file GitHub Pages serves for it. Keys are exactly
#: what goes on the form; every one is the canonical trailing-slash form.
REGISTERED: dict[str, str] = {
    "/": "index.html",
    "/privacy/": "privacy/index.html",
    "/greenbutton/": "greenbutton/index.html",
    "/greenbutton/callback/": "greenbutton/callback/index.html",
    "/greenbutton/notify/": "greenbutton/notify/index.html",
    "/logo.png": "logo.png",
}

#: Hosts the site may *link* to. Nothing may be *loaded* from them.
LINKABLE_HOSTS = frozenset(
    {"github.com", "www.greenbuttondata.org", "www.greenbuttonalliance.org"}
)


@pytest.fixture(scope="module")
def doc() -> str:
    return DOC.read_text(encoding="utf-8")


def pages() -> list[Path]:
    return sorted(SITE.rglob("*.html"))


# ------------------------------------------------------- the registered URIs


@pytest.mark.parametrize(("uri", "target"), sorted(REGISTERED.items()))
def test_every_registered_uri_resolves_to_a_file(uri: str, target: str) -> None:
    assert (SITE / target).is_file(), (
        f"the registration points at https://{HOST}{uri} and nothing serves it"
    )


@pytest.mark.parametrize("uri", sorted(REGISTERED))
def test_every_registered_uri_appears_in_the_application_draft(uri: str, doc: str) -> None:
    """The form is filled in from that table; a URI missing from it is unregistered."""
    assert f"https://{HOST}{uri}" in doc, f"docs/lge-greenbutton.md never registers {uri}"


def test_directory_uris_keep_their_trailing_slash(doc: str) -> None:
    """A 301 in the middle of an authorization is a redirect_uri mismatch."""
    for uri in REGISTERED:
        # The root is exempt: stripping its slash leaves a bare origin, which the
        # §3a verification loop legitimately writes that way, and which has no
        # directory redirect to be caught out by.
        if uri == "/" or not uri.endswith("/"):
            continue
        unslashed = f"https://{HOST}{uri.rstrip('/')}"
        # `…/privacy` may only ever appear as the prefix of `…/privacy/`.
        for hit in re.finditer(re.escape(unslashed), doc):
            after = doc[hit.end() : hit.end() + 1]
            assert after == "/", (
                f"{unslashed} appears without its trailing slash — GitHub Pages "
                f"301s to {uri}, and redirect_uri is an exact string match"
            )


def test_the_cname_claims_the_registered_host() -> None:
    assert (SITE / "CNAME").read_text(encoding="utf-8").strip() == HOST


def test_the_site_is_not_gitignored() -> None:
    """This actually happened, and it was silent.

    The GitHub Python ``.gitignore`` template ignores ``/site`` as mkdocs build
    output. Every test in this file reads from disk and passed happily while the
    entire directory was untracked, unpushed, and therefore unpublished — an
    approved redirect URI serving a 404. The tests that would catch it are the ones
    that ask git, so this is that test.
    """
    done = subprocess.run(  # noqa: S603
        ["git", "check-ignore", "-v", "site/index.html", "site/logo.png"],  # noqa: S607
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode != 0, (
        f"site/ is gitignored, so it can never be published:\n{done.stdout}"
    )


def test_every_registered_page_is_actually_tracked_by_git() -> None:
    """Present on disk is not the same as present in the repository."""
    done = subprocess.run(  # noqa: S603
        ["git", "ls-files", "--", "site"],  # noqa: S607
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = set(done.stdout.split())
    missing = {f"site/{t}" for t in REGISTERED.values()} - tracked
    assert not missing, (
        "these pages are registered with the utility but are not in the repository, "
        f"so nothing publishes them: {sorted(missing)}"
    )


def test_jekyll_is_disabled() -> None:
    """Without this, Pages runs the tree through Jekyll and may drop files."""
    assert (SITE / ".nojekyll").is_file()


# ------------------------------------------------------------ the logo asset


def test_the_logo_is_exactly_the_size_the_form_asks_for() -> None:
    """"images greater than 180 x 150 may be cropped or reduced" — so don't be."""
    raw = (SITE / "logo.png").read_bytes()
    assert raw[:8] == b"\x89PNG\r\n\x1a\n", "logo.png is not a PNG"
    width, height = struct.unpack(">II", raw[16:24])  # IHDR payload
    assert (width, height) == (180, 150), (width, height)


# ------------------------------------------------- no third-party resources


class _Resources(HTMLParser):
    """Collects loaded subresources and linked hrefs separately."""

    LOADS = {"script": "src", "img": "src", "iframe": "src", "link": "href",
             "source": "src", "video": "src", "audio": "src", "embed": "src"}

    def __init__(self) -> None:
        super().__init__()
        self.loaded: list[str] = []
        self.linked: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        got = {k: (v or "") for k, v in attrs}
        attr = self.LOADS.get(tag)
        if attr and got.get(attr):
            self.loaded.append(got[attr])
        if tag == "a" and got.get("href"):
            self.linked.append(got["href"])


@pytest.mark.parametrize("page", pages(), ids=lambda p: str(p.relative_to(SITE)))
def test_no_page_loads_anything_from_another_host(page: Path) -> None:
    """The callback page holds an authorization code. Keep its origin closed.

    Checked on every page, not just the callback: a shared stylesheet that pulls a
    font would compromise it just as effectively.
    """
    parser = _Resources()
    parser.feed(page.read_text(encoding="utf-8"))
    for url in parser.loaded:
        assert not url.startswith(("http://", "https://", "//")), (
            f"{page.relative_to(SITE)} loads {url} from another host"
        )


@pytest.mark.parametrize("page", pages(), ids=lambda p: str(p.relative_to(SITE)))
def test_outbound_links_go_only_where_we_expect(page: Path) -> None:
    parser = _Resources()
    parser.feed(page.read_text(encoding="utf-8"))
    for href in parser.linked:
        if not href.startswith(("http://", "https://")):
            continue
        host = href.split("/")[2]
        assert host in LINKABLE_HOSTS, f"{page.relative_to(SITE)} links out to {host}"


# --------------------------------------------------- the published contract


def test_the_callback_page_names_a_cli_command_the_docs_promise_to_build() -> None:
    """The page prints a command with the real code in it. That is a contract.

    Nobody can rename ``greenbutton-authorize`` without editing a page that is
    already published to an approved third-party registration, so the name must
    stay written down next to the plan that builds it.
    """
    body = (SITE / "greenbutton" / "callback" / "index.html").read_text(encoding="utf-8")
    assert "energycap greenbutton-authorize" in body
    assert "/greenbutton/callback" in body, "the hand-off target is gone"
    assert "greenbutton-authorize" in DOC.read_text(encoding="utf-8"), (
        "docs/lge-greenbutton.md must record that the published callback page "
        "already names this command"
    )


def test_the_callback_page_does_not_leave_the_code_in_browser_history() -> None:
    body = (SITE / "greenbutton" / "callback" / "index.html").read_text(encoding="utf-8")
    assert "history.replaceState" in body, (
        "the authorization code must be stripped from the address bar once read"
    )


def test_the_notify_page_does_not_promise_push_it_cannot_serve() -> None:
    """A static host answers GET, not POST. Saying so is the honest thing."""
    body = (SITE / "greenbutton" / "notify" / "index.html").read_text(encoding="utf-8")
    assert "does not rely on push notifications" in body
    assert "POST" in body


def test_the_workflow_publishes_only_the_site_directory() -> None:
    """Publishing the repo root would serve docs/ from the app's own origin."""
    body = WORKFLOW.read_text(encoding="utf-8")
    assert "upload-pages-artifact" in body
    assert re.search(r"^\s+path:\s*site\s*$", body, re.M), (
        "the Pages artifact path must be exactly `site`"
    )
