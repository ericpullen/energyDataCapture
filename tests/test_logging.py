"""Tests for the structured logger and its credential scrubber (PLAN.md §11, §15.8).

§15.8 is one line — *"passwords/tokens injected into log records never reach
output"* — and CLAUDE.md rule 8 makes it a correctness contract rather than a
nicety. So every assertion here is made against **the bytes that actually left
the handler**, never against the return value of :meth:`ScrubbingFilter.filter`
or a call to :func:`scrub` in isolation. A scrubber that redacts perfectly and
then drops the line, or emits something that is not one JSON object, has failed
the same requirement.

The layers of defence documented in ``logging.py`` are exercised *separately* on
purpose — the key-name tests never register their token, and the text-pattern
tests never register theirs — so a regression in one layer cannot hide behind
another.

Two properties are asserted almost everywhere:

* the secret does not appear anywhere in the raw stream, in any form;
* every emitted line parses as **exactly one** JSON object, and the number of
  lines equals the number of log calls (nothing silently vanished).

The second is not hypothetical. ``ScrubbingFilter`` used to scrub ``record.msg``
as free text *before* logging expanded its ``%``-args, so
``log.warning("password=%s", value)`` lost its ``%s`` to the redaction, and the
``msg % args`` inside ``record.getMessage()`` then raised ``TypeError``. The
record was dropped entirely and logging's own error handler dumped a traceback —
containing the unredacted ``Arguments:`` — to stderr.
``test_a_credential_shaped_format_string_still_emits_its_line`` pins that.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging as stdlib_logging
from collections.abc import Iterator
from typing import Any

import pytest
from pydantic import SecretStr

from energy_capture import logging as ec_logging
from energy_capture.config import get_settings
from energy_capture.logging import (
    REDACTED,
    JsonFormatter,
    ScrubbingFilter,
    configure_logging,
    get_logger,
    register_secret,
    scrub_text,
)

# --------------------------------------------------------------------------
# Fixtures & helpers
# --------------------------------------------------------------------------


@contextlib.contextmanager
def _isolated_secret_registry() -> Iterator[None]:
    """Give one test a private view of the process-global secret registry.

    ``_runtime_secrets`` outlives any single test, so without this a literal
    registered by an earlier module could make a test pass for the wrong reason
    (or make an "is not redacted" assertion fail for the wrong reason).
    """
    saved = set(ec_logging._runtime_secrets)
    saved_loaded = ec_logging._config_secrets_loaded
    ec_logging._runtime_secrets.clear()
    ec_logging.refresh_config_secrets()
    try:
        yield
    finally:
        ec_logging._runtime_secrets.clear()
        ec_logging._runtime_secrets.update(saved)
        ec_logging._config_secrets_loaded = saved_loaded


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    """The real handler, writing to a buffer instead of stdout.

    Everything under test — the filter, the formatter, the final text pass — is
    the production configuration; only the destination differs.
    """
    buffer = io.StringIO()
    with _isolated_secret_registry():
        configure_logging("DEBUG", stream=buffer, force=True)
        try:
            yield buffer
        finally:
            configure_logging(force=True)


def records(stream: io.StringIO) -> list[dict[str, Any]]:
    """Parse the captured stream, asserting one JSON object per line."""
    text = stream.getvalue()
    if not text:
        return []
    assert text.endswith("\n"), "the handler must terminate every line"
    parsed = []
    for line in text.splitlines():
        assert line.strip(), "no blank lines in the stream"
        # json.loads rejects trailing content, so this is "exactly one object".
        doc = json.loads(line)
        assert isinstance(doc, dict)
        parsed.append(doc)
    return parsed


def only(stream: io.StringIO) -> dict[str, Any]:
    """The single record the test expects — proof the line was not dropped."""
    parsed = records(stream)
    assert len(parsed) == 1, f"expected exactly one log line, got {len(parsed)}"
    return parsed[0]


def assert_absent(stream: io.StringIO, *secrets: str) -> None:
    raw = stream.getvalue()
    for secret in secrets:
        assert secret not in raw, f"{secret!r} reached the log stream"


# --------------------------------------------------------------------------
# The stream contract (PLAN.md §11)
# --------------------------------------------------------------------------


def test_a_log_call_emits_one_json_object_with_the_documented_keys(stream: io.StringIO) -> None:
    log = get_logger("uploader")
    log.info("upload_ok", hour="2026-08-16T13", rows=4212, duration_s=1.9)

    doc = only(stream)
    assert set(doc) >= {"ts", "level", "stage", "event"}
    assert doc["level"] == "INFO"
    assert doc["stage"] == "uploader"
    assert doc["event"] == "upload_ok"
    assert doc["rows"] == 4212
    assert doc["duration_s"] == 1.9
    assert doc["ts"].endswith("Z")


def test_every_level_emits_exactly_one_line_each(stream: io.StringIO) -> None:
    log = get_logger("poller")
    log.debug("a")
    log.info("b")
    log.warning("c")
    log.error("d")
    log.critical("e")

    assert [doc["event"] for doc in records(stream)] == ["a", "b", "c", "d", "e"]


def test_the_filter_keeps_the_record(stream: io.StringIO) -> None:
    """``filter()`` must return truthy — a scrubber that dropped records would
    satisfy "no secrets in the output" trivially and uselessly."""
    record = stdlib_logging.LogRecord(
        "energy_capture.poller", stdlib_logging.INFO, __file__, 1, "evt", None, None
    )
    record.password = "hunter2hunter2"
    assert ScrubbingFilter().filter(record) is True


# --------------------------------------------------------------------------
# Layer 1 — literal registered secrets (register_secret)
# --------------------------------------------------------------------------

TOKEN = "lev-3f9a2c8b1d7e6f5a4b3c2d1e"


def test_a_registered_secret_is_redacted_in_the_message(stream: io.StringIO) -> None:
    register_secret(TOKEN)
    get_logger("leviton").info(f"re-using cached token {TOKEN} for hub-a")

    doc = only(stream)
    assert_absent(stream, TOKEN)
    assert doc["event"] == f"re-using cached token {REDACTED} for hub-a"


def test_a_registered_secret_is_redacted_in_structured_fields(stream: io.StringIO) -> None:
    register_secret(TOKEN)
    get_logger("leviton").warning("leviton_poll_failed", detail=f"401 with {TOKEN}", hub="hub-a")

    doc = only(stream)
    assert_absent(stream, TOKEN)
    assert doc["detail"] == f"401 with {REDACTED}"
    assert doc["hub"] == "hub-a"


def test_a_registered_secret_is_redacted_in_extra_on_a_plain_logger(stream: io.StringIO) -> None:
    """Not every call site goes through :class:`StageLogger` — the filter is
    installed on the handler, so ``extra=`` is covered too."""
    register_secret(TOKEN)
    stdlib_logging.getLogger("energy_capture.spool").info("spool_write", extra={"note": TOKEN})

    doc = only(stream)
    assert_absent(stream, TOKEN)
    assert doc["note"] == REDACTED


def test_a_registered_secret_is_redacted_nested_in_dicts_and_lists(stream: io.StringIO) -> None:
    register_secret(TOKEN)
    get_logger("leviton").info(
        "request_replay",
        payload={"headers": [{"name": "x-custom", "value": TOKEN}], "hub": "hub-a"},
        history=[["attempt", TOKEN], ("retry", TOKEN)],
    )

    doc = only(stream)
    assert_absent(stream, TOKEN)
    assert doc["payload"]["headers"][0]["value"] == REDACTED
    assert doc["payload"]["hub"] == "hub-a"
    assert doc["history"] == [["attempt", REDACTED], ["retry", REDACTED]]


def test_a_registered_secret_is_redacted_inside_an_arbitrary_objects_repr(
    stream: io.StringIO,
) -> None:
    class Session:
        def __repr__(self) -> str:
            return f"<Session auth={TOKEN}>"

    register_secret(TOKEN)
    get_logger("leviton").info("session_open", session=Session())

    only(stream)
    assert_absent(stream, TOKEN)


def test_a_trivially_short_secret_is_not_registered(stream: io.StringIO) -> None:
    """A one-character password must not turn every log line into confetti."""
    register_secret("a")
    register_secret("")
    get_logger("poller").info("poll_ok", channel_id="ct_1_a", rows=14)

    doc = only(stream)
    assert doc["channel_id"] == "ct_1_a"
    assert REDACTED not in json.dumps(doc)


# --------------------------------------------------------------------------
# Layer 2 — key names (nothing below is ever registered as a literal)
# --------------------------------------------------------------------------

SECRET_KEYS = [
    "password",
    "token",
    "access_token",
    "refresh_token",
    "authorization",
    "api_key",
    "secret",
]


@pytest.mark.parametrize("key", SECRET_KEYS)
def test_a_credential_key_is_redacted_by_its_name_alone(stream: io.StringIO, key: str) -> None:
    value = f"unregistered-{key}-value-8f2c1d0b"
    get_logger("carrier").error("auth_failed", **{key: value}, status=401)

    doc = only(stream)
    assert_absent(stream, value)
    assert doc[key] == REDACTED
    assert doc["status"] == 401


@pytest.mark.parametrize("key", SECRET_KEYS)
def test_a_credential_key_is_redacted_at_depth(stream: io.StringIO, key: str) -> None:
    value = f"nested-{key}-value-4a3b2c1d"
    get_logger("carrier").error(
        "auth_failed", response={"body": {"items": [{key: value, "expires_in": 900}]}}
    )

    doc = only(stream)
    assert_absent(stream, value)
    item = doc["response"]["body"]["items"][0]
    assert item[key] == REDACTED
    assert item["expires_in"] == 900


def test_key_matching_ignores_case_and_punctuation(stream: io.StringIO) -> None:
    """``Authorization``/``access-token``/``API_KEY`` are the same key names."""
    get_logger("carrier").info(
        "headers",
        headers={
            "Authorization": "unregistered-header-value-1",
            "access-token": "unregistered-header-value-2",
            "API_KEY": "unregistered-header-value-3",
            "Origin": "https://myapp.leviton.com",
        },
    )

    doc = only(stream)
    assert_absent(
        stream,
        "unregistered-header-value-1",
        "unregistered-header-value-2",
        "unregistered-header-value-3",
    )
    headers = doc["headers"]
    assert headers["Authorization"] == REDACTED
    assert headers["access-token"] == REDACTED
    assert headers["API_KEY"] == REDACTED
    assert headers["Origin"] == "https://myapp.leviton.com"


def test_the_leviton_login_response_id_is_redacted(stream: io.StringIO) -> None:
    """PLAN.md §6.1: the login response's ``id`` **is** the bearer token."""
    login = {
        "id": "unregistered-login-id-token-7e6f5a4b",
        "userId": "5f1c2d3e4a5b6c7d",
        "ttl": 1209600,
        "created": "2026-08-16T18:00:00.000Z",
    }
    get_logger("leviton").info("leviton_login_ok", response=login)

    doc = only(stream)
    assert_absent(stream, login["id"])
    assert doc["response"]["id"] == REDACTED
    # The rest of the response stays legible — it is what makes the line useful.
    assert doc["response"]["userId"] == "5f1c2d3e4a5b6c7d"
    assert doc["response"]["ttl"] == 1209600
    assert doc["response"]["created"] == "2026-08-16T18:00:00.000Z"


def test_an_innocent_id_is_left_alone(stream: io.StringIO) -> None:
    """Over-redacting ``id`` would gut the logs: hub, breaker and zone ids are
    the only way to tell *which* device a line is about."""
    get_logger("leviton").info(
        "discovery_ok",
        hub={"id": "hub-a-42", "name": "Panel A", "serial_number": "LV12345"},
        breakers=[{"id": "brk-99", "position": 11, "poles": 2}],
        device_id="hub-a",
        channel_id="breaker_p11",
        id="ct_1_a",
    )

    doc = only(stream)
    assert doc["hub"]["id"] == "hub-a-42"
    assert doc["breakers"][0]["id"] == "brk-99"
    assert doc["breakers"][0]["position"] == 11
    assert doc["device_id"] == "hub-a"
    assert doc["channel_id"] == "breaker_p11"
    assert doc["id"] == "ct_1_a"
    assert REDACTED not in stream.getvalue()


def test_a_login_shaped_mapping_redacts_only_its_own_id(stream: io.StringIO) -> None:
    """The login shape is detected per-mapping, not per-record."""
    get_logger("leviton").info(
        "auth_and_discovery",
        login={"id": "unregistered-login-id-9c8b7a", "ttl": 1209600},
        hub={"id": "hub-a-42", "name": "Panel A"},
    )

    doc = only(stream)
    assert_absent(stream, "unregistered-login-id-9c8b7a")
    assert doc["login"]["id"] == REDACTED
    assert doc["login"]["ttl"] == 1209600
    assert doc["hub"]["id"] == "hub-a-42"


# --------------------------------------------------------------------------
# Layer 3 — text patterns (again, nothing here is registered)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("sent Bearer eyJhbGciOiJIUzI1NiJ9.abcdef", f"sent Bearer {REDACTED}"),
        ("header basic QWxhZGRpbjpvcGVuc2VzYW1l", f"header basic {REDACTED}"),
        ("authorization: unregistered-bare-token-1234", f"authorization: {REDACTED}"),
        ("password=hunter2hunter2", f"password={REDACTED}"),
        ('access_token="unregistered-oauth-1234"', f'access_token="{REDACTED}"'),
        ("api-key: unregistered-key-1234", f"api-key: {REDACTED}"),
    ],
)
def test_credential_shaped_text_is_redacted(
    stream: io.StringIO, text: str, expected: str
) -> None:
    get_logger("carrier").warning("upstream_rejected", detail=text)

    doc = only(stream)
    assert doc["detail"] == expected


def test_a_bearer_token_is_redacted_but_the_scheme_word_survives(stream: io.StringIO) -> None:
    """``Authorization: Bearer <tok>`` must not collapse to nonsense — knowing
    the scheme is diagnostic, and the token is what has to go."""
    get_logger("carrier").warning(
        "upstream_rejected", detail="Authorization: Bearer unregistered-jwt-abcdef1234"
    )

    doc = only(stream)
    assert_absent(stream, "unregistered-jwt-abcdef1234")
    assert doc["detail"] == f"Authorization: Bearer {REDACTED}"


def test_ordinary_text_is_untouched(stream: io.StringIO) -> None:
    get_logger("rollup").info(
        "rollup_ok",
        detail="rolled 2026-08-16 for id=hub-a: 1152 rows, kwh=12.5, sample_count=118",
    )

    doc = only(stream)
    assert REDACTED not in doc["detail"]


# --------------------------------------------------------------------------
# SecretStr
# --------------------------------------------------------------------------


def test_a_secretstr_value_never_renders(stream: io.StringIO) -> None:
    get_logger("config").info(
        "settings_loaded",
        leviton_password=SecretStr("unregistered-pydantic-secret-1"),
        nested={"carrier": {"pw": SecretStr("unregistered-pydantic-secret-2")}},
        bucket="test-energy-bucket",
    )

    doc = only(stream)
    assert_absent(stream, "unregistered-pydantic-secret-1", "unregistered-pydantic-secret-2")
    assert doc["leviton_password"] == REDACTED
    assert doc["nested"]["carrier"]["pw"] == REDACTED
    assert doc["bucket"] == "test-energy-bucket"


def test_a_secretstr_repr_embedded_in_text_is_normalised(stream: io.StringIO) -> None:
    get_logger("config").info(
        "settings_repr", detail=f"Settings(leviton_password={SecretStr('whatever')!r})"
    )

    doc = only(stream)
    assert doc["detail"] == f"Settings(leviton_password=SecretStr('{REDACTED}'))"


# --------------------------------------------------------------------------
# Exceptions and tracebacks
# --------------------------------------------------------------------------


def test_a_secret_inside_a_traceback_never_reaches_the_stream(stream: io.StringIO) -> None:
    """``log.exception`` renders frames, messages and source lines — every one
    of which is a place a token can hide."""
    token = "lev-traceback-9a8b7c6d5e4f"
    register_secret(token)
    log = get_logger("leviton")

    try:
        try:
            raise RuntimeError(f"upstream said: authorization {token} rejected")
        except RuntimeError as cause:
            raise ValueError(f"login failed for {token}") from cause
    except ValueError:
        log.exception("leviton_login_failed", attempt=2)

    doc = only(stream)
    assert_absent(stream, token)
    assert doc["level"] == "ERROR"
    assert doc["attempt"] == 2
    # The diagnosis survives; only the credential is gone.
    assert "ValueError" in doc["exc_info"]
    assert "RuntimeError" in doc["exc_info"]
    assert doc["exc_info"].count(REDACTED) >= 2


def test_an_exception_argument_is_scrubbed_when_logged_as_a_field(stream: io.StringIO) -> None:
    token = "carrier-access-1f2e3d4c5b6a"
    register_secret(token)
    exc = RuntimeError(f"401 for access_token={token}")
    get_logger("carrier").error("auth_failed", error=exc)

    only(stream)
    assert_absent(stream, token)


# --------------------------------------------------------------------------
# %-args: the shapes stdlib logging allows
#
# The invariant for every test below: a record that would have been emitted
# before scrubbing is still emitted after it, redacted, as one JSON object.
# --------------------------------------------------------------------------


def test_a_credential_shaped_format_string_still_emits_its_line(
    stream: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    """Regression. The format string is itself credential-shaped, so scrubbing it
    as free text ate the ``%s`` and ``record.getMessage()`` raised ``TypeError``:
    the line vanished from stdout and logging's error handler printed the
    unredacted ``Arguments: ('hunter2hunter2',)`` to stderr."""
    stdlib_logging.getLogger("energy_capture.carrier").warning(
        "password=%s", "hunter2hunter2"
    )

    doc = only(stream)
    assert_absent(stream, "hunter2hunter2")
    assert doc["event"] == f"password={REDACTED}"
    captured = capsys.readouterr()
    assert "hunter2hunter2" not in captured.err
    assert "--- Logging error ---" not in captured.err


@pytest.mark.parametrize(
    "template",
    [
        "password=%s",
        "token=%s",
        "authorization: %s",
        "renewed with refresh_token=%s and it worked",
        "Bearer %s",
    ],
)
def test_no_credential_shaped_format_string_can_drop_its_record(
    stream: io.StringIO, capsys: pytest.CaptureFixture[str], template: str
) -> None:
    stdlib_logging.getLogger("energy_capture.carrier").warning(template, "value-abcdef123456")

    doc = only(stream)
    assert_absent(stream, "value-abcdef123456")
    assert REDACTED in doc["event"]
    assert "--- Logging error ---" not in capsys.readouterr().err


def test_percent_args_without_any_secret_expand_normally(stream: io.StringIO) -> None:
    stdlib_logging.getLogger("energy_capture.uploader").info(
        "uploaded %d rows for %s in %.1fs", 4212, "2026-08-16T13", 1.94
    )

    doc = only(stream)
    assert doc["event"] == "uploaded 4212 rows for 2026-08-16T13 in 1.9s"


def test_a_message_with_no_args_is_left_formattable(stream: io.StringIO) -> None:
    """A bare ``%s`` with no args is literal text to stdlib logging (``if
    self.args``), and must stay that way after scrubbing."""
    stdlib_logging.getLogger("energy_capture.uploader").info("literal %s and 50%% done")

    doc = only(stream)
    assert doc["event"] == "literal %s and 50%% done"


def test_a_non_string_message_survives(stream: io.StringIO) -> None:
    logger = stdlib_logging.getLogger("energy_capture.spool")
    logger.info({"password": "unregistered-dict-msg-1234", "rows": 4})
    logger.info(42)

    parsed = records(stream)
    assert len(parsed) == 2
    assert_absent(stream, "unregistered-dict-msg-1234")
    assert REDACTED in parsed[0]["event"]
    assert "'rows': 4" in parsed[0]["event"]
    assert parsed[1]["event"] == "42"


def test_mapping_args_are_expanded_and_scrubbed(stream: io.StringIO) -> None:
    """logging's dict-args form: ``args`` is the mapping itself, not a 1-tuple."""
    stdlib_logging.getLogger("energy_capture.carrier").info(
        "logged in as %(user)s with %(password)s",
        {"user": "test-carrier@example.invalid", "password": "unregistered-mapping-pw"},
    )

    doc = only(stream)
    assert_absent(stream, "unregistered-mapping-pw")
    assert doc["event"] == f"logged in as test-carrier@example.invalid with {REDACTED}"


def test_structural_redaction_survives_percent_s_of_a_mapping(stream: io.StringIO) -> None:
    """Key-name redaction only works on the real object, so the args must be
    scrubbed *before* ``%s`` flattens them into a string — a value with spaces
    in it would otherwise only be half-caught by the text patterns."""
    stdlib_logging.getLogger("energy_capture.carrier").info(
        "token response %s", {"access_token": "unregistered oauth value with spaces", "ttl": 900}
    )

    doc = only(stream)
    assert_absent(stream, "unregistered oauth value with spaces")
    assert "with spaces" not in stream.getvalue()
    assert REDACTED in doc["event"]
    assert "900" in doc["event"]


def test_a_secret_containing_a_percent_sign_does_not_break_formatting(
    stream: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    """Redacting the literal removes its ``%`` from the message. Nothing may try
    to ``%``-format the result afterwards."""
    secret = "pw-100%s-and-50%d-abcdef"
    register_secret(secret)
    logger = stdlib_logging.getLogger("energy_capture.leviton")
    logger.warning("login rejected for %s", secret)
    logger.warning(f"login rejected for {secret}")

    parsed = records(stream)
    assert len(parsed) == 2
    assert_absent(stream, secret)
    assert all(doc["event"] == f"login rejected for {REDACTED}" for doc in parsed)
    assert "--- Logging error ---" not in capsys.readouterr().err


def test_a_caller_arity_bug_keeps_the_line_instead_of_dropping_it(
    stream: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    """A wrong-arity call is the caller's bug, but a silently missing log line
    is how an incident hides. Emit it, redacted, and say so."""
    stdlib_logging.getLogger("energy_capture.poller").warning(
        "cycle failed %s %s", "only-one-arg"
    )

    doc = only(stream)
    assert "cycle failed %s %s" in doc["event"]
    assert "only-one-arg" in doc["event"]
    assert "--- Logging error ---" not in capsys.readouterr().err


def test_the_arity_fallback_still_redacts(stream: io.StringIO) -> None:
    token = "lev-arity-1a2b3c4d5e6f"
    register_secret(token)
    stdlib_logging.getLogger("energy_capture.poller").warning("cycle failed %s %s", token)

    only(stream)
    assert_absent(stream, token)


def test_scrubbing_twice_is_idempotent(stream: io.StringIO) -> None:
    """The filter can legitimately see a record twice (a second handler, a
    re-emitted record). Redacting a redaction must not cascade."""
    record = stdlib_logging.LogRecord(
        "energy_capture.carrier",
        stdlib_logging.WARNING,
        __file__,
        1,
        "password=%s",
        ("hunter2hunter2",),
        None,
    )
    filt = ScrubbingFilter()
    filt.filter(record)
    first = record.getMessage()
    filt.filter(record)
    assert record.getMessage() == first == f"password={REDACTED}"


# --------------------------------------------------------------------------
# One parseable JSON object per line, whatever the payload contains
# --------------------------------------------------------------------------


def test_quotes_backslashes_and_newlines_keep_the_line_parseable(stream: io.StringIO) -> None:
    secret = 'quote"and\\slash-9c8b7a6d'
    register_secret(secret)
    get_logger("carrier").error(
        "upstream_rejected",
        detail=f'body={{"authorization": "{secret}"}}',
        note='line one\nline two\ttabbed "quoted" C:\\path\\to\\file',
    )

    doc = only(stream)
    assert_absent(stream, secret)
    assert REDACTED in doc["detail"]
    assert doc["note"] == 'line one\nline two\ttabbed "quoted" C:\\path\\to\\file'


def test_text_scrubbing_inside_an_already_serialised_json_line_keeps_it_parseable() -> None:
    """The documented guard: :data:`_KV_RE`'s value stops at a quote or a
    backslash, so the final text pass over the *serialised* line cannot swallow a
    JSON escape and break the object.

    The deliberate cost is that layer 3 is conservative — a quoted value with a
    space in it is only redacted up to the space, and an already-escaped ``\\"``
    stops it dead. That is the right trade here: a value like that is caught by
    layer 1 (the literal is registered) or layer 2 (the key is named), and this
    pass exists only as the last belt-and-braces sweep, where emitting a
    parseable line matters more than a greedier match.
    """
    line = json.dumps(
        {
            "detail": "password=hunter2hunter2",
            "note": 'a quote " and a backslash \\ end',
            "rows": 4212,
        }
    )
    scrubbed = scrub_text(line)

    doc = json.loads(scrubbed)
    assert doc["detail"] == f"password={REDACTED}"
    assert doc["rows"] == 4212
    assert "hunter2hunter2" not in scrubbed
    assert doc["note"] == 'a quote " and a backslash \\ end'

    # And the escape guard itself: a credential-shaped substring sitting right
    # against a JSON escape is left alone rather than half-eaten.
    escaped = json.dumps({"note": 'password="quoted value"'})
    assert json.loads(scrub_text(escaped)) == json.loads(escaped)


def test_a_damaging_text_scrub_falls_back_to_structural_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the final text pass ever *did* damage the encoding, the formatter
    re-serialises the redacted payload instead of emitting a broken line."""

    def damaging(text: str) -> str:
        return text.replace("TOKENXYZ", 'oops"\\')

    monkeypatch.setattr(ec_logging, "scrub_text", damaging)
    record = stdlib_logging.LogRecord(
        "energy_capture.carrier", stdlib_logging.ERROR, __file__, 1, "auth_failed", None, None
    )
    record.energy_fields = {"detail": "TOKENXYZ", "status": 401}

    line = JsonFormatter().format(record)

    doc = json.loads(line)  # exactly one object, still
    assert "TOKENXYZ" not in line
    assert doc["status"] == 401
    assert doc["event"] == "auth_failed"


def test_a_long_run_of_mixed_calls_yields_one_object_per_call(stream: io.StringIO) -> None:
    """End-to-end: every shape in this file, in one stream."""
    register_secret(TOKEN)
    log = get_logger("poller")
    plain = stdlib_logging.getLogger("energy_capture.poller")

    log.info("poll_ok", rows=14)
    log.warning("poll_failed", detail=f"401 {TOKEN}")
    plain.warning("password=%s", "hunter2hunter2")
    plain.info("uploaded %d rows", 4212)
    log.info("login", response={"id": TOKEN, "ttl": 1209600})
    try:
        raise RuntimeError(f"boom {TOKEN}")
    except RuntimeError:
        log.exception("cycle_crashed")

    parsed = records(stream)
    assert len(parsed) == 6
    assert_absent(stream, TOKEN, "hunter2hunter2")


# --------------------------------------------------------------------------
# Config-sourced credentials (no explicit register_secret call)
# --------------------------------------------------------------------------


def test_config_credentials_are_scrubbed_without_an_explicit_register_secret(
    stream: io.StringIO,
) -> None:
    """A password read from the environment is a secret from the first log line,
    before any code has had the chance to register it."""
    settings = get_settings()
    leviton_pw = settings.leviton_password.get_secret_value()
    carrier_pw = settings.carrier_password.get_secret_value()
    assert leviton_pw and carrier_pw, "conftest must pin both credentials"

    get_logger("config").info(
        "startup",
        detail=f"leviton uses {leviton_pw}",
        nested={"carrier": [f"grant with {carrier_pw}"]},
    )

    doc = only(stream)
    assert_absent(stream, leviton_pw, carrier_pw)
    assert doc["detail"] == f"leviton uses {REDACTED}"
    assert doc["nested"]["carrier"] == [f"grant with {REDACTED}"]


def test_a_reloaded_config_credential_is_picked_up(
    stream: io.StringIO, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``refresh_config_secrets()`` is what makes a rotated password safe."""
    from energy_capture.config import reset_settings_cache

    new_password = "rotated-leviton-password-4f3e2d1c"
    monkeypatch.setenv("LEVITON_PASSWORD", new_password)
    reset_settings_cache()
    ec_logging.refresh_config_secrets()

    get_logger("config").info("startup", detail=f"leviton uses {new_password}")

    doc = only(stream)
    assert_absent(stream, new_password)
    assert doc["detail"] == f"leviton uses {REDACTED}"
