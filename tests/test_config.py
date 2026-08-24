"""Settings, and the two documents that have to agree with them.

`.env.example` is a committed deliverable — CLAUDE.md: "keep `.env.example`
current whenever you add a setting" — but nothing was checking it, and a setting
that exists only in code is a setting the operator never learns about. The LG&E
Green Button credentials arriving on 2026-08-18 added nine at once, which is
exactly when that gap stops being theoretical.

The other half is the scrubber. ``SECRET_SETTING_FIELDS`` is how
:mod:`energy_capture.logging` learns what to redact, so a ``SecretStr`` field
missing from it is a credential that reaches the logs in plaintext. That is not
a hypothetical either: `.env` on this machine holds live Leviton, Carrier and
LG&E credentials.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pydantic import SecretStr

from energy_capture import config, logging as ec_logging

REPO = Path(__file__).resolve().parent.parent
ENV_EXAMPLE = REPO / ".env.example"

#: Settings that are deliberately absent from `.env.example`: either a machine
#: -specific path with a sensible default, or optional AWS plumbing the
#: container does not use. Anything else missing is an oversight.
UNDOCUMENTED_OK = frozenset(
    {
        "aws_profile",
        "bryant_legacy_json_path",
        "blackstart_inventory_path",
        "lge_registration_client_uri",
    }
)


@pytest.fixture(scope="module")
def env_example() -> str:
    return ENV_EXAMPLE.read_text(encoding="utf-8")


def documented_keys(text: str) -> set[str]:
    return {
        match.group(1).lower()
        for match in re.finditer(r"^([A-Z][A-Z0-9_]*)=", text, re.MULTILINE)
    }


# ------------------------------------------------------- .env.example agrees


def test_every_setting_is_in_the_env_example(env_example: str) -> None:
    """A setting only in code is one the operator never discovers."""
    documented = documented_keys(env_example)
    missing = set(config.Settings.model_fields) - documented - UNDOCUMENTED_OK
    assert not missing, (
        f".env.example does not mention {sorted(missing)} — CLAUDE.md requires "
        "keeping it current whenever a setting is added"
    )


def test_the_env_example_invents_no_setting(env_example: str) -> None:
    """The opposite failure: a key an operator sets that nothing reads."""
    unknown = documented_keys(env_example) - set(config.Settings.model_fields)
    assert not unknown, f".env.example documents settings that do not exist: {sorted(unknown)}"


def test_the_env_example_holds_no_real_looking_credential(env_example: str) -> None:
    """It is committed. Every secret in it must obviously be a placeholder."""
    for line in env_example.splitlines():
        if not re.match(r"^[A-Z][A-Z0-9_]*=", line):
            continue
        key, _, value = line.partition("=")
        value = value.split("#")[0].strip()
        # Driven off SECRET_SETTING_FIELDS rather than a name heuristic: the
        # test above guarantees that list is complete, and a heuristic on
        # "TOKEN" would flag LGE_TOKEN_URL, which is a published endpoint.
        if not value or key.lower() not in config.SECRET_SETTING_FIELDS:
            continue
        assert value in {"change-me", ""}, (
            f"{key} in .env.example looks like a real value ({value!r}); it is "
            "committed to a public repository"
        )


# --------------------------------------------------------- secrets are known


def test_every_secret_field_is_registered_for_scrubbing() -> None:
    """The list the log scrubber reads must cover every SecretStr field.

    A ``SecretStr`` keeps a value out of a ``repr``; it does nothing about a
    stage that logs the value itself. ``SECRET_SETTING_FIELDS`` is what closes
    that, and it is hand-maintained — so this is the check that it was.
    """
    secret_fields = {
        name
        for name, field in config.Settings.model_fields.items()
        if field.annotation is SecretStr
    }
    assert secret_fields == set(config.SECRET_SETTING_FIELDS), (
        "SECRET_SETTING_FIELDS is out of sync with the SecretStr fields; the "
        "difference is a credential the log scrubber does not know about"
    )


def test_the_named_secret_fields_all_exist() -> None:
    for name in config.SECRET_SETTING_FIELDS:
        assert name in config.Settings.model_fields, name


def test_configured_secrets_reach_the_scrubber(monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end: a value in the environment is redacted from log output."""
    monkeypatch.setenv("LGE_CLIENT_SECRET", "lge-client-secret-abcdef123456")
    monkeypatch.setenv("LGE_REGISTRATION_ACCESS_TOKEN", "lge-registration-token-987654")
    config.reset_settings_cache()
    ec_logging.refresh_config_secrets()

    values = config.get_settings().secret_values()
    assert "lge-client-secret-abcdef123456" in values
    assert "lge-registration-token-987654" in values

    scrubbed = ec_logging.scrub_text(
        "POST token client_secret=lge-client-secret-abcdef123456 "
        "Authorization: Bearer lge-registration-token-987654"
    )
    assert "lge-client-secret-abcdef123456" not in scrubbed
    assert "lge-registration-token-987654" not in scrubbed

    config.reset_settings_cache()
    ec_logging.refresh_config_secrets()


# --------------------------------------------------- the LG&E endpoints


def test_the_redirect_uri_matches_the_one_registered_with_the_utility() -> None:
    """``redirect_uri`` is an exact string match at the custodian.

    It is registered once, by hand, with a human in the loop; a drift here is a
    rejected authorization and a support email, not a retry. The trailing slash
    is load-bearing — GitHub Pages 301s the un-slashed form (docs §2).
    """
    registered = "https://energycap.ericpullen.com/greenbutton/callback/"
    assert config.Settings().lge_redirect_uri == registered
    doc = (REPO / "docs" / "lge-greenbutton.md").read_text(encoding="utf-8")
    assert registered in doc


@pytest.mark.parametrize(
    "field",
    ["lge_authorize_url", "lge_token_url", "lge_resource_uri", "lge_bulk_uri"],
)
def test_the_lge_endpoints_are_https(field: str) -> None:
    assert getattr(config.Settings(), field).startswith("https://")


def test_no_setting_can_be_inherited_from_the_developers_shell() -> None:
    """The suite must see the environment it declares, and nothing else.

    ``conftest._TEST_ENV`` is an allowlist and named 16 of the 48 settings. The
    other 32 came straight from whatever the developer had exported: an actual
    ``LGE_CLIENT_ID=gbc_18`` in one shell made three tests fail outright, and
    every other un-named setting -- ``LEVITON_INGEST``, ``SCHEDULED_JOBS``, both
    ``PUSHOVER_*``, the integrity thresholds -- silently ran against one
    machine's configuration and passed, which is the worse half of the bug.

    ``conftest._clear_settings_environment`` now deletes every ``Settings``
    field's variable before the fakes go in, driven off ``model_fields`` so it
    cannot go stale. This asserts the consequence: a setting the suite did not
    ask for holds its declared default, whatever the shell says.
    """
    import os

    from tests.conftest import _TEST_ENV

    chosen = {key.lower() for key in _TEST_ENV} | {"spool_dir"}
    settings = config.Settings(_env_file=None)  # type: ignore[call-arg]

    for name, field in config.Settings.model_fields.items():
        if name in chosen:
            continue
        assert name.upper() not in os.environ, (
            f"{name.upper()} survived into the test environment; "
            "conftest._clear_settings_environment should have removed it"
        )
        actual = getattr(settings, name)
        expected = field.get_default(call_default_factory=True)
        if isinstance(actual, SecretStr):
            actual = actual.get_secret_value()
        if isinstance(expected, SecretStr):
            expected = expected.get_secret_value()
        assert actual == expected, (
            f"{name} is {actual!r}, not its declared default {expected!r} — "
            "something outside the suite is configuring it"
        )


def test_no_lge_credential_has_a_default_value() -> None:
    """Endpoints ship with real defaults; credentials must not ship at all."""
    settings = config.Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.lge_client_id == ""
    assert settings.lge_client_secret.get_secret_value() == ""
    assert settings.lge_registration_access_token.get_secret_value() == ""
