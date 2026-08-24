"""Shared fixtures for the whole suite.

Three guarantees this file makes for every test, everywhere:

1. **No real credentials, and no real configuration.** ``.env`` loading is
   switched off (the ``env_file`` entry of :attr:`Settings.model_config` is
   blanked for the duration of each test) **and every environment variable that
   :class:`Settings` reads is deleted** before the fake values are set --
   see :func:`_clear_settings_environment`. Blanking ``env_file`` alone was not
   enough: pydantic-settings still reads ``os.environ``, so an exported
   ``LGE_CLIENT_ID`` reached the suite for as long as ``_TEST_ENV`` did not
   happen to name that setting. A developer's real ``.env`` -- or their real
   shell -- can therefore never reach a test, and a leaked secret in an
   assertion message is impossible.
2. **No network, no AWS.** ``AWS_*`` credentials are pinned to moto's
   conventional dummies and ``AWS_PROFILE`` is removed, so a stray boto3 call
   cannot pick up a real profile — it fails loudly instead of touching an
   account. On top of that, :func:`no_outbound_network` (autouse) replaces
   ``socket.socket.connect``/``connect_ex``/``create_connection`` with versions
   that refuse any non-loopback address, so a test can never reach a real cloud.
   This is not theoretical: before the guard existed, a scheduler test drove the
   real Bryant daily stage and opened a live TLS connection to ``sso.carrier.com``
   (it only "passed offline" because Okta answered ``invalid_grant``).
   Loopback stays open because ``/healthz`` is genuinely served over TCP in
   ``test_health``.
3. **Deterministic time and paths.** ``TZ_LOCAL`` is pinned to
   ``America/Kentucky/Louisville`` (PLAN.md §14) and ``SPOOL_DIR`` to a
   per-test ``tmp_path``, so local-date partition math and spool/token paths are
   reproducible on any machine regardless of the host clock's zone.

Tests pass explicit timestamps rather than freezing the clock — there is no
``freezegun``-style dependency and there should not be one. ``now_utc()`` is the
only wall-clock read in the package, and pure-logic tests never need it.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from collections.abc import Callable, Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto import mock_aws

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import Settings, get_settings, reset_settings_cache

# --------------------------------------------------------------------------
# Constants shared by test modules
# --------------------------------------------------------------------------

#: The house's zone. Kentucky is split across two zones, hence the explicit name.
LOCAL_TZ: str = "America/Kentucky/Louisville"

#: The bucket every S3-touching test uses. Matches ``S3_BUCKET`` below, so a
#: stage that falls back to :func:`s3io.default_bucket` lands in the same place
#: the test seeded.
BUCKET: str = "test-energy-bucket"

#: A boring summer instant used as the default for the observation factory:
#: 2026-08-16 14:00:30.123456 local (EDT, UTC-4). Microseconds are deliberate —
#: the canonical Arrow type is ``timestamp[us]`` and precision must survive.
DEFAULT_TS: datetime = datetime(2026, 8, 16, 18, 0, 30, 123456, tzinfo=timeutil.UTC)

#: Every environment variable PLAN.md §14 defines, plus the AWS SDK's own.
#: Each is either pinned to a test value or removed, so nothing inherits from
#: the developer's shell.
_TEST_ENV: dict[str, str] = {
    "S3_BUCKET": "test-energy-bucket",
    "AWS_REGION": "us-east-1",
    "GLUE_DATABASE": "energy_test",
    "LEVITON_USERNAME": "test-leviton@example.invalid",
    "LEVITON_PASSWORD": "not-a-real-leviton-password",
    "CARRIER_USERNAME": "test-carrier@example.invalid",
    "CARRIER_PASSWORD": "not-a-real-carrier-password",
    "CARRIER_SERIAL": "TEST0000001",
    "DYNAMODB_TABLE": "bryant-energy-data-test",
    "TZ_LOCAL": LOCAL_TZ,
    "POLL_INTERVAL_S": "30",
    "BRYANT_POLL_INTERVAL_S": "30",
    "LEVITON_DISCOVERY_INTERVAL_S": "3600",
    "SPOOL_RETENTION_DAYS": "7",
    "HEALTH_PORT": "18080",
    "LOG_LEVEL": "INFO",
    # moto's conventional dummies: real credentials must never be reachable.
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SECURITY_TOKEN": "testing",
    "AWS_SESSION_TOKEN": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
}

#: Removed outright: a bogus profile name would break moto-backed clients, and
#: an inherited real one is exactly what we are guarding against. These are not
#: ``Settings`` fields, so :func:`_clear_settings_environment` does not reach
#: them.
_REMOVED_ENV: tuple[str, ...] = (
    "AWS_PROFILE",
    "AWS_ENDPOINT_URL",
)


def _clear_settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete every ``Settings`` field's variable from the process environment.

    ``_TEST_ENV`` is an ALLOWLIST, and an allowlist that has to be extended by
    hand is a guarantee that decays. It named 16 of the 48 settings; the other 32
    -- every ``LGE_*``, both ``PUSHOVER_*``, ``HEALTHZ_URL``, ``SCHEDULED_JOBS``,
    the integrity thresholds -- were inherited straight from whatever the
    developer had exported. Three tests failed loudly because of it (a real
    ``LGE_CLIENT_ID`` in the shell made "no credential has a default" false);
    the rest simply ran against one machine's configuration and passed anyway,
    which is worse.

    So the rule is inverted: the environment starts EMPTY of anything
    ``Settings`` reads, and ``_TEST_ENV`` puts back only what the suite chose.
    The field list is read from the model, so a new setting is isolated the day
    it is added rather than the day someone remembers this file.

    ``Settings`` is ``case_sensitive=False``, so every case variant present has
    to go, not just the upper-case spelling.
    """
    fields = {name.lower() for name in Settings.model_fields}
    for key in list(os.environ):
        if key.lower() in fields:
            monkeypatch.delenv(key, raising=False)


# --------------------------------------------------------------------------
# Helpers (importable: ``from tests.conftest import utc``)
# --------------------------------------------------------------------------


def utc(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    """An aware UTC datetime — the only kind ``ts_utc`` ever holds."""
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=timeutil.UTC)


def naive(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> datetime:
    """A naive local wall clock — the only kind ``ts_local`` ever holds."""
    return datetime(year, month, day, hour, minute, second, microsecond)


# --------------------------------------------------------------------------
# The network guard
# --------------------------------------------------------------------------


class OutboundNetworkBlocked(RuntimeError):
    """A test tried to open a socket to a real host."""


def _is_loopback(address: Any) -> bool:
    """True for AF_UNIX paths and for loopback ``(host, port)`` tuples.

    Anything we cannot confidently classify as loopback is treated as outbound
    and therefore blocked — the guard fails closed.
    """
    if not isinstance(address, tuple) or not address:
        # AF_UNIX (a str/bytes path) and AF_NETLINK-ish addresses never leave
        # the machine.
        return not isinstance(address, (tuple, list))
    host = address[0]
    if host in (None, "", "localhost"):
        return True
    if not isinstance(host, str):
        return False
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True)
def no_outbound_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refuse every socket connection that is not to loopback. Autouse.

    CLAUDE.md: "Never hit a live API from a test." Fake credentials are not
    enough — a request with a bogus password still opens the TLS connection, and
    a rejected login looks exactly like an offline failure in the assertion. The
    only way to *know* the suite is offline is to make the socket itself refuse.

    Loopback is exempt so ``test_health`` can serve ``/healthz`` over real TCP;
    moto and ``httpx.MockTransport`` never reach this code at all.
    """
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create_connection = socket.create_connection

    def guard(address: Any) -> None:
        if not _is_loopback(address):
            raise OutboundNetworkBlocked(
                f"a test tried to connect to {address!r}. Tests must never reach "
                "a live API (CLAUDE.md); use a fixture, httpx.MockTransport or moto."
            )

    def connect(self: socket.socket, address: Any) -> None:
        guard(address)
        return real_connect(self, address)

    def connect_ex(self: socket.socket, address: Any) -> int:
        guard(address)
        return real_connect_ex(self, address)

    def create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
        guard(address)
        return real_create_connection(address, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "create_connection", create_connection)


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def test_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Pin the whole configuration surface for one test. Autouse; yields SPOOL_DIR.

    Also clears the ``get_settings()`` cache on the way in *and* out, so a test
    that mutates the environment further cannot leak that state into the next
    test.
    """
    # Disable `.env` discovery: pydantic-settings reads this key at construction
    # time, so blanking it is enough and monkeypatch restores it afterwards.
    monkeypatch.setitem(Settings.model_config, "env_file", None)

    spool_dir = tmp_path / "data"
    (spool_dir / "tokens").mkdir(parents=True)

    # Deny by default, then put back exactly what the suite asked for.
    _clear_settings_environment(monkeypatch)
    for key, value in _TEST_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("SPOOL_DIR", str(spool_dir))
    for key in _REMOVED_ENV:
        monkeypatch.delenv(key, raising=False)
        monkeypatch.delenv(key.lower(), raising=False)

    reset_settings_cache()
    try:
        yield spool_dir
    finally:
        reset_settings_cache()


@pytest.fixture
def settings() -> Settings:
    """The :class:`Settings` built from the pinned test environment."""
    return get_settings()


@pytest.fixture
def spool_dir(test_environment: Path) -> Path:
    """The per-test ``SPOOL_DIR`` (already created, with ``tokens/`` inside)."""
    return test_environment


# --------------------------------------------------------------------------
# AWS (moto)
# --------------------------------------------------------------------------


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch):
    """A mocked S3 client with an empty :data:`BUCKET`. Nothing leaves the process.

    Shared by ``test_s3io``, ``test_uploader``, ``test_compactor`` and
    ``test_integration`` — they had three byte-identical copies of this before.
    Tests pass the yielded client explicitly so they never depend on ambient AWS
    configuration; ``s3io``'s own cached client is reset on both sides so a moto
    client can never outlive its mock.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    reset_settings_cache()
    s3io.reset_clients()
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client
    s3io.reset_clients()
    reset_settings_cache()


# --------------------------------------------------------------------------
# Row factories
# --------------------------------------------------------------------------

ObservationFactory = Callable[..., model.Observation]


@pytest.fixture
def make_obs() -> ObservationFactory:
    """Factory for :class:`~energy_capture.model.Observation` rows.

    Goes through :func:`~energy_capture.model.make_observation`, so every row a
    test builds derives ``ts_local`` and ``unit`` exactly the way production code
    does — a test can never assemble a row shape production could not produce.

    ``ts_utc`` accepts an aware datetime or an ISO-8601 string::

        make_obs()                                   # default channel, 100 W
        make_obs("2026-11-01T05:30:00Z", value=250)  # the first 01:30 EDT
        make_obs(metric="volts", value=241.3, channel_id="panel_leg_a")
    """

    def _make(
        ts_utc: datetime | str = DEFAULT_TS,
        *,
        source: str = model.SOURCE_LEVITON,
        device_id: str = "hub-a",
        channel_id: str = "breaker_p11",
        metric: str = "watts",
        value: float = 100.0,
        unit: str | None = None,
        interval_s: int | None = None,
    ) -> model.Observation:
        if isinstance(ts_utc, str):
            ts_utc = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        return model.make_observation(
            ts_utc=ts_utc,
            source=source,
            device_id=device_id,
            channel_id=channel_id,
            metric=metric,
            value=value,
            unit=unit,
            interval_s=interval_s,
        )

    return _make


@pytest.fixture
def day_grain_obs(make_obs: ObservationFactory) -> Callable[..., model.Observation]:
    """Factory for ``energy/daily`` rows: ``ts_utc`` is local midnight (§7.2)."""

    def _make(
        local_day: date,
        *,
        channel_id: str = "hpheat",
        metric: str = "kwh_day",
        value: float = 12.5,
        device_id: str = "TEST0000001",
    ) -> model.Observation:
        return make_obs(
            timeutil.local_midnight_utc(local_day),
            source=model.SOURCE_BRYANT,
            device_id=device_id,
            channel_id=channel_id,
            metric=metric,
            value=value,
        )

    return _make
