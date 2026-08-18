"""Leviton LWHEM-2 load centers via ``my.leviton.com`` (PLAN.md §6).

Two 200A load centers appear as two ``IotWhem`` hubs under one residence. CT
clamp pairs on a subpanel feeder are ``IotCt`` objects; smart breakers (few or
none today, added over time) are ``ResidentialBreaker`` objects.

Layout of this module, outside-in:

* :class:`LevitonSource` — the :class:`~energy_capture.sources.base.Source` the
  poller talks to. It owns the row mapping (§6.5), the poll-cycle error policy
  (§6.6) and the bandwidth keepalive task (§6.4). It knows nothing about HTTP.
* :class:`LevitonAdapter` — the **thin adapter** required by PLAN.md §2.8. Every
  Leviton-specific quirk lives behind it: the bare ``authorization`` header, the
  ``Origin`` spoof, the token cache, the 10-second login floor, the 502/504
  retry, and the translation of ``aioleviton``'s exception tree into this
  package's :class:`SourceTransientError` / :class:`SourceAuthError`. If
  ``aioleviton`` goes stale it is vendored or replaced here and nowhere else.
* :class:`HubReading` / :class:`BreakerReading` / :class:`CtReading` — our own
  row-shaped view of a response. The adapter converts ``aioleviton``'s models
  into these, so the mapping functions (and their tests) never import a
  third-party type.

How values are kept fresh (``LEVITON_INGEST``)
----------------------------------------------

PLAN.md §2.8 and §6.4 LOCKED "REST polling at 30s + bandwidth keepalive, not
WebSocket". Measurement on 2026-08-16 against the two live LWHEM-2 hubs
(firmware 2.1.2) overturned that and the owner authorised the change; it is
recorded in ``DEVIATIONS.md``. The evidence: over a 5-minute production run and
a separate 12-minute probe, **10 of 12 channels never changed value at all** —
one whole-panel ``GRID_POWER`` CT feed held *exactly* 4086.05 W across 46
consecutive reads at 15s, another held exactly 505.17 W across the same 46. An
A/B probe proved the keepalive PUT lands (the hub's ``bandwidth`` field reads 0
at rest and 2 afterwards, i.e. 1 auto-decayed to 2 exactly as §6.4 describes)
and that both phases were **identically frozen**. The reference integration
explains why: ``bandwidth=1`` triggers a full state flood that is pushed over
the *WebSocket*, and its REST path is documented as "initial discovery, fallback
polling (10-min interval)" — we were polling REST 20× faster than the
reference's *fallback* rate and receiving a cache. With ~25 more smart breakers
arriving (12 channels → ~40), per-breaker resolution is exactly where a stale
cache hurts most.

**The socket changes how values are kept fresh. It never changes how rows are
sampled.** Whatever the mode, one set of rows per ``POLL_INTERVAL_S`` cycle with
a single ``ts_utc`` (§6.5), mapped by :meth:`LevitonSource._map_snapshot` and by
nothing else. §2.5's kWh formula and ``sample_count``'s meaning as the gap
detector both assume a fixed cadence, so a row-per-delta design is forbidden.

===========  ================================================================
mode         behaviour
===========  ================================================================
``hybrid``   (default) REST does discovery (§6.2) and a full re-read every
             ``LEVITON_REST_RECONCILE_S``; ``sources/leviton_ws.py`` keeps an
             in-memory current-state store fresh; each cycle samples that store
             when its gate is open, and otherwise falls back to a REST read for
             that cycle. The fallback is *recorded* — a cached REST value is
             what this pipeline collects today, so falling back is strictly
             better than nothing, but a reader must be able to tell the two
             apart afterwards.
``ws``       Values come only from the socket. A shut gate emits a **gap**, not
             a cached reading — exactly what a failed REST cycle does.
``rest``     The original §2.8 behaviour, byte for byte: every value read over
             REST every cycle, no socket, no reconcile task. The owner's
             no-code-change fallback, kept fully exercised.
===========  ================================================================

Rows carry no provenance column (§3 fixes the schema), so "which cycles came
from a live socket and which from a REST cache" is recorded in the
``leviton_ingest`` section of ``status.json`` (per-source cycle counters, the
active mode, the withheld reason) and logged at INFO on every transition.

The three rules this module exists to get right:

**Gaps stay gaps.** Every metric goes through :meth:`PollCycle.add`, which drops
``None`` and emits *no row*. A null second-leg CT reading is a single-leg CT, not
a zero. A poll cycle that fails after its retries returns/raises with zero rows —
it never repeats the previous reading.

**Verbatim.** Firmware v2 emits spurious zero power/current readings. They are
recorded exactly as received (PLAN.md §2.3). Nothing here filters, smooths,
clamps or rescales a number the API returned.

**Never ``bandwidth: 0``.** Firmware 2.1.0 disconnects a hub for 10–20s when it
receives ``{"bandwidth": 0}``. This module has exactly one call site that sets
bandwidth, it passes the module constant :data:`BANDWIDTH_HIGH`, no function in
the module accepts a bandwidth argument, and :meth:`LevitonAdapter.keepalive`
raises rather than issuing the PUT if that constant is ever anything but ``1``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from aioleviton import (
    LevitonAuthError,
    LevitonClient,
    LevitonConnectionError,
    LevitonError,
    LevitonInvalidCode,
    LevitonTwoFactorRequired,
)
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt

from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger, register_secret
from energy_capture.model import SOURCE_LEVITON, Observation
from energy_capture.sources.base import (
    BackgroundTask,
    BaseSource,
    DiscoveredChannel,
    DiscoveredDevice,
    Discovery,
    PollCycle,
    SourceAuthError,
    SourceTransientError,
)
from energy_capture.timeutil import now_utc

__all__ = [
    "BANDWIDTH_HIGH",
    "CT_USAGE_NOT_USED",
    "INGEST_HYBRID",
    "INGEST_REST",
    "INGEST_WS",
    "KEEPALIVE_INTERVAL_S",
    "KEEPALIVE_MAX_BACKOFF_S",
    "LOGIN_FAILURE_BACKOFF_S",
    "LOGIN_MIN_INTERVAL_S",
    "PANEL_LEG_CHANNELS",
    "PLACEHOLDER_BREAKER_MODELS",
    "RETRY_WAITS_S",
    "STATUS_SECTION_AUTH",
    "STATUS_SECTION_INGEST",
    "STATUS_SECTION_KEEPALIVE",
    "VALUE_SOURCE_REST",
    "VALUE_SOURCE_REST_FALLBACK",
    "VALUE_SOURCE_WITHHELD",
    "VALUE_SOURCE_WS",
    "WS_TICK_INTERVAL_S",
    "BreakerReading",
    "CtReading",
    "HubReading",
    "HubSnapshot",
    "LevitonAdapter",
    "LevitonSource",
    "LevitonTokenCache",
    "breaker_channel_id",
    "ct_channel_id",
    "panel_leg_channel_id",
]


# --------------------------------------------------------------------- constants

#: The **only** bandwidth value this package ever sends (PLAN.md §6.4). ``1``
#: puts the hub in high-bandwidth mode and auto-decays to ``2`` within seconds.
#: ``0`` disconnects a fw-2.1.0 hub for 10–20 seconds and must never be sent.
BANDWIDTH_HIGH: Final[int] = 1

#: Cadence of the keepalive PUT, matching the official Leviton app (PLAN.md §6.4).
KEEPALIVE_INTERVAL_S: Final[float] = 50.0

#: Ceiling for the keepalive's exponential backoff after repeated PUT failures.
KEEPALIVE_MAX_BACKOFF_S: Final[float] = 600.0

#: Hard floor between login attempts — Leviton punishes rapid logins (§6.1).
LOGIN_MIN_INTERVAL_S: Final[float] = 10.0

#: After a *failed* login, wait this long before trying again (§6.6).
LOGIN_FAILURE_BACKOFF_S: Final[float] = 60.0

#: In-cycle retry schedule for Leviton's routine 502/504s (PLAN.md §6.6):
#: the initial attempt, then a retry after 2s and another after 5s.
RETRY_WAITS_S: Final[tuple[float, ...]] = (2.0, 5.0)

#: ``model`` values that mark a dumb-breaker placeholder object, not a meter
#: (PLAN.md §6.3). They report no power and are skipped entirely.
PLACEHOLDER_BREAKER_MODELS: Final[frozenset[str]] = frozenset({"NONE", "NONE-1", "NONE-2"})

#: ``IotCt.usageType`` marking a clamp that is not installed on anything (§6.3).
CT_USAGE_NOT_USED: Final[str] = "NOT_USED"

#: Hub-level channels: the two panel legs (PLAN.md §6.5).
PANEL_LEG_CHANNELS: Final[tuple[str, str]] = ("panel_leg_a", "panel_leg_b")

#: Leviton appears to fingerprint callers; the app sends this Origin (§6.1).
LEVITON_ORIGIN: Final[str] = "https://myapp.leviton.com"

#: ``status.json`` sections this source owns. The generic ``leviton`` section
#: (poll successes/failures) belongs to ``stages/poller.py``; these describe
#: conditions only this module can see (PLAN.md §6.4, §6.6) plus the ingestion
#: provenance the row schema has no column for.
STATUS_SECTION_KEEPALIVE: Final[str] = "leviton_keepalive"
STATUS_SECTION_AUTH: Final[str] = "leviton_auth"
STATUS_SECTION_INGEST: Final[str] = "leviton_ingest"

#: ``LEVITON_INGEST`` modes, restated here so this module reads standalone.
#: ``config.LEVITON_INGEST_MODES`` is the validator; a test pins the two lists
#: against each other.
INGEST_HYBRID: Final[str] = "hybrid"
INGEST_WS: Final[str] = "ws"
INGEST_REST: Final[str] = "rest"

#: Where one cycle's *values* came from. The canonical schema (§3) has no
#: provenance column and gains none — a row is a row — so this is recorded in
#: ``status.json`` and in the log instead of in the data.
#:
#: * ``ws`` — sampled from the live push store, gate open.
#: * ``rest`` — a REST read, because no socket is running (``LEVITON_INGEST=rest``
#:   or a client that cannot open one). This is the pre-WebSocket behaviour.
#: * ``rest_fallback`` — ``hybrid`` wanted the socket and could not have it, so it
#:   read REST for this cycle. Same rows as ``rest``, very different diagnosis.
#: * ``withheld`` — ``ws`` mode with a shut gate: **zero rows**, a gap.
VALUE_SOURCE_WS: Final[str] = "ws"
VALUE_SOURCE_REST: Final[str] = "rest"
VALUE_SOURCE_REST_FALLBACK: Final[str] = "rest_fallback"
VALUE_SOURCE_WITHHELD: Final[str] = "withheld"

#: Cadence of the WebSocket supervisor tick (reconnects, the silent-stall
#: watchdog, the proactive reconnect before the server's 60-minute hard kill).
#: Mirrors ``leviton_ws.WATCHDOG_INTERVAL_S``; it cannot be imported at module
#: scope because ``leviton_ws`` imports the reading dataclasses from here, so a
#: test pins the two values equal.
WS_TICK_INTERVAL_S: Final[float] = 15.0

_LEG_SUFFIXES: Final[tuple[str, str]] = ("a", "b")


def _ws_module() -> Any:
    """Import ``sources/leviton_ws.py`` lazily.

    ``leviton_ws`` imports :class:`HubReading` and friends from *this* module, so
    the import edge only runs one way at module scope. Deferring it here also
    keeps ``LEVITON_INGEST=rest`` from importing a module it will never use.
    """
    from energy_capture.sources import leviton_ws

    return leviton_ws


# ------------------------------------------------------------- channel naming


def breaker_channel_id(position: int) -> str:
    """``breaker_p{position}`` — a 2-pole breaker is **one** channel (§6.5).

    ``position`` (the physical slot) is the identity, never the API's breaker
    ``id``: firmware ≥2.2.0 appends the panel serial to breaker ids
    (``4C45565275C6`` → ``4C45565275C6_A65E``), so an id-keyed channel would
    silently rename itself on a firmware update. ``position`` also happens to be
    the only thing that joins to the blackstart inventory (PLAN.md §9).
    """
    return f"breaker_p{int(position)}"


def ct_channel_id(channel: int, leg: str) -> str:
    """``ct_{channel}_{a,b}`` — one ``IotCt`` object is a clamp *pair* (§6.5)."""
    normalised = str(leg).lower()
    if normalised not in _LEG_SUFFIXES:
        raise ValueError(f"leg must be 'a' or 'b', got {leg!r}")
    return f"ct_{int(channel)}_{normalised}"


def panel_leg_channel_id(leg: str) -> str:
    """``panel_leg_{a,b}`` — hub-level voltage/frequency channels (§6.5)."""
    normalised = str(leg).lower()
    if normalised not in _LEG_SUFFIXES:
        raise ValueError(f"leg must be 'a' or 'b', got {leg!r}")
    return f"panel_leg_{normalised}"


# ----------------------------------------------------------------- arithmetic


def _sum2(first: float | None, second: float | None) -> float | None:
    """``first + second``, or ``None`` if either is missing (a gap, not a zero)."""
    if first is None or second is None:
        return None
    return float(first) + float(second)


def _mean2(first: float | None, second: float | None) -> float | None:
    """Mean of two poles, or ``None`` if either is missing."""
    if first is None or second is None:
        return None
    return (float(first) + float(second)) / 2.0


# ------------------------------------------------------------------- readings


@dataclass(frozen=True, slots=True)
class HubReading:
    """One ``IotWhem`` as this pipeline sees it (PLAN.md §6.3 hub fields).

    ``device_id`` is the hub id — the ``device_id`` column for every row from
    this panel, breakers and CTs included (§6.5). There is deliberately no
    panel-level power field: Leviton does not provide one and synthesising a
    panel total in raw would be fabrication (that is a query-time concern).
    """

    device_id: str
    connected: bool
    volts_a: float | None = None
    volts_b: float | None = None
    hz_a: float | None = None
    hz_b: float | None = None
    name: str | None = None
    serial: str | None = None
    version: str | None = None
    rssi: int | None = None

    @classmethod
    def from_model(cls, whem: Any) -> HubReading:
        """Build from an ``aioleviton.Whem`` (or anything with its attributes)."""
        return cls(
            device_id=str(whem.id),
            connected=bool(getattr(whem, "connected", False)),
            volts_a=getattr(whem, "rms_voltage_a", None),
            volts_b=getattr(whem, "rms_voltage_b", None),
            hz_a=getattr(whem, "frequency_a", None),
            hz_b=getattr(whem, "frequency_b", None),
            name=getattr(whem, "name", None) or None,
            serial=getattr(whem, "serial", None) or None,
            version=getattr(whem, "version", None) or None,
            rssi=getattr(whem, "rssi", None),
        )


@dataclass(frozen=True, slots=True)
class BreakerReading:
    """One ``ResidentialBreaker`` (PLAN.md §6.3 breaker fields).

    The ``2``-suffixed fields are the **second pole** of a 2-pole breaker, not
    the second panel leg. ``energyConsumption``/``energyImport`` are deliberately
    absent: firmware v2 turned them into counters that reset whenever bandwidth
    is toggled, so both reference integrations abandoned them and this pipeline
    derives kWh in the rollup instead (§6.3).
    """

    position: int
    poles: int
    model: str
    #: Present for discovery output only — never used to build a ``channel_id``.
    api_id: str | None = None
    name: str | None = None
    branch_type: str | None = None
    current_state: str | None = None
    serial_number: str | None = None
    connected: bool = False
    power: float | None = None
    power_2: float | None = None
    rms_current: float | None = None
    rms_current_2: float | None = None
    rms_voltage: float | None = None
    rms_voltage_2: float | None = None

    @classmethod
    def from_model(cls, breaker: Any) -> BreakerReading:
        """Build from an ``aioleviton.Breaker``."""
        return cls(
            position=int(getattr(breaker, "position", 0) or 0),
            poles=int(getattr(breaker, "poles", 1) or 1),
            model=str(getattr(breaker, "model", "") or ""),
            api_id=str(breaker.id) if getattr(breaker, "id", None) else None,
            name=getattr(breaker, "name", None) or None,
            branch_type=getattr(breaker, "branch_type", None),
            current_state=getattr(breaker, "current_state", None),
            serial_number=getattr(breaker, "serial_number", None),
            connected=bool(getattr(breaker, "connected", False)),
            power=getattr(breaker, "power", None),
            power_2=getattr(breaker, "power_2", None),
            rms_current=getattr(breaker, "rms_current", None),
            rms_current_2=getattr(breaker, "rms_current_2", None),
            rms_voltage=getattr(breaker, "rms_voltage", None),
            rms_voltage_2=getattr(breaker, "rms_voltage_2", None),
        )

    @property
    def is_placeholder(self) -> bool:
        """True for the ``NONE``/``NONE-1``/``NONE-2`` dumb-breaker stand-ins."""
        return self.model.strip().upper() in PLACEHOLDER_BREAKER_MODELS

    @property
    def is_multi_pole(self) -> bool:
        """True when the second pole's fields are part of this breaker's reading.

        The API exposes exactly two poles' worth of fields, so ``poles >= 2``
        selects the two-pole arithmetic of §6.5 (a 3-pole breaker does not exist
        in a US residential panel, and treating one as single-pole would silently
        drop half its load).
        """
        return self.poles >= 2

    @property
    def channel_id(self) -> str:
        return breaker_channel_id(self.position)

    def metrics(self) -> dict[str, float | None]:
        """The three metrics for this breaker per §6.5; ``None`` means *no row*.

        2-pole: ``watts`` = pole sum, ``amps`` = per-pole **mean** (both poles
        carry the same current through one load), ``volts`` = leg **sum** (240V
        across the two legs). Single-pole: the unsuffixed field, as received.
        """
        if self.is_multi_pole:
            return {
                "watts": _sum2(self.power, self.power_2),
                "amps": _mean2(self.rms_current, self.rms_current_2),
                "volts": _sum2(self.rms_voltage, self.rms_voltage_2),
            }
        return {
            "watts": self.power,
            "amps": self.rms_current,
            "volts": self.rms_voltage,
        }


@dataclass(frozen=True, slots=True)
class CtReading:
    """One ``IotCt`` — a clamp **pair**, leg A and leg B (PLAN.md §6.3).

    A null second-leg value means a single-leg installation: emit nothing for
    leg B (a gap), never a zero.
    """

    channel: int
    usage_type: str | None = None
    api_id: int | None = None
    name: str | None = None
    connected: bool = False
    active_power: float | None = None
    active_power_2: float | None = None
    rms_current: float | None = None
    rms_current_2: float | None = None

    @classmethod
    def from_model(cls, ct: Any) -> CtReading:
        """Build from an ``aioleviton.Ct``."""
        return cls(
            channel=int(getattr(ct, "channel", 0) or 0),
            usage_type=getattr(ct, "usage_type", None),
            api_id=getattr(ct, "id", None),
            name=getattr(ct, "name", None) or None,
            connected=bool(getattr(ct, "connected", False)),
            active_power=getattr(ct, "active_power", None),
            active_power_2=getattr(ct, "active_power_2", None),
            rms_current=getattr(ct, "rms_current", None),
            rms_current_2=getattr(ct, "rms_current_2", None),
        )

    @property
    def is_unused(self) -> bool:
        """True for ``usageType == "NOT_USED"`` — a clamp on nothing (§6.3)."""
        return (self.usage_type or "").strip().upper() == CT_USAGE_NOT_USED

    @property
    def has_second_leg(self) -> bool:
        """True when leg B reported anything at all this cycle."""
        return self.active_power_2 is not None or self.rms_current_2 is not None

    def leg_metrics(self, leg: str) -> dict[str, float | None]:
        """Metrics for one leg; ``None`` values become gaps, never zeros."""
        if leg == "a":
            return {"watts": self.active_power, "amps": self.rms_current}
        if leg == "b":
            return {"watts": self.active_power_2, "amps": self.rms_current_2}
        raise ValueError(f"leg must be 'a' or 'b', got {leg!r}")


@dataclass(frozen=True, slots=True)
class HubSnapshot:
    """One hub's complete response set for a single poll cycle."""

    hub: HubReading
    breakers: tuple[BreakerReading, ...] = ()
    cts: tuple[CtReading, ...] = ()


# ---------------------------------------------------------------- token cache


@dataclass(frozen=True, slots=True)
class LevitonTokenCache:
    """The ``{SPOOL_DIR}/tokens/leviton.json`` cache (PLAN.md §6.1).

    Holds the **full login response** — Leviton has no refresh endpoint, so the
    login response (whose ``id`` *is* the bearer token, ``ttl`` ≈ 60 days) is all
    there is to keep. Written mode ``0600`` on the mounted volume, never in the
    repo (CLAUDE.md rule 8).
    """

    path: Path

    def load(self) -> dict[str, Any] | None:
        """Return the cached login response, or ``None`` if absent/unusable."""
        try:
            raw = self.path.read_text(encoding="utf-8")
        except (FileNotFoundError, NotADirectoryError, IsADirectoryError, PermissionError):
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        if not payload.get("id") or not payload.get("userId"):
            return None
        # A cache written by an older run (or restored from a backup) may be
        # world-readable; tighten it on the way in rather than trusting it.
        self._chmod()
        return payload

    def save(self, payload: Mapping[str, Any]) -> None:
        """Write the login response atomically, mode 0600."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".leviton-", suffix=".json.tmp", dir=str(self.path.parent)
        )
        tmp = Path(tmp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(dict(payload), handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        self._chmod()

    def clear(self) -> None:
        """Drop the cache (a token the server rejected is worse than none)."""
        self.path.unlink(missing_ok=True)

    def _chmod(self) -> None:
        try:
            os.chmod(self.path, 0o600)
        except OSError:  # pragma: no cover - unusual filesystem
            pass


# --------------------------------------------------------------------- adapter


class LevitonAdapter:
    """Everything Leviton-specific: auth, discovery fetches, retries, keepalive.

    This is the seam PLAN.md §2.8 asks for. ``aioleviton`` is used *only* here;
    the class returns plain :class:`HubReading` / :class:`BreakerReading` /
    :class:`CtReading` values and raises only this package's
    :class:`SourceTransientError` / :class:`SourceAuthError`. Replacing or
    vendoring the upstream client means rewriting this class and nothing else.

    Injection points (all for tests — production passes none of them):

    * ``client`` — anything with ``LevitonClient``'s method names. When given,
      no ``aiohttp`` session is created and none is closed.
    * ``sleep`` / ``monotonic`` — the login floor and the retry waits are real
      time in production and instant in tests.
    * ``retry_waits`` — the §6.6 schedule, overridable so tests do not wait 7s.
    """

    def __init__(
        self,
        *,
        username: str,
        password: str,
        token_path: Path,
        client: Any = None,
        retry_waits: Sequence[float] = RETRY_WAITS_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._username = username
        self._password = password
        self._tokens = LevitonTokenCache(Path(token_path))
        self._client = client
        self._owns_client = client is None
        self._session: Any = None
        self._retry_waits = tuple(float(w) for w in retry_waits)
        self._sleep = sleep
        self._monotonic = monotonic
        self._log = get_logger("leviton")
        self._auth_lock = asyncio.Lock()
        #: Monotonic deadline before which no login may be attempted (§6.1/§6.6).
        self._login_not_before: float | None = None
        self._logins = 0
        self._authenticated = False

    # ----------------------------------------------------------- housekeeping
    @property
    def login_count(self) -> int:
        """How many login round-trips this process has made (a health signal)."""
        return self._logins

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    @property
    def token_path(self) -> Path:
        return self._tokens.path

    def _ensure_client(self) -> Any:
        if self._client is None:
            import aiohttp  # imported lazily: pure-logic tests never need it

            self._session = aiohttp.ClientSession(
                headers={
                    # PLAN.md §6.1: Leviton appears to fingerprint callers, and
                    # aioleviton only sets user-agent — Origin is added here.
                    "origin": LEVITON_ORIGIN,
                    "referer": f"{LEVITON_ORIGIN}/",
                }
            )
            self._client = LevitonClient(self._session)
        return self._client

    async def close(self) -> None:
        """Release the HTTP session. Idempotent; never logs out.

        Logging out would invalidate the cached token and force a fresh login on
        the next container start — exactly the rapid-login behaviour §6.1 warns
        against.
        """
        session, self._session = self._session, None
        if session is not None:
            await session.close()
        if self._owns_client:
            self._client = None

    # ------------------------------------------------------------------ auth
    async def start(self) -> None:
        """Restore the cached token and validate it; log in only if needed (§6.1)."""
        async with self._auth_lock:
            await self._authenticate_locked(reason="startup")

    async def reauthenticate(self, *, reason: str = "unauthorized") -> None:
        """Force a fresh login after a 401, honouring the 10-second floor."""
        async with self._auth_lock:
            self._authenticated = False
            self._tokens.clear()
            await self._login_locked(reason=reason)

    async def _authenticate_locked(self, *, reason: str) -> None:
        cached = self._tokens.load()
        if cached is not None:
            token = str(cached["id"])
            user_id = str(cached["userId"])
            # The token IS the credential: register it before any request can
            # put it in a log line (CLAUDE.md rule 8).
            register_secret(token)
            client = self._ensure_client()
            client.restore_session(token, user_id)
            self._authenticated = True
            try:
                await self._validate_token()
            except SourceAuthError:
                self._log.info("leviton_cached_token_rejected", token_path=str(self._tokens.path))
                self._authenticated = False
                self._tokens.clear()
            else:
                self._log.info("leviton_token_restored", user_id=user_id, reason=reason)
                return
        await self._login_locked(reason=reason)

    async def _validate_token(self) -> None:
        """``GET /Person/{userId}/residentialPermissions`` — the §6.1 probe."""
        client = self._ensure_client()
        await self._call(lambda: client.get_permissions(), op="validate_token")

    async def _login_locked(self, *, reason: str) -> None:
        await self._await_login_slot()
        client = self._ensure_client()
        self._logins += 1
        try:
            auth = await self._call(
                lambda: client.login(self._username, self._password), op="login"
            )
        except SourceAuthError:
            # A failed login gets the long backoff, not the 10s floor (§6.6).
            self._login_not_before = self._monotonic() + LOGIN_FAILURE_BACKOFF_S
            self._authenticated = False
            raise
        except SourceTransientError:
            self._login_not_before = self._monotonic() + LOGIN_FAILURE_BACKOFF_S
            self._authenticated = False
            raise
        self._login_not_before = self._monotonic() + LOGIN_MIN_INTERVAL_S
        register_secret(auth.token)
        self._authenticated = True
        self._tokens.save(_login_payload(auth))
        self._log.info(
            "leviton_login_ok",
            reason=reason,
            user_id=auth.user_id,
            login_count=self._logins,
        )

    async def _await_login_slot(self) -> None:
        """Block until the login floor / failure backoff has elapsed (§6.1).

        The floor is enforced here, in code — not by hoping the call sites are
        polite. Leviton punishes rapid logins, so a caller that asks twice in a
        row waits ten seconds for the second one.
        """
        if self._login_not_before is None:
            return
        remaining = self._login_not_before - self._monotonic()
        if remaining > 0:
            self._log.debug("leviton_login_floor_wait", wait_s=round(remaining, 3))
            await self._sleep(remaining)

    # ------------------------------------------------------------- discovery
    async def fetch_residence_ids(self) -> tuple[int, ...]:
        """person → residentialPermissions → residences (PLAN.md §6.2)."""
        client = self._ensure_client()
        permissions = await self._call(lambda: client.get_permissions(), op="permissions")
        residence_ids: list[int] = []
        seen: set[int] = set()

        def remember(residence_id: Any) -> None:
            if residence_id is None:
                return
            value = int(residence_id)
            if value not in seen:
                seen.add(value)
                residence_ids.append(value)

        for permission in permissions:
            account_id = getattr(permission, "residential_account_id", None)
            if account_id is not None:
                residences = await self._call(
                    lambda account_id=account_id: client.get_residences(account_id),
                    op="residences",
                )
                for residence in residences:
                    remember(getattr(residence, "id", None))
                continue
            # A residence-level permission carries no account; LoopBack exposes
            # the residence through the permission itself.
            direct = getattr(permission, "residence_id", None)
            if direct is not None:
                remember(direct)
        return tuple(residence_ids)

    async def fetch_hubs(self, residence_ids: Sequence[int]) -> tuple[HubReading, ...]:
        """``GET /Residences/{id}/iotWhems`` for each residence (§6.2)."""
        client = self._ensure_client()
        hubs: list[HubReading] = []
        seen: set[str] = set()
        for residence_id in residence_ids:
            whems = await self._call(
                lambda residence_id=residence_id: client.get_whems(residence_id),
                op="whems",
            )
            for whem in whems:
                reading = HubReading.from_model(whem)
                if reading.device_id in seen:
                    continue
                seen.add(reading.device_id)
                hubs.append(reading)
        return tuple(hubs)

    async def fetch_breakers(self, hub_id: str) -> tuple[BreakerReading, ...]:
        """``GET /IotWhems/{id}/residentialBreakers``; an empty list is normal.

        There are currently few or no smart breakers in this house (PLAN.md §6),
        so "no breakers" is the expected steady state, not an error.
        """
        client = self._ensure_client()
        breakers = await self._call(
            lambda: client.get_whem_breakers(hub_id), op="breakers"
        )
        return tuple(BreakerReading.from_model(b) for b in breakers)

    async def fetch_cts(self, hub_id: str) -> tuple[CtReading, ...]:
        """``GET /IotWhems/{id}/iotCts`` (§6.2)."""
        client = self._ensure_client()
        cts = await self._call(lambda: client.get_cts(hub_id), op="cts")
        return tuple(CtReading.from_model(c) for c in cts)

    async def fetch_snapshot(self, residence_ids: Sequence[int]) -> tuple[HubSnapshot, ...]:
        """The whole response set for one poll cycle: hubs + breakers + CTs."""
        hubs = await self.fetch_hubs(residence_ids)
        snapshots: list[HubSnapshot] = []
        for hub in hubs:
            breakers = await self.fetch_breakers(hub.device_id)
            cts = await self.fetch_cts(hub.device_id)
            snapshots.append(HubSnapshot(hub=hub, breakers=breakers, cts=cts))
        return tuple(snapshots)

    # -------------------------------------------------------------- keepalive
    async def keepalive(self, hub_id: str) -> None:
        """``PUT /IotWhems/{hub_id}`` ``{"bandwidth": 1}`` (PLAN.md §6.4).

        The cloud serves stale cached readings unless the hub is in
        high-bandwidth mode, and the mode auto-decays within seconds — hence the
        50-second re-PUT.

        This method takes **no bandwidth argument** and the client's bandwidth
        setter is called from exactly one place — the line below — so the value
        can only ever be :data:`BANDWIDTH_HIGH`. The guard is not paranoia about the
        current code but about the next edit — a fw-2.1.0 hub that receives
        ``{"bandwidth": 0}`` drops off the cloud for 10–20 seconds, which is a
        data gap we would have caused ourselves.
        """
        bandwidth = BANDWIDTH_HIGH
        if bandwidth != 1:
            raise RuntimeError(
                "refusing to PUT bandwidth "
                f"{bandwidth!r} to hub {hub_id}: only 1 is ever permitted "
                "(0 disconnects a fw-2.1.0 hub for 10-20s; PLAN.md §6.4)"
            )
        client = self._ensure_client()
        await self._call(
            lambda: client.set_whem_bandwidth(hub_id, bandwidth), op="keepalive"
        )

    # -------------------------------------------------------------- websocket
    @property
    def supports_websocket(self) -> bool:
        """Whether the client behind this adapter can open a push socket.

        A **capability** question, not a health one. ``aioleviton``'s
        ``LevitonClient`` has ``create_websocket()``; a vendored replacement or
        an injected test double may not. Answering "no" degrades the source to
        REST-only with one ERROR line — a *runtime* freshness failure (a dropped
        socket, a stall) is a different thing entirely and is handled by the
        gate in ``leviton_ws.py``, which honours ``LEVITON_INGEST``.
        """
        try:
            client = self._client if self._client is not None else self._ensure_client()
        except Exception:  # pragma: no cover - only if aiohttp is unimportable
            return False
        return callable(getattr(client, "create_websocket", None))

    def ws_transport_factory(
        self, url: str | None = None
    ) -> Callable[[], Awaitable[Any]] | None:
        """A factory the ingester calls once per connection, or ``None``.

        A **fresh** ``LevitonWebSocket`` per connection on purpose: ``connect()``
        twice on one instance leaks a listen task, and building it at connect
        time means the auth frame always carries the current token, including
        after a re-login.

        ``url`` is ``LEVITON_WS_URL``, and is checked rather than applied — see
        :meth:`_warn_if_ws_url_unreachable`.
        """
        if not self.supports_websocket:
            return None
        self._warn_if_ws_url_unreachable(url)
        transport_cls = _ws_module().AioLevitonWsTransport

        async def factory() -> Any:
            client = self._ensure_client()
            return transport_cls(client.create_websocket())

        return factory

    def _warn_if_ws_url_unreachable(self, url: str | None) -> None:
        """Say so, loudly, when ``LEVITON_WS_URL`` cannot actually be honoured.

        ``aioleviton``'s ``LevitonWebSocket.connect()`` passes
        ``const.WEBSOCKET_URL`` to ``ws_connect`` as a literal; neither
        ``create_websocket()`` nor the class constructor takes a URL, so there is
        no public surface to thread the setting through. Redirecting the socket
        would mean mutating a third-party module global for the whole process,
        which nobody has asked for and which would surprise the next reader.

        A setting that quietly does nothing is worse than one that is absent, so
        a *changed* value is reported at ERROR with the endpoint actually in use.
        The default value is honoured for free, because it is the same string.
        """
        if not url:
            return
        try:
            from aioleviton.const import WEBSOCKET_URL
        except Exception:  # pragma: no cover - only if upstream restructures
            return
        if str(url).rstrip("/") == str(WEBSOCKET_URL).rstrip("/"):
            return
        self._log.error(
            "leviton_ws_url_not_honoured",
            configured=str(url),
            effective=str(WEBSOCKET_URL),
            detail=(
                "aioleviton hardcodes the WebSocket endpoint in connect(); "
                "LEVITON_WS_URL cannot be applied without vendoring that method, "
                "so the connection uses the endpoint above"
            ),
        )

    # ---------------------------------------------------------------- plumbing
    async def call_with_retry(
        self, operation: Callable[[], Awaitable[Any]], *, op: str
    ) -> Any:
        """Run ``operation``, retrying Leviton's routine 502/504s (PLAN.md §6.6).

        The initial attempt plus :data:`RETRY_WAITS_S` retries. Auth failures are
        **not** retried here — they need a re-login, which is a different policy
        with its own floor.
        """
        waits = self._retry_waits
        attempts = len(waits) + 1

        def wait(retry_state: Any) -> float:
            index = min(retry_state.attempt_number, len(waits)) - 1
            return waits[index] if waits else 0.0

        def before_sleep(retry_state: Any) -> None:
            # DEBUG, not WARN: a 502 from Leviton's gateway is routine (§6.6).
            # Only a cycle that fails *after* its retries is worth a WARN.
            self._log.debug(
                "leviton_retry",
                op=op,
                attempt=retry_state.attempt_number,
                error=str(retry_state.outcome.exception()) if retry_state.outcome else None,
            )

        async def attempt() -> Any:
            # tenacity only awaits a genuine coroutine *function*, so the
            # zero-argument factory callers pass is wrapped rather than handed
            # over directly (a plain lambda returning a coroutine would be
            # "succeeded" without ever running).
            return await operation()

        retrying = AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait,
            retry=retry_if_exception_type(SourceTransientError),
            before_sleep=before_sleep,
            sleep=self._sleep,
            reraise=True,
        )
        return await retrying(attempt)

    async def _call(self, operation: Callable[[], Awaitable[Any]], *, op: str) -> Any:
        """Run one upstream call, translating ``aioleviton``'s exception tree.

        Nothing above this line ever sees an ``aioleviton`` exception, which is
        what makes the dependency swappable (PLAN.md §2.8).
        """
        try:
            return await operation()
        except LevitonAuthError as exc:
            # Covers LevitonTokenExpired (401 on an authenticated request) and a
            # plain credential rejection on login.
            raise SourceAuthError(f"leviton {op}: {exc}") from exc
        except (LevitonTwoFactorRequired, LevitonInvalidCode) as exc:
            # 406/408 — Leviton's abuse of status codes for 2FA. Not retryable:
            # a human has to supply a code.
            raise SourceAuthError(f"leviton {op}: {exc}") from exc
        except LevitonConnectionError as exc:
            # 5xx (the routine 502/504s) and every network-level failure.
            raise SourceTransientError(f"leviton {op}: {exc}") from exc
        except LevitonError as exc:
            raise SourceTransientError(f"leviton {op}: {exc}") from exc
        except asyncio.TimeoutError as exc:
            raise SourceTransientError(f"leviton {op}: timeout") from exc


def _login_payload(auth: Any) -> dict[str, Any]:
    """The login response, reconstructed from ``aioleviton``'s ``AuthToken``.

    ``aioleviton`` does not surface the raw JSON body, but ``AuthToken`` carries
    every field of it (``id``/``ttl``/``created``/``userId``/``user``), so the
    cache holds the full response as PLAN.md §6.1 requires.
    """
    return {
        "id": auth.token,
        "userId": auth.user_id,
        "ttl": auth.ttl,
        "created": auth.created,
        "user": dict(getattr(auth, "user", {}) or {}),
    }


# ---------------------------------------------------------------------- source


class LevitonSource(BaseSource):
    """The Leviton :class:`~energy_capture.sources.base.Source` (PLAN.md §6).

    Lifecycle (driven by ``stages/poller.py`` and ``energycap run``)::

        source = LevitonSource(settings)
        await source.start()                  # auth + first discovery
        tasks = source.background_tasks()      # keepalive + re-discovery
        rows = await source.poll()             # every 30s
        await source.close()

    ``poll()`` raises :class:`SourceTransientError` when a cycle fails after its
    in-cycle retries, per the contract in ``sources/base.py``: the loop counts
    the failure, writes **no rows**, and carries on. It never raises anything the
    loop is not told to expect, and it never returns a fabricated reading.

    ``LEVITON_INGEST`` decides where a cycle's *values* come from (see the module
    docstring). It does not change the cadence, the timestamping, or the mapping:
    :meth:`_map_snapshot` is the only mapper in the package and every mode runs
    the same :class:`HubSnapshot` values through it, so a WebSocket-sourced cycle
    and a REST-sourced cycle with the same field values produce byte-identical
    rows.
    """

    name = SOURCE_LEVITON

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        adapter: LevitonAdapter | None = None,
        status_store: Any = None,
        monotonic: Callable[[], float] = time.monotonic,
        ws: Any = None,
    ) -> None:
        resolved = settings if settings is not None else get_settings()
        super().__init__(
            poll_interval_s=resolved.poll_interval_s,
            discovery_interval_s=resolved.leviton_discovery_interval_s,
        )
        self._settings = resolved
        self._log = get_logger("leviton")
        self._monotonic = monotonic
        self._adapter = adapter if adapter is not None else _adapter_from_settings(resolved)
        self._status_store = status_store

        self._residence_ids: tuple[int, ...] = ()
        #: Last-known ``connected`` per hub — the keepalive's allow-list (§6.4).
        self._hub_connected: dict[str, bool] = {}
        self._consecutive_failures = 0
        self._keepalive_failures = 0
        self._keepalive_not_before: float | None = None

        # ---------------------------------------------------------- ingestion
        self._ingest: str = resolved.leviton_ingest
        #: The push ingester (``sources/leviton_ws.py``), or ``None`` in ``rest``
        #: mode / when the client cannot open a socket. Injected by tests.
        self._ws: Any = ws
        #: The most recent REST response set. It supplies the *structure* every
        #: cycle (which hubs, breakers and CTs exist, their positions, poles and
        #: channels) — the socket only ever supplies measurements, and §6.5's
        #: ``channel_id`` still comes from ``position``, never from an API id.
        self._snapshots: tuple[HubSnapshot, ...] = ()
        self._cycles_by_source: dict[str, int] = {}
        self._last_value_source: str | None = None
        self._last_withheld_reason: str | None = None
        self._reconciles = 0
        self._last_reconcile_drift: dict[str, int] | None = None

    # ------------------------------------------------------------- accessors
    @property
    def adapter(self) -> LevitonAdapter:
        """The Leviton-specific seam (PLAN.md §2.8)."""
        return self._adapter

    @property
    def consecutive_failures(self) -> int:
        """Failed poll cycles since the last success (PLAN.md §6.6)."""
        return self._consecutive_failures

    @property
    def keepalive_failures(self) -> int:
        """Consecutive failed keepalive rounds; drives the backoff (§6.4)."""
        return self._keepalive_failures

    @property
    def connected_hub_ids(self) -> tuple[str, ...]:
        """Hubs currently reporting ``connected: true`` — the keepalive targets."""
        return tuple(hub for hub, connected in self._hub_connected.items() if connected)

    @property
    def ingest_mode(self) -> str:
        """The active ``LEVITON_INGEST`` mode (``hybrid`` / ``ws`` / ``rest``)."""
        return self._ingest

    @property
    def websocket(self) -> Any:
        """The push ingester, or ``None`` when this source is REST-only."""
        return self._ws

    @property
    def last_value_source(self) -> str | None:
        """Where the last cycle's values came from — see :data:`VALUE_SOURCE_WS`."""
        return self._last_value_source

    def _status(self) -> Any:
        """The process ``StatusStore``, resolved lazily so imports stay cheap."""
        if self._status_store is None:
            from energy_capture.health import get_status_store

            self._status_store = get_status_store()
        return self._status_store

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Authenticate (cached token first), discover, then open the socket.

        Order matters: the ingester subscribes *per object*, so it can only
        follow what discovery found, and its first connect seeds its store from
        a REST snapshot. A socket that will not open is not fatal — the gate
        stays shut, which in ``hybrid`` means REST for every cycle and in ``ws``
        means gaps, and :meth:`LevitonWebSocketIngester.tick` keeps retrying.
        """
        await self._adapter.start()
        await self.discover(force=True)
        await self._start_websocket()

    async def close(self) -> None:
        ws = self._ws
        if ws is not None:
            # Never raises, per its contract; suppressed anyway so a socket
            # teardown cannot strand the HTTP session below it.
            with contextlib.suppress(Exception):
                await ws.aclose()
        await self._adapter.close()

    def background_tasks(self) -> Sequence[BackgroundTask]:
        """Re-discovery (§6.2), the bandwidth keepalive (§6.4), and the socket.

        The keepalive is **unchanged** and stays unchanged: ``PUT
        {"bandwidth": 1}`` every 50s, never 0. With a subscriber attached it
        finally does what it was for — that PUT is what triggers the server's
        state flood.

        ``leviton_ws`` runs :meth:`LevitonWebSocketIngester.tick`, which absorbs
        every exception, so it inherits ``run_background_task``'s "a failing
        background task never kills polling" guarantee twice over.
        """
        tasks: list[BackgroundTask] = [
            *super().background_tasks(),
            BackgroundTask(
                name="leviton_keepalive",
                interval_s=KEEPALIVE_INTERVAL_S,
                run=self.keepalive_round,
                # No initial delay: the first poll wants fresh readings, and the
                # hub is only in high-bandwidth mode once we have asked.
                initial_delay_s=0.0,
            ),
        ]
        if self._ws is not None:
            tasks.append(
                BackgroundTask(
                    name="leviton_ws",
                    interval_s=WS_TICK_INTERVAL_S,
                    run=self._ws.tick,
                    # start() already made the first connection attempt.
                    initial_delay_s=WS_TICK_INTERVAL_S,
                )
            )
            if self._settings.leviton_rest_reconcile_enabled:
                interval = float(self._settings.leviton_rest_reconcile_s)
                tasks.append(
                    BackgroundTask(
                        name="leviton_rest_reconcile",
                        interval_s=interval,
                        run=self.reconcile_round,
                        initial_delay_s=interval,
                    )
                )
        return tuple(tasks)

    # ------------------------------------------------------------- websocket
    async def _start_websocket(self) -> None:
        """Build (if needed) and start the push ingester. Never raises.

        ``rest`` mode returns immediately and imports nothing — that is what
        makes it the owner's no-code-change fallback.
        """
        if not self._settings.leviton_ws_enabled:
            self._set_status(STATUS_SECTION_INGEST, **self.ingest_status())
            return
        if self._ws is None:
            factory = self._adapter.ws_transport_factory(self._settings.leviton_ws_url)
            if factory is None:
                # A capability gap, not a runtime failure: this build cannot
                # open a socket at all. Producing permanent gaps over that would
                # be self-inflicted data loss, so we say so loudly and collect
                # over REST — which is exactly what this pipeline did before.
                self._log.error(
                    "leviton_ws_unavailable",
                    ingest=self._ingest,
                    detail=(
                        "the Leviton client behind the adapter has no "
                        "create_websocket(); collecting over REST only, which "
                        "serves the cached readings LEVITON_INGEST exists to escape"
                    ),
                )
                self._set_status(STATUS_SECTION_INGEST, **self.ingest_status())
                return
            ws_module = _ws_module()
            self._ws = ws_module.LevitonWebSocketIngester(
                transport_factory=factory,
                seed=self._websocket_seed,
                # The existing §6.4 round, fired once immediately before each
                # connect because that PUT is what triggers the state flood.
                keepalive=self.keepalive_round,
                reauthenticate=self._websocket_reauthenticate,
                targets=ws_module.subscription_targets_from_snapshots(self._snapshots),
                stall_timeout_s=float(self._settings.leviton_ws_stall_timeout_s),
                proactive_reconnect_s=float(self._settings.leviton_ws_reconnect_s),
                status_store=self._status(),
            )
        else:
            await self._sync_websocket_targets()
        try:
            await self._ws.start()
        except Exception as exc:  # noqa: BLE001 - a shut gate is not a crash
            self._log.warning("leviton_ws_start_failed", error=str(exc))
        self._set_status(STATUS_SECTION_INGEST, **self.ingest_status())

    async def _websocket_seed(self) -> tuple[HubSnapshot, ...]:
        """One full REST read, for the ingester to seed its store on connect."""
        return await self._rest_snapshot(op="ws_seed")

    async def _websocket_reauthenticate(self) -> None:
        await self._adapter.reauthenticate(reason="ws_401")

    async def _sync_websocket_targets(self) -> None:
        """Hand the ingester the subscription set implied by the last discovery.

        This is how ~25 newly installed smart breakers start streaming without a
        restart, and how firmware ≥2.2.0's mutated breaker ids are followed: the
        subscription key is the API ``id`` (which moves), while ``channel_id``
        stays ``breaker_p{position}`` (which does not).
        """
        ws = self._ws
        if ws is None:
            return
        try:
            targets = _ws_module().subscription_targets_from_snapshots(self._snapshots)
            await ws.set_targets(targets)
        except Exception as exc:  # noqa: BLE001 - discovery outranks the socket
            self._log.warning("leviton_ws_subscribe_sync_failed", error=str(exc))

    # -------------------------------------------------------------- discovery
    async def discover(self, *, force: bool = False) -> Discovery:
        """Enumerate hubs, breakers and CTs (PLAN.md §6.2).

        Re-run every ``LEVITON_DISCOVERY_INTERVAL_S`` so a smart breaker added to
        the panel shows up without restarting the container. Empty breaker lists
        are the normal case today and are not an error.
        """
        cached = self.cached_discovery
        if cached is not None and not force:
            return cached

        residence_ids = await self._guarded(
            lambda: self._adapter.fetch_residence_ids(), op="discovery_residences"
        )
        if residence_ids:
            self._residence_ids = residence_ids
        snapshots = await self._guarded(
            lambda: self._adapter.fetch_snapshot(self._residence_ids),
            op="discovery_snapshot",
        )
        # Discovery is a full REST read, so it doubles as the structural
        # skeleton every WebSocket-sourced cycle is overlaid onto.
        self._snapshots = tuple(snapshots)

        devices: list[DiscoveredDevice] = []
        channels: list[DiscoveredChannel] = []
        for snapshot in snapshots:
            hub = snapshot.hub
            self._hub_connected[hub.device_id] = hub.connected
            devices.append(
                DiscoveredDevice(
                    source=self.name,
                    device_id=hub.device_id,
                    kind="hub",
                    label=hub.name,
                    details={
                        "connected": hub.connected,
                        "version": hub.version,
                        "serial": hub.serial,
                        "rssi": hub.rssi,
                    },
                )
            )
            for leg in _LEG_SUFFIXES:
                channels.append(
                    DiscoveredChannel(
                        source=self.name,
                        device_id=hub.device_id,
                        channel_id=panel_leg_channel_id(leg),
                        kind="panel_leg",
                        label=f"{hub.name or hub.device_id} leg {leg.upper()}",
                        details={"connected": hub.connected},
                    )
                )
            for breaker in snapshot.breakers:
                if breaker.is_placeholder:
                    continue
                channels.append(
                    DiscoveredChannel(
                        source=self.name,
                        device_id=hub.device_id,
                        channel_id=breaker.channel_id,
                        kind="breaker",
                        label=breaker.name,
                        details={
                            "position": breaker.position,
                            "poles": breaker.poles,
                            "model": breaker.model,
                            "branchType": breaker.branch_type,
                            "connected": breaker.connected,
                            "serialNumber": breaker.serial_number,
                        },
                    )
                )
            for ct in snapshot.cts:
                if ct.is_unused:
                    continue
                legs = ("a", "b") if ct.has_second_leg else ("a",)
                for leg in legs:
                    channels.append(
                        DiscoveredChannel(
                            source=self.name,
                            device_id=hub.device_id,
                            channel_id=ct_channel_id(ct.channel, leg),
                            kind="ct",
                            label=f"{ct.name} (leg {leg.upper()})" if ct.name else None,
                            details={
                                "channel": ct.channel,
                                "usageType": ct.usage_type,
                                "connected": ct.connected,
                                "leg": leg,
                            },
                        )
                    )

        discovery = Discovery(
            source=self.name,
            devices=tuple(devices),
            channels=tuple(channels),
            ts_utc=now_utc(),
        )
        self._log.info(
            "leviton_discovery",
            hubs=len(devices),
            channels=len(channels),
            breakers=sum(1 for c in channels if c.kind == "breaker"),
            cts=sum(1 for c in channels if c.kind == "ct"),
        )
        # Every discovery pass re-derives the subscription set, so hardware added
        # since the last pass starts streaming without a restart (PLAN.md §6.2).
        await self._sync_websocket_targets()
        return self._remember(discovery)

    # ---------------------------------------------------------------- polling
    async def poll(self) -> list[Observation]:
        """One poll cycle → observations sharing a single ``ts_utc`` (§6.5).

        Identical in every ``LEVITON_INGEST`` mode: one cadence, one timestamp
        taken when the value set is complete, one mapper. Only *where the values
        came from* differs, and that is answered by :meth:`_sample_snapshots`.

        Failure policy (§6.6): 502/504 are retried inside the cycle; a cycle that
        still fails logs once at WARN, bumps the consecutive-failure count and
        raises :class:`SourceTransientError` with **zero rows** produced. A 401
        triggers one re-login (honouring the 10s floor) and one more attempt. In
        ``ws`` mode a shut gate takes the same path — no rows, a counted failure
        — because "we do not currently know the value" is exactly what a failed
        REST cycle means.
        """
        try:
            if not self._residence_ids or (self._ws is not None and not self._snapshots):
                await self.discover(force=True)
            snapshots, value_source, withheld = await self._sample_snapshots()
        except SourceAuthError as exc:
            self._consecutive_failures += 1
            self._log.error(
                "leviton_auth_failed",
                consecutive_failures=self._consecutive_failures,
                error=str(exc),
            )
            self._record_failure(STATUS_SECTION_AUTH, exc)
            raise
        except SourceTransientError as exc:
            self._consecutive_failures += 1
            # Once per failed cycle, not once per retry (PLAN.md §6.6).
            self._log.warning(
                "leviton_poll_failed",
                consecutive_failures=self._consecutive_failures,
                rows=0,
                error=str(exc),
            )
            raise

        # The value set is complete: this is the instant every row in this cycle
        # is stamped with (PLAN.md §6.5). One ts_utc per cycle, in every mode —
        # the socket changes freshness, never sampling.
        cycle = self.new_cycle(ts_utc=now_utc())
        for snapshot in snapshots:
            self._hub_connected[snapshot.hub.device_id] = snapshot.hub.connected
            self._map_snapshot(cycle, snapshot)
        rows = cycle.finish()

        self._consecutive_failures = 0
        self._note_value_source(value_source, withheld)
        self._log.debug(
            "leviton_poll_ok",
            rows=len(rows),
            gaps=cycle.gaps,
            hubs=len(snapshots),
            value_source=value_source,
            ws_withheld_reason=withheld,
        )
        return rows

    # ------------------------------------------------------ where values come from
    async def _sample_snapshots(self) -> tuple[tuple[HubSnapshot, ...], str, str | None]:
        """``(snapshots, value_source, ws_withheld_reason)`` for one cycle.

        The gate in ``leviton_ws.py`` decides, and it decides on **connection
        state only** — never on how long a field has gone without changing. A
        steady resistive load genuinely holds its wattage, so gapping on field
        age would delete real data; a disconnected or unsynced or silently
        stalled socket, on the other hand, means we do not know the value at all.

        What each mode does with a shut gate is the one real policy choice here:

        * ``hybrid`` reads REST for this cycle. A cached REST value is what this
          pipeline collected before the socket existed, so it is strictly better
          than nothing — but it is *recorded*, per cycle, in ``status.json`` and
          at INFO on transition, so nobody later mistakes a fallback stretch for
          fresh data.
        * ``ws`` emits a gap. That is the strict reading, and it is what the mode
          is for: an operator who wants to know exactly how good the socket is
          must not have the answer papered over by REST.
        """
        ws = self._ws
        if ws is None:
            return (await self._rest_snapshot(), VALUE_SOURCE_REST, None)

        try:
            fresh = ws.overlay_snapshots(self._snapshots)
            reason = None if fresh is not None else (ws.withheld_reason or "unknown")
        except Exception as exc:  # noqa: BLE001 - a freshness bug must not lose the cycle
            # The socket layer is an *optimisation* over a working REST path. A
            # defect in it is a reason to stop trusting its values, never a
            # reason to stop collecting.
            self._log.warning("leviton_ws_sample_failed", error=str(exc))
            fresh, reason = None, "ws_error"
        if fresh is not None:
            return (tuple(fresh), VALUE_SOURCE_WS, None)

        if self._ingest == INGEST_WS:
            self._note_value_source(VALUE_SOURCE_WITHHELD, reason)
            raise SourceTransientError(
                f"leviton websocket cannot be sampled ({reason}); "
                "LEVITON_INGEST=ws emits a gap rather than a cached REST reading"
            )
        return (await self._rest_snapshot(), VALUE_SOURCE_REST_FALLBACK, reason)

    async def _rest_snapshot(self, *, op: str = "poll") -> tuple[HubSnapshot, ...]:
        """One full REST read, retried per §6.6, kept as the structural skeleton."""
        snapshots = tuple(
            await self._guarded(
                lambda: self._adapter.fetch_snapshot(self._residence_ids), op=op
            )
        )
        if snapshots:
            self._snapshots = snapshots
        return snapshots

    def _note_value_source(self, value_source: str, withheld: str | None) -> None:
        """Count the cycle and, on any change, say so at INFO.

        Rows have no provenance column and are not getting one, so this pair —
        the counters in ``status.json`` and an INFO line at every transition —
        is how a reader reconstructs afterwards which stretch of rows came from
        a live socket and which from a REST cache.
        """
        self._cycles_by_source[value_source] = self._cycles_by_source.get(value_source, 0) + 1
        changed = (
            value_source != self._last_value_source or withheld != self._last_withheld_reason
        )
        self._last_value_source = value_source
        self._last_withheld_reason = withheld
        if changed:
            self._log.info("leviton_value_source", **self.ingest_status())
        self._set_status(STATUS_SECTION_INGEST, **self.ingest_status())

    def ingest_status(self) -> dict[str, Any]:
        """The ``leviton_ingest`` section of ``status.json`` (PLAN.md §11)."""
        return {
            "mode": self._ingest,
            "ws_enabled": self._settings.leviton_ws_enabled,
            "ws_available": self._ws is not None,
            "value_source": self._last_value_source,
            "ws_withheld_reason": self._last_withheld_reason,
            "cycles_ws": self._cycles_by_source.get(VALUE_SOURCE_WS, 0),
            "cycles_rest": self._cycles_by_source.get(VALUE_SOURCE_REST, 0),
            "cycles_rest_fallback": self._cycles_by_source.get(VALUE_SOURCE_REST_FALLBACK, 0),
            "cycles_withheld": self._cycles_by_source.get(VALUE_SOURCE_WITHHELD, 0),
            "rest_reconciles": self._reconciles,
            "rest_reconcile_s": (
                self._settings.leviton_rest_reconcile_s
                if self._settings.leviton_rest_reconcile_enabled
                else None
            ),
            "last_reconcile_drift": self._last_reconcile_drift,
        }

    # -------------------------------------------------------- REST reconcile
    async def reconcile_round(self) -> None:
        """``hybrid``'s periodic full REST re-read. Never raises.

        Two jobs, neither of which is producing rows:

        1. keep the structural skeleton current between hourly discoveries, so a
           breaker installed at 14:05 is not overlaid onto a 13:00 shape;
        2. **measure the thing this whole change is about.** Both value sets are
           mapped through the same :meth:`_map_snapshot`, and the number of
           metrics on which they disagree is recorded as ``last_reconcile_drift``
           — that is a direct, per-cycle measurement of how far behind the REST
           cache actually runs. Do not publish a freshness claim without it.

        Neither the comparison nor its rows ever reach the spool.
        """
        if not self._settings.leviton_rest_reconcile_enabled:
            return
        try:
            snapshots = await self._rest_snapshot(op="reconcile")
        except Exception as exc:  # noqa: BLE001 - a background task must not die
            self._log.warning("leviton_rest_reconcile_failed", error=str(exc))
            return
        self._reconciles += 1
        self._last_reconcile_drift = self._reconcile_drift(snapshots)
        self._log.info(
            "leviton_rest_reconcile",
            hubs=len(snapshots),
            reconciles=self._reconciles,
            drift=self._last_reconcile_drift,
        )
        await self._sync_websocket_targets()
        self._set_status(STATUS_SECTION_INGEST, **self.ingest_status())

    def _reconcile_drift(
        self, snapshots: Sequence[HubSnapshot]
    ) -> dict[str, int] | None:
        """How many metrics the REST read and the push store disagree on.

        ``None`` when the gate is shut (there is nothing trustworthy to compare
        against) or when the comparison itself fails — telemetry must never
        break collection.
        """
        ws = self._ws
        if ws is None:
            return None
        try:
            overlaid = ws.overlay_snapshots(snapshots)
            if overlaid is None:
                return None
            rest_values = self._metric_values(snapshots)
            ws_values = self._metric_values(overlaid)
            keys = set(rest_values) | set(ws_values)
            differing = sum(
                1 for key in keys if rest_values.get(key) != ws_values.get(key)
            )
            return {"compared": len(keys), "differing": differing}
        except Exception as exc:  # noqa: BLE001 - diagnostics are not the point
            self._log.debug("leviton_reconcile_drift_failed", error=str(exc))
            return None

    def _metric_values(
        self, snapshots: Sequence[HubSnapshot]
    ) -> dict[tuple[str, str, str], float]:
        """``(device, channel, metric) -> value`` through the **one** mapper.

        Deliberately built by running :meth:`_map_snapshot` rather than by
        reading fields directly: a second, private view of the mapping is exactly
        the drift this module refuses to have.
        """
        cycle = self.new_cycle(ts_utc=now_utc())
        for snapshot in snapshots:
            self._map_snapshot(cycle, snapshot)
        return {
            (row.device_id, row.channel_id, row.metric): row.value
            for row in cycle.finish()
        }

    async def _guarded(
        self, operation: Callable[[], Awaitable[Any]], *, op: str
    ) -> Any:
        """Run ``operation`` with the §6.6 policy: retry 5xx, re-login on 401.

        Two layers, in this order:

        1. :meth:`LevitonAdapter.call_with_retry` absorbs the routine 502/504s
           *within* the cycle.
        2. A 401 that survives that means the token is dead: re-login once
           (the adapter enforces the 10-second floor, and a 60-second backoff if
           the login itself fails) and try the operation once more.

        Anything still failing escapes to the caller, which counts it and moves
        on. The loop never crashes and never invents a reading.
        """
        try:
            return await self._adapter.call_with_retry(operation, op=op)
        except SourceAuthError:
            await self._adapter.reauthenticate(reason=f"{op}_401")
            self._record_success(STATUS_SECTION_AUTH, logins=self._adapter.login_count)
            return await self._adapter.call_with_retry(
                operation, op=f"{op}_after_relogin"
            )

    # ----------------------------------------------------------- row mapping
    def _map_snapshot(self, cycle: PollCycle, snapshot: HubSnapshot) -> None:
        """Map one hub's response set onto rows (PLAN.md §6.5).

        Nothing here inspects a value: a spurious fw-v2 zero is added exactly as
        received (§2.3), and a ``None`` is dropped by :meth:`PollCycle.add` so the
        row is simply absent (a gap, never a zero).
        """
        hub = snapshot.hub
        device_id = hub.device_id

        cycle.add_metrics(
            device_id,
            panel_leg_channel_id("a"),
            {"volts": hub.volts_a, "hz": hub.hz_a},
        )
        cycle.add_metrics(
            device_id,
            panel_leg_channel_id("b"),
            {"volts": hub.volts_b, "hz": hub.hz_b},
        )

        for breaker in snapshot.breakers:
            if breaker.is_placeholder:
                # A dumb-breaker placeholder: no meter, nothing to record.
                continue
            cycle.add_metrics(device_id, breaker.channel_id, breaker.metrics())

        for ct in snapshot.cts:
            if ct.is_unused:
                continue
            for leg in _LEG_SUFFIXES:
                # A single-leg CT reports nulls for leg B; add() drops them, so
                # leg B contributes no rows at all rather than zeros.
                cycle.add_metrics(device_id, ct_channel_id(ct.channel, leg), ct.leg_metrics(leg))

    # ------------------------------------------------------------- keepalive
    async def keepalive_round(self) -> None:
        """PUT ``{"bandwidth": 1}`` to every connected hub (PLAN.md §6.4).

        Scheduled every :data:`KEEPALIVE_INTERVAL_S` (50s) by the runner. Hubs
        reporting ``connected: false`` are skipped — they cannot answer, and
        hammering them would only add failures. On repeated failure the round
        backs off exponentially (the runner keeps calling on its 50s tick; this
        method returns immediately until the backoff expires) and the condition
        is recorded in ``status.json``.

        This never raises: a failing background task must not disturb polling.
        """
        if self._keepalive_not_before is not None:
            remaining = self._keepalive_not_before - self._monotonic()
            if remaining > 0:
                self._log.debug("leviton_keepalive_backoff", remaining_s=round(remaining, 1))
                return
            self._keepalive_not_before = None

        hub_ids = self.connected_hub_ids
        skipped = len(self._hub_connected) - len(hub_ids)
        if not hub_ids:
            self._log.debug("leviton_keepalive_no_connected_hubs", skipped=skipped)
            self._set_status(
                STATUS_SECTION_KEEPALIVE, connected_hubs=0, hubs_skipped=skipped
            )
            return

        failures: list[str] = []
        needs_auth = False
        for hub_id in hub_ids:
            try:
                await self._adapter.keepalive(hub_id)
            except SourceAuthError as exc:
                needs_auth = True
                failures.append(f"{hub_id}: {exc}")
                # ``aioleviton`` silently ignores every non-notification frame,
                # so an error frame mid-stream never reaches the socket layer:
                # this 401/403/406 is the only early evidence that the cloud has
                # stopped honouring us. Without it a dead token shows up on the
                # socket as nothing but silence.
                if self._ws is not None:
                    self._ws.note_auth_failure(exc)
            except Exception as exc:  # noqa: BLE001 - the task must never die
                failures.append(f"{hub_id}: {exc}")

        if not failures:
            self._keepalive_failures = 0
            self._keepalive_not_before = None
            self._log.debug(
                "leviton_keepalive_ok", hubs=len(hub_ids), hubs_skipped=skipped
            )
            self._record_success(
                STATUS_SECTION_KEEPALIVE,
                connected_hubs=len(hub_ids),
                hubs_skipped=skipped,
            )
            return

        self._keepalive_failures += 1
        backoff = min(
            KEEPALIVE_INTERVAL_S * (2 ** (self._keepalive_failures - 1)),
            KEEPALIVE_MAX_BACKOFF_S,
        )
        self._keepalive_not_before = self._monotonic() + backoff
        self._log.warning(
            "leviton_keepalive_failed",
            hubs=len(hub_ids),
            failed=len(failures),
            consecutive_failures=self._keepalive_failures,
            backoff_s=backoff,
            error="; ".join(failures)[:500],
        )
        self._record_failure(
            STATUS_SECTION_KEEPALIVE,
            "; ".join(failures)[:500],
            connected_hubs=len(hub_ids),
            backoff_s=backoff,
        )
        if needs_auth:
            try:
                await self._adapter.reauthenticate(reason="keepalive_401")
            except Exception as exc:  # noqa: BLE001 - never kill the task
                self._log.warning("leviton_keepalive_relogin_failed", error=str(exc))

    # ---------------------------------------------------------------- status
    def _record_success(self, section: str, **fields: Any) -> None:
        try:
            self._status().record_success(section, **fields)
        except Exception:  # pragma: no cover - status must never break the loop
            self._log.debug("leviton_status_write_failed", section=section)

    def _record_failure(self, section: str, error: Any, **fields: Any) -> None:
        try:
            self._status().record_failure(section, error, **fields)
        except Exception:  # pragma: no cover - status must never break the loop
            self._log.debug("leviton_status_write_failed", section=section)

    def _set_status(self, section: str, **fields: Any) -> None:
        try:
            self._status().set(section, **fields)
        except Exception:  # pragma: no cover - status must never break the loop
            self._log.debug("leviton_status_write_failed", section=section)


def _adapter_from_settings(settings: Settings) -> LevitonAdapter:
    """Build the production adapter; credentials are demanded here, not at import."""
    return LevitonAdapter(
        username=settings.require("leviton_username"),
        password=settings.require("leviton_password"),
        token_path=settings.leviton_token_path,
    )
