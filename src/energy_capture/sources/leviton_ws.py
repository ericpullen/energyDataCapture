"""Leviton real-time push (WebSocket) — the *freshness* engine for §6.

Why this module exists (measured 2026-08-16, against the two live LWHEM-2 hubs
running firmware 2.1.2)
-----------------------------------------------------------------------------
PLAN.md §2.8 and §6.4 LOCKED "REST polling at 30s + bandwidth keepalive, not
WebSocket". Live measurement overturned that and the owner authorised this
module; the deviation is recorded in ``DEVIATIONS.md``. The evidence, in one
paragraph: over a 5-minute production run and a separate 12-minute probe, 10 of
12 channels never changed value at all — one whole-panel ``GRID_POWER`` CT feed
held *exactly* 4086.05 W across 46 consecutive reads at 15s. An A/B probe proved
the keepalive PUT lands (the hub's ``bandwidth`` field reads 0 at rest and 2
afterwards, i.e. 1 auto-decayed to 2 exactly as §6.4 describes) and that both
phases were identically frozen. The reference integration explains it: setting
``bandwidth=1`` triggers a full state flood **pushed over the WebSocket**, and
its REST path is documented as "initial discovery, fallback polling (10-min
interval)" — we were polling REST 20x faster than the reference's *fallback*
rate and receiving a cache. With ~25 more smart breakers arriving (12 channels →
~40), per-breaker resolution is exactly where a stale cache hurts most.

Settled, so nobody re-researches it: the hub's ``pollBreakers`` field is not the
answer — it refreshes CT/breaker *lifetime* counters only, and those stopped
working at firmware 2.1.0 (which is also why §6.3 excludes
``energyConsumption``/``energyImport``). It is never sent from here.

**The load-bearing design decision.** The WebSocket changes how values are kept
*fresh*. It does not change how rows are *sampled*.

* WRONG: one ``Observation`` per WebSocket delta. Sampling would become
  irregular, which breaks §2.5's kWh formula (``mean_watts * sample_count *
  POLL_INTERVAL_S / 3.6e6`` assumes a fixed cadence) and destroys
  ``sample_count``'s meaning as the gap detector (§12: "sample_count < ~118
  means the hour has gaps").
* RIGHT: this module keeps an in-memory **current-state store**, merging the
  partial deltas the server pushes. The existing 30s poll cycle then *samples*
  that store and emits exactly the rows it emits today, one ``ts_utc`` per cycle
  (§6.5). Spool, uploader, compactor, rollup, Glue and every README query are
  untouched. The only thing that changes is that the sampled values are current
  instead of cached.

**The cardinal-rule trap.** Sampling an in-memory store is structurally a
hold-last-value, and CLAUDE.md rule 1 forbids that. The distinction that makes
this legitimate is *connection state, never field age*:

* While the socket is connected **and** a full state sync has completed, the
  store holds what the server currently believes is true. Sampling it is honest.
  A steady load genuinely does not update — the live capture shows a resistive
  water-heater element holding 2462 W, which is physically correct — so gapping
  on "this field has not changed recently" would DELETE real data.
* While the socket is disconnected, stalled, or before the initial sync, we do
  **not** know the current value. Emitting the last one with a current timestamp
  is fabrication. We emit nothing — a gap, exactly as a failed REST cycle does.

So :meth:`LevitonWebSocketIngester.can_sample` gates on connection state and
sync state and on nothing else. Per-field last-update instants are recorded as
**diagnostics** (:meth:`field_diagnostics`, surfaced through ``status.json``) so
the real update distribution can be measured and this decision revisited on
evidence. No max-age threshold is invented here.

"Connection state", stated precisely, because the sloppy readings of it are all
hold-last-values wearing a disguise:

* it is a property of **each field**, not of the store. A field is samplable
  only if it was established *on the connection we are sampling from*. The store
  is deliberately not cleared on reconnect, so :meth:`StateStore.evict_before`
  runs at the instant the gate opens and drops everything the new connection did
  not re-establish. That is membership, not age — a value pushed 55 minutes into
  a connection is fine; a value pushed on the *previous* connection is not.
* it is a property of **each hub's feed**, not of the socket. Two hubs share one
  socket here, so an aggregate "some frame arrived" watchdog is satisfied by
  whichever hub is healthy while the other one's push feed is dead. Liveness is
  therefore evaluated per hub (:meth:`LevitonWebSocketIngester.freshness` takes
  a ``hub_id``), and every subscription carries the hub it belongs to.
* an explicit ``null`` from the REST seed is a *clear*, not a no-op — otherwise
  the seed can overwrite a carried-over value but never remove one.

Decisions this module makes that a reviewer should see explicitly
-----------------------------------------------------------------------------
1. **An explicit ``null`` in a delta CLEARS the field** (:data:`NULL_POLICY_CLEAR`,
   the default). Both reference integrations do the opposite — they treat
   ``{"activePower": null}`` as "no news, keep the cached value" — which is a
   hold-last-value at field granularity, i.e. CLAUDE.md rule 1 territory. Our
   REST path already emits *no row* for a null field, and this keeps the two
   paths identical. Null deltas are counted per field
   (:attr:`StateStore.null_counts`) so the alternative
   (:data:`NULL_POLICY_IGNORE`) can be chosen later on evidence rather than
   taste.
2. **Every (re)connect re-establishes state before emission resumes.** The flood
   is a burst of ordinary *partial* notifications whose union is only hopefully
   complete (no reference depends on it being complete), so deltas alone cannot
   establish state. On connect we PUT the keepalive, connect, subscribe, seed the
   store from one REST snapshot, and then hold emission until either every
   subscribed object has been touched by the flood or
   :data:`SYNC_FLOOD_TIMEOUT_S` expires — recording which of the two happened as
   ``sync_mode``. Seeded fields carry ``ts_source="rest_seed"`` because the REST
   snapshot *is* the stale cache this module exists to escape; that honesty cost
   is measurable per field instead of invisible.
3. **The silent stall is treated as a disconnect.** aiohttp's heartbeat only
   proves the TCP path is alive; a server that pongs happily while pushing
   nothing is invisible to ``aioleviton``. Without our own watchdog the sampler
   would keep emitting rows from a frozen store while ``connected == True`` —
   a fabrication path straight through the cardinal rule, and the single most
   important thing in this file. :data:`STALL_TIMEOUT_S` of aggregate silence
   (any frame, from any object) flips the gate shut and forces a reconnect.
4. **``aioleviton.LevitonWebSocket.reconnect()`` is never called.** Its
   ``disconnect()`` fires ``on_disconnect`` callbacks even for an intentional
   teardown, so reconnecting from a disconnect handler is a storm. This module
   owns a single reconnect state machine and builds a fresh transport per
   connection (``connect()`` twice on one instance leaks a listen task).
5. **The notification callback is a plain ``def``.** ``_listen`` calls it without
   ``await``; an ``async def`` callback would be silently dropped, producing a
   total data outage that still reports ``connected: True``. The transport
   asserts this at registration time.
6. **The handshake carries our headers, not ``aioleviton``'s.**
   ``LevitonWebSocket.connect()`` hardcodes ``{"user-agent": ...}`` and sends no
   ``Origin``, while both other implementations of this protocol send one and
   §6.1 records that Leviton appears to fingerprint callers (our REST adapter
   already injects it). Nothing is vendored: the one attribute ``connect()``
   reads is wrapped by :class:`_HandshakeHeaderSession`, whose docstring is the
   re-check list for an ``aioleviton`` upgrade.
7. **Never ``bandwidth: 0``.** Nothing here sets bandwidth; the existing §6.4
   keepalive (PUT ``{"bandwidth": 1}`` every 50s, never 0) stays exactly as it
   is and is merely also fired once immediately before each connect, because
   that PUT is what triggers the flood that seeds the store.

Layout, outside-in
-----------------------------------------------------------------------------
* :class:`LevitonWebSocketIngester` — lifecycle, the emission gate, the
  watchdog, the counters. This is what ``sources/leviton.py`` holds and what the
  30s poll cycle asks ``can_sample()``.
* :class:`StateStore` / :class:`ChannelState` / :class:`FieldState` — the merge
  store and its provenance.
* :func:`parse_notification` — the only place the wire shape is understood.
* :class:`AioLevitonWsTransport` — the thin ``aioleviton`` seam (PLAN.md §2.8).
  Swap it and nothing else changes.

This module maps **no rows**: ``sources/leviton.py`` owns every bit of §6.5
(channel ids, pole arithmetic, gaps) and this module never writes to the spool.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final, Protocol

from energy_capture.logging import get_logger, register_secret
from energy_capture.sources.base import SourceAuthError, SourceTransientError
from energy_capture.sources.leviton import (
    LEVITON_ORIGIN,
    BreakerReading,
    CtReading,
    HubReading,
    HubSnapshot,
)
from energy_capture.timeutil import now_utc

__all__ = [
    "MAX_RECONNECT_BACKOFF_S",
    "NULL_POLICY_CLEAR",
    "NULL_POLICY_IGNORE",
    "PROACTIVE_RECONNECT_S",
    "REASON_AUTH_FAILED",
    "REASON_AWAITING_SYNC",
    "REASON_DISCONNECTED",
    "REASON_NOT_STARTED",
    "REASON_STALLED",
    "STALL_TIMEOUT_S",
    "STATUS_SECTION_WS",
    "SYNC_FLOOD_TIMEOUT_S",
    "SYNC_MODE_FLOOD",
    "SYNC_MODE_TIMEOUT",
    "TS_SOURCE_RECEIPT",
    "TS_SOURCE_REST_SEED",
    "TS_SOURCE_SERVER",
    "WATCHDOG_INTERVAL_S",
    "WS_HANDSHAKE_ORIGIN",
    "WS_MODEL_BREAKER",
    "WS_MODEL_CT",
    "WS_MODEL_HUB",
    "AioLevitonWsTransport",
    "ChannelState",
    "FieldState",
    "Freshness",
    "LevitonWsAuthError",
    "LevitonWsError",
    "LevitonWebSocketIngester",
    "ObjectDelta",
    "StateStore",
    "SubscriptionTarget",
    "WsTransport",
    "apply_ws_handshake_headers",
    "default_ws_handshake_headers",
    "overlay_breaker",
    "overlay_ct",
    "overlay_hub",
    "parse_notification",
    "subscription_targets_from_snapshots",
]


# --------------------------------------------------------------------- constants

#: Model names on the wire. ``IotWhem`` notifications may carry nested
#: ``ResidentialBreaker``/``IotCt`` child arrays; the other two also arrive as
#: direct notifications whose ``data`` frequently omits ``id``.
WS_MODEL_HUB: Final[str] = "IotWhem"
WS_MODEL_BREAKER: Final[str] = "ResidentialBreaker"
WS_MODEL_CT: Final[str] = "IotCt"

#: Keys that are *children*, not hub properties, inside an ``IotWhem`` payload.
_CHILD_KEYS: Final[tuple[str, ...]] = (WS_MODEL_BREAKER, WS_MODEL_CT)

#: The server hard-kills push after exactly 60 minutes, unconditionally — the
#: bandwidth PUT does not prevent it and neither does REST activity (confirmed
#: by a 57-hour capture of the official app, which has the same problem). Two
#: independent integrations landed on a 55-minute proactive cycle; reconnecting
#: *before* the deadline keeps the gap as small as we can make it.
PROACTIVE_RECONNECT_S: Final[float] = 3300.0

#: Aggregate silence (no frame of any kind, from any subscribed object) after
#: which the feed is considered dead. The references use 60s and 90s, both
#: chosen for a UI integration rather than an archive; 90s is the conservative
#: end. This is a *liveness* threshold, never a per-field staleness threshold.
STALL_TIMEOUT_S: Final[float] = 90.0

#: How long after subscribing we wait for the flood to touch every subscribed
#: object before giving up and sampling the REST seed anyway. Whichever happened
#: is recorded as ``sync_mode`` in ``status.json``.
SYNC_FLOOD_TIMEOUT_S: Final[float] = 20.0

#: Cadence of :meth:`LevitonWebSocketIngester.tick` — the watchdog resolution.
WATCHDOG_INTERVAL_S: Final[float] = 15.0

#: Ceiling on the reconnect backoff (``aioleviton.reconnect_delay`` already caps
#: itself at 16s; this bounds any replacement).
MAX_RECONNECT_BACKOFF_S: Final[float] = 300.0

#: What an explicit ``null`` in a delta means. ``clear`` = "the API said null",
#: so the field becomes unknown and the sampler emits no row (CLAUDE.md rule 1,
#: and identical to the REST path). ``ignore`` = the reference integrations'
#: behaviour, kept only so the decision can be flipped on measured evidence.
NULL_POLICY_CLEAR: Final[str] = "clear"
NULL_POLICY_IGNORE: Final[str] = "ignore"

#: Provenance of a field's timestamp (diagnostics only — never row data).
TS_SOURCE_RECEIPT: Final[str] = "receipt"
TS_SOURCE_SERVER: Final[str] = "server"
TS_SOURCE_REST_SEED: Final[str] = "rest_seed"

#: The only field seen anywhere in the wire payloads that looks like a server
#: timestamp, and only on the "status-only" breaker payload both references
#: filter out. Its type/precision/timezone are UNVERIFIED, so it is used for
#: diagnostics if it parses and ignored if it does not.
_SERVER_TS_KEYS: Final[tuple[str, ...]] = ("lastUpdated",)

#: ``status.json`` section this module owns. ``leviton`` (poll successes) belongs
#: to ``stages/poller.py``; ``leviton_keepalive``/``leviton_auth`` to
#: ``sources/leviton.py``.
STATUS_SECTION_WS: Final[str] = "leviton_ws"

#: How the current connection re-established state, recorded as ``sync_mode``.
#: ``flood`` is the strong form and means exactly one thing: **every desired
#: subscription target was touched by the flood on this connection**. It is the
#: signal the owner reads to decide whether the push feed is actually working,
#: so it is never reported for a connection that merely timed out — see
#: :meth:`LevitonWebSocketIngester._evaluate_sync`.
SYNC_MODE_FLOOD: Final[str] = "flood"
SYNC_MODE_TIMEOUT: Final[str] = "timeout"

#: Handshake headers for the WebSocket upgrade. ``aioleviton``'s
#: ``LevitonWebSocket.connect()`` hardcodes ``{"user-agent": ...}`` and sends no
#: ``Origin``, while both other implementations of this protocol do — and §6.1
#: records that Leviton appears to fingerprint callers, which is why the REST
#: adapter already injects the same pair. Same value, one source of truth.
WS_HANDSHAKE_ORIGIN: Final[str] = LEVITON_ORIGIN

#: Withheld reasons, in the order the gate checks them.
REASON_NOT_STARTED: Final[str] = "not_started"
REASON_DISCONNECTED: Final[str] = "disconnected"
REASON_AUTH_FAILED: Final[str] = "auth_failed"
REASON_AWAITING_SYNC: Final[str] = "awaiting_initial_sync"
REASON_STALLED: Final[str] = "stalled"


# ------------------------------------------------------------------- errors


class LevitonWsError(SourceTransientError):
    """A WebSocket transport failure: handshake, drop, stall, subscribe.

    Deliberately a :class:`~energy_capture.sources.base.SourceTransientError` so
    a caller that already handles the REST vocabulary handles this too. Nothing
    in this module lets one escape into the poll loop uncaught — the supervisor
    absorbs it and the gate closes, which turns the failure into gaps.
    """


class LevitonWsAuthError(SourceAuthError):
    """The token was rejected (or is suspected dead) on the WebSocket path.

    ``aioleviton`` cannot distinguish an auth failure from a connection failure
    during the handshake, so this is raised when we have independent evidence:
    a 401/403/406 reported through :meth:`LevitonWebSocketIngester.note_auth_failure`,
    or an auth error from the injected transport factory.
    """


# ------------------------------------------------------------- subscriptions


def _normalise_model_id(model_name: str, model_id: Any) -> str | int:
    """Canonical id type per model — mixed types are two distinct subscriptions.

    CT ids are ints on the wire (``aioleviton.models.Ct.id: int``, and the
    hand-rolled reference casts ``int(ct_id)``); hub and breaker ids are strings.
    ``aioleviton``'s subscription set is keyed on the raw value, so
    ``("IotCt", 12345)`` and ``("IotCt", "12345")`` would be sent as two
    different frames. Types are normalised here, at our seam, exactly once.
    """
    if model_name == WS_MODEL_CT:
        try:
            return int(model_id)
        except (TypeError, ValueError):
            return str(model_id)
    return str(model_id)


@dataclass(frozen=True, slots=True)
class SubscriptionTarget:
    """One ``(modelName, modelId)`` we ask the server to push to us.

    ``hub_id`` is the ``IotWhem`` whose push feed carries this object. It is
    *provenance*, not identity, so it is excluded from equality and hashing —
    ``key`` remains ``(model_name, model_id)`` and every set/dict keyed on a
    target behaves exactly as before.

    It exists because liveness is a **per-hub** property. This house has two
    hubs; one of them chattering keeps an aggregate silence watchdog perfectly
    happy while the other hub's entire push feed is dead, and the gate would
    then emit the dead hub's channels out of a frozen store. Carrying the owner
    on the subscription is what lets :meth:`LevitonWebSocketIngester.freshness`
    ask "is *this* hub still talking?".
    """

    model_name: str
    model_id: str | int
    hub_id: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "model_id", _normalise_model_id(self.model_name, self.model_id)
        )
        if self.hub_id is not None:
            object.__setattr__(self, "hub_id", str(self.hub_id))

    @property
    def key(self) -> tuple[str, str | int]:
        return (self.model_name, self.model_id)

    def __str__(self) -> str:  # pragma: no cover - logging aid
        return f"{self.model_name}/{self.model_id}"


def subscription_targets_from_snapshots(
    snapshots: Iterable[HubSnapshot],
) -> tuple[SubscriptionTarget, ...]:
    """Everything discovery found, as subscription targets.

    * the hub itself (``IotWhem``) — hub properties *and*, per the reference
      integration, CT updates nested in child arrays on all firmware;
    * every CT (``IotCt``, int id) — belt and braces. The reference that hedges
      subscribes to each CT individually, and the downside of being wrong is a
      silent outage on the whole-panel GRID_POWER feeds, which are CTs;
    * every non-placeholder breaker (``ResidentialBreaker``, str id) — firmware
      ≥2.0 does **not** deliver breaker electrical data on the hub subscription
      (PLAN.md §6.4 says so and two independent implementations confirm it).

    Breaker ids mutate on firmware ≥2.2.0 (``4C45565275C6`` →
    ``4C45565275C6_A65E``), which is precisely why the subscription set is
    recomputed from every discovery pass and diffed — see
    :meth:`LevitonWebSocketIngester.set_targets`. ``channel_id`` still comes from
    ``position`` and never from these ids (§6.5).
    """
    targets: list[SubscriptionTarget] = []
    seen: set[tuple[str, str | int]] = set()

    def remember(model_name: str, model_id: Any, hub_id: Any) -> None:
        if model_id is None or model_id == "":
            return
        target = SubscriptionTarget(
            model_name, model_id, None if hub_id in (None, "") else str(hub_id)
        )
        if target.key in seen:
            return
        seen.add(target.key)
        targets.append(target)

    for snapshot in snapshots:
        owner = snapshot.hub.device_id
        remember(WS_MODEL_HUB, owner, owner)
        for ct in snapshot.cts:
            if ct.is_unused:
                continue
            remember(WS_MODEL_CT, ct.api_id, owner)
        for breaker in snapshot.breakers:
            if breaker.is_placeholder:
                continue
            remember(WS_MODEL_BREAKER, breaker.api_id, owner)
    return tuple(targets)


# ------------------------------------------------------------ wire decoding


@dataclass(frozen=True, slots=True)
class ObjectDelta:
    """A partial update for one object, extracted from one notification frame.

    ``fields`` is exactly what arrived — never padded, never defaulted. A field
    absent from a delta is *not mentioned*, which is different from a field
    explicitly ``null`` (see :data:`NULL_POLICY_CLEAR`).
    """

    model_name: str
    model_id: str | int
    fields: Mapping[str, Any]
    server_ts_utc: datetime | None = None

    @property
    def key(self) -> tuple[str, str | int]:
        return (self.model_name, self.model_id)


def _parse_server_timestamp(value: Any) -> datetime | None:
    """Best-effort parse of a server-supplied update instant. ``None`` if unsure.

    Handles epoch seconds, epoch milliseconds and ISO-8601 (including a trailing
    ``Z``). Everything else returns ``None`` rather than guessing — this value is
    diagnostic provenance, and a wrong guess would misreport freshness.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e11:  # milliseconds
            seconds /= 1000.0
        if not (0.0 < seconds < 4.0e9):
            return None
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _server_ts_from_fields(fields: Mapping[str, Any]) -> datetime | None:
    for key in _SERVER_TS_KEYS:
        if key in fields:
            return _parse_server_timestamp(fields[key])
    return None


def parse_notification(notification: Any) -> tuple[ObjectDelta, ...]:
    """Decode one ``aioleviton`` notification into per-object partial deltas.

    ``aioleviton._listen`` hands callbacks the **inner** object, so the shape
    here is ``{"modelName", "modelId", "data"}`` — not the outer
    ``{"type": "notification", ...}`` envelope.

    Three rules, each of which is a silent-data-loss bug if got wrong:

    * ``IotWhem``: ``data["ResidentialBreaker"]`` and ``data["IotCt"]`` are lists
      of partial child dicts, each carrying its own ``"id"``. Every *other* key
      is a hub property, so the child keys are stripped before the hub delta is
      built.
    * a **direct** ``ResidentialBreaker``/``IotCt`` notification frequently omits
      ``"id"`` from ``data``; the envelope's ``modelId`` is the fallback. Missing
      this drops every per-breaker update, silently.
    * an unknown ``modelName`` (``IotSwitch``, ``Residence``, …) is ignored.

    Anything malformed yields ``()`` rather than raising: this runs inside the
    socket read loop, where an exception costs a frame and a log line.
    """
    if not isinstance(notification, Mapping):
        return ()
    model_name = notification.get("modelName")
    if not isinstance(model_name, str):
        return ()
    payload = notification.get("data")
    if not isinstance(payload, Mapping):
        return ()
    envelope_id = notification.get("modelId")

    if model_name == WS_MODEL_HUB:
        deltas: list[ObjectDelta] = []
        hub_fields = {k: v for k, v in payload.items() if k not in _CHILD_KEYS}
        if envelope_id is not None:
            deltas.append(
                ObjectDelta(
                    model_name=WS_MODEL_HUB,
                    model_id=_normalise_model_id(WS_MODEL_HUB, envelope_id),
                    fields=hub_fields,
                    server_ts_utc=_server_ts_from_fields(hub_fields),
                )
            )
        for child_model in _CHILD_KEYS:
            children = payload.get(child_model)
            if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
                continue
            for child in children:
                if not isinstance(child, Mapping):
                    continue
                child_id = child.get("id")
                if child_id is None:
                    # A nested child with no id cannot be attributed to an
                    # object; guessing would corrupt a different channel.
                    continue
                fields = {k: v for k, v in child.items() if k != "id"}
                deltas.append(
                    ObjectDelta(
                        model_name=child_model,
                        model_id=_normalise_model_id(child_model, child_id),
                        fields=fields,
                        server_ts_utc=_server_ts_from_fields(fields),
                    )
                )
        return tuple(deltas)

    if model_name in (WS_MODEL_BREAKER, WS_MODEL_CT):
        raw_id = payload.get("id")
        if raw_id is None:
            raw_id = envelope_id
        if raw_id is None:
            return ()
        fields = {k: v for k, v in payload.items() if k != "id"}
        return (
            ObjectDelta(
                model_name=model_name,
                model_id=_normalise_model_id(model_name, raw_id),
                fields=fields,
                server_ts_utc=_server_ts_from_fields(fields),
            ),
        )

    return ()


# -------------------------------------------------------------- state store


@dataclass(frozen=True, slots=True)
class FieldState:
    """The current known value of one field, plus how we know it.

    ``updated_utc``/``ts_source`` are **diagnostics**. They never reach an
    ``Observation``: §6.5 stamps one ``ts_utc`` per poll cycle and there is no
    usable per-field server timestamp on any electrical payload anywhere in the
    protocol, so provenance cannot be upgraded (see the module docstring).
    """

    value: Any
    updated_utc: datetime
    updated_monotonic: float
    ts_source: str = TS_SOURCE_RECEIPT
    updates: int = 1

    def age_s(self, now_monotonic: float) -> float:
        return max(0.0, float(now_monotonic) - self.updated_monotonic)


@dataclass(slots=True)
class ChannelState:
    """Current state of one subscribed object (hub, breaker or CT).

    Merging is strictly per field. A field that has never been received is
    **absent** — not zero, not ``None``-valued, not present-with-a-default —
    so a sampler asking for it gets nothing and emits no row.
    """

    model_name: str
    model_id: str | int
    fields: dict[str, FieldState] = field(default_factory=dict)
    null_counts: dict[str, int] = field(default_factory=dict)
    last_update_utc: datetime | None = None
    last_update_monotonic: float | None = None
    updates: int = 0

    @property
    def key(self) -> tuple[str, str | int]:
        return (self.model_name, self.model_id)

    def has(self, name: str) -> bool:
        return name in self.fields

    def value(self, name: str, default: Any = None) -> Any:
        state = self.fields.get(name)
        return default if state is None else state.value

    def values(self) -> dict[str, Any]:
        """Plain ``{field: value}`` of everything currently known."""
        return {name: state.value for name, state in self.fields.items()}

    def age_s(self, now_monotonic: float) -> float | None:
        if self.last_update_monotonic is None:
            return None
        return max(0.0, float(now_monotonic) - self.last_update_monotonic)

    def field_ages(self, now_monotonic: float) -> dict[str, float]:
        return {name: st.age_s(now_monotonic) for name, st in self.fields.items()}


class StateStore:
    """The current-state store: partial deltas merged per object, per field.

    Deliberately dumb and allocation-cheap — :meth:`apply` runs inline in the
    socket read loop (``aioleviton`` spawns no task and applies no backpressure,
    so a slow callback stalls socket reads and can starve pong handling).

    Ordering: last writer wins. The protocol carries no sequence number and no
    per-field timestamp, so out-of-order delivery is undetectable by anyone —
    both references are last-writer-wins too. Duplicates are therefore harmless
    (a repeated value re-writes the same value) and reordering is unverifiable;
    neither can corrupt the *shape* of the state.
    """

    __slots__ = ("_objects", "_null_policy", "deltas_applied", "fields_applied",
                 "fields_cleared", "fields_evicted", "nulls_seen")

    def __init__(self, *, null_policy: str = NULL_POLICY_CLEAR) -> None:
        if null_policy not in (NULL_POLICY_CLEAR, NULL_POLICY_IGNORE):
            raise ValueError(f"unknown null_policy {null_policy!r}")
        self._objects: dict[tuple[str, str | int], ChannelState] = {}
        self._null_policy = null_policy
        self.deltas_applied = 0
        self.fields_applied = 0
        self.fields_cleared = 0
        self.fields_evicted = 0
        self.nulls_seen = 0

    # ------------------------------------------------------------- accessors
    @property
    def null_policy(self) -> str:
        return self._null_policy

    def __len__(self) -> int:
        return len(self._objects)

    def __contains__(self, key: object) -> bool:
        return key in self._objects

    def keys(self) -> tuple[tuple[str, str | int], ...]:
        return tuple(self._objects)

    def objects(self) -> tuple[ChannelState, ...]:
        return tuple(self._objects.values())

    def get(self, model_name: str, model_id: Any) -> ChannelState | None:
        """Current state of one object, or ``None`` if nothing was ever received."""
        return self._objects.get((model_name, _normalise_model_id(model_name, model_id)))

    def values_for(self, model_name: str, model_id: Any) -> dict[str, Any]:
        """``{field: value}`` for one object; ``{}`` when nothing is known."""
        state = self.get(model_name, model_id)
        return {} if state is None else state.values()

    @property
    def field_count(self) -> int:
        return sum(len(state.fields) for state in self._objects.values())

    @property
    def null_counts(self) -> dict[str, int]:
        """How many explicit nulls arrived, per field name, across all objects."""
        totals: dict[str, int] = {}
        for state in self._objects.values():
            for name, count in state.null_counts.items():
                totals[name] = totals.get(name, 0) + count
        return totals

    # -------------------------------------------------------------- mutation
    def apply(
        self,
        delta: ObjectDelta,
        *,
        received_utc: datetime | None = None,
        received_monotonic: float | None = None,
        ts_source: str = TS_SOURCE_RECEIPT,
        only_if_older_than: float | None = None,
        count_nulls: bool = True,
    ) -> tuple[int, int]:
        """Merge one partial delta. Returns ``(fields_applied, fields_cleared)``.

        * A field present with a value overwrites (or creates) that field only.
        * A field present with ``null`` clears it under
          :data:`NULL_POLICY_CLEAR` — the API said "unknown", and CLAUDE.md rule
          1 says an unknown value emits no row rather than a stale one.
        * A field *absent* from the delta is untouched. This is what makes a
          partial delta safe and is why the store is never replaced wholesale.

        ``only_if_older_than`` is the seeding guard: a REST snapshot applied
        after the flood has already started must not overwrite a value the
        server pushed us seconds ago. Fields whose ``updated_monotonic`` is
        ``>=`` that mark are left alone.

        ``count_nulls=False`` applies the null policy without adding to
        :attr:`nulls_seen` / :attr:`null_counts`. Those counters exist to
        measure how often *the socket* sprays nulls (#153), which is the
        evidence that would justify flipping :data:`NULL_POLICY_IGNORE`; nulls
        that came from a REST seed would conflate two different questions.
        """
        stamp_utc = received_utc if received_utc is not None else now_utc()
        stamp_mono = (
            received_monotonic if received_monotonic is not None else time.monotonic()
        )
        if delta.server_ts_utc is not None and ts_source == TS_SOURCE_RECEIPT:
            # Prefer a server-supplied instant when one parses, and say so.
            stamp_utc = delta.server_ts_utc
            ts_source = TS_SOURCE_SERVER

        state = self._objects.get(delta.key)
        if state is None:
            state = ChannelState(model_name=delta.model_name, model_id=delta.model_id)
            self._objects[delta.key] = state

        applied = 0
        cleared = 0
        for name, value in delta.fields.items():
            existing = state.fields.get(name)
            if (
                only_if_older_than is not None
                and existing is not None
                and existing.updated_monotonic >= only_if_older_than
            ):
                continue
            if value is None:
                if count_nulls:
                    self.nulls_seen += 1
                    state.null_counts[name] = state.null_counts.get(name, 0) + 1
                if self._null_policy == NULL_POLICY_CLEAR and existing is not None:
                    del state.fields[name]
                    cleared += 1
                continue
            state.fields[name] = FieldState(
                value=value,
                updated_utc=stamp_utc,
                updated_monotonic=stamp_mono,
                ts_source=ts_source,
                updates=(existing.updates + 1) if existing is not None else 1,
            )
            applied += 1

        state.updates += 1
        state.last_update_utc = stamp_utc
        state.last_update_monotonic = stamp_mono
        self.deltas_applied += 1
        self.fields_applied += applied
        self.fields_cleared += cleared
        return (applied, cleared)

    def evict_before(self, mark: float) -> int:
        """Forget every field **not established since** ``mark``. Returns the count.

        This is *not* an age threshold and must never become one — field age
        never gates emission, because a resistive load genuinely holds its
        wattage for hours and gapping on "has not changed recently" would delete
        real data (see the module docstring).

        ``mark`` is the current connection's start instant, so this expresses
        exactly one thing: *"was this value established on the connection we are
        about to start sampling?"*. It makes the gate's connection-state
        criterion reach individual fields. Without it, a reconnect whose REST
        seed failed and whose flood missed an object leaves that object's fields
        carrying values from the **previous** connection — which the timeout
        path would then publish with a fresh ``ts_utc`` and
        ``value_source="ws"``, i.e. a hold-last-value across a disconnect
        (CLAUDE.md rule 1).

        The :class:`ChannelState` shells survive with their counters and null
        tallies, so ``status.json`` still shows what was being tracked; only the
        *values* go, and a value that is gone is a gap.
        """
        dropped = 0
        for state in self._objects.values():
            stale = [
                name
                for name, entry in state.fields.items()
                if entry.updated_monotonic < mark
            ]
            for name in stale:
                del state.fields[name]
            dropped += len(stale)
        self.fields_evicted += dropped
        return dropped

    def clear(self) -> None:
        """Forget everything. Used only when a caller wants a hard reset.

        Note that a reconnect does **not** clear the store wholesale: the values
        a fresh connection re-establishes are kept (and are what the gate
        publishes), while everything it does *not* re-establish is dropped by
        :meth:`evict_before` at the moment the gate opens.
        """
        self._objects.clear()

    # ------------------------------------------------------------ diagnostics
    def diagnostics(self, now_monotonic: float) -> dict[str, Any]:
        """Per-object, per-field last-update instants and provenance.

        This is the measurement the whole change hangs on: it is what will show
        whether WS + PUT-1-only updates CTs at ~1s or at the 2–12 minute cadence
        one reference describes once ``bandwidth`` decays from 1 to 2. Do not
        publish a freshness claim before reading it.
        """
        report: dict[str, Any] = {}
        for (model_name, model_id), state in self._objects.items():
            report[f"{model_name}/{model_id}"] = {
                "updates": state.updates,
                "age_s": state.age_s(now_monotonic),
                "last_update_utc": _iso(state.last_update_utc),
                "fields": {
                    name: {
                        "age_s": round(fs.age_s(now_monotonic), 3),
                        "updates": fs.updates,
                        "ts_source": fs.ts_source,
                        "updated_utc": _iso(fs.updated_utc),
                    }
                    for name, fs in state.fields.items()
                },
                "nulls": dict(state.null_counts),
            }
        return report


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


# ----------------------------------------------------------------- overlays


#: Wire field name → :class:`~energy_capture.sources.leviton.HubReading` attribute.
#: The WS wire is camelCase; ``aioleviton``'s REST models are snake_case. This
#: map is WS-wire knowledge and therefore lives here (PLAN.md §2.8), while §6.5's
#: row mapping stays entirely in ``sources/leviton.py``.
WS_HUB_FIELDS: Final[Mapping[str, str]] = {
    "rmsVoltageA": "volts_a",
    "rmsVoltageB": "volts_b",
    "frequencyA": "hz_a",
    "frequencyB": "hz_b",
}
WS_BREAKER_FIELDS: Final[Mapping[str, str]] = {
    "power": "power",
    "power2": "power_2",
    "rmsCurrent": "rms_current",
    "rmsCurrent2": "rms_current_2",
    "rmsVoltage": "rms_voltage",
    "rmsVoltage2": "rms_voltage_2",
}
WS_CT_FIELDS: Final[Mapping[str, str]] = {
    "activePower": "active_power",
    "activePower2": "active_power_2",
    "rmsCurrent": "rms_current",
    "rmsCurrent2": "rms_current_2",
}

#: Non-measurement fields: control state, not archived data. When the store has
#: not received one, the discovery reading's value is kept rather than being
#: turned into a gap — ``connected`` drives the keepalive's allow-list, and a
#: missing ``connected`` must not silently stop the keepalive.
WS_STATE_FIELDS: Final[Mapping[str, str]] = {
    "connected": "connected",
    "currentState": "current_state",
}


def _overlay(reading: Any, state: ChannelState | None, mapping: Mapping[str, str]) -> Any:
    """Replace measurement attributes from the store; absent field → ``None``.

    "Absent → ``None``" is the whole point: ``sources/leviton.py`` turns ``None``
    into *no row* (a gap). A field the server has never sent us must never
    surface as a zero, and must never silently fall back to the REST value the
    reading was built from — the REST value is the stale cache.
    """
    fields = {} if state is None else state.fields
    changes: dict[str, Any] = {}
    for wire_name, attr in mapping.items():
        entry = fields.get(wire_name)
        changes[attr] = None if entry is None else entry.value
    for wire_name, attr in WS_STATE_FIELDS.items():
        if not hasattr(reading, attr):
            continue
        entry = fields.get(wire_name)
        if entry is not None:
            changes[attr] = entry.value
    return dataclasses.replace(reading, **changes)


def overlay_hub(reading: HubReading, state: ChannelState | None) -> HubReading:
    """A :class:`HubReading` whose electrical fields come from the WS store."""
    result = _overlay(reading, state, WS_HUB_FIELDS)
    return dataclasses.replace(result, connected=bool(result.connected))


def overlay_breaker(reading: BreakerReading, state: ChannelState | None) -> BreakerReading:
    """A :class:`BreakerReading` whose electrical fields come from the WS store.

    ``position``/``poles``/``model`` are structural facts from discovery and are
    kept — the WS never resends them, and §6.5's ``channel_id`` depends on
    ``position`` (never on the mutable API id).
    """
    return _overlay(reading, state, WS_BREAKER_FIELDS)


def overlay_ct(reading: CtReading, state: ChannelState | None) -> CtReading:
    """A :class:`CtReading` whose electrical fields come from the WS store."""
    return _overlay(reading, state, WS_CT_FIELDS)


# ---------------------------------------------------------------- transport


class WsTransport(Protocol):
    """The seam ``aioleviton`` sits behind (PLAN.md §2.8).

    A fresh transport is built for every connection attempt, so ``connect()`` is
    called exactly once per instance and there is no ``reconnect()`` in this
    surface at all — reconnect policy belongs to the ingester's single state
    machine, not to the upstream library.
    """

    @property
    def connected(self) -> bool: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def subscribe(self, model_name: str, model_id: str | int) -> None: ...

    async def unsubscribe(self, model_name: str, model_id: str | int) -> None: ...

    def on_notification(self, callback: Callable[[Any], None]) -> Callable[[], None]: ...

    def on_disconnect(self, callback: Callable[[], None]) -> Callable[[], None]: ...

    @property
    def close_code(self) -> int | None: ...


class AioLevitonWsTransport:
    """Thin wrapper over ``aioleviton.LevitonWebSocket``.

    What it adds, all of it defensive:

    * translates ``aioleviton``'s exception tree into :class:`LevitonWsError` /
      :class:`LevitonWsAuthError`, so nothing above this line ever sees an
      upstream type (PLAN.md §2.8);
    * refuses an ``async def`` notification callback — ``_listen`` calls
      callbacks without ``await``, so a coroutine callback is *silently dropped*
      and the store simply never updates while ``connected`` stays ``True``;
    * registers the token with the log scrubber (CLAUDE.md rule 8);
    * unregisters callbacks *before* an intentional disconnect, because
      ``aioleviton``'s ``disconnect()`` fires ``on_disconnect`` even when the
      teardown was ours — the classic reconnect-storm trigger;
    * guards the ``remove()`` closures, which raise ``ValueError`` if called
      twice or after ``reset()``;
    * exposes ``close_code``. Nobody in the ecosystem records what code the
      60-minute kill actually delivers; logging ours settles it in a day. No
      logic keys on the value.
    """

    def __init__(self, ws: Any) -> None:
        self._ws = ws
        self._removers: list[Callable[[], None]] = []
        register_secret(getattr(ws, "_token", None))

    @property
    def connected(self) -> bool:
        return bool(getattr(self._ws, "connected", False))

    @property
    def close_code(self) -> int | None:
        inner = getattr(self._ws, "_ws", None)
        return getattr(inner, "close_code", None)

    async def connect(self) -> None:
        await self._guard(self._ws.connect(), op="connect")

    async def disconnect(self) -> None:
        for remove in self._removers:
            # remove() raises ValueError if called twice or after reset().
            with contextlib.suppress(Exception):
                remove()
        self._removers.clear()
        with contextlib.suppress(Exception):
            await self._ws.disconnect()

    async def subscribe(self, model_name: str, model_id: str | int) -> None:
        await self._guard(self._ws.subscribe(model_name, model_id), op="subscribe")

    async def unsubscribe(self, model_name: str, model_id: str | int) -> None:
        await self._guard(self._ws.unsubscribe(model_name, model_id), op="unsubscribe")

    def on_notification(self, callback: Callable[[Any], None]) -> Callable[[], None]:
        _reject_coroutine_callback(callback)
        remove = self._ws.on_notification(callback)
        self._removers.append(remove)
        return remove

    def on_disconnect(self, callback: Callable[[], None]) -> Callable[[], None]:
        _reject_coroutine_callback(callback)
        remove = self._ws.on_disconnect(callback)
        self._removers.append(remove)
        return remove

    @staticmethod
    async def _guard(awaitable: Awaitable[Any], *, op: str) -> Any:
        from aioleviton import (  # local import: pure-logic tests never need it
            LevitonAuthError,
            LevitonConnectionError,
            LevitonError,
        )

        try:
            return await awaitable
        except LevitonAuthError as exc:
            raise LevitonWsAuthError(f"leviton ws {op}: {exc}") from exc
        except LevitonConnectionError as exc:
            raise LevitonWsError(f"leviton ws {op}: {exc}") from exc
        except LevitonError as exc:
            raise LevitonWsError(f"leviton ws {op}: {exc}") from exc
        except (asyncio.TimeoutError, OSError) as exc:
            raise LevitonWsError(f"leviton ws {op}: {exc}") from exc


def _reject_coroutine_callback(callback: Callable[..., Any]) -> None:
    if inspect.iscoroutinefunction(callback):
        raise TypeError(
            "leviton ws callbacks must be plain functions: aioleviton calls them "
            "without await, so a coroutine callback is silently dropped and the "
            "state store never updates while the socket still reports connected"
        )


def default_ws_handshake_headers() -> dict[str, str]:
    """The headers our WebSocket upgrade must carry, as the REST path sends them.

    ``Origin``/``Referer`` are §6.1's fingerprinting spoof and are the same
    values ``sources/leviton.py`` puts on the REST session. The user-agent is
    ``aioleviton``'s own, read from the installed package so an upstream bump
    moves both paths together instead of leaving us pinned to a string it no
    longer sends. If the package cannot be imported (pure-logic test runs), the
    two headers that actually matter are still returned.
    """
    headers = {
        "origin": WS_HANDSHAKE_ORIGIN,
        "referer": f"{WS_HANDSHAKE_ORIGIN}/",
    }
    try:  # local import: pure-logic tests never need aioleviton
        from aioleviton.const import USER_AGENT
    except Exception:  # pragma: no cover - defensive; upstream layout change
        return headers
    headers["user-agent"] = str(USER_AGENT)
    return headers


class _HandshakeHeaderSession:
    """An ``aiohttp.ClientSession`` proxy that owns the WS upgrade's headers.

    **This is the vendoring note. Re-check it on every ``aioleviton`` upgrade.**

    ``aioleviton.websocket.LevitonWebSocket.connect()`` calls
    ``self._session.ws_connect(WEBSOCKET_URL, heartbeat=..., headers={"user-agent":
    USER_AGENT})`` — a hardcoded literal, with **no** ``Origin``. Both other
    implementations of this protocol send one, and PLAN.md §6.1 records that
    Leviton appears to fingerprint callers (which is why our REST adapter
    already injects ``Origin``/``Referer``). The class exposes no hook for it.

    What is vendored is therefore *nothing at all*: rather than copying
    ``connect()`` — 50 lines carrying the auth frame, the ready-status handshake
    and the listen-task lifecycle, all of which we would then own forever — the
    single attribute the call reads (``_session``) is wrapped. Every other
    attribute and method passes straight through, so ``LevitonWebSocket`` keeps
    doing its own job and only the handshake headers become ours.

    The two ways this could silently stop working, both cheap to check:

    * ``connect()`` stops reading ``self._session`` (it builds its own session,
      or the attribute is renamed) — :func:`apply_ws_handshake_headers` logs a
      warning and returns the object unwrapped rather than failing the connect;
    * ``connect()`` starts passing an ``origin=`` keyword to ``ws_connect``.
      aiohttp writes that into the ``Origin`` header *after* merging ``headers``,
      so it would win. ``ws_connect`` below drops it for exactly that reason.
    """

    __slots__ = ("_session", "_headers")

    def __init__(self, session: Any, headers: Mapping[str, str]) -> None:
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_headers", dict(headers))

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_session"), name)

    @property
    def handshake_headers(self) -> dict[str, str]:
        return dict(object.__getattribute__(self, "_headers"))

    def ws_connect(self, *args: Any, **kwargs: Any) -> Any:
        ours = object.__getattribute__(self, "_headers")
        merged: dict[str, str] = dict(kwargs.get("headers") or {})
        merged.update(ours)  # our seam is authoritative, not the library's
        kwargs["headers"] = merged
        kwargs.pop("origin", None)  # aiohttp would apply it after the merge
        return object.__getattribute__(self, "_session").ws_connect(*args, **kwargs)


def apply_ws_handshake_headers(
    ws: Any, headers: Mapping[str, str] | None = None
) -> Any:
    """Make ``aioleviton``'s hardcoded handshake headers controllable from here.

    Returns ``ws`` (wrapped in place). Never raises: a handshake missing the
    ``Origin`` spoof may well work, and refusing to connect over it would turn a
    suspicion into a guaranteed outage.
    """
    chosen = default_ws_handshake_headers() if headers is None else dict(headers)
    session = getattr(ws, "_session", None)
    if session is None:
        get_logger("leviton_ws").warning(
            "leviton_ws_handshake_headers_not_injectable",
            reason="aioleviton.LevitonWebSocket has no _session attribute",
        )
        return ws
    if isinstance(session, _HandshakeHeaderSession):
        session = object.__getattribute__(session, "_session")
    ws._session = _HandshakeHeaderSession(session, chosen)  # noqa: SLF001 - the seam
    return ws


def transport_factory_from_client(
    client: Any, *, handshake_headers: Mapping[str, str] | None = None
) -> Callable[[], Awaitable[WsTransport]]:
    """Build the production transport factory from an authenticated client.

    A **new** ``LevitonWebSocket`` per connection: ``connect()`` twice on one
    instance leaks a listen task, and building fresh means the token in the auth
    frame is always the current one after a re-login. Each one gets our
    handshake headers (:func:`apply_ws_handshake_headers`) before it connects.
    """

    async def factory() -> WsTransport:
        ws = client.create_websocket()
        apply_ws_handshake_headers(ws, handshake_headers)
        return AioLevitonWsTransport(ws)

    return factory


def _default_reconnect_delay(attempts: int) -> float:
    """``aioleviton``'s exponential backoff with jitter (capped at 16s)."""
    from aioleviton.websocket import LevitonWebSocket

    return float(LevitonWebSocket.reconnect_delay(attempts))


# -------------------------------------------------------------- the ingester


@dataclass(frozen=True, slots=True)
class Freshness:
    """Answer to "may the sampler emit rows right now?" and, if not, why not."""

    ok: bool
    reason: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.ok


class LevitonWebSocketIngester:
    """Keeps the current-state store fresh, and says when it may be sampled.

    Wiring (all injected, all optional — production passes the real ones,
    tests pass fakes and never open a socket)::

        ws = LevitonWebSocketIngester(
            transport_factory=transport_factory_from_client(adapter_client),
            seed=lambda: adapter.fetch_snapshot(residence_ids),
            keepalive=source.keepalive_round,
            reauthenticate=lambda: adapter.reauthenticate(reason="ws_401"),
        )
        await ws.set_targets(subscription_targets_from_snapshots(snapshots))
        await ws.start()
        ...                                  # run() as a background task
        if ws.can_sample():                  # THE gate — see the module docstring
            snapshot = ws.overlay_snapshot(rest_snapshot)

    Nothing here raises into a caller. :meth:`tick` and :meth:`run` absorb
    everything; a failure closes the gate, and a closed gate means gaps, which
    is the honest outcome (CLAUDE.md rule 1).
    """

    def __init__(
        self,
        *,
        transport_factory: Callable[[], Awaitable[WsTransport]],
        seed: Callable[[], Awaitable[Sequence[HubSnapshot]]] | None = None,
        keepalive: Callable[[], Awaitable[None]] | None = None,
        reauthenticate: Callable[[], Awaitable[None]] | None = None,
        targets: Iterable[SubscriptionTarget] = (),
        stall_timeout_s: float = STALL_TIMEOUT_S,
        proactive_reconnect_s: float = PROACTIVE_RECONNECT_S,
        sync_flood_timeout_s: float = SYNC_FLOOD_TIMEOUT_S,
        watchdog_interval_s: float = WATCHDOG_INTERVAL_S,
        max_backoff_s: float = MAX_RECONNECT_BACKOFF_S,
        null_policy: str = NULL_POLICY_CLEAR,
        status_store: Any = None,
        reconnect_delay: Callable[[int], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = now_utc,
    ) -> None:
        self._transport_factory = transport_factory
        self._seed = seed
        self._keepalive = keepalive
        self._reauthenticate = reauthenticate
        self._stall_timeout_s = float(stall_timeout_s)
        self._proactive_reconnect_s = float(proactive_reconnect_s)
        self._sync_flood_timeout_s = float(sync_flood_timeout_s)
        self._watchdog_interval_s = float(watchdog_interval_s)
        self._max_backoff_s = float(max_backoff_s)
        self._reconnect_delay = reconnect_delay or _default_reconnect_delay
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now
        self._log = get_logger("leviton_ws")
        self._status_store = status_store

        self._store = StateStore(null_policy=null_policy)
        self._targets: dict[tuple[str, str | int], SubscriptionTarget] = {
            t.key: t for t in targets
        }
        self._subscribed: set[tuple[str, str | int]] = set()
        self._transport: WsTransport | None = None
        self._connect_lock = asyncio.Lock()

        self._started = False
        self._stopping = False
        self._connected = False
        self._synced = False
        self._auth_suspect = False
        self._intentional_disconnect = False
        self._sync_mode: str | None = None
        self._seeded = False
        self._awaiting: set[tuple[str, str | int]] = set()
        self._sync_deadline: float | None = None
        self._connect_not_before: float | None = None
        self._connect_attempts = 0
        self._withheld_reason: str | None = REASON_NOT_STARTED

        self._connected_at_monotonic: float | None = None
        self._connected_at_utc: datetime | None = None
        self._last_message_monotonic: float | None = None
        self._last_message_utc: datetime | None = None
        self._last_hub_message_monotonic: float | None = None
        self._sync_completed_utc: datetime | None = None
        #: Per-hub liveness: ``{hub_id: monotonic of its last frame}``. One hub
        #: chattering must not certify the other hub's feed as alive.
        self._hub_activity: dict[str, float] = {}

        # counters (status.json)
        self._messages = 0
        self._messages_this_connection = 0
        self._deltas_this_connection = 0
        self._ignored_frames = 0
        self._callback_errors = 0
        self._reconnects = 0
        self._connect_failures = 0
        self._auth_failures = 0
        self._server_drops = 0
        self._subscribe_failures = 0
        self._stalls = 0
        self._keepalive_errors = 0
        self._seed_errors = 0
        self._last_close_code: int | None = None
        self._last_error: str | None = None

    # -------------------------------------------------------------- accessors
    @property
    def store(self) -> StateStore:
        return self._store

    @property
    def connected(self) -> bool:
        return bool(self._connected)

    @property
    def synced(self) -> bool:
        """True once a full state sync completed on the *current* connection."""
        return bool(self._synced)

    @property
    def subscriptions(self) -> tuple[SubscriptionTarget, ...]:
        return tuple(self._targets.values())

    @property
    def messages_received(self) -> int:
        return self._messages

    @property
    def deltas_applied(self) -> int:
        return self._store.deltas_applied

    @property
    def reconnects(self) -> int:
        return self._reconnects

    @property
    def withheld_reason(self) -> str | None:
        """Why rows are being withheld right now, or ``None`` when sampling is on.

        Re-evaluated by :meth:`freshness`, so read it after ``can_sample()`` (or
        just read ``freshness().reason``).
        """
        return self._withheld_reason

    # ------------------------------------------------------------- the gate
    def freshness(self, hub_id: str | None = None) -> Freshness:
        """The emission gate. Connection state decides; field age never does.

        Order of checks is the order of honesty: if we are not connected we do
        not know anything; if we are connected but have not re-established
        state, we still do not know; if frames have stopped arriving entirely,
        the socket is open but dead and we are back to not knowing.

        ``hub_id`` asks the liveness question of **one hub's** push feed. With
        two hubs on one socket, an aggregate "any frame from anyone" watchdog is
        satisfied by whichever hub is healthy, so a hub whose feed has gone
        silent would keep being sampled out of a frozen store while the socket
        reports ``connected: True``. Called without a hub the answer is the
        strict one — every tracked hub must be alive — because the aggregate
        callers (:meth:`overlay_snapshots`, ``sources/leviton.py``) sample all
        hubs in one cycle and must not publish a dead one's last words.

        Note this method may complete a pending sync as a side effect (the flood
        deadline is a wall-clock condition, and the sampler must not depend on
        the watchdog having ticked recently). Only the aggregate call records
        :attr:`withheld_reason`; a per-hub question must not overwrite the
        answer the poll cycle is about to read.
        """
        now_m = self._monotonic()
        record = hub_id is None
        if not self._started:
            return self._withhold(REASON_NOT_STARTED, record=record)
        if self._auth_suspect:
            return self._withhold(REASON_AUTH_FAILED, record=record)
        if not self._connected or (
            self._transport is not None and not self._transport.connected
        ):
            return self._withhold(REASON_DISCONNECTED, record=record)
        self._evaluate_sync(now_m)
        if not self._synced:
            return self._withhold(
                REASON_AWAITING_SYNC,
                record=record,
                awaiting=len(self._awaiting),
                seeded=self._seeded,
            )
        stalled_hub, silence = self._stall_candidate(now_m, hub_id)
        if silence is not None and silence > self._stall_timeout_s:
            # Open but dead. Treating this as fresh is the fabrication path.
            return self._withhold(
                REASON_STALLED,
                record=record,
                silence_s=round(silence, 1),
                hub_id=stalled_hub,
            )
        if record:
            self._withheld_reason = None
        return Freshness(ok=True, reason=None, detail={"sync_mode": self._sync_mode})

    def can_sample(self, hub_id: str | None = None) -> bool:
        """``True`` when the store may be sampled into rows. See :meth:`freshness`."""
        return self.freshness(hub_id).ok

    #: Readable alias — some call sites read better as ``is_fresh()``.
    def is_fresh(self, hub_id: str | None = None) -> bool:
        return self.can_sample(hub_id)

    def _withhold(self, reason: str, *, record: bool = True, **detail: Any) -> Freshness:
        if record:
            self._withheld_reason = reason
        return Freshness(ok=False, reason=reason, detail=detail)

    def _silence_s(self, now_m: float) -> float | None:
        """Aggregate silence: any frame, from any object, on any hub."""
        mark = self._last_message_monotonic
        if mark is None:
            mark = self._connected_at_monotonic
        if mark is None:
            return None
        return max(0.0, now_m - mark)

    def _hub_silence_s(self, now_m: float, hub_id: str) -> float | None:
        """Silence of **one hub's** feed, since connect or its last frame."""
        mark = self._hub_activity.get(str(hub_id))
        if mark is None:
            mark = self._connected_at_monotonic
        if mark is None:
            return None
        return max(0.0, now_m - mark)

    def _tracked_hubs(self) -> tuple[str, ...]:
        """Every hub any current subscription target belongs to, in order."""
        return tuple(
            dict.fromkeys(
                target.hub_id
                for target in self._targets.values()
                if target.hub_id is not None
            )
        )

    def _worst_hub_silence(self, now_m: float) -> tuple[str | None, float | None]:
        """The tracked hub that has been quiet longest, and for how long."""
        worst_hub: str | None = None
        worst: float | None = None
        for hub in self._tracked_hubs():
            silence = self._hub_silence_s(now_m, hub)
            if silence is not None and (worst is None or silence > worst):
                worst_hub, worst = hub, silence
        return (worst_hub, worst)

    def _stall_candidate(
        self, now_m: float, hub_id: str | None = None
    ) -> tuple[str | None, float | None]:
        """``(hub, silence)`` to judge the stall guard on."""
        if hub_id is not None:
            return (str(hub_id), self._hub_silence_s(now_m, str(hub_id)))
        aggregate = self._silence_s(now_m)
        worst_hub, worst = self._worst_hub_silence(now_m)
        if worst is not None and (aggregate is None or worst >= aggregate):
            return (worst_hub, worst)
        return (None, aggregate)

    def _hub_of(self, model_name: str, model_id: str | int) -> str | None:
        """Which hub's feed carries this object, if we were told."""
        target = self._targets.get((model_name, model_id))
        if target is not None and target.hub_id is not None:
            return target.hub_id
        if model_name == WS_MODEL_HUB:
            return str(model_id)
        return None

    # ------------------------------------------------------------- sampling
    def sample_object(self, model_name: str, model_id: Any) -> dict[str, Any] | None:
        """Current values for one object, or ``None`` when the gate is shut.

        ``None`` and ``{}`` mean different things and callers must keep them
        apart: ``None`` = "we do not know anything right now, emit nothing";
        ``{}`` = "the connection is healthy and this object has told us nothing",
        which is still no rows but is a very different diagnosis.

        The gate is asked about *this object's* hub, so a dead hub's channels go
        silent while its healthy sibling keeps reporting.
        """
        normalised = _normalise_model_id(model_name, model_id)
        if not self.can_sample(self._hub_of(model_name, normalised)):
            return None
        return self._store.values_for(model_name, normalised)

    def peek_object(self, model_name: str, model_id: Any) -> dict[str, Any]:
        """Ungated read — **diagnostics only**, never a source of rows."""
        return self._store.values_for(model_name, model_id)

    def overlay_snapshot(self, snapshot: HubSnapshot) -> HubSnapshot | None:
        """A :class:`HubSnapshot` with current WS values, or ``None`` if withheld.

        The structure (which hubs, breakers, CTs exist, their positions, poles
        and channels) comes from REST discovery; every *measurement* comes from
        the store, and a measurement the store has never received becomes
        ``None`` — which ``sources/leviton.py`` maps to no row at all.

        Gated on **this snapshot's hub**: the other hub having gone quiet says
        nothing about this one, and vice versa.
        """
        if not self.can_sample(str(snapshot.hub.device_id)):
            return None
        return self.overlay_snapshot_unchecked(snapshot)

    def overlay_snapshot_unchecked(self, snapshot: HubSnapshot) -> HubSnapshot:
        """:meth:`overlay_snapshot` without the gate — tests and diagnostics."""
        hub_state = self._store.get(WS_MODEL_HUB, snapshot.hub.device_id)
        return HubSnapshot(
            hub=overlay_hub(snapshot.hub, hub_state),
            breakers=tuple(
                overlay_breaker(b, self._store.get(WS_MODEL_BREAKER, b.api_id))
                for b in snapshot.breakers
            ),
            cts=tuple(
                overlay_ct(c, self._store.get(WS_MODEL_CT, c.api_id))
                for c in snapshot.cts
            ),
        )

    def overlay_snapshots(
        self, snapshots: Sequence[HubSnapshot]
    ) -> tuple[HubSnapshot, ...] | None:
        """All hubs, or none — the strict aggregate answer.

        Deliberately all-or-nothing rather than per-hub filtering. The caller
        maps whatever comes back into one poll cycle's rows, so quietly
        returning a subset would drop a hub's channels with no reason recorded
        anywhere and no REST fallback in ``hybrid``. A shut aggregate gate, by
        contrast, names its reason and each mode's documented policy applies.
        Per-hub sampling is available through :meth:`overlay_snapshot`.
        """
        if not self.can_sample():
            return None
        return tuple(self.overlay_snapshot_unchecked(s) for s in snapshots)

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Mark the ingester live and make the first connection attempt.

        A failed first attempt is *not* fatal: the gate stays shut (so the
        collector produces gaps rather than stale rows) and :meth:`tick` retries
        on the backoff ladder. A container that refuses to boot because a
        third-party cloud is having a bad minute is worse than one that boots
        degraded and heals.
        """
        self._started = True
        self._stopping = False
        self._withheld_reason = REASON_DISCONNECTED
        await self._attempt_connect(reason="start")

    async def aclose(self) -> None:
        """Stop and tear down. Idempotent; never raises."""
        self._stopping = True
        self._started = False
        await self._teardown()
        self._withheld_reason = REASON_NOT_STARTED

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Supervisor loop: watchdog, proactive reconnect, backoff. Never raises.

        Intended to be spawned as a background task (or driven by the source's
        ``background_tasks()`` seam, whose runner already absorbs exceptions).
        """
        if not self._started:
            await self.start()
        try:
            while not self._stopping and not (stop is not None and stop.is_set()):
                await self.tick()
                await self._sleep(self._watchdog_interval_s)
        except asyncio.CancelledError:
            raise
        finally:
            await self.aclose()

    async def tick(self) -> None:
        """One watchdog iteration. Absorbs everything; never raises.

        Responsibilities, in order:

        1. reconnect if disconnected (respecting the backoff deadline);
        2. re-authenticate and cycle if a 401/403/406 was reported;
        3. cycle if the socket, or any one hub's feed, has gone silent (the
           silent-stall guard);
        4. cycle *before* the server's 60-minute hard kill;
        5. otherwise retry any subscribe that failed, finish a pending sync, and
           publish the counters.
        """
        if self._stopping or not self._started:
            return
        try:
            now_m = self._monotonic()
            if self._auth_suspect:
                await self._cycle(reason="auth", reauth=True)
            elif not self._connected:
                await self._attempt_connect(reason="reconnect")
            else:
                stalled_hub, silence = self._stall_candidate(now_m)
                age = (
                    None
                    if self._connected_at_monotonic is None
                    else now_m - self._connected_at_monotonic
                )
                if silence is not None and silence > self._stall_timeout_s:
                    self._stalls += 1
                    self._log.warning(
                        "leviton_ws_stalled",
                        silence_s=round(silence, 1),
                        stall_timeout_s=self._stall_timeout_s,
                        hub_id=stalled_hub,
                        messages=self._messages_this_connection,
                    )
                    await self._cycle(reason="stalled")
                elif age is not None and age >= self._proactive_reconnect_s:
                    self._log.info(
                        "leviton_ws_proactive_reconnect",
                        connection_age_s=round(age, 1),
                        deadline_s=self._proactive_reconnect_s,
                    )
                    await self._cycle(reason="proactive")
                else:
                    await self._retry_pending_subscribes()
                    self._evaluate_sync(now_m)
            self._publish_status()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - the watchdog must never die
            self._last_error = str(exc)
            self._log.warning("leviton_ws_tick_error", error=str(exc))

    def note_auth_failure(self, error: Any = None) -> None:
        """Report independent evidence that the token is dead (a 401/403/406).

        ``aioleviton`` silently ignores every non-``notification`` frame, so an
        error frame mid-stream never reaches us: auth expiry otherwise surfaces
        only as silence or a close, and a socket that looks connected while the
        cloud has stopped honouring us is the worst kind of quiet failure. The
        §6.4 keepalive sees the 401 first — this is how it tells us.
        """
        self._auth_failures += 1
        self._auth_suspect = True
        self._synced = False
        self._withheld_reason = REASON_AUTH_FAILED
        self._last_error = None if error is None else str(error)
        self._log.warning("leviton_ws_auth_suspect", error=self._last_error)

    # ---------------------------------------------------------- subscriptions
    async def set_targets(self, targets: Iterable[SubscriptionTarget]) -> None:
        """Replace the desired subscription set, subscribing/unsubscribing the diff.

        This is how ~25 newly installed breakers start streaming without a
        restart: the hourly discovery pass recomputes the targets and hands them
        here. It is also how firmware ≥2.2.0's mutated breaker ids are handled —
        the old id is unsubscribed and the new one subscribed, while
        ``channel_id`` (built from ``position``) never moves.

        Subscribing is driven off "desired but not currently subscribed", not
        off the added/removed diff. A target whose subscribe failed earlier is
        already in ``self._targets``, so a diff-only implementation would never
        retry it and that object would go unpushed for the life of the
        connection while nothing above noticed.
        """
        desired = {t.key: t for t in targets}
        added = [t for key, t in desired.items() if key not in self._targets]
        removed = [t for key, t in self._targets.items() if key not in desired]
        self._targets = desired
        # An object we no longer want must not hold the flood sync open.
        self._awaiting &= set(desired)
        tracked = set(self._tracked_hubs())
        self._hub_activity = {
            hub: mark for hub, mark in self._hub_activity.items() if hub in tracked
        }
        if added or removed:
            self._log.info(
                "leviton_ws_subscriptions_changed",
                added=len(added),
                removed=len(removed),
                total=len(desired),
            )
        if not self._connected or self._transport is None:
            # Nothing to send; the next connect subscribes the whole set.
            self._subscribed &= set(desired)
            return
        for target in removed:
            await self._unsubscribe_one(target)
        now_m = self._monotonic()
        for hub in self._tracked_hubs():
            self._hub_activity.setdefault(hub, now_m)
        if not self._synced:
            # Still establishing state: a newly desired object must be part of
            # what `flood` means, or the strong sync would be declared without it.
            self._awaiting |= {t.key for t in added}
        await self._retry_pending_subscribes()

    async def add_target(self, target: SubscriptionTarget) -> None:
        """Subscribe one newly discovered object (idempotent)."""
        if target.key in self._targets and target.key in self._subscribed:
            return
        new = target.key not in self._targets
        self._targets[target.key] = target
        if target.hub_id is not None:
            self._hub_activity.setdefault(target.hub_id, self._monotonic())
        if new and self._connected and not self._synced:
            self._awaiting.add(target.key)
        if self._connected and self._transport is not None:
            await self._subscribe_one(target)

    async def remove_target(self, target: SubscriptionTarget) -> None:
        """Stop subscribing an object that discovery no longer reports."""
        self._targets.pop(target.key, None)
        self._awaiting.discard(target.key)
        if self._connected and self._transport is not None:
            await self._unsubscribe_one(target)
        else:
            self._subscribed.discard(target.key)

    async def _retry_pending_subscribes(self) -> None:
        """Subscribe every desired target that is not currently subscribed.

        A per-target subscribe failure is a silent data outage for that object
        that lasts the whole connection unless something retries it — the
        server never pushes what it was never asked for, and the object cannot
        block the sync either, so nothing else would notice. Runs every watchdog
        tick; subscribing is one small frame and this is not a
        performance-sensitive process.
        """
        if not self._connected or self._transport is None:
            return
        for target in tuple(self._targets.values()):
            if target.key not in self._subscribed:
                await self._subscribe_one(target)

    async def _subscribe_one(self, target: SubscriptionTarget) -> None:
        transport = self._transport
        if transport is None:
            return
        try:
            await transport.subscribe(target.model_name, target.model_id)
        except Exception as exc:  # noqa: BLE001 - a failed subscribe is data loss, not a crash
            # A failed subscribe is a silent data outage for that object, so it
            # is counted and the object is left out of _subscribed — the next
            # watchdog tick retries it, and until it succeeds the object keeps
            # the strong `flood` sync open rather than being quietly forgotten.
            self._subscribe_failures += 1
            self._log.warning(
                "leviton_ws_subscribe_failed", target=str(target), error=str(exc)
            )
            self._subscribed.discard(target.key)
            if isinstance(exc, LevitonWsAuthError):
                self.note_auth_failure(exc)
            return
        self._subscribed.add(target.key)

    async def _unsubscribe_one(self, target: SubscriptionTarget) -> None:
        transport = self._transport
        self._subscribed.discard(target.key)
        if transport is None:
            return
        with contextlib.suppress(LevitonWsError, LevitonWsAuthError):
            await transport.unsubscribe(target.model_name, target.model_id)

    # ------------------------------------------------------------- connecting
    async def _attempt_connect(self, *, reason: str, reauth: bool = False) -> bool:
        """One connect attempt, gated by the backoff deadline. Never raises."""
        if self._stopping:
            return False
        now_m = self._monotonic()
        if self._connect_not_before is not None and now_m < self._connect_not_before:
            return False
        async with self._connect_lock:
            if self._connected:
                return True
            if reauth and self._reauthenticate is not None:
                try:
                    await self._reauthenticate()
                    self._auth_suspect = False
                except Exception as exc:  # noqa: BLE001 - keep the loop alive
                    self._auth_failures += 1
                    self._last_error = str(exc)
                    self._log.warning("leviton_ws_reauth_failed", error=str(exc))
                    self._back_off()
                    return False
            try:
                await self._connect_once(reason=reason)
            except LevitonWsAuthError as exc:
                self._auth_failures += 1
                self._auth_suspect = True
                self._last_error = str(exc)
                self._withheld_reason = REASON_AUTH_FAILED
                self._log.warning("leviton_ws_connect_auth_failed", error=str(exc))
                await self._teardown()
                self._back_off()
                return False
            except Exception as exc:  # noqa: BLE001 - typed above, absorbed here
                self._connect_failures += 1
                self._last_error = str(exc)
                self._withheld_reason = REASON_DISCONNECTED
                self._log.warning(
                    "leviton_ws_connect_failed",
                    reason=reason,
                    attempts=self._connect_attempts + 1,
                    error=str(exc),
                )
                await self._teardown()
                self._back_off()
                return False
        # NOTE: the backoff ladder is NOT cleared here. A handshake that
        # completes has not proved anything — a server that accepts every
        # connection and drops it a second later would reset the ladder on every
        # attempt and turn this into a hot reconnect loop against Leviton. The
        # ladder is cleared in `_mark_synced`, i.e. when a connection has
        # actually re-established state on a live socket. Only the *deadline* is
        # cleared, so the next attempt is not blocked by the one that worked.
        self._connect_not_before = None
        return True

    async def _connect_once(self, *, reason: str) -> None:
        """PUT keepalive → connect → subscribe → seed → await flood.

        The keepalive PUT goes **first**: ``bandwidth: 1`` is what triggers the
        server's state flood, and the flood is what re-establishes state after a
        gap. (It is the existing §6.4 PUT, value ``1``, never ``0``.)
        """
        await self._fire_keepalive()

        transport = await self._transport_factory()
        transport.on_notification(self._handle_notification)
        transport.on_disconnect(self._handle_disconnect)
        self._transport = transport
        self._intentional_disconnect = False
        await transport.connect()

        now_m = self._monotonic()
        self._connected = True
        self._synced = False
        self._sync_mode = None
        self._seeded = False
        self._connected_at_monotonic = now_m
        self._connected_at_utc = self._now()
        self._last_message_monotonic = now_m
        self._messages_this_connection = 0
        self._deltas_this_connection = 0
        self._subscribed.clear()
        self._sync_deadline = now_m + self._sync_flood_timeout_s
        self._hub_activity = {hub: now_m for hub in self._tracked_hubs()}
        self._log.info("leviton_ws_connected", reason=reason, targets=len(self._targets))

        # Objects that must be touched by the flood before we call it a sync.
        # Derived from what we WANT, not from what happened to subscribe: a
        # target whose subscribe failed is never pushed, so letting it drop out
        # of the wait set would let a partial connection claim the strong
        # `flood` sync while that object stayed dark.
        self._awaiting = set(self._targets)
        for target in tuple(self._targets.values()):
            await self._subscribe_one(target)

        await self._seed_from_rest()
        self._evaluate_sync(self._monotonic())

    async def _fire_keepalive(self) -> None:
        if self._keepalive is None:
            return
        try:
            await self._keepalive()
        except Exception as exc:  # noqa: BLE001 - a 502 here must not stop connect
            self._keepalive_errors += 1
            self._log.debug("leviton_ws_keepalive_failed", error=str(exc))

    async def _seed_from_rest(self) -> None:
        """Seed the store from one REST snapshot so state is complete on connect.

        The honesty cost, stated plainly because it must not be an accident: the
        REST snapshot *is* the stale cache this module exists to escape, so for
        the first seconds after a (re)connect a seeded field may be minutes old
        while the collector believes it is current. At a 55-minute reconnect
        cadence that is ≲0.1% of samples. It is mitigated three ways: the flood
        overwrites seeded fields within seconds (and a seed never overwrites a
        value the flood already delivered — see ``only_if_older_than``), the
        gate still waits for flood coverage or the timeout, and every seeded
        field is tagged ``ts_source="rest_seed"`` so the cost is measurable.
        """
        if self._seed is None:
            return
        connect_mark = self._connected_at_monotonic or self._monotonic()
        try:
            snapshots = await self._seed()
        except Exception as exc:  # noqa: BLE001 - a failed seed is not fatal
            self._seed_errors += 1
            self._last_error = str(exc)
            self._log.warning("leviton_ws_seed_failed", error=str(exc))
            return
        stamp_utc = self._now()
        stamp_mono = self._monotonic()
        applied = 0
        cleared = 0
        for delta in _seed_deltas(snapshots):
            one_applied, one_cleared = self._store.apply(
                delta,
                received_utc=stamp_utc,
                received_monotonic=stamp_mono,
                ts_source=TS_SOURCE_REST_SEED,
                only_if_older_than=connect_mark,
                # These nulls are REST's, not the socket's; #153's counters
                # measure how often the *socket* sprays nulls.
                count_nulls=False,
            )
            applied += one_applied
            cleared += one_cleared
        # "Seeded" must mean "the seed established at least one value we could
        # emit". A seed that produced nothing — no hubs, an empty snapshot, all
        # nulls — is not state, and letting it set this flag is how a connection
        # carrying nothing at all ends up reporting `synced: true`.
        self._seeded = applied > 0
        self._log.debug(
            "leviton_ws_seeded",
            objects=len(self._store),
            fields=applied,
            cleared=cleared,
            seeded=self._seeded,
        )

    def _back_off(self) -> None:
        self._connect_attempts += 1
        delay = min(
            float(self._reconnect_delay(self._connect_attempts)), self._max_backoff_s
        )
        self._connect_not_before = self._monotonic() + delay
        self._log.debug(
            "leviton_ws_backoff", attempts=self._connect_attempts, delay_s=round(delay, 2)
        )

    async def _cycle(self, *, reason: str, reauth: bool = False) -> None:
        """Tear the connection down and immediately build a new one.

        Emission stops *first*: ``_synced`` is cleared before anything else, so
        there is no window in which a sampler could read the store while the
        socket is being replaced.
        """
        self._synced = False
        self._withheld_reason = REASON_DISCONNECTED
        self._reconnects += 1
        await self._teardown()
        self._log.info("leviton_ws_reconnecting", reason=reason, reconnects=self._reconnects)
        await self._attempt_connect(reason=reason, reauth=reauth)

    async def _teardown(self) -> None:
        transport, self._transport = self._transport, None
        self._connected = False
        self._synced = False
        self._subscribed.clear()
        self._awaiting = set()
        self._sync_deadline = None
        if transport is None:
            return
        self._intentional_disconnect = True
        with contextlib.suppress(Exception):
            self._last_close_code = transport.close_code
        with contextlib.suppress(Exception):
            await transport.disconnect()
        self._intentional_disconnect = False

    # ------------------------------------------------------------ the callback
    def _handle_notification(self, notification: Any) -> None:
        """Merge one frame into the store. **Plain ``def``, and hot.**

        ``aioleviton`` calls this inline in its read loop with no queue and no
        task, so it must stay a dict update plus a timestamp: a slow callback
        stalls socket reads and, with ``heartbeat=30``, can starve pong handling.
        Exceptions are counted rather than raised — the upstream loop would log
        them forever while the store silently stopped updating.
        """
        try:
            now_m = self._monotonic()
            self._messages += 1
            self._messages_this_connection += 1
            self._last_message_monotonic = now_m
            self._last_message_utc = self._now()
            deltas = parse_notification(notification)
            if not deltas:
                self._ignored_frames += 1
                return
            for delta in deltas:
                if delta.model_name == WS_MODEL_HUB:
                    # Line voltage and frequency genuinely jitter, so hub chatter
                    # is the most trustworthy liveness beacon we have. Recorded
                    # as a diagnostic; the stall guard still keys on all frames.
                    self._last_hub_message_monotonic = now_m
                owner = self._hub_of(delta.model_name, delta.model_id)
                if owner is not None:
                    # Liveness is per hub: this frame proves *this* hub's feed is
                    # alive and says nothing whatever about its sibling's.
                    self._hub_activity[owner] = now_m
                self._store.apply(
                    delta,
                    received_utc=self._last_message_utc,
                    received_monotonic=now_m,
                )
                self._deltas_this_connection += 1
                self._awaiting.discard(delta.key)
        except Exception as exc:  # noqa: BLE001 - never break the read loop
            self._callback_errors += 1
            self._last_error = str(exc)

    def _handle_disconnect(self) -> None:
        """Server-side drop. **Plain ``def``**; sets state only, never reconnects.

        ``aioleviton`` fires disconnect callbacks for *intentional* teardowns as
        well (its ``disconnect()`` cancels the listen task, whose ``finally``
        fires them), so calling ``reconnect()`` from here is a recursion storm.
        The single reconnect state machine lives in :meth:`tick`.

        It does, however, **count the drop and step the backoff ladder**. This is
        the other half of "a connection succeeded" meaning "a connection proved
        itself": a server that accepts a handshake and drops it immediately would
        otherwise be retried by the next tick with no delay at all, forever.
        """
        if self._intentional_disconnect:
            return
        self._connected = False
        self._synced = False
        self._server_drops += 1
        self._reconnects += 1
        self._back_off()
        self._withheld_reason = REASON_DISCONNECTED
        transport = self._transport
        code = None
        if transport is not None:
            with contextlib.suppress(Exception):
                code = transport.close_code
        self._last_close_code = code
        age = (
            None
            if self._connected_at_monotonic is None
            else round(self._monotonic() - self._connected_at_monotonic, 1)
        )
        self._log.warning(
            "leviton_ws_disconnected",
            close_code=code,
            connection_age_s=age,
            messages=self._messages_this_connection,
            server_drops=self._server_drops,
            attempts=self._connect_attempts,
        )

    # ------------------------------------------------------------------ sync
    def _evaluate_sync(self, now_m: float) -> None:
        """Decide whether the current connection has re-established state.

        Two ways in, both recorded:

        * :data:`SYNC_MODE_FLOOD` — there is at least one desired target and the
          flood has touched **every** one of them on this connection. This is
          the strong form and the one we want.
        * :data:`SYNC_MODE_TIMEOUT` — the flood did not cover everything within
          ``sync_flood_timeout_s``, but we do have state (a REST seed, or at
          least some deltas). Sampling resumes for the fields this connection
          established; the ones it did not are evicted by :meth:`_mark_synced`
          and become gaps.

        With neither (no seed, no deltas) the gate stays shut. That is the
        honest answer to "we connected and nothing ever arrived".

        ``flood`` is never reported off the timeout path — not even when
        ``_awaiting`` happens to be empty because there was nothing to await.
        ``sync_mode`` is the signal the owner has been told to read to decide
        whether the WebSocket is working, so a connection carrying nothing but
        the REST cache reporting ``flood`` is worse than reporting nothing.
        """
        if self._synced or not self._connected:
            return
        if self._targets and not self._awaiting:
            self._mark_synced(SYNC_MODE_FLOOD, now_m)
            return
        if self._sync_deadline is not None and now_m >= self._sync_deadline:
            if self._seeded or self._deltas_this_connection:
                self._mark_synced(SYNC_MODE_TIMEOUT, now_m)

    def _mark_synced(self, mode: str, now_m: float) -> None:
        """Open the gate — and first, drop everything this connection did not establish.

        The eviction is the per-field half of the emission gate. ``StateStore``
        is deliberately not cleared on reconnect, so without it a field the
        flood never re-established and the seed never refreshed (a failed seed,
        a 502, an object the flood missed) would be sampled straight out of the
        **previous** connection and emitted with a current ``ts_utc`` labelled
        ``value_source="ws"`` — the hold-last-value CLAUDE.md rule 1 forbids.

        This is a connection-membership test, never an age threshold: a field
        pushed one second after connect and a field pushed fifty-five minutes
        after connect are equally welcome to stay.
        """
        mark = self._connected_at_monotonic
        evicted = 0 if mark is None else self._store.evict_before(mark)
        self._synced = True
        self._sync_mode = mode
        self._sync_completed_utc = self._now()
        self._withheld_reason = None
        # A connection that reached sync is a connection that proved itself;
        # that, and only that, clears the reconnect ladder (see _attempt_connect).
        self._connect_attempts = 0
        self._connect_not_before = None
        self._log.info(
            "leviton_ws_synced",
            mode=mode,
            objects=len(self._store),
            subscriptions=len(self._subscribed),
            targets=len(self._targets),
            awaiting=len(self._awaiting),
            seeded=self._seeded,
            fields_evicted=evicted,
            elapsed_s=(
                None
                if self._connected_at_monotonic is None
                else round(now_m - self._connected_at_monotonic, 2)
            ),
        )

    # ---------------------------------------------------------------- status
    def status_snapshot(self, *, include_fields: bool = False) -> dict[str, Any]:
        """Everything ``status.json`` should carry about the push feed.

        The eight keys the task requires are first; the rest exist because
        several load-bearing facts about this protocol are *unmeasured* and only
        our own hardware can settle them: the close code of the 60-minute kill,
        the real message volume, how often explicit nulls arrive, whether the
        flood ever covers everything, and how stale a REST seed actually is.
        """
        now_m = self._monotonic()
        fresh = self.freshness()
        connection_age = (
            None
            if self._connected_at_monotonic is None or not self._connected
            else round(now_m - self._connected_at_monotonic, 1)
        )
        silence = self._silence_s(now_m)
        snapshot: dict[str, Any] = {
            "connected": self._connected,
            "subscriptions": len(self._targets),
            "messages_received": self._messages,
            "deltas_applied": self._store.deltas_applied,
            "reconnects": self._reconnects,
            "last_message_utc": _iso(self._last_message_utc),
            "sync_completed_utc": _iso(self._sync_completed_utc),
            "withheld_reason": fresh.reason,
            # --- beyond the required set, all of it diagnostic ---
            "can_sample": fresh.ok,
            "synced": self._synced,
            "sync_mode": self._sync_mode,
            "seeded_from_rest": self._seeded,
            "subscriptions_active": len(self._subscribed),
            "subscriptions_pending": len(
                set(self._targets) - self._subscribed
            ),
            "subscribe_failures": self._subscribe_failures,
            "awaiting_sync": len(self._awaiting),
            "objects_tracked": len(self._store),
            "fields_tracked": self._store.field_count,
            "fields_applied": self._store.fields_applied,
            "fields_cleared": self._store.fields_cleared,
            "fields_evicted": self._store.fields_evicted,
            "null_deltas": self._store.nulls_seen,
            "null_deltas_by_field": self._store.null_counts,
            "null_policy": self._store.null_policy,
            "ignored_frames": self._ignored_frames,
            "callback_errors": self._callback_errors,
            "connect_failures": self._connect_failures,
            "auth_failures": self._auth_failures,
            "server_drops": self._server_drops,
            "connect_attempts": self._connect_attempts,
            "stalls": self._stalls,
            "keepalive_errors": self._keepalive_errors,
            "seed_errors": self._seed_errors,
            "connection_age_s": connection_age,
            "connected_at_utc": _iso(self._connected_at_utc),
            "seconds_since_message": None if silence is None else round(silence, 1),
            # Per hub, because the aggregate above is satisfied by whichever hub
            # is healthy — the exact way a dead feed hid behind a live one.
            "hub_silence_s": {
                hub: (
                    None
                    if (per_hub := self._hub_silence_s(now_m, hub)) is None
                    else round(per_hub, 1)
                )
                for hub in self._tracked_hubs()
            },
            "stalled_hubs": [
                hub
                for hub in self._tracked_hubs()
                if (per_hub := self._hub_silence_s(now_m, hub)) is not None
                and per_hub > self._stall_timeout_s
            ],
            "seconds_since_hub_message": (
                None
                if self._last_hub_message_monotonic is None
                else round(now_m - self._last_hub_message_monotonic, 1)
            ),
            "messages_this_connection": self._messages_this_connection,
            "messages_per_s": (
                None
                if not connection_age
                else round(self._messages_this_connection / connection_age, 3)
            ),
            "last_close_code": self._last_close_code,
            "stall_timeout_s": self._stall_timeout_s,
            "proactive_reconnect_s": self._proactive_reconnect_s,
            "last_error": self._last_error,
        }
        if include_fields:
            snapshot["objects"] = self._store.diagnostics(now_m)
        return snapshot

    def field_diagnostics(self) -> dict[str, Any]:
        """Per-field last-update instants and provenance (see :meth:`StateStore.diagnostics`)."""
        return self._store.diagnostics(self._monotonic())

    def _publish_status(self) -> None:
        """Write the ``leviton_ws`` section, per-field diagnostics included.

        ``include_fields=True`` deliberately: the per-field last-update instants
        are the measurement this whole change hangs on — whether the frozen
        channels move, and at what cadence — and a diagnostic that lives only
        behind a Python method call is a diagnostic nobody reads at 3am. At ~40
        channels the ``objects`` map is tens of kilobytes, which is nothing for a
        file rewritten atomically every watchdog tick on a machine that is not
        performance-sensitive.

        This is what makes ``field_diagnostics`` observable *without* letting it
        become a gate: it is reported, never consulted by :meth:`freshness`.
        """
        store = self._status_store
        if store is None:
            return
        try:
            store.set(STATUS_SECTION_WS, **self.status_snapshot(include_fields=True))
        except Exception:  # pragma: no cover - status must never break the loop
            self._log.debug("leviton_ws_status_write_failed")


# ------------------------------------------------------------ seed conversion


#: Measurement fields the seed may carry as an explicit ``None``. Exactly the
#: keys of the three overlay maps — i.e. everything that becomes row data.
_SEED_MEASUREMENT_FIELDS: Final[frozenset[str]] = frozenset(
    {*WS_HUB_FIELDS, *WS_BREAKER_FIELDS, *WS_CT_FIELDS}
)


def _seed_deltas(snapshots: Iterable[HubSnapshot]) -> tuple[ObjectDelta, ...]:
    """Turn REST readings into store deltas keyed exactly like WS notifications.

    The store speaks the WS wire's camelCase field names, so this maps our
    reading dataclasses back onto them.

    **A measurement field keeps its explicit ``None``**, and that is the whole
    subtlety here. Under :data:`NULL_POLICY_CLEAR` a null is not "no news", it
    is "the API said unknown" — which is precisely what should clear a value
    carried over from a previous connection. Dropping the ``None`` instead would
    make the seed able to *overwrite* a stale field but never to *clear* one, so
    a channel whose current REST value is null would keep the previous
    connection's number and be emitted as current, where the REST path emits no
    row at all. Same input, two ingestion paths, different data.

    The control-state fields (:data:`WS_STATE_FIELDS`: ``connected``,
    ``currentState``) still drop their ``None``s. They are not archived data —
    ``connected`` drives the keepalive's allow-list — and clearing one would
    silently stop the keepalive for that hub.
    """
    deltas: list[ObjectDelta] = []

    def add(model_name: str, model_id: Any, fields: Mapping[str, Any]) -> None:
        if model_id is None or model_id == "":
            return
        present = {
            k: v
            for k, v in fields.items()
            if v is not None or k in _SEED_MEASUREMENT_FIELDS
        }
        if not present:
            return
        deltas.append(
            ObjectDelta(
                model_name=model_name,
                model_id=_normalise_model_id(model_name, model_id),
                fields=present,
            )
        )

    for snapshot in snapshots:
        hub = snapshot.hub
        add(
            WS_MODEL_HUB,
            hub.device_id,
            {
                "rmsVoltageA": hub.volts_a,
                "rmsVoltageB": hub.volts_b,
                "frequencyA": hub.hz_a,
                "frequencyB": hub.hz_b,
                "connected": hub.connected,
            },
        )
        for breaker in snapshot.breakers:
            if breaker.is_placeholder:
                continue
            add(
                WS_MODEL_BREAKER,
                breaker.api_id,
                {
                    "power": breaker.power,
                    "power2": breaker.power_2,
                    "rmsCurrent": breaker.rms_current,
                    "rmsCurrent2": breaker.rms_current_2,
                    "rmsVoltage": breaker.rms_voltage,
                    "rmsVoltage2": breaker.rms_voltage_2,
                    "connected": breaker.connected,
                    "currentState": breaker.current_state,
                },
            )
        for ct in snapshot.cts:
            if ct.is_unused:
                continue
            add(
                WS_MODEL_CT,
                ct.api_id,
                {
                    "activePower": ct.active_power,
                    "activePower2": ct.active_power_2,
                    "rmsCurrent": ct.rms_current,
                    "rmsCurrent2": ct.rms_current_2,
                    "connected": ct.connected,
                },
            )
    return tuple(deltas)
