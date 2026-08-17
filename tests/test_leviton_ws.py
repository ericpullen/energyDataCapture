"""Leviton WebSocket ingester tests — offline, fake transport, never a socket.

``tests/conftest.py`` refuses any non-loopback connection; nothing here would
reach one anyway. Every test drives :class:`FakeTransport`, which speaks the
same four verbs and two callbacks ``aioleviton.LevitonWebSocket`` does and
reproduces the two behaviours of it that matter: notifications are delivered to
a **plain function** inline, and ``disconnect()`` fires the disconnect callback
even when the teardown was ours.

What is pinned here, in order of how much it would hurt to get wrong:

1. **The emission gate.** ``can_sample()`` is False before the initial sync,
   False the instant the socket drops, False when the socket is open but silent,
   and False again after every reconnect until state has been re-established.
   A True here that should be False means the sampler writes rows from a frozen
   store — fabrication with a current timestamp, CLAUDE.md rule 1.
2. **Partial-delta merging.** A field absent from a delta is untouched; a field
   never received is *absent*, never zero; an explicit null clears it (so the
   sampler gaps, exactly as the REST path does for a null API field).
3. **The silent-stall guard**, because an open-but-dead socket is the failure
   mode that would otherwise look perfectly healthy.
4. **Lifecycle**: the proactive reconnect before the server's 60-minute hard
   kill resubscribes everything and re-syncs; a breaker discovered at runtime
   starts receiving without a restart (~25 are arriving).
5. **Nothing crashes a caller**, and the errors that do escape are the
   ``SourceTransientError``/``SourceAuthError`` vocabulary the poll loop already
   knows.
"""

from __future__ import annotations

import ast
import asyncio
import json
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from energy_capture import logging as ec_logging
from energy_capture.health import StatusStore
from energy_capture.sources.base import SourceAuthError, SourceTransientError
from energy_capture.sources.leviton import (
    BreakerReading,
    CtReading,
    HubReading,
    HubSnapshot,
)
from energy_capture.sources import leviton_ws as ws_module
from energy_capture.sources.leviton_ws import (
    NULL_POLICY_CLEAR,
    NULL_POLICY_IGNORE,
    REASON_AUTH_FAILED,
    REASON_AWAITING_SYNC,
    REASON_DISCONNECTED,
    REASON_NOT_STARTED,
    REASON_STALLED,
    SYNC_MODE_FLOOD,
    SYNC_MODE_TIMEOUT,
    TS_SOURCE_RECEIPT,
    TS_SOURCE_REST_SEED,
    TS_SOURCE_SERVER,
    WS_HANDSHAKE_ORIGIN,
    WS_MODEL_BREAKER,
    WS_MODEL_CT,
    WS_MODEL_HUB,
    AioLevitonWsTransport,
    LevitonWebSocketIngester,
    LevitonWsAuthError,
    LevitonWsError,
    ObjectDelta,
    StateStore,
    SubscriptionTarget,
    apply_ws_handshake_headers,
    default_ws_handshake_headers,
    overlay_breaker,
    overlay_hub,
    parse_notification,
    subscription_targets_from_snapshots,
    transport_factory_from_client,
)

HUB_A = "1000_0046_1D52"
HUB_B = "1000_0046_1D48"
BREAKER_ID = "4C45565275C6_A65E"
CT_ID = 3
#: Panel B's own channels — this house has exactly two hubs on one socket, and
#: the per-hub liveness tests need each one to own distinct objects.
BREAKER_ID_B = "4C4556521234_B77F"
CT_ID_B = 7

BASE_UTC = datetime(2026, 8, 17, 12, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------- doubles


class Clock:
    """Monotonic clock and matching UTC wall clock, both driven by hand."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def utc(self) -> datetime:
        return BASE_UTC + timedelta(seconds=self.t - 1000.0)

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FakeTransport:
    """A stand-in for ``aioleviton.LevitonWebSocket`` behind our seam."""

    def __init__(self) -> None:
        self.connected = False
        self.close_code: int | None = None
        self.subscribed: list[tuple[str, str | int]] = []
        self.unsubscribed: list[tuple[str, str | int]] = []
        self.notification_cb: Any = None
        self.disconnect_cb: Any = None
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.connect_error: BaseException | None = None
        self.subscribe_error: BaseException | None = None
        #: Targets whose ``subscribe`` fails, so a *per-target* failure can be
        #: reproduced. A blanket ``subscribe_error`` cannot: the interesting
        #: shape is one object silently never being pushed while the rest of the
        #: connection looks perfectly healthy.
        self.subscribe_fail_keys: set[tuple[str, str | int]] = set()
        #: aioleviton fires disconnect callbacks even for an intentional
        #: teardown; reproduce that, because it is the reconnect-storm trigger.
        self.fire_disconnect_on_close = True

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False
        if self.fire_disconnect_on_close and self.disconnect_cb is not None:
            self.disconnect_cb()

    async def subscribe(self, model_name: str, model_id: str | int) -> None:
        if self.subscribe_error is not None:
            raise self.subscribe_error
        if (model_name, model_id) in self.subscribe_fail_keys:
            raise LevitonWsError(f"leviton ws subscribe: 502 for {model_name}/{model_id}")
        self.subscribed.append((model_name, model_id))

    async def unsubscribe(self, model_name: str, model_id: str | int) -> None:
        self.unsubscribed.append((model_name, model_id))
        if (model_name, model_id) in self.subscribed:
            self.subscribed.remove((model_name, model_id))

    def on_notification(self, callback: Any) -> Any:
        ws_module._reject_coroutine_callback(callback)
        self.notification_cb = callback
        return lambda: None

    def on_disconnect(self, callback: Any) -> Any:
        ws_module._reject_coroutine_callback(callback)
        self.disconnect_cb = callback
        return lambda: None

    # ------------------------------------------------------------- test verbs
    def push(self, notification: dict[str, Any]) -> None:
        """Deliver one notification the way the read loop does: inline, sync."""
        assert self.notification_cb is not None, "nothing subscribed to notifications"
        self.notification_cb(notification)

    def drop(self, close_code: int | None = 1006) -> None:
        """The server drops us (or the 60-minute kill lands)."""
        self.connected = False
        self.close_code = close_code
        assert self.disconnect_cb is not None
        self.disconnect_cb()


def hub_notification(hub_id: str = HUB_A, **fields: Any) -> dict[str, Any]:
    return {"modelName": WS_MODEL_HUB, "modelId": hub_id, "data": dict(fields)}


def breaker_notification(breaker_id: str = BREAKER_ID, **fields: Any) -> dict[str, Any]:
    """A *direct* breaker notification — note ``data`` carries no ``id``."""
    return {"modelName": WS_MODEL_BREAKER, "modelId": breaker_id, "data": dict(fields)}


def ct_notification(ct_id: int = CT_ID, **fields: Any) -> dict[str, Any]:
    return {"modelName": WS_MODEL_CT, "modelId": ct_id, "data": dict(fields)}


def make_snapshot(
    hub_id: str = HUB_A,
    *,
    with_breaker: bool = True,
    with_ct: bool = True,
    breaker_id: str = BREAKER_ID,
    ct_id: int = CT_ID,
) -> HubSnapshot:
    return HubSnapshot(
        hub=HubReading(
            device_id=hub_id,
            connected=True,
            volts_a=121.0,
            volts_b=120.6,
            hz_a=60.0,
            hz_b=60.0,
            name="Panel A",
            version="2.1.2",
        ),
        breakers=(
            (
                BreakerReading(
                    position=11,
                    poles=2,
                    model="LB230-1W",
                    api_id=breaker_id,
                    name="Water heater",
                    connected=True,
                    power=1231.0,
                    power_2=1231.0,
                    rms_current=10.2,
                    rms_current_2=10.2,
                    rms_voltage=120.5,
                    rms_voltage_2=120.4,
                ),
            )
            if with_breaker
            else ()
        ),
        cts=(
            (
                CtReading(
                    channel=1,
                    usage_type="GRID_POWER",
                    api_id=ct_id,
                    name="Main feed",
                    connected=True,
                    active_power=4086.05,
                    active_power_2=505.17,
                    rms_current=34.0,
                    rms_current_2=4.2,
                ),
            )
            if with_ct
            else ()
        ),
    )


def default_targets() -> tuple[SubscriptionTarget, ...]:
    return subscription_targets_from_snapshots([make_snapshot()])


class Harness:
    """An ingester wired to fake everything, plus the transports it built."""

    def __init__(self, ingester: LevitonWebSocketIngester, transports: list[FakeTransport],
                 clock: Clock, calls: dict[str, int]) -> None:
        self.ws = ingester
        self.transports = transports
        self.clock = clock
        self.calls = calls

    @property
    def transport(self) -> FakeTransport:
        return self.transports[-1]

    def flood(self, transport: FakeTransport | None = None) -> None:
        """One frame per subscribed object — the full-state flood, minimally."""
        target = transport or self.transport
        target.push(hub_notification(HUB_A, rmsVoltageA=121.4, frequencyA=60.01))
        target.push(breaker_notification(BREAKER_ID, power=1240.0, power2=1239.0))
        target.push(ct_notification(CT_ID, activePower=4102.5, activePower2=505.2))


def build(
    *,
    seed: Any = "default",
    targets: Any = "default",
    keepalive: Any = None,
    reauthenticate: Any = None,
    **kwargs: Any,
) -> Harness:
    clock = Clock()
    transports: list[FakeTransport] = []
    calls: dict[str, int] = {"seed": 0, "keepalive": 0, "reauth": 0, "sleep": 0}

    async def factory() -> FakeTransport:
        transport = FakeTransport()
        transports.append(transport)
        return transport

    async def default_seed() -> tuple[HubSnapshot, ...]:
        calls["seed"] += 1
        return (make_snapshot(),)

    async def counting_keepalive() -> None:
        calls["keepalive"] += 1
        if keepalive is not None:
            await keepalive()

    async def counting_reauth() -> None:
        calls["reauth"] += 1
        if reauthenticate is not None:
            await reauthenticate()

    async def sleep(_seconds: float) -> None:
        calls["sleep"] += 1

    ingester = LevitonWebSocketIngester(
        transport_factory=factory,
        seed=default_seed if seed == "default" else seed,
        keepalive=counting_keepalive,
        reauthenticate=counting_reauth,
        targets=default_targets() if targets == "default" else targets,
        monotonic=clock,
        now=clock.utc,
        sleep=sleep,
        reconnect_delay=lambda attempts: min(2.0**attempts, 16.0),
        **kwargs,
    )
    return Harness(ingester, transports, clock, calls)


async def started(**kwargs: Any) -> Harness:
    """A harness that has connected, been flooded, and is therefore sampling."""
    harness = build(**kwargs)
    await harness.ws.start()
    harness.flood()
    assert harness.ws.can_sample(), harness.ws.withheld_reason
    return harness


# ------------------------------------------------------------ wire decoding


def test_hub_notification_splits_hub_props_from_nested_children() -> None:
    """``IotWhem`` payloads nest child arrays; every other key is a hub property.

    Leaving ``ResidentialBreaker``/``IotCt`` in the hub delta would file a list
    under a hub field, and dropping the children would lose every breaker and CT
    update that arrives nested rather than direct.
    """
    deltas = parse_notification(
        {
            "modelName": WS_MODEL_HUB,
            "modelId": HUB_A,
            "data": {
                "rmsVoltageA": 121,
                "connected": False,
                "ResidentialBreaker": [{"id": BREAKER_ID, "power": 500}],
                "IotCt": [{"id": CT_ID, "activePower": 999}],
            },
        }
    )
    by_key = {d.key: d for d in deltas}
    assert by_key[(WS_MODEL_HUB, HUB_A)].fields == {"rmsVoltageA": 121, "connected": False}
    assert by_key[(WS_MODEL_BREAKER, BREAKER_ID)].fields == {"power": 500}
    assert by_key[(WS_MODEL_CT, CT_ID)].fields == {"activePower": 999}


def test_direct_breaker_notification_without_an_id_falls_back_to_the_envelope() -> None:
    """Power payloads routinely omit ``data["id"]``; the envelope carries it.

    Getting this wrong drops every per-breaker update silently — and per-breaker
    resolution is the entire reason ~25 smart breakers are being installed.
    """
    (delta,) = parse_notification(breaker_notification(BREAKER_ID, power=0))
    assert delta.key == (WS_MODEL_BREAKER, BREAKER_ID)
    assert delta.fields == {"power": 0}


def test_ct_ids_are_ints_and_breaker_ids_are_strings() -> None:
    """Mixed id types would be two distinct subscriptions for one object."""
    (ct,) = parse_notification(ct_notification("3", activePower=1.0))
    assert ct.model_id == 3
    (breaker,) = parse_notification(
        {"modelName": WS_MODEL_BREAKER, "modelId": 77, "data": {"power": 1}}
    )
    assert breaker.model_id == "77"
    assert SubscriptionTarget(WS_MODEL_CT, "3").key == SubscriptionTarget(WS_MODEL_CT, 3).key


@pytest.mark.parametrize(
    "frame",
    [
        {"modelName": "IotSwitch", "modelId": "x", "data": {"power": 1}},
        {"modelName": "Residence", "modelId": 1, "data": {}},
        {"modelId": "x", "data": {"power": 1}},
        {"modelName": WS_MODEL_CT, "modelId": None, "data": {"activePower": 1}},
        "not a mapping",
        None,
    ],
)
def test_unusable_frames_are_ignored_rather_than_raising(frame: Any) -> None:
    """This runs inside the socket read loop; an exception costs frames."""
    assert parse_notification(frame) == ()


# ------------------------------------------------------------- merge semantics


def test_a_partial_delta_updates_only_the_fields_it_carries() -> None:
    """The norm on this protocol: ``{"power": 0}`` with no accompanying current.

    Anything that replaced the object wholesale would blank the other pole every
    time one leg reported alone.
    """
    store = StateStore()
    store.apply(ObjectDelta(WS_MODEL_BREAKER, BREAKER_ID, {"power": 500, "power2": 480}))
    store.apply(ObjectDelta(WS_MODEL_BREAKER, BREAKER_ID, {"power": 0}))

    state = store.get(WS_MODEL_BREAKER, BREAKER_ID)
    assert state is not None
    assert state.value("power") == 0
    assert state.value("power2") == 480, "an untouched field must not move"


def test_a_field_never_received_is_absent_not_zero() -> None:
    """Absence is the whole gap contract: no key, no row, no invented zero."""
    store = StateStore()
    store.apply(ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": 4086.05}))

    state = store.get(WS_MODEL_CT, CT_ID)
    assert state is not None
    assert state.has("activePower")
    assert not state.has("activePower2")
    assert "activePower2" not in state.values()
    assert state.value("activePower2") is None
    assert state.value("activePower2", "sentinel") == "sentinel"


def test_an_explicit_null_clears_the_field_so_the_sampler_gaps() -> None:
    """``{"activePower": null}`` means the API said null — CLAUDE.md rule 1.

    Both reference integrations keep the cached value here, which is a
    hold-last-value at field granularity. Our REST path emits no row for a null
    field and this keeps the two identical. Counted per field so the decision
    can be revisited on measured evidence rather than taste.
    """
    store = StateStore(null_policy=NULL_POLICY_CLEAR)
    store.apply(ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": 100.0, "rmsCurrent": 1.0}))
    store.apply(ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": None}))

    state = store.get(WS_MODEL_CT, CT_ID)
    assert state is not None
    assert not state.has("activePower")
    assert state.value("rmsCurrent") == 1.0
    assert store.nulls_seen == 1
    assert store.null_counts == {"activePower": 1}
    assert store.fields_cleared == 1


def test_the_null_policy_can_be_flipped_to_the_reference_behaviour() -> None:
    """Kept only so a measured null rate can change the decision cheaply."""
    store = StateStore(null_policy=NULL_POLICY_IGNORE)
    store.apply(ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": 100.0}))
    store.apply(ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": None}))

    state = store.get(WS_MODEL_CT, CT_ID)
    assert state is not None and state.value("activePower") == 100.0
    assert store.null_counts == {"activePower": 1}


def test_duplicate_and_reordered_deltas_do_not_corrupt_state() -> None:
    """Last writer wins; the protocol has no sequence number and no timestamp.

    Duplicates re-write the same value (harmless — subscribing to both IotWhem
    and each IotCt guarantees them) and a reordered pair cannot invent a field
    or move one it did not carry.
    """
    store = StateStore()
    first = ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": 100.0})
    second = ObjectDelta(WS_MODEL_CT, CT_ID, {"rmsCurrent": 2.0})

    store.apply(first)
    store.apply(first)  # duplicate delivery
    store.apply(second)
    store.apply(first)  # arrives late / out of order

    state = store.get(WS_MODEL_CT, CT_ID)
    assert state is not None
    assert state.values() == {"activePower": 100.0, "rmsCurrent": 2.0}
    assert len(store) == 1
    assert state.fields["activePower"].updates == 3


def test_a_server_timestamp_is_preferred_and_its_source_recorded() -> None:
    """Provenance is diagnostic: §6.5 still stamps one ``ts_utc`` per cycle."""
    store = StateStore()
    server_ts = "2026-08-17T11:59:00Z"
    delta = ObjectDelta(
        WS_MODEL_BREAKER,
        BREAKER_ID,
        {"power": 12.0, "lastUpdated": server_ts},
        server_ts_utc=ws_module._parse_server_timestamp(server_ts),
    )
    store.apply(delta, received_utc=BASE_UTC, received_monotonic=1.0)

    entry = store.get(WS_MODEL_BREAKER, BREAKER_ID).fields["power"]
    assert entry.ts_source == TS_SOURCE_SERVER
    assert entry.updated_utc == datetime(2026, 8, 17, 11, 59, tzinfo=timezone.utc)


def test_an_unparseable_server_timestamp_falls_back_to_our_receipt_instant() -> None:
    """``lastUpdated``'s type and semantics are unverified; never guess."""
    assert ws_module._parse_server_timestamp("not-a-time") is None
    assert ws_module._parse_server_timestamp(True) is None
    (delta,) = parse_notification(breaker_notification(BREAKER_ID, power=1, lastUpdated="?"))
    store = StateStore()
    store.apply(delta, received_utc=BASE_UTC, received_monotonic=1.0)
    entry = store.get(WS_MODEL_BREAKER, BREAKER_ID).fields["power"]
    assert entry.ts_source == TS_SOURCE_RECEIPT
    assert entry.updated_utc == BASE_UTC


def test_field_diagnostics_expose_per_field_ages_without_gating_on_them() -> None:
    """The measurement this whole change must be judged on — and only that.

    Nothing in the gate reads these. They exist so the real update distribution
    can be seen before anyone publishes a freshness claim.
    """
    store = StateStore()
    store.apply(
        ObjectDelta(WS_MODEL_CT, CT_ID, {"activePower": 1.0}),
        received_utc=BASE_UTC,
        received_monotonic=10.0,
    )
    report = store.diagnostics(70.0)
    entry = report[f"{WS_MODEL_CT}/{CT_ID}"]
    assert entry["fields"]["activePower"]["age_s"] == 60.0
    assert entry["fields"]["activePower"]["ts_source"] == TS_SOURCE_RECEIPT


# ------------------------------------------------------------- the emission gate


async def test_can_sample_is_false_before_the_initial_sync_and_true_after() -> None:
    """Before the flood we do not know current values; emitting one would lie."""
    harness = build()
    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_NOT_STARTED

    await harness.ws.start()
    assert harness.ws.connected is True
    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_AWAITING_SYNC

    harness.flood()
    assert harness.ws.can_sample() is True
    assert harness.ws.withheld_reason is None
    assert harness.ws.status_snapshot()["sync_mode"] == "flood"


async def test_can_sample_is_false_the_instant_the_socket_drops() -> None:
    """No tick required: a dropped socket must shut the gate immediately."""
    harness = await started()
    harness.transport.drop(close_code=1006)

    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_DISCONNECTED
    assert harness.ws.status_snapshot()["last_close_code"] == 1006


async def test_sampling_is_withheld_while_the_socket_is_open_but_silent() -> None:
    """The silent-stall guard — the most dangerous failure mode for an archive.

    aiohttp's heartbeat only proves the TCP path is alive. A server that pongs
    while pushing nothing is invisible to aioleviton, so without this the
    sampler would keep emitting rows from a frozen store while ``connected`` is
    True. Note the gate goes shut on its own, without waiting for a watchdog
    tick, because the sampler asks on its own 30s cadence.
    """
    harness = await started(stall_timeout_s=90.0)

    harness.clock.advance(89.0)
    assert harness.ws.can_sample() is True

    harness.clock.advance(2.0)
    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_STALLED

    await harness.ws.tick()
    assert len(harness.transports) == 2, "a stalled feed must be cycled"
    assert harness.ws.status_snapshot()["stalls"] == 1


async def test_a_steady_load_that_never_changes_is_still_sampled() -> None:
    """Freshness gates on connection state, never on field age.

    The live capture shows a resistive water-heater element holding 2462 W,
    which is physically correct. Gapping on "this field has not changed
    recently" would delete real data — so as long as *some* traffic proves the
    feed is alive, a motionless field stays samplable.
    """
    harness = await started(stall_timeout_s=90.0)
    state = harness.ws.store.get(WS_MODEL_BREAKER, BREAKER_ID)
    assert state is not None
    power_before = state.value("power")

    for _ in range(10):  # ten minutes of hub chatter, no breaker movement
        harness.clock.advance(60.0)
        harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))
        assert harness.ws.can_sample() is True

    assert harness.ws.store.get(WS_MODEL_BREAKER, BREAKER_ID).value("power") == power_before
    ages = harness.ws.field_diagnostics()[f"{WS_MODEL_BREAKER}/{BREAKER_ID}"]
    assert ages["fields"]["power"]["age_s"] >= 600.0, "the staleness is visible..."
    assert harness.ws.can_sample() is True, "...but it is a diagnostic, not a gate"


async def test_sample_object_returns_none_while_withheld_and_values_when_fresh() -> None:
    """``None`` (do not know) and ``{}`` (know nothing) are different answers."""
    harness = build()
    await harness.ws.start()
    assert harness.ws.sample_object(WS_MODEL_CT, CT_ID) is None

    harness.flood()
    assert harness.ws.sample_object(WS_MODEL_CT, CT_ID)["activePower"] == 4102.5
    assert harness.ws.sample_object(WS_MODEL_CT, 999) == {}
    assert harness.ws.peek_object(WS_MODEL_CT, CT_ID)["activePower"] == 4102.5


async def test_overlay_turns_an_unseen_field_into_a_gap_not_a_zero() -> None:
    """The store's absence must survive all the way into §6.5's row mapping.

    ``sources/leviton.py`` maps ``None`` to *no row*. So an overlaid reading
    whose second pole was never pushed produces no ``watts`` row at all, rather
    than a fabricated half-load or a zero.
    """
    # No REST seed, so "not in the store" means "never received from anyone".
    harness = build(seed=None)
    await harness.ws.start()
    harness.flood()
    harness.transport.push(breaker_notification(BREAKER_ID, power=1500.0))
    snapshot = harness.ws.overlay_snapshot(make_snapshot())
    assert snapshot is not None

    breaker = snapshot.breakers[0]
    assert breaker.power == 1500.0
    assert breaker.rms_voltage is None, "never pushed, and the REST value is the cache"
    assert breaker.position == 11 and breaker.poles == 2, "structure survives"
    assert breaker.channel_id == "breaker_p11"
    assert breaker.metrics()["volts"] is None, "a gap, per §6.5"

    hub = snapshot.hub
    assert hub.volts_a == 121.4
    assert hub.hz_b is None


async def test_overlay_is_withheld_entirely_when_the_gate_is_shut() -> None:
    harness = await started()
    harness.transport.drop()
    assert harness.ws.overlay_snapshot(make_snapshot()) is None
    assert harness.ws.overlay_snapshots([make_snapshot()]) is None


def test_overlay_keeps_connected_from_discovery_when_the_feed_is_silent_on_it() -> None:
    """``connected`` is control state (it drives the keepalive), not archived data."""
    reading = BreakerReading(position=11, poles=2, model="LB230", api_id=BREAKER_ID,
                             connected=True, power=1.0)
    overlaid = overlay_breaker(reading, None)
    assert overlaid.connected is True
    assert overlaid.power is None


# ------------------------------------------------------------------ lifecycle


async def test_the_proactive_reconnect_resubscribes_everything_and_re_syncs() -> None:
    """The server hard-kills push at exactly 60 minutes, unconditionally.

    We cycle at 55 so the gap is ours and small. Crucially the gate shuts again
    until a fresh full-state sync completes: the server keeps no subscriptions
    across a reconnect, and the flood's completeness is unverified, so deltas
    alone cannot re-establish state.
    """
    harness = await started(proactive_reconnect_s=3300.0)
    first = harness.transport
    assert len(first.subscribed) == 3

    harness.clock.advance(3301.0)
    await harness.ws.tick()

    assert len(harness.transports) == 2
    second = harness.transport
    assert sorted(map(str, second.subscribed)) == sorted(map(str, first.subscribed))
    assert harness.ws.reconnects == 1
    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_AWAITING_SYNC

    harness.flood()
    assert harness.ws.can_sample() is True


async def test_an_intentional_teardown_does_not_trigger_a_reconnect_storm() -> None:
    """aioleviton fires ``on_disconnect`` for *our* disconnect too.

    Reconnecting from inside that callback is a recursion/storm setup, which is
    why this module owns a single reconnect state machine and never calls
    ``aioleviton.reconnect()``.
    """
    harness = await started(proactive_reconnect_s=10.0)
    harness.clock.advance(11.0)
    await harness.ws.tick()

    assert len(harness.transports) == 2, "exactly one new connection"
    assert harness.ws.reconnects == 1


async def test_a_new_breaker_discovered_at_runtime_starts_receiving() -> None:
    """~25 smart breakers are arriving; none of them may require a restart.

    Firmware ≥2.2.0 also mutates breaker ids, so discovery hands the full target
    set and the diff is subscribed/unsubscribed — while ``channel_id`` stays
    ``breaker_p{position}`` and never moves.
    """
    harness = await started()
    new_id = "4C45565299AA_A65E"

    await harness.ws.add_target(SubscriptionTarget(WS_MODEL_BREAKER, new_id))
    assert (WS_MODEL_BREAKER, new_id) in harness.transport.subscribed

    harness.transport.push(breaker_notification(new_id, power=42.0))
    assert harness.ws.sample_object(WS_MODEL_BREAKER, new_id) == {"power": 42.0}
    assert harness.ws.status_snapshot()["subscriptions"] == 4


async def test_set_targets_unsubscribes_objects_discovery_no_longer_reports() -> None:
    harness = await started()
    keep = SubscriptionTarget(WS_MODEL_HUB, HUB_A)
    await harness.ws.set_targets([keep, SubscriptionTarget(WS_MODEL_HUB, HUB_B)])

    assert (WS_MODEL_CT, CT_ID) in harness.transport.unsubscribed
    assert (WS_MODEL_HUB, HUB_B) in harness.transport.subscribed
    assert {t.key for t in harness.ws.subscriptions} == {
        (WS_MODEL_HUB, HUB_A),
        (WS_MODEL_HUB, HUB_B),
    }


def test_subscription_targets_cover_hub_cts_and_breakers_but_not_placeholders() -> None:
    """CT subscriptions are belt-and-braces: the GRID_POWER feeds *are* CTs.

    One reference says the hub subscription alone carries CT updates on all
    firmware; the other hedges and subscribes each CT. The cost of the extra
    frame is nothing and the cost of being wrong is a silent outage on the
    whole-panel feeds.
    """
    snapshot = make_snapshot()
    snapshot = replace(
        snapshot,
        breakers=(
            *snapshot.breakers,
            BreakerReading(position=3, poles=1, model="NONE", api_id="placeholder"),
        ),
        cts=(
            *snapshot.cts,
            CtReading(channel=9, usage_type="NOT_USED", api_id=99),
        ),
    )
    keys = {t.key for t in subscription_targets_from_snapshots([snapshot])}
    assert keys == {
        (WS_MODEL_HUB, HUB_A),
        (WS_MODEL_BREAKER, BREAKER_ID),
        (WS_MODEL_CT, CT_ID),
    }


# ---------------------------------------------------------------- seeding


async def test_the_connect_sequence_puts_the_keepalive_before_the_socket() -> None:
    """``bandwidth: 1`` is what triggers the flood that seeds the store.

    (It is the existing §6.4 PUT, value 1, fired by ``sources/leviton.py``. This
    module never sets bandwidth itself and never, ever sends 0 — firmware 2.1.0
    drops a hub off the cloud for 10-20s on receipt.)
    """
    harness = build()
    await harness.ws.start()
    assert harness.calls["keepalive"] == 1
    assert harness.calls["seed"] == 1


async def test_a_rest_seed_never_overwrites_a_value_the_flood_already_delivered() -> None:
    """The seed *is* the stale cache this module exists to escape.

    Seeded fields are tagged ``rest_seed`` so the honesty cost is measurable,
    and a field the live feed has already refreshed on this connection is left
    alone even though the seed lands afterwards.
    """
    transports: list[FakeTransport] = []
    clock = Clock()

    async def factory() -> FakeTransport:
        transport = FakeTransport()
        transports.append(transport)
        return transport

    async def slow_seed() -> tuple[HubSnapshot, ...]:
        # The flood beats the REST round trip — the common case at reconnect.
        transports[-1].push(ct_notification(CT_ID, activePower=9999.0))
        return (make_snapshot(),)

    ingester = LevitonWebSocketIngester(
        transport_factory=factory,
        seed=slow_seed,
        targets=default_targets(),
        monotonic=clock,
        now=clock.utc,
    )
    await ingester.start()

    state = ingester.store.get(WS_MODEL_CT, CT_ID)
    assert state is not None
    assert state.value("activePower") == 9999.0, "live value must win"
    assert state.fields["activePower"].ts_source == "receipt"
    assert state.fields["rmsCurrent"].ts_source == TS_SOURCE_REST_SEED


async def test_sync_completes_on_the_timeout_when_the_flood_misses_an_object() -> None:
    """Recorded as ``sync_mode`` so partial floods are visible, not assumed."""
    harness = build(sync_flood_timeout_s=20.0)
    await harness.ws.start()
    harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.0))

    assert harness.ws.can_sample() is False
    harness.clock.advance(21.0)
    assert harness.ws.can_sample() is True
    assert harness.ws.status_snapshot()["sync_mode"] == "timeout"


async def test_with_neither_a_seed_nor_a_delta_the_gate_stays_shut() -> None:
    """"We connected and nothing ever arrived" is not a state we may sample."""
    harness = build(seed=None, sync_flood_timeout_s=20.0)
    await harness.ws.start()
    harness.clock.advance(60.0)

    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason in {REASON_AWAITING_SYNC, REASON_STALLED}


async def test_a_failed_seed_is_not_fatal_and_is_counted() -> None:
    async def broken_seed() -> tuple[HubSnapshot, ...]:
        raise RuntimeError("502 Bad Gateway")

    harness = build(seed=broken_seed)
    await harness.ws.start()
    harness.flood()

    assert harness.ws.can_sample() is True, "the flood established state on its own"
    assert harness.ws.status_snapshot()["seed_errors"] == 1


# ------------------------------------------------------------------- failures


async def test_a_mid_stream_auth_failure_is_typed_and_recoverable() -> None:
    """aioleviton ignores every non-notification frame, so auth expiry is silent.

    The §6.4 keepalive sees the 401/403/406 first and reports it here; the gate
    shuts at once (a socket that looks connected while the cloud has stopped
    honouring us is the quiet failure this guards), then the next tick
    re-authenticates and rebuilds the connection.
    """
    harness = await started()
    harness.ws.note_auth_failure(SourceAuthError("leviton keepalive: 401"))

    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_AUTH_FAILED

    await harness.ws.tick()
    assert harness.calls["reauth"] == 1
    assert len(harness.transports) == 2

    harness.flood()
    assert harness.ws.can_sample() is True
    assert harness.ws.status_snapshot()["auth_failures"] == 1


async def test_an_auth_error_from_the_handshake_is_absorbed_and_backed_off() -> None:
    """A caller is never crashed; the gate simply stays shut, which means gaps."""
    harness = build()

    async def failing_factory() -> FakeTransport:
        transport = FakeTransport()
        transport.connect_error = LevitonWsAuthError("leviton ws connect: 401")
        harness.transports.append(transport)
        return transport

    harness.ws._transport_factory = failing_factory  # noqa: SLF001 - injection point
    await harness.ws.start()

    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_AUTH_FAILED
    assert harness.ws.connected is False


async def test_a_transient_connect_failure_backs_off_and_then_recovers() -> None:
    """Leviton's gateway 502s routinely; a reconnect ladder must absorb that."""
    harness = build()
    attempts = {"n": 0}

    async def flaky_factory() -> FakeTransport:
        attempts["n"] += 1
        transport = FakeTransport()
        if attempts["n"] == 1:
            transport.connect_error = LevitonWsError("leviton ws connect: 502")
        harness.transports.append(transport)
        return transport

    harness.ws._transport_factory = flaky_factory  # noqa: SLF001 - injection point
    await harness.ws.start()
    assert harness.ws.connected is False
    assert harness.ws.status_snapshot()["connect_failures"] == 1

    await harness.ws.tick()  # still inside the backoff window
    assert harness.ws.connected is False

    harness.clock.advance(60.0)
    await harness.ws.tick()
    assert harness.ws.connected is True


async def test_tick_never_raises_even_when_the_transport_factory_explodes() -> None:
    """CLAUDE.md: the poll loop and the process never die from a source error."""
    harness = build()

    async def exploding_factory() -> FakeTransport:
        raise ValueError("kaboom")

    harness.ws._transport_factory = exploding_factory  # noqa: SLF001
    await harness.ws.start()
    harness.clock.advance(3600.0)
    await harness.ws.tick()

    assert harness.ws.can_sample() is False


async def test_a_throwing_callback_is_counted_and_does_not_break_the_read_loop() -> None:
    """A persistently throwing callback stops the store silently; count it."""
    harness = await started()
    before = harness.ws.messages_received
    harness.ws._store = None  # type: ignore[assignment]  # forces an internal error
    harness.transport.push(ct_notification(CT_ID, activePower=1.0))

    assert harness.ws.messages_received == before + 1
    harness.ws._store = StateStore()
    assert harness.ws._callback_errors == 1  # noqa: SLF001


def test_errors_reuse_the_source_error_vocabulary() -> None:
    """The poll loop already knows how to count these two and carry on."""
    assert issubclass(LevitonWsError, SourceTransientError)
    assert issubclass(LevitonWsAuthError, SourceAuthError)


# ------------------------------------------------------------------ counters


async def test_status_counters_are_accurate() -> None:
    harness = build()
    await harness.ws.start()
    harness.flood()
    harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.9))

    snapshot = harness.ws.status_snapshot()
    assert snapshot["connected"] is True
    assert snapshot["subscriptions"] == 3
    assert snapshot["messages_received"] == 4
    assert snapshot["deltas_applied"] == 7  # 3 seeded objects + 4 pushed frames
    assert snapshot["reconnects"] == 0
    assert snapshot["last_message_utc"] == harness.clock.utc().isoformat()
    assert snapshot["sync_completed_utc"] is not None
    assert snapshot["withheld_reason"] is None
    assert snapshot["objects_tracked"] == 3
    assert snapshot["null_deltas"] == 0
    assert snapshot["can_sample"] is True

    harness.clock.advance(4000.0)
    await harness.ws.tick()
    assert harness.ws.status_snapshot()["reconnects"] == 1
    assert harness.ws.status_snapshot()["withheld_reason"] == REASON_AWAITING_SYNC


async def test_status_snapshot_carries_every_key_the_task_requires() -> None:
    harness = await started()
    snapshot = harness.ws.status_snapshot(include_fields=True)
    for key in (
        "connected",
        "subscriptions",
        "messages_received",
        "deltas_applied",
        "reconnects",
        "last_message_utc",
        "sync_completed_utc",
        "withheld_reason",
    ):
        assert key in snapshot, key
    assert f"{WS_MODEL_CT}/{CT_ID}" in snapshot["objects"]


async def test_the_status_store_section_is_written_by_the_watchdog() -> None:
    class RecordingStatus:
        def __init__(self) -> None:
            self.sections: dict[str, dict[str, Any]] = {}

        def set(self, section: str, **fields: Any) -> None:
            self.sections[section] = fields

    status = RecordingStatus()
    harness = build(status_store=status)
    await harness.ws.start()
    harness.flood()
    await harness.ws.tick()

    section = status.sections[ws_module.STATUS_SECTION_WS]
    assert section["connected"] is True
    # The per-field last-update instants must reach status.json, not just a
    # Python method: they are the measurement that decides whether the socket
    # actually unfroze anything, and they are reported, never consulted.
    fields = section["objects"][f"{WS_MODEL_CT}/{CT_ID}"]["fields"]
    assert "age_s" in fields["activePower"]
    assert "updated_utc" in fields["activePower"]
    assert "ts_source" in fields["activePower"]


async def test_run_stops_on_the_stop_event_and_closes_cleanly() -> None:
    harness = build()
    stop = asyncio.Event()

    async def sleep(_seconds: float) -> None:
        stop.set()

    harness.ws._sleep = sleep  # noqa: SLF001 - injection point
    await harness.ws.run(stop=stop)

    assert harness.ws.connected is False
    assert harness.ws.withheld_reason == REASON_NOT_STARTED
    assert harness.transports[0].disconnect_calls == 1


# ------------------------------------------------------------------ the seam


async def test_an_async_notification_callback_is_refused_at_registration() -> None:
    """aioleviton calls callbacks without ``await``.

    An ``async def`` callback returns a coroutine that is never awaited: the
    store never updates, nothing raises, and the collector reports
    ``connected: True`` while archiving nothing. That is a total silent data
    outage, so it is refused loudly at the seam instead.
    """

    class StubWs:
        def on_notification(self, cb: Any) -> Any:
            return lambda: None

        def on_disconnect(self, cb: Any) -> Any:
            return lambda: None

    transport = AioLevitonWsTransport(StubWs())

    async def async_cb(_notification: Any) -> None:  # pragma: no cover - never called
        return None

    with pytest.raises(TypeError, match="plain functions"):
        transport.on_notification(async_cb)
    transport.on_notification(lambda _n: None)  # a plain def is fine


async def test_the_adapter_translates_aioleviton_errors_into_our_vocabulary() -> None:
    """Nothing above this line ever sees an ``aioleviton`` type (PLAN.md §2.8)."""
    from aioleviton import LevitonAuthError, LevitonConnectionError

    class StubWs:
        def __init__(self, error: BaseException) -> None:
            self._error = error
            self._token = "a-very-secret-leviton-token"

        async def connect(self) -> None:
            raise self._error

        async def subscribe(self, *_args: Any) -> None:
            raise self._error

    with pytest.raises(LevitonWsError):
        await AioLevitonWsTransport(StubWs(LevitonConnectionError("502"))).connect()
    with pytest.raises(LevitonWsAuthError):
        await AioLevitonWsTransport(StubWs(LevitonAuthError("401"))).connect()
    with pytest.raises(LevitonWsError):
        await AioLevitonWsTransport(StubWs(OSError("reset"))).subscribe("IotWhem", "x")


def test_the_token_is_registered_with_the_log_scrubber() -> None:
    """CLAUDE.md rule 8: no credential ever reaches a log line."""

    class StubWs:
        _token = "leviton-token-that-must-never-be-logged"

    AioLevitonWsTransport(StubWs())
    assert "leviton-token-that-must-never-be-logged" not in ec_logging.scrub_text(
        "authorization: leviton-token-that-must-never-be-logged"
    )


def test_this_module_maps_no_rows_and_never_touches_bandwidth() -> None:
    """§6.5 row mapping belongs to ``sources/leviton.py``, and only there.

    Also pinned: nothing here sets bandwidth (the §6.4 keepalive is injected and
    owned by the source), nothing sends ``pollBreakers`` — a Poll request only
    refreshes lifetime counters, which stopped working at firmware 2.1.0 and
    which §6.3 already excludes — and nothing writes to the spool.
    """
    code = _executable_source(Path(ws_module.__file__).read_text(encoding="utf-8"))

    assert "pollBreakers" not in code
    assert "set_whem_bandwidth" not in code
    assert not re.search(r"bandwidth", code, flags=re.IGNORECASE)
    assert "breaker_p" not in code and "panel_leg" not in code
    assert "Observation" not in code
    assert "spool" not in code.lower()


# ==================================================================================
# The emission gate, per FIELD and per HUB
#
# Everything below is one mistake wearing six different costumes: the gate is
# allowed to withhold rows on connection state, and is never allowed to emit a
# value we do not currently know. A value carried over from a previous
# connection, kept alive by a REST cache after the socket died, or lifted from a
# hub whose feed is dead while its sibling keeps an aggregate watchdog happy, is
# a hold-last-value stamped with a fresh ts_utc — CLAUDE.md rule 1.
# ==================================================================================


def two_hub_targets() -> tuple[SubscriptionTarget, ...]:
    """Both panels' objects, each carrying the hub whose feed pushes it."""
    return subscription_targets_from_snapshots(
        [
            make_snapshot(HUB_A),
            make_snapshot(HUB_B, breaker_id=BREAKER_ID_B, ct_id=CT_ID_B),
        ]
    )


async def two_hub_seed() -> tuple[HubSnapshot, ...]:
    return (
        make_snapshot(HUB_A),
        make_snapshot(HUB_B, breaker_id=BREAKER_ID_B, ct_id=CT_ID_B),
    )


def flood_hub(transport: FakeTransport, hub_id: str, breaker_id: str, ct_id: int) -> None:
    transport.push(hub_notification(hub_id, rmsVoltageA=121.4, frequencyA=60.01))
    transport.push(breaker_notification(breaker_id, power=1240.0, power2=1239.0))
    transport.push(ct_notification(ct_id, activePower=4102.5, activePower2=505.2))


# ------------------------------------------- (1) a field must belong to THIS connection


async def test_a_field_the_reconnect_never_re_established_is_not_emitted() -> None:
    """The store survives a reconnect; the *values* must not, unless re-proved.

    ``StateStore`` is deliberately not cleared on reconnect (``status.json``
    wants to show the last known state). The gate then opens on the timeout path
    for objects the flood never touched — so if the REST seed also failed, those
    objects are sampled from values the **previous** connection delivered, and
    emitted with a current ``ts_utc`` labelled ``value_source="ws"``. That is a
    hold-last-value across a disconnect: exactly the thing the connection gate
    exists to prevent, leaking through at field granularity.

    So "established on the current connection" is a per-field property, and the
    gate's connection-state criterion reaches individual fields. Note what is
    *not* being asserted: nothing here is about the field being *old*. Field age
    never gates emission — see the steady-load test above, which must keep
    passing.
    """
    seed_calls = {"n": 0}

    async def seed_that_dies_after_the_first_connection() -> tuple[HubSnapshot, ...]:
        seed_calls["n"] += 1
        if seed_calls["n"] > 1:
            raise RuntimeError("502 Bad Gateway")
        return (make_snapshot(),)

    harness = build(
        seed=seed_that_dies_after_the_first_connection,
        proactive_reconnect_s=100.0,
        sync_flood_timeout_s=20.0,
        stall_timeout_s=200.0,
    )
    await harness.ws.start()
    harness.flood()
    harness.transport.push(hub_notification(HUB_A, rmsVoltageA=240.5))
    assert harness.ws.can_sample() is True
    assert harness.ws.overlay_snapshot(make_snapshot()).hub.volts_a == 240.5

    # The 55-minute proactive cycle lands. New socket, no usable seed, and a
    # flood that only ever reaches the CT.
    harness.clock.advance(101.0)
    await harness.ws.tick()
    assert len(harness.transports) == 2
    assert harness.ws.can_sample() is False

    harness.transport.push(ct_notification(CT_ID, activePower=4102.5))
    harness.clock.advance(21.0)

    assert harness.ws.can_sample() is True, "the timeout path still opens the gate"
    assert harness.ws.status_snapshot()["sync_mode"] == SYNC_MODE_TIMEOUT

    overlaid = harness.ws.overlay_snapshot(make_snapshot())
    assert overlaid is not None
    assert overlaid.hub.volts_a is None, (
        "240.5 came from the PREVIOUS connection; re-emitting it now with a "
        "current ts_utc is the hold-last-value CLAUDE.md rule 1 forbids"
    )
    assert overlaid.breakers[0].power is None, "same story, per breaker"
    assert overlaid.breakers[0].metrics()["watts"] is None, "a gap, per §6.5"
    assert overlaid.cts[0].active_power == 4102.5, "the CT re-proved itself"
    assert harness.ws.status_snapshot()["fields_evicted"] >= 1


async def test_the_eviction_is_connection_membership_and_never_a_max_age() -> None:
    """A value pushed 55 minutes into a connection is still current.

    The distinction the whole module rests on: ``evict_before`` asks "did this
    connection establish this?", never "is this old?". A resistive element
    genuinely holds its wattage for hours; an age threshold here would delete
    real data, which is why none is invented.
    """
    harness = await started(proactive_reconnect_s=100000.0, stall_timeout_s=90.0)
    harness.transport.push(breaker_notification(BREAKER_ID, power=2462.0))

    for _ in range(55):  # 55 minutes of hub-only chatter on one connection
        harness.clock.advance(60.0)
        harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))

    assert harness.ws.can_sample() is True
    overlaid = harness.ws.overlay_snapshot(make_snapshot())
    assert overlaid is not None
    assert overlaid.breakers[0].power == 2462.0
    assert harness.ws.store.fields_evicted == 0


# --------------------------------------- (2) an explicit REST null must be able to clear


def test_a_seed_keeps_an_explicit_null_for_a_measurement_and_drops_it_for_state() -> None:
    """A null measurement must be *expressible* from the seed, or it cannot clear.

    Filtering every ``None`` out of the seed means a REST snapshot can overwrite
    a stale field but never clear one — so a channel whose current REST value is
    null keeps whatever the previous connection said, while the REST ingestion
    path would emit no row for it at all. Two paths, one input, different data.

    ``connected``/``currentState`` are the deliberate exception: control state,
    not archived data. ``connected`` drives the keepalive's allow-list, and
    clearing it would silently stop the keepalive for that hub.
    """
    snapshot = make_snapshot()
    snapshot = replace(
        snapshot,
        hub=replace(snapshot.hub, volts_a=None, hz_b=None, connected=None),
        breakers=(replace(snapshot.breakers[0], power=None, connected=None),),
        cts=(replace(snapshot.cts[0], active_power=None, connected=None),),
    )

    deltas = {d.model_name: d.fields for d in ws_module._seed_deltas([snapshot])}  # noqa: SLF001

    assert "rmsVoltageA" in deltas[WS_MODEL_HUB]
    assert deltas[WS_MODEL_HUB]["rmsVoltageA"] is None
    assert deltas[WS_MODEL_HUB]["frequencyB"] is None
    assert deltas[WS_MODEL_HUB]["rmsVoltageB"] == 120.6, "present values still land"
    assert "power" in deltas[WS_MODEL_BREAKER] and deltas[WS_MODEL_BREAKER]["power"] is None
    assert "activePower" in deltas[WS_MODEL_CT] and deltas[WS_MODEL_CT]["activePower"] is None

    for fields in deltas.values():
        assert "connected" not in fields, "control state keeps the discovery value"


def test_a_null_rest_value_clears_a_value_carried_over_from_a_previous_connection() -> None:
    """The behavioural half: NULL_POLICY_CLEAR must reach the seed path too.

    #153 decided an explicit null means "the API said unknown", so it clears the
    field and the sampler gaps — identical to what §6.5 does with a null REST
    field. A seed that silently drops its nulls exempts itself from that policy
    and re-publishes the old connection's number.
    """
    store = StateStore(null_policy=NULL_POLICY_CLEAR)
    store.apply(
        ObjectDelta(WS_MODEL_HUB, HUB_A, {"rmsVoltageA": 240.5}),
        received_monotonic=100.0,
    )

    null_side = make_snapshot()
    null_side = replace(null_side, hub=replace(null_side.hub, volts_a=None))
    for delta in ws_module._seed_deltas([null_side]):  # noqa: SLF001
        store.apply(delta, received_monotonic=200.0, ts_source=TS_SOURCE_REST_SEED)

    state = store.get(WS_MODEL_HUB, HUB_A)
    assert state is not None
    assert state.has("rmsVoltageA") is False, "the API said null: we do not know it"
    assert overlay_hub(make_snapshot().hub, state).volts_a is None
    assert state.value("rmsVoltageB") == 120.6, "the fields it did report still land"


# ------------------------------------------------ (3) liveness is per hub, not per socket


async def test_one_hub_going_silent_does_not_ride_on_the_other_hubs_traffic() -> None:
    """Two hubs, one socket: an aggregate silence mark is the wrong instrument.

    Panel A's line voltage jitters every few seconds, which keeps a
    "any frame from anyone" watchdog permanently satisfied — while Panel B's
    entire push feed is dead and its channels are being lifted out of a frozen
    store and stamped with a current ``ts_utc``. This house has exactly two
    hubs, so it is not hypothetical.
    """
    harness = build(
        seed=two_hub_seed, targets=two_hub_targets(), stall_timeout_s=90.0
    )
    await harness.ws.start()
    flood_hub(harness.transport, HUB_A, BREAKER_ID, CT_ID)
    flood_hub(harness.transport, HUB_B, BREAKER_ID_B, CT_ID_B)
    assert harness.ws.can_sample() is True
    assert harness.ws.status_snapshot()["sync_mode"] == SYNC_MODE_FLOOD

    # Ten minutes in which Panel A chatters happily and Panel B says nothing.
    for _ in range(20):
        harness.clock.advance(30.0)
        harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))

    assert harness.ws.connected is True, "the failure under test is an OPEN socket"
    assert harness.ws.can_sample(HUB_A) is True, "Panel A really is alive"
    assert harness.ws.can_sample(HUB_B) is False
    assert harness.ws.freshness(HUB_B).reason == REASON_STALLED

    snapshot_b = make_snapshot(HUB_B, breaker_id=BREAKER_ID_B, ct_id=CT_ID_B)
    assert harness.ws.overlay_snapshot(snapshot_b) is None, (
        "Panel B's frozen store must not be published as current"
    )
    assert harness.ws.overlay_snapshot(make_snapshot(HUB_A)) is not None
    assert harness.ws.sample_object(WS_MODEL_CT, CT_ID_B) is None
    assert harness.ws.sample_object(WS_MODEL_CT, CT_ID) is not None

    status = harness.ws.status_snapshot()
    assert status["stalled_hubs"] == [HUB_B]
    assert status["hub_silence_s"][HUB_A] == 0.0
    assert status["hub_silence_s"][HUB_B] > 90.0


async def test_the_aggregate_gate_is_strict_when_any_hub_has_gone_dead() -> None:
    """``overlay_snapshots`` is all-or-nothing, so it must answer for every hub.

    ``sources/leviton.py`` maps whatever comes back into one poll cycle. Quietly
    returning a subset would drop a hub's channels with no reason recorded
    anywhere and no REST fallback in ``hybrid``; a shut aggregate gate names its
    reason and each mode's documented policy applies.
    """
    harness = build(
        seed=two_hub_seed, targets=two_hub_targets(), stall_timeout_s=90.0
    )
    await harness.ws.start()
    flood_hub(harness.transport, HUB_A, BREAKER_ID, CT_ID)
    flood_hub(harness.transport, HUB_B, BREAKER_ID_B, CT_ID_B)

    harness.clock.advance(91.0)
    harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))

    snapshots = [
        make_snapshot(HUB_A),
        make_snapshot(HUB_B, breaker_id=BREAKER_ID_B, ct_id=CT_ID_B),
    ]
    assert harness.ws.overlay_snapshots(snapshots) is None
    assert harness.ws.can_sample() is False
    assert harness.ws.withheld_reason == REASON_STALLED


async def test_a_dead_hub_forces_a_reconnect_rather_than_being_left_dark() -> None:
    """Subscriptions do not survive a connection, so recovery means reconnecting."""
    harness = build(
        seed=two_hub_seed, targets=two_hub_targets(), stall_timeout_s=90.0
    )
    await harness.ws.start()
    flood_hub(harness.transport, HUB_A, BREAKER_ID, CT_ID)
    flood_hub(harness.transport, HUB_B, BREAKER_ID_B, CT_ID_B)

    for _ in range(4):
        harness.clock.advance(30.0)
        harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))
    await harness.ws.tick()

    assert len(harness.transports) == 2, "a hub whose feed died must be recovered"
    assert harness.ws.status_snapshot()["stalls"] == 1


# --------------------------------------- (4) the wait set is what we WANT, not what stuck


async def test_a_failed_subscribe_blocks_the_flood_sync_and_is_retried() -> None:
    """One object silently never pushed is a silent outage on that channel.

    A per-target subscribe failure drops the target out of ``_subscribed``, so
    deriving the wait set from ``_subscribed`` lets the connection claim the
    strong ``flood`` sync while that object is dark for the life of the
    connection — and ``set_targets`` never retries it, because it is already in
    ``self._targets`` and only the added/removed diff is sent. So the wait set
    comes from the DESIRED targets, and the watchdog retries.
    """
    harness = build(seed=None, sync_flood_timeout_s=20.0)

    async def factory() -> FakeTransport:
        transport = FakeTransport()
        transport.subscribe_fail_keys.add((WS_MODEL_CT, CT_ID))
        harness.transports.append(transport)
        return transport

    harness.ws._transport_factory = factory  # noqa: SLF001 - injection point
    await harness.ws.start()
    assert (WS_MODEL_CT, CT_ID) not in harness.transport.subscribed

    # The flood covers everything that *did* subscribe.
    harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.4))
    harness.transport.push(breaker_notification(BREAKER_ID, power=1240.0))

    assert harness.ws.can_sample() is False, (
        "an object we never managed to subscribe cannot be declared synced"
    )
    assert harness.ws.status_snapshot()["sync_mode"] != SYNC_MODE_FLOOD
    assert harness.ws.status_snapshot()["subscriptions_pending"] == 1
    assert harness.ws.status_snapshot()["subscribe_failures"] >= 1

    harness.transport.subscribe_fail_keys.clear()
    await harness.ws.tick()
    assert (WS_MODEL_CT, CT_ID) in harness.transport.subscribed, "the watchdog retries"
    assert harness.ws.can_sample() is False, "subscribed is not the same as pushed"

    harness.transport.push(ct_notification(CT_ID, activePower=4102.5))
    assert harness.ws.can_sample() is True
    assert harness.ws.status_snapshot()["sync_mode"] == SYNC_MODE_FLOOD


async def test_set_targets_retries_a_target_it_already_holds_but_never_subscribed() -> None:
    """The hourly discovery pass must be able to heal a failed subscribe too."""
    harness = build(seed=None)

    async def factory() -> FakeTransport:
        transport = FakeTransport()
        transport.subscribe_fail_keys.add((WS_MODEL_BREAKER, BREAKER_ID))
        harness.transports.append(transport)
        return transport

    harness.ws._transport_factory = factory  # noqa: SLF001 - injection point
    await harness.ws.start()
    assert (WS_MODEL_BREAKER, BREAKER_ID) not in harness.transport.subscribed

    harness.transport.subscribe_fail_keys.clear()
    # Discovery reports the identical set — no diff at all, and yet the missing
    # subscription must be repaired.
    await harness.ws.set_targets(default_targets())

    assert (WS_MODEL_BREAKER, BREAKER_ID) in harness.transport.subscribed


# ------------------------------- (5) "a connection succeeded" means "it proved itself"


async def test_a_server_that_drops_every_connection_is_backed_off_not_hot_looped() -> None:
    """A handshake that completes has proved nothing.

    Resetting the ladder whenever ``_connect_once`` returns without raising, and
    never backing off on the server-drop path, turns a server that accepts and
    immediately drops every connection into an unthrottled reconnect loop
    against Leviton — the exact behaviour §6.1's "never log in more than once
    per 10 seconds" exists to avoid, one layer down.
    """
    harness = build(seed=None)
    await harness.ws.start()
    assert harness.ws.connected is True

    harness.transport.drop(close_code=1006)
    assert harness.ws.connected is False
    assert harness.ws.status_snapshot()["server_drops"] == 1
    assert harness.ws.reconnects == 1, "the drop is counted"

    await harness.ws.tick()
    assert len(harness.transports) == 1, "the next attempt waits for the backoff"
    assert harness.ws.status_snapshot()["connect_attempts"] == 1

    harness.clock.advance(60.0)
    await harness.ws.tick()
    assert len(harness.transports) == 2

    # Dropped again before it ever synced: the ladder must keep climbing.
    harness.transport.drop(close_code=1006)
    await harness.ws.tick()
    assert len(harness.transports) == 2
    assert harness.ws.status_snapshot()["connect_attempts"] == 2
    assert harness.ws.status_snapshot()["server_drops"] == 2


async def test_the_reconnect_ladder_clears_only_once_a_connection_reaches_sync() -> None:
    """"Proved itself" is defined as "re-established state on a live socket"."""
    harness = build(seed=None)
    await harness.ws.start()
    harness.transport.drop()
    harness.clock.advance(60.0)
    await harness.ws.tick()

    assert harness.ws.connected is True
    assert harness.ws.status_snapshot()["connect_attempts"] == 1, "not yet proved"

    harness.flood()
    assert harness.ws.can_sample() is True
    assert harness.ws.status_snapshot()["connect_attempts"] == 0


# ------------------------------------------------ (6) `sync_mode` must not lie to the owner


async def test_a_connection_carrying_only_the_rest_cache_is_never_called_a_flood() -> None:
    """``sync_mode`` is the signal the owner reads to judge the socket.

    Labelling the timeout path ``flood`` whenever ``_awaiting`` happens to be
    empty reports the strongest possible answer for the weakest possible
    connection — one that subscribed to nothing and received nothing, sampling
    a REST cache. A false ``flood`` is worse than a missing one.
    """
    harness = build(targets=(), sync_flood_timeout_s=20.0)
    await harness.ws.start()
    harness.clock.advance(21.0)

    assert harness.ws.can_sample() is True, "a REST seed is still state"
    assert harness.ws.status_snapshot()["sync_mode"] == SYNC_MODE_TIMEOUT
    assert harness.ws.status_snapshot()["seeded_from_rest"] is True


async def test_a_seed_that_produced_nothing_does_not_count_as_seeded() -> None:
    """``synced: true, can_sample: true`` for a connection carrying nothing at all."""

    async def empty_seed() -> tuple[HubSnapshot, ...]:
        return ()

    harness = build(targets=(), seed=empty_seed, sync_flood_timeout_s=20.0)
    await harness.ws.start()
    harness.clock.advance(21.0)

    status = harness.ws.status_snapshot()
    assert status["seeded_from_rest"] is False
    assert status["can_sample"] is False
    assert status["synced"] is False
    assert status["sync_mode"] is None
    assert status["withheld_reason"] == REASON_AWAITING_SYNC


async def test_the_status_file_the_owner_reads_tells_the_truth_about_both_hubs(
    tmp_path: Path,
) -> None:
    """The first live run is read off ``status.json``, not off a Python method.

    Two things in that file decide what the owner concludes about step 9, and
    both were capable of lying before this pass: ``sync_mode``, which must say
    ``flood`` only when a real flood covered every desired subscription, and the
    per-hub silence, without which a dead feed hides behind its sibling. This
    goes through the real :class:`StatusStore` and re-reads the file from disk,
    because a value that never leaves ``status_snapshot()`` is not what gets
    read at 3am.
    """
    store = StatusStore(path=tmp_path / "status.json", load_existing=False)
    harness = build(
        seed=two_hub_seed,
        targets=two_hub_targets(),
        status_store=store,
        stall_timeout_s=90.0,
        sync_flood_timeout_s=20.0,
    )
    await harness.ws.start()

    # The flood covers Panel A only, and the gate opens on the timeout. Calling
    # that `flood` is the lie: it is the signal that says the push feed works.
    flood_hub(harness.transport, HUB_A, BREAKER_ID, CT_ID)
    harness.clock.advance(21.0)
    await harness.ws.tick()

    section = json.loads((tmp_path / "status.json").read_text())[
        ws_module.STATUS_SECTION_WS
    ]
    assert section["sync_mode"] == SYNC_MODE_TIMEOUT
    assert section["awaiting_sync"] == 3, "Panel B's three objects never arrived"
    assert set(section["hub_silence_s"]) == {HUB_A, HUB_B}

    # Ten more minutes of Panel A only: the file must name the hub that died.
    for _ in range(20):
        harness.clock.advance(30.0)
        harness.transport.push(hub_notification(HUB_A, rmsVoltageA=121.3))
    await harness.ws.tick()  # the stall guard cycles the socket

    section = json.loads((tmp_path / "status.json").read_text())[
        ws_module.STATUS_SECTION_WS
    ]
    assert section["stalls"] == 1
    assert section["reconnects"] == 1

    # And on a connection the flood really did cover, `flood` is earned.
    flood_hub(harness.transport, HUB_A, BREAKER_ID, CT_ID)
    flood_hub(harness.transport, HUB_B, BREAKER_ID_B, CT_ID_B)
    await harness.ws.tick()

    section = json.loads((tmp_path / "status.json").read_text())[
        ws_module.STATUS_SECTION_WS
    ]
    assert section["sync_mode"] == SYNC_MODE_FLOOD
    assert section["can_sample"] is True
    assert section["stalled_hubs"] == []
    assert section["hub_silence_s"] == {HUB_A: 0.0, HUB_B: 0.0}


# ------------------------------------------------------ (7) the handshake fingerprint


class RecordingSession:
    """The two things ``LevitonWebSocket`` uses its session for."""

    def __init__(self) -> None:
        self.ws_connect_calls: list[dict[str, Any]] = []

    def ws_connect(self, url: str, **kwargs: Any) -> str:
        self.ws_connect_calls.append({"url": url, **kwargs})
        return "ws-response"

    async def close(self) -> None:  # pragma: no cover - pass-through proof
        return None


class StubLevitonWs:
    """Just enough of ``aioleviton.LevitonWebSocket`` to exercise the seam."""

    def __init__(self) -> None:
        self._session = RecordingSession()
        self._token = "leviton-token"


def test_the_websocket_handshake_carries_the_origin_the_rest_adapter_uses() -> None:
    """PLAN.md §6.1: Leviton appears to fingerprint callers.

    ``aioleviton.LevitonWebSocket.connect()`` sends **no** ``Origin`` while both
    other implementations of this protocol do, and our REST adapter already
    injects one for exactly this reason. Its headers are a hardcoded literal, so
    the handshake is made controllable at our seam instead — and if it were not,
    the very first live run could be refused by the cloud.
    """
    from aioleviton.const import USER_AGENT

    ws = StubLevitonWs()
    apply_ws_handshake_headers(ws)
    # Reproduce upstream's exact call: WEBSOCKET_URL, heartbeat, and a hardcoded
    # user-agent-only header dict.
    ws._session.ws_connect(  # noqa: SLF001
        "wss://socket.cloud.leviton.com/",
        heartbeat=30.0,
        headers={"user-agent": USER_AGENT},
    )

    sent = ws._session._session.ws_connect_calls[0]["headers"]  # noqa: SLF001
    assert sent["origin"] == WS_HANDSHAKE_ORIGIN
    assert sent["referer"].startswith(WS_HANDSHAKE_ORIGIN)
    assert sent["user-agent"] == USER_AGENT
    assert default_ws_handshake_headers()["origin"] == WS_HANDSHAKE_ORIGIN


def test_the_handshake_origin_is_the_one_the_rest_path_already_sends() -> None:
    """One spoofed identity, not two — the value lives in ``sources/leviton.py``."""
    from energy_capture.sources.leviton import LEVITON_ORIGIN

    assert WS_HANDSHAKE_ORIGIN == LEVITON_ORIGIN


def test_injecting_the_handshake_headers_twice_does_not_nest_the_wrapper() -> None:
    ws = StubLevitonWs()
    original = ws._session  # noqa: SLF001
    apply_ws_handshake_headers(ws)
    apply_ws_handshake_headers(ws, {"origin": "https://example.invalid"})

    assert ws._session._session is original  # noqa: SLF001
    assert ws._session.handshake_headers["origin"] == "https://example.invalid"  # noqa: SLF001


def test_a_websocket_we_cannot_reach_into_still_connects() -> None:
    """A missing ``Origin`` may well work; refusing to connect over it must not."""

    class NoSession:
        pass

    subject = NoSession()
    assert apply_ws_handshake_headers(subject) is subject


async def test_the_production_factory_injects_the_headers_before_connecting() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.made: list[StubLevitonWs] = []

        def create_websocket(self) -> StubLevitonWs:
            ws = StubLevitonWs()
            self.made.append(ws)
            return ws

    client = FakeClient()
    transport = await transport_factory_from_client(client)()

    assert isinstance(transport, AioLevitonWsTransport)
    headers = client.made[0]._session.handshake_headers  # noqa: SLF001
    assert headers["origin"] == WS_HANDSHAKE_ORIGIN


def test_the_aioleviton_handshake_seam_is_still_where_we_reach_into_it() -> None:
    """The re-check list for an ``aioleviton`` upgrade, as an executable test.

    Nothing is vendored — the one attribute ``connect()`` reads is wrapped. Two
    upstream changes would silently undo that, and this is what notices: the
    call moving off ``self._session``, or upstream starting to send its own
    ``Origin`` (in which case wrapping is no longer needed and the ``origin=``
    keyword would win over our merged headers anyway).
    """
    import inspect

    from aioleviton.websocket import LevitonWebSocket

    source = inspect.getsource(LevitonWebSocket.connect)
    assert "self._session.ws_connect(" in source, (
        "aioleviton no longer builds the handshake from self._session; "
        "leviton_ws._HandshakeHeaderSession must be revisited"
    )
    assert "origin" not in source.lower(), (
        "aioleviton now sends its own Origin; re-check whether our injection is "
        "still needed and whether it still wins"
    )


def _executable_source(source: str) -> str:
    """``source`` with every docstring and comment removed.

    The prose in this module discusses bandwidth, ``pollBreakers`` and the spool
    at length; the point of the test above is that none of it is *code*.
    """
    tree = ast.parse(source)
    docstring_lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstring_lines.update(range(first.lineno, (first.end_lineno or first.lineno) + 1))
    return "\n".join(
        line
        for number, line in enumerate(source.splitlines(), start=1)
        if number not in docstring_lines and not line.strip().startswith("#")
    )
