"""``energycap discover`` — live hierarchy → paste-ready ``channel_map.json`` (PLAN.md §9).

This is the five-minute path from *"I installed a new smart breaker"* to *"it is
in the semantic layer"*:

.. code-block:: console

   $ energycap discover                     # look at the panel, copy the skeleton
   $ $EDITOR config/channel_map.json        # paste, fill blackstart_device_id
   $ energycap build-dim                    # the label reaches every query

Three things come out of one run:

1. **A readable table** of everything the two clouds report — Leviton hubs
   (id, firmware, connected), every breaker (position, name, model,
   ``branchType``, poles, connected), every CT (channel, ``usageType``) and the
   Bryant system serial with its zones. Objects the pipeline *skips* —
   ``NONE``/``NONE-1``/``NONE-2`` dumb-breaker placeholders, ``NOT_USED`` CTs,
   the phantom zones a single-zone install reports — are **shown and labelled
   SKIP**, never hidden. Knowing what is skipped is most of the point: a breaker
   that produces no rows because Leviton calls it ``NONE-2`` is otherwise
   indistinguishable from one the collector is failing to read.
2. **Ready-to-paste ``channel_map.json`` skeleton entries** for every live
   channel not already in the map, in the exact shape
   :meth:`~energy_capture.sources.base.DiscoveredChannel.channel_map_entry`
   defines and ``stages/dim.py`` consumes.
3. **A machine-readable live-channel file** (``config/live_channels.json`` by
   default, beside the map — :func:`live_channels_path`) so ``build-dim`` can
   WARN about unmapped live channels (PLAN.md §9) **without a second live
   call**. The document is versioned and self-describing; see
   :func:`live_channels_document`.

Operating rules, because this command talks to two third-party clouds:

* **It degrades.** Each source is enumerated inside its own guard. A cloud that
  cannot authenticate reports one clear line and the other source still prints
  its table and still contributes skeleton entries. Nothing here shows the
  operator a traceback.
* **It writes no data.** No S3 object, no spool row, no Parquet. The only files
  it touches are the live-channel sidecar and, on request, the raw dump.
* **It reuses the sources.** Auth, the token caches, the login floor, the
  502-retry policy, the Origin spoof and the 401 ladder all stay in
  ``sources/leviton.py`` and ``sources/bryant.py``. This module names no URL.

.. _discover-raw:

Capturing evidence: the raw dump
--------------------------------

PLAN.md §7.3 and DEVIATIONS.md #75 list fields nothing offline can settle —
whether ``infinityStatus(serial:)`` resolves at all, whether ``odu.opstat`` is a
word or a capacity percentage, how many zones are really enabled, whether
``cfgem`` is ``F``. **The first live run is the only chance to capture that
cheaply**, so this command can write every raw Leviton and Carrier response to
one JSON file:

.. code-block:: console

   $ ENERGYCAP_DISCOVER_DUMP=discover-raw.json energycap discover

Commit the interesting parts as a test fixture (``tests/fixtures/leviton/``,
``tests/fixtures/bryant/``) *before* freezing any encoding that depends on them
— particularly the ``stage`` enum (DEVIATIONS.md #59). The dump is written mode
``0600``: it is a complete record of the house's hardware.

The dump is reachable today through ``ENERGYCAP_DISCOVER_DUMP`` because
``energycap discover`` has no ``--raw/--dump`` flag yet. :func:`run` already
takes ``dump_path=``/``raw=``, so adding the flag is one option in ``cli.py``
passed straight through; :data:`DUMP_HELP` is the wording for it.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Final, TextIO

from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger, scrub_text
from energy_capture.model import SOURCE_BRYANT, SOURCE_LEVITON
from energy_capture.sources.base import DiscoveredDevice, Discovery
from energy_capture.timeutil import now_utc

__all__ = [
    "DUMP_HELP",
    "ENV_DUMP",
    "ENV_LIVE_CHANNELS",
    "ENV_NO_WRITE",
    "LIVE_CHANNELS_FILENAME",
    "LIVE_CHANNELS_VERSION",
    "SUPPORTED_SOURCES",
    "ChannelMap",
    "DiscoverResult",
    "DiscoveryFailed",
    "LiveChannel",
    "SourceReport",
    "live_channels_document",
    "live_channels_path",
    "read_channel_map",
    "render_report",
    "run",
    "skeleton_document",
]


# ============================================================================
# Constants
# ============================================================================

#: Sources this command can enumerate. ``lge`` (PLAN.md §13) has no live API yet.
SUPPORTED_SOURCES: Final[tuple[str, ...]] = (SOURCE_LEVITON, SOURCE_BRYANT)

#: Sidecar written beside ``channel_map.json`` so ``build-dim`` can WARN about
#: unmapped live channels without making its own live call (PLAN.md §9).
LIVE_CHANNELS_FILENAME: Final[str] = "live_channels.json"

#: Schema version of that document. Bump it if a consumer would break.
LIVE_CHANNELS_VERSION: Final[int] = 1

#: ``ENERGYCAP_DISCOVER_DUMP=<path>`` (or ``=1`` for a timestamped default)
#: writes the raw upstream responses — see :ref:`discover-raw`.
ENV_DUMP: Final[str] = "ENERGYCAP_DISCOVER_DUMP"

#: Override the live-channel sidecar path.
ENV_LIVE_CHANNELS: Final[str] = "ENERGYCAP_DISCOVER_OUT"

#: Set to ``1`` to skip writing the sidecar (a strictly read-only run).
ENV_NO_WRITE: Final[str] = "ENERGYCAP_DISCOVER_NO_WRITE"

#: Help text for the ``--raw/--dump`` flag ``cli.py`` should grow. Kept here so
#: the wording lives next to the behaviour it describes.
DUMP_HELP: Final[str] = (
    "Write every raw Leviton and Carrier response to FILE (mode 0600). "
    "The first live run is the only cheap chance to capture the evidence "
    "PLAN.md §7.3 leaves UNVERIFIED — whether infinityStatus(serial:) "
    "resolves, whether odu.opstat is a word or a capacity percentage, the real "
    "zone count, whether cfgem is F — so capture it and commit the payloads as "
    "test fixtures. Also settable as ENERGYCAP_DISCOVER_DUMP."
)

#: ``model`` values for the Leviton **LSBMA** accessory — a physical CT-style
#: add-on, not a dumb-breaker placeholder. PLAN.md §6.3's skip list is exactly
#: ``NONE``/``NONE-1``/``NONE-2``, so an LSBMA *is* metered and *is* mappable
#: (DEVIATIONS.md #16 flags that ``aioleviton`` disagrees). It is labelled in
#: the table so the operator can see which is which.
ACCESSORY_BREAKER_MODELS: Final[frozenset[str]] = frozenset({"LSBMA"})

#: ``stages/dim.py``'s marker for a map entry whose real id is not known yet
#: (a Leviton hub id cannot be guessed offline). Kept as a literal so this
#: command never needs ``build-dim`` to be importable.
PLACEHOLDER_TOKEN: Final[str] = "PLACEHOLDER"

_SKIP_PLACEHOLDER: Final[str] = "SKIP: placeholder model (dumb breaker, no meter)"
_SKIP_CT_UNUSED: Final[str] = "SKIP: usageType=NOT_USED (clamp on nothing)"
_SKIP_ZONE_DISABLED: Final[str] = "SKIP: enabled!=on (phantom zone, not installed)"
_SKIP_UNPOSITIONED: Final[str] = (
    "SKIP: no position from the cloud (un-positioned breaker — run the "
    "positioning wizard in the Leviton app)"
)
_NOTE_ACCESSORY: Final[str] = "LSBMA accessory — metered, mapped like a breaker"
_NOTE_SINGLE_LEG: Final[str] = "single-leg CT — leg B reported nothing this cycle"

_LOG = get_logger("discover")


class DiscoveryFailed(RuntimeError):
    """Every requested source failed. Raised **after** the report has printed.

    The CLI turns this into a single red line and exit code 1 (``--traceback``
    for more), so a scripted run still fails loudly while the operator has
    already seen everything that *did* work.
    """


# ============================================================================
# Data model
# ============================================================================


@dataclass(frozen=True, slots=True)
class LiveChannel:
    """One row of the live hierarchy — mappable or deliberately skipped.

    ``mappable`` is what separates "this channel produces rows and wants a
    ``channel_map.json`` entry" from "the API returns this object but the
    pipeline emits nothing for it" (a ``NONE-2`` placeholder, a ``NOT_USED``
    CT, a phantom zone). Skipped channels are printed with their
    :attr:`skip_reason` and are excluded from the skeleton — they must never
    become a ``dim_channel`` row, and ``build-dim`` must never WARN about them.
    """

    source: str
    device_id: str
    channel_id: str
    kind: str
    label: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)
    mappable: bool = True
    skip_reason: str | None = None
    note: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        """``(source, device_id, channel_id)`` — ``model.DIM_KEY``'s columns."""
        return (self.source, self.device_id, self.channel_id)

    def channel_map_entry(self) -> dict[str, Any]:
        """The paste-ready ``mappings`` entry (PLAN.md §9).

        Identical in shape to
        :meth:`energy_capture.sources.base.DiscoveredChannel.channel_map_entry`
        — that method is the shared contract with ``stages/dim.py`` and this one
        exists only so a skipped channel can never accidentally produce one.
        """
        return {
            "source": self.source,
            "device_id": self.device_id,
            "channel_id": self.channel_id,
            "label": self.label or "",
            "blackstart_device_id": "",
        }

    def to_json(self, *, mapped: bool) -> dict[str, Any]:
        """This channel as the sidecar document records it."""
        return {
            "source": self.source,
            "device_id": self.device_id,
            "channel_id": self.channel_id,
            "kind": self.kind,
            "label": self.label,
            "mappable": self.mappable,
            "skip_reason": self.skip_reason,
            "mapped": mapped,
            "details": _jsonable(self.details),
        }


@dataclass(frozen=True, slots=True)
class SourceReport:
    """What one cloud told us — or why it could not (PLAN.md §9's degradation)."""

    name: str
    ok: bool
    error: str | None = None
    devices: tuple[DiscoveredDevice, ...] = ()
    channels: tuple[LiveChannel, ...] = ()
    #: Free-form observations worth printing: the query that resolved, the raw
    #: ``odu.opstat``, ``cfgem`` — the UNVERIFIED list of DEVIATIONS.md #75.
    facts: Mapping[str, Any] = field(default_factory=dict)

    @property
    def mappable(self) -> tuple[LiveChannel, ...]:
        return tuple(channel for channel in self.channels if channel.mappable)


@dataclass(frozen=True, slots=True)
class ChannelMap:
    """The hand-maintained ``config/channel_map.json``, as far as we can read it."""

    path: Path
    keys: frozenset[tuple[str, str, str]] = frozenset()
    entries: int = 0
    exists: bool = False
    error: str | None = None
    #: Entries that lacked one of the three key fields — they map nothing.
    malformed: int = 0
    #: Entries still carrying ``stages/dim.py``'s ``PLACEHOLDER`` token. They
    #: are excluded from :attr:`keys`, so the channel they describe still shows
    #: up as unmapped — which is the truth until a real id is pasted in.
    placeholders: int = 0


@dataclass(frozen=True, slots=True)
class DiscoverResult:
    """Everything one ``energycap discover`` run produced."""

    generated_utc: datetime
    channel_map: ChannelMap
    reports: tuple[SourceReport, ...]
    live_channels_path: Path | None = None
    dump_path: Path | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------- accessors
    @property
    def channels(self) -> tuple[LiveChannel, ...]:
        return tuple(channel for report in self.reports for channel in report.channels)

    @property
    def devices(self) -> tuple[DiscoveredDevice, ...]:
        return tuple(device for report in self.reports for device in report.devices)

    @property
    def unmapped(self) -> tuple[LiveChannel, ...]:
        """Mappable live channels with no ``channel_map.json`` entry, in order."""
        known = self.channel_map.keys
        return tuple(
            channel
            for channel in sorted(self.channels, key=_channel_sort_key)
            if channel.mappable and channel.key not in known
        )

    def is_mapped(self, channel: LiveChannel) -> bool:
        return channel.key in self.channel_map.keys

    @property
    def failed_sources(self) -> tuple[str, ...]:
        return tuple(report.name for report in self.reports if not report.ok)

    def summary(self) -> dict[str, Any]:
        """The mapping ``cli._run_stage`` folds into its ``stage_ok`` line."""
        out: dict[str, Any] = {
            "sources_ok": [r.name for r in self.reports if r.ok],
            "sources_failed": list(self.failed_sources),
            "devices": len(self.devices),
            "channels": len([c for c in self.channels if c.mappable]),
            "channels_skipped": len([c for c in self.channels if not c.mappable]),
            "mapped": len([c for c in self.channels if c.mappable and self.is_mapped(c)]),
            "unmapped": len(self.unmapped),
            "map_entries": self.channel_map.entries,
            "map_placeholders": self.channel_map.placeholders,
        }
        if self.live_channels_path is not None:
            out["live_channels_path"] = str(self.live_channels_path)
        if self.dump_path is not None:
            out["dump_path"] = str(self.dump_path)
        return out


# ============================================================================
# channel_map.json
# ============================================================================


def read_channel_map(path: Path) -> ChannelMap:
    """Read the committed map, tolerating every way it can be missing.

    A map that does not exist yet is the normal first-run state, not an error —
    the whole point of this command is to produce the first one. A map that
    exists but cannot be parsed reports the parse error and is treated as empty,
    so the operator still gets a table and a skeleton; nothing is written on the
    strength of an unreadable file.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ChannelMap(path=path, exists=False)
    except OSError as exc:
        return ChannelMap(path=path, exists=True, error=scrub_text(str(exc)))

    try:
        payload = json.loads(raw)
    except ValueError as exc:
        return ChannelMap(path=path, exists=True, error=f"not valid JSON: {exc}")
    if not isinstance(payload, Mapping):
        return ChannelMap(path=path, exists=True, error="top level is not an object")

    mappings = payload.get("mappings")
    if not isinstance(mappings, Sequence) or isinstance(mappings, (str, bytes)):
        return ChannelMap(path=path, exists=True, error='no "mappings" array')

    keys: set[tuple[str, str, str]] = set()
    malformed = 0
    placeholders = 0
    for entry in mappings:
        if not isinstance(entry, Mapping):
            malformed += 1
            continue
        key = (
            str(entry.get("source") or "").strip(),
            str(entry.get("device_id") or "").strip(),
            str(entry.get("channel_id") or "").strip(),
        )
        if not all(key):
            malformed += 1
            continue
        if _is_placeholder_entry(entry, key):
            # An entry waiting for a real id (``stages/dim.py``'s PLACEHOLDER)
            # maps nothing yet: the channel it describes is still unmapped, and
            # counting it would hide exactly the work this command exists for.
            placeholders += 1
            continue
        keys.add(key)
    return ChannelMap(
        path=path,
        keys=frozenset(keys),
        entries=len(mappings),
        exists=True,
        malformed=malformed,
        placeholders=placeholders,
    )


def _is_placeholder_entry(entry: Mapping[str, Any], key: tuple[str, str, str]) -> bool:
    """``{"placeholder": true}`` or the literal ``PLACEHOLDER`` in an id.

    Mirrors ``stages/dim.py``'s ``PLACEHOLDER_TOKEN``; duplicated as a literal
    rather than imported so ``discover`` never depends on ``build-dim`` being
    importable (they are separate stages and each must run alone).
    """
    if entry.get("placeholder") in (True, "true", "True", 1):
        return True
    return any(PLACEHOLDER_TOKEN in part.upper() for part in key)


def live_channels_path(map_path: Path) -> Path:
    """Where the machine-readable live-channel document lives.

    Beside ``channel_map.json`` — the two files describe the same thing (what
    exists vs what is named), and ``build-dim`` already knows the map's path, so
    it can find this one without a new setting.
    """
    return Path(map_path).parent / LIVE_CHANNELS_FILENAME


# ============================================================================
# Recording wrappers — how the raw dump is captured
# ============================================================================


class _RecordingGraphQLClient:
    """Wraps ``CarrierGraphQLClient`` and keeps every request/response.

    Injected through :class:`~energy_capture.sources.bryant.BryantStatusSource`'s
    documented ``client=`` seam, so the auth ladder, the throttle handling and
    the query text all stay in ``sources/`` — this only remembers what crossed
    the wire. That recording is what makes the raw dump (and the "did
    ``infinityStatus(serial:)`` resolve?" answer) possible.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.records: list[dict[str, Any]] = []

    async def query(
        self,
        query: str,
        *,
        variables: Mapping[str, Any] | None = None,
        operation_name: str | None = None,
        op: str | None = None,
    ) -> Any:
        record: dict[str, Any] = {
            "operation": operation_name or op or "graphql",
            "variables": dict(variables or {}),
        }
        try:
            data = await self._inner.query(
                query, variables=variables, operation_name=operation_name, op=op
            )
        except Exception as exc:  # recorded, then re-raised untouched
            record["error"] = scrub_text(f"{type(exc).__name__}: {exc}")
            self.records.append(record)
            raise
        record["data"] = data
        self.records.append(record)
        return data

    async def close(self) -> None:
        await self._inner.close()

    def __getattr__(self, name: str) -> Any:
        # `status_fields`, `throttle`, `auth`, … stay available untouched.
        return getattr(self._inner, name)


def _leviton_adapter_class() -> Any:
    """Build the raw-capturing Leviton adapter class (imported lazily).

    ``sources/leviton.py`` imports ``aioleviton``; keeping the import inside the
    function means ``energycap discover --help`` and every offline test that
    only touches the Bryant path stay cheap.
    """
    from energy_capture.sources.leviton import (
        BreakerReading,
        CtReading,
        HubReading,
        LevitonAdapter,
    )

    class _RawCapturingLevitonAdapter(LevitonAdapter):
        """A :class:`LevitonAdapter` that also keeps the untouched JSON.

        ``aioleviton`` preserves each response body on its model objects
        (``Whem.raw``, ``Breaker.raw``, ``Ct.raw``), but the adapter converts
        models into readings and drops it, and exposes no hook to observe them.
        The three fetchers are therefore re-expressed here — each is the same
        single upstream call plus the same ``from_model`` conversion the parent
        does — so authentication, the ``Origin`` spoof, the token cache, the
        502-retry policy and the exception translation all stay in
        ``sources/leviton.py`` and are not duplicated.
        """

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            # Keyed by object/hub id: `discover` fetches the hierarchy twice
            # (see `_collect_leviton`), and the dump should hold one copy of
            # each object, not one per pass.
            self._raw_whems: dict[str, dict[str, Any]] = {}
            self._raw_breakers: dict[str, list[dict[str, Any]]] = {}
            self._raw_cts: dict[str, list[dict[str, Any]]] = {}

        def raw_document(self) -> dict[str, Any]:
            """The untouched response bodies, ready for the raw dump."""
            return {
                "iotWhems": list(self._raw_whems.values()),
                "residentialBreakers": dict(self._raw_breakers),
                "iotCts": dict(self._raw_cts),
            }

        async def fetch_hubs(self, residence_ids: Sequence[int]) -> tuple[Any, ...]:
            client = self._ensure_client()
            hubs: list[Any] = []
            seen: set[str] = set()
            for residence_id in residence_ids:
                whems = await self._call(
                    lambda residence_id=residence_id: client.get_whems(residence_id),
                    op="whems",
                )
                for whem in whems:
                    reading = HubReading.from_model(whem)
                    self._raw_whems[reading.device_id] = _model_raw(whem)
                    if reading.device_id in seen:
                        continue
                    seen.add(reading.device_id)
                    hubs.append(reading)
            return tuple(hubs)

        async def fetch_breakers(self, hub_id: str) -> tuple[Any, ...]:
            client = self._ensure_client()
            breakers = await self._call(
                lambda: client.get_whem_breakers(hub_id), op="breakers"
            )
            self._raw_breakers[hub_id] = [_model_raw(b) for b in breakers]
            return tuple(BreakerReading.from_model(b) for b in breakers)

        async def fetch_cts(self, hub_id: str) -> tuple[Any, ...]:
            client = self._ensure_client()
            cts = await self._call(lambda: client.get_cts(hub_id), op="cts")
            self._raw_cts[hub_id] = [_model_raw(c) for c in cts]
            return tuple(CtReading.from_model(c) for c in cts)

    return _RawCapturingLevitonAdapter


def _model_raw(model: Any) -> dict[str, Any]:
    """The untouched response body ``aioleviton`` parsed, or ``{}``."""
    raw = getattr(model, "raw", None)
    return dict(raw) if isinstance(raw, Mapping) else {}


# ============================================================================
# Leviton
# ============================================================================


async def _collect_leviton(
    settings: Settings, *, source: Any = None
) -> tuple[SourceReport, Mapping[str, Any]]:
    """Enumerate hubs, breakers and CTs — including the objects we skip (§6.2).

    Two passes on purpose:

    * ``source.discover()`` produces the **authoritative** channel set. The skip
      rules (placeholder models, ``NOT_USED``, single-leg CTs) live in
      ``sources/leviton.py`` and are not restated here, so what this command
      offers for mapping is exactly what the poller will emit rows for.
    * ``adapter.fetch_snapshot()`` returns the **whole** hierarchy, skipped
      objects included, which is what the table needs — a dumb-breaker
      placeholder must be visible as such.

    That is two hierarchy fetches for one manual command. Deliberate: nothing
    here runs on the 30s loop, and a stale or half-fetched table is worse than a
    second round trip.
    """
    from energy_capture.sources.leviton import (
        CT_USAGE_NOT_USED,
        LevitonSource,
        breaker_channel_id,
        ct_channel_id,
        panel_leg_channel_id,
    )

    owns = source is None
    if source is None:
        adapter_cls = _leviton_adapter_class()
        adapter = adapter_cls(
            username=settings.require("leviton_username"),
            password=settings.require("leviton_password"),
            token_path=settings.leviton_token_path,
        )
        source = LevitonSource(settings, adapter=adapter)

    try:
        await source.start()  # cached token first, then discovery (§6.1/§6.2)
        discovery = getattr(source, "cached_discovery", None) or await source.discover(
            force=True
        )

        adapter = getattr(source, "adapter", None)
        snapshots: tuple[Any, ...] = ()
        if adapter is not None:
            residence_ids = await adapter.fetch_residence_ids()
            snapshots = await adapter.fetch_snapshot(residence_ids)

        live_keys = discovery.channel_keys()
        channels: list[LiveChannel] = []
        devices = list(discovery.devices)

        for snapshot in snapshots:
            hub = snapshot.hub
            device_id = hub.device_id

            for leg in ("a", "b"):
                channel_id = panel_leg_channel_id(leg)
                channels.append(
                    LiveChannel(
                        source=SOURCE_LEVITON,
                        device_id=device_id,
                        channel_id=channel_id,
                        kind="panel_leg",
                        label=f"{hub.name or device_id} leg {leg.upper()}",
                        details={
                            "leg": leg.upper(),
                            "volts": hub.volts_a if leg == "a" else hub.volts_b,
                            "hz": hub.hz_a if leg == "a" else hub.hz_b,
                            "connected": hub.connected,
                        },
                        mappable=(SOURCE_LEVITON, device_id, channel_id) in live_keys,
                    )
                )

            for breaker in snapshot.breakers:
                model_name = (breaker.model or "").strip().upper()
                placeholder = breaker.is_placeholder
                # Shown, never hidden: an un-positioned breaker is a live
                # circuit producing no rows, and this table is where an operator
                # would look to find out why. Its channel_id is displayed as the
                # fiction it is, marked SKIP.
                unpositioned = breaker.is_unpositioned
                channel_id = breaker_channel_id(breaker.position)
                note = _NOTE_ACCESSORY if model_name in ACCESSORY_BREAKER_MODELS else None
                channels.append(
                    LiveChannel(
                        source=SOURCE_LEVITON,
                        device_id=device_id,
                        channel_id=channel_id,
                        kind="breaker",
                        label=breaker.name,
                        details={
                            "position": breaker.position,
                            "poles": breaker.poles,
                            "model": breaker.model,
                            "branchType": breaker.branch_type,
                            "currentState": breaker.current_state,
                            "serialNumber": breaker.serial_number,
                            "connected": breaker.connected,
                            "watts": breaker.metrics().get("watts"),
                        },
                        mappable=not placeholder and not unpositioned,
                        skip_reason=(
                            _SKIP_PLACEHOLDER
                            if placeholder
                            else _SKIP_UNPOSITIONED
                            if unpositioned
                            else None
                        ),
                        note=note,
                    )
                )

            for ct in snapshot.cts:
                unused = ct.is_unused
                # A single-leg CT reports nothing for leg B, so leg B is not a
                # channel at all — the same rule sources/leviton.py applies.
                legs = ("a", "b") if ct.has_second_leg else ("a",)
                for leg in legs:
                    metrics = ct.leg_metrics(leg)
                    single_leg = leg == "a" and not ct.has_second_leg and not unused
                    channels.append(
                        LiveChannel(
                            source=SOURCE_LEVITON,
                            device_id=device_id,
                            channel_id=ct_channel_id(ct.channel, leg),
                            kind="ct",
                            label=f"{ct.name} (leg {leg.upper()})" if ct.name else None,
                            details={
                                "channel": ct.channel,
                                "leg": leg.upper(),
                                "usageType": ct.usage_type or CT_USAGE_NOT_USED,
                                "connected": ct.connected,
                                "watts": metrics.get("watts"),
                            },
                            mappable=not unused,
                            skip_reason=_SKIP_CT_UNUSED if unused else None,
                            note=_NOTE_SINGLE_LEG if single_leg else None,
                        )
                    )

        if not snapshots:
            # No adapter to ask (an injected stub, or a source that only
            # implements the protocol): fall back to the discovery result, which
            # is authoritative for channels but cannot show what was skipped.
            channels = [
                LiveChannel(
                    source=channel.source,
                    device_id=channel.device_id,
                    channel_id=channel.channel_id,
                    kind=channel.kind,
                    label=channel.label,
                    details=dict(channel.details),
                )
                for channel in discovery.channels
            ]

        raw_document = getattr(adapter, "raw_document", None)
        raw = raw_document() if callable(raw_document) else {}
        report = SourceReport(
            name=SOURCE_LEVITON,
            ok=True,
            devices=tuple(devices),
            channels=tuple(channels),
            facts={
                "hubs": len(devices),
                "hubs_connected": sum(
                    1 for d in devices if bool(d.details.get("connected"))
                ),
                "breakers": sum(1 for c in channels if c.kind == "breaker"),
                "cts": sum(1 for c in channels if c.kind == "ct"),
            },
        )
        return report, raw
    finally:
        if owns:
            await _close_quietly(source)


# ============================================================================
# Bryant / Carrier
# ============================================================================


async def _collect_bryant(
    settings: Settings, *, source: Any = None, capture_energy: bool = False
) -> tuple[SourceReport, Mapping[str, Any]]:
    """Enumerate the Bryant system: serial, zones (enabled and phantom), state.

    One status call. Its raw payload is recovered from the recording client so
    the table can show **every** zone the API returns with its ``enabled`` flag —
    a single-zone install reports eight, seven of them carrying plausible
    numbers (DEVIATIONS.md #66), and "how many zones are actually enabled" is
    an open question the first live run has to answer (#75.6).
    """
    from energy_capture.sources.bryant import (
        SYSTEM_CHANNEL,
        BryantStatusSource,
        SystemStatus,
        zone_channel_id,
    )
    from energy_capture.sources.carrier_auth import graphql_client_from_settings

    owns = source is None
    recorder: _RecordingGraphQLClient | None = None
    if source is None:
        recorder = _RecordingGraphQLClient(graphql_client_from_settings(settings))
        source = BryantStatusSource(settings, client=recorder)
    else:
        candidate = getattr(source, "client", None)
        recorder = candidate if isinstance(candidate, _RecordingGraphQLClient) else None

    try:
        discovery = await source.discover(force=True)
        device_id = _device_id_of(discovery, fallback=getattr(source, "device_id", ""))
        payload = _status_payload(recorder, serial=device_id) if recorder else None

        channels: list[LiveChannel] = []
        facts: dict[str, Any] = {"operation": getattr(source, "operation", None)}

        status = (
            SystemStatus.from_payload(payload, device_id=device_id)
            if payload is not None
            else None
        )

        channels.append(
            LiveChannel(
                source=SOURCE_BRYANT,
                device_id=device_id,
                channel_id=SYSTEM_CHANNEL,
                kind="system",
                label="HVAC system (outdoor_temp_f, mode, stage, blower_rpm)",
                details={
                    "mode": status.mode if status else None,
                    "outdoor_temp": status.outdoor_temp if status else None,
                    "cfgem": status.temp_unit if status else None,
                    "odu.opstat": status.odu_opstat if status else None,
                },
            )
        )

        if status is not None:
            for zone in sorted(status.zones, key=lambda z: z.sort_key):
                channels.append(
                    LiveChannel(
                        source=SOURCE_BRYANT,
                        device_id=device_id,
                        channel_id=zone_channel_id(zone.zone_id),
                        kind="zone",
                        label=zone.name,
                        details={
                            "zone": zone.zone_id,
                            "enabled": zone.enabled,
                            "rt": zone.indoor_temp,
                            "rh": zone.humidity_pct,
                            "htsp": zone.setpoint_heat,
                            "clsp": zone.setpoint_cool,
                            "fan": zone.fan,
                        },
                        mappable=zone.enabled,
                        skip_reason=None if zone.enabled else _SKIP_ZONE_DISABLED,
                    )
                )
            facts.update(_unverified_facts(status, recorder))
        else:
            # No raw payload (an injected stub): the enabled zones from the
            # discovery result are still authoritative, we just cannot show the
            # phantom ones or the UNVERIFIED evidence.
            for channel in discovery.channels:
                if channel.kind == "zone":
                    channels.append(
                        LiveChannel(
                            source=channel.source,
                            device_id=channel.device_id,
                            channel_id=channel.channel_id,
                            kind="zone",
                            label=channel.label,
                            details=dict(channel.details),
                        )
                    )

        facts["zones_enabled"] = sum(
            1 for c in channels if c.kind == "zone" and c.mappable
        )
        facts["zones_reported"] = sum(1 for c in channels if c.kind == "zone")

        if capture_energy and recorder is not None:
            await _probe_daily_energy(recorder, serial=device_id, facts=facts)

        raw: dict[str, Any] = {}
        if recorder is not None:
            raw["graphql"] = list(recorder.records)

        report = SourceReport(
            name=SOURCE_BRYANT,
            ok=True,
            devices=tuple(discovery.devices),
            channels=tuple(channels),
            facts=facts,
        )
        return report, raw
    finally:
        if owns:
            await _close_quietly(source)
            if recorder is not None:
                # BryantStatusSource never closes a client it was handed.
                await _close_quietly(recorder)


async def _probe_daily_energy(
    recorder: _RecordingGraphQLClient, *, serial: str, facts: dict[str, Any]
) -> None:
    """Issue ``getInfinityEnergy`` once, purely so the dump records it.

    Only in dump mode, and only ever one extra request. DEVIATIONS.md #75.8
    lists open questions about this query that a single captured response
    answers: that it still resolves with the field set ``stages/daily.py`` asks
    for, whether ``energyPeriods`` values are numbers or strings, whether
    ``energyConfig.<name>.enabled`` is a boolean or a string, and whether this
    system's ``gas`` component is really disabled. The response is not parsed
    here — the recorder already has it, and interpreting it is the daily
    stage's job.
    """
    try:
        from energy_capture.stages import daily
    except Exception as exc:  # noqa: BLE001 - a probe never breaks discover
        facts["energy_probe"] = scrub_text(f"unavailable: {exc}")
        return
    try:
        await daily.fetch_energy(serial=serial, client=recorder)
    except Exception as exc:  # noqa: BLE001 - the raw record is the point
        facts["energy_probe"] = scrub_text(f"{type(exc).__name__}: {exc}")
    else:
        facts["energy_probe"] = "ok"


def _device_id_of(discovery: Discovery, *, fallback: str) -> str:
    for device in discovery.devices:
        if device.device_id:
            return device.device_id
    for channel in discovery.channels:
        if channel.device_id:
            return channel.device_id
    return str(fallback or "")


def _status_payload(
    recorder: _RecordingGraphQLClient, *, serial: str
) -> Mapping[str, Any] | None:
    """Recover the ``InfinityStatus`` object from what the recorder saw.

    Both operations are handled because ``sources/bryant.py`` may fall back from
    ``infinityStatus(serial:)`` to ``infinitySystems(userName:)`` mid-run
    (DEVIATIONS.md #64). The newest matching response wins.
    """
    for record in reversed(recorder.records):
        data = record.get("data")
        if not isinstance(data, Mapping):
            continue
        status = data.get("infinityStatus")
        if isinstance(status, Mapping) and status:
            return status
        systems = data.get("infinitySystems")
        if isinstance(systems, Sequence) and not isinstance(systems, (str, bytes)):
            for system in systems:
                if not isinstance(system, Mapping):
                    continue
                profile = system.get("profile")
                profile = profile if isinstance(profile, Mapping) else {}
                if str(profile.get("serial") or "").strip() == serial:
                    candidate = system.get("status")
                    if isinstance(candidate, Mapping) and candidate:
                        return candidate
    return None


def _unverified_facts(
    status: Any, recorder: _RecordingGraphQLClient | None
) -> dict[str, Any]:
    """The evidence PLAN.md §7.3 / DEVIATIONS.md #75 want from a live run.

    ``odu_opstat_numeric`` is the one to read first: a numeric ``opstat`` means
    this outdoor unit reports a capacity **percentage**, the ``stage`` metric
    will emit nothing (DEVIATIONS.md #59), and a new metric has to be designed
    before any history is worth collecting.
    """
    from energy_capture.sources.bryant import OPERATION_STATUS

    opstat = status.stage_raw
    facts: dict[str, Any] = {
        "cfgem": status.temp_unit,
        "mode": status.mode,
        "odu_opstat": opstat,
        "odu_opstat_numeric": bool(opstat is not None and _is_number(opstat)),
        "disconnected": status.disconnected,
        "server_utc_time": status.server_utc_time,
    }
    if recorder is not None:
        attempted = [r.get("operation") for r in recorder.records]
        facts["infinity_status_resolved"] = any(
            r.get("operation") == OPERATION_STATUS and "data" in r
            and isinstance(r.get("data"), Mapping)
            and isinstance(r["data"].get("infinityStatus"), Mapping)
            and r["data"]["infinityStatus"]
            for r in recorder.records
        )
        facts["operations_attempted"] = attempted
        for record in recorder.records:
            data = record.get("data")
            if not isinstance(data, Mapping):
                continue
            systems = data.get("infinitySystems")
            if isinstance(systems, Sequence) and not isinstance(systems, (str, bytes)):
                for system in systems:
                    if isinstance(system, Mapping) and isinstance(
                        system.get("profile"), Mapping
                    ):
                        facts["profile"] = _jsonable(system["profile"])
    return facts


def _is_number(text: str) -> bool:
    """True when an enum-shaped field actually holds a number."""
    try:
        float(text)
    except (TypeError, ValueError):
        return False
    return True


async def _close_quietly(closeable: Any) -> None:
    close = getattr(closeable, "close", None)
    if close is None:
        return
    try:
        result = close()
        if asyncio.iscoroutine(result):
            await result
    except Exception as exc:  # noqa: BLE001 - shutdown must not mask the report
        _LOG.debug("discover_close_failed", error=scrub_text(str(exc)))


# ============================================================================
# Rendering
# ============================================================================

#: Per-kind table layout: ``(header, details-key or attribute)``. ``MAPPED`` and
#: ``NOTE`` are appended to every table by :func:`_channel_table`.
_COLUMNS: Final[Mapping[str, tuple[tuple[str, str], ...]]] = {
    "breaker": (
        ("POS", "position"),
        ("CHANNEL_ID", "@channel_id"),
        ("NAME", "@label"),
        ("MODEL", "model"),
        ("BRANCH", "branchType"),
        ("POLES", "poles"),
        ("CONN", "connected"),
        ("WATTS", "watts"),
    ),
    "ct": (
        ("CH", "channel"),
        ("CHANNEL_ID", "@channel_id"),
        ("NAME", "@label"),
        ("USAGE", "usageType"),
        ("LEG", "leg"),
        ("CONN", "connected"),
        ("WATTS", "watts"),
    ),
    "panel_leg": (
        ("CHANNEL_ID", "@channel_id"),
        ("LEG", "leg"),
        ("VOLTS", "volts"),
        ("HZ", "hz"),
        ("CONN", "connected"),
    ),
    "zone": (
        ("ZONE", "zone"),
        ("CHANNEL_ID", "@channel_id"),
        ("NAME", "@label"),
        ("ENABLED", "enabled"),
        ("RT", "rt"),
        ("RH", "rh"),
        ("HTSP", "htsp"),
        ("CLSP", "clsp"),
        ("FAN", "fan"),
    ),
    "system": (
        ("CHANNEL_ID", "@channel_id"),
        ("MODE", "mode"),
        ("OAT", "outdoor_temp"),
        ("CFGEM", "cfgem"),
        ("ODU.OPSTAT", "odu.opstat"),
    ),
}

_KIND_TITLES: Final[Mapping[str, str]] = {
    "breaker": "breakers",
    "ct": "CTs (one IotCt object = one clamp pair, leg A / leg B)",
    "panel_leg": "panel legs (hub-level volts/hz)",
    "zone": "zones",
    "system": "system channel",
}

_KIND_ORDER: Final[tuple[str, ...]] = ("system", "zone", "breaker", "ct", "panel_leg")

_SOURCE_TITLES: Final[Mapping[str, str]] = {
    SOURCE_LEVITON: "Leviton LWHEM-2 (my.leviton.com)",
    SOURCE_BRYANT: "Bryant Evolution / Carrier Infinity cloud",
}


def render_report(result: DiscoverResult) -> str:
    """The human table: one section per source, then the skeleton (PLAN.md §9)."""
    lines: list[str] = []
    lines.append(f"energycap discover — {result.generated_utc.isoformat(timespec='seconds')}")
    lines.extend(_map_lines(result.channel_map))
    for report in result.reports:
        lines.append("")
        lines.extend(_render_source(report, result))
    lines.append("")
    lines.extend(_render_skeleton(result))
    lines.append("")
    lines.extend(_render_footer(result))
    return "\n".join(lines) + "\n"


def _map_lines(channel_map: ChannelMap) -> list[str]:
    if channel_map.error:
        return [
            (
                f"channel_map: {channel_map.path} COULD NOT BE READ "
                f"({channel_map.error}); treating every live channel as unmapped"
            )
        ]
    if not channel_map.exists:
        return [
            (
                f"channel_map: {channel_map.path} does not exist yet — "
                "this run is how you write the first one"
            )
        ]
    suffix = f", {channel_map.malformed} unusable" if channel_map.malformed else ""
    lines = [f"channel_map: {channel_map.path} — {channel_map.entries} mapping(s){suffix}"]
    if channel_map.placeholders:
        lines.append(
            f"  {channel_map.placeholders} of them still carry the "
            f"{PLACEHOLDER_TOKEN} token: paste the real device_id printed below "
            'into each one and delete its "placeholder": true line '
            "(build-dim leaves them out of dim_channel until you do)."
        )
    return lines


def _render_source(report: SourceReport, result: DiscoverResult) -> list[str]:
    """One section per source, grouped **per device**.

    Grouping matters: two Leviton hubs both have a ``panel_leg_a``, and a table
    that merged them would show the same channel id twice with no way to tell
    which panel it belongs to.
    """
    title = _SOURCE_TITLES.get(report.name, report.name)
    lines = [f"=== {title} ==="]
    if not report.ok:
        lines.append(f"  UNAVAILABLE: {report.error}")
        lines.append(
            "  (every other source is unaffected — a cloud that is down never "
            "blocks the rest of the report)"
        )
        return lines

    by_device: dict[str, list[LiveChannel]] = {}
    for channel in report.channels:
        by_device.setdefault(channel.device_id, []).append(channel)

    devices = list(report.devices)
    known = {device.device_id for device in devices}
    for device_id in by_device:
        if device_id not in known:
            devices.append(
                DiscoveredDevice(source=report.name, device_id=device_id, kind="device")
            )

    for device in devices:
        lines.append("")
        lines.append(_device_line(device))
        for kind, group in _grouped_by_kind(by_device.get(device.device_id, ())):
            lines.append(f"    {_KIND_TITLES.get(kind, kind)}:")
            lines.extend(_indent(_channel_table(kind, group, result), width=6))

    if report.facts:
        lines.append("")
        lines.append("  observed: " + _facts_text(report.facts))
    return lines


def _device_line(device: DiscoveredDevice) -> str:
    """``hub 1000_AAAA_1111 "Main Panel" — connected=yes, version=2.2.0, …``"""
    label = f' "{device.label}"' if device.label else ""
    details = ", ".join(f"{k}={_fmt(v)}" for k, v in dict(device.details).items())
    head = f"  {device.kind} {device.device_id}{label}"
    return f"{head} — {details}" if details else head


def _grouped_by_kind(
    channels: Iterable[LiveChannel],
) -> list[tuple[str, list[LiveChannel]]]:
    by_kind: dict[str, list[LiveChannel]] = {}
    for channel in channels:
        by_kind.setdefault(channel.kind, []).append(channel)
    order = (*_KIND_ORDER, *sorted(set(by_kind) - set(_KIND_ORDER)))
    return [(kind, by_kind[kind]) for kind in order if by_kind.get(kind)]


def _channel_table(
    kind: str, channels: Sequence[LiveChannel], result: DiscoverResult
) -> list[str]:
    spec = _COLUMNS.get(kind, (("CHANNEL_ID", "@channel_id"), ("NAME", "@label")))
    headers = [header for header, _ in spec] + ["MAPPED", "NOTE"]
    rows: list[list[str]] = []
    for channel in sorted(channels, key=_channel_sort_key):
        row = [_fmt(_column_value(channel, key)) for _, key in spec]
        if not channel.mappable:
            mapped = "-"
        else:
            mapped = "yes" if result.is_mapped(channel) else "NO"
        note = channel.skip_reason or channel.note or ""
        rows.append([*row, mapped, note])
    return _table(headers, rows)


def _column_value(channel: LiveChannel, key: str) -> Any:
    if key.startswith("@"):
        attribute = key[1:]
        value = getattr(channel, attribute, None)
        if attribute == "channel_id" and not channel.mappable:
            # A skipped object has a channel_id only in principle: no row will
            # ever carry it, so showing it as a live channel would be a lie.
            return f"({value})"
        return value
    return dict(channel.details).get(key)


def _render_skeleton(result: DiscoverResult) -> list[str]:
    unmapped = result.unmapped
    lines = [f"=== channel_map.json skeleton — {len(unmapped)} unmapped channel(s) ==="]
    if not unmapped:
        lines.append("  Everything live is already mapped. Run: energycap build-dim")
        return lines
    lines.append(f"  Paste into {result.channel_map.path} under \"mappings\", set")
    lines.append("  blackstart_device_id (label/panel/slots/category then come from")
    lines.append("  montfort.json — delete the empty \"label\" to let it win), then run:")
    lines.append("      energycap build-dim")
    lines.append("")
    # Flush left, so the block can be selected and pasted as-is.
    lines.extend(json.dumps(skeleton_document(result), indent=2).splitlines())
    return lines


def _render_footer(result: DiscoverResult) -> list[str]:
    lines = ["=== next ==="]
    if result.live_channels_path is not None:
        lines.append(
            f"  live channel list written to {result.live_channels_path} — "
            "build-dim reads it to WARN about unmapped channels without a "
            "second live call."
        )
    else:
        lines.append(
            "  live channel list NOT written (disabled or unwritable); build-dim "
            "will have no live channel set to compare against."
        )
    if result.dump_path is not None:
        lines.append(f"  raw upstream responses written to {result.dump_path} (mode 0600).")
        lines.append(
            "  Commit the interesting payloads as test fixtures — that is how "
            "PLAN.md §7.3's UNVERIFIED fields get settled."
        )
    else:
        lines.append(
            f"  raw capture is OFF. Re-run with {ENV_DUMP}=<file> to record every raw"
        )
        lines.append(
            "  Leviton and Carrier response: whether infinityStatus(serial:) resolves,"
        )
        lines.append(
            "  whether odu.opstat is a word or a percentage, the real zone count and"
        )
        lines.append(
            "  whether cfgem is F are all UNVERIFIED (PLAN.md §7.3, DEVIATIONS.md #75)"
        )
        lines.append("  and a live run is the only cheap chance to capture the evidence.")
    return lines


def _facts_text(facts: Mapping[str, Any]) -> str:
    return ", ".join(f"{key}={_fmt(value)}" for key, value in facts.items())


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    """A left-aligned fixed-width table. Empty trailing columns are dropped."""
    if not rows:
        return ["(none)"]
    columns = len(headers)
    keep = [
        index
        for index in range(columns)
        if headers[index] in ("MAPPED",)
        or any(str(row[index]).strip() for row in rows)
    ]
    widths = [
        max(len(str(headers[index])), *(len(str(row[index])) for row in rows))
        for index in keep
    ]
    out = ["  ".join(str(headers[i]).ljust(w) for i, w in zip(keep, widths)).rstrip()]
    for row in rows:
        out.append(
            "  ".join(str(row[i]).ljust(w) for i, w in zip(keep, widths)).rstrip()
        )
    return out


def _indent(lines: Iterable[str], *, width: int = 2) -> list[str]:
    pad = " " * width
    return [f"{pad}{line}" if line else "" for line in lines]


def _fmt(value: Any) -> str:
    """Display one cell. ``None`` is ``-`` — never ``0``, never blank."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        text = f"{value:.3f}".rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, (list, tuple)):
        return ",".join(_fmt(item) for item in value)
    if isinstance(value, Mapping):
        return ";".join(f"{k}={_fmt(v)}" for k, v in value.items())
    return str(value)


_DIGITS = re.compile(r"(\d+)")


def _channel_sort_key(channel: LiveChannel) -> tuple[Any, ...]:
    """Stable, human order: source, device, kind, then natural channel id."""
    kind_rank = (
        _KIND_ORDER.index(channel.kind) if channel.kind in _KIND_ORDER else len(_KIND_ORDER)
    )
    parts = tuple(
        (int(part), "") if part.isdigit() else (1 << 30, part)
        for part in _DIGITS.split(channel.channel_id)
        if part
    )
    return (channel.source, channel.device_id, kind_rank, parts, channel.channel_id)


# ============================================================================
# Machine-readable output
# ============================================================================


def skeleton_document(result: DiscoverResult) -> dict[str, Any]:
    """``{"mappings": [...]}`` for every unmapped live channel (PLAN.md §9).

    Exactly the shape ``config/channel_map.json`` holds, so the block can be
    pasted whole into an empty file or merged into an existing ``mappings``
    array. Skipped objects never appear: a ``NONE-2`` placeholder has no rows to
    label.
    """
    return {"mappings": [channel.channel_map_entry() for channel in result.unmapped]}


def live_channels_document(result: DiscoverResult) -> dict[str, Any]:
    """The sidecar ``build-dim`` reads (PLAN.md §9's "never silently absent").

    Consumer contract — ``stages/dim.py``'s :func:`load_live_channels` reads
    ``channels[]`` and treats every entry as a live channel, so:

    * **``channels[]`` holds only channels the collectors actually emit rows
      for.** Objects the pipeline deliberately skips (``NONE-2`` placeholders,
      ``NOT_USED`` CTs, phantom zones) are in ``skipped_channels[]`` instead —
      warning that a dumb breaker is unmapped would train the operator to
      ignore the warning that matters.
    * ``sources.<name>.ok`` says whether that cloud answered. **A source that is
      not ``ok`` contributed no channels**, so its absence must not be read as
      "those channels are gone" — do not delete dim rows on the strength of it.
      (A run where *no* source answered writes no file at all.)
    * ``generated_utc`` is when the live call happened; a stale file is a stale
      picture of the panel, not an error.
    """
    return {
        "version": LIVE_CHANNELS_VERSION,
        "generated_utc": result.generated_utc.isoformat(),
        "map_path": str(result.channel_map.path),
        "sources": {
            report.name: {
                "ok": report.ok,
                "error": report.error,
                "devices": len(report.devices),
                "channels": len(report.mappable),
                "channels_skipped": len(report.channels) - len(report.mappable),
                **({"facts": _jsonable(report.facts)} if report.facts else {}),
            }
            for report in result.reports
        },
        "devices": [
            {
                "source": device.source,
                "device_id": device.device_id,
                "kind": device.kind,
                "label": device.label,
                "details": _jsonable(device.details),
            }
            for device in result.devices
        ],
        "channels": [
            channel.to_json(mapped=result.is_mapped(channel))
            for channel in sorted(result.channels, key=_channel_sort_key)
            if channel.mappable
        ],
        "skipped_channels": [
            channel.to_json(mapped=False)
            for channel in sorted(result.channels, key=_channel_sort_key)
            if not channel.mappable
        ],
        "unmapped": [
            {
                "source": channel.source,
                "device_id": channel.device_id,
                "channel_id": channel.channel_id,
            }
            for channel in result.unmapped
        ],
        "skeleton": skeleton_document(result),
    }


def _jsonable(value: Any) -> Any:
    """Coerce a details mapping into something ``json.dumps`` accepts."""
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _write_json(path: Path, payload: Mapping[str, Any], *, mode: int = 0o644) -> None:
    """Write JSON atomically (temp file → ``os.replace``) at ``mode``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# ============================================================================
# The stage entry point
# ============================================================================


def run(
    *,
    sources: tuple[str, ...] | None = None,
    map_path: Path = Path("config/channel_map.json"),
    json_only: bool = False,
    dump_path: Path | str | None = None,
    raw: bool = False,
    out_path: Path | str | None = None,
    write_live_channels: bool = True,
    settings: Settings | None = None,
    leviton_source: Any = None,
    bryant_source: Any = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> dict[str, Any]:
    """Enumerate the live hierarchy and print the mapping stub (PLAN.md §9).

    Args:
        sources: limit to these source names; ``None`` means all of
            :data:`SUPPORTED_SOURCES`.
        map_path: the committed ``channel_map.json`` to compare against.
        json_only: print **only** the paste-ready skeleton JSON on stdout (the
            table, the warnings and the file notices go to stderr, so stdout
            stays a single parseable document).
        dump_path / raw: write every raw upstream response to a file — see
            :ref:`discover-raw`. ``raw=True`` without a path uses a timestamped
            default. Both default from ``ENERGYCAP_DISCOVER_DUMP``.
        out_path / write_live_channels: where (and whether) to write the
            machine-readable sidecar ``build-dim`` reads — see
            :func:`live_channels_document`. Default from
            ``ENERGYCAP_DISCOVER_OUT`` / ``ENERGYCAP_DISCOVER_NO_WRITE``.
        leviton_source / bryant_source: injected sources (tests only).
        stdout / stderr: output streams (tests only).

    Returns:
        The mapping ``cli._run_stage`` folds into its ``stage_ok`` line.

    Raises:
        DiscoveryFailed: every requested source failed — **after** the report
            has been printed, so the operator sees the errors in context.
        ValueError: an unknown source name.
    """
    return asyncio.run(
        _run_async(
            sources=sources,
            map_path=Path(map_path),
            json_only=json_only,
            dump_path=dump_path,
            raw=raw,
            out_path=out_path,
            write_live_channels=write_live_channels,
            settings=settings,
            leviton_source=leviton_source,
            bryant_source=bryant_source,
            stdout=stdout,
            stderr=stderr,
        )
    )


async def _run_async(
    *,
    sources: tuple[str, ...] | None,
    map_path: Path,
    json_only: bool,
    dump_path: Path | str | None,
    raw: bool,
    out_path: Path | str | None,
    write_live_channels: bool,
    settings: Settings | None,
    leviton_source: Any,
    bryant_source: Any,
    stdout: TextIO | None,
    stderr: TextIO | None,
) -> dict[str, Any]:
    resolved = settings if settings is not None else get_settings()
    wanted = _resolve_sources(sources)
    generated = now_utc()
    channel_map = read_channel_map(map_path)

    # Resolved before collection: in dump mode the Bryant pass issues one extra
    # (recorded) query so the daily-energy shape gets captured too.
    dump_target = _resolve_dump_path(dump_path, raw=raw, generated=generated)

    collectors: dict[str, Callable[[], Any]] = {
        SOURCE_LEVITON: lambda: _collect_leviton(resolved, source=leviton_source),
        SOURCE_BRYANT: lambda: _collect_bryant(
            resolved, source=bryant_source, capture_energy=dump_target is not None
        ),
    }

    reports: list[SourceReport] = []
    raw_payloads: dict[str, Any] = {}
    for name in wanted:
        try:
            report, payload = await collectors[name]()
        except Exception as exc:  # noqa: BLE001 — a down cloud is not a crash
            message = scrub_text(f"{type(exc).__name__}: {exc}")
            _LOG.warning("discover_source_failed", source=name, error=message)
            reports.append(SourceReport(name=name, ok=False, error=message))
            continue
        raw_payloads[name] = payload
        reports.append(report)
        _LOG.info(
            "discover_source_ok",
            source=name,
            devices=len(report.devices),
            channels=len(report.mappable),
            channels_skipped=len(report.channels) - len(report.mappable),
        )

    result = DiscoverResult(
        generated_utc=generated,
        channel_map=channel_map,
        reports=tuple(reports),
        raw=raw_payloads,
    )

    # ---------------------------------------------------------------- files
    notices: list[str] = []
    any_ok = any(report.ok for report in result.reports)
    live_path = _resolve_out_path(out_path, map_path=map_path)
    if not write_live_channels or _env_flag(ENV_NO_WRITE):
        pass  # explicitly disabled: a strictly read-only run
    elif not any_ok:
        # Every cloud was down. A sidecar saying "no live channels" would tell
        # build-dim the panel is empty, which is the opposite of the truth; the
        # previous file (if any) is a better picture than this run.
        notices.append(
            f"not writing {live_path}: no source could be enumerated, so this run "
            "knows nothing about what is live"
        )
    else:
        try:
            _write_json(live_path, live_channels_document(result))
        except OSError as exc:
            notices.append(f"could not write {live_path}: {scrub_text(str(exc))}")
            _LOG.warning("discover_live_channels_write_failed", path=str(live_path))
        else:
            result = replace(result, live_channels_path=live_path)

    if dump_target is not None:
        document = {
            "generated_utc": generated.isoformat(),
            "note": (
                "Raw upstream responses captured by `energycap discover`. Commit the "
                "interesting payloads as test fixtures; PLAN.md §7.3 and "
                "DEVIATIONS.md #75 list what they settle."
            ),
            "sources": _jsonable(raw_payloads),
        }
        try:
            _write_json(dump_target, document, mode=0o600)
        except OSError as exc:
            notices.append(f"could not write {dump_target}: {scrub_text(str(exc))}")
            _LOG.warning("discover_dump_write_failed", path=str(dump_target))
        else:
            result = replace(result, dump_path=dump_target)
            _LOG.info("discover_raw_dump", path=str(dump_target))

    # -------------------------------------------------------------- output
    out = stdout if stdout is not None else _stdout()
    err = stderr if stderr is not None else _stderr()
    report_text = render_report(result)
    if json_only:
        # stdout stays a single parseable document; everything else is stderr.
        print(report_text, file=err, end="")
        print(json.dumps(skeleton_document(result), indent=2), file=out)
    else:
        print(report_text, file=out, end="")
    for notice in notices:
        print(f"WARNING: {notice}", file=err)

    summary = result.summary()
    if result.reports and not any(report.ok for report in result.reports):
        raise DiscoveryFailed(
            "no source could be enumerated: "
            + "; ".join(f"{r.name}: {r.error}" for r in result.reports)
        )
    return summary


def _resolve_sources(sources: tuple[str, ...] | None) -> tuple[str, ...]:
    if not sources:
        return SUPPORTED_SOURCES
    wanted: list[str] = []
    for name in sources:
        key = str(name).strip().lower()
        if key not in SUPPORTED_SOURCES:
            raise ValueError(
                f"unknown source {name!r}; discover supports "
                f"{', '.join(SUPPORTED_SOURCES)}"
            )
        if key not in wanted:
            wanted.append(key)
    return tuple(wanted)


def _resolve_out_path(out_path: Path | str | None, *, map_path: Path) -> Path:
    if out_path is not None:
        return Path(out_path)
    from_env = os.environ.get(ENV_LIVE_CHANNELS, "").strip()
    if from_env:
        return Path(from_env)
    return live_channels_path(map_path)


def _resolve_dump_path(
    dump_path: Path | str | None, *, raw: bool, generated: datetime
) -> Path | None:
    if dump_path is not None:
        return Path(dump_path)
    from_env = os.environ.get(ENV_DUMP, "").strip()
    if from_env and from_env.lower() not in {"1", "true", "yes", "on"}:
        return Path(from_env)
    if raw or (from_env and from_env.lower() in {"1", "true", "yes", "on"}):
        stamp = generated.strftime("%Y%m%dT%H%M%SZ")
        return Path(f"discover-raw-{stamp}.json")
    return None


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _stdout() -> TextIO:
    """Resolved per call, so ``capsys``/redirection in a test is honoured."""
    return sys.stdout


def _stderr() -> TextIO:
    return sys.stderr
