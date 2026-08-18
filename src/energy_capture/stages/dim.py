"""The semantic layer: ``channel_map.json`` + blackstart -> ``dim_channel``.

PLAN.md §9. This is the stage that makes an LLM answer useful: without it a
query returns ``breaker_p11 = 412 W`` and nobody can tell whether that is the
sump pump or the dryer. Every README/Glue example query joins through the table
this module writes.

Why a hand-maintained map exists at all
---------------------------------------
Nothing in Leviton's cloud identifiers can auto-join to the blackstart panel
inventory — verified in PLAN.md §9: ``montfort.json`` holds no serial numbers and
no channel ids. The **only** linkage between the two worlds is a breaker's
``position`` (which is what ``channel_id`` is built from, PLAN.md §6.5) matching
a blackstart device's ``slots``. So a human writes the correspondence down once,
in ``config/channel_map.json``, and this stage joins it.

The two legal shapes of a mapping entry
---------------------------------------
::

    {"source": "leviton", "device_id": "<hub>", "channel_id": "breaker_p11",
     "blackstart_device_id": "A-11"}                       # inherit everything

    {"source": "bryant",  "device_id": "4022W200213", "channel_id": "hpheat",
     "label": "HVAC — heat pump heating", "category": "hvac"}   # explicit only

and the hybrid, where explicit fields **override** what the inventory says:

    {"source": "leviton", "device_id": "<hub-b>", "channel_id": "ct_1_a",
     "blackstart_device_id": "B-6-8", "label": "HVAC subpanel feeder (leg A)"}

The rules, all of them enforced here and each with its own error message:

* ``blackstart_device_id`` set -> ``label`` / ``short_label`` / ``panel`` /
  ``slots`` / ``category`` / ``priority`` / ``estimated_watts`` / ``room`` are
  pulled from ``montfort.json``. **Blackstart stays the source of truth for
  labels** — a circuit renamed in the panel inventory renames itself here.
* an explicit field in ``channel_map.json`` wins over the inventory.
* an entry with **neither** a ``blackstart_device_id`` nor any explicit field is
  a build error naming the offending entry — a dim row with no label is worse
  than no row, because it looks like the channel was documented.
* a ``blackstart_device_id`` that does not exist in the inventory is a build
  error too (with the near-miss ids from the same panel, since the usual cause
  is ``A-1-3`` vs ``A-13``).

Placeholders
------------
Leviton hub ids are the panel serial numbers and are unknowable until
``energycap discover`` runs against the live panels. Rather than invent them,
``config/channel_map.json`` ships entries flagged ``"placeholder": true`` whose
``device_id`` carries the literal token ``PLACEHOLDER``. They document the exact
format — and they are **excluded from ``dim_channel.parquet``** and reported at
WARN with the remedy, so nothing downstream can mistake documentation for data.
The flag and the token must agree in both directions; either one alone is a
build error that spells out what to do.

Unmapped live channels never vanish
-----------------------------------
PLAN.md §9: a live channel that nobody has mapped appears in ``discover`` output
**and** as a WARN here. ``build()`` takes the live channel list as an argument
(``live_channels=`` or ``live_channels_path=``), so the WARN needs no live call:
``energycap discover`` writes ``live_channels.json`` beside the map and this
stage picks it up automatically when no list was passed. Every unmapped
channel is named in the log *and* returned in the summary, and if a PLACEHOLDER
entry already covers that ``(source, channel_id)`` the warning says so.

Determinism
-----------
CLAUDE.md rule 7 applies here as much as to any dated stage: the output key is
fixed (``energy/dim_channel/dim_channel.parquet``), rows are sorted by
:data:`~energy_capture.model.DIM_KEY`, and ``updated_at`` is derived from the
*inputs* (the inventory's ``metadata.lastUpdated``), never from the wall clock —
so a re-run over unchanged inputs produces byte-identical bytes instead of
churning the object every time it is invoked.

Sources
-------
The ``source`` vocabulary is :data:`energy_capture.model.SOURCES`, which already
contains ``lge`` (PLAN.md §13). An LG&E meter channel can be mapped here today
with no code change — deliberately, because §13 says the design must not paint
that dataset into a corner.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
from botocore.client import BaseClient

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import Settings, get_settings
from energy_capture.logging import get_logger

__all__ = [
    "CATEGORY_FOR_CIRCUIT_TYPE",
    "CATEGORY_FOR_ROLE",
    "DIM_COLUMNS",
    "DIM_SCHEMA",
    "ENTRY_KEYS",
    "INHERITABLE_FIELDS",
    "KNOWN_CATEGORIES",
    "PLACEHOLDER_TOKEN",
    "STAGE",
    "ChannelEntry",
    "DimBuildError",
    "DimRow",
    "Inventory",
    "InventoryDevice",
    "build",
    "build_table",
    "default_live_channels_path",
    "load_channel_map",
    "load_inventory",
    "load_live_channels",
    "normalize_category",
    "normalize_slots",
    "resolve_rows",
]

#: Log ``stage`` field.
STAGE = "dim"

log = get_logger(STAGE)


class DimBuildError(RuntimeError):
    """``channel_map.json`` or the inventory cannot be turned into a dim table.

    Always raised with every problem found, not just the first: fixing a mapping
    file one error per run is miserable, and the whole point of this stage is
    that a human maintains the file by hand.
    """


# --------------------------------------------------------------- vocabulary

#: The literal token that marks an identifier as not-yet-known. Entries carrying
#: it are documentation, never data (see the module docstring).
PLACEHOLDER_TOKEN = "PLACEHOLDER"

#: Keys a ``mappings[]`` entry may carry. Anything else is a build error, which
#: is what turns a typo like ``"labell"`` into a loud failure instead of a
#: silently unlabelled channel.
ENTRY_KEYS: frozenset[str] = frozenset(
    {
        "source",
        "device_id",
        "channel_id",
        "blackstart_device_id",
        "label",
        "short_label",
        "panel",
        "slots",
        "category",
        "room",
        "priority",
        "estimated_watts",
        "placeholder",
        # Marks the one channel a whole-system comparison should use when a
        # source exposes several. The account has two LG&E meters — the house
        # and a separately metered barn — and only the house has panel CTs to
        # compare against, which is knowledge that belongs in the map rather
        # than in a heuristic ("the bigger one is the house") in code.
        "primary",
        "updated_at",
        # Free-form human documentation. Never written to the Parquet file — it
        # is for whoever edits the map next, not for a query engine.
        "notes",
    }
)

#: Fields inherited from ``montfort.json`` when ``blackstart_device_id`` is set,
#: and overridable by spelling them out in the entry (PLAN.md §9).
INHERITABLE_FIELDS: tuple[str, ...] = (
    "label",
    "short_label",
    "panel",
    "slots",
    "category",
    "room",
    "priority",
    "estimated_watts",
)

#: blackstart ``circuitType`` -> normalized ``category``. The inventory's strings
#: are prose ("MWBC (two 120V legs sharing one neutral, one common-trip
#: handle)"); these are the query-friendly forms. Matching is on the leading
#: token so a parenthesised explanation can be edited upstream without breaking
#: the join.
CATEGORY_FOR_CIRCUIT_TYPE: dict[str, str] = {
    "120v branch": "branch_120v",
    "240v appliance": "appliance_240v",
    "240v device": "device_240v",
    "mwbc": "mwbc",
    "backup-feed": "backup_feed",
    "bus tap / feed-through lug": "feed_through",
}

#: blackstart ``role`` -> normalized ``category``, used when ``circuitType`` is
#: absent or unrecognised.
CATEGORY_FOR_ROLE: dict[str, str] = {
    "branch": "branch",
    "feedthrough": "feed_through",
    "generatorinlet": "backup_feed",
}

#: Categories this project has agreed on. An entry may use a category outside
#: this set — it is normalized and kept, with a WARN — because inventing a
#: registry that blocks a new kind of channel would be worse than a noisy log.
KNOWN_CATEGORIES: frozenset[str] = frozenset(
    set(CATEGORY_FOR_CIRCUIT_TYPE.values())
    | set(CATEGORY_FOR_ROLE.values())
    | {
        "hvac",  # PLAN.md §9's own example category
        "hvac_status",  # the 30s Bryant thermostat/system channels (§7.3)
        "panel",  # hub-level volts/hz legs (§6.5)
        "meter",  # utility interval meters (§13)
    }
)

#: Schema of ``energy/dim_channel/dim_channel.parquet`` (PLAN.md §9's column
#: list, in its order). Nullable everywhere the answer can honestly be "not
#: recorded" — a dim table must never guess a room or a wattage.
DIM_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("device_id", pa.string(), nullable=False),
        pa.field("channel_id", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("short_label", pa.string(), nullable=False),
        pa.field("panel", pa.string(), nullable=True),
        # Rendered as the documented string form, e.g. "1,3" for a 2-pole
        # breaker and "11" for a single-pole one.
        pa.field("slots", pa.string(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("room", pa.string(), nullable=True),
        pa.field("priority", pa.string(), nullable=True),
        pa.field("estimated_watts", pa.float64(), nullable=True),
        pa.field("blackstart_device_id", pa.string(), nullable=True),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

#: Column order of :data:`DIM_SCHEMA`.
DIM_COLUMNS: tuple[str, ...] = tuple(field.name for field in DIM_SCHEMA)


# ------------------------------------------------------------------- parsing


def _text(value: Any) -> str | None:
    """A trimmed non-empty string, or ``None``. Never the string ``"None"``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return str(value)
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed or trimmed.lower() == "none":
        return None
    return trimmed


def _looks_like_placeholder(*values: str | None) -> bool:
    return any(PLACEHOLDER_TOKEN in (value or "").upper() for value in values)


def normalize_slots(value: Any) -> str | None:
    """Render a slot list as the documented string, e.g. ``[1, 3]`` -> ``"1,3"``.

    Accepts what the inventory holds (a list of ints) and what a human might
    type into ``channel_map.json`` (``"1,3"``, ``[1, 3]``, ``11``). Order is
    preserved exactly as recorded — the first slot is the breaker ``position``
    that ``channel_id`` is built from (PLAN.md §6.5), so re-sorting it would
    quietly break the only join that exists between the two worlds.
    """
    if value is None:
        return None
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = [str(part).strip() for part in value]
    elif isinstance(value, int) and not isinstance(value, bool):
        parts = [str(value)]
    else:
        raise ValueError(f"slots must be a list or a comma-separated string, got {value!r}")
    kept = [part for part in parts if part and part.lower() != "none"]
    return ",".join(kept) or None


def normalize_category(value: Any) -> str | None:
    """Lowercase, whitespace/dash -> underscore. ``"240V appliance"`` is not a
    column value anybody wants to type into a ``WHERE`` clause."""
    text = _text(value)
    if text is None:
        return None
    return "_".join(text.lower().replace("-", " ").replace("/", " ").split())


def _number(value: Any, *, what: str) -> float | None:
    """A finite number, or ``None``. Raises for a value that is not numeric."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{what} must be a number, got {value!r}")
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return None
        try:
            number = float(text)
        except ValueError:
            raise ValueError(f"{what} must be a number, got {value!r}") from None
    else:
        raise ValueError(f"{what} must be a number, got {value!r}")
    if math.isnan(number) or math.isinf(number):
        raise ValueError(f"{what} must be a finite number, got {value!r}")
    return number


def _timestamp(value: Any, *, what: str) -> datetime | None:
    """An ISO date or datetime -> aware UTC. A bare date means LOCAL midnight."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return timeutil.ensure_utc(value)
    if isinstance(value, date):
        return timeutil.local_midnight_utc(value)
    text = _text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return timeutil.local_midnight_utc(timeutil.parse_local_date(text))
        except (ValueError, TypeError):
            raise ValueError(
                f"{what} must be an ISO-8601 date or datetime, got {value!r}"
            ) from None
    if parsed.tzinfo is None:
        # A naive wall clock is a LOCAL wall clock everywhere in this project.
        return timeutil.local_naive_to_utc(parsed)
    return timeutil.ensure_utc(parsed)


@dataclass(frozen=True, slots=True)
class ChannelEntry:
    """One parsed ``mappings[]`` entry, before the inventory join."""

    index: int
    source: str
    device_id: str
    channel_id: str
    blackstart_device_id: str | None = None
    label: str | None = None
    short_label: str | None = None
    panel: str | None = None
    slots: str | None = None
    category: str | None = None
    room: str | None = None
    priority: str | None = None
    estimated_watts: float | None = None
    placeholder: bool = False
    #: See ``ALLOWED_KEYS``: the channel a whole-system comparison should pick.
    primary: bool = False
    updated_at: datetime | None = None
    notes: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        """:data:`energy_capture.model.DIM_KEY` — the identity of a channel."""
        return (self.source, self.device_id, self.channel_id)

    @property
    def where(self) -> str:
        """How this entry is named in an error message."""
        return (
            f"mappings[{self.index}] "
            f"({self.source}/{self.device_id}/{self.channel_id})"
        )

    def explicit_fields(self) -> tuple[str, ...]:
        """Which inheritable fields this entry states for itself."""
        return tuple(
            name for name in INHERITABLE_FIELDS if getattr(self, name) is not None
        )


def _parse_entry(raw: Any, index: int, errors: list[str]) -> ChannelEntry | None:
    """Parse one entry, appending to ``errors`` rather than raising."""
    where = f"mappings[{index}]"
    if not isinstance(raw, Mapping):
        errors.append(f"{where} is {type(raw).__name__}, expected a JSON object")
        return None

    unknown = sorted(set(raw) - ENTRY_KEYS)
    if unknown:
        errors.append(
            f"{where} has unknown key(s) {unknown}. Allowed: "
            f"{sorted(ENTRY_KEYS)}. (A misspelled field is silently dropped "
            "otherwise, which is how a channel ends up unlabelled.)"
        )

    source = _text(raw.get("source"))
    device_id = _text(raw.get("device_id"))
    channel_id = _text(raw.get("channel_id"))
    missing = [
        name
        for name, value in (
            ("source", source),
            ("device_id", device_id),
            ("channel_id", channel_id),
        )
        if value is None
    ]
    if missing:
        errors.append(f"{where} is missing required key(s) {missing}")
        return None
    assert source is not None and device_id is not None and channel_id is not None

    if source not in model.SOURCES:
        errors.append(
            f"{where} has unknown source {source!r}; expected one of "
            f"{sorted(model.SOURCES)} (model.SOURCES — add a source there, "
            "not here)"
        )
        return None

    flag = raw.get("placeholder", False)
    if not isinstance(flag, bool):
        errors.append(f"{where} 'placeholder' must be true or false, got {flag!r}")
        flag = bool(flag)

    tokened = _looks_like_placeholder(source, device_id, channel_id)
    if tokened and not flag:
        errors.append(
            f"{where} contains the literal token {PLACEHOLDER_TOKEN!r} but is not "
            'flagged. Either (a) run `energycap discover`, paste the real id it '
            "prints into device_id, and remove the token; or (b) add "
            '"placeholder": true so the entry is documentation rather than data '
            "and is left out of dim_channel.parquet."
        )
    if flag and not tokened:
        errors.append(
            f'{where} is flagged "placeholder": true but names no '
            f"{PLACEHOLDER_TOKEN!r} token, so it looks like a real channel that "
            "would be silently dropped from dim_channel.parquet. Remove the flag "
            "if the ids are real."
        )

    try:
        slots = normalize_slots(raw.get("slots"))
        estimated_watts = _number(raw.get("estimated_watts"), what=f"{where} estimated_watts")
        updated_at = _timestamp(raw.get("updated_at"), what=f"{where} updated_at")
    except ValueError as exc:
        errors.append(str(exc))
        return None

    category = normalize_category(raw.get("category"))
    if category is not None and category not in KNOWN_CATEGORIES:
        log.warning(
            "dim_unknown_category",
            entry=where,
            category=category,
            known=sorted(KNOWN_CATEGORIES),
            detail=(
                "kept as written; add it to dim.KNOWN_CATEGORIES if it is a real "
                "category rather than a typo"
            ),
        )

    return ChannelEntry(
        index=index,
        source=source,
        device_id=device_id,
        channel_id=channel_id,
        blackstart_device_id=_text(raw.get("blackstart_device_id")),
        label=_text(raw.get("label")),
        short_label=_text(raw.get("short_label")),
        panel=_text(raw.get("panel")),
        slots=slots,
        category=category,
        room=_text(raw.get("room")),
        priority=_text(raw.get("priority")),
        estimated_watts=estimated_watts,
        placeholder=flag,
        primary=bool(raw.get("primary", False)),
        updated_at=updated_at,
        notes=_text(raw.get("notes")),
    )


def load_channel_map(path: Path | str) -> list[ChannelEntry]:
    """Parse ``config/channel_map.json`` (PLAN.md §9's ``{"mappings": [...]}``).

    Raises:
        DimBuildError: the file is missing, is not JSON, has the wrong shape, or
            any entry is malformed. Every problem in the file is reported at
            once.
    """
    map_path = Path(path)
    try:
        text = map_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise DimBuildError(
            f"channel_map not found at {map_path}. It is hand-maintained and "
            "committed (PLAN.md §9); `energycap discover` prints ready-to-paste "
            "entries for anything unmapped."
        ) from None
    except OSError as exc:
        raise DimBuildError(f"channel_map at {map_path} could not be read: {exc}") from None

    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DimBuildError(f"channel_map at {map_path} is not valid JSON: {exc}") from None

    if not isinstance(document, Mapping) or "mappings" not in document:
        raise DimBuildError(
            f"channel_map at {map_path} must be a JSON object with a 'mappings' "
            'list (PLAN.md §9): {"mappings": [ … ]}'
        )
    raw_entries = document["mappings"]
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, (str, bytes)):
        raise DimBuildError(f"channel_map at {map_path}: 'mappings' must be a list")

    extra_keys = sorted(set(document) - {"mappings"})
    if extra_keys:
        raise DimBuildError(
            f"channel_map at {map_path} has unexpected top-level key(s) "
            f"{extra_keys}; PLAN.md §9 defines exactly one, 'mappings'. Per-entry "
            "'notes' is where human documentation goes."
        )

    errors: list[str] = []
    entries: list[ChannelEntry] = []
    for index, raw in enumerate(raw_entries):
        entry = _parse_entry(raw, index, errors)
        if entry is not None:
            entries.append(entry)

    seen: dict[tuple[str, str, str], ChannelEntry] = {}
    for entry in entries:
        first = seen.get(entry.key)
        if first is not None:
            errors.append(
                f"{entry.where} duplicates {first.where}: "
                f"(source, device_id, channel_id) is the identity of a channel "
                "(model.DIM_KEY) and must appear once"
            )
            continue
        seen[entry.key] = entry

    if errors:
        raise DimBuildError(_problem_report(f"channel_map at {map_path}", errors))
    return entries


def _problem_report(subject: str, errors: Sequence[str]) -> str:
    lines = [f"{subject} has {len(errors)} problem(s):"]
    lines += [f"  {n}. {message}" for n, message in enumerate(errors, start=1)]
    return "\n".join(lines)


# ----------------------------------------------------------------- inventory


@dataclass(frozen=True, slots=True)
class InventoryDevice:
    """One ``devices[]` entry of ``montfort.json`` (schemaVersion 2).

    The real shape, not an invented one::

        {"id": "A-1-3", "panel": "A", "slots": [1, 3], "poles": 2, "amps": 30,
         "role": "branch", "label": "Dryer outlet", "labelShort": …,
         "circuitType": "240V appliance", "priority": null,
         "estimatedWattsTotal": 5000, "circuits": [{"room": …}, …]}
    """

    device_id: str
    panel: str | None
    slots: str | None
    label: str | None
    short_label: str | None
    category: str | None
    room: str | None
    priority: str | None
    estimated_watts: float | None
    role: str | None
    circuit_type: str | None


@dataclass(frozen=True, slots=True)
class Inventory:
    """The parsed blackstart inventory: devices by id, plus its vintage."""

    path: Path
    devices: dict[str, InventoryDevice]
    #: ``metadata.lastUpdated`` as an instant (local midnight UTC), or ``None``.
    updated_at: datetime | None
    schema_version: Any = None

    def get(self, device_id: str) -> InventoryDevice | None:
        return self.devices.get(device_id)

    def near_misses(self, device_id: str, limit: int = 6) -> list[str]:
        """Ids that a typo could plausibly have meant — same panel first."""
        panel = device_id.split("-", 1)[0].upper()
        same_panel = [key for key in self.devices if key.upper().startswith(f"{panel}-")]
        return sorted(same_panel or self.devices)[:limit]


def _device_room(raw: Mapping[str, Any], aliases: Mapping[str, str]) -> str | None:
    """Rooms this device's circuits reach, de-duplicated, in recorded order.

    A breaker routinely feeds several rooms (``A-11`` reaches four), so the
    single ``room`` column of PLAN.md §9 is a join of what the inventory
    recorded, not a guess at "the" room. ``roomAliases`` is applied because the
    inventory keeps the walk-through's raw names ("Second Bedroom on Left")
    alongside the names the household actually uses ("Office").
    """
    circuits = raw.get("circuits")
    if not isinstance(circuits, Sequence) or isinstance(circuits, (str, bytes)):
        return None
    rooms: list[str] = []
    for circuit in circuits:
        if not isinstance(circuit, Mapping):
            continue
        room = _text(circuit.get("room"))
        if room is None:
            continue
        room = aliases.get(room, room)
        if room not in rooms:
            rooms.append(room)
    return ", ".join(rooms) or None


def _device_category(raw: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    """``(category, role, circuit_type)`` — normalized from the inventory.

    ``circuitType`` is tried first (it is the more specific fact), falling back
    to ``role``. The circuit-type match is on the leading token so that the
    inventory's prose tail — "MWBC (two 120V legs sharing one neutral…)" — can be
    reworded upstream without silently emptying this column.
    """
    circuit_type = _text(raw.get("circuitType"))
    role = _text(raw.get("role"))

    if circuit_type is not None:
        lowered = circuit_type.lower()
        for prefix, category in CATEGORY_FOR_CIRCUIT_TYPE.items():
            if lowered.startswith(prefix):
                return category, role, circuit_type
    if role is not None:
        category = CATEGORY_FOR_ROLE.get(role.lower())
        if category is not None:
            return category, role, circuit_type
    if circuit_type is not None or role is not None:
        log.warning(
            "dim_unknown_circuit_type",
            blackstart_device_id=_text(raw.get("id")),
            circuit_type=circuit_type,
            role=role,
            detail=(
                "no normalized category for this blackstart circuitType/role; "
                "add it to dim.CATEGORY_FOR_CIRCUIT_TYPE / CATEGORY_FOR_ROLE"
            ),
        )
    return None, role, circuit_type


def _parse_device(
    raw: Mapping[str, Any], aliases: Mapping[str, str], errors: list[str]
) -> InventoryDevice | None:
    device_id = _text(raw.get("id"))
    if device_id is None:
        errors.append(f"a devices[] entry has no 'id': {sorted(raw)[:6]}")
        return None
    category, role, circuit_type = _device_category(raw)
    try:
        slots = normalize_slots(raw.get("slots"))
    except ValueError as exc:
        errors.append(f"device {device_id}: {exc}")
        slots = None
    try:
        watts = _number(raw.get("estimatedWattsTotal"), what=f"device {device_id} estimatedWattsTotal")
    except ValueError as exc:
        errors.append(str(exc))
        watts = None
    label = _text(raw.get("label"))
    return InventoryDevice(
        device_id=device_id,
        panel=_text(raw.get("panel")),
        slots=slots,
        label=label,
        # The inventory records a short label only where the long one is
        # unwieldy; falling back to the long one keeps `short_label` non-null
        # for every row, which is what a compact UI or an LLM summary wants.
        short_label=_text(raw.get("shortLabel")) or label,
        category=category,
        room=_device_room(raw, aliases),
        priority=_text(raw.get("priority")),
        estimated_watts=watts,
        role=role,
        circuit_type=circuit_type,
    )


def load_inventory(path: Path | str) -> Inventory:
    """Parse ``montfort.json`` (blackstart, schemaVersion 2).

    Only the top-level ``devices[]`` list is joined: a device ``id`` **is**
    PLAN.md §9's ``blackstart_device_id``, and its ``slots`` are what a Leviton
    breaker ``position`` corresponds to. ``subpanels[].devices[]`` are
    deliberately *not* indexed — those breakers live at the air handler, behind a
    feed-through lug, and no Leviton smart breaker can ever meter them (the
    inventory says so itself), so pretending they are joinable would invite a
    mapping that can never produce data.

    Raises:
        DimBuildError: missing file, bad JSON, or a ``devices`` list this code
            cannot read.
    """
    inventory_path = Path(path)
    try:
        text = inventory_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise DimBuildError(
            f"blackstart inventory not found at {inventory_path}. Set "
            "BLACKSTART_INVENTORY_PATH (PLAN.md §9/§14) or pass --inventory-path; "
            "on the Mac it is ~/code/blackstart/data/montfort.json."
        ) from None
    except OSError as exc:
        raise DimBuildError(
            f"blackstart inventory at {inventory_path} could not be read: {exc}"
        ) from None

    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DimBuildError(
            f"blackstart inventory at {inventory_path} is not valid JSON: {exc}"
        ) from None
    if not isinstance(document, Mapping):
        raise DimBuildError(
            f"blackstart inventory at {inventory_path} must be a JSON object"
        )

    schema_version = document.get("schemaVersion")
    if schema_version is not None and str(schema_version).split(".", 1)[0] != "2":
        log.warning(
            "dim_inventory_schema_version",
            path=str(inventory_path),
            schema_version=schema_version,
            detail=(
                "this stage was written against blackstart schemaVersion 2 "
                "(devices[] with id/panel/slots/role/circuitType); check the "
                "join if the shape changed"
            ),
        )

    raw_devices = document.get("devices")
    if not isinstance(raw_devices, Sequence) or isinstance(raw_devices, (str, bytes)):
        raise DimBuildError(
            f"blackstart inventory at {inventory_path} has no 'devices' list "
            "(schemaVersion 2 is device-first: one entry per physical breaker)"
        )

    aliases_raw = document.get("roomAliases")
    aliases: dict[str, str] = {}
    if isinstance(aliases_raw, Mapping):
        for key, value in aliases_raw.items():
            name, friendly = _text(key), _text(value)
            if name is not None and friendly is not None:
                aliases[name] = friendly

    errors: list[str] = []
    devices: dict[str, InventoryDevice] = {}
    for raw in raw_devices:
        if not isinstance(raw, Mapping):
            errors.append(f"a devices[] entry is {type(raw).__name__}, expected an object")
            continue
        device = _parse_device(raw, aliases, errors)
        if device is None:
            continue
        if device.device_id in devices:
            errors.append(
                f"device id {device.device_id!r} appears more than once; ids must "
                "be unique — they are what channel_map.json joins on"
            )
            continue
        devices[device.device_id] = device

    if errors:
        raise DimBuildError(
            _problem_report(f"blackstart inventory at {inventory_path}", errors)
        )

    metadata = document.get("metadata")
    updated_at: datetime | None = None
    if isinstance(metadata, Mapping):
        try:
            updated_at = _timestamp(
                metadata.get("lastUpdated"), what="inventory metadata.lastUpdated"
            )
        except ValueError as exc:
            log.warning("dim_inventory_vintage_unreadable", path=str(inventory_path), error=str(exc))

    log.info(
        "dim_inventory_loaded",
        path=str(inventory_path),
        devices=len(devices),
        schema_version=schema_version,
        last_updated=timeutil.format_utc(updated_at) if updated_at else None,
        room_aliases=len(aliases),
    )
    return Inventory(
        path=inventory_path,
        devices=devices,
        updated_at=updated_at,
        schema_version=schema_version,
    )


# --------------------------------------------------------------- the join


@dataclass(frozen=True, slots=True)
class DimRow:
    """One resolved ``dim_channel`` row."""

    source: str
    device_id: str
    channel_id: str
    label: str
    short_label: str
    panel: str | None
    slots: str | None
    category: str | None
    room: str | None
    priority: str | None
    estimated_watts: float | None
    blackstart_device_id: str | None
    updated_at: datetime

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.source, self.device_id, self.channel_id)


def _resolve_entry(
    entry: ChannelEntry,
    inventory: Inventory | None,
    default_updated_at: datetime,
    errors: list[str],
) -> DimRow | None:
    """Join one entry against the inventory, applying PLAN.md §9's rules."""
    device: InventoryDevice | None = None
    if entry.blackstart_device_id is not None:
        if inventory is None:  # pragma: no cover - build() loads it when needed
            errors.append(
                f"{entry.where} names blackstart_device_id "
                f"{entry.blackstart_device_id!r} but no inventory was loaded"
            )
            return None
        device = inventory.get(entry.blackstart_device_id)
        if device is None:
            errors.append(
                f"{entry.where} names blackstart_device_id "
                f"{entry.blackstart_device_id!r}, which does not exist in "
                f"{inventory.path}. Ids from that panel: "
                f"{inventory.near_misses(entry.blackstart_device_id)}. "
                "(A device id is panel-slot(s), e.g. 'A-11' for one pole and "
                "'A-1-3' for a 2-pole breaker across slots 1 and 3.)"
            )
            return None

    if device is None and not entry.explicit_fields():
        errors.append(
            f"{entry.where} has neither a blackstart_device_id nor any explicit "
            f"field ({', '.join(INHERITABLE_FIELDS)}). PLAN.md §9 requires one or "
            "the other: an entry that describes nothing produces a dim row with "
            "no label, which reads as 'documented' when it is not. Either add "
            '"blackstart_device_id": "<panel>-<slot(s)>" or spell out at least a '
            '"label".'
        )
        return None

    def pick(name: str) -> Any:
        """Explicit field wins; otherwise inherit from blackstart (PLAN.md §9)."""
        explicit = getattr(entry, name)
        if explicit is not None:
            return explicit
        return getattr(device, name) if device is not None else None

    label = pick("label")
    if label is None:
        errors.append(
            f"{entry.where} produces no label: blackstart device "
            f"{entry.blackstart_device_id!r} has none either. Add an explicit "
            '"label" — every dim row must be nameable to a human.'
        )
        return None

    return DimRow(
        source=entry.source,
        device_id=entry.device_id,
        channel_id=entry.channel_id,
        label=label,
        short_label=pick("short_label") or label,
        panel=pick("panel"),
        slots=pick("slots"),
        category=pick("category"),
        room=pick("room"),
        priority=pick("priority"),
        estimated_watts=pick("estimated_watts"),
        blackstart_device_id=entry.blackstart_device_id,
        updated_at=entry.updated_at or default_updated_at,
    )


def resolve_rows(
    entries: Sequence[ChannelEntry],
    inventory: Inventory | None,
    *,
    updated_at: datetime,
    subject: str = "channel_map",
) -> list[DimRow]:
    """Join every non-placeholder entry, or raise naming every bad one.

    Placeholders are dropped here (they are documentation, not data) but are
    still validated against the inventory, so a PLACEHOLDER entry pointing at a
    blackstart id that no longer exists fails the build rather than rotting
    quietly until the day somebody makes it real.
    """
    errors: list[str] = []
    rows: list[DimRow] = []
    for entry in entries:
        row = _resolve_entry(entry, inventory, updated_at, errors)
        if row is not None and not entry.placeholder:
            rows.append(row)
    if errors:
        raise DimBuildError(_problem_report(subject, errors))
    rows.sort(key=lambda row: row.key)
    return rows


def build_table(rows: Sequence[DimRow]) -> pa.Table:
    """Rows -> the ``dim_channel`` Arrow table, sorted by ``model.DIM_KEY``."""
    ordered = sorted(rows, key=lambda row: row.key)
    records = [
        {name: getattr(row, name) for name in DIM_COLUMNS} for row in ordered
    ]
    return pa.Table.from_pylist(records, schema=DIM_SCHEMA)


# ------------------------------------------------------------ live channels


def _coerce_live_channel(raw: Any) -> tuple[str, str, str] | None:
    if isinstance(raw, Mapping):
        source = _text(raw.get("source"))
        device_id = _text(raw.get("device_id"))
        channel_id = _text(raw.get("channel_id"))
    elif isinstance(raw, (list, tuple)) and len(raw) == 3:
        source, device_id, channel_id = (_text(part) for part in raw)
    else:
        raise DimBuildError(
            f"live channel {raw!r} must be a (source, device_id, channel_id) "
            "triple or an object with those keys"
        )
    if source is None or device_id is None or channel_id is None:
        raise DimBuildError(
            f"live channel {raw!r} is missing source, device_id or channel_id"
        )
    return (source, device_id, channel_id)


def load_live_channels(path: Path | str) -> list[tuple[str, str, str]]:
    """Read the live channel list ``energycap discover`` writes.

    Accepts a bare list, ``{"channels": [...]}``, or the ``{"mappings": [...]}``
    shape of ``channel_map.json`` itself — so ``discover``'s ready-to-paste
    skeleton can be fed straight back in without editing. This is the seam that
    lets ``build-dim`` report unmapped channels (PLAN.md §9) **without** making a
    live call: discovery happens in ``discover``, and this stage stays offline
    and idempotent.
    """
    channels_path = Path(path)
    try:
        document = json.loads(channels_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise DimBuildError(f"live channel list not found at {channels_path}") from None
    except json.JSONDecodeError as exc:
        raise DimBuildError(
            f"live channel list at {channels_path} is not valid JSON: {exc}"
        ) from None

    if isinstance(document, Mapping):
        for key in ("channels", "live_channels", "mappings"):
            if key in document:
                document = document[key]
                break
        else:
            raise DimBuildError(
                f"live channel list at {channels_path} must be a list, or an "
                "object with a 'channels' (or 'mappings') list"
            )
    if not isinstance(document, Sequence) or isinstance(document, (str, bytes)):
        raise DimBuildError(f"live channel list at {channels_path} must be a list")

    out: list[tuple[str, str, str]] = []
    for raw in document:
        channel = _coerce_live_channel(raw)
        if channel is not None and channel not in out:
            out.append(channel)
    return out


def default_live_channels_path(map_path: Path | str) -> Path | None:
    """The sidecar ``energycap discover`` writes beside ``channel_map.json``.

    This is the seam that makes PLAN.md §9's promise real without a live call:
    ``discover`` writes ``live_channels.json`` next to the map, and ``build()``
    picks it up automatically, so an operator who runs the two commands in the
    documented order gets the unmapped-channel WARNs with no extra flag.

    The path comes from :mod:`energy_capture.stages.discover` when that module is
    importable, so the two stages cannot drift apart; the filename is duplicated
    only as a fallback, keeping ``build-dim`` runnable if ``discover`` is not.
    Returns ``None`` when no sidecar exists yet — a first build, before anything
    has been discovered, is not an error.
    """
    directory = Path(map_path).parent
    try:  # local import: `discover` is a peer stage, not a dependency
        from energy_capture.stages import discover as _discover

        candidate = _discover.live_channels_path(Path(map_path))
    except Exception:  # pragma: no cover - discover absent or unimportable
        candidate = directory / "live_channels.json"
    return candidate if candidate.is_file() else None


def _report_unmapped(
    live: Sequence[tuple[str, str, str]],
    entries: Sequence[ChannelEntry],
    rows: Sequence[DimRow],
) -> list[dict[str, str]]:
    """WARN once per live channel that no entry covers (PLAN.md §9).

    Returned as well as logged, so the CLI's ``stage_ok`` line and any caller
    can see them — "never silently absent" means absent from *neither* the log
    nor the summary.
    """
    mapped = {row.key for row in rows}
    placeholder_index: dict[tuple[str, str], ChannelEntry] = {
        (entry.source, entry.channel_id): entry
        for entry in entries
        if entry.placeholder
    }
    unmapped: list[dict[str, str]] = []
    for source, device_id, channel_id in live:
        if (source, device_id, channel_id) in mapped:
            continue
        hint = (
            "add an entry to config/channel_map.json — `energycap discover` "
            "prints a ready-to-paste skeleton"
        )
        placeholder = placeholder_index.get((source, channel_id))
        if placeholder is not None:
            hint = (
                f"{placeholder.where} is a PLACEHOLDER for this channel: replace "
                f"its device_id with {device_id!r} and delete "
                '"placeholder": true'
            )
        record = {
            "source": source,
            "device_id": device_id,
            "channel_id": channel_id,
            "remedy": hint,
        }
        unmapped.append(record)
        log.warning("dim_unmapped_live_channel", **record)
    return unmapped


# ------------------------------------------------------------------- stage


def _default_updated_at(inventory: Inventory | None, map_path: Path) -> datetime:
    """The vintage stamped on every row, derived from the **inputs**.

    Never ``now()``: a wall-clock stamp would make every ``build-dim`` rewrite
    the object with different bytes, and CLAUDE.md rule 7 wants a re-run over
    unchanged inputs to be a no-op. The inventory's ``metadata.lastUpdated`` is
    the honest answer — blackstart is the source of truth for labels, so its
    vintage is the semantic layer's vintage. An entry may override it per row
    with its own ``updated_at``.
    """
    if inventory is not None and inventory.updated_at is not None:
        return inventory.updated_at
    stamp = datetime.fromtimestamp(map_path.stat().st_mtime, tz=timezone.utc).replace(
        microsecond=0
    )
    log.warning(
        "dim_updated_at_from_mtime",
        map_path=str(map_path),
        updated_at=timeutil.format_utc(stamp),
        detail=(
            "no blackstart metadata.lastUpdated was available, so updated_at "
            "falls back to the channel_map file's mtime (stable across re-runs "
            "on this machine, but not across checkouts)"
        ),
    )
    return stamp


def build(
    *,
    map_path: Path | str = Path("config/channel_map.json"),
    inventory_path: Path | str | None = None,
    dry_run: bool = False,
    bucket: str | None = None,
    client: BaseClient | None = None,
    live_channels: Iterable[Any] | None = None,
    live_channels_path: Path | str | None = None,
    updated_at: datetime | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """``energycap build-dim`` — join map + inventory, write ``dim_channel``.

    Args:
        map_path: hand-maintained ``config/channel_map.json`` (PLAN.md §9).
        inventory_path: blackstart ``montfort.json``; defaults to
            ``BLACKSTART_INVENTORY_PATH``. Only required when some entry names a
            ``blackstart_device_id`` — a Bryant-only map builds without it.
        dry_run: resolve, validate and log everything, write nothing.
        bucket: destination bucket; defaults to ``S3_BUCKET``.
        client: boto3 S3 client; defaults to the cached one.
        live_channels: the channels the collectors are actually producing, as
            ``(source, device_id, channel_id)`` triples or objects. Anything not
            covered by the map is WARNed and returned (PLAN.md §9). This is what
            ``energycap discover`` passes; no live call happens here.
        live_channels_path: same, read from a JSON file. When neither this nor
            ``live_channels`` is given, the sidecar ``energycap discover`` writes
            beside ``map_path`` is used if it exists (see
            :func:`default_live_channels_path`).
        updated_at: override the row vintage (normally derived from the
            inventory, so re-runs are byte-identical).
        settings: override :class:`Settings` (tests).

    Returns:
        A mapping of loggable fields — rows, key, placeholders, unmapped
        channels — which the CLI folds into its ``stage_ok`` line.

    Raises:
        DimBuildError: any problem in the map or the inventory, reported all at
            once; or a build that would write an empty table over a good one.
    """
    resolved_settings = settings if settings is not None else get_settings()
    map_file = Path(map_path)

    entries = load_channel_map(map_file)
    placeholders = [entry for entry in entries if entry.placeholder]
    needs_inventory = any(entry.blackstart_device_id is not None for entry in entries)

    inventory: Inventory | None = None
    if needs_inventory:
        chosen = inventory_path or resolved_settings.blackstart_inventory_path
        if chosen is None:
            named = _with_blackstart(entries)
            raise DimBuildError(
                f"{len(named)} entr(y/ies) in {map_file} name a "
                "blackstart_device_id, but no inventory path is set. Set "
                "BLACKSTART_INVENTORY_PATH (PLAN.md §9/§14) or pass "
                "--inventory-path; on the Mac it is "
                "~/code/blackstart/data/montfort.json. The entries are: "
                + ", ".join(
                    f"{entry.where} -> {entry.blackstart_device_id}"
                    for entry in named[:8]
                )
                + (" …" if len(named) > 8 else "")
            )
        inventory = load_inventory(chosen)

    stamp = (
        timeutil.ensure_utc(updated_at)
        if updated_at is not None
        else _default_updated_at(inventory, map_file)
    )
    rows = resolve_rows(
        entries, inventory, updated_at=stamp, subject=f"channel_map at {map_file}"
    )
    table = build_table(rows)

    live: list[tuple[str, str, str]] = []
    live_source: str | None = None
    if live_channels_path is None and live_channels is None:
        # Nothing was passed: pick up whatever `energycap discover` last wrote
        # beside the map. Without this, PLAN.md §9's "unmapped live channels are
        # WARNed by build-dim" only fires for callers that already knew to look.
        live_channels_path = default_live_channels_path(map_file)
    if live_channels_path is not None:
        live.extend(load_live_channels(live_channels_path))
        live_source = str(live_channels_path)
    if live_channels is not None:
        for raw in live_channels:
            channel = _coerce_live_channel(raw)
            if channel is not None and channel not in live:
                live.append(channel)
    unmapped = _report_unmapped(live, entries, rows)

    if placeholders:
        log.warning(
            "dim_placeholders_skipped",
            count=len(placeholders),
            entries=[entry.where for entry in placeholders],
            detail=(
                "these entries carry the literal token PLACEHOLDER and were left "
                "OUT of dim_channel.parquet. Run `energycap discover` against the "
                "live panels, paste the real hub id (Leviton device_id = the panel "
                "serial) into each entry, and delete its \"placeholder\": true "
                "line. Until then those channels have no labels and will be "
                "reported as unmapped."
            ),
        )

    if table.num_rows == 0:
        raise DimBuildError(
            f"{map_file} produced 0 dim_channel rows"
            + (
                f" — all {len(placeholders)} entr(y/ies) are PLACEHOLDERs. "
                "Run `energycap discover`, paste the real ids in, and delete the "
                '"placeholder": true lines.'
                if placeholders
                else " — it has no usable mappings."
            )
            + " Refusing to overwrite dim_channel.parquet with an empty table."
        )

    key = s3io.dim_channel_key()
    target_bucket = bucket
    if target_bucket is None:
        try:
            target_bucket = s3io.default_bucket()
        except RuntimeError:
            if not dry_run:
                raise
            target_bucket = None

    written = False
    if not dry_run:
        assert target_bucket is not None
        # Single file, overwritten atomically (PLAN.md §4/§9): temp key ->
        # verify -> copy -> delete temp. `sort_key=()` because dim_channel has
        # no time column; its order is model.DIM_KEY, applied in build_table.
        s3io.write_table_atomic(
            table, target_bucket, key, sort_key=(), client=client
        )
        written = True

    summary: dict[str, Any] = {
        "map_path": str(map_file),
        "inventory_path": str(inventory.path) if inventory is not None else None,
        "bucket": target_bucket,
        "key": key,
        "rows": table.num_rows,
        "entries": len(entries),
        "placeholders": len(placeholders),
        "from_blackstart": sum(1 for row in rows if row.blackstart_device_id),
        "sources": sorted({row.source for row in rows}),
        "live_channels": len(live),
        "live_channels_path": live_source,
        "unmapped": [
            f"{item['source']}/{item['device_id']}/{item['channel_id']}"
            for item in unmapped
        ],
        "unmapped_count": len(unmapped),
        "updated_at": timeutil.format_utc(stamp),
        "written": written,
        "dry_run": dry_run,
    }
    log.info("dim_ok", **summary)
    return summary


def _with_blackstart(entries: Sequence[ChannelEntry]) -> list[ChannelEntry]:
    return [entry for entry in entries if entry.blackstart_device_id is not None]
