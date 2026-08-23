"""The semantic layer: ``channel_map.json`` + blackstart -> ``dim_channel``.

PLAN.md §9 and §15.10. Everything here is offline: the inventory fixtures in
``tests/fixtures/blackstart/`` are trimmed copies of the **real**
``~/code/blackstart/data/montfort.json`` (schemaVersion 2, device-first), so the
join is pinned against the shape that actually exists rather than one invented
for the tests. S3 is ``moto``; no socket is opened.

What these tests exist to pin:

* the join itself, on a 1-pole and a 2-pole device, with ``slots`` rendered as
  the documented ``"1,3"`` string — breaker ``position`` -> blackstart ``slots``
  is the *only* linkage between the Leviton cloud and the panel inventory
  (PLAN.md §9), so it gets a test per shape;
* blackstart is the source of truth for labels, **and** an explicit field in
  ``channel_map.json`` overrides it;
* an entry with neither a ``blackstart_device_id`` nor explicit fields is a
  build error that names it, and so is a ``blackstart_device_id`` the inventory
  does not have;
* unmapped live channels are WARNed and returned — never silently absent;
* PLACEHOLDER entries stay out of the data and say how to make them real;
* a re-run is byte-identical (CLAUDE.md rule 7 applies to this stage too);
* an ``lge`` channel round-trips today, with no code change (PLAN.md §13).
"""

from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pa_pq
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.logging import configure_logging
from energy_capture.stages import dim
from tests.conftest import BUCKET

FIXTURES = Path(__file__).parent / "fixtures" / "blackstart"

#: The trimmed copy of the real inventory: 9 devices, real keys, real prose.
MONTFORT = FIXTURES / "montfort_trimmed.json"

#: A sparse inventory with no ``metadata`` block and no ``roomAliases``.
MONTFORT_NO_METADATA = FIXTURES / "montfort_no_metadata.json"

#: The same device id twice — a copy/paste while editing the inventory.
MONTFORT_DUPLICATE = FIXTURES / "montfort_duplicate_ids.json"

#: The committed, hand-maintained map (PLAN.md §9).
SHIPPED_MAP = Path(__file__).resolve().parents[1] / "config" / "channel_map.json"

#: A hub id standing in for the Leviton panel serial that ``discover`` will
#: print. Deliberately not the PLACEHOLDER token: these tests exercise the
#: real-data path.
HUB_A = "4C45565275C6"
HUB_B = "4C45565275D1"


# ----------------------------------------------------------------- helpers


def write_map(tmp_path: Path, *entries: dict[str, Any], name: str = "channel_map.json") -> Path:
    """Write a ``{"mappings": [...]}`` file and return its path."""
    path = tmp_path / name
    path.write_text(json.dumps({"mappings": list(entries)}, indent=2), encoding="utf-8")
    return path


def rows_by_channel(table) -> dict[str, dict[str, Any]]:
    """``channel_id -> row dict`` for a built ``dim_channel`` table."""
    return {row["channel_id"]: row for row in table.to_pylist()}


def build_table_for(tmp_path: Path, *entries: dict[str, Any], inventory: Path = MONTFORT):
    """Resolve entries into a ``dim_channel`` Arrow table (no S3 involved)."""
    map_path = write_map(tmp_path, *entries)
    parsed = dim.load_channel_map(map_path)
    loaded = dim.load_inventory(inventory)
    return dim.build_table(
        dim.resolve_rows(parsed, loaded, updated_at=loaded.updated_at or timeutil.now_utc())
    )


@pytest.fixture
def log_stream():
    """Capture the structured JSON log lines this stage emits."""
    buffer = io.StringIO()
    configure_logging("DEBUG", stream=buffer, force=True)
    yield buffer
    configure_logging("INFO", stream=io.StringIO(), force=True)


def log_events(stream: io.StringIO, event: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in stream.getvalue().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("event") == event:
            out.append(record)
    return out


# --------------------------------------------------- the fixtures are real


def test_the_inventory_fixture_has_the_real_montfort_shape() -> None:
    """Guard the guard: if the fixture drifts from the real file, everything
    below is testing a fiction. These are the keys the join actually reads."""
    document = json.loads(MONTFORT.read_text(encoding="utf-8"))
    assert document["schemaVersion"] == 2
    assert set(document) >= {"home", "metadata", "panels", "subpanels", "devices", "roomAliases"}

    devices = {device["id"]: device for device in document["devices"]}
    assert devices["A-1-3"]["slots"] == [1, 3]
    assert devices["A-1-3"]["poles"] == 2
    assert devices["A-11"]["slots"] == [11]
    assert devices["A-11"]["poles"] == 1
    for device in document["devices"]:
        assert {"id", "panel", "slots", "poles", "role", "label", "circuits"} <= set(device)


def test_the_real_inventory_if_present_parses_with_the_same_reader() -> None:
    """The fixtures are trimmed copies; the real file must still load.

    Skipped on a machine without the blackstart checkout — but where it exists,
    a schema change over there fails here instead of at build-dim time.
    """
    real = Path.home() / "code" / "blackstart" / "data" / "montfort.json"
    if not real.exists():  # pragma: no cover - depends on the machine
        pytest.skip("blackstart checkout not present")
    inventory = dim.load_inventory(real)
    assert len(inventory.devices) >= 30
    assert inventory.get("A-11") is not None
    assert inventory.updated_at is not None


# ------------------------------------------------------------- the join


def test_a_one_pole_device_joins_and_renders_its_single_slot(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    row = rows_by_channel(table)["breaker_p11"]

    # Everything comes from blackstart: it is the source of truth for labels.
    assert row["label"] == "Mud room + basement lights & plugs"
    assert row["short_label"] == "Mud room + basement lights"
    assert row["panel"] == "A"
    assert row["slots"] == "11"
    assert row["category"] == "branch_120v"
    assert row["priority"] is None
    assert row["estimated_watts"] == 1480.0
    assert row["blackstart_device_id"] == "A-11"
    assert row["source"] == "leviton"
    assert row["device_id"] == HUB_A
    # Four rooms, in the order the inventory recorded them.
    assert row["room"] == "Mud Room, Finished Basement, Basement Bathroom, Unfinished Basement"


def test_a_two_pole_device_is_one_channel_with_both_slots(tmp_path: Path) -> None:
    """PLAN.md §6.5: a 2-pole breaker is ONE channel; the slot list lives here."""
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p1",
            "blackstart_device_id": "A-1-3",
        },
    )
    row = rows_by_channel(table)["breaker_p1"]
    assert row["slots"] == "1,3"
    assert row["label"] == "Dryer outlet"
    # No shortLabel in the inventory for this one -> falls back to the label,
    # so the column is never null for a row that has a label.
    assert row["short_label"] == "Dryer outlet"
    assert row["category"] == "appliance_240v"
    assert row["estimated_watts"] == 5000.0


def test_slots_render_as_the_documented_comma_string(tmp_path: Path) -> None:
    """PLAN.md §9: ``slots`` is a string, e.g. ``"1,3"`` — never a list."""
    table = build_table_for(
        tmp_path,
        *[
            {
                "source": "leviton",
                "device_id": HUB_A,
                "channel_id": channel,
                "blackstart_device_id": device,
            }
            for channel, device in (
                ("breaker_p9", "A-9"),
                ("breaker_p1", "A-1-3"),
                ("breaker_p13", "A-13-15"),
            )
        ],
    )
    rows = rows_by_channel(table)
    assert rows["breaker_p9"]["slots"] == "9"
    assert rows["breaker_p1"]["slots"] == "1,3"
    assert rows["breaker_p13"]["slots"] == "13,15"
    assert dim.DIM_SCHEMA.field("slots").type == pa.string()


def test_slot_order_is_preserved_not_sorted() -> None:
    """The first slot is the breaker ``position`` ``channel_id`` is built from."""
    assert dim.normalize_slots([3, 1]) == "3,1"
    assert dim.normalize_slots("13, 15") == "13,15"
    assert dim.normalize_slots(11) == "11"
    assert dim.normalize_slots(None) is None


def test_priority_and_room_aliases_come_through(tmp_path: Path) -> None:
    """``critical`` is what a blackout plan reads; aliases are what humans say."""
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p13",
            "blackstart_device_id": "A-13-15",
        },
    )
    row = rows_by_channel(table)["breaker_p13"]
    assert row["priority"] == "critical"
    # roomAliases maps "Second Bedroom on Left" -> "Office".
    assert "Office" in row["room"]
    assert "Second Bedroom on Left" not in row["room"]


def test_role_and_circuit_type_normalize_into_categories(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        *[
            {
                "source": "leviton",
                "device_id": HUB_B,
                "channel_id": channel,
                "blackstart_device_id": device,
            }
            for channel, device in (
                ("ct_1_a", "B-6-8"),  # role feedThrough / "bus tap / feed-through lug"
                ("breaker_p10", "B-10-12"),  # 240V appliance
                ("breaker_p2", "A-2-4"),  # role generatorInlet / backup-feed
                ("breaker_p5", "A-5-7"),  # MWBC (with a prose tail)
            )
        ],
    )
    rows = rows_by_channel(table)
    assert rows["ct_1_a"]["category"] == "feed_through"
    assert rows["breaker_p10"]["category"] == "appliance_240v"
    assert rows["breaker_p2"]["category"] == "backup_feed"
    # The inventory's circuitType carries a parenthesised explanation; matching
    # on the leading token means rewording it upstream cannot empty the column.
    assert rows["breaker_p5"]["category"] == "mwbc"


def test_a_zero_estimated_watts_is_recorded_as_recorded(tmp_path: Path) -> None:
    """B-10-12 has no load figure and the inventory records 0. That is what the
    inventory says, so that is what we write — no guessing a heat pump's draw."""
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_B,
            "channel_id": "breaker_p10",
            "blackstart_device_id": "B-10-12",
        },
    )
    assert rows_by_channel(table)["breaker_p10"]["estimated_watts"] == 0.0


# --------------------------------------------------------------- overrides


def test_an_explicit_field_overrides_the_inventory(tmp_path: Path) -> None:
    """PLAN.md §9's own example: the CT on the HVAC subpanel feeder."""
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_B,
            "channel_id": "ct_1_a",
            "blackstart_device_id": "B-6-8",
            "label": "HVAC subpanel feeder (leg A)",
            "panel": "B",
            "category": "hvac",
        },
    )
    row = rows_by_channel(table)["ct_1_a"]
    assert row["label"] == "HVAC subpanel feeder (leg A)"
    assert row["category"] == "hvac"
    assert row["panel"] == "B"
    # Not overridden -> still inherited from blackstart.
    assert row["slots"] == "6,8"
    assert row["blackstart_device_id"] == "B-6-8"
    # short_label was not overridden either, so the inventory's wins.
    assert row["short_label"] == "Feed-through lug — HVAC sub"


def test_an_override_can_be_partial_and_the_rest_still_inherits(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "room": "Mud Room",
            "estimated_watts": 900,
        },
    )
    row = rows_by_channel(table)["breaker_p11"]
    assert row["room"] == "Mud Room"
    assert row["estimated_watts"] == 900.0
    assert row["label"] == "Mud room + basement lights & plugs"  # inherited
    assert row["slots"] == "11"


def test_an_explicit_only_entry_needs_no_inventory(tmp_path: Path) -> None:
    """A Bryant-only map must build on a machine with no blackstart checkout."""
    map_path = write_map(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
            "category": "hvac",
        },
    )
    summary = build_offline(map_path, inventory_path=None, dry_run=True)
    assert summary["rows"] == 1
    assert summary["inventory_path"] is None


def build_offline(map_path: Path, **kwargs: Any) -> dict[str, Any]:
    """``dim.build`` with no bucket resolution (dry run by default)."""
    kwargs.setdefault("dry_run", True)
    return dim.build(map_path=map_path, **kwargs)


# ----------------------------------------------------------------- errors


def test_an_entry_with_neither_blackstart_nor_explicit_fields_is_an_error(
    tmp_path: Path,
) -> None:
    map_path = write_map(
        tmp_path,
        {"source": "leviton", "device_id": HUB_A, "channel_id": "breaker_p11"},
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    message = str(excinfo.value)
    # The offending entry is named — index and identity.
    assert "mappings[0]" in message
    assert "breaker_p11" in message
    assert "blackstart_device_id" in message


def test_an_unknown_blackstart_device_id_is_an_error_with_near_misses(
    tmp_path: Path,
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p13",
            # The classic typo: A-13 is not a device; A-13-15 is.
            "blackstart_device_id": "A-13",
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    message = str(excinfo.value)
    assert "A-13" in message
    assert "A-13-15" in message  # the near-miss list points at the real id
    assert "mappings[0]" in message


def test_every_problem_in_the_file_is_reported_at_once(tmp_path: Path) -> None:
    map_path = write_map(
        tmp_path,
        {"source": "leviton", "device_id": HUB_A, "channel_id": "breaker_p11"},
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p9",
            "blackstart_device_id": "A-99",
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    message = str(excinfo.value)
    assert "2 problem(s)" in message
    assert "breaker_p11" in message and "breaker_p9" in message


def test_an_unknown_source_is_an_error_naming_the_vocabulary(tmp_path: Path) -> None:
    map_path = write_map(
        tmp_path,
        {"source": "levitron", "device_id": HUB_A, "channel_id": "x", "label": "y"},
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert "levitron" in str(excinfo.value)
    assert "model.SOURCES" in str(excinfo.value)


def test_a_misspelled_field_is_an_error_not_a_silently_dropped_label(
    tmp_path: Path,
) -> None:
    map_path = write_map(
        tmp_path,
        {"source": "bryant", "device_id": "S1", "channel_id": "hpheat", "labell": "oops"},
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert "labell" in str(excinfo.value)


def test_a_duplicate_channel_key_is_an_error(tmp_path: Path) -> None:
    entry = {
        "source": "bryant",
        "device_id": "4022W200213",
        "channel_id": "hpheat",
        "label": "Heat pump",
    }
    map_path = write_map(tmp_path, entry, {**entry, "label": "Heat pump (again)"})
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert "duplicates" in str(excinfo.value)


def test_a_top_level_key_other_than_mappings_is_an_error(tmp_path: Path) -> None:
    path = tmp_path / "channel_map.json"
    path.write_text(json.dumps({"mappings": [], "version": 3}), encoding="utf-8")
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.load_channel_map(path)
    assert "version" in str(excinfo.value)


def test_a_missing_map_or_inventory_says_where_to_look(tmp_path: Path) -> None:
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.load_channel_map(tmp_path / "nope.json")
    assert "hand-maintained" in str(excinfo.value)

    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=None, dry_run=True)
    assert "BLACKSTART_INVENTORY_PATH" in str(excinfo.value)


def test_a_duplicate_device_id_in_the_inventory_is_an_error() -> None:
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.load_inventory(MONTFORT_DUPLICATE)
    assert "appears more than once" in str(excinfo.value)


def test_an_all_placeholder_map_refuses_to_write_an_empty_table(tmp_path: Path) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": "PLACEHOLDER-HUB",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "placeholder": True,
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    message = str(excinfo.value)
    assert "energycap discover" in message
    assert "PLACEHOLDER" in message
    assert "Refusing to overwrite" in message


# ------------------------------------------------------------ placeholders


def test_placeholders_are_excluded_and_warned_with_the_remedy(
    tmp_path: Path, log_stream: io.StringIO
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": "PLACEHOLDER-LEVITON-HUB-A-SERIAL",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "placeholder": True,
        },
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
            "category": "hvac",
        },
    )
    summary = dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert summary["rows"] == 1  # only the real bryant channel
    assert summary["placeholders"] == 1

    (event,) = log_events(log_stream, "dim_placeholders_skipped")
    assert "energycap discover" in event["detail"]
    assert "placeholder" in event["detail"]


def test_a_placeholder_token_without_the_flag_is_an_error(tmp_path: Path) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": "PLACEHOLDER-HUB",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    message = str(excinfo.value)
    assert "energycap discover" in message
    assert '"placeholder": true' in message


def test_the_flag_without_the_token_is_an_error(tmp_path: Path) -> None:
    """Otherwise a real channel is silently dropped from the dim table."""
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "placeholder": True,
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert "Remove the flag" in str(excinfo.value)


def test_a_placeholder_is_still_validated_against_the_inventory(tmp_path: Path) -> None:
    """A PLACEHOLDER pointing at a device that no longer exists must fail now,
    not on the day somebody pastes a real hub id into it."""
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": "PLACEHOLDER-HUB",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-404",
            "placeholder": True,
        },
    )
    with pytest.raises(dim.DimBuildError) as excinfo:
        dim.build(map_path=map_path, inventory_path=MONTFORT, dry_run=True)
    assert "A-404" in str(excinfo.value)


# -------------------------------------------------------- unmapped channels


def test_an_unmapped_live_channel_warns_and_is_reported(
    tmp_path: Path, log_stream: io.StringIO
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    summary = dim.build(
        map_path=map_path,
        inventory_path=MONTFORT,
        dry_run=True,
        live_channels=[
            ("leviton", HUB_A, "breaker_p11"),  # mapped
            ("leviton", HUB_A, "breaker_p20"),  # not mapped
            {"source": "leviton", "device_id": HUB_A, "channel_id": "panel_leg_a"},
        ],
    )
    assert summary["unmapped_count"] == 2
    assert f"leviton/{HUB_A}/breaker_p20" in summary["unmapped"]
    assert f"leviton/{HUB_A}/panel_leg_a" in summary["unmapped"]

    events = log_events(log_stream, "dim_unmapped_live_channel")
    assert {event["channel_id"] for event in events} == {"breaker_p20", "panel_leg_a"}
    assert all("channel_map.json" in event["remedy"] for event in events)


def test_an_unmapped_channel_covered_by_a_placeholder_says_so(
    tmp_path: Path, log_stream: io.StringIO
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": "PLACEHOLDER-LEVITON-HUB-A-SERIAL",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "placeholder": True,
        },
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
        },
    )
    dim.build(
        map_path=map_path,
        inventory_path=MONTFORT,
        dry_run=True,
        live_channels=[("leviton", HUB_A, "breaker_p11")],
    )
    (event,) = log_events(log_stream, "dim_unmapped_live_channel")
    assert "PLACEHOLDER" in event["remedy"]
    assert HUB_A in event["remedy"]


def test_the_live_channel_list_can_come_from_a_file(tmp_path: Path) -> None:
    """The seam ``energycap discover`` writes through — no live call needed."""
    live = tmp_path / "live.json"
    live.write_text(
        json.dumps(
            {
                "mappings": [
                    {"source": "leviton", "device_id": HUB_A, "channel_id": "breaker_p20"}
                ]
            }
        ),
        encoding="utf-8",
    )
    map_path = write_map(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
        },
    )
    summary = dim.build(map_path=map_path, dry_run=True, live_channels_path=live)
    assert summary["unmapped"] == [f"leviton/{HUB_A}/breaker_p20"]


# ----------------------------------------------------------------- sources


def test_an_lge_channel_round_trips(tmp_path: Path, s3) -> None:
    """PLAN.md §13: ``dim_channel`` must happily hold an LG&E meter channel."""
    map_path = write_map(
        tmp_path,
        {
            "source": "lge",
            "device_id": "5091234567",
            "channel_id": "electric_main",
            "label": "LG&E electric meter — whole-home interval energy",
            "short_label": "LG&E electric meter",
            "category": "meter",
        },
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
            "category": "hvac",
        },
    )
    summary = dim.build(map_path=map_path, bucket=BUCKET, client=s3)
    assert summary["sources"] == ["bryant", "lge"]

    table = s3io.read_table(BUCKET, s3io.dim_channel_key(), client=s3)
    row = rows_by_channel(table)["electric_main"]
    assert row["source"] == "lge"
    assert row["device_id"] == "5091234567"
    assert row["category"] == "meter"
    assert row["panel"] is None and row["slots"] is None
    # The vocabulary is model.SOURCES, not a hardcoded pair.
    assert "lge" in model.SOURCES


def test_every_model_source_is_accepted(tmp_path: Path) -> None:
    entries = [
        {
            "source": source,
            "device_id": f"dev-{source}",
            "channel_id": "ch",
            "label": f"{source} channel",
        }
        for source in sorted(model.SOURCES)
    ]
    summary = dim.build(map_path=write_map(tmp_path, *entries), dry_run=True)
    assert summary["sources"] == sorted(model.SOURCES)


# ------------------------------------------------------------------ output


def test_the_table_is_written_atomically_to_the_one_dim_key(tmp_path: Path, s3) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    summary = dim.build(
        map_path=map_path, inventory_path=MONTFORT, bucket=BUCKET, client=s3
    )
    assert summary["key"] == s3io.dim_channel_key() == "energy/dim_channel/dim_channel.parquet"
    assert summary["written"] is True

    # Exactly one object, and nothing left behind in the staging prefix.
    keys = s3io.list_keys(BUCKET, "energy/", client=s3)
    assert keys == [s3io.dim_channel_key()]

    table = s3io.read_table(BUCKET, summary["key"], client=s3)
    assert table.schema == dim.DIM_SCHEMA
    assert table.num_rows == 1


def test_a_dry_run_writes_nothing(tmp_path: Path, s3) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    summary = dim.build(
        map_path=map_path,
        inventory_path=MONTFORT,
        bucket=BUCKET,
        client=s3,
        dry_run=True,
    )
    assert summary["rows"] == 1 and summary["written"] is False
    assert s3io.list_keys(BUCKET, "energy/", client=s3) == []


def test_a_rerun_is_byte_identical(tmp_path: Path, s3) -> None:
    """CLAUDE.md rule 7. ``updated_at`` is derived from the inputs, never from
    the clock, so re-running build-dim does not churn the object."""
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
            "category": "hvac",
        },
    )
    key = s3io.dim_channel_key()

    dim.build(map_path=map_path, inventory_path=MONTFORT, bucket=BUCKET, client=s3)
    first = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    dim.build(map_path=map_path, inventory_path=MONTFORT, bucket=BUCKET, client=s3)
    second = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    assert first == second


def test_updated_at_is_the_inventory_vintage(tmp_path: Path) -> None:
    inventory = dim.load_inventory(MONTFORT)
    # metadata.lastUpdated = 2026-08-22 -> local midnight, converted to UTC.
    assert inventory.updated_at == timeutil.local_midnight_utc(
        timeutil.parse_local_date("2026-08-22")
    )

    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
    )
    assert table.to_pylist()[0]["updated_at"] == inventory.updated_at


def test_an_entry_may_stamp_its_own_updated_at(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "updated_at": "2026-08-15",
        },
    )
    assert table.to_pylist()[0]["updated_at"] == timeutil.local_midnight_utc(
        timeutil.parse_local_date("2026-08-15")
    )


def test_an_inventory_without_metadata_falls_back_to_the_map_mtime(
    tmp_path: Path, log_stream: io.StringIO
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "leviton",
            "device_id": HUB_A,
            "channel_id": "breaker_p28",
            "blackstart_device_id": "A-28",
        },
    )
    summary = dim.build(
        map_path=map_path, inventory_path=MONTFORT_NO_METADATA, dry_run=True
    )
    assert summary["rows"] == 1
    assert log_events(log_stream, "dim_updated_at_from_mtime")
    # Still deterministic for a re-run on this machine.
    again = dim.build(
        map_path=map_path, inventory_path=MONTFORT_NO_METADATA, dry_run=True
    )
    assert again["updated_at"] == summary["updated_at"]


def test_rows_are_sorted_by_the_dim_key(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        *[
            {
                "source": source,
                "device_id": device_id,
                "channel_id": channel_id,
                "label": "x",
            }
            for source, device_id, channel_id in (
                ("leviton", HUB_B, "ct_1_a"),
                ("bryant", "4022W200213", "hpheat"),
                ("leviton", HUB_A, "breaker_p9"),
                ("leviton", HUB_A, "breaker_p11"),
            )
        ],
    )
    keys = [tuple(row[name] for name in model.DIM_KEY) for row in table.to_pylist()]
    assert keys == sorted(keys)


def test_the_written_object_is_readable_parquet_with_the_dim_schema(
    tmp_path: Path, s3
) -> None:
    map_path = write_map(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "HVAC — heat pump heating",
        },
    )
    dim.build(map_path=map_path, bucket=BUCKET, client=s3)
    body = s3.get_object(Bucket=BUCKET, Key=s3io.dim_channel_key())["Body"].read()
    parquet = pa_pq.ParquetFile(io.BytesIO(body))
    assert parquet.metadata.num_rows == 1
    assert [f.name for f in parquet.schema_arrow] == list(dim.DIM_COLUMNS)


# --------------------------------------------------- the committed map file


def test_the_shipped_channel_map_parses() -> None:
    entries = dim.load_channel_map(SHIPPED_MAP)
    assert entries, "config/channel_map.json must not be empty"
    keys = [entry.key for entry in entries]
    assert len(set(keys)) == len(keys)


def test_the_shipped_map_covers_the_eight_bryant_energy_components() -> None:
    """PLAN.md §7.2's components must all be labelled, disabled ones included:
    a component that is disabled today is still a channel we want named if it
    ever reports."""
    from energy_capture.stages import daily

    entries = dim.load_channel_map(SHIPPED_MAP)
    bryant = {
        entry.channel_id
        for entry in entries
        if entry.source == model.SOURCE_BRYANT and not entry.placeholder
    }
    assert {spec.channel_id for spec in daily.COMPONENTS} <= bryant
    # ...and the 30s status channels of §7.3.
    assert {"system", "zone_1"} <= bryant


def test_the_shipped_map_builds_against_the_inventory(s3) -> None:
    """Every source now reaches dim_channel with real ids.

    `energycap discover` gave the Leviton entries real hub ids (2026-08-17), and
    Green Button Connect gave the LG&E entries real meter numbers (2026-08-18) —
    so nothing in the shipped map is documentation any more.
    """
    summary = dim.build(
        map_path=SHIPPED_MAP, inventory_path=MONTFORT, bucket=BUCKET, client=s3
    )
    assert summary["sources"] == ["bryant", "leviton", "lge"]
    # 10 Bryant + 32 live Leviton + 4 LG&E meter ids (house, barn, two retired
    # aliases of the house the Download export republishes — DEVIATIONS #168).
    # The Leviton count went 12 -> 32 on 2026-08-22, when 20 more smart breakers
    # went in and `energycap discover` reported every live channel as mapped.
    assert summary["rows"] == 46
    assert summary["placeholders"] == 0

    table = s3io.read_table(BUCKET, summary["key"], client=s3)
    by_channel = rows_by_channel(table)

    row = by_channel["hpheat"]
    assert row["category"] == "hvac"
    assert row["label"]
    assert row["blackstart_device_id"] is None

    # The blackstart join is what makes a Leviton row readable: this entry
    # carries NO label of its own, so every human-facing field below came from
    # montfort.json (PLAN.md §9 — blackstart stays the source of truth).
    water_heater = by_channel["breaker_p19"]
    assert water_heater["blackstart_device_id"] == "A-19-21"
    assert water_heater["label"] == "Water heater"
    assert water_heater["panel"] == "A"
    assert water_heater["slots"] == "19,21"

    # ...and an explicit label still overrides the inventory, which is the whole
    # point for this pair: Leviton calls it HEAT_PUMP and it is the strip heat.
    strips = by_channel["ct_2_a"]
    assert strips["blackstart_device_id"] == "B-6-8"
    assert "strip" in strips["label"].lower()
    assert strips["slots"] == "6,8"


def test_every_shipped_entry_is_real() -> None:
    entries = dim.load_channel_map(SHIPPED_MAP)

    # Discovery has happened for Leviton (2026-08-17) and Green Button Connect
    # for LG&E (2026-08-18), so nothing here may still be documentation — a
    # placeholder silently drops a real channel out of dim_channel.
    assert [e.device_id for e in entries if e.placeholder] == []

    leviton = [e for e in entries if e.source == "leviton"]
    assert leviton, "the shipped map must describe the live Leviton channels"
    # Both hubs, and channel_id genuinely repeats across them — which is why
    # device_id is part of the identity and of every GROUP BY.
    assert {e.device_id for e in leviton} == {"1000_0046_1D52", "1000_0046_1D48"}
    assert len({(e.device_id, e.channel_id) for e in leviton}) == len(leviton)
    repeated = {e.channel_id for e in leviton if e.device_id == "1000_0046_1D52"} & {
        e.channel_id for e in leviton if e.device_id == "1000_0046_1D48"
    }
    assert {"ct_1_a", "panel_leg_a"} <= repeated

    # The channel-id shapes of PLAN.md §6.5 are all represented by real entries.
    channels = {e.channel_id for e in leviton}
    assert {"breaker_p19", "ct_1_a", "ct_2_a", "panel_leg_a"} <= channels


def test_placeholder_entries_name_real_blackstart_devices() -> None:
    """They are documentation, so they must still be *true* documentation."""
    inventory = dim.load_inventory(MONTFORT)
    for entry in dim.load_channel_map(SHIPPED_MAP):
        if entry.blackstart_device_id is not None:
            assert inventory.get(entry.blackstart_device_id) is not None, entry.where


def test_a_newly_installed_smart_breaker_is_reported_as_unmapped(
    log_stream: io.StringIO,
) -> None:
    """A breaker that appears after the map was written must never be silently
    absent from dim_channel (PLAN.md §9).

    This is the live workflow, not a hypothetical: smart breakers are being
    added to these panels over time, and each one shows up first as an unmapped
    live channel. The channels the map already knows about must stay quiet, so
    the WARN means "something new arrived" rather than being constant noise.
    """
    known = ("leviton", "1000_0046_1D52", "breaker_p19")
    # Panel B position 9 still holds a dumb breaker (blackstart B-9, full bath
    # fan/light/heater), so it is genuinely absent from the shipped map and is a
    # realistic next arrival. Panel A has none left to use: its four non-smart
    # positions are the generator inlet, the energy monitor's own supply at
    # 27/29, and the two circuits inside the LSPD1-T at 22/24 — a combination
    # SPD/breaker with no metering variant. None of them can ever be a channel.
    fresh = ("leviton", "1000_0046_1D48", "breaker_p9")

    summary = dim.build(
        map_path=SHIPPED_MAP,
        inventory_path=MONTFORT,
        dry_run=True,
        live_channels=[known, fresh],
    )

    assert summary["unmapped"] == ["leviton/1000_0046_1D48/breaker_p9"]
    (event,) = log_events(log_stream, "dim_unmapped_live_channel")
    assert event["channel_id"] == "breaker_p9"
    # No placeholder exists for it any more, so the remedy is the discover flow.
    assert "skeleton" in event["remedy"]


# ------------------------------------------------------------- housekeeping


def test_the_cli_entrypoint_matches_this_module() -> None:
    from energy_capture import cli

    assert cli.STAGE_ENTRYPOINTS["build-dim"] == ("energy_capture.stages.dim", "build")
    assert callable(dim.build)


def test_the_dim_columns_are_plan_9s_list_plus_primary() -> None:
    """PLAN.md §9 lists thirteen columns; `primary` is a deliberate 14th.

    energy_meter's table comment tells readers "dim_channel marks the house
    primary — join it rather than hardcoding an id", and that was a promise the
    file could not keep. DEVIATIONS.md #178.
    """
    assert dim.DIM_COLUMNS == (
        "source",
        "device_id",
        "channel_id",
        "label",
        "short_label",
        "panel",
        "slots",
        "category",
        "room",
        "priority",
        "estimated_watts",
        "blackstart_device_id",
        "is_primary",
        "updated_at",
    )
    assert dim.DIM_COLUMNS[:3] == model.DIM_KEY


def test_categories_normalize_predictably() -> None:
    assert dim.normalize_category("240V appliance") == "240v_appliance"
    assert dim.normalize_category("Feed-Through") == "feed_through"
    assert dim.normalize_category("  HVAC  ") == "hvac"
    assert dim.normalize_category(None) is None


def test_an_unknown_category_warns_but_is_kept(
    tmp_path: Path, log_stream: io.StringIO
) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "Heat pump",
            "category": "wildly novel",
        },
    )
    assert table.to_pylist()[0]["category"] == "wildly_novel"
    assert log_events(log_stream, "dim_unknown_category")


def test_notes_never_reach_the_parquet_file(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "Heat pump",
            "notes": "for the next human, not for Athena",
        },
    )
    assert "notes" not in table.column_names


def test_updated_at_is_an_aware_utc_timestamp(tmp_path: Path) -> None:
    table = build_table_for(
        tmp_path,
        {
            "source": "bryant",
            "device_id": "4022W200213",
            "channel_id": "hpheat",
            "label": "Heat pump",
        },
    )
    value = table.to_pylist()[0]["updated_at"]
    assert isinstance(value, datetime) and value.tzinfo is not None
    assert str(dim.DIM_SCHEMA.field("updated_at").type) == "timestamp[us, tz=UTC]"


# ------------------------------------------- the committed map, as shipped


def test_the_shipped_map_has_no_placeholders_left() -> None:
    """Every channel in ``config/channel_map.json`` is now a real one.

    The LG&E entry was the last placeholder — a stand-in proving ``dim_channel``
    could hold an ``lge`` channel before there was one. Both real meters landed
    2026-08-18. A placeholder reappearing means someone described a channel they
    had not actually seen, and ``build-dim`` silently drops those, so the
    semantic layer would lose a channel without saying so.
    """
    import json

    entries = json.loads(
        (Path(__file__).resolve().parent.parent / "config" / "channel_map.json")
        .read_text(encoding="utf-8")
    )["mappings"]
    placeholders = [e["device_id"] for e in entries if e.get("placeholder")]
    assert placeholders == [], placeholders


def test_both_lge_meters_are_mapped_and_distinguishable() -> None:
    """House and barn are separate services on one account.

    They must never be summed — ``compare-meter`` refuses to guess between them
    — so a reader meeting either id in the data needs to know which is which.
    The retired ids the download republishes are mapped for the same reason.
    """
    import json

    entries = json.loads(
        (Path(__file__).resolve().parent.parent / "config" / "channel_map.json")
        .read_text(encoding="utf-8")
    )["mappings"]
    meters = {e["device_id"]: e for e in entries if e["source"] == "lge"}

    assert "1308468" in meters and "house" in meters["1308468"]["label"].lower()
    assert "1326254" in meters and "barn" in meters["1326254"]["label"].lower()
    # The download's retired aliases of the house meter (DEVIATIONS #168).
    assert {"944006", "944401"} <= set(meters)
    assert all(e["category"] == "meter" for e in meters.values())
    assert all(e["channel_id"] == "electric_main" for e in meters.values())
