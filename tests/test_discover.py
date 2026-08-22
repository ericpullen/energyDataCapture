"""``energycap discover`` tests — offline, fixture-driven (PLAN.md §9).

Everything here replays the same recorded bodies the source tests use
(``tests/fixtures/leviton/``, ``tests/fixtures/bryant/``) through the real
:class:`~energy_capture.sources.leviton.LevitonSource` and
:class:`~energy_capture.sources.bryant.BryantStatusSource`, with only the socket
replaced. No test may reach the network (CLAUDE.md "Testing"), and there are no
credentials in this environment.

What is pinned, in order of how much it would hurt to get wrong:

1. **The skeleton is paste-ready and correct.** Valid JSON, exactly the shape
   ``stages/dim.py`` consumes, and *only* channels that are both live and
   unmapped. A skeleton entry for a ``NONE-2`` placeholder would invite the
   operator to label a circuit that can never produce a row.
2. **Skipped objects are shown, not hidden.** Placeholder breakers, ``NOT_USED``
   CTs and phantom zones appear in the table marked SKIP — knowing what the
   pipeline ignores is the point of the command.
3. **One cloud being down never suppresses the other**, and never shows the
   operator a traceback.
4. **``--json`` and the raw dump write what they promise**, because the dump is
   the only chance to capture the evidence PLAN.md §7.3 leaves UNVERIFIED.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from aioleviton import AuthToken, Breaker, Ct, Permission, Residence, Whem

from energy_capture.config import Settings
from energy_capture.model import DIM_KEY, SOURCE_BRYANT, SOURCE_LEVITON
from energy_capture.sources.base import DiscoveredChannel, SourceAuthError
from energy_capture.sources.bryant import BryantStatusSource
from energy_capture.sources.leviton import LevitonSource
from energy_capture.stages import discover as discover_module
from energy_capture.stages.discover import (
    ENV_DUMP,
    LIVE_CHANNELS_FILENAME,
    DiscoveryFailed,
    live_channels_path,
    read_channel_map,
    run,
)

LEVITON_FIXTURES = Path(__file__).parent / "fixtures" / "leviton"
BRYANT_FIXTURES = Path(__file__).parent / "fixtures" / "bryant"

HUB_A = "1000_AAAA_1111"
HUB_B = "1000_BBBB_2222"
SERIAL = "TEST0000001"


def leviton_fixture(name: str) -> Any:
    return json.loads((LEVITON_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def bryant_fixture(name: str) -> dict[str, Any]:
    """The ``data`` object of a recorded GraphQL response."""
    payload = json.loads((BRYANT_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    return copy.deepcopy(payload["data"])


# ---------------------------------------------------------------- fake clouds


class FakeLevitonClient:
    """``aioleviton.LevitonClient``'s surface, backed by the recorded JSON.

    Returns genuine ``aioleviton`` model objects, so the ``model.raw`` bodies the
    raw dump captures are the real thing rather than a hand-written double.
    """

    def __init__(self) -> None:
        self.permissions = leviton_fixture("permissions")
        self.residences = leviton_fixture("residences")
        self.whems = leviton_fixture("whems")
        self.breakers: dict[str, list[dict[str, Any]]] = {
            HUB_A: leviton_fixture(f"breakers_{HUB_A}"),
            HUB_B: leviton_fixture(f"breakers_{HUB_B}"),
        }
        self.cts: dict[str, list[dict[str, Any]]] = {
            HUB_A: leviton_fixture(f"cts_{HUB_A}"),
            HUB_B: leviton_fixture(f"cts_{HUB_B}"),
        }
        self.login_payload = leviton_fixture("login")
        self.calls: list[str] = []

    async def login(self, email: str, password: str, code: str | None = None) -> AuthToken:
        self.calls.append("login")
        data = self.login_payload
        return AuthToken(
            token=data["id"],
            ttl=data["ttl"],
            created=data["created"],
            user_id=data["userId"],
            user=data.get("user", {}),
        )

    def restore_session(self, token: str, user_id: str) -> None:
        self.calls.append("restore_session")

    async def get_permissions(self) -> list[Permission]:
        self.calls.append("get_permissions")
        return [Permission.from_api(p) for p in self.permissions]

    async def get_residences(self, account_id: int) -> list[Residence]:
        self.calls.append("get_residences")
        return [Residence.from_api(r) for r in self.residences]

    async def get_whems(self, residence_id: int) -> list[Whem]:
        self.calls.append("get_whems")
        return [Whem.from_api(w) for w in self.whems]

    async def get_whem_breakers(self, whem_id: str) -> list[Breaker]:
        self.calls.append("get_whem_breakers")
        return [Breaker.from_api(b) for b in self.breakers.get(whem_id, [])]

    async def get_cts(self, whem_id: str) -> list[Ct]:
        self.calls.append("get_cts")
        return [Ct.from_api(c) for c in self.cts.get(whem_id, [])]

    async def set_whem_bandwidth(self, whem_id: str, bandwidth: int) -> None:
        raise AssertionError("discover must never touch the hub's bandwidth")


class FakeCarrierClient:
    """``CarrierGraphQLClient``'s surface: one canned ``data`` object per operation."""

    def __init__(
        self,
        data: dict[str, Any] | None = None,
        *,
        error: Exception | None = None,
        by_operation: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.data = data if data is not None else bryant_fixture("status_single_zone")
        self.by_operation = by_operation or {}
        self.error = error
        self.operations: list[str | None] = []
        self.closed = False

    async def query(
        self,
        query: str,
        *,
        variables: Any = None,
        operation_name: str | None = None,
        op: str | None = None,
    ) -> dict[str, Any]:
        self.operations.append(operation_name)
        if self.error is not None:
            raise self.error
        return copy.deepcopy(self.by_operation.get(operation_name or "", self.data))

    def status_fields(self) -> dict[str, Any]:
        return {}

    async def close(self) -> None:
        self.closed = True


# --------------------------------------------------------------------- sources


@pytest.fixture
def leviton_client() -> FakeLevitonClient:
    return FakeLevitonClient()


@pytest.fixture
def leviton_source(
    settings: Settings, leviton_client: FakeLevitonClient, spool_dir: Path
) -> LevitonSource:
    """A real :class:`LevitonSource` on the raw-capturing adapter."""
    adapter_cls = discover_module._leviton_adapter_class()
    adapter = adapter_cls(
        username="fake-user",
        password="fake-password",
        token_path=spool_dir / "tokens" / "leviton.json",
        client=leviton_client,
        retry_waits=(),
    )
    return LevitonSource(settings, adapter=adapter)


@pytest.fixture
def carrier_client() -> FakeCarrierClient:
    return FakeCarrierClient()


@pytest.fixture
def bryant_source(settings: Settings, carrier_client: FakeCarrierClient) -> BryantStatusSource:
    """A real :class:`BryantStatusSource` behind the recording wrapper."""
    recorder = discover_module._RecordingGraphQLClient(carrier_client)
    return BryantStatusSource(
        settings, client=recorder, device_id=SERIAL, username="fake@example.invalid"
    )


@pytest.fixture
def map_path(tmp_path: Path) -> Path:
    """A channel_map path inside the test's tmp dir (the file need not exist)."""
    return tmp_path / "config" / "channel_map.json"


def write_map(path: Path, *entries: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"mappings": list(entries)}, indent=2), encoding="utf-8")
    return path


def discover(
    *,
    map_path: Path,
    leviton_source: Any = None,
    bryant_source: Any = None,
    **kwargs: Any,
) -> dict[str, Any]:
    return run(
        map_path=map_path,
        leviton_source=leviton_source,
        bryant_source=bryant_source,
        **kwargs,
    )


def live_channels(map_path: Path) -> dict[str, Any]:
    return json.loads(live_channels_path(map_path).read_text(encoding="utf-8"))


# ============================================================================
# The table
# ============================================================================


def test_the_table_shows_hubs_breakers_cts_zones_and_the_serial(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
) -> None:
    """PLAN.md §9: hubs, breakers, CTs and Bryant zones, all in one readable pass."""
    discover(map_path=map_path, leviton_source=leviton_source, bryant_source=bryant_source)
    out = capsys.readouterr().out

    # Hubs: id, firmware, connected.
    assert HUB_A in out and HUB_B in out
    assert "2.2.0" in out  # the fw of hub A, straight from whems.json

    # Breakers: position, name, model, branchType, poles, connected.
    for token in ("breaker_p11", "Heat pump", "LB230-2P", "GENERAL", "breaker_p13"):
        assert token in out, token

    # CTs: channel and usageType (including the pair/leg split).
    for token in ("ct_1_a", "ct_1_b", "OTHER", "NOT_USED"):
        assert token in out, token

    # Panel legs are channels too (PLAN.md §6.5).
    assert "panel_leg_a" in out and "panel_leg_b" in out

    # Bryant: the system serial and its zones.
    assert SERIAL in out
    assert "zone_1" in out
    assert "system" in out


def test_channels_are_grouped_under_the_hub_they_belong_to(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    """Both hubs have a ``panel_leg_a``; the table must say which is which."""
    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    lines = capsys.readouterr().out.splitlines()

    hub_a = next(i for i, line in enumerate(lines) if line.startswith(f"  hub {HUB_A}"))
    hub_b = next(i for i, line in enumerate(lines) if line.startswith(f"  hub {HUB_B}"))
    assert hub_a < hub_b

    end = next(i for i, line in enumerate(lines) if line.startswith("=== channel_map"))
    section_a = "\n".join(lines[hub_a:hub_b])
    section_b = "\n".join(lines[hub_b:end])
    assert "breaker_p11" in section_a and "breaker_p11" not in section_b
    assert "ct_4_a" in section_b and "ct_4_a" not in section_a
    # Firmware and connectedness ride on the hub header (PLAN.md §9).
    assert "version=2.2.0" in lines[hub_a] and "connected=yes" in lines[hub_a]
    assert "connected=no" in lines[hub_b]
    # Every hub carries its own pair of panel legs.
    assert section_a.count("panel_leg_a") == 1 and section_b.count("panel_leg_a") == 1
    # The physical slot is the identity, so it is a column of its own.
    assert "POS" in section_a


def test_a_new_smart_breaker_appears_with_its_position_not_its_api_id(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_client: FakeLevitonClient,
    leviton_source: LevitonSource,
) -> None:
    """The five-minute path: install a breaker, run discover, see it unmapped."""
    leviton_client.breakers[HUB_A].extend(leviton_fixture("breakers_new_smart_breaker"))

    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    out = capsys.readouterr().out

    assert "breaker_p21" in out and "EV charger" in out
    # fw >=2.2.0 suffixes the API id; the channel is keyed on position (§6.5).
    assert "4C4556527611_A65E" not in out

    skeleton = json.loads(live_channels(map_path)["skeleton"] and json.dumps(live_channels(map_path)["skeleton"]))
    assert {"leviton", HUB_A, "breaker_p21"} <= {
        v for entry in skeleton["mappings"] for v in entry.values()
    }


def test_placeholder_breakers_and_unused_cts_are_shown_but_marked_skipped(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    """PLAN.md §6.3's skip list is visible, not silent — and never mappable."""
    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    out = capsys.readouterr().out

    assert "NONE-2" in out and "NONE" in out  # the placeholder models themselves
    assert "SKIP: placeholder model" in out
    assert "SKIP: usageType=NOT_USED" in out

    document = live_channels(map_path)
    live = {c["channel_id"] for c in document["channels"]}
    skipped = {c["channel_id"]: c for c in document["skipped_channels"]}

    # Position 5 and 7 are placeholders; channel 3 is the NOT_USED clamp. None
    # of them is a live channel, so build-dim must never WARN about them.
    for channel_id in ("breaker_p5", "breaker_p7", "ct_3_a"):
        assert channel_id not in live
        assert skipped[channel_id]["mappable"] is False
        assert skipped[channel_id]["skip_reason"]

    # ... and a real breaker is not collateral damage.
    assert "breaker_p11" in live
    assert "breaker_p11" not in skipped


def test_an_unpositioned_breaker_is_shown_as_skipped_not_hidden(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_client: FakeLevitonClient,
    leviton_source: LevitonSource,
) -> None:
    """The table is where an operator finds out why a live circuit has no rows.

    A breaker enrolled but not yet located reports watts and no position, so the
    collector cannot name it and writes nothing. Hiding it would leave a real
    circuit missing from the data with nothing anywhere to explain it; offering
    it for mapping would invite a channel_map entry for `breaker_p0`, a slot no
    panel has. So it is listed, marked SKIP, and never mappable.
    """
    leviton_client.breakers[HUB_A].extend(leviton_fixture("breakers_unpositioned"))

    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    out = capsys.readouterr().out

    assert "SKIP: no position from the cloud" in out
    assert "positioning wizard" in out
    # Named by something an operator can act on, since the slot is exactly what
    # is missing.
    assert "Guest bath" in out and "Well pump" in out

    document = live_channels(map_path)
    live = {c["channel_id"] for c in document["channels"]}
    skipped = {c["channel_id"]: c for c in document["skipped_channels"]}

    assert "breaker_p0" not in live
    assert skipped["breaker_p0"]["mappable"] is False
    assert "positioning wizard" in skipped["breaker_p0"]["skip_reason"]

    # No skeleton entry either: build-dim must not be told to label a fiction.
    skeleton = json.dumps(live_channels(map_path)["skeleton"])
    assert "breaker_p0" not in skeleton

    # ... and the positioned breakers are not collateral damage.
    assert "breaker_p11" in live


def test_an_lsbma_accessory_is_labelled_rather_than_hidden(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_client: FakeLevitonClient,
    leviton_source: LevitonSource,
) -> None:
    """DEVIATIONS.md #16: an LSBMA meters real load, so it stays mappable."""
    leviton_client.breakers[HUB_A].append(
        {
            "id": "ACCESSORY_0019",
            "name": "Solar accessory",
            "model": "LSBMA",
            "branchType": "GENERAL",
            "position": 19,
            "poles": 1,
            "connected": True,
            "power": 240,
            "rmsCurrent": 2.0,
            "rmsVoltage": 120,
            "iotWhemId": HUB_A,
        }
    )

    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    out = capsys.readouterr().out

    assert "LSBMA accessory" in out
    entry = {c["channel_id"]: c for c in live_channels(map_path)["channels"]}["breaker_p19"]
    assert entry["mappable"] is True
    assert entry["skip_reason"] is None


def test_phantom_zones_are_shown_as_skipped_and_never_offered_for_mapping(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    bryant_source: BryantStatusSource,
) -> None:
    """DEVIATIONS.md #66: 8 zones are reported, 1 exists. The other 7 are not channels."""
    discover(map_path=map_path, bryant_source=bryant_source, sources=(SOURCE_BRYANT,))
    out = capsys.readouterr().out

    assert "SKIP: enabled!=on" in out

    document = live_channels(map_path)
    live_zones = [c for c in document["channels"] if c["kind"] == "zone"]
    skipped_zones = [c for c in document["skipped_channels"] if c["kind"] == "zone"]
    assert len(live_zones) + len(skipped_zones) == 8, "every zone stays visible"
    assert [z["channel_id"] for z in live_zones] == ["zone_1"]

    skeleton_ids = {entry["channel_id"] for entry in document["skeleton"]["mappings"]}
    assert "zone_2" not in skeleton_ids
    assert {"zone_1", "system"} <= skeleton_ids


def test_the_bryant_section_reports_the_unverified_evidence(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    settings: Settings,
) -> None:
    """DEVIATIONS.md #59/#75: a numeric ``odu.opstat`` is the thing to notice."""
    client = FakeCarrierClient(bryant_fixture("status_varcap"))
    source = BryantStatusSource(
        settings,
        client=discover_module._RecordingGraphQLClient(client),
        device_id=SERIAL,
        username="fake@example.invalid",
    )

    discover(map_path=map_path, bryant_source=source, sources=(SOURCE_BRYANT,))
    out = capsys.readouterr().out

    assert "odu_opstat_numeric=yes" in out
    facts = live_channels(map_path)["sources"]["bryant"]["facts"]
    assert facts["odu_opstat_numeric"] is True
    assert facts["cfgem"] == "F"
    assert facts["infinity_status_resolved"] is True


# ============================================================================
# The skeleton — the whole reason the command exists
# ============================================================================


def test_the_skeleton_is_valid_json_in_the_shape_dim_consumes(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
) -> None:
    """PLAN.md §9's ``mappings`` entry shape, keyed on ``model.DIM_KEY``."""
    discover(map_path=map_path, leviton_source=leviton_source, bryant_source=bryant_source)
    printed = capsys.readouterr().out

    # The block printed for the operator really is parseable JSON.
    start = printed.index('{\n  "mappings"')
    end = printed.index("=== next ===")
    block = json.loads(printed[start:end].strip())

    assert set(block) == {"mappings"}
    assert block["mappings"], "there is nothing mapped yet, so nothing can be empty"

    reference = set(
        DiscoveredChannel(
            source=SOURCE_LEVITON, device_id="d", channel_id="c", kind="breaker"
        ).channel_map_entry()
    )
    for entry in block["mappings"]:
        assert set(entry) == reference, "must match sources/base.py's contract exactly"
        assert all(entry[key] for key in DIM_KEY), "the join key is never blank"
        # Left empty on purpose: PLAN.md §9 makes an entry with neither a label
        # nor a blackstart id a build error, so the human must say what it is.
        assert entry["blackstart_device_id"] == ""

    assert block == live_channels(map_path)["skeleton"]


def test_already_mapped_channels_are_excluded_from_the_skeleton(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    write_map(
        map_path,
        {
            "source": SOURCE_LEVITON,
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
        },
        {
            "source": SOURCE_LEVITON,
            "device_id": HUB_A,
            "channel_id": "ct_1_a",
            "label": "HVAC subpanel feeder (leg A)",
        },
    )

    summary = discover(
        map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,)
    )
    document = live_channels(map_path)
    skeleton_ids = {
        (e["device_id"], e["channel_id"]) for e in document["skeleton"]["mappings"]
    }

    assert (HUB_A, "breaker_p11") not in skeleton_ids
    assert (HUB_A, "ct_1_a") not in skeleton_ids
    assert (HUB_A, "breaker_p13") in skeleton_ids
    assert summary["mapped"] == 2
    assert summary["unmapped"] == len(skeleton_ids)

    out = capsys.readouterr().out
    assert "2 mapping(s)" in out  # the map's entry count is reported
    assert "breaker_p11" in out and "yes" in out  # and shown as MAPPED in the table


def test_an_absent_channel_map_is_the_normal_first_run(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    assert not map_path.exists()
    summary = discover(
        map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,)
    )
    out = capsys.readouterr().out
    assert "does not exist yet" in out
    assert summary["mapped"] == 0
    assert summary["unmapped"] > 0


def test_an_unreadable_channel_map_degrades_instead_of_crashing(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text("{ this is not json", encoding="utf-8")

    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))
    out = capsys.readouterr().out
    assert "COULD NOT BE READ" in out
    assert "breaker_p11" in out  # the report is still produced


def test_placeholder_map_entries_do_not_count_as_mapped(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    """``stages/dim.py`` leaves PLACEHOLDER entries out of dim_channel (PLAN.md §9).

    The hub id cannot be known before this command runs, so an entry waiting for
    one maps nothing — and the operator needs the id printed next to the nudge.
    """
    write_map(
        map_path,
        {
            "source": SOURCE_LEVITON,
            "device_id": "PLACEHOLDER-PANEL-A",
            "channel_id": "breaker_p11",
            "blackstart_device_id": "A-11",
            "placeholder": True,
        },
    )

    summary = discover(
        map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,)
    )
    out = capsys.readouterr().out

    assert summary["map_placeholders"] == 1
    assert summary["mapped"] == 0
    assert "PLACEHOLDER token" in out
    assert HUB_A in out  # the id to paste is right there
    skeleton_ids = {
        (e["device_id"], e["channel_id"])
        for e in live_channels(map_path)["skeleton"]["mappings"]
    }
    assert (HUB_A, "breaker_p11") in skeleton_ids


def test_read_channel_map_ignores_entries_without_a_full_key(tmp_path: Path) -> None:
    path = write_map(
        tmp_path / "channel_map.json",
        {"source": "leviton", "device_id": HUB_A, "channel_id": "breaker_p11"},
        {"source": "leviton", "channel_id": "breaker_p13"},  # no device_id
    )
    parsed = read_channel_map(path)
    assert parsed.keys == frozenset({("leviton", HUB_A, "breaker_p11")})
    assert parsed.entries == 2
    assert parsed.malformed == 1


# ============================================================================
# Composing with build-dim
# ============================================================================


def test_the_live_channel_file_lands_beside_the_map_and_describes_itself(
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """PLAN.md §9: build-dim must be able to WARN without a second live call."""
    summary = discover(
        map_path=map_path, leviton_source=leviton_source, bryant_source=bryant_source
    )

    path = live_channels_path(map_path)
    assert path == map_path.parent / LIVE_CHANNELS_FILENAME
    assert summary["live_channels_path"] == str(path)

    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["version"] == 1
    assert document["generated_utc"].endswith("+00:00")
    assert set(document) >= {
        "sources",
        "devices",
        "channels",
        "unmapped",
        "skeleton",
        "map_path",
    }
    assert document["sources"]["leviton"]["ok"] is True
    assert document["sources"]["bryant"]["ok"] is True

    # Every unmapped entry is a live channel — build-dim's WARN set.
    live = {(c["source"], c["device_id"], c["channel_id"]) for c in document["channels"]}
    assert all(c["mappable"] for c in document["channels"])
    for entry in document["unmapped"]:
        assert (entry["source"], entry["device_id"], entry["channel_id"]) in live

    assert str(path) in capsys.readouterr().out


def test_build_dim_can_read_the_live_channel_file_this_stage_writes(
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
) -> None:
    """The two stages compose: discover writes it, build-dim reads it (PLAN.md §9).

    ``stages/dim.py`` treats every entry of ``channels[]`` as a live channel, so
    a placeholder breaker leaking in there would produce a WARN telling the
    operator to label a circuit that can never emit a row.
    """
    dim = pytest.importorskip("energy_capture.stages.dim")

    discover(map_path=map_path, leviton_source=leviton_source, bryant_source=bryant_source)
    live = dim.load_live_channels(live_channels_path(map_path))

    assert (SOURCE_LEVITON, HUB_A, "breaker_p11") in live
    assert (SOURCE_BRYANT, SERIAL, "zone_1") in live
    for skipped in ("breaker_p5", "breaker_p7", "ct_3_a", "zone_2"):
        assert not any(key[2] == skipped for key in live), skipped


def test_discover_then_build_dim_warns_about_the_channels_nobody_mapped(
    map_path: Path,
    settings: Settings,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The full round trip, with no argument passed between the two commands.

    PLAN.md §9 promises an unmapped live channel is WARNed by ``build-dim``. The
    operator runs ``energycap discover`` and then ``energycap build-dim`` — they
    do not hand one command's output to the other. So ``build()`` must find the
    sidecar beside the map on its own; when it does not, that promise is dead
    code that no test of either stage alone would notice.
    """
    dim = pytest.importorskip("energy_capture.stages.dim")

    write_map(
        map_path,
        {
            "source": SOURCE_LEVITON,
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "label": "Kitchen Receptacles",
        },
    )
    discover(map_path=map_path, leviton_source=leviton_source, bryant_source=bryant_source)

    with caplog.at_level("WARNING"):
        summary = dim.build(map_path=map_path, dry_run=True, settings=settings)

    # It found the sidecar without being told where it was.
    assert summary["live_channels_path"] == str(live_channels_path(map_path))
    assert summary["live_channels"] == len(dim.load_live_channels(live_channels_path(map_path)))
    assert summary["rows"] == 1

    # The one mapped channel is quiet; the live ones nobody named are not.
    assert f"{SOURCE_LEVITON}/{HUB_A}/breaker_p11" not in summary["unmapped"]
    assert f"{SOURCE_BRYANT}/{SERIAL}/zone_1" in summary["unmapped"]
    assert summary["unmapped_count"] == len(summary["unmapped"]) > 1
    warned = {
        getattr(record, "energy_fields", {}).get("channel_id")
        for record in caplog.records
        if record.getMessage() == "dim_unmapped_live_channel"
    }
    assert "zone_1" in warned
    assert "breaker_p11" not in warned

    # Objects the collectors never emit rows for must not be nagged about.
    for skipped in ("breaker_p5", "breaker_p7", "ct_3_a", "zone_2"):
        assert not any(skipped in item for item in summary["unmapped"]), skipped


def test_build_dim_uses_no_sidecar_when_discover_has_never_run(
    map_path: Path,
    settings: Settings,
) -> None:
    """A first build, before any discovery, is not an error — just no WARNs."""
    dim = pytest.importorskip("energy_capture.stages.dim")

    write_map(
        map_path,
        {
            "source": SOURCE_LEVITON,
            "device_id": HUB_A,
            "channel_id": "breaker_p11",
            "label": "Kitchen Receptacles",
        },
    )
    assert not live_channels_path(map_path).exists()

    summary = dim.build(map_path=map_path, dry_run=True, settings=settings)
    assert summary["live_channels_path"] is None
    assert summary["live_channels"] == 0
    assert summary["unmapped"] == []


@pytest.mark.parametrize("via", ["argument", "environment"])
def test_writing_the_live_channel_file_can_be_turned_off(
    via: str,
    map_path: Path,
    leviton_source: LevitonSource,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    if via == "environment":
        monkeypatch.setenv(discover_module.ENV_NO_WRITE, "1")

    summary = discover(
        map_path=map_path,
        leviton_source=leviton_source,
        sources=(SOURCE_LEVITON,),
        write_live_channels=(via != "argument"),
    )
    assert "live_channels_path" not in summary
    assert not live_channels_path(map_path).exists()
    captured = capsys.readouterr()
    assert "NOT written" in captured.out
    assert "not writing" not in captured.err  # disabled is not a warning


def test_an_unwritable_sidecar_warns_and_does_not_fail_the_run(
    tmp_path: Path,
    leviton_source: LevitonSource,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")

    summary = discover(
        map_path=blocked / "channel_map.json",
        leviton_source=leviton_source,
        sources=(SOURCE_LEVITON,),
    )
    captured = capsys.readouterr()
    assert "WARNING: could not write" in captured.err
    assert "live_channels_path" not in summary
    assert "breaker_p11" in captured.out


# ============================================================================
# --json
# ============================================================================


def test_json_mode_prints_only_the_skeleton_on_stdout(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
) -> None:
    """``--json`` output must survive a pipe into ``json.load``."""
    discover(
        map_path=map_path,
        leviton_source=leviton_source,
        bryant_source=bryant_source,
        json_only=True,
    )
    captured = capsys.readouterr()

    document = json.loads(captured.out)
    assert set(document) == {"mappings"}
    assert document == live_channels(map_path)["skeleton"]

    # The human table still happened — on stderr, so stdout stays parseable.
    assert "=== channel_map.json skeleton" in captured.err
    assert "breaker_p11" in captured.err


# ============================================================================
# Degradation — one cloud down must not take the report with it
# ============================================================================


def test_one_source_failing_still_prints_the_other(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    settings: Settings,
    leviton_source: LevitonSource,
) -> None:
    dead = BryantStatusSource(
        settings,
        client=FakeCarrierClient(error=SourceAuthError("carrier auth: 401 rejected")),
        device_id=SERIAL,
        username="fake@example.invalid",
    )

    summary = discover(map_path=map_path, leviton_source=leviton_source, bryant_source=dead)
    out = capsys.readouterr().out

    assert "UNAVAILABLE" in out and "401 rejected" in out
    assert "breaker_p11" in out, "Leviton must still print"
    assert summary["sources_failed"] == [SOURCE_BRYANT]
    assert summary["sources_ok"] == [SOURCE_LEVITON]

    document = live_channels(map_path)
    assert document["sources"]["bryant"]["ok"] is False
    assert document["sources"]["bryant"]["channels"] == 0
    assert any(c["source"] == SOURCE_LEVITON for c in document["channels"])


def test_a_missing_credential_is_a_clear_line_not_a_traceback(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    leviton_source: LevitonSource,
) -> None:
    """Bryant has no credentials here; Leviton is injected and still prints."""
    monkeypatch.delenv("CARRIER_USERNAME", raising=False)
    monkeypatch.delenv("CARRIER_PASSWORD", raising=False)
    from energy_capture.config import reset_settings_cache

    reset_settings_cache()

    summary = discover(map_path=map_path, leviton_source=leviton_source)
    out = capsys.readouterr().out

    assert summary["sources_failed"] == [SOURCE_BRYANT]
    assert "CARRIER_" in out
    assert "Traceback" not in out


def test_every_source_failing_raises_after_the_report_is_printed(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    settings: Settings,
) -> None:
    dead_bryant = BryantStatusSource(
        settings,
        client=FakeCarrierClient(error=SourceAuthError("carrier: nope")),
        device_id=SERIAL,
        username="fake@example.invalid",
    )

    class DeadLeviton:
        async def start(self) -> None:
            raise SourceAuthError("leviton login: 401")

        async def close(self) -> None:
            return None

    with pytest.raises(DiscoveryFailed):
        discover(map_path=map_path, leviton_source=DeadLeviton(), bryant_source=dead_bryant)

    captured = capsys.readouterr()
    assert "leviton login: 401" in captured.out
    assert "carrier: nope" in captured.out

    # A sidecar claiming "nothing is live" would tell build-dim the panel is
    # empty — the opposite of the truth. Nothing is written.
    assert not live_channels_path(map_path).exists()
    assert "not writing" in captured.err


def test_an_unknown_source_name_is_rejected(map_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown source"):
        discover(map_path=map_path, sources=("nest",))


def test_no_password_ever_reaches_the_output(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    settings: Settings,
) -> None:
    """CLAUDE.md rule 8 — the error path is where a credential would leak."""
    password = settings.leviton_password.get_secret_value()

    class LeakySource:
        async def start(self) -> None:
            raise SourceAuthError(f"login rejected for password {password}")

        async def close(self) -> None:
            return None

    discover(
        map_path=map_path,
        leviton_source=LeakySource(),
        bryant_source=BryantStatusSource(
            settings,
            client=discover_module._RecordingGraphQLClient(FakeCarrierClient()),
            device_id=SERIAL,
            username="fake@example.invalid",
        ),
    )
    captured = capsys.readouterr()
    assert password not in captured.out + captured.err
    assert "REDACTED" in captured.out


# ============================================================================
# The raw dump — evidence for PLAN.md §7.3's UNVERIFIED fields
# ============================================================================


def test_the_dump_captures_the_raw_leviton_and_carrier_responses(
    tmp_path: Path,
    map_path: Path,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dump = tmp_path / "discover-raw.json"
    summary = discover(
        map_path=map_path,
        leviton_source=leviton_source,
        bryant_source=bryant_source,
        dump_path=dump,
    )

    assert summary["dump_path"] == str(dump)
    document = json.loads(dump.read_text(encoding="utf-8"))
    leviton_raw = document["sources"]["leviton"]
    bryant_raw = document["sources"]["bryant"]

    # Leviton: the untouched bodies, fields and all — including the ones the
    # pipeline deliberately ignores (energyConsumption, lineFrequency, ...).
    hub = next(w for w in leviton_raw["iotWhems"] if w["id"] == HUB_A)
    assert hub["version"] == "2.2.0" and hub["panelSize"] == 200
    # One copy per object, even though the hierarchy is fetched twice.
    assert [w["id"] for w in leviton_raw["iotWhems"]] == [HUB_A, HUB_B]
    breaker = leviton_raw["residentialBreakers"][HUB_A][0]
    assert breaker["id"] == "4C45565275C6_A65E"
    assert "energyConsumption" in breaker
    assert leviton_raw["iotCts"][HUB_A][2]["usageType"] == "NOT_USED"

    # Carrier: the whole GraphQL response, which is what settles §7.3.
    record = bryant_raw["graphql"][0]
    assert record["operation"] == "getInfinityStatus"
    assert record["variables"] == {"serial": SERIAL}
    status = record["data"]["infinityStatus"]
    assert status["cfgem"] == "F"
    assert status["odu"]["opstat"] == "off"
    assert len(status["zones"]) == 8

    # And it is written 0600: it is a complete map of the house's hardware.
    assert dump.stat().st_mode & 0o777 == 0o600
    assert str(dump) in capsys.readouterr().out


def test_the_dump_holds_no_credential(
    tmp_path: Path,
    map_path: Path,
    settings: Settings,
    leviton_source: LevitonSource,
    bryant_source: BryantStatusSource,
) -> None:
    """CLAUDE.md rule 8, applied to the one file that is *meant* to be raw.

    The dump exists to capture upstream bodies verbatim, so "we scrub it" is not
    the answer — 0600 is. But verbatim must still stop at the credentials: the
    Leviton login response's ``id`` **is** the session token, and a session token
    or a password sitting in a file an operator will paste into an issue or a
    fixture is exactly the leak the mode bits cannot prevent.
    """
    dump = tmp_path / "discover-raw.json"
    discover(
        map_path=map_path,
        leviton_source=leviton_source,
        bryant_source=bryant_source,
        dump_path=dump,
    )

    text = dump.read_text(encoding="utf-8")
    session_token = leviton_fixture("login")["id"]
    for secret in (
        settings.leviton_password.get_secret_value(),
        settings.carrier_password.get_secret_value(),
        session_token,
    ):
        assert secret not in text
    assert "authorization" not in text.lower()
    assert dump.stat().st_mode & 0o777 == 0o600


def test_the_dump_also_captures_the_daily_energy_response(
    tmp_path: Path,
    map_path: Path,
    settings: Settings,
) -> None:
    """DEVIATIONS.md #75.8: one captured ``getInfinityEnergy`` settles four questions."""
    client = FakeCarrierClient(
        by_operation={
            "getInfinityStatus": bryant_fixture("status_single_zone"),
            "getInfinityEnergy": bryant_fixture("energy_response"),
        }
    )
    source = BryantStatusSource(
        settings,
        client=discover_module._RecordingGraphQLClient(client),
        device_id=SERIAL,
        username="fake@example.invalid",
    )
    dump = tmp_path / "raw.json"

    discover(
        map_path=map_path,
        bryant_source=source,
        sources=(SOURCE_BRYANT,),
        dump_path=dump,
    )

    records = json.loads(dump.read_text(encoding="utf-8"))["sources"]["bryant"]["graphql"]
    operations = [record["operation"] for record in records]
    assert operations == ["getInfinityStatus", "getInfinityEnergy"]
    energy = records[1]["data"]["infinityEnergy"]
    assert "energyConfig" in energy and "energyPeriods" in energy

    # Exactly one extra request, and only because a dump was asked for.
    assert client.operations.count("getInfinityEnergy") == 1


def test_the_energy_probe_only_runs_in_dump_mode(
    map_path: Path,
    settings: Settings,
) -> None:
    client = FakeCarrierClient()
    source = BryantStatusSource(
        settings,
        client=discover_module._RecordingGraphQLClient(client),
        device_id=SERIAL,
        username="fake@example.invalid",
    )
    discover(map_path=map_path, bryant_source=source, sources=(SOURCE_BRYANT,))
    assert client.operations == ["getInfinityStatus"]


def test_a_failing_energy_probe_does_not_fail_the_run(
    tmp_path: Path,
    map_path: Path,
    settings: Settings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The probe is evidence-gathering; its failure is itself evidence."""
    source = BryantStatusSource(
        settings,
        # Every operation returns the status payload, so getInfinityEnergy has
        # no infinityEnergy object and the daily stage rejects it.
        client=discover_module._RecordingGraphQLClient(FakeCarrierClient()),
        device_id=SERIAL,
        username="fake@example.invalid",
    )
    summary = discover(
        map_path=map_path,
        bryant_source=source,
        sources=(SOURCE_BRYANT,),
        dump_path=tmp_path / "raw.json",
    )

    assert summary["sources_ok"] == [SOURCE_BRYANT]
    assert "energy_probe=" in capsys.readouterr().out
    facts = live_channels(map_path)["sources"]["bryant"]["facts"]
    assert facts["energy_probe"] != "ok"


def test_the_dump_is_off_by_default_and_says_how_to_turn_it_on(
    capsys: pytest.CaptureFixture[str],
    map_path: Path,
    leviton_source: LevitonSource,
) -> None:
    summary = discover(
        map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,)
    )
    out = capsys.readouterr().out

    assert "dump_path" not in summary
    assert ENV_DUMP in out
    assert "UNVERIFIED" in out


def test_the_dump_path_can_come_from_the_environment(
    tmp_path: Path,
    map_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    leviton_source: LevitonSource,
) -> None:
    dump = tmp_path / "from-env.json"
    monkeypatch.setenv(ENV_DUMP, str(dump))

    summary = discover(
        map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,)
    )
    assert summary["dump_path"] == str(dump)
    assert json.loads(dump.read_text(encoding="utf-8"))["sources"]["leviton"]["iotWhems"]


def test_an_unwritable_dump_warns_and_the_report_still_prints(
    tmp_path: Path,
    map_path: Path,
    leviton_source: LevitonSource,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocked = tmp_path / "file"
    blocked.write_text("not a directory", encoding="utf-8")

    summary = discover(
        map_path=map_path,
        leviton_source=leviton_source,
        sources=(SOURCE_LEVITON,),
        dump_path=blocked / "dump.json",
    )
    captured = capsys.readouterr()
    assert "WARNING: could not write" in captured.err
    assert "dump_path" not in summary
    assert "breaker_p11" in captured.out


# ============================================================================
# Read-only-ness
# ============================================================================


def test_discover_never_touches_the_hub_bandwidth_or_the_spool(
    map_path: Path,
    leviton_client: FakeLevitonClient,
    leviton_source: LevitonSource,
    spool_dir: Path,
) -> None:
    """PLAN.md §6.4: a stray ``bandwidth`` PUT is a self-inflicted data gap."""
    discover(map_path=map_path, leviton_source=leviton_source, sources=(SOURCE_LEVITON,))

    assert "set_whem_bandwidth" not in leviton_client.calls
    assert not (spool_dir / "spool.db").exists()
