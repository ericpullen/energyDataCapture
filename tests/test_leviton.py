"""Leviton source tests — offline, fixture-driven (PLAN.md §6, §15).

Every test here runs against recorded ``my.leviton.com`` JSON in
``tests/fixtures/leviton/``, replayed through a fake client that speaks
``aioleviton``'s method names and returns *real* ``aioleviton`` model objects.
So the path under test is the production one end to end — raw API JSON →
``aioleviton`` models → :class:`LevitonAdapter` readings → row mapping →
:class:`~energy_capture.model.Observation` — with only the socket replaced.

No test may ever reach the network (CLAUDE.md "Testing"), and no real
credentials exist in this environment.

What is pinned here, in order of how much it would hurt to get wrong:

1. **``bandwidth: 0`` is unreachable.** Firmware 2.1.0 drops a hub off the cloud
   for 10–20s when it receives one, which is a self-inflicted data gap. Pinned
   three ways: the module's single call site, the absence of any bandwidth
   parameter in the public surface, and a live keepalive round.
2. **Gaps stay gaps.** Null API fields, a null CT second leg, and a poll cycle
   that fails after its retries all produce *absent rows*, never zeros.
3. **Verbatim zeros.** Firmware v2's spurious zeros pass through untouched.
4. **``position``, never the API's breaker ``id``** — fw ≥2.2.0 mutates ids.
"""

from __future__ import annotations

import ast
import inspect
import io
import json
import stat
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from aioleviton import (
    AuthToken,
    Breaker,
    Ct,
    LevitonAuthError,
    LevitonConnectionError,
    LevitonTokenExpired,
    Permission,
    Residence,
    Whem,
)

from energy_capture import logging as ec_logging
from energy_capture.config import LEVITON_INGEST_MODES, Settings
from energy_capture.health import StatusStore
from energy_capture.model import SOURCE_LEVITON, Observation
from energy_capture.sources import leviton as leviton_module
from energy_capture.sources import leviton_ws as ws_module
from energy_capture.sources.base import SourceAuthError, SourceTransientError
from energy_capture.sources.leviton import (
    BANDWIDTH_HIGH,
    INGEST_HYBRID,
    INGEST_REST,
    INGEST_WS,
    KEEPALIVE_INTERVAL_S,
    LOGIN_FAILURE_BACKOFF_S,
    LOGIN_MIN_INTERVAL_S,
    MIN_BREAKER_POSITION,
    STATUS_SECTION_INGEST,
    VALUE_SOURCE_REST,
    VALUE_SOURCE_REST_FALLBACK,
    VALUE_SOURCE_WITHHELD,
    VALUE_SOURCE_WS,
    WS_TICK_INTERVAL_S,
    BreakerReading,
    CtReading,
    LevitonAdapter,
    LevitonSource,
    LevitonTokenCache,
    breaker_channel_id,
    ct_channel_id,
)
from energy_capture.sources.leviton_ws import (
    REASON_STALLED,
    STALL_TIMEOUT_S,
    SYNC_MODE_TIMEOUT,
    WATCHDOG_INTERVAL_S,
    WS_MODEL_CT,
    WS_MODEL_BREAKER,
    WS_MODEL_HUB,
    LevitonWebSocketIngester,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "leviton"

HUB_A = "1000_AAAA_1111"
HUB_B = "1000_BBBB_2222"


def load_fixture(name: str) -> Any:
    """Read one recorded response body."""
    return json.loads((FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------- fake client


class FakeLevitonClient:
    """A stand-in for ``aioleviton.LevitonClient`` backed by the fixtures.

    It returns genuine ``aioleviton`` model objects (``Whem``, ``Breaker``,
    ``Ct``, …) parsed from the recorded JSON, so the field-name mapping in
    ``sources/leviton.py`` is exercised against the real upstream parser rather
    than against a hand-written double.

    ``fail(op, exc, ...)`` queues exceptions for the next calls to ``op``, which
    is how the 502-retry, 401-relogin and failed-cycle tests are driven.
    """

    def __init__(self) -> None:
        self.login_payload: dict[str, Any] = load_fixture("login")
        self.permissions: list[dict[str, Any]] = load_fixture("permissions")
        self.residences: list[dict[str, Any]] = load_fixture("residences")
        self.whems: list[dict[str, Any]] = load_fixture("whems")
        self.breakers: dict[str, list[dict[str, Any]]] = {
            HUB_A: load_fixture(f"breakers_{HUB_A}"),
            HUB_B: load_fixture(f"breakers_{HUB_B}"),
        }
        self.cts: dict[str, list[dict[str, Any]]] = {
            HUB_A: load_fixture(f"cts_{HUB_A}"),
            HUB_B: load_fixture(f"cts_{HUB_B}"),
        }

        self.calls: list[str] = []
        self.bandwidth_puts: list[tuple[str, int]] = []
        self.login_count = 0
        self.restored: list[tuple[str, str]] = []
        self.token: str | None = None
        self.user_id: str | None = None
        self._errors: dict[str, list[Exception]] = {}

    # ------------------------------------------------------ failure injection
    def fail(self, op: str, *errors: Exception) -> None:
        """Queue ``errors`` to be raised by the next calls to ``op``."""
        self._errors.setdefault(op, []).extend(errors)

    def fail_always(self, op: str, error: Exception) -> None:
        """Make ``op`` fail forever (a cycle that never recovers)."""
        self._errors.setdefault(op, []).append(error)
        self._errors[op].append(_Repeat(error))

    def heal(self) -> None:
        """Clear every queued failure — the upstream came back."""
        self._errors.clear()

    def _enter(self, op: str) -> None:
        self.calls.append(op)
        queue = self._errors.get(op)
        if not queue:
            return
        head = queue[0]
        if isinstance(head, _Repeat):
            raise head.error
        raise queue.pop(0)

    def count(self, op: str) -> int:
        return sum(1 for call in self.calls if call == op)

    # -------------------------------------------------------------- auth API
    async def login(self, email: str, password: str, code: str | None = None) -> AuthToken:
        self.login_count += 1
        self._enter("login")
        data = self.login_payload
        self.token = data["id"]
        self.user_id = data["userId"]
        return AuthToken(
            token=data["id"],
            ttl=data["ttl"],
            created=data["created"],
            user_id=data["userId"],
            user=data.get("user", {}),
        )

    def restore_session(self, token: str, user_id: str) -> None:
        self.restored.append((token, user_id))
        self.token = token
        self.user_id = user_id

    # --------------------------------------------------------- discovery API
    async def get_permissions(self) -> list[Permission]:
        self._enter("get_permissions")
        return [Permission.from_api(p) for p in self.permissions]

    async def get_residences(self, account_id: int) -> list[Residence]:
        self._enter("get_residences")
        return [Residence.from_api(r) for r in self.residences]

    async def get_residence_from_permission(self, permission_id: int) -> Residence:
        self._enter("get_residence_from_permission")
        return Residence.from_api(self.residences[0])

    async def get_whems(self, residence_id: int) -> list[Whem]:
        self._enter("get_whems")
        return [Whem.from_api(w) for w in self.whems]

    async def get_whem_breakers(self, whem_id: str) -> list[Breaker]:
        self._enter("get_whem_breakers")
        return [Breaker.from_api(b) for b in self.breakers.get(whem_id, [])]

    async def get_cts(self, whem_id: str) -> list[Ct]:
        self._enter("get_cts")
        return [Ct.from_api(c) for c in self.cts.get(whem_id, [])]

    # --------------------------------------------------------- keepalive API
    async def set_whem_bandwidth(self, whem_id: str, bandwidth: int) -> None:
        self._enter("set_whem_bandwidth")
        if bandwidth != 1:
            # The hub's own behaviour, encoded: fw 2.1.0 drops off the cloud for
            # 10-20 seconds on `{"bandwidth": 0}` (PLAN.md §6.4). If any code
            # path ever reaches here with 0, the test that drove it must fail.
            raise AssertionError(
                f"bandwidth={bandwidth!r} PUT to {whem_id}: only 1 is ever permitted"
            )
        self.bandwidth_puts.append((whem_id, bandwidth))


class _Repeat:
    """Marker: raise this error for every subsequent call, not just the next."""

    def __init__(self, error: Exception) -> None:
        self.error = error


class FakeClock:
    """Deterministic monotonic clock + sleep, so no test waits on real time."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


# -------------------------------------------------------------------- fixtures


@pytest.fixture
def client() -> FakeLevitonClient:
    return FakeLevitonClient()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def token_path(spool_dir: Path) -> Path:
    return spool_dir / "tokens" / "leviton.json"


@pytest.fixture
def adapter(
    client: FakeLevitonClient, clock: FakeClock, token_path: Path
) -> LevitonAdapter:
    return LevitonAdapter(
        username="test-leviton@example.invalid",
        password="not-a-real-leviton-password",
        token_path=token_path,
        client=client,
        # The real schedule is (2s, 5s); tests keep the *shape* (two retries)
        # without the wall-clock cost. FakeClock records what was requested.
        retry_waits=(2.0, 5.0),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


@pytest.fixture
def status_store(spool_dir: Path) -> StatusStore:
    return StatusStore(path=spool_dir / "status.json", load_existing=False)


@pytest.fixture
def source(
    settings: Settings,
    adapter: LevitonAdapter,
    status_store: StatusStore,
    clock: FakeClock,
) -> LevitonSource:
    return LevitonSource(
        settings,
        adapter=adapter,
        status_store=status_store,
        monotonic=clock.monotonic,
    )


@pytest.fixture
async def started_source(source: LevitonSource) -> LevitonSource:
    await source.start()
    return source


def rows_by_channel(rows: list[Observation]) -> dict[tuple[str, str], dict[str, float]]:
    """``(device_id, channel_id) -> {metric: value}`` for readable assertions."""
    out: dict[tuple[str, str], dict[str, float]] = {}
    for row in rows:
        out.setdefault((row.device_id, row.channel_id), {})[row.metric] = row.value
    return out


# =============================================================== row mapping


async def test_two_pole_breaker_sums_power_means_amps_sums_volts(
    started_source: LevitonSource,
) -> None:
    """PLAN.md §6.5 arithmetic for a 2-pole breaker, exactly.

    watts = pole sum; amps = per-pole **mean** (one load, one current, measured
    twice); volts = leg **sum** (240V across both legs).
    """
    rows = await started_source.poll()
    channel = rows_by_channel(rows)[(HUB_A, "breaker_p11")]

    assert channel["watts"] == pytest.approx(1200 + 1150)
    assert channel["amps"] == pytest.approx((5.1 + 5.3) / 2)
    assert channel["volts"] == pytest.approx(120 + 121)


async def test_two_pole_breaker_is_one_channel_not_two(
    started_source: LevitonSource,
) -> None:
    """A 2-pole breaker occupies two slots but is ONE channel (§6.5)."""
    rows = await started_source.poll()
    breaker_channels = {
        row.channel_id for row in rows if row.channel_id.startswith("breaker_p")
    }
    assert "breaker_p11" in breaker_channels
    # No per-pole variants, and no channel for the paired slot.
    assert not any(c.endswith(("_a", "_b")) for c in breaker_channels)
    assert "breaker_p12" not in breaker_channels


async def test_fw22_suffixed_breaker_id_is_ignored_in_favour_of_position(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """Firmware ≥2.2.0 appends the panel serial to breaker ids (§6.5).

    ``4C45565275C6`` becomes ``4C45565275C6_A65E`` after a firmware update. If
    the id ever reached ``channel_id``, every channel would silently rename
    itself and a year of history would split in two.
    """
    fixture_id = client.breakers[HUB_A][0]["id"]
    assert fixture_id == "4C45565275C6_A65E"  # the fixture really is suffixed

    rows = await started_source.poll()
    channel_ids = {row.channel_id for row in rows}

    assert "breaker_p11" in channel_ids
    for channel_id in channel_ids:
        assert "4C45565275C6" not in channel_id
        assert "A65E" not in channel_id


async def test_single_pole_breaker_uses_the_unsuffixed_fields(
    started_source: LevitonSource,
) -> None:
    rows = await started_source.poll()
    channel = rows_by_channel(rows)[(HUB_A, "breaker_p3")]
    assert channel == {"watts": 0.0, "amps": 0.0, "volts": 121.0}


async def test_spurious_zero_readings_pass_through_verbatim(
    started_source: LevitonSource,
) -> None:
    """Locked decision §2.3: fw v2's spurious zeros are recorded, not filtered.

    A zero is a real reading as far as this pipeline is concerned. Suppressing
    it here would be indistinguishable from a gap downstream, which is exactly
    the confusion ``sample_count`` exists to prevent.
    """
    rows = await started_source.poll()
    by_channel = rows_by_channel(rows)

    # Breaker with 0W but live voltage.
    assert by_channel[(HUB_A, "breaker_p3")]["watts"] == 0.0
    assert by_channel[(HUB_A, "breaker_p3")]["amps"] == 0.0
    # CT pair reporting zeros on both legs.
    assert by_channel[(HUB_B, "ct_4_a")] == {"watts": 0.0, "amps": 0.0}
    assert by_channel[(HUB_B, "ct_4_b")] == {"watts": 0.0, "amps": 0.0}

    # And they really are rows, not absences.
    zero_rows = [row for row in rows if row.value == 0.0]
    assert len(zero_rows) == 6


async def test_placeholder_breakers_are_skipped(
    started_source: LevitonSource,
) -> None:
    """``NONE``/``NONE-1``/``NONE-2`` are dumb-breaker stand-ins, not meters (§6.3)."""
    rows = await started_source.poll()
    channel_ids = {row.channel_id for row in rows}
    assert "breaker_p5" not in channel_ids  # model NONE
    assert "breaker_p7" not in channel_ids  # model NONE-2


@pytest.mark.parametrize("model", ["NONE", "NONE-1", "NONE-2", "none", " None-1 "])
def test_every_placeholder_model_spelling_is_recognised(model: str) -> None:
    assert BreakerReading(position=1, poles=1, model=model).is_placeholder


def test_a_real_breaker_model_is_not_a_placeholder() -> None:
    assert not BreakerReading(position=1, poles=1, model="LB230-2P").is_placeholder


# ------------------------------------------------- un-positioned breakers
# Observed on both live hubs on 2026-08-22, in the 37 minutes between plugging
# in 20 smart breakers and finishing the positioning wizard. Leviton omits
# `position` while a breaker is enrolled but not yet located, `aioleviton`
# defaults the missing key to 0, and `breaker_p{position}` turned that default
# into rows on a slot no panel has.


async def test_an_unpositioned_breaker_produces_no_rows(
    started_source: LevitonSource,
    client: FakeLevitonClient,
) -> None:
    """No position means no channel_id, so it means no rows (cardinal rule 1).

    ``breaker_p0`` is not a slot — Leviton panels number from 1. Writing to it
    would attribute a real circuit's watts to a fiction, and a downstream reader
    cannot tell an invented slot from a real one.
    """
    client.breakers[HUB_A].extend(load_fixture("breakers_unpositioned"))
    await started_source.discover(force=True)

    rows = await started_source.poll()
    channel_ids = {row.channel_id for row in rows}

    assert "breaker_p0" not in channel_ids
    # Both spellings of the state are in the fixture and both are caught: the
    # key absent, and the key present but explicitly null.
    assert len([b for b in client.breakers[HUB_A] if not b.get("position")]) == 2
    assert not any(cid.startswith("breaker_p0") for cid in channel_ids)

    # ...and the guard is narrow: every positioned breaker still reports.
    assert {"breaker_p11", "breaker_p3", "breaker_p9", "breaker_p13"} <= channel_ids


async def test_the_unpositioned_warning_is_logged_once_per_breaker(
    started_source: LevitonSource,
    client: FakeLevitonClient,
) -> None:
    """Loud once, not 2,880 times a day.

    Only the operator can fix this — the remedy is the positioning wizard in the
    Leviton app — so silence would be wrong. But the condition persists until
    somebody walks to the panel, and a 30s loop must not turn one fixable
    problem into a log flood. The distinct count stays in ``status.json``.
    """
    client.breakers[HUB_A].extend(load_fixture("breakers_unpositioned"))
    await started_source.discover(force=True)

    stream = io.StringIO()
    ec_logging.configure_logging("DEBUG", stream=stream, force=True)
    try:
        for _ in range(3):
            await started_source.poll()
    finally:
        ec_logging.configure_logging("INFO", force=True)

    warnings = [
        json.loads(line)
        for line in stream.getvalue().splitlines()
        if line.strip() and json.loads(line).get("event") == "leviton_breaker_unpositioned"
    ]
    # Two breakers, three cycles, two lines.
    assert len(warnings) == 2
    assert {w["api_id"] for w in warnings} == {
        "4C4556527612_A65E",
        "4C4556527613_A65E",
    }
    for warning in warnings:
        assert warning["level"] == "WARNING"
        assert warning["rows"] == 0
        # The app identifies breakers by slot, which is the one thing this
        # breaker has not got — so the line must name it some other way.
        assert warning["name"] in {"Guest bath", "Well pump"}
        assert "positioning wizard" in warning["detail"]

    assert started_source.ingest_status()["unpositioned_breakers"] == 2


def test_position_zero_is_not_a_slot() -> None:
    assert BreakerReading(position=0, poles=1, model="LB120-0ST").is_unpositioned
    assert BreakerReading(position=-1, poles=1, model="LB120-0ST").is_unpositioned
    assert not BreakerReading(
        position=MIN_BREAKER_POSITION, poles=1, model="LB120-0ST"
    ).is_unpositioned
    assert MIN_BREAKER_POSITION == 1


@pytest.mark.parametrize("payload", [{}, {"position": None}, {"position": 0}])
def test_the_zero_position_comes_from_the_client_library(payload: dict[str, Any]) -> None:
    """Pins the root cause, so a future upstream fix cannot pass unnoticed.

    ``aioleviton``'s ``models.py`` does ``position=data.get("position", 0)``
    against an ``int``-typed field, so a missing position arrives as a valid
    -looking slot number rather than as ``None``. This project cannot rely on
    that being fixed, and if it ever is — ``None`` instead of ``0`` — the
    ``or 0`` in :meth:`BreakerReading.from_model` keeps the guard working.
    """
    breaker = types.SimpleNamespace(
        position=payload.get("position", 0),
        poles=1,
        model="LB120-0ST",
        id="4C4556527612_A65E",
        name="Guest bath",
    )
    assert BreakerReading.from_model(breaker).position == 0
    assert BreakerReading.from_model(breaker).is_unpositioned


async def test_not_used_cts_are_skipped(started_source: LevitonSource) -> None:
    """``usageType == "NOT_USED"`` means the clamp is on nothing (§6.3)."""
    rows = await started_source.poll()
    channel_ids = {row.channel_id for row in rows}
    assert "ct_3_a" not in channel_ids
    assert "ct_3_b" not in channel_ids


async def test_ct_pair_emits_one_channel_per_leg(
    started_source: LevitonSource,
) -> None:
    """One ``IotCt`` object is a clamp *pair*: ``ct_{channel}_a`` and ``_b`` (§6.5)."""
    rows = await started_source.poll()
    by_channel = rows_by_channel(rows)
    assert by_channel[(HUB_A, "ct_1_a")] == {"watts": 900.0, "amps": 7.5}
    assert by_channel[(HUB_A, "ct_1_b")] == {"watts": 880.0, "amps": 7.3}


async def test_ct_with_a_null_second_leg_emits_nothing_for_that_leg(
    started_source: LevitonSource,
) -> None:
    """A single-leg CT is a gap on leg B — never a zero (CLAUDE.md rule 1).

    This is the difference between "nothing is clamped to leg B" and "leg B is
    drawing no power", and the two must not be confusable in the archive.
    """
    rows = await started_source.poll()
    by_channel = rows_by_channel(rows)

    assert by_channel[(HUB_A, "ct_2_a")] == {"watts": 340.0, "amps": 2.8}
    assert (HUB_A, "ct_2_b") not in by_channel
    assert not any(row.channel_id == "ct_2_b" for row in rows)


def test_ct_leg_metrics_are_none_not_zero_for_a_missing_leg() -> None:
    ct = CtReading(channel=2, active_power=340, rms_current=2.8)
    assert ct.leg_metrics("b") == {"watts": None, "amps": None}
    assert not ct.has_second_leg


async def test_null_breaker_field_emits_no_row_for_that_metric(
    started_source: LevitonSource,
) -> None:
    """A null field is one absent row, not a zero and not a dropped channel."""
    rows = await started_source.poll()
    by_channel = rows_by_channel(rows)

    # Single-pole, power=null: watts absent, amps/volts present.
    assert by_channel[(HUB_A, "breaker_p9")] == {"amps": 1.2, "volts": 120.0}

    # 2-pole with a null second pole: the SUM is unknowable, so no watts row.
    # Amps (mean) and volts (sum) still have both halves.
    p13 = by_channel[(HUB_A, "breaker_p13")]
    assert "watts" not in p13
    assert p13["amps"] == pytest.approx((3.0 + 3.1) / 2)
    assert p13["volts"] == pytest.approx(241.0)


async def test_hub_emits_volts_and_hz_per_leg(
    started_source: LevitonSource,
) -> None:
    """Hub-level rows land on ``panel_leg_a`` / ``panel_leg_b`` (§6.5)."""
    rows = await started_source.poll()
    by_channel = rows_by_channel(rows)

    assert by_channel[(HUB_A, "panel_leg_a")] == {"volts": 121.0, "hz": 60.0}
    assert by_channel[(HUB_A, "panel_leg_b")] == {"volts": 122.0, "hz": 60.0}

    units = {row.metric: row.unit for row in rows if row.channel_id.startswith("panel_leg")}
    assert units == {"volts": "V", "hz": "Hz"}


async def test_no_panel_total_is_synthesised(started_source: LevitonSource) -> None:
    """§6.3: there is no panel-level power field; do NOT invent one in raw."""
    rows = await started_source.poll()
    panel_metrics = {
        row.metric for row in rows if row.channel_id.startswith("panel_leg")
    }
    assert panel_metrics == {"volts", "hz"}


async def test_disconnected_hub_with_null_readings_emits_no_rows_for_itself(
    started_source: LevitonSource,
) -> None:
    """Hub B is offline and reports nulls: no volts/hz rows, no fabricated zeros."""
    rows = await started_source.poll()
    hub_b_panel = [
        row
        for row in rows
        if row.device_id == HUB_B and row.channel_id.startswith("panel_leg")
    ]
    assert hub_b_panel == []


async def test_empty_breaker_list_is_normal_not_an_error(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """There are currently few/no smart breakers; discovery must cope (§6)."""
    assert client.breakers[HUB_B] == []
    rows = await started_source.poll()
    assert any(row.device_id == HUB_B for row in rows)  # the CTs still report
    assert not any(
        row.device_id == HUB_B and row.channel_id.startswith("breaker_") for row in rows
    )


async def test_energy_counters_are_never_collected(
    started_source: LevitonSource,
) -> None:
    """§6.3: fw v2 turned ``energyConsumption``/``energyImport`` into resettable
    counters. They are in the fixtures and must not become rows — kWh is derived
    in the rollup from observed-time watts instead (§2.5)."""
    assert "energyConsumption" in client_fixture_keys()
    rows = await started_source.poll()
    assert {row.metric for row in rows} <= {"watts", "amps", "volts", "hz"}


def client_fixture_keys() -> set[str]:
    keys: set[str] = set()
    for entry in load_fixture(f"breakers_{HUB_A}"):
        keys |= set(entry)
    return keys


async def test_whole_cycle_row_count_is_exactly_what_the_fixtures_justify(
    started_source: LevitonSource,
) -> None:
    """One explicit total, so an accidental extra (or missing) row is loud."""
    rows = await started_source.poll()
    expected = {
        (HUB_A, "panel_leg_a"): {"volts", "hz"},
        (HUB_A, "panel_leg_b"): {"volts", "hz"},
        (HUB_A, "breaker_p11"): {"watts", "amps", "volts"},
        (HUB_A, "breaker_p3"): {"watts", "amps", "volts"},
        (HUB_A, "breaker_p9"): {"amps", "volts"},
        (HUB_A, "breaker_p13"): {"amps", "volts"},
        (HUB_A, "ct_1_a"): {"watts", "amps"},
        (HUB_A, "ct_1_b"): {"watts", "amps"},
        (HUB_A, "ct_2_a"): {"watts", "amps"},
        (HUB_B, "ct_4_a"): {"watts", "amps"},
        (HUB_B, "ct_4_b"): {"watts", "amps"},
    }
    actual = {key: set(metrics) for key, metrics in rows_by_channel(rows).items()}
    assert actual == expected
    assert len(rows) == sum(len(m) for m in expected.values()) == 24


async def test_all_rows_from_one_cycle_share_a_single_ts_utc(
    started_source: LevitonSource,
) -> None:
    """§6.5: one timestamp per source per poll cycle, at µs precision."""
    rows = await started_source.poll()
    stamps = {row.ts_utc for row in rows}
    assert len(stamps) == 1
    stamp = stamps.pop()
    assert stamp.tzinfo is not None
    assert stamp.utcoffset().total_seconds() == 0

    second = await started_source.poll()
    assert {row.ts_utc for row in second} != {stamp}  # a new instant per cycle


async def test_rows_are_tagged_as_the_leviton_source(
    started_source: LevitonSource,
) -> None:
    rows = await started_source.poll()
    assert {row.source for row in rows} == {SOURCE_LEVITON}
    assert {row.device_id for row in rows} == {HUB_A, HUB_B}


# ======================================================== failure & gap policy


async def test_a_failed_poll_cycle_emits_exactly_zero_rows(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§15.4 / CLAUDE.md rule 1: a failed cycle writes nothing at all.

    Not a partial cycle, not the previous reading, not zeros — nothing. The loop
    counts the failure and moves on.
    """
    client.fail_always("get_whems", LevitonConnectionError("Server error: HTTP 502"))

    rows: list[Observation] = []
    with pytest.raises(SourceTransientError):
        rows = await started_source.poll()

    assert rows == []
    assert started_source.consecutive_failures == 1


async def test_a_partially_failed_cycle_emits_zero_rows_not_partial_ones(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """Hub A answers, hub B's CT call dies: the whole cycle is discarded.

    Stamping half a response set with a completion time we never reached would
    be a fabrication, however small.
    """
    client.fail_always("get_cts", LevitonConnectionError("Server error: HTTP 504"))

    rows: list[Observation] = []
    with pytest.raises(SourceTransientError):
        rows = await started_source.poll()
    assert rows == []


async def test_transient_502_is_retried_within_the_cycle(
    started_source: LevitonSource, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """§6.6: 502/504 from Leviton's gateway are NORMAL. Retry ~2 times at 2s/5s."""
    before = client.count("get_whems")
    clock.slept.clear()
    client.fail(
        "get_whems",
        LevitonConnectionError("Server error: HTTP 502"),
        LevitonConnectionError("Server error: HTTP 504"),
    )

    rows = await started_source.poll()

    assert rows  # the third attempt succeeded, inside the same cycle
    assert client.count("get_whems") - before == 3
    assert clock.slept == [2.0, 5.0]
    assert started_source.consecutive_failures == 0


async def test_retries_are_exhausted_then_the_cycle_gives_up_quietly(
    started_source: LevitonSource, client: FakeLevitonClient, clock: FakeClock
) -> None:
    before = client.count("get_whems")
    clock.slept.clear()
    client.fail_always("get_whems", LevitonConnectionError("Server error: HTTP 502"))

    with pytest.raises(SourceTransientError):
        await started_source.poll()

    assert client.count("get_whems") - before == 3  # initial + 2 retries
    assert clock.slept == [2.0, 5.0]


async def test_consecutive_failures_accumulate_then_reset_on_success(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§6.6: the consecutive-failure count is what ``status.json`` reports."""
    client.fail(
        "get_whems",
        *[LevitonConnectionError("502")] * 3,
    )
    with pytest.raises(SourceTransientError):
        await started_source.poll()
    assert started_source.consecutive_failures == 1

    client.fail("get_whems", *[LevitonConnectionError("502")] * 3)
    with pytest.raises(SourceTransientError):
        await started_source.poll()
    assert started_source.consecutive_failures == 2

    await started_source.poll()
    assert started_source.consecutive_failures == 0


async def test_the_poll_loop_never_sees_an_aioleviton_exception(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """The adapter is the only thing that knows ``aioleviton`` exists (§2.8)."""
    client.fail_always("get_whems", LevitonConnectionError("boom"))
    with pytest.raises(SourceTransientError) as transient:
        await started_source.poll()
    assert not isinstance(transient.value, LevitonConnectionError)

    client.heal()
    client.fail_always("get_whems", LevitonTokenExpired("Authorization Required"))
    client.fail_always("login", LevitonAuthError("bad credentials"))
    with pytest.raises(SourceAuthError):
        await started_source.poll()


# ============================================================== authentication


async def test_login_caches_the_full_response_at_mode_0600(
    adapter: LevitonAdapter, token_path: Path, client: FakeLevitonClient
) -> None:
    """§6.1: cache the full login response; the ``id`` IS the token."""
    await adapter.start()

    assert client.login_count == 1
    cached = json.loads(token_path.read_text(encoding="utf-8"))
    assert cached["id"] == client.login_payload["id"]
    assert cached["userId"] == client.login_payload["userId"]
    assert cached["ttl"] == client.login_payload["ttl"]
    assert cached["created"] == client.login_payload["created"]
    assert cached["user"] == client.login_payload["user"]

    mode = stat.S_IMODE(token_path.stat().st_mode)
    assert mode == 0o600, f"token cache must be 0600, got {mode:o}"


async def test_a_cached_token_is_validated_and_reused_without_logging_in(
    adapter: LevitonAdapter, token_path: Path, client: FakeLevitonClient
) -> None:
    """§6.1: validate on startup with ``/Person/{id}/residentialPermissions``.

    A restart must not cost a login — Leviton punishes rapid logins and there is
    no refresh endpoint, so the cached token is all we have.
    """
    LevitonTokenCache(token_path).save(client.login_payload)

    await adapter.start()

    assert client.login_count == 0
    assert client.restored == [(client.login_payload["id"], "person-1234")]
    assert client.count("get_permissions") == 1  # the validation probe


async def test_a_rejected_cached_token_triggers_exactly_one_login(
    adapter: LevitonAdapter, token_path: Path, client: FakeLevitonClient
) -> None:
    LevitonTokenCache(token_path).save(
        {"id": "stale-token-value-aaaaaaaa", "userId": "person-1234", "ttl": 1, "created": ""}
    )
    client.fail("get_permissions", LevitonTokenExpired("Authorization Required"))

    await adapter.start()

    assert client.login_count == 1
    cached = json.loads(token_path.read_text(encoding="utf-8"))
    assert cached["id"] == client.login_payload["id"]  # the fresh one replaced it


async def test_a_corrupt_token_cache_is_ignored_rather_than_fatal(
    adapter: LevitonAdapter, token_path: Path, client: FakeLevitonClient
) -> None:
    token_path.write_text("{not json", encoding="utf-8")
    await adapter.start()
    assert client.login_count == 1


async def test_logins_are_never_closer_than_ten_seconds_apart(
    adapter: LevitonAdapter, clock: FakeClock, client: FakeLevitonClient
) -> None:
    """§6.1 HARD FLOOR, enforced in code — Leviton punishes rapid logins.

    Not "we only call login rarely" but "a caller that asks twice in a row is
    made to wait".
    """
    await adapter.start()
    clock.slept.clear()

    await adapter.reauthenticate(reason="test")

    assert client.login_count == 2
    assert clock.slept == [pytest.approx(LOGIN_MIN_INTERVAL_S)]
    assert LOGIN_MIN_INTERVAL_S == 10.0


async def test_a_third_login_also_waits_the_full_floor(
    adapter: LevitonAdapter, clock: FakeClock
) -> None:
    await adapter.start()
    await adapter.reauthenticate(reason="test")
    clock.slept.clear()
    await adapter.reauthenticate(reason="test")
    assert clock.slept == [pytest.approx(LOGIN_MIN_INTERVAL_S)]


async def test_time_already_elapsed_counts_against_the_login_floor(
    adapter: LevitonAdapter, clock: FakeClock
) -> None:
    """The floor is a minimum *interval*, not an unconditional sleep."""
    await adapter.start()
    clock.advance(LOGIN_MIN_INTERVAL_S + 1)
    clock.slept.clear()
    await adapter.reauthenticate(reason="test")
    assert clock.slept == []


async def test_a_failed_login_backs_off_sixty_seconds_before_retrying(
    adapter: LevitonAdapter, clock: FakeClock, client: FakeLevitonClient
) -> None:
    """§6.6: if login fails, back off 60s and keep trying — never hammer."""
    client.fail("login", LevitonAuthError("Invalid credentials"))
    with pytest.raises(SourceAuthError):
        await adapter.start()

    clock.slept.clear()
    await adapter.reauthenticate(reason="retry")

    assert clock.slept == [pytest.approx(LOGIN_FAILURE_BACKOFF_S)]
    assert LOGIN_FAILURE_BACKOFF_S == 60.0


async def test_two_factor_required_is_an_auth_error_not_a_retry_loop(
    adapter: LevitonAdapter, client: FakeLevitonClient
) -> None:
    """Leviton returns 406 for "2FA required" — a human must intervene (§6.1)."""
    from aioleviton import LevitonTwoFactorRequired

    client.fail("login", LevitonTwoFactorRequired("code required"))
    with pytest.raises(SourceAuthError):
        await adapter.start()
    assert client.login_count == 1


async def test_the_token_is_registered_as_a_secret_the_moment_it_is_obtained(
    adapter: LevitonAdapter, client: FakeLevitonClient
) -> None:
    """CLAUDE.md rule 8: the login ``id`` is a bearer credential.

    Registering it at the point of acquisition is what keeps it out of every
    later log line and out of ``status.json``.
    """
    token = client.login_payload["id"]
    await adapter.start()

    assert ec_logging.scrub_text(f"got {token} back") == f"got {ec_logging.REDACTED} back"


async def test_the_token_never_reaches_the_log_stream(
    adapter: LevitonAdapter, client: FakeLevitonClient
) -> None:
    stream = io.StringIO()
    ec_logging.configure_logging("DEBUG", stream=stream, force=True)
    try:
        await adapter.start()
        log = ec_logging.get_logger("leviton")
        log.info("leviton_login_ok", session=client.login_payload["id"])
        output = stream.getvalue()
    finally:
        ec_logging.configure_logging("INFO", force=True)

    assert output.strip()
    assert client.login_payload["id"] not in output


async def test_a_401_during_a_poll_triggers_relogin_and_one_more_attempt(
    started_source: LevitonSource, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """§6.6: 401 → re-login (respecting the 10s floor) → retry the cycle."""
    logins_before = client.login_count
    client.fail("get_whems", LevitonTokenExpired("Authorization Required"))
    clock.slept.clear()

    rows = await started_source.poll()

    assert rows, "the cycle should succeed after re-authenticating"
    assert client.login_count == logins_before + 1
    assert clock.slept == [pytest.approx(LOGIN_MIN_INTERVAL_S)]


async def test_a_401_whose_relogin_also_fails_raises_without_crashing(
    started_source: LevitonSource, client: FakeLevitonClient, status_store: StatusStore
) -> None:
    client.fail_always("get_whems", LevitonTokenExpired("Authorization Required"))
    client.fail_always("login", LevitonAuthError("Invalid credentials"))

    with pytest.raises(SourceAuthError):
        await started_source.poll()

    assert started_source.consecutive_failures == 1
    assert status_store.section("leviton_auth")["consecutive_failures"] == 1

    # And the source is still usable: recovery needs no restart.
    client.heal()
    assert await started_source.poll()


async def test_no_login_happens_per_poll(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§6.1: "never per-poll". One login at startup covers ~60 days."""
    before = client.login_count
    for _ in range(5):
        await started_source.poll()
    assert client.login_count == before


# ================================================================== keepalive


def test_the_keepalive_task_runs_every_fifty_seconds(source: LevitonSource) -> None:
    """§6.4: the official app re-PUTs every 50s; high bandwidth decays in seconds."""
    tasks = {task.name: task for task in source.background_tasks()}
    keepalive = tasks["leviton_keepalive"]

    assert keepalive.interval_s == 50.0 == KEEPALIVE_INTERVAL_S
    assert keepalive.initial_delay_s == 0.0  # the first poll wants fresh data


def test_rediscovery_is_also_scheduled(source: LevitonSource, settings: Settings) -> None:
    """§6.2: re-discover so new smart breakers appear without a restart."""
    tasks = {task.name: task for task in source.background_tasks()}
    assert tasks["leviton_discovery"].interval_s == settings.leviton_discovery_interval_s


async def test_keepalive_puts_bandwidth_one_to_every_connected_hub(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    await started_source.keepalive_round()

    assert client.bandwidth_puts == [(HUB_A, 1)]
    assert all(value == BANDWIDTH_HIGH for _, value in client.bandwidth_puts)


async def test_keepalive_skips_hubs_reporting_connected_false(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§6.4: a disconnected hub cannot answer; PUTting at it only adds failures."""
    assert started_source.connected_hub_ids == (HUB_A,)
    await started_source.keepalive_round()
    assert [hub for hub, _ in client.bandwidth_puts] == [HUB_A]
    assert HUB_B not in [hub for hub, _ in client.bandwidth_puts]


async def test_a_hub_that_reconnects_starts_getting_keepalives(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """Connectivity is re-read every poll, so recovery needs no restart."""
    client.whems[1]["connected"] = True
    await started_source.poll()
    await started_source.keepalive_round()
    assert sorted(hub for hub, _ in client.bandwidth_puts) == [HUB_A, HUB_B]


async def test_keepalive_with_no_connected_hubs_is_a_no_op_not_a_failure(
    started_source: LevitonSource, client: FakeLevitonClient, status_store: StatusStore
) -> None:
    for whem in client.whems:
        whem["connected"] = False
    await started_source.poll()

    await started_source.keepalive_round()

    assert client.bandwidth_puts == []
    assert started_source.keepalive_failures == 0
    assert status_store.section("leviton_keepalive")["connected_hubs"] == 0


# --------------------------------------------------- bandwidth 0 is impossible


def test_the_bandwidth_constant_is_one() -> None:
    """§6.4: 1 = high bandwidth. 0 disconnects a fw-2.1.0 hub for 10–20s."""
    assert BANDWIDTH_HIGH == 1


def test_no_function_in_the_module_accepts_a_bandwidth_argument() -> None:
    """You cannot pass 0 to something that has no parameter to pass it to."""
    offenders: list[str] = []
    for owner_name, owner in vars(leviton_module).items():
        if inspect.isclass(owner) and owner.__module__ == leviton_module.__name__:
            members = [
                (f"{owner_name}.{n}", f)
                for n, f in vars(owner).items()
                if inspect.isfunction(f)
            ]
        elif inspect.isfunction(owner) and owner.__module__ == leviton_module.__name__:
            members = [(owner_name, owner)]
        else:
            continue
        for qualname, func in members:
            if "bandwidth" in inspect.signature(func).parameters:
                offenders.append(qualname)
    assert offenders == []


def test_the_module_has_exactly_one_bandwidth_call_site_and_it_passes_the_constant() -> None:
    """An AST-level pin: the next edit cannot quietly add a second call site.

    Read the module's syntax tree rather than its text, so prose about
    ``bandwidth: 0`` in the docstrings cannot mask (or fake) the real thing.
    """
    tree = ast.parse(Path(leviton_module.__file__).read_text(encoding="utf-8"))

    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_whem_bandwidth"
    ]
    assert len(calls) == 1, "there must be exactly one place that sets bandwidth"
    call = calls[0]
    assert not call.keywords
    assert len(call.args) == 2
    # The value is a *name*, never a literal — and never a literal 0.
    assert isinstance(call.args[1], ast.Name)
    assert call.args[1].id == "bandwidth"

    assigns = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "bandwidth" for t in node.targets)
    ]
    assert len(assigns) == 1
    assert isinstance(assigns[0].value, ast.Name)
    assert assigns[0].value.id == "BANDWIDTH_HIGH"


async def test_the_guard_refuses_to_put_anything_but_one(
    started_source: LevitonSource,
    client: FakeLevitonClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defence against the *next* edit, not the current one.

    If someone ever changes :data:`BANDWIDTH_HIGH`, the PUT must not happen —
    the request is refused before it reaches the client, so a fw-2.1.0 hub never
    receives the value that would knock it off the cloud for 10–20 seconds.
    """
    monkeypatch.setattr(leviton_module, "BANDWIDTH_HIGH", 0)

    with pytest.raises(RuntimeError, match="only 1 is ever permitted"):
        await started_source.adapter.keepalive(HUB_A)

    assert client.bandwidth_puts == []
    assert client.count("set_whem_bandwidth") == 0


async def test_a_keepalive_round_never_sends_zero_even_when_misconfigured(
    started_source: LevitonSource,
    client: FakeLevitonClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The whole round, not just the single call: still no PUT of 0."""
    monkeypatch.setattr(leviton_module, "BANDWIDTH_HIGH", 0)

    await started_source.keepalive_round()  # must not raise: it is a background task

    assert client.bandwidth_puts == []
    assert started_source.keepalive_failures == 1


# ------------------------------------------------------------ keepalive backoff


async def test_repeated_keepalive_failures_back_off_exponentially(
    started_source: LevitonSource,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """§6.4: don't hammer a down API, and record the condition in status.json."""
    client.fail_always("set_whem_bandwidth", LevitonConnectionError("HTTP 502"))

    await started_source.keepalive_round()
    assert started_source.keepalive_failures == 1
    section = status_store.section("leviton_keepalive")
    assert section["consecutive_failures"] == 1
    assert section["backoff_s"] == pytest.approx(50.0)

    # The runner still ticks every 50s; the round returns immediately until the
    # backoff expires, so no extra requests are made.
    calls_after_first = client.count("set_whem_bandwidth")
    await started_source.keepalive_round()
    assert client.count("set_whem_bandwidth") == calls_after_first
    assert started_source.keepalive_failures == 1

    clock.advance(50.0)
    await started_source.keepalive_round()
    assert started_source.keepalive_failures == 2
    assert status_store.section("leviton_keepalive")["backoff_s"] == pytest.approx(100.0)


async def test_keepalive_backoff_is_capped(
    started_source: LevitonSource,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """Exponential, but bounded: a long outage must not stop us retrying at all.

    Without a cap, 2^n seconds would put the next attempt weeks away and the
    hubs would sit in low-bandwidth mode (serving stale cached readings) long
    after the API came back.
    """
    client.fail_always("set_whem_bandwidth", LevitonConnectionError("HTTP 502"))
    for _ in range(12):
        await started_source.keepalive_round()
        clock.advance(leviton_module.KEEPALIVE_MAX_BACKOFF_S)

    assert started_source.keepalive_failures == 12
    assert status_store.section("leviton_keepalive")["backoff_s"] == pytest.approx(
        leviton_module.KEEPALIVE_MAX_BACKOFF_S
    )
    # Every round after the backoff expired really did try again.
    assert client.count("set_whem_bandwidth") == 12


async def test_a_recovered_keepalive_clears_the_backoff(
    started_source: LevitonSource,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    client.fail("set_whem_bandwidth", LevitonConnectionError("HTTP 502"))
    await started_source.keepalive_round()
    assert started_source.keepalive_failures == 1

    clock.advance(60.0)
    await started_source.keepalive_round()

    assert started_source.keepalive_failures == 0
    assert client.bandwidth_puts == [(HUB_A, 1)]
    assert status_store.section("leviton_keepalive")["consecutive_failures"] == 0


async def test_the_keepalive_task_never_raises(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """A failing background task must not be able to kill the poll loop."""
    client.fail_always("set_whem_bandwidth", RuntimeError("something unforeseen"))
    await started_source.keepalive_round()  # no exception
    assert started_source.keepalive_failures == 1


# ================================================================== discovery


async def test_discovery_walks_the_documented_hierarchy(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§6.2: person → permissions → residences → whems → breakers + CTs."""
    assert client.count("get_permissions") >= 1
    assert client.count("get_residences") >= 1
    assert client.count("get_whems") >= 1
    assert client.count("get_whem_breakers") >= 2  # one per hub
    assert client.count("get_cts") >= 2


async def test_discovery_lists_devices_and_channels(
    started_source: LevitonSource,
) -> None:
    discovery = await started_source.discover()

    assert {device.device_id for device in discovery.devices} == {HUB_A, HUB_B}
    channels = {(c.device_id, c.channel_id) for c in discovery.channels}

    assert (HUB_A, "breaker_p11") in channels
    assert (HUB_A, "panel_leg_a") in channels
    assert (HUB_B, "panel_leg_b") in channels
    assert (HUB_A, "ct_1_a") in channels
    assert (HUB_A, "ct_1_b") in channels
    # A single-leg CT advertises only the leg it actually has.
    assert (HUB_A, "ct_2_a") in channels
    assert (HUB_A, "ct_2_b") not in channels
    # Placeholders and unused clamps are not channels at all.
    assert (HUB_A, "breaker_p5") not in channels
    assert (HUB_A, "ct_3_a") not in channels


async def test_discovery_is_cached_until_forced(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    before = client.count("get_permissions")
    await started_source.discover()
    assert client.count("get_permissions") == before

    await started_source.discover(force=True)
    assert client.count("get_permissions") == before + 1


async def test_rediscovery_finds_a_newly_installed_breaker_without_a_restart(
    started_source: LevitonSource, client: FakeLevitonClient
) -> None:
    """§6.2: smart breakers are being added over time; a restart must not be the
    price of seeing one."""
    first = await started_source.discover()
    assert not any(c.channel_id == "breaker_p21" for c in first.channels)

    client.breakers[HUB_A] = client.breakers[HUB_A] + load_fixture(
        "breakers_new_smart_breaker"
    )

    refreshed = await started_source.discover(force=True)
    assert any(c.channel_id == "breaker_p21" for c in refreshed.channels)

    rows = await started_source.poll()
    new_channel = rows_by_channel(rows)[(HUB_A, "breaker_p21")]
    assert new_channel["watts"] == pytest.approx(3600 + 3550)


async def test_discovery_skeleton_is_paste_ready_for_channel_map(
    started_source: LevitonSource,
) -> None:
    """§9: ``energycap discover`` prints entries a human can paste and fill in."""
    discovery = await started_source.discover()
    skeleton = discovery.skeleton()
    assert {"source", "device_id", "channel_id"} <= set(skeleton[0])
    assert all(entry["source"] == SOURCE_LEVITON for entry in skeleton)


# ==================================================== ingestion (LEVITON_INGEST)
#
# The socket changes how values are kept FRESH. It must not change how rows are
# SAMPLED — one cycle, one ``ts_utc``, one mapper, in every mode. These tests
# exist mostly to catch the two ways that could quietly stop being true: a
# second mapper appearing for the WebSocket path, and a shut gate being papered
# over with a cached reading that nobody can identify afterwards.


class FakeWsTransport:
    """A stand-in for the ``aioleviton`` seam in ``sources/leviton_ws.py``.

    Speaks the four verbs and two callbacks the real transport does. Nothing
    here opens a socket; ``tests/conftest.py`` would refuse one anyway.
    """

    def __init__(self) -> None:
        self.connected = False
        self.close_code: int | None = None
        self.subscribed: list[tuple[str, Any]] = []
        self.unsubscribed: list[tuple[str, Any]] = []
        self.notification_cb: Any = None
        self.disconnect_cb: Any = None
        self.connect_calls = 0
        self.connect_error: BaseException | None = None

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def subscribe(self, model_name: str, model_id: Any) -> None:
        self.subscribed.append((model_name, model_id))

    async def unsubscribe(self, model_name: str, model_id: Any) -> None:
        self.unsubscribed.append((model_name, model_id))
        if (model_name, model_id) in self.subscribed:
            self.subscribed.remove((model_name, model_id))

    def on_notification(self, callback: Any) -> Any:
        self.notification_cb = callback
        return lambda: None

    def on_disconnect(self, callback: Any) -> Any:
        self.disconnect_cb = callback
        return lambda: None

    # ------------------------------------------------------------- test verbs
    def push(self, model_name: str, model_id: Any, **fields: Any) -> None:
        """Deliver one notification the way the read loop does: inline, sync."""
        assert self.notification_cb is not None, "nothing subscribed to notifications"
        self.notification_cb(
            {"modelName": model_name, "modelId": model_id, "data": dict(fields)}
        )

    def drop(self, close_code: int | None = 1006) -> None:
        """The server drops us (or the 60-minute hard kill lands)."""
        self.connected = False
        self.close_code = close_code
        assert self.disconnect_cb is not None
        self.disconnect_cb()


WsSetup = tuple[LevitonSource, LevitonWebSocketIngester, FakeWsTransport]


@pytest.fixture
def ws_source(
    settings: Settings,
    adapter: LevitonAdapter,
    status_store: StatusStore,
    clock: FakeClock,
):
    """Factory: a :class:`LevitonSource` in ``mode``, with a fake push transport.

    The ingester is injected rather than built from the adapter so its clock is
    the test's, but everything else is the production wiring: the same seed
    callback, the same keepalive round, the same subscription set derived from
    discovery.
    """

    def _build(mode: str = INGEST_HYBRID) -> WsSetup:
        transport = FakeWsTransport()
        holder: dict[str, LevitonSource] = {}

        async def factory() -> FakeWsTransport:
            return transport

        async def seed() -> Any:
            return await holder["source"]._websocket_seed()

        async def keepalive() -> None:
            await holder["source"].keepalive_round()

        ingester = LevitonWebSocketIngester(
            transport_factory=factory,
            seed=seed,
            keepalive=keepalive,
            status_store=status_store,
            monotonic=clock.monotonic,
        )
        source = LevitonSource(
            settings.model_copy(update={"leviton_ingest": mode}),
            adapter=adapter,
            status_store=status_store,
            monotonic=clock.monotonic,
            ws=ingester,
        )
        holder["source"] = source
        return (source, ingester, transport)

    return _build


def open_the_gate(ingester: LevitonWebSocketIngester, clock: FakeClock) -> None:
    """Let the post-connect flood window expire so the store may be sampled.

    The strict path (the flood touching every subscribed object) is exercised in
    ``tests/test_leviton_ws.py``; here we only need the gate open.
    """
    clock.advance(30.0)
    assert ingester.can_sample(), f"gate still shut: {ingester.withheld_reason}"


def row_shapes(rows: list[Observation]) -> list[tuple[Any, ...]]:
    """Everything about a row except when it was taken."""
    return [
        (row.source, row.device_id, row.channel_id, row.metric, row.value, row.unit)
        for row in rows
    ]


def test_the_ingest_modes_match_the_config_validator() -> None:
    """One list of modes, in two files, that must not drift apart."""
    assert set(LEVITON_INGEST_MODES) == {INGEST_HYBRID, INGEST_WS, INGEST_REST}


def test_the_ws_tick_interval_mirrors_the_watchdog_constant() -> None:
    """``leviton_ws`` imports from ``leviton``, so the constant cannot be shared.

    A tick slower than the watchdog interval would delay every reconnect and
    every stall detection by the difference, silently.
    """
    assert WS_TICK_INTERVAL_S == WATCHDOG_INTERVAL_S


# ------------------------------------------------------------------- hybrid


async def test_hybrid_prefers_websocket_state_over_a_rest_read(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """The whole point: sample the live store, do not re-read the REST cache.

    The recorded fixture says leg A is 121 V. The socket then says 240.5 V. The
    row must be 240.5, and the cycle must not have gone anywhere near REST — a
    hybrid cycle that quietly kept polling would leave the frozen-cache problem
    exactly where it was.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    rest_reads_before = client.count("get_whems")

    rows = await source.poll()

    assert rows_by_channel(rows)[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(240.5)
    assert client.count("get_whems") == rest_reads_before
    assert source.last_value_source == VALUE_SOURCE_WS


async def test_hybrid_falls_back_to_rest_on_a_dead_socket_and_records_it(
    ws_source: Any,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """A cached REST value beats nothing — but only if it is labelled as one.

    Rows carry no provenance column and are not getting one, so the fallback has
    to be reconstructable afterwards from ``status.json``: which mode is active,
    how many cycles came from where, and *why* the socket was not used.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    transport.drop()

    rest_reads_before = client.count("get_whems")
    rows = await source.poll()

    # The REST fixture's value, not the socket's last-known 240.5: a shut gate
    # means we do not know what the socket would have said.
    assert rows_by_channel(rows)[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(121)
    assert client.count("get_whems") == rest_reads_before + 1
    assert source.last_value_source == VALUE_SOURCE_REST_FALLBACK

    section = status_store.section(STATUS_SECTION_INGEST)
    assert section["mode"] == INGEST_HYBRID
    assert section["value_source"] == VALUE_SOURCE_REST_FALLBACK
    assert section["cycles_rest_fallback"] == 1
    assert section["ws_withheld_reason"]  # a named reason, not just "off"


async def test_hybrid_will_not_sample_a_socket_that_is_open_but_silent(
    ws_source: Any,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """The silent-stall guard, proved where it costs something: at the rows.

    This is the dangerous shape, because ``connected`` stays ``True`` throughout.
    aiohttp's heartbeat only proves the TCP path is alive; a server that pongs
    while pushing nothing is invisible to the library. Without the guard the
    sampler would keep lifting the *last* values out of a frozen store and
    stamping each cycle with a current ``ts_utc`` — a hold-last-value at 30s
    cadence, indistinguishable in the archive from a genuinely steady load, and
    exactly what CLAUDE.md rule 1 forbids.

    So: the socket never drops, but after ``STALL_TIMEOUT_S`` of total silence
    the gate shuts on its own — no watchdog tick required, because the sampler
    asks on its own cadence — and hybrid reads REST for the cycle and says why.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    assert ingester.can_sample() is True

    # No drop, no close code: the socket is still open and simply says nothing.
    clock.advance(STALL_TIMEOUT_S + 1.0)
    assert transport.connected is True, "the failure under test is an OPEN socket"
    assert ingester.connected is True
    assert ingester.can_sample() is False
    assert ingester.withheld_reason == REASON_STALLED

    rest_reads_before = client.count("get_whems")
    rows = await source.poll()

    # 121 is the REST fixture. 240.5 would be the frozen store's last word,
    # re-emitted as if it were current.
    assert rows_by_channel(rows)[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(121)
    assert client.count("get_whems") == rest_reads_before + 1
    assert source.last_value_source == VALUE_SOURCE_REST_FALLBACK
    assert status_store.section(STATUS_SECTION_INGEST)["ws_withheld_reason"] == (
        REASON_STALLED
    )


async def test_ws_mode_gaps_rather_than_sampling_a_stalled_socket(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """The same stall in ``ws`` mode: zero rows, a counted failure, no REST read.

    ``ws`` is the mode that makes the socket measurable, so it must not paper
    over a stall any more than it papers over a disconnect.
    """
    source, ingester, transport = ws_source(INGEST_WS)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    clock.advance(STALL_TIMEOUT_S + 1.0)

    rest_reads_before = client.count("get_whems")
    rows: list[Observation] = []
    with pytest.raises(SourceTransientError, match=REASON_STALLED):
        rows = await source.poll()

    assert rows == []
    assert client.count("get_whems") == rest_reads_before
    assert source.consecutive_failures == 1
    assert source.ingest_status()["cycles_withheld"] == 1


async def test_a_value_the_reconnect_never_re_established_produces_no_row(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """The per-field half of the emission gate, proved where it costs something.

    ``StateStore`` deliberately survives a reconnect. So when the flood misses an
    object and the REST seed fails, the timeout path opens the gate over values
    the **previous** connection delivered — and the sampler stamps them with a
    current ``ts_utc`` and reports ``value_source=ws``. That is a hold-last-value
    across a disconnect, indistinguishable in the archive from a genuine reading,
    and exactly what CLAUDE.md rule 1 forbids.

    A field this connection did not establish must therefore be *absent*, which
    §6.5 turns into no row at all. A gap stays a gap.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)

    rows = await source.poll()
    assert rows_by_channel(rows)[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(240.5)

    # The socket is cycled while the REST cloud is 502ing, so the new connection
    # gets no seed; the flood then only ever reaches one CT.
    client.fail_always("get_whems", LevitonConnectionError("502 Bad Gateway"))
    clock.advance(STALL_TIMEOUT_S + 1.0)
    await ingester.tick()
    assert ingester.can_sample() is False

    transport.push(WS_MODEL_CT, 4001, activePower=1234.0)
    clock.advance(30.0)
    assert ingester.can_sample() is True, ingester.withheld_reason
    assert ingester.status_snapshot()["sync_mode"] == SYNC_MODE_TIMEOUT

    rows = await source.poll()

    assert rows, "a vacuous pass would be worse than a failure"
    assert source.last_value_source == VALUE_SOURCE_WS
    leg_a = rows_by_channel(rows).get((HUB_A, "panel_leg_a"), {})
    assert "volts" not in leg_a, (
        "240.5 was established on the PREVIOUS connection; re-emitting it with a "
        "current ts_utc is a hold-last-value across a disconnect"
    )
    assert any(row.value == pytest.approx(1234.0) for row in rows), (
        "the one object the flood did reach must still report"
    )


async def test_hybrid_will_not_sample_a_hub_whose_own_feed_has_died(
    ws_source: Any,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """Two hubs share one socket, so liveness cannot be an aggregate.

    Panel A's line voltage jitters constantly, which keeps an "any frame from
    anyone" watchdog permanently satisfied. Meanwhile Panel B's entire push feed
    is dead — no drop, no close code, ``connected`` still ``True`` — and its
    channels are being lifted out of a frozen store and stamped as current. This
    house has exactly two hubs; this is the shape the aggregate mark hides.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_B, rmsVoltageA=250.0)
    assert rows_by_channel(await source.poll())[(HUB_B, "panel_leg_a")][
        "volts"
    ] == pytest.approx(250.0)

    # Panel A chatters for four minutes. Panel B says nothing at all.
    for _ in range(8):
        clock.advance(30.0)
        transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=121.3)

    assert transport.connected is True, "the failure under test is an OPEN socket"
    assert ingester.can_sample(HUB_A) is True, "Panel A really is alive"
    assert ingester.can_sample(HUB_B) is False
    assert ingester.can_sample() is False

    rest_reads_before = client.count("get_whems")
    rows = await source.poll()

    assert client.count("get_whems") == rest_reads_before + 1
    assert source.last_value_source == VALUE_SOURCE_REST_FALLBACK
    assert rows, "a vacuous pass would be worse than a failure"
    assert not any(row.value == pytest.approx(250.0) for row in rows), (
        "250.0 is Panel B's frozen store, re-emitted with a current ts_utc while "
        "Panel A's chatter kept the aggregate watchdog happy"
    )
    assert status_store.section(STATUS_SECTION_INGEST)["ws_withheld_reason"] == (
        REASON_STALLED
    )
    assert ingester.status_snapshot()["stalled_hubs"] == [HUB_B], (
        "status.json must name which hub went dark, not just that something did"
    )


async def test_a_dead_hub_a_reconnect_and_a_null_seed_compose_into_gaps(
    ws_source: Any,
    client: FakeLevitonClient,
    clock: FakeClock,
    status_store: StatusStore,
) -> None:
    """The three connection-state rules, in one story, judged at the rows.

    Each removes a different way for a value we do **not** currently know to
    reach the archive with a fresh ``ts_utc``, and the risk of fixing them
    separately is that one quietly re-opens another's hole — the seed can
    re-establish what the eviction just dropped, the eviction can delete what
    the seed just cleared, and a per-hub gate is only worth anything if the
    reconnect it forces does not carry the dead hub's last words across.

    The story runs in two halves, because the two failure modes are two
    different states of the REST cloud:

    1. **Panel B's feed dies** while Panel A chatters, so the aggregate mark
       never goes stale and only per-hub liveness notices. The reconnect it
       forces lands while REST is 502ing, so nothing is re-established except
       the one CT the flood reaches — and everything else must become a gap
       rather than the previous connection's last words (per-field membership).
    2. **REST comes back, reporting Panel A's leg as unknown.** The socket has
       since re-established that leg on the live connection, so it is emitted
       honestly; then another reconnect seeds from that null, which must
       *clear* the value rather than leave it standing.

    The assertion that matters in both halves is the same: no row carries a
    number that was only ever true on a connection we are no longer on.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)

    # Connection 1 establishes three values, and the archive receives them.
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    transport.push(WS_MODEL_HUB, HUB_B, rmsVoltageA=250.0)
    transport.push(WS_MODEL_BREAKER, "4C45565275C9_A65E", power=9999.0)
    first = rows_by_channel(await source.poll())
    assert first[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(240.5)
    assert first[(HUB_B, "panel_leg_a")]["volts"] == pytest.approx(250.0)
    assert first[(HUB_A, "breaker_p9")]["watts"] == pytest.approx(9999.0)

    # -- half one: a dead hub, a reconnect, and a seed that never arrives -----
    # Panel B's push feed dies. Panel A chatters through the whole four minutes,
    # so the socket never drops and the aggregate mark never goes stale.
    for _ in range(8):
        clock.advance(30.0)
        transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=121.3)
    clock.advance(1.0)  # the watchdog fires *after* the last frame, never during it
    assert transport.connected is True, "the failure under test is an OPEN socket"
    assert ingester.status_snapshot()["stalled_hubs"] == [HUB_B]
    assert ingester.can_sample() is False, "one dead hub shuts the aggregate gate"

    client.fail_always("get_whems", LevitonConnectionError("502 Bad Gateway"))
    reconnects_before = ingester.reconnects
    await ingester.tick()
    assert ingester.reconnects == reconnects_before + 1
    assert transport.connect_calls == 2, "a fresh connection, not a repaired one"

    # On the new connection the flood only ever reaches one CT.
    transport.push(WS_MODEL_CT, 4001, activePower=4102.5)
    clock.advance(30.0)
    assert ingester.can_sample() is True, ingester.withheld_reason
    assert ingester.status_snapshot()["sync_mode"] == SYNC_MODE_TIMEOUT

    rows = await source.poll()
    by_channel = rows_by_channel(rows)

    assert rows, "a vacuous pass would be worse than a failure"
    assert source.last_value_source == VALUE_SOURCE_WS
    assert by_channel[(HUB_A, "ct_1_a")]["watts"] == pytest.approx(4102.5), (
        "the one object this connection re-proved must still report"
    )
    assert "volts" not in by_channel.get((HUB_A, "panel_leg_a"), {}), (
        "the live hub's leg was not re-proved on this connection either"
    )
    assert "volts" not in by_channel.get((HUB_B, "panel_leg_a"), {}), (
        "250.0 was the dead hub's last word on the previous connection"
    )
    assert "watts" not in by_channel.get((HUB_A, "breaker_p9"), {})
    stale = {240.5, 250.0, 9999.0, 121.3}
    assert not any(
        any(row.value == pytest.approx(value) for value in stale) for row in rows
    ), (
        "a value established on the PREVIOUS connection reached the archive with "
        "a current ts_utc — the hold-last-value CLAUDE.md rule 1 forbids"
    )

    status = ingester.status_snapshot()
    assert status["fields_evicted"] >= 3, "the eviction is what dropped them"
    assert status["stalled_hubs"] == [], "the reconnect gave both feeds a clean slate"
    assert set(status["hub_silence_s"]) == {HUB_A, HUB_B}, (
        "both hubs' silence must be visible for the first live run"
    )

    # -- half two: REST comes back saying "unknown", and the seed must clear ---
    client.heal()
    client.whems[0]["rmsVoltageA"] = None
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=118.0)
    live = rows_by_channel(await source.poll())
    assert live[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(118.0), (
        "a value this connection established is honest, however old REST is"
    )

    clock.advance(STALL_TIMEOUT_S + 1.0)  # both feeds quiet: cycle again
    cleared_before = ingester.status_snapshot()["fields_cleared"]
    await ingester.tick()

    assert ingester.status_snapshot()["seeded_from_rest"] is True, (
        "this seed did land — the null is a value it carried, not a failure"
    )
    assert "rmsVoltageA" not in ingester.peek_object(WS_MODEL_HUB, HUB_A), (
        "REST said the leg is unknown; a seed that can overwrite a value but "
        "never remove one leaves 118.0 standing for the sampler to publish"
    )
    assert ingester.status_snapshot()["fields_cleared"] > cleared_before, (
        "the null was applied as a clear, not dropped on the way in"
    )

    transport.push(WS_MODEL_CT, 4001, activePower=4102.5)
    clock.advance(30.0)
    assert ingester.can_sample() is True, ingester.withheld_reason
    final = await source.poll()
    assert not any(row.value == pytest.approx(118.0) for row in final)
    assert "volts" not in rows_by_channel(final).get((HUB_A, "panel_leg_a"), {})


async def test_hybrid_records_a_fallback_when_the_ingester_itself_misbehaves(
    ws_source: Any, status_store: StatusStore
) -> None:
    """A bug in the freshness layer must not cost a cycle of data.

    The socket is an optimisation over a REST path that already works; a defect
    in it is a reason to stop trusting its *values*, never a reason to stop
    collecting.
    """
    source, ingester, _transport = ws_source(INGEST_HYBRID)
    await source.start()

    def explode(_snapshots: Any) -> None:
        raise RuntimeError("overlay is broken")

    ingester.overlay_snapshots = explode  # type: ignore[method-assign]

    rows = await source.poll()

    assert rows
    assert source.last_value_source == VALUE_SOURCE_REST_FALLBACK
    assert status_store.section(STATUS_SECTION_INGEST)["ws_withheld_reason"] == "ws_error"


async def test_the_rest_reconcile_refreshes_the_skeleton_and_measures_drift(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """Hybrid's cross-check: how far behind is the REST cache, in metrics?

    Both value sets go through the same mapper, so the count is a direct measure
    of the thing that motivated this whole change. It is diagnostics only —
    ``reconcile_round`` never produces a row.
    """
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)

    await source.reconcile_round()

    drift = source.ingest_status()["last_reconcile_drift"]
    assert drift is not None
    assert drift["differing"] == 1  # exactly leg A's volts
    assert drift["compared"] > 1
    assert source.ingest_status()["rest_reconciles"] == 1


async def test_the_reconcile_task_only_exists_in_hybrid_mode(ws_source: Any) -> None:
    hybrid, _, _ = ws_source(INGEST_HYBRID)
    ws_only, _, _ = ws_source(INGEST_WS)
    assert "leviton_rest_reconcile" in {t.name for t in hybrid.background_tasks()}
    assert "leviton_rest_reconcile" not in {t.name for t in ws_only.background_tasks()}


# ----------------------------------------------------------------- ws mode


async def test_ws_mode_emits_a_gap_rather_than_falling_back(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """CLAUDE.md rule 1, at connection granularity.

    While the socket is down we do not know the current value. Emitting the last
    one stamped with a current timestamp is fabrication; emitting a cached REST
    reading is the very thing ``ws`` mode exists to refuse. So: zero rows, a
    counted failure, and no REST call at all.
    """
    source, ingester, transport = ws_source(INGEST_WS)
    await source.start()
    open_the_gate(ingester, clock)
    transport.drop()

    rest_reads_before = client.count("get_whems")
    rows: list[Observation] = []
    with pytest.raises(SourceTransientError, match="gap rather than a cached"):
        rows = await source.poll()

    assert rows == []
    assert client.count("get_whems") == rest_reads_before
    assert source.consecutive_failures == 1
    assert source.ingest_status()["cycles_withheld"] == 1
    assert source.ingest_status()["value_source"] == VALUE_SOURCE_WITHHELD


async def test_ws_mode_samples_the_store_once_the_gate_opens(
    ws_source: Any, clock: FakeClock
) -> None:
    source, ingester, transport = ws_source(INGEST_WS)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=239.0)

    rows = await source.poll()

    assert rows_by_channel(rows)[(HUB_A, "panel_leg_a")]["volts"] == pytest.approx(239.0)
    assert source.last_value_source == VALUE_SOURCE_WS


# ---------------------------------------------------------------- rest mode


async def test_rest_mode_never_builds_a_socket(
    settings: Settings, adapter: LevitonAdapter, status_store: StatusStore
) -> None:
    """The owner's no-code-change fallback, and it must stay exactly today's path."""
    source = LevitonSource(
        settings.model_copy(update={"leviton_ingest": INGEST_REST}),
        adapter=adapter,
        status_store=status_store,
    )
    await source.start()

    assert source.websocket is None
    names = {task.name for task in source.background_tasks()}
    assert "leviton_ws" not in names
    assert "leviton_rest_reconcile" not in names
    assert {"leviton_keepalive", "leviton_discovery"} <= names

    await source.poll()
    assert source.last_value_source == VALUE_SOURCE_REST


async def test_a_client_that_cannot_open_a_socket_degrades_to_rest(
    settings: Settings, adapter: LevitonAdapter, status_store: StatusStore
) -> None:
    """A capability gap is loud, and it collects. It does not gap forever.

    ``FakeLevitonClient`` has no ``create_websocket()`` — nor would a vendored
    replacement necessarily. Producing permanent gaps over that would be
    self-inflicted data loss, so the source says so and reads REST.
    """
    assert adapter.supports_websocket is False
    assert adapter.ws_transport_factory() is None

    source = LevitonSource(
        settings.model_copy(update={"leviton_ingest": INGEST_HYBRID}),
        adapter=adapter,
        status_store=status_store,
    )
    await source.start()

    assert source.websocket is None
    rows = await source.poll()
    assert rows
    assert source.last_value_source == VALUE_SOURCE_REST
    assert status_store.section(STATUS_SECTION_INGEST)["ws_available"] is False


async def test_the_adapter_builds_a_fresh_transport_for_every_connection(
    tmp_path: Path,
) -> None:
    """``connect()`` twice on one ``LevitonWebSocket`` leaks a listen task.

    Building fresh also means the auth frame always carries the *current* token,
    including after a re-login — and that the token reaches the log scrubber
    before anything can print it (CLAUDE.md rule 8).
    """

    class WsCapableClient:
        def __init__(self) -> None:
            self.created = 0

        def create_websocket(self) -> Any:
            self.created += 1
            return types.SimpleNamespace(
                _token="ws-token-aaaaaaaaaaaaaaaa", connected=False
            )

    client = WsCapableClient()
    adapter = LevitonAdapter(
        username="u",
        password="p",
        token_path=tmp_path / "leviton.json",
        client=client,
    )
    assert adapter.supports_websocket is True

    factory = adapter.ws_transport_factory()
    assert factory is not None
    first = await factory()
    second = await factory()

    assert client.created == 2
    assert first is not second
    assert (
        ec_logging.scrub_text("token=ws-token-aaaaaaaaaaaaaaaa")
        == f"token={ec_logging.REDACTED}"
    )


async def test_a_ws_url_that_cannot_be_honoured_is_reported_not_ignored(
    tmp_path: Path,
) -> None:
    """``aioleviton`` hardcodes the endpoint in ``connect()``.

    ``LEVITON_WS_URL`` therefore cannot be applied without vendoring that
    method, and a setting that silently does nothing is worse than one that does
    not exist. The default value is honoured for free (same string) and says
    nothing; a *changed* value says so at ERROR, naming the endpoint really used.
    """
    from aioleviton.const import WEBSOCKET_URL

    class WsCapableClient:
        def create_websocket(self) -> Any:
            return types.SimpleNamespace(_token="t", connected=False)

    adapter = LevitonAdapter(
        username="u",
        password="p",
        token_path=tmp_path / "leviton.json",
        client=WsCapableClient(),
    )

    stream = io.StringIO()
    ec_logging.configure_logging("DEBUG", stream=stream, force=True)
    try:
        adapter.ws_transport_factory(WEBSOCKET_URL)  # the default: no complaint
        assert "leviton_ws_url_not_honoured" not in stream.getvalue()

        adapter.ws_transport_factory("wss://example.invalid/socket")
        record = json.loads(
            [
                line
                for line in stream.getvalue().splitlines()
                if "leviton_ws_url_not_honoured" in line
            ][0]
        )
    finally:
        ec_logging.configure_logging("INFO", force=True)

    assert record["level"] == "ERROR"
    assert record["configured"] == "wss://example.invalid/socket"
    assert record["effective"] == WEBSOCKET_URL


# --------------------------------------------------- sampling is mode-invariant


@pytest.mark.parametrize("mode", [INGEST_HYBRID, INGEST_WS, INGEST_REST])
async def test_one_ts_utc_per_cycle_in_every_mode(
    ws_source: Any,
    settings: Settings,
    adapter: LevitonAdapter,
    status_store: StatusStore,
    clock: FakeClock,
    mode: str,
) -> None:
    """PLAN.md §6.5, and the assumption §2.5's kWh formula rests on.

    A row-per-delta design would have broken this and taken ``sample_count``'s
    meaning as the gap detector with it.
    """
    if mode == INGEST_REST:
        source = LevitonSource(
            settings.model_copy(update={"leviton_ingest": mode}),
            adapter=adapter,
            status_store=status_store,
        )
        await source.start()
    else:
        source, ingester, transport = ws_source(mode)
        await source.start()
        open_the_gate(ingester, clock)
        transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)

    rows = await source.poll()

    assert rows
    assert len({row.ts_utc for row in rows}) == 1
    assert len({row.ts_local for row in rows}) == 1


def test_exactly_one_function_in_the_package_maps_a_leviton_row() -> None:
    """The structural half of the one-mapper claim (§6.5).

    The behavioural half is the test below: two ingestion paths producing the
    same rows. But two mappers could agree *today* and drift tomorrow, and the
    drift would be silent — so pin the shape as well as the behaviour. Every
    Leviton row is born in a ``PollCycle.add``/``add_metrics`` call, so counting
    the functions that make one counts the mappers.
    """
    emitters: dict[str, set[str]] = {}
    for module in (leviton_module, ws_module):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in {"add", "add_metrics"}
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "cycle"
                for call in ast.walk(node)
            ):
                emitters.setdefault(module.__name__, set()).add(node.name)

    assert emitters == {leviton_module.__name__: {"_map_snapshot"}}, (
        "§6.5 row mapping must live in exactly one function, in sources/leviton.py; "
        f"found {emitters}"
    )


async def test_a_ws_sourced_cycle_and_a_rest_sourced_cycle_map_identically(
    ws_source: Any,
    settings: Settings,
    adapter: LevitonAdapter,
    status_store: StatusStore,
    clock: FakeClock,
) -> None:
    """The proof that there is exactly ONE mapper (PLAN.md §6.5).

    The ingester's store is seeded from the same REST snapshot and then sampled
    through the WebSocket path; the rows must match a plain REST cycle row for
    row, in order, including the gaps — the null CT leg, the null breaker power,
    the 2-pole breaker whose second pole did not report, the placeholder
    breakers that are skipped, and the firmware-v2 spurious zeros that pass
    through verbatim. Two mappers would drift; this is what catches it.
    """
    ws_side, ingester, _transport = ws_source(INGEST_WS)
    await ws_side.start()
    open_the_gate(ingester, clock)

    rest_side = LevitonSource(
        settings.model_copy(update={"leviton_ingest": INGEST_REST}),
        adapter=adapter,
        status_store=status_store,
    )
    await rest_side.start()

    ws_rows = await ws_side.poll()
    rest_rows = await rest_side.poll()

    assert ws_side.last_value_source == VALUE_SOURCE_WS
    assert rest_side.last_value_source == VALUE_SOURCE_REST
    assert row_shapes(ws_rows) == row_shapes(rest_rows)
    assert ws_rows  # a vacuous pass would be worse than a failure


# ------------------------------------------------------------- subscriptions


async def test_a_newly_discovered_breaker_is_subscribed_without_a_restart(
    ws_source: Any, client: FakeLevitonClient
) -> None:
    """~25 smart breakers are being installed; none of them warrants a restart.

    The subscription key is the API ``id``, which firmware ≥2.2.0 mutates. That
    is fine and is exactly why the set is re-derived from every discovery pass —
    ``channel_id`` still comes from ``position`` and never moves.
    """
    source, _ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    assert (WS_MODEL_BREAKER, "4C4556527611_A65E") not in transport.subscribed

    client.breakers[HUB_A] = load_fixture("breakers_new_smart_breaker")
    await source.discover(force=True)

    assert (WS_MODEL_BREAKER, "4C4556527611_A65E") in transport.subscribed


async def test_every_hub_ct_and_real_breaker_is_subscribed_on_connect(
    ws_source: Any,
) -> None:
    """Hub *and* per-CT *and* per-breaker: firmware ≥2.0 needs the last of those.

    The hub subscription is documented to carry CT updates on all firmware, but
    only one of the two reference integrations is confident about it, and the
    downside of being wrong is a silent outage on the whole-panel GRID_POWER
    feeds — which are CTs. Belt and braces.
    """
    source, _ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()

    subscribed = set(transport.subscribed)
    assert (WS_MODEL_HUB, HUB_A) in subscribed
    assert (WS_MODEL_HUB, HUB_B) in subscribed
    assert ("IotCt", 4001) in subscribed  # int ids, per the wire
    assert (WS_MODEL_BREAKER, "4C45565275C6_A65E") in subscribed
    # Placeholders and NOT_USED clamps are not real channels and are not subscribed.
    assert (WS_MODEL_BREAKER, "PLACEHOLDER_0005") not in subscribed
    assert ("IotCt", 4003) not in subscribed


# ------------------------------------------------------- keepalive, unchanged


async def test_the_socket_fires_the_existing_keepalive_and_still_never_sends_zero(
    ws_source: Any, client: FakeLevitonClient
) -> None:
    """§6.4's PUT is what triggers the state flood, so it goes first — as ``1``.

    This module still has exactly one bandwidth call site and it still passes
    the constant; attaching a subscriber only means the PUT finally does what it
    was always for.
    """
    source, _ingester, _transport = ws_source(INGEST_HYBRID)
    await source.start()

    assert client.bandwidth_puts  # fired once before the connect
    assert all(value == BANDWIDTH_HIGH == 1 for _, value in client.bandwidth_puts)


async def test_a_keepalive_401_tells_the_socket_the_token_is_dead(
    ws_source: Any, client: FakeLevitonClient, clock: FakeClock
) -> None:
    """``aioleviton`` drops mid-stream error frames, so the PUT is our only warning.

    Without this the gate would stay open on a socket the cloud had already
    stopped honouring, and the sampler would keep emitting rows from a store
    nothing was updating.
    """
    source, ingester, _transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    assert ingester.can_sample()

    client.fail_always("set_whem_bandwidth", LevitonAuthError("Authorization Required"))
    await source.keepalive_round()

    assert ingester.can_sample() is False
    assert ingester.withheld_reason == "auth_failed"


# -------------------------------------------------------------- shutdown / status


async def test_closing_the_source_tears_the_socket_down(
    ws_source: Any, clock: FakeClock
) -> None:
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)

    await source.close()

    assert transport.connected is False
    assert ingester.can_sample() is False


async def test_status_json_carries_the_mode_and_the_socket_counters(
    ws_source: Any, clock: FakeClock, status_store: StatusStore
) -> None:
    """PLAN.md §11: the dashboard has to answer "how is it collecting right now"."""
    source, ingester, transport = ws_source(INGEST_HYBRID)
    await source.start()
    open_the_gate(ingester, clock)
    transport.push(WS_MODEL_HUB, HUB_A, rmsVoltageA=240.5)
    await source.poll()
    await ingester.tick()

    ingest = status_store.section(STATUS_SECTION_INGEST)
    assert ingest["mode"] == INGEST_HYBRID
    assert ingest["ws_available"] is True
    assert ingest["cycles_ws"] == 1
    assert ingest["value_source"] == VALUE_SOURCE_WS

    ws = status_store.section("leviton_ws")
    assert ws["connected"] is True
    assert ws["messages_received"] >= 1
    assert ws["withheld_reason"] is None


# ================================================================ misc / wiring


def test_channel_id_helpers_follow_the_conventions() -> None:
    assert breaker_channel_id(11) == "breaker_p11"
    assert ct_channel_id(1, "a") == "ct_1_a"
    assert ct_channel_id(1, "B") == "ct_1_b"
    with pytest.raises(ValueError):
        ct_channel_id(1, "c")


def test_the_poll_interval_floor_is_thirty_seconds(settings: Settings) -> None:
    """§6.6: hard-coded floor, whatever the env var says."""
    source = LevitonSource(settings.model_copy(update={"poll_interval_s": 1}))
    assert source.poll_interval_s == 30


def test_the_source_names_itself_leviton(source: LevitonSource) -> None:
    assert source.name == SOURCE_LEVITON


def test_the_default_source_reads_its_token_path_from_settings(
    settings: Settings,
) -> None:
    """Token caches live only on the mounted volume (CLAUDE.md rule 8)."""
    source = LevitonSource(settings)
    assert source.adapter.token_path == settings.spool_dir / "tokens" / "leviton.json"


async def test_close_is_idempotent(source: LevitonSource) -> None:
    await source.close()
    await source.close()


def test_the_token_cache_returns_none_when_absent(tmp_path: Path) -> None:
    assert LevitonTokenCache(tmp_path / "missing.json").load() is None


def test_the_token_cache_rejects_a_payload_without_a_token(tmp_path: Path) -> None:
    path = tmp_path / "leviton.json"
    path.write_text(json.dumps({"userId": "person-1234"}), encoding="utf-8")
    assert LevitonTokenCache(path).load() is None


def test_the_token_cache_tightens_loose_permissions_on_read(tmp_path: Path) -> None:
    path = tmp_path / "leviton.json"
    cache = LevitonTokenCache(path)
    cache.save({"id": "token-value-aaaa", "userId": "person-1234"})
    path.chmod(0o644)

    assert cache.load() is not None
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


@pytest.fixture(autouse=True)
def _no_real_http(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Belt and braces: constructing a real ``aiohttp`` session is a test failure."""

    def explode(*args: object, **kwargs: object) -> None:
        raise AssertionError("a test tried to open a real HTTP session to Leviton")

    monkeypatch.setattr(
        LevitonAdapter, "_ensure_client", _ensure_client_or_explode(explode), raising=True
    )
    yield


def _ensure_client_or_explode(explode: Any) -> Any:
    original = LevitonAdapter._ensure_client

    def guarded(self: LevitonAdapter) -> Any:
        if getattr(self, "_client", None) is None:
            explode()
        return original(self)

    return guarded
