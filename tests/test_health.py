"""Tests for ``status.json`` and ``/healthz`` (PLAN.md §11).

Four properties are load-bearing and each gets a test:

1. the status write is **atomic** — no partial file, no stray temp file;
2. ``consecutive_failures`` counts up on failure and resets to 0 on success;
3. ``/healthz`` flips to non-200 at *exactly* 3× the poll interval, not before;
4. a crash mid-write leaves the *previous* document on disk, still valid JSON.

Time is injected (``FakeClock``) rather than frozen — the suite has no
``freezegun``-style dependency and should not grow one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from energy_capture import health
from energy_capture.health import (
    STALE_INTERVAL_MULTIPLIER,
    STATUS_SECTIONS,
    HealthServer,
    StatusStore,
    default_status_document,
)
from energy_capture.logging import REDACTED, register_secret
from energy_capture.timeutil import UTC

T0 = datetime(2026, 8, 16, 18, 0, 0, tzinfo=UTC)


class FakeClock:
    """A callable clock the tests advance by hand."""

    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float = 0, *, microseconds: int = 0) -> datetime:
        self.now += timedelta(seconds=seconds, microseconds=microseconds)
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """An empty directory of our own — ``tmp_path`` already holds ``SPOOL_DIR``."""
    path = tmp_path / "health"
    path.mkdir()
    return path


@pytest.fixture
def status_path(tmp_path: Path) -> Path:
    """Deliberately in a directory that does not exist yet: the writer creates it."""
    return tmp_path / "statusdir" / "status.json"


@pytest.fixture
def store(status_path: Path, clock: FakeClock) -> StatusStore:
    """A store with a single 30s poller watched, so staleness math is obvious."""
    return StatusStore(
        status_path,
        poll_intervals={"leviton": 30},
        clock=clock,
        load_existing=False,
    )


def read_doc(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def temp_files(path: Path) -> list[Path]:
    return sorted(p for p in path.parent.iterdir() if p.name != path.name)


# --------------------------------------------------------------------------
# Document shape
# --------------------------------------------------------------------------


def test_default_document_has_every_plan_section() -> None:
    doc = default_status_document()
    for section in STATUS_SECTIONS:
        assert section in doc, f"PLAN.md §11 section {section!r} missing"
        assert isinstance(doc[section], dict)
    # The §11 example's keys, verbatim.
    assert set(doc["leviton"]) >= {"last_success_utc", "consecutive_failures", "channels_seen"}
    assert set(doc["uploader"]) >= {"last_success_utc", "last_hour_uploaded", "rows"}
    assert set(doc["compactor"]) >= {"last_day_compacted", "rows"}
    assert set(doc["rollup"]) >= {"last_day_rolled", "rows"}
    assert set(doc["spool"]) >= {"pending_rows", "oldest_pending_utc"}


def test_nothing_is_zero_filled_before_it_happens() -> None:
    """"Never happened" is ``null``, never a zero timestamp (cardinal rule 1)."""
    doc = default_status_document()
    assert doc["leviton"]["last_success_utc"] is None
    assert doc["uploader"]["last_hour_uploaded"] is None
    assert doc["spool"]["oldest_pending_utc"] is None


def test_default_store_path_is_spool_dir_status_json(spool_dir: Path) -> None:
    assert StatusStore(load_existing=False).path == spool_dir / "status.json"


# --------------------------------------------------------------------------
# Atomic write
# --------------------------------------------------------------------------


def test_write_creates_directory_and_leaves_no_temp_file(store: StatusStore, status_path: Path) -> None:
    store.record_success("leviton", channels_seen=14)

    assert status_path.exists()
    assert temp_files(status_path) == [], "temp file left behind after a successful write"
    doc = read_doc(status_path)
    assert doc["leviton"]["channels_seen"] == 14


def test_every_write_is_a_complete_document(store: StatusStore, status_path: Path) -> None:
    """Whatever is on disk always parses and always has all the sections."""
    for index in range(20):
        store.record_success("uploader", rows=index, last_hour_uploaded="2026-08-16T13")
        doc = read_doc(status_path)
        for section in STATUS_SECTIONS:
            assert section in doc
        assert doc["uploader"]["rows"] == index
        assert temp_files(status_path) == []


def test_atomic_helper_uses_same_directory_and_replaces(workdir: Path) -> None:
    target = workdir / "nested" / "status.json"
    health.write_json_atomic(target, {"a": 1})
    health.write_json_atomic(target, {"a": 2})
    assert read_doc(target) == {"a": 2}
    assert [p.name for p in target.parent.iterdir()] == ["status.json"]


def test_bad_payload_never_creates_a_file(workdir: Path) -> None:
    """Serialisation happens before the temp file exists, so a bad doc is a no-op."""

    class Hostile:
        def __str__(self) -> str:
            raise RuntimeError("boom")

    target = workdir / "status.json"
    with pytest.raises(RuntimeError):
        health.write_json_atomic(target, {"bad": Hostile()})
    assert not target.exists()
    assert list(workdir.iterdir()) == []


# --------------------------------------------------------------------------
# Crash safety
# --------------------------------------------------------------------------


def test_crash_mid_write_leaves_previous_document_intact(
    store: StatusStore, status_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.record_success("uploader", rows=100, last_hour_uploaded="2026-08-16T12")
    before = read_doc(status_path)

    def die(src: object, dst: object) -> None:  # simulates a kill before the rename lands
        raise OSError("simulated crash mid-write")

    monkeypatch.setattr(health.os, "replace", die)
    # A failed status write is telemetry loss, not pipeline loss: it must not raise.
    store.record_success("uploader", rows=999, last_hour_uploaded="2026-08-16T13")
    monkeypatch.undo()

    after = read_doc(status_path)  # still valid JSON...
    assert after == before  # ...and still the *previous* complete document
    assert after["uploader"]["rows"] == 100
    assert temp_files(status_path) == [], "half-written temp file survived the crash"

    # The next successful write publishes the state that was held in memory.
    store.record_success("uploader", rows=1000)
    assert read_doc(status_path)["uploader"]["rows"] == 1000


def test_unwritable_path_does_not_raise(workdir: Path, clock: FakeClock) -> None:
    blocker = workdir / "not-a-dir"
    blocker.write_text("x", encoding="utf-8")
    store = StatusStore(blocker / "status.json", poll_intervals={}, clock=clock, load_existing=False)
    store.record_failure("leviton", "nowhere to write")  # must not raise
    assert store.section("leviton")["consecutive_failures"] == 1


def test_corrupt_existing_file_is_ignored(status_path: Path, clock: FakeClock) -> None:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text('{"leviton": {"last_suc', encoding="utf-8")  # truncated
    store = StatusStore(status_path, poll_intervals={}, clock=clock)
    assert store.snapshot()["leviton"]["consecutive_failures"] == 0
    store.record_success("leviton")
    assert read_doc(status_path)["leviton"]["last_success_utc"] is not None


def test_existing_document_survives_restart(status_path: Path, clock: FakeClock) -> None:
    first = StatusStore(status_path, poll_intervals={}, clock=clock, load_existing=False)
    first.record_failure("leviton", "502 from gateway")
    first.record_failure("leviton", "504 from gateway")

    restarted = StatusStore(status_path, poll_intervals={}, clock=clock)
    assert restarted.section("leviton")["consecutive_failures"] == 2
    assert restarted.snapshot()["started_utc"] is not None


# --------------------------------------------------------------------------
# Counters
# --------------------------------------------------------------------------


def test_consecutive_failures_increment_and_reset(store: StatusStore, status_path: Path) -> None:
    assert store.section("leviton")["consecutive_failures"] == 0

    for expected in (1, 2, 3):
        store.record_failure("leviton", RuntimeError("502 Bad Gateway"))
        assert store.section("leviton")["consecutive_failures"] == expected
        assert read_doc(status_path)["leviton"]["consecutive_failures"] == expected

    body = read_doc(status_path)["leviton"]
    assert body["last_error"] == "RuntimeError: 502 Bad Gateway"
    assert body["last_failure_utc"] is not None

    store.record_success("leviton", channels_seen=14)
    body = read_doc(status_path)["leviton"]
    assert body["consecutive_failures"] == 0
    assert "last_error" not in body
    assert "last_failure_utc" not in body
    assert body["channels_seen"] == 14


def test_failure_does_not_touch_last_success(store: StatusStore, clock: FakeClock) -> None:
    store.record_success("leviton")
    stamped = store.section("leviton")["last_success_utc"]
    clock.advance(60)
    store.record_failure("leviton", "boom")
    assert store.section("leviton")["last_success_utc"] == stamped


def test_set_merges_without_touching_counters(store: StatusStore, status_path: Path) -> None:
    store.record_failure("spool", "disk hiccup")
    store.set("spool", pending_rows=1240, oldest_pending_utc=T0)
    body = read_doc(status_path)["spool"]
    assert body["pending_rows"] == 1240
    assert body["oldest_pending_utc"] == "2026-08-16T18:00:00.000000Z"
    assert body["consecutive_failures"] == 1, "set() must not reset the failure counter"


def test_reset_failures_without_claiming_success(store: StatusStore) -> None:
    store.record_failure("leviton", "boom")
    store.reset_failures("leviton")
    body = store.section("leviton")
    assert body["consecutive_failures"] == 0
    assert body["last_success_utc"] is None


def test_ad_hoc_section_is_created_on_demand(store: StatusStore, status_path: Path) -> None:
    """PLAN.md §6.4: keepalive backoff is recorded here too."""
    store.record_failure("leviton_keepalive", "PUT bandwidth=1 failed", backoff_s=120)
    body = read_doc(status_path)["leviton_keepalive"]
    assert body["consecutive_failures"] == 1
    assert body["backoff_s"] == 120


def test_updates_are_serialised_across_threads(store: StatusStore, status_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    def work(index: int) -> None:
        if index % 2:
            store.record_failure("bryant_status", f"transient {index}")
        else:
            store.set("spool", pending_rows=index)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(work, range(200)))

    doc = read_doc(status_path)  # never a torn write
    assert doc["bryant_status"]["consecutive_failures"] == 100
    assert store.section("bryant_status")["consecutive_failures"] == 100
    assert temp_files(status_path) == []


async def test_usable_from_the_asyncio_loop(store: StatusStore, status_path: Path) -> None:
    async def tick(index: int) -> None:
        store.record_success("leviton", channels_seen=index)

    await asyncio.gather(*(tick(i) for i in range(25)))
    assert read_doc(status_path)["leviton"]["consecutive_failures"] == 0


# --------------------------------------------------------------------------
# Secrets (CLAUDE.md cardinal rule 8)
# --------------------------------------------------------------------------


def test_secrets_never_reach_status_json(store: StatusStore, status_path: Path) -> None:
    token = "lev-token-9f3a2b1c8d7e6f5a"
    register_secret(token)

    store.record_success(
        "leviton",
        access_token=token,
        password="hunter2hunter2",
        detail={"authorization": token, "channels": 14},
        note=f"logged in with token {token}",
    )
    store.record_failure("bryant_status", RuntimeError(f"401 for access_token={token}"))

    raw = status_path.read_text(encoding="utf-8")
    assert token not in raw
    assert "hunter2hunter2" not in raw
    body = read_doc(status_path)["leviton"]
    assert body["access_token"] == REDACTED
    assert body["password"] == REDACTED
    assert body["detail"]["authorization"] == REDACTED
    assert body["detail"]["channels"] == 14
    assert REDACTED in read_doc(status_path)["bryant_status"]["last_error"]


def test_long_errors_are_truncated(store: StatusStore) -> None:
    store.record_failure("uploader", "x" * 5000)
    assert len(store.section("uploader")["last_error"]) <= 500


# --------------------------------------------------------------------------
# /healthz staleness rule (PLAN.md §11)
# --------------------------------------------------------------------------


def test_healthz_flips_at_exactly_three_intervals(store: StatusStore, clock: FakeClock) -> None:
    store.record_success("leviton")
    limit = 30 * STALE_INTERVAL_MULTIPLIER
    assert limit == 90

    clock.advance(limit - 1)
    assert store.health_report()[0] == 200

    clock.advance(1)  # age == exactly 3× the interval: still healthy
    status, body = store.health_report()
    assert status == 200
    assert body["health"]["ok"] is True
    assert body["health"]["checks"][0]["age_s"] == pytest.approx(90.0)

    clock.advance(microseconds=1)  # *older than* 3× — the rule trips
    status, body = store.health_report()
    assert status == 503
    assert body["health"]["ok"] is False
    check = body["health"]["checks"][0]
    assert check["section"] == "leviton"
    assert check["stale"] is True
    assert check["max_age_s"] == 90

    store.record_success("leviton")  # a fresh poll heals it immediately
    assert store.health_report()[0] == 200


def test_healthz_uses_each_pollers_own_interval(status_path: Path, clock: FakeClock) -> None:
    store = StatusStore(
        status_path,
        poll_intervals={"leviton": 30, "bryant_status": 300},
        clock=clock,
        load_existing=False,
    )
    store.record_success("leviton")
    store.record_success("bryant_status")

    clock.advance(120)  # past 3×30 but well inside 3×300
    status, body = store.health_report()
    assert status == 503
    stale = {c["section"]: c["stale"] for c in body["health"]["checks"]}
    assert stale == {"leviton": True, "bryant_status": False}


def test_startup_grace_then_failure_when_no_poll_ever_succeeds(
    store: StatusStore, clock: FakeClock
) -> None:
    """A fresh process is healthy for three intervals, then honestly unhealthy."""
    assert store.health_report()[0] == 200
    clock.advance(90)
    assert store.health_report()[0] == 200
    clock.advance(1)
    status, body = store.health_report()
    assert status == 503
    assert body["health"]["checks"][0]["never_succeeded"] is True


def test_unwatched_sections_never_fail_health(store: StatusStore, clock: FakeClock) -> None:
    store.record_success("leviton")
    store.forget_poller("leviton")
    clock.advance(86_400)
    assert store.health_report()[0] == 200


def test_watch_poller_records_an_effective_cadence(store: StatusStore, clock: FakeClock) -> None:
    """PLAN.md §7.3: if the API throttles us, health follows the real cadence."""
    store.forget_poller("leviton")
    store.record_success("bryant_status")
    store.watch_poller("bryant_status", 600)
    clock.advance(1000)
    assert store.health_report()[0] == 200
    clock.advance(801)
    assert store.health_report()[0] == 503


def test_health_block_is_not_persisted(store: StatusStore, status_path: Path) -> None:
    store.record_success("leviton")
    store.health_report()
    assert "health" not in read_doc(status_path)
    assert "health" not in store.snapshot()


def test_report_contains_the_whole_status_document(store: StatusStore) -> None:
    store.record_success("uploader", rows=4212, last_hour_uploaded="2026-08-16T13")
    _, body = store.health_report()
    assert body["uploader"]["rows"] == 4212
    for section in STATUS_SECTIONS:
        assert section in body


# --------------------------------------------------------------------------
# The HTTP server
# --------------------------------------------------------------------------


async def http_get(port: int, path: str = "/healthz", method: str = "GET") -> tuple[int, dict | None]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(
        f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode("ascii")
    )
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=5)
    writer.close()
    await writer.wait_closed()

    head, _, body = raw.partition(b"\r\n\r\n")
    status = int(head.split(b"\r\n")[0].split()[1])
    return status, (json.loads(body) if body.strip() else None)


@pytest.fixture
async def server(store: StatusStore):
    srv = HealthServer(store, host="127.0.0.1", port=0)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.aclose()


async def test_healthz_serves_the_status_json(server: HealthServer, store: StatusStore) -> None:
    store.record_success("leviton", channels_seen=14)
    store.set("spool", pending_rows=1240)

    status, body = await http_get(server.port)
    assert status == 200
    assert body["leviton"]["channels_seen"] == 14
    assert body["spool"]["pending_rows"] == 1240
    assert body["health"]["ok"] is True


async def test_healthz_returns_503_when_the_poller_is_stale(
    server: HealthServer, store: StatusStore, clock: FakeClock
) -> None:
    store.record_success("leviton")
    assert (await http_get(server.port))[0] == 200

    clock.advance(90, microseconds=1)
    status, body = await http_get(server.port)
    assert status == 503
    assert body["health"]["checks"][0]["stale"] is True


async def test_query_string_and_alias_paths(server: HealthServer) -> None:
    for path in ("/healthz", "/healthz?verbose=1", "/health", "/", "/status.json"):
        status, body = await http_get(server.port, path)
        assert status == 200, path
        assert body is not None


async def test_unknown_path_is_404_and_bad_method_is_405(server: HealthServer) -> None:
    status, body = await http_get(server.port, "/nope")
    assert status == 404
    assert body["error"] == "not found"

    status, _ = await http_get(server.port, "/healthz", method="POST")
    assert status == 405


async def test_head_request_sends_no_body(server: HealthServer) -> None:
    status, body = await http_get(server.port, "/healthz", method="HEAD")
    assert status == 200
    assert body is None


async def test_server_survives_a_garbage_request(server: HealthServer) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
    writer.write(b"\x00\x01\x02 not http at all\r\n\r\n")
    await writer.drain()
    await asyncio.wait_for(reader.read(), timeout=5)
    writer.close()
    await writer.wait_closed()

    assert (await http_get(server.port))[0] == 200  # still serving


async def test_server_survives_an_abandoned_connection(server: HealthServer) -> None:
    """A connection that never sends a request line must not wedge the handler."""
    _, writer = await asyncio.open_connection("127.0.0.1", server.port)
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    await asyncio.sleep(0)
    assert (await http_get(server.port))[0] == 200


async def test_server_can_be_restarted_on_the_same_store(store: StatusStore) -> None:
    srv = HealthServer(store, host="127.0.0.1", port=0)
    port = await srv.start()
    assert port > 0
    await srv.aclose()
    await srv.aclose()  # idempotent
    with pytest.raises(OSError):
        await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=2)


async def test_serve_health_context_manager(store: StatusStore) -> None:
    async with health.serve_health(store, host="127.0.0.1", port=0) as srv:
        assert (await http_get(srv.port))[0] == 200


def test_process_wide_store_is_a_singleton(spool_dir: Path) -> None:
    health.reset_status_store(None)
    try:
        first = health.get_status_store()
        assert first is health.get_status_store()
        assert first.path == spool_dir / "status.json"
    finally:
        health.reset_status_store(None)


def test_status_file_is_written_under_spool_dir(spool_dir: Path) -> None:
    store = StatusStore(load_existing=False)
    store.record_success("rollup", last_day_rolled="2026-08-16", rows=1152)
    assert os.path.exists(spool_dir / "status.json")
    assert read_doc(spool_dir / "status.json")["rollup"]["rows"] == 1152


# =========================================================================
# Meter freshness: measured on the DATA, and never a 503
# =========================================================================


def _store_with_meter(tmp_path: Path, newest_age: timedelta | None):
    store = health.StatusStore(path=tmp_path / "status.json")
    if newest_age is not None:
        store.record_success(
            "greenbutton",
            rows=2018,
            newest_interval_utc=(datetime.now(UTC) - newest_age).isoformat(),
            meters=["1308468", "1326254"],
        )
    return store


def test_meter_freshness_is_absent_until_green_button_is_actually_used(
    tmp_path: Path,
) -> None:
    """A deployment that does not use Green Button grows no permanently-stale field."""
    _, doc = _store_with_meter(tmp_path, None).health_report()
    assert "meter" not in doc[health.HEALTH_SECTION]


def test_a_fresh_meter_reports_its_age_and_is_not_stale(tmp_path: Path) -> None:
    code, doc = _store_with_meter(tmp_path, timedelta(hours=3)).health_report()
    meter = doc[health.HEALTH_SECTION]["meter"]
    assert code == 200
    assert meter["stale"] is False
    assert meter["age_days"] == pytest.approx(0.125, abs=0.01)
    assert meter["stale_after_days"] == 3


def test_a_stale_meter_is_reported_but_never_fails_healthz(tmp_path: Path) -> None:
    """The lag is LG&E's, not ours.

    A utility that publishes late must not mark this container unhealthy or
    invite a restart. What IS loud is a revoked authorisation, which fails the
    greenbutton_daily job and lands in consecutive_failures. DEVIATIONS.md #177.
    """
    code, doc = _store_with_meter(tmp_path, timedelta(days=5)).health_report()
    meter = doc[health.HEALTH_SECTION]["meter"]
    assert meter["stale"] is True
    assert meter["age_days"] == pytest.approx(5.0, abs=0.01)
    assert code == 200, "utility publication lag must not produce a 503"


def test_meter_freshness_measures_the_data_not_the_job(tmp_path: Path) -> None:
    """The failure mode this exists for: a fetch that SUCCEEDS and returns nothing.

    `last_success_utc` is stamped now, so a job-based check would call this
    perfectly healthy while the newest interval it holds is a week old — which is
    exactly the state the meter dataset sat in while its token was revoked.
    """
    store = _store_with_meter(tmp_path, timedelta(days=7))
    _, doc = store.health_report()
    assert doc["greenbutton"]["last_success_utc"], "the job did succeed"
    assert doc[health.HEALTH_SECTION]["meter"]["stale"] is True
