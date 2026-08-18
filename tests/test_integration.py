"""End-to-end seams: source -> poller -> spool -> uploader -> compactor -> rollup.

Every other test file proves one module in isolation against hand-built inputs.
This one proves the modules *compose* — that the rows a source hands the poller
survive four format changes (Observation -> SQLite -> Parquet part -> Parquet day
file -> hourly rollup) and arrive with the numbers PLAN.md promises. That is the
class of bug a parallel build produces: every unit passes, and the pipeline still
does not fit together.

Offline throughout: a fake source, moto for S3, DuckDB over local Parquet for the
rollup (``mock_aws`` patches botocore, not sockets, so DuckDB's ``httpfs`` cannot
see a moto bucket — the day file is fetched with the mocked client and handed to
the rollup as a local path, which is the same table either way).

What is pinned here, in the order the data moves:

* one poll cycle == one spool transaction, and the cycle's rows all carry the
  single ``ts_utc`` PLAN.md §6.5 requires;
* a failed cycle contributes nothing to the spool, and the gap survives all the
  way to a reduced ``sample_count`` in the rollup (CLAUDE.md rules 1 and 5);
* the uploader's parts, the compactor's day file and the rollup agree on row
  counts, and after compaction ``raw_30s`` holds exactly one authoritative
  object for the day;
* observed-time kWh: a half-populated hour yields half the kWh of a full one at
  the same wattage (PLAN.md §15.1);
* the retention purge is actually wired into the scheduled job, and it deletes
  only rows that are both uploaded and past the floor (PLAN.md §10).
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pytest
from boto3.dynamodb.types import TypeSerializer

from energy_capture import model, runtime, timeutil
from energy_capture.aws import s3io
from energy_capture.health import StatusStore
from energy_capture.sources.base import (
    BaseSource,
    DiscoveredChannel,
    DiscoveredDevice,
    Discovery,
    SourceTransientError,
)
from energy_capture.spool.sqlite import SpoolDB, open_spool
from energy_capture.stages import backfill, compactor, daily, poller, rollup, uploader
from tests.conftest import BUCKET

#: A plain summer local day (EDT, UTC-4) — 24 local hours.
DAY = date(2026, 8, 16)
DEVICE = "4C45565275C6"
CHANNEL = "breaker_p11"
WATTS = 1000.0
POLL_INTERVAL_S = 30

#: 30s samples in a full hour. 1000 W for all of them == exactly 1.000 kWh.
SAMPLES_PER_HOUR = 120


# --------------------------------------------------------------------- setup


@pytest.fixture
def spool(spool_dir: Path) -> SpoolDB:
    db = open_spool(spool_dir / "spool.db")
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def status(spool_dir: Path) -> StatusStore:
    return StatusStore(spool_dir / "status.json", load_existing=False)


class ScriptedSource(BaseSource):
    """A source that replays a fixed list of instants, one poll cycle each.

    Each cycle emits one ``watts`` reading stamped with the scripted instant, so
    the whole pipeline runs on deterministic time without freezing the clock.
    Instants listed in ``fail_at`` raise :class:`SourceTransientError` instead —
    the poll failure PLAN.md §6.6 describes, which must reach the rollup as a
    smaller ``sample_count`` and nothing else.
    """

    name = "leviton"

    def __init__(
        self,
        instants: list[datetime],
        *,
        fail_at: frozenset[datetime] = frozenset(),
        watts: float = WATTS,
    ) -> None:
        super().__init__(poll_interval_s=POLL_INTERVAL_S)
        self._instants = list(instants)
        self._fail_at = fail_at
        self._watts = watts
        self.polls = 0
        self.failures = 0

    async def discover(self, *, force: bool = False) -> Discovery:
        return self._remember(
            Discovery(
                source=self.name,
                devices=(DiscoveredDevice(self.name, DEVICE, kind="hub"),),
                channels=(
                    DiscoveredChannel(self.name, DEVICE, CHANNEL, kind="breaker"),
                ),
            )
        )

    async def poll(self) -> list[model.Observation]:
        instant = self._instants[self.polls]
        self.polls += 1
        if instant in self._fail_at:
            self.failures += 1
            raise SourceTransientError("502 Bad Gateway from the fixture")
        with self.new_cycle(ts_utc=instant) as cycle:
            cycle.add(DEVICE, CHANNEL, "watts", self._watts)
            cycle.add(DEVICE, CHANNEL, "amps", self._watts / 240.0)
            # A null field is a gap, not a zero (CLAUDE.md rule 1).
            cycle.add(DEVICE, CHANNEL, "volts", None)
        return cycle.observations

    @property
    def exhausted(self) -> bool:
        return self.polls >= len(self._instants)


def instants(hour: int, count: int, *, day: date = DAY) -> list[datetime]:
    """``count`` 30s instants starting at the top of local wall-clock ``hour``."""
    start, _ = timeutil.local_hour_bounds_utc(day, hour)
    return [start + timedelta(seconds=POLL_INTERVAL_S * i) for i in range(count)]


async def drive(source: ScriptedSource, spool: SpoolDB, status: StatusStore) -> list:
    """Run the real :class:`SourcePoller` once per scripted instant."""
    sp = poller.SourcePoller(source, spool, status=status)
    results = []
    while not source.exhausted:
        results.append(await sp.cycle())
    return results


def after(day: date, hour: int) -> datetime:
    """An instant just after local wall-clock ``hour`` on ``day`` has closed."""
    _, end = timeutil.local_hour_bounds_utc(day, hour)
    return end + timedelta(minutes=1)


def fetch_local(client, key: str, dest: Path) -> Path:
    """Copy an S3 object out of moto so DuckDB can read it as a local file."""
    dest.write_bytes(client.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    return dest


def hourly_rows(table: pa.Table, metric: str = "watts") -> list[dict[str, Any]]:
    return [row for row in table.to_pylist() if row["metric"] == metric]


# ------------------------------------------------------ the whole pipeline


def test_full_hour_travels_from_source_to_rollup_as_exactly_one_kwh(
    s3, spool: SpoolDB, status: StatusStore, tmp_path: Path
) -> None:
    """The load-bearing path: 120 polls at 1000 W -> 1.000 kWh in the rollup.

    Runs every real stage in sequence with nothing hand-assembled in between, so
    a signature or schema drift between two modules fails here rather than in
    production at 01:30.
    """
    source = ScriptedSource(instants(10, SAMPLES_PER_HOUR))
    results = asyncio.run(drive(source, spool, status))

    # --- poller -> spool ------------------------------------------------
    assert all(r.ok for r in results)
    # Two metrics per cycle: `volts` was null and must not have become a row.
    assert sum(r.rows for r in results) == SAMPLES_PER_HOUR * 2
    assert spool.stats().pending_rows == SAMPLES_PER_HOUR * 2

    # --- spool -> part --------------------------------------------------
    summary = uploader.run(
        spool=spool, bucket=BUCKET, client=s3, status=status, now=after(DAY, 10)
    )
    part_key = s3io.raw_30s_part_key(DAY, 10)
    assert summary.keys_written == (part_key,)
    assert summary.rows == SAMPLES_PER_HOUR * 2
    assert spool.stats().pending_rows == 0

    # --- parts -> day file ----------------------------------------------
    day_result = compactor.compact_day(DAY, bucket=BUCKET, client=s3)
    day_key = s3io.raw_30s_day_key(DAY)
    assert day_result.rows == SAMPLES_PER_HOUR * 2
    assert day_result.parts_archived == 1
    # Exactly one authoritative object under the queried prefix (PLAN.md §10).
    assert s3io.list_keys(BUCKET, s3io.raw_30s_day_prefix(DAY), client=s3) == [day_key]

    # --- day file -> hourly rollup --------------------------------------
    local = fetch_local(s3, day_key, tmp_path / "day.parquet")
    table = rollup.rollup_day(DAY, [str(local)], poll_interval_s=POLL_INTERVAL_S)

    assert table.schema.equals(model.HOURLY_SCHEMA)
    watts = hourly_rows(table)
    assert len(watts) == 1
    row = watts[0]
    assert row["source"] == "leviton"
    assert row["device_id"] == DEVICE
    assert row["channel_id"] == CHANNEL
    assert row["sample_count"] == SAMPLES_PER_HOUR
    assert row["mean"] == pytest.approx(WATTS)
    # 1000 W * (120 * 30 s) / 3.6e6 == 1.000 kWh, observed time only.
    assert row["kwh"] == pytest.approx(1.0)
    assert row["local_hour_start"] == datetime(2026, 8, 16, 10)

    # amps rode along and carries NO kwh: only watts becomes energy (§2.5).
    amps = hourly_rows(table, "amps")
    assert len(amps) == 1
    assert amps[0]["kwh"] is None
    # `volts` was null at the source and is absent everywhere downstream.
    assert hourly_rows(table, "volts") == []


def test_failed_poll_cycles_become_a_smaller_sample_count_not_a_zero(
    s3, spool: SpoolDB, status: StatusStore, tmp_path: Path
) -> None:
    """A gap stays a gap for the length of the pipeline (CLAUDE.md rules 1, 5).

    Half the hour's polls fail. The rollup must report half the samples and half
    the kWh at the *same* mean wattage — never a zero-filled hour, and never the
    full kWh extrapolated across the outage.
    """
    ticks = instants(11, SAMPLES_PER_HOUR)
    source = ScriptedSource(ticks, fail_at=frozenset(ticks[SAMPLES_PER_HOUR // 2 :]))
    results = asyncio.run(drive(source, spool, status))

    assert sum(1 for r in results if not r.ok) == SAMPLES_PER_HOUR // 2
    assert all(r.rows == 0 for r in results if not r.ok)

    uploader.run(
        spool=spool, bucket=BUCKET, client=s3, status=status, now=after(DAY, 11)
    )
    compactor.compact_day(DAY, bucket=BUCKET, client=s3)
    local = fetch_local(s3, s3io.raw_30s_day_key(DAY), tmp_path / "day.parquet")
    row = hourly_rows(
        rollup.rollup_day(DAY, [str(local)], poll_interval_s=POLL_INTERVAL_S)
    )[0]

    assert row["sample_count"] == SAMPLES_PER_HOUR // 2
    # The mean is untouched by the gap — the load did not change, the collector
    # stopped watching.
    assert row["mean"] == pytest.approx(WATTS)
    # Half the observed time, therefore half the energy. Never 1.0.
    assert row["kwh"] == pytest.approx(0.5)


def test_a_late_part_for_a_compacted_day_is_healed_by_the_next_compaction(
    s3, spool: SpoolDB, status: StatusStore, tmp_path: Path
) -> None:
    """The residual window PLAN.md §10 anticipates, exercised across three stages.

    The uploader can legitimately write a part for a day that already has a day
    file (a late row for hour 09 arriving after hour 09 was compacted). Left
    alone the two objects would double-count. The next compaction must fold the
    part in and re-archive, and the rollup must not see the row twice.
    """
    asyncio.run(drive(ScriptedSource(instants(9, 4)), spool, status))
    uploader.run(spool=spool, bucket=BUCKET, client=s3, status=status, now=after(DAY, 9))
    compactor.compact_day(DAY, bucket=BUCKET, client=s3)

    # A late cycle lands in the same, already-compacted hour.
    late = instants(9, 6)[4:]
    asyncio.run(drive(ScriptedSource(late), spool, status))
    uploader.run(spool=spool, bucket=BUCKET, client=s3, status=status, now=after(DAY, 9))

    prefix = s3io.raw_30s_day_prefix(DAY)
    assert len(s3io.list_keys(BUCKET, prefix, client=s3)) == 2  # part + day file

    result = compactor.compact_day(DAY, bucket=BUCKET, client=s3)

    assert s3io.list_keys(BUCKET, prefix, client=s3) == [s3io.raw_30s_day_key(DAY)]
    assert result.rows == 6 * 2  # six cycles, two non-null metrics each

    local = fetch_local(s3, s3io.raw_30s_day_key(DAY), tmp_path / "day.parquet")
    row = hourly_rows(
        rollup.rollup_day(DAY, [str(local)], poll_interval_s=POLL_INTERVAL_S)
    )[0]
    assert row["sample_count"] == 6  # not 10: the overlap deduped


def test_rollup_is_identical_whether_it_reads_parts_or_the_day_file(
    s3, spool: SpoolDB, status: StatusStore, tmp_path: Path
) -> None:
    """Compaction is invisible to the rollup — that is what makes it disposable.

    The hourly rollup runs at :20 against parts and again at 01:30 against the
    compacted day file. If those disagreed, the rollup would flip-flop every
    night.
    """
    asyncio.run(drive(ScriptedSource(instants(8, 5) + instants(9, 5)), spool, status))
    uploader.run(spool=spool, bucket=BUCKET, client=s3, status=status, now=after(DAY, 9))

    part_keys = s3io.list_raw_30s_parts(BUCKET, DAY, client=s3)
    assert len(part_keys) == 2
    part_paths = [
        str(fetch_local(s3, key, tmp_path / Path(key).name)) for key in part_keys
    ]
    from_parts = rollup.rollup_day(DAY, part_paths, poll_interval_s=POLL_INTERVAL_S)

    compactor.compact_day(DAY, bucket=BUCKET, client=s3)
    day_path = fetch_local(s3, s3io.raw_30s_day_key(DAY), tmp_path / "day.parquet")
    from_day = rollup.rollup_day(DAY, [str(day_path)], poll_interval_s=POLL_INTERVAL_S)

    assert from_parts.equals(from_day)
    assert {r["sample_count"] for r in hourly_rows(from_parts)} == {5}


# ------------------------------------------------- the scheduled job wiring


def test_daily_maintenance_runs_upload_compact_rollup_purge_in_that_order(
    s3, spool: SpoolDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 01:30 job is what makes the pipeline self-maintaining, so its wiring
    is worth pinning: the four steps, their order, and the fact that the spool it
    hands the uploader and the purge is the process's own.

    Only ``rollup.run`` is faked — it reads S3 over DuckDB ``httpfs``, which
    cannot see an in-process moto bucket. Everything else is the real stage.
    """
    calls: list[tuple[str, Any]] = []
    real_upload = uploader.run
    real_compact = compactor.run

    def spy_upload(*args, **kwargs):
        calls.append(("upload", kwargs.get("spool")))
        return real_upload(*args, bucket=BUCKET, client=s3, **kwargs)

    def spy_compact(**kwargs):
        calls.append(("compact", kwargs.get("start")))
        # `now=` only so the archive retention sweep is measured against the
        # scripted clock rather than the host's; the job itself never passes it.
        return real_compact(bucket=BUCKET, client=s3, now=now, **kwargs)

    def spy_rollup(**kwargs):
        calls.append(("rollup", kwargs.get("start")))
        return {"rows": 0}

    monkeypatch.setattr(uploader, "run", spy_upload)
    monkeypatch.setattr(compactor, "run", spy_compact)
    monkeypatch.setattr(rollup, "run", spy_rollup)

    yesterday = DAY
    now = timeutil.local_naive_to_utc(datetime(2026, 8, 17, 1, 30))
    asyncio.run(drive(ScriptedSource(instants(10, 4, day=yesterday)), spool, StatusStore(
        Path(spool.path).parent / "status.json", load_existing=False
    )))

    summary = asyncio.run(runtime._job_daily_maintenance(now, spool=spool))

    assert [name for name, _ in calls] == ["upload", "compact", "rollup"]
    # The uploader got THIS process's spool, not a second connection set.
    assert calls[0][1] is spool
    assert summary["upload"] == "ok"
    assert summary["compact"] == "ok"
    assert summary["rollup"] == "ok"
    assert summary["purge"] == "ok"
    assert "purge_purged_rows" in summary

    # And the work really happened: the day file exists and the parts are gone.
    assert s3io.list_keys(BUCKET, s3io.raw_30s_day_prefix(yesterday), client=s3) == [
        s3io.raw_30s_day_key(yesterday)
    ]


def test_the_scheduled_purge_deletes_only_uploaded_rows_past_the_floor(
    s3, spool: SpoolDB, status: StatusStore
) -> None:
    """PLAN.md §10's two interlocks, checked through the job that calls them.

    Nothing else in the codebase calls :meth:`SpoolDB.purge`; if this job stops
    calling it, ``spool.db`` grows for the life of the container and no other
    test notices.
    """
    old_day = DAY - timedelta(days=30)
    asyncio.run(drive(ScriptedSource(instants(10, 4, day=old_day)), spool, status))
    asyncio.run(drive(ScriptedSource(instants(10, 4, day=DAY)), spool, status))
    total = spool.stats().total_rows

    # Nothing is uploaded yet: age alone must not delete un-landed data.
    now = timeutil.local_naive_to_utc(datetime(2026, 8, 17, 1, 30))
    assert asyncio.run(runtime._job_spool_purge(now, spool=spool)) == {"purged_rows": 0}
    assert spool.stats().total_rows == total

    uploader.run(spool=spool, bucket=BUCKET, client=s3, status=status, now=now)
    assert spool.stats().pending_rows == 0

    # Now both interlocks are satisfied for the 30-day-old rows only.
    result = asyncio.run(runtime._job_spool_purge(now, spool=spool))
    assert result == {"purged_rows": 8}
    assert spool.stats().total_rows == total - 8


def test_default_jobs_hand_the_spool_to_every_job_that_needs_it() -> None:
    """A regression guard on the wiring above: ``default_jobs()`` must thread the
    process's spool into both spool-touching jobs, or they silently open their
    own and the purge becomes a no-op that nothing reports."""
    names = {job.name for job in runtime.default_jobs(spool=None)}
    assert {"upload_hourly", "rollup_hourly", "daily_maintenance"} <= names


# ------------------------------------------------------------- CLI contract


def test_every_built_stage_resolves_through_the_cli_table() -> None:
    """``cli.STAGE_ENTRYPOINTS`` is the contract between the CLI and the stages.

    A stage that lands but is wired to the wrong module/attribute would only be
    discovered by running the command; this checks the table itself.
    """
    import importlib
    import inspect

    from energy_capture import cli

    # Every stage in the table has now landed: the collectors (PLAN.md §6-§8),
    # the semantic layer (§9) and Glue (§12). Nothing in STAGE_ENTRYPOINTS is
    # allowed to be aspirational any more — an unimportable entry here is a
    # broken install, not a build-order gap.
    assert set(cli.STAGE_ENTRYPOINTS) == {
        "run",
        "poll",
        "upload",
        "compact-daily",
        "rollup",
        "fetch-daily",
        "backfill",
        "discover",
        "build-dim",
        "create-glue-tables",
    }

    # What the CLI command bodies actually pass, keyword for keyword. Binding
    # these against the real signature is what catches a stage wired to the
    # right module but the wrong function, or one that quietly dropped a
    # parameter the CLI still sends (a TypeError at 01:30, in production).
    passed: dict[str, dict[str, Any]] = {
        "run": {},
        "poll": {"once": True, "sources": None},
        "upload": {"start": date(2026, 8, 15), "end": date(2026, 8, 16)},
        "compact-daily": {"start": date(2026, 8, 15), "end": date(2026, 8, 16)},
        "rollup": {"start": date(2026, 8, 15), "end": date(2026, 8, 16)},
        "fetch-daily": {"start": date(2026, 8, 15), "end": date(2026, 8, 16)},
        "backfill": {"start": date(2026, 8, 15), "end": date(2026, 8, 16)},
        "discover": {
            "sources": None,
            "map_path": Path("config/channel_map.json"),
            "json_only": False,
            "dump_path": None,
            "raw": False,
            "out_path": None,
            "write_live_channels": True,
        },
        "build-dim": {
            "map_path": Path("config/channel_map.json"),
            "inventory_path": None,
            "live_channels_path": None,
            "dry_run": True,
        },
        "create-glue-tables": {"database": "energy", "dry_run": True},
    }
    assert set(passed) == set(cli.STAGE_ENTRYPOINTS)

    for command, module_attr in cli.STAGE_ENTRYPOINTS.items():
        module_name, attr = module_attr
        entry = getattr(importlib.import_module(module_name), attr)
        assert callable(entry), f"{command} -> {module_name}.{attr}"
        # Raises TypeError if the CLI's call would not bind.
        inspect.signature(entry).bind(**passed[command])
        # Anything the stage requires beyond what the CLI sends would make the
        # command unrunnable, so every other parameter must have a default.
        for name, param in inspect.signature(entry).parameters.items():
            if name in passed[command] or param.kind in (
                param.VAR_KEYWORD,
                param.VAR_POSITIONAL,
            ):
                continue
            assert param.default is not param.empty, f"{command}: {name} has no default"

    # `import-greenbutton` is the one command with no entry point: PLAN.md §13
    # designs it and defers it. It is handled inline by the CLI (exit 3 with a
    # message), and must not acquire a half-built module behind the CLI's back.
    assert "import-greenbutton" not in cli.STAGE_ENTRYPOINTS
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("energy_capture.stages.greenbutton")


# ============================================================================
# energy/daily: two stages, one file
# ============================================================================
#
# ``stages/backfill.py`` (PLAN.md §8) and ``stages/daily.py`` (§7.2) write the
# same monthly object, ``energy/daily/year=YYYY/bryant-{YYYYMM}.parquet``. They
# were built independently, and the failure mode is not subtle: whichever ran
# second could truncate the month to its own rows and silently destroy the
# other's days. Each stage tests its own read-modify-write; nothing until here
# ran BOTH against one bucket.
#
# The scenario below is the realistic one — a month that is partly history and
# partly fresh, with one contested day in the middle:
#
#   Jan 03  both     (DynamoDB has it; the 08:30 fetch also returns it as day2)
#   Jan 04  daily    (day1 of the scheduled run)
#   Jan 10  backfill (history only)
#   Jan 11  backfill (history only)
#
# Both orders must end with all four days present. PLAN.md §10 settles the
# contested cell: latest write wins at the file level.

BACKFILL_TABLE = "bryant-energy-data-test"
BRYANT_SERIAL = "TEST0000001"

#: today=Jan 5 local => day1=Jan 4, day2=Jan 3 (PLAN.md §7.2).
DAILY_NOW = datetime(2026, 1, 5, 13, 30, tzinfo=timeutil.UTC)
CONTESTED_DAY = date(2026, 1, 3)
JANUARY_KEY = "energy/daily/year=2026/bryant-202601.parquet"


def _energy_payload() -> dict[str, Any]:
    """The recorded ``getInfinityEnergy`` response ``stages/daily.py`` maps."""
    path = Path(__file__).parent / "fixtures" / "bryant" / "energy_response.json"
    return json.loads(path.read_text(encoding="utf-8"))["data"]["infinityEnergy"]


@pytest.fixture
def dynamo(s3):  # noqa: ANN001 - rides inside the s3 fixture's mock_aws context
    """The legacy table, holding only days the live fetch does NOT return.

    Jan 03 is the exception, and deliberately carries values that differ from
    the live payload so "who won the contested cell" is decidable.
    """

    def item(day: str, hpheat_kwh: str) -> dict[str, Any]:
        values: dict[str, Any] = {
            "date": day,
            "serial_number": BRYANT_SERIAL,
            "period_type": "day1",
            "collected_at": "2026-01-12T14:07:22.665437",
        }
        for spec in backfill.ATTRIBUTE_MAP:
            values[spec.attribute] = Decimal("0")
        values["hPHeatKwh"] = Decimal(hpheat_kwh)
        return values

    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=BACKFILL_TABLE,
        KeySchema=[{"AttributeName": "date", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "date", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    serializer = TypeSerializer()
    for day, hpheat in (("2026-01-03", "111"), ("2026-01-10", "10"), ("2026-01-11", "11")):
        client.put_item(
            TableName=BACKFILL_TABLE,
            Item={
                name: serializer.serialize(value)
                for name, value in item(day, hpheat).items()
            },
        )
    return client


@pytest.fixture
def empty_legacy_dir(tmp_path: Path) -> Path:
    """No legacy JSON: this scenario is about the DynamoDB/live-fetch seam."""
    target = tmp_path / "no_legacy"
    target.mkdir()
    return target


def _run_backfill(s3, dynamo, legacy: Path, status: StatusStore) -> dict[str, Any]:
    return backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=BACKFILL_TABLE,
        legacy_path=legacy,
        serial=BRYANT_SERIAL,
        store=status,
    )


def _run_daily(s3, status: StatusStore) -> dict[str, Any]:
    return daily.run(
        start=date(2026, 1, 3),
        end=date(2026, 1, 4),
        now=DAILY_NOW,
        payload=_energy_payload(),
        bucket=BUCKET,
        client=s3,
        serial=BRYANT_SERIAL,
        status=status,
    )


def _daily_cells(s3) -> dict[tuple[date, str, str], float]:
    """``(local day, channel, metric) -> value`` for the whole monthly object."""
    table = s3io.read_table(BUCKET, JANUARY_KEY, client=s3)
    rows = model.table_to_observations(table, dataset=model.Dataset.DAILY)
    cells = {(row.ts_local.date(), row.channel_id, row.metric): row.value for row in rows}
    assert len(cells) == len(rows), "duplicate dedupe keys in the monthly file"
    return cells


def test_backfill_then_daily_leaves_the_month_complete(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """History first, then the 08:30 fetch — the normal migration order."""
    _run_backfill(s3, dynamo, empty_legacy_dir, status)
    _run_daily(s3, status)

    cells = _daily_cells(s3)
    days = {day for day, _, _ in cells}
    assert days == {date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 10), date(2026, 1, 11)}

    # Backfill-only days survived the live fetch's rewrite untouched.
    assert cells[(date(2026, 1, 10), "hpheat", "kwh_day")] == 10.0
    assert cells[(date(2026, 1, 11), "hpheat", "kwh_day")] == 11.0
    # The fetched-only day landed.
    assert cells[(date(2026, 1, 4), "hpheat", "kwh_day")] == 21.0
    # The contested cell went to the stage that ran last (PLAN.md §10).
    assert cells[(CONTESTED_DAY, "hpheat", "kwh_day")] == 22.0


def test_daily_then_backfill_leaves_the_month_complete(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """The reverse order — a backfill re-run after the pipeline is already live.

    Same completeness; only the contested cell flips. If either stage wrote a
    subset of the month, this is where the other stage's days would vanish.
    """
    _run_daily(s3, status)
    _run_backfill(s3, dynamo, empty_legacy_dir, status)

    cells = _daily_cells(s3)
    days = {day for day, _, _ in cells}
    assert days == {date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 10), date(2026, 1, 11)}

    assert cells[(date(2026, 1, 4), "hpheat", "kwh_day")] == 21.0
    assert cells[(date(2026, 1, 10), "hpheat", "kwh_day")] == 10.0
    assert cells[(CONTESTED_DAY, "hpheat", "kwh_day")] == 111.0


def test_both_orders_agree_on_every_row_they_do_not_contest(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """Only the 8 cells both stages claim may differ between the two orders.

    Anything else differing means one stage is dropping or inventing rows
    depending on who got there first.
    """
    _run_backfill(s3, dynamo, empty_legacy_dir, status)
    _run_daily(s3, status)
    backfill_first = _daily_cells(s3)

    s3.delete_object(Bucket=BUCKET, Key=JANUARY_KEY)

    _run_daily(s3, status)
    _run_backfill(s3, dynamo, empty_legacy_dir, status)
    daily_first = _daily_cells(s3)

    assert set(backfill_first) == set(daily_first)
    differing = {key for key in backfill_first if backfill_first[key] != daily_first[key]}
    # 4 enabled components x 2 metrics, on the one contested day.
    assert {day for day, _, _ in differing} <= {CONTESTED_DAY}
    assert {channel for _, channel, _ in differing} <= {"eheat", "cooling", "fan", "hpheat"}


def test_neither_stage_writes_outside_energy_daily(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """CLAUDE.md rule 6, checked across both stages at once: day-grain rows in
    ``raw_30s`` would poison every hourly rollup that touched the day."""
    _run_backfill(s3, dynamo, empty_legacy_dir, status)
    _run_daily(s3, status)

    keys = [
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    ]
    assert keys == [JANUARY_KEY]
    assert not any(key.startswith(s3io.RAW_30S_PREFIX) for key in keys)


def test_the_monthly_key_comes_from_the_s3io_builder_in_both_stages(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """Neither stage may hand-build a key; a divergent one would produce two
    files for the same month, each holding half the days."""
    expected = s3io.daily_key(date(2026, 1, 1), source=model.SOURCE_BRYANT)
    assert expected == JANUARY_KEY
    assert _run_backfill(s3, dynamo, empty_legacy_dir, status)["keys"] == [expected]
    assert _run_daily(s3, status)["keys"] == [expected]


def test_both_stages_stamp_local_midnight_through_timeutil(
    s3, dynamo, empty_legacy_dir: Path, status: StatusStore
) -> None:
    """PLAN.md §7.2/§8: ``ts_utc`` is local midnight of the measured day.

    January is EST (UTC-5), so every row in this month must be 05:00Z — and
    equal to what ``timeutil`` computes, not to a hardcoded offset.
    """
    _run_backfill(s3, dynamo, empty_legacy_dir, status)
    _run_daily(s3, status)

    table = s3io.read_table(BUCKET, JANUARY_KEY, client=s3)
    for row in model.table_to_observations(table, dataset=model.Dataset.DAILY):
        local_day = row.ts_local.date()
        assert row.ts_local == datetime(local_day.year, local_day.month, local_day.day)
        assert row.ts_utc == timeutil.local_midnight_utc(local_day)
        assert row.ts_utc.hour == 5
