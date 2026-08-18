"""Backfill of the legacy Bryant daily energy (PLAN.md §8, §7.2, §15.5).

Everything here runs offline: ``moto`` for both S3 and DynamoDB, and the real
fixture JSON in ``tests/fixtures/backfill/`` for the old collector's exports.
No Carrier credentials, no AWS account, no network.

The contract these tests hold the stage to:

* the 16-attribute mapping is pinned **attribute by attribute**, so a rename can
  never silently drop a component;
* both legacy formats produce byte-for-byte identical rows for the same day;
* DynamoDB wins where the two overlap (it carries provenance);
* recorded zeros are written **as recorded** — the opposite of the live daily
  fetch's rule, and deliberately so (§8);
* an *absent* attribute is still a gap;
* local midnight -> UTC is right on both DST transition days;
* a double run over the same range writes byte-identical objects;
* DynamoDB is touched with ``Scan`` and nothing else;
* day-grain rows never land anywhere near ``raw_30s``.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pyarrow.parquet as pa_pq
import pytest

from energy_capture import model, timeutil
from energy_capture.aws import s3io
from energy_capture.config import get_settings, reset_settings_cache
from energy_capture.health import StatusStore
from energy_capture.logging import configure_logging
from energy_capture.stages import backfill

from tests.conftest import BUCKET

FIXTURES = Path(__file__).parent / "fixtures" / "backfill"

LEGACY_2026_01 = FIXTURES / "legacy_energy_2026_01.json"
LEGACY_2026_03 = FIXTURES / "legacy_energy_2026_03.json"
LEGACY_2026_11 = FIXTURES / "legacy_energy_2026_11.json"
DYNAMO_ITEMS = FIXTURES / "dynamodb_items.json"

TABLE = "bryant-energy-data-test"
SERIAL = "TEST0000001"


# ======================================================================
# Fixtures
# ======================================================================


@pytest.fixture
def legacy_dir(tmp_path: Path) -> Path:
    """A directory holding all three legacy JSON fixtures, ``energy_*.json``."""
    target = tmp_path / "energy_data"
    target.mkdir()
    for source in (LEGACY_2026_01, LEGACY_2026_03, LEGACY_2026_11):
        (target / source.name.removeprefix("legacy_")).write_text(
            source.read_text(encoding="utf-8"), encoding="utf-8"
        )
    return target


def _dynamo_items() -> list[dict[str, Any]]:
    """The fixture items, numbers as ``Decimal`` — what boto3 produces live."""
    document = json.loads(DYNAMO_ITEMS.read_text(encoding="utf-8"), parse_float=Decimal)
    return document["items"]


@pytest.fixture
def dynamo(s3):  # noqa: ANN001 - the s3 fixture opens the shared mock_aws context
    """A moto-backed DynamoDB client with the legacy table seeded.

    Depends on the shared ``s3`` fixture purely so both services live inside the
    same ``mock_aws`` context; a test can then run the whole stage end to end.
    """
    from boto3.dynamodb.types import TypeSerializer

    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName=TABLE,
        KeySchema=[{"AttributeName": "date", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "date", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    serializer = TypeSerializer()
    for item in _dynamo_items():
        client.put_item(
            TableName=TABLE,
            Item={name: serializer.serialize(value) for name, value in item.items()},
        )
    return client


@pytest.fixture
def status(tmp_path: Path) -> StatusStore:
    return StatusStore(tmp_path / "status.json", load_existing=False)


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


def record(local_day: date, values: dict[str, Any], *, origin: str = backfill.ORIGIN_DYNAMODB):
    return backfill.DailyRecord(
        local_day=local_day, serial=SERIAL, origin=origin, values=values
    )


def rows_by_key(observations) -> dict[tuple[str, str], float]:
    return {(o.channel_id, o.metric): o.value for o in observations}


def object_bytes(s3_client, key: str) -> bytes:
    return s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def read_daily(s3_client, key: str):
    return s3io.read_table(BUCKET, key, client=s3_client)


# ======================================================================
# The 16-attribute mapping — pinned metric by metric
# ======================================================================

#: Written out by hand, on purpose. If a future refactor renames a component or
#: "simplifies" the map into string munging, this literal is what fails.
EXPECTED_MAP: tuple[tuple[str, str, str], ...] = (
    ("eHeatKwh", "eheat", "kwh_day"),
    ("eHeatDollars", "eheat", "cost_day_usd"),
    ("coolingKwh", "cooling", "kwh_day"),
    ("coolingDollars", "cooling", "cost_day_usd"),
    ("fanKwh", "fan", "kwh_day"),
    ("fanDollars", "fan", "cost_day_usd"),
    ("fanGasKwh", "fangas", "kwh_day"),
    ("fanGasDollars", "fangas", "cost_day_usd"),
    ("hPHeatKwh", "hpheat", "kwh_day"),
    ("hPHeatDollars", "hpheat", "cost_day_usd"),
    ("loopPumpKwh", "looppump", "kwh_day"),
    ("loopPumpDollars", "looppump", "cost_day_usd"),
    ("gasKwh", "gas", "kwh_day"),
    ("gasDollars", "gas", "cost_day_usd"),
    ("reheatKwh", "reheat", "kwh_day"),
    ("reheatDollars", "reheat", "cost_day_usd"),
)


def test_attribute_map_is_pinned_attribute_by_attribute() -> None:
    actual = tuple(
        (spec.attribute, spec.channel_id, spec.metric) for spec in backfill.ATTRIBUTE_MAP
    )
    assert actual == EXPECTED_MAP


def test_attribute_map_has_exactly_sixteen_entries() -> None:
    assert len(backfill.ATTRIBUTE_MAP) == 16
    assert len(backfill.ATTRIBUTE_INDEX) == 16


@pytest.mark.parametrize(
    ("attribute", "channel_id", "metric"),
    EXPECTED_MAP,
    ids=[a for a, _, _ in EXPECTED_MAP],
)
def test_each_attribute_maps_to_its_component(
    attribute: str, channel_id: str, metric: str
) -> None:
    """One test per attribute: a dropped component fails by name, not by count."""
    spec = backfill.ATTRIBUTE_INDEX[attribute]
    assert spec.channel_id == channel_id
    assert spec.metric == metric


def test_the_camelcase_traps_are_mapped_the_way_plan_7_2_spells_them() -> None:
    """The four names that no lowercasing rule gets right on its own."""
    assert backfill.ATTRIBUTE_INDEX["hPHeatDollars"].channel_id == "hpheat"
    assert backfill.ATTRIBUTE_INDEX["eHeatKwh"].channel_id == "eheat"
    assert backfill.ATTRIBUTE_INDEX["fanGasKwh"].channel_id == "fangas"
    assert backfill.ATTRIBUTE_INDEX["loopPumpDollars"].channel_id == "looppump"


def test_every_component_has_both_a_kwh_and_a_cost_row() -> None:
    pairs = {(spec.channel_id, spec.metric) for spec in backfill.ATTRIBUTE_MAP}
    for component in backfill.COMPONENTS:
        assert (component, "kwh_day") in pairs
        assert (component, "cost_day_usd") in pairs
    assert {c for c, _ in pairs} == set(backfill.COMPONENTS)


def test_channel_ids_are_lowercase_and_units_come_from_the_model() -> None:
    for spec in backfill.ATTRIBUTE_MAP:
        assert spec.channel_id == spec.channel_id.lower()
        assert spec.metric in model.DAY_GRAIN_METRICS
    assert backfill.ATTRIBUTE_INDEX["eHeatKwh"].unit == "kWh"
    assert backfill.ATTRIBUTE_INDEX["eHeatDollars"].unit == "USD"


# ======================================================================
# Row mapping (PLAN.md §7.2)
# ======================================================================


def test_a_full_record_emits_sixteen_day_grain_rows() -> None:
    values = {attribute: Decimal("1.5") for attribute, _, _ in EXPECTED_MAP}
    rows = backfill.record_to_observations(record(date(2026, 1, 2), values))

    assert len(rows) == 16
    assert {o.source for o in rows} == {model.SOURCE_BRYANT}
    assert {o.device_id for o in rows} == {SERIAL}
    assert {o.metric for o in rows} == {"kwh_day", "cost_day_usd"}
    assert {o.unit for o in rows} == {"kWh", "USD"}
    assert {o.channel_id for o in rows} == set(backfill.COMPONENTS)


def test_ts_utc_is_local_midnight_and_ts_local_is_that_midnight() -> None:
    day = date(2026, 8, 16)  # boring summer day, EDT (UTC-4)
    rows = backfill.record_to_observations(record(day, {"eHeatKwh": Decimal("4")}))

    assert rows[0].ts_utc == timeutil.local_midnight_utc(day)
    assert rows[0].ts_utc == datetime(2026, 8, 16, 4, 0, tzinfo=timeutil.UTC)
    assert rows[0].ts_local == datetime(2026, 8, 16, 0, 0)
    assert rows[0].ts_local.tzinfo is None


def test_local_midnight_to_utc_is_correct_on_the_spring_forward_day() -> None:
    """2026-03-08 is 23 local hours long; its midnight is still EST (UTC-5)."""
    day = date(2026, 3, 8)
    assert timeutil.local_hours_in_day(day) == 23

    rows = backfill.record_to_observations(record(day, {"hPHeatKwh": Decimal("17")}))
    assert rows[0].ts_utc == datetime(2026, 3, 8, 5, 0, tzinfo=timeutil.UTC)
    assert rows[0].ts_local == datetime(2026, 3, 8, 0, 0)
    # The row must land in that local day's partition, not the previous one.
    assert timeutil.local_date_of(rows[0].ts_utc) == day


def test_local_midnight_to_utc_is_correct_on_the_fall_back_day() -> None:
    """2026-11-01 is 25 local hours long; its midnight is still EDT (UTC-4)."""
    day = date(2026, 11, 1)
    assert timeutil.local_hours_in_day(day) == 25

    rows = backfill.record_to_observations(record(day, {"coolingKwh": Decimal("7")}))
    assert rows[0].ts_utc == datetime(2026, 11, 1, 4, 0, tzinfo=timeutil.UTC)
    assert rows[0].ts_local == datetime(2026, 11, 1, 0, 0)
    assert timeutil.local_date_of(rows[0].ts_utc) == day


def test_the_two_dst_days_get_different_utc_offsets() -> None:
    """A fixed offset would put one of them on the wrong local date."""
    spring = backfill.record_to_observations(
        record(date(2026, 3, 8), {"eHeatKwh": Decimal("1")})
    )[0]
    fall = backfill.record_to_observations(
        record(date(2026, 11, 1), {"eHeatKwh": Decimal("1")})
    )[0]
    assert spring.ts_utc.hour == 5  # EST
    assert fall.ts_utc.hour == 4  # EDT


def test_recorded_zeros_are_written_as_recorded() -> None:
    """§8, deliberately the opposite of the live fetch's skip-disabled rule.

    We cannot know retroactively whether a component was structurally disabled,
    so a zero the API recorded is written as a zero.
    """
    values = {attribute: Decimal("0") for attribute, _, _ in EXPECTED_MAP}
    rows = backfill.record_to_observations(record(date(2026, 1, 2), values))

    assert len(rows) == 16
    assert all(o.value == 0.0 for o in rows)


def test_an_absent_attribute_emits_no_row() -> None:
    """Zero is recorded; *absent* is not invented (CLAUDE.md rule 1)."""
    rows = backfill.record_to_observations(
        record(date(2026, 1, 5), {"eHeatKwh": Decimal("2"), "eHeatDollars": Decimal("0.22")})
    )
    assert rows_by_key(rows) == {("eheat", "kwh_day"): 2.0, ("eheat", "cost_day_usd"): 0.22}


@pytest.mark.parametrize(
    "value",
    [None, "", "not-a-number", [], {}, True, Decimal("NaN"), Decimal("Infinity")],
    ids=["none", "empty", "text", "list", "dict", "bool", "nan", "inf"],
)
def test_a_null_or_non_numeric_attribute_emits_no_row(value: Any) -> None:
    rows = backfill.record_to_observations(record(date(2026, 1, 2), {"eHeatKwh": value}))
    assert rows == []


def test_a_numeric_string_is_recorded_but_flagged(log_stream) -> None:
    """DynamoDB numbers arrive as ``Decimal``; a string one is worth noticing."""
    rows = backfill.record_to_observations(record(date(2026, 1, 2), {"eHeatKwh": "4.5"}))
    assert [o.value for o in rows] == [4.5]
    assert log_events(log_stream, "backfill_string_number")


def test_an_unmapped_attribute_is_warned_not_silently_dropped(log_stream) -> None:
    """The tripwire for a future field rename (``hPHeatKwh`` -> something else)."""
    backfill.record_to_observations(
        record(date(2026, 1, 5), {"eHeatKwh": Decimal("2"), "heatPumpHeatKwh": Decimal("31")})
    )
    events = log_events(log_stream, "backfill_unknown_attribute")
    assert events and events[0]["attributes"] == ["heatPumpHeatKwh"]


def test_bookkeeping_attributes_do_not_trip_the_rename_warning(log_stream) -> None:
    backfill.record_to_observations(
        record(
            date(2026, 1, 2),
            {
                "serial_number": SERIAL,
                "period_type": "day1",
                "collected_at": "2026-01-03T14:07:22",
                "energyPeriodType": "day1",
                "eHeatKwh": Decimal("4"),
            },
        )
    )
    assert log_events(log_stream, "backfill_unknown_attribute") == []


def test_decimal_precision_loss_is_logged_but_the_row_still_lands(log_stream) -> None:
    """PLAN.md: Decimal -> float must not lose precision *silently*."""
    lossy = Decimal("0.123456789012345678901234567890")
    rows = backfill.record_to_observations(record(date(2026, 1, 2), {"eHeatKwh": lossy}))

    assert len(rows) == 1  # dropping it would manufacture a gap
    events = log_events(log_stream, "backfill_precision_loss")
    assert events and events[0]["decimal"] == str(lossy)
    assert events[0]["attribute"] == "eHeatKwh"


def test_ordinary_carrier_decimals_round_trip_without_a_warning(log_stream) -> None:
    rows = backfill.record_to_observations(
        record(date(2026, 1, 2), {"hPHeatDollars": Decimal("2.4199998")})
    )
    assert rows[0].value == 2.4199998
    assert log_events(log_stream, "backfill_precision_loss") == []


def test_nonzero_gas_kwh_is_flagged_for_a_human(log_stream) -> None:
    """§7.2: the field says kWh but gas probably is not kWh. Never guess."""
    backfill.record_to_observations(record(date(2026, 1, 2), {"gasKwh": Decimal("12")}))
    assert log_events(log_stream, "backfill_gas_kwh_nonzero")

    backfill.record_to_observations(record(date(2026, 1, 3), {"gasKwh": Decimal("0")}))
    assert len(log_events(log_stream, "backfill_gas_kwh_nonzero")) == 1


# ======================================================================
# Source B — the legacy JSON
# ======================================================================


def test_the_real_legacy_file_shape_parses() -> None:
    records = backfill.load_legacy_file(LEGACY_2026_01, serial=SERIAL)
    by_day = {r.local_day: r for r in records}

    assert set(by_day) == {date(2026, 1, 2), date(2026, 1, 3)}
    assert by_day[date(2026, 1, 3)].period_type == "day1"
    assert by_day[date(2026, 1, 2)].period_type == "day2"
    assert by_day[date(2026, 1, 3)].collected_at == "2026-01-04T14:07:22.665437"
    assert by_day[date(2026, 1, 3)].serial == SERIAL
    assert by_day[date(2026, 1, 3)].origin == backfill.ORIGIN_LEGACY_JSON


def test_legacy_rows_carry_the_configured_serial_because_the_file_has_none() -> None:
    records = backfill.load_legacy_file(LEGACY_2026_01, serial="OTHER-SERIAL")
    rows = backfill.record_to_observations(records[0])
    assert {o.device_id for o in rows} == {"OTHER-SERIAL"}


def test_legacy_json_floats_keep_their_decimal_text() -> None:
    """Parsed with ``parse_float=Decimal`` so the one float conversion is checked."""
    records = {r.local_day: r for r in backfill.load_legacy_file(LEGACY_2026_01, serial=SERIAL)}
    raw = records[date(2026, 1, 3)].values["hPHeatDollars"]
    assert isinstance(raw, Decimal)
    assert str(raw) == "2.3099999"


def test_a_directory_of_legacy_files_is_read_in_sorted_order(legacy_dir: Path) -> None:
    files = backfill.legacy_json_files(legacy_dir)
    assert [p.name for p in files] == [
        "energy_2026_01.json",
        "energy_2026_03.json",
        "energy_2026_11.json",
    ]
    records = backfill.load_legacy_json(legacy_dir, serial=SERIAL)
    assert {r.local_day for r in records} == {
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 3, 7),
        date(2026, 3, 8),
        date(2026, 11, 1),
    }


def test_a_missing_legacy_path_is_a_warning_not_a_crash(tmp_path: Path, log_stream) -> None:
    assert backfill.legacy_json_files(tmp_path / "nope") == []
    assert log_events(log_stream, "backfill_legacy_path_missing")


def test_the_legacy_path_is_configurable_and_defaults_sensibly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``BRYANT_LEGACY_JSON_PATH`` reaches the stage through ``Settings``.

    ``get_settings()`` is cached for the life of the process, so changing the
    environment mid-test requires clearing it — exactly what a real process does
    at startup and never again.
    """
    monkeypatch.delenv(backfill.LEGACY_PATH_ENV, raising=False)
    reset_settings_cache()
    assert backfill.resolve_legacy_path() == Path(
        "~/code/bryantDataCollector/energy_data"
    ).expanduser()

    monkeypatch.setenv(backfill.LEGACY_PATH_ENV, str(tmp_path))
    reset_settings_cache()
    assert backfill.resolve_legacy_path() == tmp_path
    # ...and it is a real Settings field, not an ad-hoc os.environ read.
    assert get_settings().bryant_legacy_json_path == tmp_path

    # An explicit argument always beats the environment.
    assert backfill.resolve_legacy_path(LEGACY_2026_01) == LEGACY_2026_01


def test_an_unparseable_legacy_key_is_skipped_with_a_warning(
    tmp_path: Path, log_stream
) -> None:
    path = tmp_path / "energy_bad.json"
    path.write_text(
        json.dumps({"not-a-date": {"period_type": "day1", "data": {"eHeatKwh": 1}}}),
        encoding="utf-8",
    )
    assert backfill.load_legacy_file(path, serial=SERIAL) == []
    assert log_events(log_stream, "backfill_unparseable_date")


def test_a_legacy_entry_without_data_is_skipped(tmp_path: Path, log_stream) -> None:
    path = tmp_path / "energy_nodata.json"
    path.write_text(json.dumps({"2026-01-02": {"period_type": "day1"}}), encoding="utf-8")
    assert backfill.load_legacy_file(path, serial=SERIAL) == []
    assert log_events(log_stream, "backfill_legacy_entry_without_data")


# ======================================================================
# Source A — DynamoDB
# ======================================================================


def test_scan_reads_every_item(dynamo) -> None:
    records = backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)
    assert {r.local_day for r in records} == {
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 5),
        date(2026, 2, 15),
        date(2026, 3, 8),
        date(2026, 11, 1),
    }
    assert {r.origin for r in records} == {backfill.ORIGIN_DYNAMODB}


def test_scan_preserves_decimals_and_provenance(dynamo) -> None:
    records = {
        r.local_day: r
        for r in backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)
    }
    jan2 = records[date(2026, 1, 2)]
    assert jan2.period_type == "day2"
    assert jan2.collected_at == "2026-01-04T14:07:22.665437"
    assert isinstance(jan2.values["hPHeatDollars"], Decimal)
    assert str(jan2.values["hPHeatDollars"]) == "2.4199998"
    # The partition key is not a metric and must not travel as one.
    assert backfill.DATE_ATTRIBUTE not in jan2.values


def test_an_item_without_a_serial_falls_back_to_carrier_serial(dynamo, log_stream) -> None:
    records = {
        r.local_day: r
        for r in backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)
    }
    assert records[date(2026, 2, 15)].serial == SERIAL
    events = log_events(log_stream, "backfill_missing_serial")
    assert events and events[0]["local_date"] == "2026-02-15"


def test_dynamodb_is_touched_with_scan_and_nothing_else(dynamo) -> None:
    """The stage must need only ``dynamodb:Scan`` on that one table (§8)."""
    operations: list[str] = []

    def spy(params, model, context, **kwargs):  # noqa: ANN001 - botocore hook
        operations.append(model.name)

    dynamo.meta.events.register("before-parameter-build.dynamodb", spy)
    backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)

    assert operations, "no DynamoDB call was made at all"
    assert set(operations) == {"Scan"}


def test_the_dynamodb_client_is_pinned_to_the_tables_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``AWS_REGION`` moves the bucket; it must not move the legacy table (§8)."""
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    reset_settings_cache()
    s3io.reset_clients()
    try:
        assert backfill.DYNAMODB_REGION == "us-east-1"
        assert backfill._dynamodb_client().meta.region_name == "us-east-1"
        assert backfill._dynamodb_client(region="us-west-2").meta.region_name == "us-west-2"
    finally:
        s3io.reset_clients()
        reset_settings_cache()


def test_the_stage_never_calls_a_mutating_dynamodb_operation(dynamo) -> None:
    """A belt-and-braces guard: any write attempt blows up loudly."""

    class ReadOnlyClient:
        MUTATORS = frozenset(
            {
                "put_item",
                "update_item",
                "delete_item",
                "batch_write_item",
                "transact_write_items",
                "create_table",
                "update_table",
                "delete_table",
                "restore_table_from_backup",
            }
        )

        def __init__(self, inner) -> None:  # noqa: ANN001
            self._inner = inner

        def __getattr__(self, name: str):
            if name in self.MUTATORS:
                raise AssertionError(f"backfill must never call {name}()")
            return getattr(self._inner, name)

    records = backfill.scan_dynamodb(
        table=TABLE, client=ReadOnlyClient(dynamo), serial_default=SERIAL
    )
    assert records


# ======================================================================
# §15.5 — both legacy formats parse to IDENTICAL row shapes
# ======================================================================


def test_both_legacy_formats_produce_identical_rows_for_the_same_day(dynamo) -> None:
    """The core §15.5 assertion, on a date the two stores agree about."""
    day = date(2026, 1, 2)

    from_dynamo = next(
        r
        for r in backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)
        if r.local_day == day
    )
    from_json = next(
        r
        for r in backfill.load_legacy_file(LEGACY_2026_01, serial=SERIAL)
        if r.local_day == day
    )

    dynamo_rows = backfill.record_to_observations(from_dynamo)
    json_rows = backfill.record_to_observations(from_json)

    assert len(dynamo_rows) == 16
    # Identical *rows*, not merely identical values: same ts, source, device,
    # channel, metric, value and unit — so they collapse under the dedupe key.
    assert dynamo_rows == json_rows
    assert {
        tuple(getattr(o, k) for k in model.DEDUPE_KEY) for o in dynamo_rows
    } == {tuple(getattr(o, k) for k in model.DEDUPE_KEY) for o in json_rows}


def test_the_two_formats_agree_on_every_one_of_the_sixteen_metrics(dynamo) -> None:
    day = date(2026, 1, 2)
    from_dynamo = next(
        r
        for r in backfill.scan_dynamodb(table=TABLE, client=dynamo, serial_default=SERIAL)
        if r.local_day == day
    )
    from_json = next(
        r
        for r in backfill.load_legacy_file(LEGACY_2026_01, serial=SERIAL)
        if r.local_day == day
    )
    assert rows_by_key(backfill.record_to_observations(from_dynamo)) == rows_by_key(
        backfill.record_to_observations(from_json)
    ) == {
        ("eheat", "kwh_day"): 5.0,
        ("eheat", "cost_day_usd"): 0.55,
        ("cooling", "kwh_day"): 0.0,
        ("cooling", "cost_day_usd"): 0.0,
        ("fan", "kwh_day"): 1.0,
        ("fan", "cost_day_usd"): 0.11,
        ("fangas", "kwh_day"): 0.0,
        ("fangas", "cost_day_usd"): 0.0,
        ("hpheat", "kwh_day"): 22.0,
        ("hpheat", "cost_day_usd"): 2.4199998,
        ("looppump", "kwh_day"): 0.0,
        ("looppump", "cost_day_usd"): 0.0,
        ("gas", "kwh_day"): 0.0,
        ("gas", "cost_day_usd"): 0.0,
        ("reheat", "kwh_day"): 0.0,
        ("reheat", "cost_day_usd"): 0.0,
    }


# ======================================================================
# Overlap: DynamoDB wins
# ======================================================================


def test_dynamodb_wins_on_overlap() -> None:
    day = date(2026, 1, 3)
    dynamo_record = record(day, {"hPHeatKwh": Decimal("95")})
    json_record = record(day, {"hPHeatKwh": Decimal("21")}, origin=backfill.ORIGIN_LEGACY_JSON)

    # Both orderings must give the same answer: precedence is by origin, not by
    # the order the caller happened to concatenate the lists in.
    for records in ([dynamo_record, json_record], [json_record, dynamo_record]):
        table, counts = backfill.build_month_table(records)
        assert table.num_rows == 1
        assert table.column("value").to_pylist() == [95.0]
        assert counts == {backfill.ORIGIN_DYNAMODB: 1}


def test_non_overlapping_legacy_rows_survive_the_merge() -> None:
    day = date(2026, 1, 3)
    table, counts = backfill.build_month_table(
        [
            record(day, {"hPHeatKwh": Decimal("95")}),
            record(
                day,
                {"hPHeatKwh": Decimal("21"), "fanKwh": Decimal("3")},
                origin=backfill.ORIGIN_LEGACY_JSON,
            ),
        ]
    )
    assert table.num_rows == 2
    assert counts == {backfill.ORIGIN_DYNAMODB: 1, backfill.ORIGIN_LEGACY_JSON: 1}
    values = dict(zip(table.column("channel_id").to_pylist(), table.column("value").to_pylist()))
    assert values == {"hpheat": 95.0, "fan": 3.0}


def test_backfill_rows_beat_rows_already_in_the_monthly_file() -> None:
    day = date(2026, 1, 3)
    existing = backfill.record_to_observations(
        record(day, {"hPHeatKwh": Decimal("1")}, origin=backfill.ORIGIN_LEGACY_JSON)
    )
    table, counts = backfill.build_month_table(
        [record(day, {"hPHeatKwh": Decimal("95")})], existing
    )
    assert table.column("value").to_pylist() == [95.0]
    assert counts == {backfill.ORIGIN_DYNAMODB: 1}


def test_existing_rows_the_backfill_has_no_opinion_about_are_preserved() -> None:
    """Regenerating a month must never delete a day this run did not cover."""
    existing = backfill.record_to_observations(
        record(date(2026, 1, 20), {"fanKwh": Decimal("7")})
    )
    table, counts = backfill.build_month_table(
        [record(date(2026, 1, 3), {"hPHeatKwh": Decimal("95")})], existing
    )
    assert table.num_rows == 2
    assert counts[backfill.ORIGIN_EXISTING] == 1
    assert sorted(table.column("ts_local").to_pylist()) == [
        datetime(2026, 1, 3, 0, 0),
        datetime(2026, 1, 20, 0, 0),
    ]


def test_the_month_table_is_sorted_and_uses_the_daily_schema() -> None:
    table, _ = backfill.build_month_table(
        [
            record(date(2026, 1, 20), {"fanKwh": Decimal("7")}),
            record(date(2026, 1, 3), {"fanKwh": Decimal("3")}),
        ]
    )
    assert table.schema == model.DAILY_SCHEMA
    assert table.column("ts_utc").to_pylist() == sorted(table.column("ts_utc").to_pylist())


def test_a_non_day_grain_metric_can_never_reach_the_daily_dataset() -> None:
    """The dataset guard, exercised through this stage's own builder."""
    bogus = [
        model.make_observation(
            ts_utc=timeutil.local_midnight_utc(date(2026, 1, 3)),
            source=model.SOURCE_BRYANT,
            device_id=SERIAL,
            channel_id="hpheat",
            metric="watts",
            value=1.0,
        )
    ]
    with pytest.raises(ValueError, match="non-day-grain"):
        backfill.build_month_table([], bogus)


# ======================================================================
# run() — end to end over moto S3 + DynamoDB
# ======================================================================


def january_key() -> str:
    return s3io.daily_key(date(2026, 1, 1))


def test_run_writes_the_deterministic_monthly_key(s3, dynamo, legacy_dir, status) -> None:
    summary = backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )

    assert summary["keys"] == ["energy/daily/year=2026/bryant-202601.parquet"]
    assert s3io.key_exists(BUCKET, january_key(), client=s3)
    assert summary["months"] == 1
    # 2026-01-02 (16) + 2026-01-03 (16) + 2026-01-05 (2 present attributes)
    assert summary["rows"] == 34


def test_run_prefers_dynamodb_over_the_legacy_json_end_to_end(
    s3, dynamo, legacy_dir, status
) -> None:
    backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    table = read_daily(s3, january_key())
    observations = model.table_to_observations(table, dataset=model.Dataset.DAILY)

    jan3 = {
        (o.channel_id, o.metric): o.value
        for o in observations
        if o.ts_local == datetime(2026, 1, 3, 0, 0)
    }
    # The DynamoDB item says 95 kWh for the heat pump; the JSON says 21.
    assert jan3[("hpheat", "kwh_day")] == 95.0
    assert jan3[("hpheat", "cost_day_usd")] == 9.95
    # The date only DynamoDB has still lands.
    assert any(o.ts_local == datetime(2026, 1, 5, 0, 0) for o in observations)


def test_a_double_run_is_byte_identical(s3, dynamo, legacy_dir, status) -> None:
    """§15.5's idempotency clause, asserted on the actual object bytes."""
    kwargs = dict(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    first = backfill.run(**kwargs)
    first_bytes = object_bytes(s3, january_key())

    second = backfill.run(**kwargs)
    second_bytes = object_bytes(s3, january_key())

    assert first_bytes == second_bytes
    assert first["rows"] == second["rows"]
    assert s3io.parquet_row_count(BUCKET, january_key(), client=s3) == first["rows"]


def test_recorded_zeros_survive_a_full_run(s3, dynamo, legacy_dir, status) -> None:
    backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    observations = model.table_to_observations(
        read_daily(s3, january_key()), dataset=model.Dataset.DAILY
    )
    jan2 = {
        (o.channel_id, o.metric): o.value
        for o in observations
        if o.ts_local == datetime(2026, 1, 2, 0, 0)
    }
    assert jan2[("cooling", "kwh_day")] == 0.0
    assert jan2[("gas", "kwh_day")] == 0.0
    assert jan2[("reheat", "cost_day_usd")] == 0.0
    assert len(jan2) == 16  # every component present, zeros included


def test_the_range_bounds_which_days_are_imported(s3, dynamo, legacy_dir, status) -> None:
    summary = backfill.run(
        start=date(2026, 1, 3),
        end=date(2026, 1, 3),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    assert summary["days"] == 1
    observations = model.table_to_observations(
        read_daily(s3, january_key()), dataset=model.Dataset.DAILY
    )
    assert {o.ts_local.date() for o in observations} == {date(2026, 1, 3)}


def test_a_narrow_rerun_does_not_delete_the_rest_of_the_month(
    s3, dynamo, legacy_dir, status
) -> None:
    """Regenerating the file must not drop days outside --start/--end."""
    common = dict(
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    backfill.run(start=date(2026, 1, 1), end=date(2026, 1, 31), **common)
    full = read_daily(s3, january_key()).num_rows

    backfill.run(start=date(2026, 1, 3), end=date(2026, 1, 3), **common)
    after = model.table_to_observations(
        read_daily(s3, january_key()), dataset=model.Dataset.DAILY
    )
    assert len(after) == full
    assert {o.ts_local.date() for o in after} == {
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 5),
    }


def test_each_touched_month_gets_its_own_file(s3, dynamo, legacy_dir, status) -> None:
    summary = backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    assert summary["keys"] == [
        "energy/daily/year=2026/bryant-202601.parquet",
        "energy/daily/year=2026/bryant-202602.parquet",
        "energy/daily/year=2026/bryant-202603.parquet",
        "energy/daily/year=2026/bryant-202611.parquet",
    ]
    # The DST days landed, from whichever store had them.
    march = model.table_to_observations(
        read_daily(s3, s3io.daily_key(date(2026, 3, 1))), dataset=model.Dataset.DAILY
    )
    assert {o.ts_utc for o in march if o.ts_local.date() == date(2026, 3, 8)} == {
        datetime(2026, 3, 8, 5, 0, tzinfo=timeutil.UTC)
    }
    november = model.table_to_observations(
        read_daily(s3, s3io.daily_key(date(2026, 11, 1))), dataset=model.Dataset.DAILY
    )
    assert {o.ts_utc for o in november} == {
        datetime(2026, 11, 1, 4, 0, tzinfo=timeutil.UTC)
    }


def test_run_writes_nothing_outside_energy_daily(s3, dynamo, legacy_dir, status) -> None:
    """Day-grain rows must never appear under ``raw_30s`` (CLAUDE.md rule 6)."""
    backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    keys = s3io.list_keys(BUCKET, "", client=s3)
    assert keys, "the run wrote nothing at all"
    assert all(key.startswith(s3io.DAILY_PREFIX + "/") for key in keys), keys
    assert not s3io.list_keys(BUCKET, s3io.RAW_30S_PREFIX, client=s3)
    # No staging objects left stranded either.
    assert not s3io.list_keys(BUCKET, s3io.TMP_PREFIX, client=s3)


def test_a_range_with_no_legacy_data_writes_nothing(s3, dynamo, legacy_dir, status, log_stream) -> None:
    summary = backfill.run(
        start=date(2025, 5, 1),
        end=date(2025, 5, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    assert summary["records"] == 0
    assert summary["months"] == 0
    assert s3io.list_keys(BUCKET, s3io.DAILY_PREFIX, client=s3) == []
    assert log_events(log_stream, "backfill_no_records")


def test_dry_run_computes_but_writes_nothing(s3, dynamo, legacy_dir, status) -> None:
    summary = backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
        dry_run=True,
    )
    assert summary["rows"] == 34
    assert summary["dry_run"] is True
    assert s3io.list_keys(BUCKET, s3io.DAILY_PREFIX, client=s3) == []


def test_run_can_be_limited_to_one_origin(s3, dynamo, legacy_dir, status) -> None:
    summary = backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
        origins=(backfill.ORIGIN_LEGACY_JSON,),
    )
    assert summary["records_dynamodb"] == 0
    assert summary["rows_from_legacy_json"] == 32
    observations = model.table_to_observations(
        read_daily(s3, january_key()), dataset=model.Dataset.DAILY
    )
    jan3 = {
        (o.channel_id, o.metric): o.value
        for o in observations
        if o.ts_local == datetime(2026, 1, 3, 0, 0)
    }
    assert jan3[("hpheat", "kwh_day")] == 21.0  # the JSON's value, unopposed


def test_run_records_its_outcome_in_status_json(s3, dynamo, legacy_dir, status) -> None:
    backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    section = status.section(backfill.STATUS_SECTION)
    assert section["last_success_utc"]
    assert section["rows"] == 34
    assert section["months"] == 1
    assert section["last_range"] == "2026-01-01..2026-01-31"


def test_a_failing_month_is_reported_and_does_not_strand_the_others(
    s3, dynamo, legacy_dir, status
) -> None:
    """A corrupt existing object fails its month only, and the run exits non-zero."""
    s3.put_object(
        Bucket=BUCKET, Key=s3io.daily_key(date(2026, 3, 1)), Body=b"not parquet at all"
    )
    with pytest.raises(backfill.BackfillError, match="2026-03"):
        backfill.run(
            start=date(2026, 1, 1),
            end=date(2026, 12, 31),
            bucket=BUCKET,
            client=s3,
            dynamodb_client=dynamo,
            table=TABLE,
            legacy_path=legacy_dir,
            serial=SERIAL,
            store=status,
        )
    # January and November were still written.
    assert s3io.key_exists(BUCKET, january_key(), client=s3)
    assert s3io.key_exists(BUCKET, s3io.daily_key(date(2026, 11, 1)), client=s3)
    assert status.section(backfill.STATUS_SECTION)["consecutive_failures"] == 1


def test_an_inverted_range_is_a_caller_error(dynamo) -> None:
    with pytest.raises(ValueError, match="before start"):
        backfill.collect_records(
            start=date(2026, 2, 1),
            end=date(2026, 1, 1),
            origins=(),
        )


def test_the_written_object_is_readable_parquet_with_the_canonical_columns(
    s3, dynamo, legacy_dir, status
) -> None:
    backfill.run(
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        bucket=BUCKET,
        client=s3,
        dynamodb_client=dynamo,
        table=TABLE,
        legacy_path=legacy_dir,
        serial=SERIAL,
        store=status,
    )
    raw = object_bytes(s3, january_key())
    table = pa_pq.read_table(io.BytesIO(raw))
    assert table.column_names == list(model.CANONICAL_COLUMNS)
    assert set(table.column("metric").to_pylist()) <= model.DAY_GRAIN_METRICS
    assert set(table.column("source").to_pylist()) == {model.SOURCE_BRYANT}
    assert set(table.column("unit").to_pylist()) == {"kWh", "USD"}
