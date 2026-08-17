"""Tests for ``energy_capture.aws.glue`` — the Glue tables and their comments.

Three things are being defended here, in rising order of how quietly they fail:

1. **Idempotency.** ``energycap create-glue-tables`` is meant to be safe to run
   any number of times. Running it twice must leave byte-identical definitions
   and, the second time, issue no writes at all.
2. **Agreement with the data.** A Glue column type that disagrees with the
   Parquet the pipeline actually writes does not raise — Athena returns NULLs.
   So the type mapping is checked against real Parquet files these tests write
   through :func:`energy_capture.aws.s3io.write_table_atomic`, and the locations
   are checked against the ``s3io`` key builders rather than against re-typed
   strings.
3. **The comments.** PLAN.md §12 and CLAUDE.md both call them a first-class
   deliverable: they are what an LLM reads before writing a query. So every
   table and every column must carry a real, non-placeholder comment; each table
   comment must state the grain, the LOCAL-date partitioning, the dedupe key and
   that gaps mean collector downtime; ``energy_hourly`` must carry §12's blunt
   ``sample_count`` warning; and the enum decode in the ``value`` comment must
   match ``sources/bryant.py``'s append-only tables integer for integer, so a
   future enum addition cannot leave the catalog lying.

**Why there is a fake Glue client here.** ``moto`` is installed as ``moto[s3]``
(``pyproject.toml``), and moto's Glue backend needs ``pyparsing``, which that
extra does not pull in. Adding a dependency is out of scope for this change, so
the stateful tests drive :class:`FakeGlue` — a small, faithful stand-in that
mimics the four Glue calls this module makes, including the fields the real
service adds server-side. :func:`test_moto_glue_backend_behaves_the_same` runs
the same flow against real moto and **skips** when ``pyparsing`` is absent, so
the day ``moto[glue]`` lands the moto path is exercised automatically.
"""

from __future__ import annotations

import copy
import importlib
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from energy_capture import model, timeutil
from energy_capture.aws import glue, s3io
from energy_capture.sources import bryant
from energy_capture.sources.leviton import (
    breaker_channel_id,
    ct_channel_id,
    panel_leg_channel_id,
)
from energy_capture.stages.daily import COMPONENTS as DAILY_COMPONENTS
from tests.conftest import BUCKET, utc

DATABASE = "energy_test"

ALL_TABLES = (
    glue.TABLE_ENERGY_RAW_30S,
    glue.TABLE_ENERGY_HOURLY,
    glue.TABLE_ENERGY_DAILY,
    glue.TABLE_DIM_CHANNEL,
)

#: Text that betrays an unwritten comment. A comment is only useful if somebody
#: actually wrote it; "TODO" in a table an LLM is reading is worse than nothing.
PLACEHOLDER_MARKERS = (
    "todo",
    "tbd",
    "fixme",
    "xxx",
    "placeholder",
    "lorem",
    "coming soon",
    "fill in",
    "describe me",
    "???",
    "n/a",
)


# ===========================================================================
# A faithful stand-in for the Glue service
# ===========================================================================


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": f"{code} raised by FakeGlue"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        operation,
    )


class FakeGlue:
    """In-process Glue catalog covering exactly the calls ``glue.py`` makes.

    Deliberately unforgiving in the same places the real service is: an unknown
    database or table raises ``EntityNotFoundException``, and creating something
    twice raises ``AlreadyExistsException``. It also *adds* the fields real Glue
    adds server-side (``CreateTime``, a ``transient_lastDdlTime`` parameter,
    ``StorageDescriptor`` defaults), because a comparison that only worked on
    definitions we ourselves stored would prove nothing about production.
    """

    def __init__(self) -> None:
        self.databases: dict[str, dict[str, Any]] = {}
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        #: Every mutating or reading call, in order: ``(operation, name)``.
        self.calls: list[tuple[str, str]] = []

    # -- reads -------------------------------------------------------------
    def get_database(self, *, Name: str) -> dict[str, Any]:  # noqa: N803 - boto3 casing
        self.calls.append(("get_database", Name))
        if Name not in self.databases:
            raise _client_error("EntityNotFoundException", "GetDatabase")
        return {"Database": copy.deepcopy(self.databases[Name])}

    def get_table(self, *, DatabaseName: str, Name: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("get_table", Name))
        stored = self.tables.get((DatabaseName, Name))
        if stored is None:
            raise _client_error("EntityNotFoundException", "GetTable")
        return {"Table": self._as_service_would_return(DatabaseName, stored)}

    def get_tables(self, *, DatabaseName: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append(("get_tables", DatabaseName))
        return {
            "TableList": [
                self._as_service_would_return(DatabaseName, stored)
                for (db, _), stored in sorted(self.tables.items())
                if db == DatabaseName
            ]
        }

    # -- writes ------------------------------------------------------------
    def create_database(self, *, DatabaseInput: dict[str, Any]) -> dict[str, Any]:  # noqa: N803
        name = DatabaseInput["Name"]
        self.calls.append(("create_database", name))
        if name in self.databases:
            raise _client_error("AlreadyExistsException", "CreateDatabase")
        self.databases[name] = copy.deepcopy(DatabaseInput)
        return {}

    def create_table(self, *, DatabaseName: str, TableInput: dict[str, Any]) -> dict[str, Any]:  # noqa: N803
        name = TableInput["Name"]
        self.calls.append(("create_table", name))
        if DatabaseName not in self.databases:
            raise _client_error("EntityNotFoundException", "CreateTable")
        if (DatabaseName, name) in self.tables:
            raise _client_error("AlreadyExistsException", "CreateTable")
        self.tables[(DatabaseName, name)] = copy.deepcopy(TableInput)
        return {}

    def update_table(self, *, DatabaseName: str, TableInput: dict[str, Any]) -> dict[str, Any]:  # noqa: N803
        name = TableInput["Name"]
        self.calls.append(("update_table", name))
        if (DatabaseName, name) not in self.tables:
            raise _client_error("EntityNotFoundException", "UpdateTable")
        self.tables[(DatabaseName, name)] = copy.deepcopy(TableInput)
        return {}

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _as_service_would_return(database: str, stored: dict[str, Any]) -> dict[str, Any]:
        table = copy.deepcopy(stored)
        table["DatabaseName"] = database
        table["CatalogId"] = "123456789012"
        table["CreateTime"] = datetime(2026, 8, 16, 12, 0, tzinfo=timeutil.UTC)
        table["UpdateTime"] = datetime(2026, 8, 16, 12, 0, tzinfo=timeutil.UTC)
        table["CreatedBy"] = "arn:aws:sts::123456789012:assumed-role/energycap"
        table["IsRegisteredWithLakeFormation"] = False
        # Athena stamps this on every table it touches; `glue.py` must ignore it
        # or every run would look like a change.
        table["Parameters"] = {
            **table.get("Parameters", {}),
            "transient_lastDdlTime": "1755350000",
        }
        descriptor = table.setdefault("StorageDescriptor", {})
        descriptor.setdefault("NumberOfBuckets", -1)
        descriptor.setdefault("BucketColumns", [])
        descriptor.setdefault("SortColumns", [])
        descriptor.setdefault("Parameters", {})
        return table

    def definitions(self, database: str = DATABASE) -> dict[str, dict[str, Any]]:
        """What is actually stored, i.e. exactly the ``TableInput``s we sent."""
        return {
            name: copy.deepcopy(stored)
            for (db, name), stored in self.tables.items()
            if db == database
        }

    def operations(self, *names: str) -> list[tuple[str, str]]:
        return [call for call in self.calls if call[0] in names]


@pytest.fixture
def fake_glue() -> FakeGlue:
    return FakeGlue()


def run(client: FakeGlue, **kwargs: Any) -> dict[str, Any]:
    """``create_or_update_tables`` against the fake, with the test defaults."""
    kwargs.setdefault("database", DATABASE)
    kwargs.setdefault("bucket", BUCKET)
    return glue.create_or_update_tables(client=client, **kwargs)


# ===========================================================================
# Create / update / idempotency
# ===========================================================================


def test_a_first_run_creates_the_database_and_the_four_tables(fake_glue: FakeGlue) -> None:
    summary = run(fake_glue)

    assert summary["created"] == 4
    assert summary["updated"] == 0
    assert summary["unchanged"] == 0
    assert summary["database_created"] is True
    assert summary["database"] == DATABASE
    assert set(fake_glue.definitions()) == set(ALL_TABLES)
    assert fake_glue.databases[DATABASE]["Description"] == glue.DATABASE_DESCRIPTION


def test_an_existing_database_is_used_and_never_modified(fake_glue: FakeGlue) -> None:
    fake_glue.databases[DATABASE] = {"Name": DATABASE, "Description": "hand-made"}

    summary = run(fake_glue)

    assert summary["database_created"] is False
    assert fake_glue.databases[DATABASE]["Description"] == "hand-made"
    assert fake_glue.operations("create_database") == []
    assert summary["created"] == 4


def test_a_second_run_changes_nothing_and_issues_no_writes(fake_glue: FakeGlue) -> None:
    """The whole point of "idempotent create-or-update"."""
    run(fake_glue)
    first = fake_glue.definitions()

    fake_glue.calls.clear()
    summary = run(fake_glue)

    assert fake_glue.definitions() == first, "a re-run rewrote the definitions"
    assert summary["created"] == 0
    assert summary["updated"] == 0
    assert summary["unchanged"] == 4
    assert fake_glue.operations("create_table", "update_table", "create_database") == []


def test_a_third_run_is_still_a_no_op(fake_glue: FakeGlue) -> None:
    run(fake_glue)
    run(fake_glue)
    first = fake_glue.definitions()
    summary = run(fake_glue)
    assert fake_glue.definitions() == first
    assert summary["unchanged"] == 4


def test_an_existing_table_is_updated_in_place_never_duplicated(fake_glue: FakeGlue) -> None:
    """A table left over from an older layout is corrected, not added beside."""
    fake_glue.databases[DATABASE] = {"Name": DATABASE}
    stale = glue.table_input(glue.table_specs()[0], BUCKET)
    stale["StorageDescriptor"]["Location"] = "s3://old-bucket/energy/raw30s/"
    stale["Description"] = "an older, less useful description"
    del stale["Parameters"]["projection.day.range"]
    fake_glue.create_table(DatabaseName=DATABASE, TableInput=stale)
    fake_glue.calls.clear()

    summary = run(fake_glue)

    assert summary["updated"] == 1
    assert summary["updated_tables"] == [glue.TABLE_ENERGY_RAW_30S]
    assert summary["created"] == 3
    assert len(fake_glue.get_tables(DatabaseName=DATABASE)["TableList"]) == 4
    fixed = fake_glue.definitions()[glue.TABLE_ENERGY_RAW_30S]
    assert fixed["StorageDescriptor"]["Location"] == f"s3://{BUCKET}/energy/raw_30s/"
    assert fixed["Parameters"]["projection.day.range"] == glue.PROJECTION_DAY_RANGE
    assert fixed["Description"] == glue.table_input(glue.table_specs()[0], BUCKET)["Description"]
    assert fake_glue.operations("create_table") == [
        ("create_table", glue.TABLE_ENERGY_HOURLY),
        ("create_table", glue.TABLE_ENERGY_DAILY),
        ("create_table", glue.TABLE_DIM_CHANNEL),
    ]


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda t: t["StorageDescriptor"]["Columns"].pop(), id="dropped-column"),
        pytest.param(
            lambda t: t["StorageDescriptor"]["Columns"][0].update(Comment=""),
            id="blanked-comment",
        ),
        pytest.param(
            lambda t: t["StorageDescriptor"]["Columns"][0].update(Type="string"),
            id="wrong-type",
        ),
        pytest.param(lambda t: t["PartitionKeys"].pop(), id="dropped-partition-key"),
        pytest.param(
            lambda t: t["Parameters"].update({"projection.enabled": "false"}),
            id="projection-disabled",
        ),
        pytest.param(
            lambda t: t["StorageDescriptor"].update(InputFormat="org.apache.TextInput"),
            id="wrong-input-format",
        ),
        pytest.param(lambda t: t.update(Description="stale"), id="stale-description"),
    ],
)
def test_every_kind_of_drift_is_detected_and_repaired(
    fake_glue: FakeGlue, mutate: Any
) -> None:
    spec = glue.table_specs()[0]
    desired = glue.table_input(spec, BUCKET)
    drifted = copy.deepcopy(desired)
    mutate(drifted)
    fake_glue.databases[DATABASE] = {"Name": DATABASE}
    fake_glue.create_table(DatabaseName=DATABASE, TableInput=drifted)

    summary = run(fake_glue)

    assert spec.name in summary["updated_tables"]
    assert fake_glue.definitions()[spec.name] == desired


def test_a_parameter_athena_adds_on_its_own_does_not_trigger_an_update(
    fake_glue: FakeGlue,
) -> None:
    """Athena stamps ``transient_lastDdlTime``; that must not cause a rewrite."""
    run(fake_glue)
    for stored in fake_glue.tables.values():
        stored["Parameters"]["transient_lastDdlTime"] = "1700000000"
        stored["Parameters"]["UPDATED_BY_CRAWLER"] = "someone-elses-crawler"
    fake_glue.calls.clear()

    summary = run(fake_glue)

    assert summary["unchanged"] == 4
    assert fake_glue.operations("update_table") == []


def test_dry_run_reads_but_never_writes(fake_glue: FakeGlue) -> None:
    summary = run(fake_glue, dry_run=True)

    assert summary["dry_run"] is True
    assert summary["created"] == 4
    assert fake_glue.databases == {}
    assert fake_glue.tables == {}
    assert fake_glue.operations("create_database", "create_table", "update_table") == []


def test_dry_run_against_an_existing_catalog_reports_no_change(fake_glue: FakeGlue) -> None:
    run(fake_glue)
    summary = run(fake_glue, dry_run=True)
    assert summary["unchanged"] == 4
    assert summary["created"] == 0


def test_the_database_defaults_to_the_glue_database_setting(fake_glue: FakeGlue) -> None:
    summary = glue.create_or_update_tables(client=fake_glue, bucket=BUCKET)
    assert summary["database"] == DATABASE  # GLUE_DATABASE in tests/conftest.py


def test_the_bucket_defaults_to_the_s3_bucket_setting(fake_glue: FakeGlue) -> None:
    summary = glue.create_or_update_tables(client=fake_glue, database=DATABASE)
    assert summary["bucket"] == BUCKET
    location = fake_glue.definitions()[glue.TABLE_ENERGY_RAW_30S]["StorageDescriptor"][
        "Location"
    ]
    assert location == f"s3://{BUCKET}/energy/raw_30s/"


def test_a_lost_race_on_the_database_is_not_a_failure(fake_glue: FakeGlue) -> None:
    """Two runners at once: whoever loses ``create_database`` still proceeds."""
    real_create = fake_glue.create_database

    def racing_create(*, DatabaseInput: dict[str, Any]) -> dict[str, Any]:  # noqa: N803
        real_create(DatabaseInput=DatabaseInput)  # the "other" runner got there
        raise _client_error("AlreadyExistsException", "CreateDatabase")

    fake_glue.create_database = racing_create  # type: ignore[method-assign]
    summary = run(fake_glue)
    assert summary["created"] == 4
    assert summary["database_created"] is False


def test_an_unexpected_client_error_is_not_swallowed(fake_glue: FakeGlue) -> None:
    def denied(*, Name: str) -> dict[str, Any]:  # noqa: N803
        raise _client_error("AccessDeniedException", "GetDatabase")

    fake_glue.get_database = denied  # type: ignore[method-assign]
    with pytest.raises(ClientError):
        run(fake_glue)


def test_the_summary_is_loggable(fake_glue: FakeGlue) -> None:
    """``cli._run_stage`` folds the return value into its ``stage_ok`` line."""
    summary = run(fake_glue)
    assert set(summary) >= {
        "database",
        "bucket",
        "tables",
        "created",
        "updated",
        "unchanged",
        "dry_run",
    }
    for value in summary.values():
        assert isinstance(value, (str, int, bool, list))


# ===========================================================================
# Partition projection — pinned literally
# ===========================================================================


def _parameters(name: str) -> dict[str, str]:
    spec = next(s for s in glue.table_specs() if s.name == name)
    return glue.table_input(spec, BUCKET)["Parameters"]


def test_raw_30s_projection_properties_are_exactly_as_specified() -> None:
    assert _parameters(glue.TABLE_ENERGY_RAW_30S) == {
        "EXTERNAL": "TRUE",
        "classification": "parquet",
        "projection.enabled": "true",
        "projection.year.type": "integer",
        "projection.year.range": "2024,2035",
        "projection.month.type": "integer",
        "projection.month.range": "1,12",
        "projection.month.digits": "2",
        "projection.day.type": "integer",
        "projection.day.range": "1,31",
        "projection.day.digits": "2",
        "storage.location.template": (
            f"s3://{BUCKET}/energy/raw_30s/year=${{year}}/month=${{month}}/day=${{day}}"
        ),
    }


def test_hourly_projection_properties_are_exactly_as_specified() -> None:
    assert _parameters(glue.TABLE_ENERGY_HOURLY) == {
        "EXTERNAL": "TRUE",
        "classification": "parquet",
        "projection.enabled": "true",
        "projection.year.type": "integer",
        "projection.year.range": "2024,2035",
        "projection.month.type": "integer",
        "projection.month.range": "1,12",
        "projection.month.digits": "2",
        "storage.location.template": (
            f"s3://{BUCKET}/energy/hourly/year=${{year}}/month=${{month}}"
        ),
    }


def test_daily_is_projected_on_year_only() -> None:
    assert _parameters(glue.TABLE_ENERGY_DAILY) == {
        "EXTERNAL": "TRUE",
        "classification": "parquet",
        "projection.enabled": "true",
        "projection.year.type": "integer",
        "projection.year.range": "2024,2035",
        "storage.location.template": f"s3://{BUCKET}/energy/daily/year=${{year}}",
    }


def test_dim_channel_is_not_partitioned_and_has_no_projection() -> None:
    parameters = _parameters(glue.TABLE_DIM_CHANNEL)
    assert parameters == {"EXTERNAL": "TRUE", "classification": "parquet"}
    assert not any(key.startswith("projection.") for key in parameters)
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_DIM_CHANNEL)
    assert spec.partition_keys == ()
    assert glue.table_input(spec, BUCKET)["PartitionKeys"] == []


def test_every_partitioned_table_enables_projection_and_has_a_template() -> None:
    for spec in glue.table_specs():
        parameters = glue.table_input(spec, BUCKET)["Parameters"]
        if not spec.partition_keys:
            continue
        assert parameters["projection.enabled"] == "true"
        assert "storage.location.template" in parameters
        for key in spec.partition_keys:
            assert parameters[f"projection.{key}.type"] == "integer"


def test_partition_columns_are_integers_and_carry_comments() -> None:
    for spec in glue.table_specs():
        for column in glue.table_input(spec, BUCKET)["PartitionKeys"]:
            assert column["Type"] == glue.PARTITION_COLUMN_TYPE == "int"
            assert column["Comment"].strip()


def test_a_partition_key_is_never_also_a_data_column() -> None:
    """Glue rejects the duplicate, and Hive path partitions are not in the file."""
    for spec in glue.table_specs():
        rendered = glue.table_input(spec, BUCKET)
        data = {c["Name"] for c in rendered["StorageDescriptor"]["Columns"]}
        partitions = {c["Name"] for c in rendered["PartitionKeys"]}
        assert data.isdisjoint(partitions)
        assert partitions.isdisjoint(set(spec.schema.names))


# ===========================================================================
# Locations come from the s3io builders, not from re-typed strings
# ===========================================================================


def test_locations_match_the_s3io_prefixes() -> None:
    expected = {
        glue.TABLE_ENERGY_RAW_30S: s3io.RAW_30S_PREFIX,
        glue.TABLE_ENERGY_HOURLY: s3io.HOURLY_PREFIX,
        glue.TABLE_ENERGY_DAILY: s3io.DAILY_PREFIX,
        glue.TABLE_DIM_CHANNEL: s3io.DIM_CHANNEL_PREFIX,
    }
    for spec in glue.table_specs():
        assert spec.location(BUCKET) == s3io.s3_uri(BUCKET, expected[spec.name] + "/")


def test_locations_match_the_layout_pinned_in_plan_section_4() -> None:
    locations = {
        spec.name: spec.location(BUCKET) for spec in glue.table_specs()
    }
    assert locations == {
        glue.TABLE_ENERGY_RAW_30S: f"s3://{BUCKET}/energy/raw_30s/",
        glue.TABLE_ENERGY_HOURLY: f"s3://{BUCKET}/energy/hourly/",
        glue.TABLE_ENERGY_DAILY: f"s3://{BUCKET}/energy/daily/",
        glue.TABLE_DIM_CHANNEL: f"s3://{BUCKET}/energy/dim_channel/",
    }


@pytest.mark.parametrize(
    ("table", "builder", "local_day"),
    [
        (glue.TABLE_ENERGY_RAW_30S, s3io.raw_30s_day_prefix, date(2026, 8, 5)),
        (glue.TABLE_ENERGY_HOURLY, s3io.hourly_month_prefix, date(2026, 8, 5)),
        (glue.TABLE_ENERGY_DAILY, s3io.daily_year_prefix, date(2026, 8, 5)),
    ],
)
def test_a_real_partition_prefix_matches_the_projection_template(
    table: str, builder: Any, local_day: date
) -> None:
    """Substituting a real local date into the template reproduces the real key.

    This is the check that would catch a template that looks plausible but does
    not address the objects the pipeline actually writes.
    """
    spec = next(s for s in glue.table_specs() if s.name == table)
    template = spec.location_template(BUCKET)
    year, month, day = timeutil.partition_parts_for_local_date(local_day)
    rendered = (
        template.replace("${year}", year).replace("${month}", month).replace("${day}", day)
    )
    assert rendered + "/" == s3io.s3_uri(BUCKET, builder(local_day))
    assert "${" not in rendered


def test_a_real_written_object_lives_under_its_table_location(s3: Any) -> None:
    """Write a part the way the uploader does, then check the table can see it."""
    local_day = date(2026, 8, 16)
    key = s3io.raw_30s_part_key(local_day, 14)
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_RAW_30S)
    s3io.write_table_atomic(model.empty_table(), BUCKET, key, client=s3)

    assert s3io.s3_uri(BUCKET, key).startswith(spec.location(BUCKET))


def test_the_archive_prefix_is_deliberately_not_a_table() -> None:
    """Parts and the day file must never both be visible to a query (§10)."""
    tabled = [spec.location(BUCKET) for spec in glue.table_specs()]
    archive = s3io.s3_uri(BUCKET, s3io.ARCHIVE_PREFIX + "/")
    assert archive not in tabled
    assert not any(archive.startswith(location) for location in tabled)


def test_the_staging_prefix_is_outside_every_table_location() -> None:
    """A half-written temp object must be invisible to Athena."""
    staging = s3io.s3_uri(BUCKET, s3io.TMP_PREFIX + "/")
    for spec in glue.table_specs():
        assert not staging.startswith(spec.location(BUCKET))


def test_a_spec_whose_partition_keys_disagree_with_the_layout_raises() -> None:
    spec = glue.TableSpec(
        name="mislabelled",
        schema=model.RAW_30S_SCHEMA,
        prefix_builder=s3io.raw_30s_day_prefix,  # year/month/day
        partition_keys=("year",),
        description="x",
    )
    with pytest.raises(ValueError, match="partitions on"):
        spec.location_template(BUCKET)


def test_a_spec_cannot_declare_partition_keys_out_of_order() -> None:
    with pytest.raises(ValueError, match="outermost-first"):
        glue.TableSpec(
            name="backwards",
            schema=model.RAW_30S_SCHEMA,
            prefix_builder=s3io.raw_30s_day_prefix,
            partition_keys=("day", "year"),
            description="x",
        )


def test_a_spec_cannot_shadow_a_partition_key_with_a_data_column() -> None:
    with pytest.raises(ValueError, match="must not"):
        glue.TableSpec(
            name="shadowed",
            schema=pa.schema([pa.field("year", pa.int32())]),
            prefix_builder=s3io.daily_year_prefix,
            partition_keys=("year",),
            description="x",
        )


# ===========================================================================
# Column types agree with the Parquet the writers actually produce
# ===========================================================================


def _declared_types(table: str) -> dict[str, str]:
    spec = next(s for s in glue.table_specs() if s.name == table)
    return {c["Name"]: c["Type"] for c in spec.columns()}


def _round_tripped_schema(table: pa.Table, bucket: str, key: str, client: Any) -> pa.Schema:
    """Write a real ZSTD Parquet object through ``s3io`` and read its schema back."""
    sort_key: tuple[str, ...] | None = None
    if not set(model.SORT_KEY) <= set(table.column_names) and not set(
        model.HOURLY_SORT_KEY
    ) <= set(table.column_names):
        sort_key = ()
    s3io.write_table_atomic(table, bucket, key, client=client, sort_key=sort_key)
    data = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_schema(pa.BufferReader(data))


def _hourly_table() -> pa.Table:
    hour = utc(2026, 8, 16, 18)
    row = {
        "hour_start_utc": hour,
        "local_hour_start": timeutil.to_local_naive(hour),
        "source": model.SOURCE_LEVITON,
        "device_id": "hub-a",
        "channel_id": "breaker_p11",
        "metric": "watts",
        "unit": "W",
        "mean": 101.5,
        "min": 90.0,
        "max": 120.0,
        "p95": 118.0,
        "sample_count": 118,
        "first_ts_utc": hour,
        "last_ts_utc": utc(2026, 8, 16, 18, 59, 30),
        "kwh": 0.0998,
    }
    return pa.Table.from_pylist([row], schema=model.HOURLY_SCHEMA)


def _dim_table() -> pa.Table:
    row = {
        "source": model.SOURCE_LEVITON,
        "device_id": "hub-a",
        "channel_id": "breaker_p11",
        "label": "Dryer outlet",
        "short_label": "Dryer",
        "panel": "A",
        "slots": "1,3",
        "category": "240v-appliance",
        "room": "Mud Room",
        "priority": "critical",
        "estimated_watts": 5000.0,
        "blackstart_device_id": "A-1-3",
        "updated_at": utc(2026, 8, 16, 12),
    }
    return pa.Table.from_pylist([row], schema=glue.DIM_CHANNEL_SCHEMA)


def test_raw_30s_column_types_match_a_real_parquet_file(s3: Any, make_obs: Any) -> None:
    table = model.observations_to_table(
        [
            make_obs(metric="watts", value=1200.0),
            make_obs(metric="volts", value=241.3, channel_id="panel_leg_a"),
            make_obs(
                source=model.SOURCE_BRYANT,
                device_id="TEST0000001",
                channel_id="system",
                metric="mode",
                value=1.0,
            ),
        ]
    )
    schema = _round_tripped_schema(table, BUCKET, "probe/raw_30s.parquet", s3)

    declared = _declared_types(glue.TABLE_ENERGY_RAW_30S)
    assert {f.name: glue.arrow_to_glue_type(f.type) for f in schema} == declared
    assert declared == {
        "ts_utc": "timestamp",
        "ts_local": "timestamp",
        "source": "string",
        "device_id": "string",
        "channel_id": "string",
        "metric": "string",
        "value": "double",
        "unit": "string",
    }


def test_hourly_column_types_match_a_real_parquet_file(s3: Any) -> None:
    schema = _round_tripped_schema(_hourly_table(), BUCKET, "probe/hourly.parquet", s3)

    declared = _declared_types(glue.TABLE_ENERGY_HOURLY)
    assert {f.name: glue.arrow_to_glue_type(f.type) for f in schema} == declared
    assert declared["sample_count"] == "bigint"
    assert declared["kwh"] == "double"
    assert declared["hour_start_utc"] == declared["local_hour_start"] == "timestamp"


def test_daily_column_types_match_a_real_parquet_file(s3: Any, day_grain_obs: Any) -> None:
    table = model.observations_to_table(
        [
            day_grain_obs(date(2026, 8, 15), channel_id="hpheat", metric="kwh_day"),
            day_grain_obs(
                date(2026, 8, 15), channel_id="hpheat", metric="cost_day_usd", value=1.4
            ),
        ],
        dataset=model.Dataset.DAILY,
    )
    schema = _round_tripped_schema(table, BUCKET, "probe/daily.parquet", s3)
    assert {f.name: glue.arrow_to_glue_type(f.type) for f in schema} == _declared_types(
        glue.TABLE_ENERGY_DAILY
    )


def test_dim_channel_column_types_match_a_real_parquet_file(s3: Any) -> None:
    schema = _round_tripped_schema(_dim_table(), BUCKET, "probe/dim.parquet", s3)

    declared = _declared_types(glue.TABLE_DIM_CHANNEL)
    assert {f.name: glue.arrow_to_glue_type(f.type) for f in schema} == declared
    # PLAN.md §9: these two are the ones a reader will guess wrong.
    assert declared["slots"] == "string"
    assert declared["priority"] == "string"
    assert declared["estimated_watts"] == "double"


def test_dim_channel_columns_are_exactly_plan_section_9s_list() -> None:
    assert list(_declared_types(glue.TABLE_DIM_CHANNEL)) == [
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
        "updated_at",
    ]


def test_the_dim_channel_table_describes_what_build_dim_actually_writes(s3: Any) -> None:
    """The Glue table is declared against the *writer*, not against a copy of it.

    ``glue.DIM_CHANNEL_SCHEMA`` is ``dim.DIM_SCHEMA`` — one declaration, so the
    two cannot drift on paper. What that aliasing does **not** prove is that
    ``build-dim``'s row-building path produces those types: a Python ``int``
    where the column is ``double``, or a naive datetime where it is
    ``timestamp[us, tz=UTC]``, would still reach S3 and read back in Athena as
    NULL. So this goes the whole way — real rows through the real
    ``dim.build_table``, written as a real Parquet object, schema read back off
    the bytes and compared to what the table tells Athena to expect.
    """
    from energy_capture.stages import dim

    rows = [
        dim.DimRow(
            source=model.SOURCE_LEVITON,
            device_id="hub-a",
            channel_id="breaker_p11",
            label="Dryer outlet",
            short_label="Dryer",
            panel="A",
            slots="1,3",
            category="appliance_240v",
            room="Mud Room",
            priority="critical",
            estimated_watts=5000,  # an int, exactly as blackstart records it
            blackstart_device_id="A-1-3",
            updated_at=utc(2026, 8, 16, 12),
        ),
        # The all-nulls case: a channel with no inventory entry still gets a row.
        dim.DimRow(
            source=model.SOURCE_BRYANT,
            device_id="TEST0000001",
            channel_id="system",
            label="Infinity system",
            short_label="System",
            panel=None,
            slots=None,
            category=None,
            room=None,
            priority=None,
            estimated_watts=None,
            blackstart_device_id=None,
            updated_at=utc(2026, 8, 16, 12),
        ),
    ]
    table = dim.build_table(rows)
    schema = _round_tripped_schema(table, BUCKET, s3io.dim_channel_key(), s3)

    declared = _declared_types(glue.TABLE_DIM_CHANNEL)
    assert {f.name: glue.arrow_to_glue_type(f.type) for f in schema} == declared
    # The int that has to become a double, and the tz-aware stamp.
    assert declared["estimated_watts"] == "double"
    assert declared["updated_at"] == "timestamp"


def test_the_declared_columns_are_the_model_schemas_in_order() -> None:
    for table, schema in (
        (glue.TABLE_ENERGY_RAW_30S, model.RAW_30S_SCHEMA),
        (glue.TABLE_ENERGY_HOURLY, model.HOURLY_SCHEMA),
        (glue.TABLE_ENERGY_DAILY, model.DAILY_SCHEMA),
    ):
        assert list(_declared_types(table)) == list(schema.names)


def test_arrow_to_glue_type_refuses_to_guess() -> None:
    with pytest.raises(ValueError, match="no Glue type mapping"):
        glue.arrow_to_glue_type(pa.list_(pa.int32()))


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.timestamp("us", tz="UTC"), "timestamp"),
        (pa.timestamp("us"), "timestamp"),
        (pa.string(), "string"),
        (pa.float64(), "double"),
        (pa.int64(), "bigint"),
        (pa.int32(), "int"),
        (pa.bool_(), "boolean"),
    ],
)
def test_the_type_mapping_is_pinned(arrow_type: pa.DataType, expected: str) -> None:
    assert glue.arrow_to_glue_type(arrow_type) == expected


# ===========================================================================
# The comments — the first-class deliverable
# ===========================================================================


def _all_comments() -> list[tuple[str, str, str]]:
    """``(table, column, comment)`` for every column of every table."""
    out: list[tuple[str, str, str]] = []
    for spec in glue.table_specs():
        rendered = glue.table_input(spec, BUCKET)
        for column in rendered["PartitionKeys"] + rendered["StorageDescriptor"]["Columns"]:
            out.append((spec.name, column["Name"], column["Comment"]))
    return out


def test_every_table_has_a_substantial_comment() -> None:
    for spec in glue.table_specs():
        description = glue.table_input(spec, BUCKET)["Description"]
        assert description.strip(), f"{spec.name} has no table comment"
        assert len(description) > 300, f"{spec.name}'s comment is too thin to orient anyone"
        assert len(description) <= glue.GLUE_DESCRIPTION_MAX_LEN


def test_every_column_of_every_table_has_a_real_comment() -> None:
    comments = _all_comments()
    assert comments, "no columns were rendered at all"
    for table, column, comment in comments:
        assert comment.strip(), f"{table}.{column} has an empty comment"
        assert len(comment) >= 40, f"{table}.{column}'s comment is a stub: {comment!r}"
        assert len(comment) <= glue.GLUE_COMMENT_MAX_LEN


def test_no_comment_anywhere_contains_placeholder_text() -> None:
    texts = [glue.DATABASE_DESCRIPTION]
    texts += [glue.table_input(spec, BUCKET)["Description"] for spec in glue.table_specs()]
    texts += [comment for _, _, comment in _all_comments()]
    for text in texts:
        lowered = text.lower()
        for marker in PLACEHOLDER_MARKERS:
            assert marker not in lowered, f"placeholder {marker!r} in: {text!r}"


def test_no_two_columns_of_a_table_share_a_comment() -> None:
    """Two columns sharing a comment usually means one was copy-pasted."""
    for spec in glue.table_specs():
        rendered = glue.table_input(spec, BUCKET)
        columns = rendered["PartitionKeys"] + rendered["StorageDescriptor"]["Columns"]
        comments = [c["Comment"] for c in columns]
        assert len(set(comments)) == len(comments), f"duplicate comment in {spec.name}"


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_table_comment_states_the_grain(table: str) -> None:
    spec = next(s for s in glue.table_specs() if s.name == table)
    assert "GRAIN:" in spec.description


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_table_comment_says_gaps_mean_downtime_not_zero_load(table: str) -> None:
    """CLAUDE.md rule 1, restated wherever a query author will meet it."""
    spec = next(s for s in glue.table_specs() if s.name == table)
    text = spec.description.lower()
    if table == glue.TABLE_DIM_CHANNEL:
        # Not time series: its version of the rule is "a missing mapping must
        # never drop a measurement".
        assert "left join" in text and "coverage is not guaranteed" in text.lower()
        return
    assert "gaps mean collector downtime, never zero load" in text
    assert "interpolated" in text and "zero-filled" in text


@pytest.mark.parametrize(
    "table",
    [glue.TABLE_ENERGY_RAW_30S, glue.TABLE_ENERGY_HOURLY, glue.TABLE_ENERGY_DAILY],
)
def test_every_time_series_table_comment_states_local_date_partitioning(table: str) -> None:
    spec = next(s for s in glue.table_specs() if s.name == table)
    assert "PARTITIONED ON LOCAL DATE" in spec.description
    assert timeutil.tz_name() in spec.description
    assert "America/Kentucky/Louisville" in spec.description
    assert "not UTC" in spec.description


@pytest.mark.parametrize(
    ("table", "expected_key"),
    [
        (glue.TABLE_ENERGY_RAW_30S, "(ts_utc, source, device_id, channel_id, metric)"),
        (glue.TABLE_ENERGY_DAILY, "(ts_utc, source, device_id, channel_id, metric)"),
        (
            glue.TABLE_ENERGY_HOURLY,
            "(hour_start_utc, source, device_id, channel_id, metric)",
        ),
    ],
)
def test_every_table_comment_states_its_dedupe_key(table: str, expected_key: str) -> None:
    spec = next(s for s in glue.table_specs() if s.name == table)
    assert "Dedupe key:" in spec.description
    assert expected_key in spec.description


def test_the_dedupe_key_in_the_comments_is_the_one_the_code_uses() -> None:
    canonical = "(" + ", ".join(model.DEDUPE_KEY) + ")"
    hourly = "(" + ", ".join(model.HOURLY_DEDUPE_KEY) + ")"
    specs = {spec.name: spec for spec in glue.table_specs()}
    assert canonical in specs[glue.TABLE_ENERGY_RAW_30S].description
    assert canonical in specs[glue.TABLE_ENERGY_DAILY].description
    assert hourly in specs[glue.TABLE_ENERGY_HOURLY].description


def test_the_hourly_comment_carries_plan_section_12s_warning_verbatim() -> None:
    """PLAN.md §12 dictates this sentence; it must survive intact."""
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_HOURLY)
    warning = (
        "sample_count < ~118 (watts@30s) means the hour has gaps; an absent row "
        "means the collector was down — do NOT read absence or low kwh as the "
        "load being off."
    )
    assert warning == glue.HOURLY_GAP_WARNING
    assert warning in spec.description


def test_the_hourly_comment_states_the_observed_time_kwh_formula() -> None:
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_HOURLY)
    assert "mean * sample_count * poll_interval_s / 3.6e6" in spec.description
    assert "NULL (not 0)" in spec.description


def test_the_kwh_column_comment_states_the_formula_and_the_null_rule() -> None:
    comment = glue._HOURLY_COLUMN_COMMENTS["kwh"]
    assert "mean * sample_count * poll_interval_s / 3.6e6" in comment
    assert "OBSERVED TIME ONLY" in comment
    assert "NULL" in comment and "never 0" in comment
    # The schema had better agree that it is the one nullable column.
    assert model.HOURLY_SCHEMA.field("kwh").nullable
    assert not model.HOURLY_SCHEMA.field("mean").nullable


def test_the_sample_count_comment_distinguishes_off_from_down() -> None:
    comment = glue._HOURLY_COLUMN_COMMENTS["sample_count"]
    assert "the load was off" in comment
    assert "the collector was down" in comment
    assert "~120" in comment and "~118" in comment


def test_the_ts_utc_comment_says_it_is_canonical_and_owns_bucketing() -> None:
    comment = glue.CANONICAL_COLUMN_COMMENTS["ts_utc"]
    assert "CANONICAL INSTANT" in comment
    assert "dedupe" in comment and "bucketing" in comment
    assert "never ts_local" in comment


def test_the_ts_local_comment_says_naive_ambiguous_and_partition_source() -> None:
    comment = glue.CANONICAL_COLUMN_COMMENTS["ts_local"]
    assert "WALL CLOCK" in comment
    assert timeutil.tz_name() in comment
    assert "AMBIGUOUS" in comment and "fall-back" in comment
    assert "partition" in comment
    assert "Never sort, bucket or dedupe on it" in comment


def test_the_hourly_bucket_comments_explain_the_25_hour_day() -> None:
    assert "25 buckets" in glue._HOURLY_COLUMN_COMMENTS["hour_start_utc"]
    assert "23" in glue._HOURLY_COLUMN_COMMENTS["hour_start_utc"]
    assert "AMBIGUOUS" in glue._HOURLY_COLUMN_COMMENTS["local_hour_start"]


@pytest.mark.parametrize("source_column", ["ts_local", "local_hour_start"])
def test_the_partition_column_comments_warn_that_the_column_is_an_integer(
    source_column: str,
) -> None:
    """``month=08`` in the path but ``WHERE month = 8`` in the query.

    Checked for both derivations, since the comments are now generated per table
    from the local timestamp column that table actually carries.
    """
    comments = glue._partition_column_comments(source_column)
    for key in ("month", "day"):
        assert "THE COLUMN IS AN INTEGER" in comments[key]
        assert "digits=2" in comments[key]
    for key in ("year", "month", "day"):
        assert "LOCAL" in comments[key]
        assert source_column in comments[key]


def test_every_rendered_partition_comment_still_says_local_and_integer() -> None:
    """The same guarantee, read off what the catalog is actually told."""
    for table in ALL_TABLES:
        for key, comment in _partition_comments(table).items():
            assert "LOCAL" in comment
            if key in ("month", "day"):
                assert "THE COLUMN IS AN INTEGER" in comment


# ------------------------------------------------------- the enum decode


def _parse_decode(comment: str) -> dict[str, dict[str, int]]:
    """Read the ``mode 0=off, 1=heat; stage …`` decode back out of a comment."""
    _, _, body = comment.partition("Enum decode:")
    assert body, f"no enum decode found in {comment!r}"
    parsed: dict[str, dict[str, int]] = {}
    for section in body.strip().rstrip(".").split(";"):
        metric, _, pairs = section.strip().partition(" ")
        table: dict[str, int] = {}
        for pair in pairs.split(","):
            code, _, name = pair.strip().partition("=")
            table[name.strip()] = int(code)
        parsed[metric] = table
    return parsed


def test_the_value_comment_decodes_the_enums_integer_for_integer() -> None:
    """The comment and ``sources/bryant.py`` must never disagree.

    The decode is *generated* from ``bryant.ENUM_TABLES``, so an appended enum
    value propagates automatically; this test is what stops somebody replacing
    the generated string with a hand-typed copy that then rots.
    """
    parsed = _parse_decode(glue.CANONICAL_COLUMN_COMMENTS["value"])
    assert parsed == {metric: dict(table) for metric, table in bryant.ENUM_TABLES.items()}
    assert parsed["mode"] == dict(bryant.MODE_CODES)
    assert parsed["stage"] == dict(bryant.STAGE_CODES)
    assert parsed["fan"] == dict(bryant.FAN_CODES)


def test_the_rendered_raw_30s_value_column_carries_the_full_decode() -> None:
    columns = {
        c["Name"]: c["Comment"]
        for c in glue.table_input(glue.table_specs()[0], BUCKET)["StorageDescriptor"][
            "Columns"
        ]
    }
    parsed = _parse_decode(columns["value"])
    assert parsed == {metric: dict(table) for metric, table in bryant.ENUM_TABLES.items()}
    for metric in ("mode", "stage", "fan"):
        assert bryant.enum_decode_text(metric) in columns["value"]


def test_every_enum_metric_the_model_knows_about_is_decoded() -> None:
    parsed = _parse_decode(glue.CANONICAL_COLUMN_COMMENTS["value"])
    assert set(parsed) == set(model.ENUM_METRICS)


def test_an_appended_enum_value_reaches_the_comment_without_anyone_editing_it() -> None:
    """Proves the decode is derived, not copied.

    Extends the ``mode`` table, reloads the module, and expects the new pair to
    appear. If somebody ever replaces the generated decode with a literal, this
    fails — which is exactly the "a future enum addition that forgets the
    comment fails the suite" guarantee PLAN.md §7.3 asks for.
    """
    original_tables = bryant.ENUM_TABLES
    original_modes = bryant.MODE_CODES
    extended = dict(original_modes)
    extended["vacation"] = max(extended.values()) + 1
    try:
        bryant.MODE_CODES = extended  # type: ignore[misc]
        bryant.ENUM_TABLES = {**dict(original_tables), "mode": extended}  # type: ignore[misc]
        reloaded = importlib.reload(glue)
        comment = reloaded.CANONICAL_COLUMN_COMMENTS["value"]
        assert f"{extended['vacation']}=vacation" in comment
        assert len(comment) <= reloaded.GLUE_COMMENT_MAX_LEN, (
            "the value comment has no headroom left for a new enum value; "
            "shorten its prose, never drop a decode entry"
        )
    finally:
        bryant.MODE_CODES = original_modes  # type: ignore[misc]
        bryant.ENUM_TABLES = original_tables  # type: ignore[misc]
        importlib.reload(glue)

    # And the restored module is back to the real tables.
    assert _parse_decode(glue.CANONICAL_COLUMN_COMMENTS["value"])["mode"] == dict(
        bryant.MODE_CODES
    )


def test_the_unit_comment_points_at_the_enum_decode() -> None:
    comment = glue.CANONICAL_COLUMN_COMMENTS["unit"]
    assert "'enum'" in comment
    assert "decode" in comment
    for unit in ("W", "A", "V", "Hz", "degF", "kWh", "USD", "pct"):
        assert re.search(rf"\b{re.escape(unit)}\b", comment), f"{unit} missing from unit comment"


# ------------------------------------------------- the channel_id conventions


def test_the_channel_id_comment_matches_the_functions_that_mint_the_ids() -> None:
    """The comment claims a naming convention; these build the real thing."""
    comment = glue.CANONICAL_COLUMN_COMMENTS["channel_id"]

    assert "breaker_p{position}" in comment
    assert comment.replace("{position}", "11").count(breaker_channel_id(11)) == 1

    assert "ct_{channel}_{a,b}" in comment
    assert ct_channel_id(1, "a") == "ct_1_a"
    assert ct_channel_id(1, "b") == "ct_1_b"

    assert "panel_leg_{a,b}" in comment
    assert panel_leg_channel_id("a") == "panel_leg_a"
    assert panel_leg_channel_id("b") == "panel_leg_b"

    assert "zone_{n}" in comment
    assert bryant.zone_channel_id(1) == "zone_1"
    assert bryant.SYSTEM_CHANNEL in comment


def test_the_channel_id_comment_lists_every_bryant_daily_component() -> None:
    comment = glue.CANONICAL_COLUMN_COMMENTS["channel_id"]
    for spec in DAILY_COMPONENTS:
        assert spec.channel_id in comment, f"{spec.channel_id} missing from channel_id comment"
    assert len(DAILY_COMPONENTS) == 8


def test_the_daily_channel_id_comment_lists_exactly_the_eight_components() -> None:
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_DAILY)
    comment = spec.comment_for("channel_id")
    for component in DAILY_COMPONENTS:
        assert component.channel_id in comment
    assert "camelCase" in comment  # the hPHeat/fanGas/loopPump trap


def test_the_channel_id_comment_says_a_two_pole_breaker_is_one_channel() -> None:
    assert "2-pole = ONE channel" in glue.CANONICAL_COLUMN_COMMENTS["channel_id"]


def test_the_metric_comment_lists_the_day_grain_metrics_as_daily_only() -> None:
    comment = glue.CANONICAL_COLUMN_COMMENTS["metric"]
    for metric in sorted(model.DAY_GRAIN_METRICS):
        assert metric in comment
    assert "energy_daily only" in comment


def test_the_daily_table_says_its_rows_never_enter_the_other_tables() -> None:
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_DAILY)
    assert "never appear in energy_raw_30s" in spec.description
    assert "excluded from" in spec.description and "energy_hourly" in spec.description
    assert "LOCAL MIDNIGHT" in spec.description


def test_the_raw_table_explains_that_parts_and_the_day_file_never_coexist() -> None:
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_ENERGY_RAW_30S)
    assert "raw_30s_parts_archive" in spec.description
    assert "never both" in spec.description
    assert "DISTINCT" in spec.description


def test_the_dim_table_tells_a_reader_to_left_join() -> None:
    spec = next(s for s in glue.table_specs() if s.name == glue.TABLE_DIM_CHANNEL)
    assert "LEFT JOIN" in spec.description
    assert "(source, device_id, channel_id)" in spec.description
    assert "build-dim" in spec.description


def test_the_estimated_watts_comment_refuses_to_be_mistaken_for_a_measurement() -> None:
    comment = glue._DIM_COLUMN_COMMENTS["estimated_watts"]
    assert "NEVER A MEASUREMENT" in comment
    assert "energy_hourly" in comment


def test_a_comment_longer_than_the_glue_limit_is_rejected_not_truncated() -> None:
    """Truncating "do NOT read absence as off" into "do" would be a disaster."""
    with pytest.raises(ValueError, match="the Glue API allows"):
        glue._fit("x" * 300, glue.GLUE_COMMENT_MAX_LEN, "a test comment")


def test_a_column_with_no_comment_is_a_hard_error() -> None:
    spec = glue.TableSpec(
        name="uncommented",
        schema=pa.schema([pa.field("mystery", pa.string())]),
        prefix_builder=lambda _d: "energy/mystery/",
        partition_keys=(),
        description="x",
    )
    with pytest.raises(ValueError, match="first-class deliverable"):
        spec.columns()


def test_the_database_description_orients_a_reader_too() -> None:
    assert "dim_channel" in glue.DATABASE_DESCRIPTION
    assert timeutil.tz_name() in glue.DATABASE_DESCRIPTION
    assert "GAPS MEAN COLLECTOR DOWNTIME" in glue.DATABASE_DESCRIPTION


def test_no_credential_can_reach_a_table_definition() -> None:
    """Comments are hand-written prose; make sure nobody pasted a secret in."""
    blob = repr(glue.table_inputs(BUCKET)) + glue.DATABASE_DESCRIPTION
    for forbidden in ("password", "not-a-real-", "authorization", "Bearer ", "secret"):
        assert forbidden.lower() not in blob.lower()


# ===========================================================================
# A comment that is WRONG is worse than one that is missing
# ===========================================================================
#
# The four groups below all defend the same property from different sides: a
# comment must be true *of the table it is attached to*. A shared string that is
# right for energy_raw_30s and wrong for energy_daily does not read as broken —
# an LLM simply believes it, writes `WHERE year = 2026 AND month = 8` against a
# year-partitioned table, and gets COLUMN_NOT_FOUND.


def _spec(table: str) -> glue.TableSpec:
    return next(s for s in glue.table_specs() if s.name == table)


def _rendered(table: str) -> dict[str, Any]:
    return glue.table_input(_spec(table), BUCKET)


def _column_comments(table: str) -> dict[str, str]:
    """``{column: comment}`` for the data columns of one table."""
    return {
        c["Name"]: c["Comment"] for c in _rendered(table)["StorageDescriptor"]["Columns"]
    }


def _partition_comments(table: str) -> dict[str, str]:
    return {c["Name"]: c["Comment"] for c in _rendered(table)["PartitionKeys"]}


def _all_texts(table: str) -> dict[str, str]:
    """Every string the catalog carries for one table, keyed by where it lives."""
    texts = {"<table comment>": _rendered(table)["Description"]}
    texts.update({f"partition {k}": v for k, v in _partition_comments(table).items()})
    texts.update({f"column {k}": v for k, v in _column_comments(table).items()})
    return texts


# -------------------------------------------- 1. partition prose is per table

#: "there is no month or day column" is a *denial* that a partition column
#: exists, so the keys inside it are stripped before claims are read out.
_DENIAL = re.compile(
    r"\bno\s+(?:year|month|day)(?:\s+or\s+(?:year|month|day))*\s+(?:partition\s+)?column",
    re.IGNORECASE,
)

#: The shapes in which a comment tells a reader "this table has that partition
#: column": a partition list, a prune/partitioned-on phrase, a WHERE, or a
#: ``key=value`` path segment.
_CLAIM_CONTEXTS = (
    re.compile(r"(?:year|month|day)(?:/(?:year|month|day))*\s+partitions?\b"),
    re.compile(r"(?:[Pp]rune|[Pp]artitioned)\s+on\s+(?:year|month|day)(?:/(?:year|month|day))*"),
    re.compile(r"WHERE\s+(?:year|month|day)\b"),
    re.compile(r"\b(?:year|month|day)\s*=\s*"),
)
_KEY_WORD = re.compile(r"\b(year|month|day)\b")


def _partition_columns_claimed(text: str) -> set[str]:
    """Partition columns a comment asserts the table has."""
    asserted = _DENIAL.sub(" ", text)
    claimed: set[str] = set()
    for pattern in _CLAIM_CONTEXTS:
        for span in pattern.findall(asserted):
            claimed.update(_KEY_WORD.findall(span))
    return claimed


def test_the_claim_extractor_reads_a_partition_list_out_of_prose() -> None:
    """The detector the next test depends on — pinned so it cannot go blind."""
    assert _partition_columns_claimed(
        "Prune with the year/month/day partitions first"
    ) == {"year", "month", "day"}
    assert _partition_columns_claimed("Source of the year partition value") == {"year"}
    assert _partition_columns_claimed("write WHERE month = 8, not month = '08'") == {"month"}
    assert _partition_columns_claimed("zero-padded (day=05) by projection") == {"day"}
    # A denial is not a claim, and ordinary prose about local days is not either.
    assert _partition_columns_claimed("this table has no month or day column") == set()
    assert _partition_columns_claimed(
        "A local day normally holds part-*.parquet or day-*.parquet"
    ) == set()


@pytest.mark.parametrize("table", ALL_TABLES)
def test_no_comment_claims_a_partition_column_its_table_does_not_have(table: str) -> None:
    """DEFECT: energy_daily is partitioned by YEAR ONLY.

    Its ts_utc comment used to say "prune with the year/month/day partitions"
    and its ts_local comment called it "the source of the year/month/day
    partition values", both inherited verbatim from the day-partitioned table.
    A reader who followed either wrote a query that cannot run.
    """
    real = set(_spec(table).partition_keys)
    for where, text in _all_texts(table).items():
        claimed = _partition_columns_claimed(text)
        assert claimed <= real, (
            f"{table} {where} tells a reader to use {sorted(claimed - real)}, "
            f"but this table is partitioned on {sorted(real) or 'nothing'}: "
            f"{text!r}"
        )


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_partition_column_it_does_have_is_named_where_it_matters(table: str) -> None:
    """The other half: a real partition column must be pruneable from the prose."""
    real = set(_spec(table).partition_keys)
    claimed: set[str] = set()
    for text in _all_texts(table).values():
        claimed |= _partition_columns_claimed(text)
    assert real <= claimed, f"{table} never tells anyone to prune on {sorted(real - claimed)}"


@pytest.mark.parametrize("table", ALL_TABLES)
def test_every_partition_comment_names_a_source_column_the_table_actually_has(
    table: str,
) -> None:
    """DEFECT: energy_hourly has no ts_local at all.

    Its year/month partition comments described values "taken from ts_local",
    sending a reader after a column that is not in the table. The partition
    values there come from local_hour_start.
    """
    columns = set(_column_comments(table))
    candidates = ("ts_local", "ts_utc", "local_hour_start", "hour_start_utc")
    for key, comment in _partition_comments(table).items():
        cited = {name for name in candidates if name in comment}
        assert cited, f"{table}.{key} does not say which column its values come from"
        assert cited <= columns, (
            f"{table}.{key} sources its values from {sorted(cited - columns)}, "
            f"which is not a column of {table} ({sorted(columns)})"
        )


@pytest.mark.parametrize("table", ALL_TABLES)
def test_no_column_comment_points_at_a_column_the_table_does_not_have(table: str) -> None:
    """energy_hourly's unit comment used to send readers to "the value column"."""
    columns = set(_column_comments(table))
    for name, comment in _column_comments(table).items():
        for phrase, needed in (("value column", "value"), ("ts_local", "ts_local")):
            if phrase in comment:
                assert needed in columns, (
                    f"{table}.{name} refers to {phrase!r}, but {table} has no "
                    f"{needed} column"
                )


def test_a_partitioned_table_with_no_local_timestamp_column_is_refused() -> None:
    """The partition comments cannot say where the values come from, so: loud."""
    spec = glue.TableSpec(
        name="sourceless",
        schema=pa.schema([pa.field("ts_utc", pa.timestamp("us", tz="UTC"))]),
        prefix_builder=s3io.daily_year_prefix,
        partition_keys=("year",),
        description="x",
    )
    with pytest.raises(ValueError, match="none of"):
        spec.partition_columns()


# ------------------------------ 2. energy_hourly aggregates enum rows, and says so


def test_the_rollup_really_does_carry_enum_rows_into_energy_hourly() -> None:
    """Grounds the warning: rollup.sql excludes ONLY the day-grain metrics.

    If this ever stops being true the hourly enum prose becomes the wrong kind
    of comment — one that warns about rows that are not there.
    """
    from energy_capture.stages import rollup

    sql = (Path(rollup.__file__).with_name("rollup.sql")).read_text()
    assert "rollup_excluded_metrics" in sql
    for metric in sorted(model.ENUM_METRICS):
        assert not re.search(rf"\b{metric}\b", sql), (
            f"rollup.sql now names {metric}; if enum metrics are excluded from "
            "the rollup, energy_hourly's enum warning has to change with it"
        )
    assert model.ENUM_METRICS.isdisjoint(model.DAY_GRAIN_METRICS)


def test_the_hourly_table_comment_warns_that_enum_aggregates_are_meaningless() -> None:
    """DEFECT: mean/p95 over mode/stage/fan are arithmetic on a label."""
    description = _spec(glue.TABLE_ENERGY_HOURLY).description
    assert "MEANINGLESS" in description
    for metric in sorted(model.ENUM_METRICS):
        assert metric in description
    assert "mean and p95" in description
    assert "min, max and sample_count carry meaning" in description
    assert glue.ENUM_ROLLUP_WARNING in description


def test_the_hourly_table_comment_carries_the_enum_decode_integer_for_integer() -> None:
    """DEFECT: energy_hourly has no value column, so the decode had no home."""
    description = _spec(glue.TABLE_ENERGY_HOURLY).description
    head, marker, tail = description.partition("Enum decode:")
    assert marker, "energy_hourly's comment carries no enum decode"
    parsed = _parse_decode(f"Enum decode: {tail.split('. ')[0]}")
    assert parsed == {metric: dict(table) for metric, table in bryant.ENUM_TABLES.items()}
    for metric in ("mode", "stage", "fan"):
        assert bryant.enum_decode_text(metric) in description


def test_an_appended_enum_value_reaches_the_hourly_table_comment_too() -> None:
    """Proves the hourly decode is generated, not a hand-typed second copy."""
    original_tables = bryant.ENUM_TABLES
    original_modes = bryant.MODE_CODES
    extended = dict(original_modes)
    extended["vacation"] = max(extended.values()) + 1
    try:
        bryant.MODE_CODES = extended  # type: ignore[misc]
        bryant.ENUM_TABLES = {**dict(original_tables), "mode": extended}  # type: ignore[misc]
        reloaded = importlib.reload(glue)
        hourly = next(
            s for s in reloaded.table_specs() if s.name == reloaded.TABLE_ENERGY_HOURLY
        )
        assert f"{extended['vacation']}=vacation" in hourly.description
        assert (
            len(reloaded.table_input(hourly, BUCKET)["Description"])
            <= reloaded.GLUE_DESCRIPTION_MAX_LEN
        )
    finally:
        bryant.MODE_CODES = original_modes  # type: ignore[misc]
        bryant.ENUM_TABLES = original_tables  # type: ignore[misc]
        importlib.reload(glue)


def test_the_hourly_aggregate_comments_say_which_ones_mean_anything_for_enums() -> None:
    comments = _column_comments(glue.TABLE_ENERGY_HOURLY)
    for column in ("mean", "p95"):
        assert "MEANINGLESS" in comments[column], column
        assert "enum" in comments[column], column
    for column in ("min", "max"):
        assert "IS meaningful" in comments[column], column
        assert "enum" in comments[column], column
    # And the unit column, which is where a reader meets 'enum' in the first
    # place, has to point at the decode that is actually reachable from here.
    assert "'enum'" in comments["unit"]
    assert "this table's comment" in comments["unit"]


# ------------------------------- 3. the metric and unit vocabularies are closed


def test_the_metric_groups_cover_exactly_the_metrics_the_model_defines() -> None:
    """DEFECT: blower_rpm and cfm were real, emitted, and missing from the list.

    The comment reads as a closed vocabulary — somebody writes
    `WHERE metric IN (...)` from it — so an omission silently drops rows.
    """
    grouped: list[str] = [m for g in glue._METRIC_GROUPS for m in sorted(g.metrics)]
    assert sorted(grouped) == sorted(model.METRICS)
    assert len(grouped) == len(set(grouped)), "a metric is in two groups"
    for metric in ("blower_rpm", "cfm"):
        assert metric in model.METRICS
        assert metric in glue._metrics_for_table(glue.TABLE_ENERGY_RAW_30S)


def test_a_metric_added_to_the_model_fails_the_build_instead_of_the_comment() -> None:
    """The whole defect class: prose quietly falling behind ``model.py``."""
    original_units = model.UNIT_FOR_METRIC
    original_metrics = model.METRICS
    try:
        model.UNIT_FOR_METRIC = {**original_units, "duct_pressure": model.UNIT_PCT}  # type: ignore[misc]
        model.METRICS = frozenset(model.UNIT_FOR_METRIC)  # type: ignore[misc]
        with pytest.raises(ValueError, match="duct_pressure"):
            importlib.reload(glue)
    finally:
        model.UNIT_FOR_METRIC = original_units  # type: ignore[misc]
        model.METRICS = original_metrics  # type: ignore[misc]
        importlib.reload(glue)
    assert "duct_pressure" not in glue.CANONICAL_COLUMN_COMMENTS["metric"]


_TABLES_WITH_METRICS = (
    glue.TABLE_ENERGY_RAW_30S,
    glue.TABLE_ENERGY_HOURLY,
    glue.TABLE_ENERGY_DAILY,
)


@pytest.mark.parametrize("table", _TABLES_WITH_METRICS)
def test_every_metric_a_table_can_hold_is_named_in_its_metric_comment(table: str) -> None:
    comment = _column_comments(table)["metric"]
    for metric in glue._metrics_for_table(table):
        assert re.search(rf"\b{re.escape(metric)}\b", comment), (
            f"{table}.metric never mentions {metric}, which really lands there — "
            "a reader filtering on this list drops those rows"
        )


@pytest.mark.parametrize("table", _TABLES_WITH_METRICS)
def test_a_metric_comment_names_no_metric_that_cannot_appear_there(table: str) -> None:
    """The day-grain pair is the one deliberate cross-reference, and it is labelled."""
    allowed = set(glue._metrics_for_table(table)) | set(model.DAY_GRAIN_METRICS)
    comment = _column_comments(table)["metric"]
    for metric in sorted(model.METRICS - allowed):
        assert not re.search(rf"\b{re.escape(metric)}\b", comment), (
            f"{table}.metric offers {metric}, which never appears in {table}"
        )


@pytest.mark.parametrize("table", _TABLES_WITH_METRICS)
def test_every_unit_a_table_can_hold_is_named_in_its_unit_comment(table: str) -> None:
    """DEFECT: 'W, A, V, Hz, degF, kWh, USD, pct, or enum' omitted rpm and CFM."""
    comment = _column_comments(table)["unit"]
    for metric in glue._metrics_for_table(table):
        unit = model.unit_for_metric(metric)
        assert re.search(rf"\b{re.escape(unit)}\b", comment), (
            f"{table}.unit never mentions {unit!r} (the unit of {metric})"
        )


@pytest.mark.parametrize("table", _TABLES_WITH_METRICS)
def test_a_unit_comment_names_no_unit_that_cannot_appear_there(table: str) -> None:
    allowed = {
        model.unit_for_metric(metric)
        for metric in set(glue._metrics_for_table(table)) | set(model.DAY_GRAIN_METRICS)
    }
    comment = _column_comments(table)["unit"]
    for unit in sorted(model.UNITS - allowed):
        assert not re.search(rf"\b{re.escape(unit)}\b", comment), (
            f"{table}.unit offers {unit!r}, which no metric in {table} uses"
        )


def test_the_unit_lists_are_generated_from_the_models_metric_table() -> None:
    """Not a re-typed string: the units come from ``model.unit_for_metric``."""
    assert glue._unit_list_text(("watts", "amps", "blower_rpm", "cfm", "mode")) == (
        "W, A, rpm, CFM, 'enum'"
    )
    assert glue._unit_list_text(("watts", "volts", "watts")) == "W, V"


# ---------------------- 4. an invariant that only holds after a successful run


def test_the_raw_table_states_the_one_window_where_parts_and_day_file_coexist() -> None:
    """DEFECT: "never both ... no de-duplication is needed" was an absolute.

    The compactor writes day-{D}.parquet first and archives the parts second, so
    a crash between the two leaves both in the partition — the state the
    compactor's own failure path refuses to leave silently.
    """
    description = _spec(glue.TABLE_ENERGY_RAW_30S).description
    # Still the normal case, stated as such: not weakened into a coin flip.
    assert "normally holds EITHER" in description
    assert "never both" in description
    assert "THE ONE EXCEPTION" in description
    # What goes wrong, how to notice it, and what to do about it.
    assert "count twice" in description
    assert "part-*.parquet beside day-*.parquet" in description
    assert "energycap compact-daily --start D --end D" in description
    assert "dedupe that day" in description


def test_the_exception_named_is_the_one_the_compactor_itself_admits_to() -> None:
    """The comment's remedy has to be a real command, and the risk a real risk."""
    from energy_capture import cli
    from energy_capture.stages import compactor

    source = Path(compactor.__file__).read_text()
    assert "double-counting state" in source, (
        "the compactor no longer describes the double-counting window; if the "
        "ordering changed, energy_raw_30s's comment must change with it"
    )
    assert "compact-daily" in cli.STAGE_ENTRYPOINTS


# ------------------- 5. one field, two metrics, and only one of them ever lands
#
# `odu.opstat` is rendered as EITHER `stage` (an enum code, staged compressors)
# or `stage_pct` (a 0-100 capacity percentage, variable-capacity compressors),
# never both, and the choice belongs to the hardware. This house is
# variable-capacity, so `stage` matches zero rows for all time — a failure mode
# no vocabulary list can express, because both metrics are in the list. Everything
# below pins the prose that has to carry it, and pins it to the *source* of each
# fact (bryant's constants, model's units, the committed live capture) rather
# than to a re-typed copy.

#: The live capture, trimmed and committed by the source-side change. It is the
#: evidence behind the words "THIS unit is variable-capacity" in the catalog.
_VARCAP_FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "bryant" / "status_varcap.json"
)

_STAGE_TABLES = (glue.TABLE_ENERGY_RAW_30S, glue.TABLE_ENERGY_HOURLY)


def _observed_odu() -> dict[str, Any]:
    import json

    payload = json.loads(_VARCAP_FIXTURE.read_text())
    return payload["data"]["infinityStatus"]["odu"]


def test_the_catalog_names_the_outdoor_unit_the_live_capture_actually_showed() -> None:
    """``odu.type gs3ngiphp`` in a table comment is a claim about this house.

    It is only true because one real response said so, so it is checked against
    that response. Swap the fixture for a different system and this fails —
    which is exactly right: the comment would then be describing somebody else's
    hardware, and the metric it tells readers to expect would be the wrong one.
    """
    odu = _observed_odu()
    assert odu["type"] == glue.ODU_TYPE_OBSERVED
    assert bryant.stage_metric_for(odu["opstat"]) == bryant.STAGE_PCT_METRIC, (
        "the committed live capture no longer renders as stage_pct; the Glue "
        "comments claim this system emits stage_pct and never stage"
    )


def test_both_renderings_are_one_group_and_reach_the_same_tables() -> None:
    """They are alternatives for one field, so they cannot land in different places.

    A reader told stage_pct exists in raw_30s but stage only in hourly would
    conclude the two are different measurements. They are the same measurement.
    """
    groups = [
        group
        for group in glue._METRIC_GROUPS
        if group.metrics & {bryant.STAGE_METRIC, bryant.STAGE_PCT_METRIC}
    ]
    assert len(groups) == 1, "the two renderings are in different metric groups"
    assert {bryant.STAGE_METRIC, bryant.STAGE_PCT_METRIC} <= groups[0].metrics
    for table in _STAGE_TABLES:
        reachable = glue._metrics_for_table(table)
        assert bryant.STAGE_METRIC in reachable and bryant.STAGE_PCT_METRIC in reachable


def test_the_stage_note_is_built_from_the_source_of_truth_not_retyped() -> None:
    """Rename the metric in bryant/model and this sentence must follow it."""
    note = glue.STAGE_REPRESENTATION_NOTE
    assert bryant.STAGE_METRIC in note and bryant.STAGE_PCT_METRIC in note
    assert model.unit_for_metric(bryant.STAGE_PCT_METRIC) in note
    assert glue.ODU_TYPE_OBSERVED in note
    # ...and the group entry is the same string, not a lookalike literal.
    group = next(g for g in glue._METRIC_GROUPS if bryant.STAGE_PCT_METRIC in g.metrics)
    assert bryant.STAGE_PCT_METRIC in group.metrics


@pytest.mark.parametrize("table", _STAGE_TABLES)
def test_every_table_that_can_hold_either_stage_metric_explains_the_trap(
    table: str,
) -> None:
    """Both metrics named, the exclusivity stated, and which one THIS system emits.

    Naming them in the ``metric`` list is not enough: the list reads as "these
    can all appear", and one of these two never will.
    """
    description = _spec(table).description
    assert glue.STAGE_REPRESENTATION_NOTE in description
    assert "MUTUALLY EXCLUSIVE" in description
    assert "VARIABLE-CAPACITY" in description
    assert "absence, not zero" in description, (
        f"{table} names both stage metrics without saying that the missing one "
        "is absence rather than zero — CLAUDE.md rule 1"
    )
    for metric in (bryant.STAGE_METRIC, bryant.STAGE_PCT_METRIC):
        assert re.search(rf"\b{re.escape(metric)}\b", _column_comments(table)["metric"])


def test_the_database_description_carries_the_trap_too() -> None:
    """It is the first string an LLM reads, and it is where orientation happens."""
    assert glue.STAGE_REPRESENTATION_NOTE in glue.DATABASE_DESCRIPTION
    assert len(glue.DATABASE_DESCRIPTION) <= glue.GLUE_DESCRIPTION_MAX_LEN


def test_stage_pct_is_a_measurement_and_nothing_calls_it_an_enum() -> None:
    """It renders the same field as an enum metric, and is not one.

    ``unit='pct'``, not ``'enum'``; not in ``model.ENUM_METRICS``; not in the
    decode; and the hourly table says its mean is real, so the enum warning next
    to it cannot be read as covering it.
    """
    assert model.unit_for_metric(bryant.STAGE_PCT_METRIC) == model.UNIT_PCT
    assert bryant.STAGE_PCT_METRIC not in model.ENUM_METRICS
    assert bryant.STAGE_PCT_METRIC not in glue._ENUM_DECODE
    assert bryant.STAGE_PCT_METRIC not in glue.ENUM_ROLLUP_WARNING

    hourly = _spec(glue.TABLE_ENERGY_HOURLY).description
    assert glue.STAGE_MEAN_NOTE in hourly
    assert bryant.STAGE_PCT_METRIC in _column_comments(glue.TABLE_ENERGY_HOURLY)["mean"]


def test_the_enum_warnings_name_exactly_the_models_enum_metrics() -> None:
    """The "mean/p95 are meaningless" warning has to cover the right set.

    Generated from ``model.ENUM_METRICS``, so a metric that becomes (or stops
    being) an enum cannot leave the warning naming yesterday's set — and
    ``stage_pct``, which sits right beside them, is never swept in.
    """
    roster = glue._ENUM_METRIC_NAMES
    assert set(roster.split("/")) == set(model.ENUM_METRICS)
    assert bryant.STAGE_PCT_METRIC not in roster.split("/")
    for text in (
        glue.ENUM_ROLLUP_WARNING,
        _column_comments(glue.TABLE_ENERGY_HOURLY)["unit"],
        _column_comments(glue.TABLE_ENERGY_HOURLY)["mean"],
    ):
        assert roster in text, (
            "this warning lists the enum metrics by hand instead of from "
            f"model.ENUM_METRICS: {text!r}"
        )


# ===========================================================================
# PLAN.md §13: energy_meter must drop in, not be retrofitted
# ===========================================================================


def _meter_spec() -> glue.TableSpec:
    """What adding §13's table will look like: one TableSpec, nothing else."""
    return glue.TableSpec(
        name="energy_meter",
        schema=model.METER_SCHEMA,
        prefix_builder=s3io.meter_year_prefix,
        partition_keys=("year",),
        description=(
            "GRAIN: one row per metering interval. PARTITIONED ON LOCAL DATE "
            "(America/Kentucky/Louisville), not UTC. Dedupe key: (ts_utc, "
            "source, device_id, channel_id, metric). GAPS MEAN COLLECTOR "
            "DOWNTIME, NEVER ZERO LOAD."
        ),
    )


def test_the_future_meter_table_needs_no_new_machinery() -> None:
    spec = _meter_spec()
    rendered = glue.table_input(spec, BUCKET)

    assert rendered["StorageDescriptor"]["Location"] == f"s3://{BUCKET}/energy/meter/"
    assert rendered["Parameters"]["storage.location.template"] == (
        f"s3://{BUCKET}/energy/meter/year=${{year}}"
    )
    assert rendered["Parameters"]["projection.year.range"] == glue.PROJECTION_YEAR_RANGE
    columns = {c["Name"]: c for c in rendered["StorageDescriptor"]["Columns"]}
    assert columns["interval_s"]["Type"] == "int"
    assert "interval START" in columns["interval_s"]["Comment"]
    assert list(columns) == list(model.METER_SCHEMA.names)


def test_the_meter_table_is_not_created_yet() -> None:
    """PLAN.md §13 is design-only: nothing may create it before it is built."""
    assert "energy_meter" not in {spec.name for spec in glue.table_specs()}


# ===========================================================================
# The same flow against a real Glue backend
# ===========================================================================


def test_moto_glue_backend_behaves_the_same() -> None:
    """Runs the real boto3 client against moto's Glue backend.

    ``create-glue-tables`` is the least-covered link in the pipeline — it is the
    one stage with no offline stand-in for its actual service call — so the dev
    extra is ``moto[s3,glue]`` and this runs rather than skipping. The guard
    stays so a stripped install degrades to a skip instead of a collection error.
    """
    pytest.importorskip("pyparsing", reason="moto[glue] is not installed")
    import boto3
    from moto import mock_aws

    with mock_aws():
        client = boto3.client("glue", region_name="us-east-1")

        first = glue.create_or_update_tables(
            database=DATABASE, bucket=BUCKET, client=client
        )
        assert first["created"] == 4

        second = glue.create_or_update_tables(
            database=DATABASE, bucket=BUCKET, client=client
        )
        assert second["unchanged"] == 4
        assert second["created"] == 0
        assert second["updated"] == 0

        listed = client.get_tables(DatabaseName=DATABASE)["TableList"]
        assert {t["Name"] for t in listed} == set(ALL_TABLES)
        raw = client.get_table(DatabaseName=DATABASE, Name=glue.TABLE_ENERGY_RAW_30S)[
            "Table"
        ]
        assert raw["Parameters"]["projection.enabled"] == "true"
        assert all(c["Comment"] for c in raw["StorageDescriptor"]["Columns"])
