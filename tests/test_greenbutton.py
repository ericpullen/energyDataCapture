"""The Green Button importer (PLAN.md §13).

The failure this file mostly exists to prevent is a **silent factor of 1000**.
ESPI carries its scale in a ``ReadingType`` (``uom`` + ``powerOfTenMultiplier``),
utilities publish watt-hours far more often than kilowatt-hours, and the whole
point of importing meter data is to compare it against the panels — so an
importer that quietly assumed units would produce a beautiful, plausible,
completely wrong answer. Several tests here assert that it *refuses* instead.

The second theme is the cardinal rule: a reading the export does not contain
produces no row. Not a zero, not a carried-forward value.

No fixtures on disk and no network: the ESPI documents are built inline so the
structure under test is visible next to the assertion.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from energy_capture import model, timeutil
from energy_capture.model import MeterObservation
from energy_capture.stages import greenbutton
from energy_capture.stages.greenbutton import GreenButtonError

BASE = "https://api.example.com/espi/1_1/resource"
USAGE_POINT = f"{BASE}/Subscription/9/UsagePoint/1308468"
METER_READING = f"{USAGE_POINT}/MeterReading/00121840"
READING_TYPE = f"{BASE}/ReadingType/07000100"


def epoch(text: str) -> int:
    """``"2026-08-16T04:00:00Z"`` -> unix seconds."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


def espi(
    *,
    readings: list[tuple[int, int, int]],
    uom: int = 72,
    power_of_ten: int = 0,
    flow_direction: int = 1,
    with_reading_type: bool = True,
) -> str:
    """A Green Button feed shaped like a real one: linked entries, not nesting."""
    blocks = "".join(
        f"""
        <espi:IntervalReading>
          <espi:timePeriod>
            <espi:duration>{duration}</espi:duration>
            <espi:start>{start}</espi:start>
          </espi:timePeriod>
          <espi:value>{value}</espi:value>
        </espi:IntervalReading>"""
        for start, duration, value in readings
    )
    reading_type_entry = (
        f"""
  <entry>
    <link rel="self" href="{READING_TYPE}"/>
    <content>
      <espi:ReadingType>
        <espi:powerOfTenMultiplier>{power_of_ten}</espi:powerOfTenMultiplier>
        <espi:uom>{uom}</espi:uom>
        <espi:flowDirection>{flow_direction}</espi:flowDirection>
      </espi:ReadingType>
    </content>
  </entry>"""
        if with_reading_type
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:espi="http://naesb.org/espi">
  <entry>
    <link rel="self" href="{USAGE_POINT}"/>
    <content><espi:UsagePoint><espi:serviceCategory>
      <espi:kind>0</espi:kind>
    </espi:serviceCategory></espi:UsagePoint></content>
  </entry>{reading_type_entry}
  <entry>
    <link rel="self" href="{METER_READING}"/>
    <link rel="related" href="{READING_TYPE}"/>
    <content><espi:MeterReading/></content>
  </entry>
  <entry>
    <link rel="self" href="{METER_READING}/IntervalBlock/1"/>
    <content>
      <espi:IntervalBlock>
        <espi:interval>
          <espi:duration>86400</espi:duration>
          <espi:start>{readings[0][0] if readings else 0}</espi:start>
        </espi:interval>{blocks}
      </espi:IntervalBlock>
    </content>
  </entry>
</feed>
"""


# ------------------------------------------------------------------ the XML


def test_a_watt_hour_reading_becomes_kwh_and_keeps_its_interval() -> None:
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(espi(readings=[(start, 900, 1234)]))

    assert parsed.rows == 1
    obs = parsed.observations[0]
    assert isinstance(obs, MeterObservation)
    assert obs.value == pytest.approx(1.234)  # 1234 Wh -> kWh
    assert obs.unit == model.UNIT_KWH
    assert obs.metric == "kwh_interval"
    assert obs.interval_s == 900
    assert obs.ts_utc == datetime(2026, 8, 16, 4, 0, tzinfo=UTC)
    assert obs.source == model.SOURCE_LGE
    assert obs.device_id == "1308468"
    assert obs.channel_id == "electric_main"


def test_the_power_of_ten_multiplier_is_applied() -> None:
    """A custodian publishing decawatt-hours is rare but legal, and it scales."""
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        espi(readings=[(start, 3600, 42)], power_of_ten=1)
    )
    assert parsed.observations[0].value == pytest.approx(0.42)  # 42 * 10^1 Wh


def test_ts_utc_is_the_interval_start_not_its_end() -> None:
    """The whole reason the meter dataset has its own schema variant."""
    start = epoch("2026-08-16T12:00:00")
    parsed = greenbutton.parse_espi_xml(espi(readings=[(start, 3600, 1000)]))
    obs = parsed.observations[0]
    assert obs.ts_utc == datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
    assert obs.interval_s == 3600
    # ts_local is the wall clock of the START, via timeutil and nothing else.
    assert obs.ts_local == timeutil.to_local_naive(obs.ts_utc)


def test_an_export_with_no_reading_type_is_refused_not_guessed() -> None:
    """The load-bearing test. Assuming Wh here would be a silent 1000x error."""
    start = epoch("2026-08-16T04:00:00")
    with pytest.raises(GreenButtonError, match="ReadingType"):
        greenbutton.parse_espi_xml(
            espi(readings=[(start, 900, 1234)], with_reading_type=False)
        )


def test_assume_uom_is_the_deliberate_override_and_says_so() -> None:
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        espi(readings=[(start, 900, 1234)], with_reading_type=False),
        assume_uom="Wh",
    )
    assert parsed.observations[0].value == pytest.approx(1.234)
    assert any("assume-uom" in note for note in parsed.notes), parsed.notes


def test_an_unconvertible_uom_is_skipped_and_counted_not_converted() -> None:
    """A real export can carry demand (kW) beside energy (Wh).

    Refusing to *guess* is the rule; aborting the whole import because one
    MeterReading is in other units would be obtuse. What must never happen is
    the number arriving as if it were watt-hours.
    """
    start = epoch("2026-08-16T04:00:00")
    with pytest.raises(GreenButtonError, match="none could be imported"):
        greenbutton.parse_espi_xml(espi(readings=[(start, 900, 1)], uom=31))


def test_nothing_importable_is_an_error_rather_than_an_empty_success() -> None:
    """An export of pure generation would otherwise write a file of no rows."""
    start = epoch("2026-08-16T04:00:00")
    with pytest.raises(GreenButtonError, match="reverse flow"):
        greenbutton.parse_espi_xml(
            espi(readings=[(start, 900, 500)], flow_direction=19)
        )


def lge_shaped(
    meters: list[tuple[str, str, list[tuple[int, int, int]], list[tuple[int, int, int]]]],
) -> str:
    """A feed wired the way LG&E actually wires one.

    Two things here are not what a naive fixture would do, and both were taken
    from a real ``mymeter.lge-ku.com`` export (2026-08-18):

    1. The **ReadingType** carries ``related`` pointing *down* at its
       MeterReading. The MeterReading's own ``related`` points *up* at the
       UsagePoint. Following the MeterReading's links to find a ReadingType
       therefore finds nothing.
    2. The UsagePoint carries a ``name`` — the human meter number — which is a
       better ``device_id`` than the opaque UsagePoint id, and is what the CSV
       export prints.

    Each meter gets a forward and a reverse MeterReading, as the real one does.
    """
    base = "https://mymeter.lge-ku.com/Usage/Download"
    parts = []
    for point_id, name, forward, reverse in meters:
        point = f"{base}/UsagePoint/{point_id}"
        parts.append(
            f"""
  <entry><link rel="self" href="{point}"/>
    <link rel="related" href="{point}/MeterReading/{point_id}f"/>
    <content><espi:UsagePoint><espi:ServiceCategory><espi:kind>0</espi:kind>
      </espi:ServiceCategory><espi:name>{name}</espi:name>
    </espi:UsagePoint></content></entry>"""
        )
        for suffix, flow, readings in (("f", 1, forward), ("r", 19, reverse)):
            mr = f"{point}/MeterReading/{point_id}{suffix}"
            body = "".join(
                f"""<espi:IntervalReading><espi:timePeriod>
                <espi:duration>{d}</espi:duration><espi:start>{s}</espi:start>
                </espi:timePeriod><espi:value>{v}</espi:value></espi:IntervalReading>"""
                for s, d, v in readings
            )
            parts.append(
                f"""
  <entry><link rel="self" href="{mr}"/>
    <link rel="related" href="{point}"/>
    <content><espi:MeterReading/></content></entry>
  <entry><link rel="self" href="{point}/ReadingType/{point_id}{suffix}"/>
    <link rel="related" href="{mr}"/>
    <content><espi:ReadingType>
      <espi:flowDirection>{flow}</espi:flowDirection>
      <espi:intervalLength>900</espi:intervalLength>
      <espi:powerOfTenMultiplier>0</espi:powerOfTenMultiplier>
      <espi:uom>72</espi:uom>
    </espi:ReadingType></content></entry>
  <entry><link rel="self" href="{mr}/IntervalBlock/1"/><content>
    <espi:IntervalBlock><espi:interval><espi:duration>86400</espi:duration>
      <espi:start>{readings[0][0] if readings else 0}</espi:start></espi:interval>
      {body}</espi:IntervalBlock></content></entry>"""
            )
    return (
        '<?xml version="1.0"?>\n<feed xmlns="http://www.w3.org/2005/Atom" '
        f'xmlns:espi="http://naesb.org/espi">{"".join(parts)}\n</feed>\n'
    )


def test_the_reading_type_is_found_when_it_points_down_at_the_meter_reading() -> None:
    """LG&E's link direction. Following MeterReading -> related finds nothing."""
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        lge_shaped([("00121847", "1308468", [(start, 900, 412)], [])])
    )
    assert parsed.rows == 1
    assert parsed.observations[0].value == pytest.approx(0.412)


def test_the_meter_number_is_preferred_over_the_usage_point_id() -> None:
    """``1308468`` is what appears on the bill and in the CSV; ``00121847`` is not."""
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        lge_shaped([("00121847", "1308468", [(start, 900, 412)], [])])
    )
    assert parsed.meters == {"1308468"}


def test_reverse_flow_is_skipped_while_forward_flow_imports() -> None:
    """Every real export pairs them. Importing generation as consumption would
    inflate consumption silently, which is exactly the error this whole
    comparison is meant to detect."""
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        lge_shaped(
            [
                (
                    "00121847",
                    "1308468",
                    [(start, 900, 500), (start + 900, 900, 600)],
                    [(start, 900, 9999), (start + 900, 900, 8888)],
                )
            ]
        )
    )
    assert parsed.rows == 2
    assert parsed.skipped_reverse == 2
    assert [o.value for o in parsed.observations] == pytest.approx([0.5, 0.6])


def test_several_meters_on_one_account_stay_separate() -> None:
    """The real export carries three. Summing them would be a fiction."""
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        lge_shaped(
            [
                ("00121847", "1308468", [(start, 900, 400)], []),
                ("00026043", "944401", [(start, 900, 100)], []),
                ("00025473", "944006", [(start, 900, 0)], []),
            ]
        )
    )
    assert parsed.meters == {"1308468", "944401", "944006"}
    assert parsed.rows == 3
    assert {o.device_id for o in parsed.observations} == parsed.meters


def test_a_reading_with_no_value_produces_no_row() -> None:
    """Gaps stay gaps (CLAUDE.md rule 1) — not a zero, not the previous value."""
    start = epoch("2026-08-16T04:00:00")
    document = espi(readings=[(start, 900, 100), (start + 900, 900, 200)])
    # Blank out the second reading's value, exactly as a custodian omitting it
    # would look after serialisation.
    document = document.replace("<espi:value>200</espi:value>", "<espi:value></espi:value>")
    parsed = greenbutton.parse_espi_xml(document)
    assert parsed.rows == 1
    assert parsed.observations[0].value == pytest.approx(0.1)


def test_a_csv_handed_to_the_xml_parser_says_which_file_to_pass() -> None:
    with pytest.raises(GreenButtonError, match="IntervalBlock"):
        greenbutton.parse_espi_xml("<feed xmlns='http://www.w3.org/2005/Atom'/>")


def test_intervals_and_meters_are_reported_for_the_operator() -> None:
    start = epoch("2026-08-16T04:00:00")
    parsed = greenbutton.parse_espi_xml(
        espi(readings=[(start, 900, 1), (start + 900, 900, 2), (start + 1800, 3600, 3)])
    )
    assert parsed.intervals == {900: 2, 3600: 1}
    assert parsed.meters == {"1308468"}
    assert parsed.to_dict()["first_ts_utc"].startswith("2026-08-16T04:00")


# ------------------------------------------------------------------ the CSV

CSV_HEADER = "Start,End,Account,Meter,Rate,Units,Direction,Estimated,kWh,Cost"


def csv_rows(*lines: str) -> str:
    return "\n".join([CSV_HEADER, *lines]) + "\n"


def test_csv_columns_are_found_by_header_name_not_position() -> None:
    text = csv_rows(
        '08/16/2026 12:00:00 AM,08/16/2026 12:15:00 AM,1,Meter #1308468 - Total Energy Charge,RS,kWh,Delivered,N,0.412,$0.05',
        '08/16/2026 12:15:00 AM,08/16/2026 12:30:00 AM,1,Meter #1308468 - Total Energy Charge,RS,kWh,Delivered,N,0.398,$0.05',
    )
    parsed = greenbutton.parse_csv(text)

    assert parsed.rows == 2
    assert parsed.meters == {"1308468"}
    assert parsed.intervals == {900: 2}
    assert parsed.observations[0].value == pytest.approx(0.412)
    # The CSV's stamps are LOCAL wall clock; ts_utc is the converted instant.
    assert parsed.observations[0].ts_local == datetime(2026, 8, 16, 0, 0)


def test_csv_received_rows_are_skipped() -> None:
    text = csv_rows(
        '08/16/2026 12:00:00 AM,08/16/2026 01:00:00 AM,1,Meter #1308468,RS,kWh,Delivered,N,1.0,$0.12',
        '08/16/2026 12:00:00 AM,08/16/2026 01:00:00 AM,1,Meter #1308468,RS,kWh,Received,N,9.9,$0.00',
    )
    parsed = greenbutton.parse_csv(text)
    assert parsed.rows == 1
    assert parsed.skipped_reverse == 1
    assert parsed.observations[0].value == pytest.approx(1.0)


def test_a_blank_csv_reading_produces_no_row() -> None:
    text = csv_rows(
        '08/16/2026 12:00:00 AM,08/16/2026 01:00:00 AM,1,Meter #1308468,RS,kWh,Delivered,N,1.0,$0.12',
        '08/16/2026 01:00:00 AM,08/16/2026 02:00:00 AM,1,Meter #1308468,RS,kWh,Delivered,N,,',
    )
    parsed = greenbutton.parse_csv(text)
    assert parsed.rows == 1


def test_an_inferred_csv_interval_is_reported_as_inferred() -> None:
    """Deriving it is fine. Deriving it silently is not."""
    text = (
        "Start,Meter,Direction,kWh\n"
        "08/16/2026 12:00:00 AM,Meter #1308468,Delivered,1.0\n"
        "08/16/2026 01:00:00 AM,Meter #1308468,Delivered,1.1\n"
        "08/16/2026 02:00:00 AM,Meter #1308468,Delivered,1.2\n"
    )
    parsed = greenbutton.parse_csv(text)
    assert parsed.intervals == {3600: 3}
    assert any("INFERRED" in note for note in parsed.notes), parsed.notes


def test_an_unrecognisable_csv_header_names_the_header() -> None:
    with pytest.raises(GreenButtonError, match="header"):
        greenbutton.parse_csv("alpha,beta,gamma\n1,2,3\n")


# ---------------------------------------------------------------- landing it


def test_a_reimport_replaces_rather_than_duplicating(tmp_path: Path) -> None:
    """MyMeter revises recent readings, so the fresh value has to win."""
    start = epoch("2026-08-16T04:00:00")
    first = tmp_path / "first.xml"
    first.write_text(espi(readings=[(start, 3600, 1000), (start + 3600, 3600, 2000)]))
    greenbutton.run(path=first, out_dir=tmp_path / "meter")

    revised = tmp_path / "revised.xml"
    revised.write_text(espi(readings=[(start, 3600, 1500)]))
    summary = greenbutton.run(path=revised, out_dir=tmp_path / "meter")

    table = pq.read_table(Path(summary["files"][0]))
    assert table.num_rows == 2, "the overlapping hour was duplicated"
    values = dict(
        zip(
            [timeutil.format_utc(t) for t in table.column("ts_utc").to_pylist()],
            table.column("value").to_pylist(),
            strict=True,
        )
    )
    assert values[timeutil.format_utc(datetime.fromtimestamp(start, tz=UTC))] == (
        pytest.approx(1.5)
    ), "the stale value survived the re-import"


def test_the_month_file_is_named_the_way_s3_names_it(tmp_path: Path) -> None:
    from energy_capture.aws import s3io

    start = epoch("2026-08-16T04:00:00")
    export = tmp_path / "gb.xml"
    export.write_text(espi(readings=[(start, 3600, 1000)]))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")

    local_day = timeutil.local_date_of(datetime.fromtimestamp(start, tz=UTC))
    assert Path(summary["files"][0]).name == Path(s3io.meter_key(local_day)).name


def test_the_output_matches_the_meter_schema(tmp_path: Path) -> None:
    start = epoch("2026-08-16T04:00:00")
    export = tmp_path / "gb.xml"
    export.write_text(espi(readings=[(start, 900, 250)]))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")

    table = pq.read_table(Path(summary["files"][0]))
    assert table.schema.names == model.METER_SCHEMA.names
    assert table.column("interval_s").to_pylist() == [900]


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    start = epoch("2026-08-16T04:00:00")
    export = tmp_path / "gb.xml"
    export.write_text(espi(readings=[(start, 900, 250)]))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter", dry_run=True)

    assert summary["files"] == []
    assert not (tmp_path / "meter").exists() or not list((tmp_path / "meter").iterdir())


def test_readings_spanning_a_month_boundary_land_in_separate_files(
    tmp_path: Path,
) -> None:
    """One file per month, because that is what meter_key promises."""
    august = epoch("2026-08-31T20:00:00")  # 2026-08-31 16:00 local
    september = epoch("2026-09-01T20:00:00")
    export = tmp_path / "gb.xml"
    export.write_text(espi(readings=[(august, 3600, 1000), (september, 3600, 2000)]))
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")

    names = sorted(Path(f).name for f in summary["files"])
    assert names == ["lge-202608.parquet", "lge-202609.parquet"]


def test_the_file_type_is_decided_by_content_not_extension(tmp_path: Path) -> None:
    """An export saved as .txt, or XML with a .csv name, still imports."""
    start = epoch("2026-08-16T04:00:00")
    misnamed = tmp_path / "GreenButton.csv"
    misnamed.write_text(espi(readings=[(start, 900, 250)]))
    parsed = greenbutton.parse_export(misnamed)
    assert parsed.rows == 1


# ------------------------------------------- two resolutions of one meter


def test_hourly_and_quarter_hourly_series_do_not_overwrite_each_other(
    tmp_path: Path,
) -> None:
    """LG&E publishes the same energy at two resolutions. Both must survive.

    Measured 2026-08-18 on the live Connect feed: every UsagePoint carries a
    900-second series *and* a 3600-second one, colliding at 167 timestamps in
    four days. Under the canonical dedupe key — which has no ``interval_s`` —
    one silently replaced the other, so an hour boundary held either 15 minutes
    of energy or a whole hour of it, unpredictably, and any SUM was wrong.

    ``model.METER_DEDUPE_KEY`` adds ``interval_s`` for exactly this. Choosing
    between the series is a query-time decision (``compare-meter`` takes the
    finest), not a reason to discard the custodian's data at ingest.
    """
    start = epoch("2026-08-16T04:00:00")
    document = espi(readings=[(start, 900, 400)]).replace(
        "</espi:IntervalBlock>",
        f"""<espi:IntervalReading><espi:timePeriod>
          <espi:duration>3600</espi:duration><espi:start>{start}</espi:start>
        </espi:timePeriod><espi:value>1600</espi:value></espi:IntervalReading>
        </espi:IntervalBlock>""",
    )
    export = tmp_path / "gb.xml"
    export.write_text(document)
    summary = greenbutton.run(path=export, out_dir=tmp_path / "meter")

    table = pq.read_table(Path(summary["files"][0]))
    assert table.num_rows == 2, "the two resolutions collapsed into one"
    by_interval = dict(
        zip(
            table.column("interval_s").to_pylist(),
            table.column("value").to_pylist(),
            strict=True,
        )
    )
    assert by_interval == {900: pytest.approx(0.4), 3600: pytest.approx(1.6)}


def test_the_meter_dedupe_key_is_the_canonical_one_plus_interval() -> None:
    """Pinned: dropping interval_s here is a silent data-loss bug."""
    assert model.METER_DEDUPE_KEY == (*model.DEDUPE_KEY, "interval_s")
    assert model.dedupe_key_for(model.Dataset.METER) == model.METER_DEDUPE_KEY
    # Only the meter dataset differs — raw_30s has no interval_s column at all.
    assert model.dedupe_key_for(model.Dataset.RAW_30S) == model.DEDUPE_KEY
