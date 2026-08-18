"""Green Button import: an LG&E export -> ``energy/meter`` (PLAN.md §13).

What this is for
----------------
The utility meter is the only authoritative record of what the household is
billed for. Everything else in this pipeline is sub-metering, and sub-metering
is only trustworthy once someone has checked it against the meter. This stage
is the "check it" half: it reads a **Download My Data** export and lands it as
``MeterObservation`` rows so ``energycap compare-meter`` can put the two side by
side.

It is deliberately independent of Green Button *Connect*, the automated OAuth'd
API (approved 2026-08-18 — ``docs/lge-greenbutton.md``). Connect fetches the same
ESPI over HTTP in :mod:`energy_capture.stages.greenbutton_fetch`, and lands it
through *this* module's parser and :func:`write_months`, so the manual and
automated paths cannot drift in how a reading becomes a row. Download My Data
remains the route for bulk history and the fallback if an authorisation lapses.

Meter data is *interval* data
-----------------------------
Hence ``MeterObservation``: ``ts_utc`` is the interval **START** and
``interval_s`` is how long it covers. A 15-minute reading stamped 14:00 covers
14:00–14:15. This is the one place in the codebase where a timestamp is not an
instantaneous sample, which is exactly why §13 gave the meter dataset its own
schema variant rather than pretending.

What it will not do
-------------------
* **Guess at scale.** ESPI carries ``uom`` and ``powerOfTenMultiplier`` in a
  ``ReadingType``; this reads them. If the export omits the ReadingType the
  import *fails* rather than assuming watt-hours, because a silent factor of
  1000 is precisely the error a meter comparison exists to detect. Override
  deliberately with ``--assume-uom Wh`` if you have checked.
* **Fill a gap.** An interval the export does not contain produces no row.
* **Invent an interval length.** For CSV (which is not self-describing) the
  length comes from an end-time column, or ``--interval-s``, or is inferred from
  the spacing of consecutive readings and *logged* — never silently defaulted.

Idempotency
-----------
Output is one Parquet file per calendar month touched, named exactly as
:func:`energy_capture.aws.s3io.meter_key` names it. A re-import merges into the
existing month on the canonical dedupe key with the *newly read* row winning, so
importing overlapping ranges converges rather than duplicating — which matters
because MyMeter revises recent readings.
"""

from __future__ import annotations

import csv
import io
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import pyarrow as pa
import pyarrow.parquet as pq

from energy_capture import model, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.model import Dataset, MeterObservation

STAGE = "greenbutton"
log = get_logger(STAGE)

ESPI_NS = "http://naesb.org/espi"
ATOM_NS = "http://www.w3.org/2005/Atom"

#: ESPI ``UomType``. Only what we can convert without guessing is listed; an
#: unlisted code is an error naming the code, not a silent passthrough.
UOM_WH = 72

#: ESPI ``FlowDirectionType``. 1 is consumption; 19 is what a solar array sends
#: back. Only forward is imported — see :data:`ParsedExport.skipped_reverse`.
FLOW_FORWARD = 1

#: Datetime layouts seen in LG&E CSV exports, most specific first. These are
#: LOCAL wall-clock times, which is why they go through ``timeutil``.
CSV_DATETIME_FORMATS: tuple[str, ...] = (
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y",
    "%Y-%m-%d",
)

__all__ = [
    "ParsedExport",
    "parse_csv",
    "parse_espi_xml",
    "parse_export",
    "run",
    "write_months",
]


class GreenButtonError(RuntimeError):
    """The export could not be read without guessing at what it means."""


@dataclass
class ParsedExport:
    """Everything one export file yielded, including what it did *not* yield."""

    observations: list[MeterObservation] = field(default_factory=list)
    #: ``device_id`` values seen — the ESPI UsagePoint ids / meter numbers.
    meters: set[str] = field(default_factory=set)
    #: Interval lengths seen, in seconds, with counts. More than one entry is
    #: legitimate (a day boundary can change granularity) but worth reporting.
    intervals: Counter[int] = field(default_factory=Counter)
    #: Readings skipped because ``flowDirection`` was not forward.
    skipped_reverse: int = 0
    #: Readings skipped because their ``uom`` is not something we can convert
    #: without guessing (demand in kW, say, alongside energy in Wh).
    skipped_unconvertible: int = 0
    #: Human-readable notes to surface to the operator, not to swallow.
    notes: list[str] = field(default_factory=list)

    @property
    def rows(self) -> int:
        return len(self.observations)

    @property
    def span(self) -> tuple[datetime, datetime] | None:
        if not self.observations:
            return None
        stamps = [obs.ts_utc for obs in self.observations]
        return min(stamps), max(stamps)

    def to_dict(self) -> dict[str, Any]:
        span = self.span
        return {
            "rows": self.rows,
            "meters": sorted(self.meters),
            "intervals_s": dict(sorted(self.intervals.items())),
            "skipped_reverse": self.skipped_reverse,
            "skipped_unconvertible": self.skipped_unconvertible,
            "first_ts_utc": timeutil.format_utc(span[0]) if span else None,
            "last_ts_utc": timeutil.format_utc(span[1]) if span else None,
            "notes": list(self.notes),
        }


# --------------------------------------------------------------- ESPI XML


def _tag(local: str, ns: str = ESPI_NS) -> str:
    return f"{{{ns}}}{local}"


def _text(node: ElementTree.Element | None) -> str | None:
    return None if node is None else (node.text or "").strip() or None


def _int(node: ElementTree.Element | None) -> int | None:
    raw = _text(node)
    if raw is None:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _self_href(entry: ElementTree.Element) -> str:
    for link in entry.findall(_tag("link", ATOM_NS)):
        if link.get("rel") == "self":
            return link.get("href") or ""
    return ""


def _related_hrefs(entry: ElementTree.Element) -> list[str]:
    return [
        link.get("href") or ""
        for link in entry.findall(_tag("link", ATOM_NS))
        if link.get("rel") == "related"
    ]


def _usage_point_of(href: str) -> str | None:
    found = re.search(r"/UsagePoint/([^/]+)", href)
    return found.group(1) if found else None


def _meter_reading_href(interval_block_href: str) -> str:
    """``…/MeterReading/456/IntervalBlock/7`` -> ``…/MeterReading/456``."""
    return re.sub(r"/IntervalBlock/.*$", "", interval_block_href)


@dataclass(frozen=True)
class _ReadingType:
    """The part of an ESPI ReadingType that decides what a number *means*."""

    uom: int | None
    power_of_ten: int
    flow_direction: int | None
    interval_length: int | None

    @property
    def convertible(self) -> bool:
        return self.uom == UOM_WH

    def to_kwh(self, raw: float) -> float:
        if not self.convertible:
            raise GreenButtonError(
                f"ESPI uom {self.uom!r} is not watt-hours (72) and this importer "
                "will not guess at the conversion."
            )
        return raw * (10.0**self.power_of_ten) / 1000.0


def _parse_reading_type(entry: ElementTree.Element) -> _ReadingType:
    content = entry.find(_tag("content", ATOM_NS))
    node = content if content is not None else entry
    return _ReadingType(
        uom=_int(node.find(f".//{_tag('uom')}")),
        power_of_ten=_int(node.find(f".//{_tag('powerOfTenMultiplier')}")) or 0,
        flow_direction=_int(node.find(f".//{_tag('flowDirection')}")),
        interval_length=_int(node.find(f".//{_tag('intervalLength')}")),
    )


def parse_espi_xml(
    data: str | bytes,
    *,
    source: str = model.SOURCE_LGE,
    channel_id: str = "electric_main",
    assume_uom: str | None = None,
) -> ParsedExport:
    """Parse a Green Button (ESPI/NAESB REQ.21) Atom feed.

    The feed is a flat list of ``<entry>`` elements wired together by ``href``:
    an IntervalBlock's *self* href says which MeterReading and which UsagePoint
    it belongs to, and the MeterReading's *related* href points at the
    ReadingType that gives units, scale and flow direction. This walks those
    links rather than assuming a document order.
    """
    result = ParsedExport()
    root = ElementTree.fromstring(data if isinstance(data, bytes) else data.encode())

    entries = root.findall(f".//{_tag('entry', ATOM_NS)}")
    by_href: dict[str, ElementTree.Element] = {}
    reading_types: dict[str, _ReadingType] = {}
    #: MeterReading href -> its ReadingType. LG&E wires the relation from the
    #: *ReadingType* end (its ``related`` points at the MeterReading, while the
    #: MeterReading's ``related`` points up at the UsagePoint), so following the
    #: MeterReading's links alone finds nothing. Both directions are indexed.
    by_meter_reading: dict[str, _ReadingType] = {}
    meter_names: dict[str, str] = {}
    blocks: list[tuple[str, ElementTree.Element]] = []

    for entry in entries:
        href = _self_href(entry).rstrip("/")
        if href:
            by_href[href] = entry
        if entry.find(f".//{_tag('ReadingType')}") is not None:
            parsed_type = _parse_reading_type(entry)
            reading_types[href] = parsed_type
            for related in _related_hrefs(entry):
                if "/MeterReading/" in related:
                    by_meter_reading[related.rstrip("/")] = parsed_type
        if entry.find(f".//{_tag('UsagePoint')}") is not None:
            name = _text(entry.find(f".//{_tag('name')}"))
            point = _usage_point_of(href)
            if name and point:
                # The human-facing meter number, e.g. "1308468" — the same
                # identity the CSV export prints, so the two agree on device_id.
                meter_names[point] = name
        for block in entry.findall(f".//{_tag('IntervalBlock')}"):
            blocks.append((href, block))

    if not blocks:
        raise GreenButtonError(
            "no IntervalBlock elements found — this does not look like a Green "
            "Button ESPI export (is it the CSV? pass the .csv file instead)"
        )

    override = _uom_override(assume_uom)
    if override is not None:
        result.notes.append(
            f"--assume-uom {assume_uom!r} was given: ReadingType units were not consulted"
        )

    for block_href, block in blocks:
        reading_type = override or _reading_type_for(
            block_href, by_href, reading_types, by_meter_reading, result
        )
        if reading_type.flow_direction not in (None, FLOW_FORWARD):
            result.skipped_reverse += len(block.findall(_tag("IntervalReading")))
            continue
        if not reading_type.convertible:
            # Not an error: an export may carry demand (kW) alongside energy.
            # Refusing to *guess* is the rule; refusing to import anything at
            # all because one MeterReading is in other units would be obtuse.
            result.skipped_unconvertible += len(block.findall(_tag("IntervalReading")))
            note = f"skipped readings in uom {reading_type.uom!r} (not watt-hours)"
            if note not in result.notes:
                result.notes.append(note)
            continue

        point = _usage_point_of(block_href)
        device_id = meter_names.get(point or "", point) or "unknown"
        block_duration = _int(block.find(f"{_tag('interval')}/{_tag('duration')}"))

        for reading in block.findall(_tag("IntervalReading")):
            start = _int(reading.find(f"{_tag('timePeriod')}/{_tag('start')}"))
            raw = _text(reading.find(_tag("value")))
            if start is None or raw is None:
                # A reading the custodian did not publish. Gaps stay gaps.
                continue
            duration = (
                _int(reading.find(f"{_tag('timePeriod')}/{_tag('duration')}"))
                or reading_type.interval_length
                or block_duration
            )
            if not duration or duration <= 0:
                raise GreenButtonError(
                    f"interval at {start} has no duration in the reading, the "
                    "ReadingType or the block — refusing to assume one"
                )
            result.observations.append(
                model.make_observation(
                    ts_utc=datetime.fromtimestamp(start, tz=UTC),
                    source=source,
                    device_id=device_id,
                    channel_id=channel_id,
                    metric="kwh_interval",
                    value=reading_type.to_kwh(float(raw)),
                    interval_s=int(duration),
                )
            )
            result.meters.add(device_id)
            result.intervals[int(duration)] += 1

    if not result.observations and (result.skipped_reverse or result.skipped_unconvertible):
        raise GreenButtonError(
            "the export contained readings but none could be imported: "
            f"{result.skipped_reverse} were reverse flow (generation) and "
            f"{result.skipped_unconvertible} were in units this importer will "
            "not convert. Nothing was written."
        )
    return result


def _uom_override(assume_uom: str | None) -> _ReadingType | None:
    if assume_uom is None:
        return None
    normalised = assume_uom.strip().lower()
    if normalised in {"wh", "watt-hours", "watthours"}:
        return _ReadingType(
            uom=UOM_WH, power_of_ten=0, flow_direction=None, interval_length=None
        )
    if normalised in {"kwh", "kilowatt-hours"}:
        return _ReadingType(
            uom=UOM_WH, power_of_ten=3, flow_direction=None, interval_length=None
        )
    raise GreenButtonError(
        f"--assume-uom {assume_uom!r} is not understood; use 'Wh' or 'kWh'"
    )


def _reading_type_for(
    block_href: str,
    by_href: dict[str, ElementTree.Element],
    reading_types: dict[str, _ReadingType],
    by_meter_reading: dict[str, _ReadingType],
    result: ParsedExport,
) -> _ReadingType:
    """Resolve IntervalBlock -> MeterReading -> ReadingType, either direction.

    ESPI does not say which end of that relation carries the link, and the two
    exports in evidence disagree: LG&E points from the ReadingType *down* to the
    MeterReading, so following the MeterReading's own ``related`` links finds
    only the UsagePoint. Both indexes are consulted before giving up.
    """
    meter_reading_href = _meter_reading_href(block_href)
    direct = by_meter_reading.get(meter_reading_href)
    if direct is not None:
        return direct

    meter_reading = by_href.get(meter_reading_href)
    if meter_reading is not None:
        for href in _related_hrefs(meter_reading):
            found = reading_types.get(href.rstrip("/"))
            if found is not None:
                return found

    if len(reading_types) == 1:
        only = next(iter(reading_types.values()))
        note = (
            "this export has exactly one ReadingType and no link from the "
            "IntervalBlock's MeterReading to it; using it for every reading"
        )
        if note not in result.notes:
            result.notes.append(note)
            log.warning("greenbutton_reading_type_inferred", block=block_href)
        return only

    raise GreenButtonError(
        f"no ReadingType could be resolved for {block_href!r} "
        f"({len(reading_types)} present in the feed). Units and scale are "
        "therefore unknown, and this importer will not assume watt-hours — "
        "re-run with --assume-uom Wh if you have confirmed them yourself."
    )


# --------------------------------------------------------------- LG&E CSV


def _find_column(header: list[str], *candidates: str) -> int | None:
    lowered = [h.strip().lower() for h in header]
    for want in candidates:
        for index, name in enumerate(lowered):
            if want in name:
                return index
    return None


def _parse_local_stamp(raw: str) -> datetime:
    text = raw.strip()
    for layout in CSV_DATETIME_FORMATS:
        try:
            return timeutil.local_naive_to_utc(datetime.strptime(text, layout))
        except ValueError:
            continue
    raise GreenButtonError(f"could not read {raw!r} as a date/time")


def parse_csv(
    text: str,
    *,
    source: str = model.SOURCE_LGE,
    channel_id: str = "electric_main",
    interval_s: int | None = None,
) -> ParsedExport:
    """Parse an LG&E MyMeter ``Usage.csv`` export.

    Columns are located **by header name**, not by position, because the header
    is the only thing about this format anyone has documented. If the headers
    cannot be recognised the error names them, so the fix is one line here.

    Unlike the XML this format is not self-describing about interval length, so
    ``interval_s`` comes from an end-time column if there is one, else the
    ``interval_s`` argument, else the most common spacing between consecutive
    readings — and which of those happened is recorded in ``notes``.
    """
    result = ParsedExport()
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise GreenButtonError("the CSV is empty")

    header = rows[0]
    col_start = _find_column(header, "start", "interval", "date", "time")
    col_value = _find_column(header, "kwh", "usage", "consumption", "value")
    if col_start is None or col_value is None:
        raise GreenButtonError(
            "could not find a start-time column and a kWh column in the CSV "
            f"header: {header!r}"
        )
    col_end = _find_column(header, "end")
    col_flow = _find_column(header, "direction", "flow")
    col_meter = _find_column(header, "meter", "usage point", "account")

    parsed: list[tuple[datetime, float, int | None, str]] = []
    for line in rows[1:]:
        if not any(cell.strip() for cell in line):
            continue
        if len(line) <= max(col_start, col_value):
            continue
        if col_flow is not None and len(line) > col_flow:
            flow = line[col_flow].strip().lower()
            if flow and not flow.startswith(("deliver", "consum", "forward")):
                result.skipped_reverse += 1
                continue
        raw_value = line[col_value].strip().replace(",", "").replace("$", "")
        if not raw_value:
            continue  # a blank cell is a missing reading, not a zero
        start = _parse_local_stamp(line[col_start])
        end = None
        if col_end is not None and len(line) > col_end and line[col_end].strip():
            end = _parse_local_stamp(line[col_end])
        device = "unknown"
        if col_meter is not None and len(line) > col_meter:
            device = _meter_id(line[col_meter]) or device
        duration = int((end - start).total_seconds()) if end else None
        parsed.append((start, float(raw_value), duration, device))

    if not parsed:
        raise GreenButtonError("no usable rows in the CSV after filtering")

    parsed.sort(key=lambda item: item[0])
    fallback = interval_s or _infer_interval_s(parsed, result)

    for start, value, duration, device in parsed:
        length = duration or fallback
        if not length or length <= 0:
            raise GreenButtonError(
                f"no interval length for the reading at {start.isoformat()} — "
                "pass --interval-s"
            )
        result.observations.append(
            model.make_observation(
                ts_utc=start,
                source=source,
                device_id=device,
                channel_id=channel_id,
                metric="kwh_interval",
                value=value,
                interval_s=int(length),
            )
        )
        result.meters.add(device)
        result.intervals[int(length)] += 1

    if interval_s:
        result.notes.append(f"interval_s={interval_s} was supplied on the command line")
    return result


def _meter_id(cell: str) -> str | None:
    """``"Meter #1308468 - Total Energy Charge"`` -> ``"1308468"``."""
    found = re.search(r"\d{4,}", cell)
    if found:
        return found.group(0)
    trimmed = cell.strip()
    return trimmed or None


def _infer_interval_s(
    parsed: list[tuple[datetime, float, int | None, str]], result: ParsedExport
) -> int | None:
    """Most common spacing between consecutive readings, loudly reported."""
    gaps = Counter(
        int((b[0] - a[0]).total_seconds())
        for a, b in zip(parsed, parsed[1:], strict=False)
        if b[0] > a[0]
    )
    if not gaps:
        return None
    common, count = gaps.most_common(1)[0]
    result.notes.append(
        f"interval length was INFERRED as {common}s from the spacing of "
        f"{count} consecutive readings — the CSV does not state it. Pass "
        "--interval-s to override, or export the XML, which does state it."
    )
    log.warning("greenbutton_interval_inferred", interval_s=common, samples=count)
    return common


# ------------------------------------------------------------ file -> rows


def parse_export(
    path: str | Path,
    *,
    source: str = model.SOURCE_LGE,
    channel_id: str = "electric_main",
    assume_uom: str | None = None,
    interval_s: int | None = None,
) -> ParsedExport:
    """Dispatch on content, not on the file extension."""
    text = Path(path).read_text(encoding="utf-8-sig", errors="replace")
    head = text.lstrip()[:400].lower()
    if head.startswith("<?xml") or "<feed" in head or "espi" in head:
        return parse_espi_xml(
            text, source=source, channel_id=channel_id, assume_uom=assume_uom
        )
    return parse_csv(
        text, source=source, channel_id=channel_id, interval_s=interval_s
    )


# --------------------------------------------------------------- landing it


def month_filename(local_day: date, *, source: str = model.SOURCE_LGE) -> str:
    """The basename :func:`s3io.meter_key` uses, so local and S3 agree."""
    return f"{source}-{local_day:%Y%m}.parquet"


def merge_into_month(
    existing: pa.Table | None, incoming: pa.Table
) -> pa.Table:
    """Concatenate and collapse on the canonical dedupe key, newest winning.

    MyMeter revises recently published readings, so a re-import of an
    overlapping range must *replace* rather than accumulate. ``model.dedupe_table``
    keeps the **first** occurrence, so ``incoming`` is concatenated *first* — the
    freshly read value wins and the stale one is dropped.
    """
    if existing is None or existing.num_rows == 0:
        combined = incoming
    else:
        combined = pa.concat_tables(
            [incoming.cast(model.METER_SCHEMA), existing.cast(model.METER_SCHEMA)]
        )
    return model.sort_table(model.dedupe_table(combined))


def write_months(
    parsed: ParsedExport,
    destination: Path,
    *,
    source: str = model.SOURCE_LGE,
    dry_run: bool = False,
) -> list[str]:
    """Land a parsed export as one Parquet file per calendar month it touches.

    Shared by the file import and the Connect fetch on purpose: the two paths
    differ in where the ESPI came from and in nothing else, so they must not be
    able to drift in how a reading becomes a row on disk.

    Months are keyed on the **local** date of the interval start, because that
    is what ``meter_key`` partitions on (CLAUDE.md rule 4).
    """
    months: dict[str, list[MeterObservation]] = {}
    for obs in parsed.observations:
        months.setdefault(month_filename(obs.ts_local.date(), source=source), []).append(obs)

    written: list[str] = []
    if not dry_run and months:
        destination.mkdir(parents=True, exist_ok=True)
    for name, rows in sorted(months.items()):
        target = destination / name
        incoming = model.observations_to_table(rows, dataset=Dataset.METER)
        existing = pq.read_table(target) if target.exists() else None
        merged = merge_into_month(existing, incoming)
        if dry_run:
            log.info("greenbutton_would_write", file=str(target), rows=merged.num_rows)
            continue
        pq.write_table(merged, target)
        written.append(str(target))
        log.info("greenbutton_wrote", file=str(target), rows=merged.num_rows)
    return written


def run(
    *,
    path: str | Path,
    source: str = model.SOURCE_LGE,
    channel_id: str = "electric_main",
    out_dir: str | Path | None = None,
    bucket: str | None = None,
    assume_uom: str | None = None,
    interval_s: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """``energycap import-greenbutton FILE`` (PLAN.md §13).

    Writes one Parquet file per calendar month the export touches, under
    ``out_dir`` (default ``SPOOL_DIR/meter``), and to ``energy/meter/year=YYYY/``
    as well **only if** ``bucket`` is given. Local output is the default on
    purpose: nothing in this deployment has an S3 bucket yet, and the comparison
    against the panels runs off these files.
    """
    parsed = parse_export(
        path,
        source=source,
        channel_id=channel_id,
        assume_uom=assume_uom,
        interval_s=interval_s,
    )
    log.info("greenbutton_parsed", file=str(path), **parsed.to_dict())
    for note in parsed.notes:
        log.warning("greenbutton_note", note=note)

    destination = Path(out_dir) if out_dir else get_settings().spool_dir / "meter"
    written = write_months(parsed, destination, source=source, dry_run=dry_run)

    summary: dict[str, Any] = {
        "source_file": str(path),
        "files": written,
        "dry_run": dry_run,
        **parsed.to_dict(),
    }

    # S3 is opt-in, unlike the scheduled stages. An import is a manual act on a
    # file a human just downloaded, and having it fan out to a bucket because an
    # env var happened to be set is a surprise, not a convenience. Pass --bucket
    # (or bucket=) to mirror it.
    if bucket and not dry_run:
        summary["s3"] = _upload_months(destination, written, bucket, source)
    else:
        summary["s3"] = "not uploaded (pass --bucket to mirror to S3)"
    return summary


def _upload_months(
    destination: Path, written: list[str], bucket: str, source: str
) -> list[str]:
    """Mirror the month files to ``energy/meter/`` with the atomic writer."""
    from energy_capture.aws import s3io  # local: keeps boto3 off the import path

    keys: list[str] = []
    for name in written:
        table = pq.read_table(name)
        stamp = Path(name).stem.rsplit("-", 1)[-1]
        local_day = date(int(stamp[:4]), int(stamp[4:6]), 1)
        key = s3io.meter_key(local_day, source=source)
        s3io.write_table_atomic(table, bucket, key)
        keys.append(key)
        log.info("greenbutton_uploaded", key=key, rows=table.num_rows)
    return keys
