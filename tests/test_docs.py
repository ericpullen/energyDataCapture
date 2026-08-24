"""The README is a deliverable, so it is pinned to the code like anything else.

CLAUDE.md makes the Glue comments a first-class deliverable "because they are what
an LLM reads to orient itself". The README is the other half of that: it is the
document a human *and* an LLM read before touching this data, and every number in
it is a claim about the code. Documentation that drifts is worse than none — a
wrong enum decode silently rewrites the meaning of years of archived rows for
whoever trusts it.

What is pinned here is only what would actually mislead someone:

1. the enum decodes, integer for integer, in both the prose table and the SQL
   ``VALUES`` lists the queries use to join them (``sources/bryant.py`` is the
   only source of truth, and its tables are append-only);
2. that every command the CLI exposes is documented, and that no documented
   command has quietly disappeared from the CLI;
3. the kWh formula and the dedupe key, which are the two things a reader is most
   likely to re-derive incorrectly;
4. the ``metric`` and ``unit`` vocabularies, which the README presents as closed
   lists — a reader filtering on an incomplete one drops real rows;
5. **the example queries themselves.** PLAN.md §12 asks for real queries and an
   LLM will paste them verbatim, so they are extracted from the README and
   *executed* against local Parquet written by this package's own writers
   (``model.observations_to_table``, ``stages.rollup.rollup_day``,
   ``stages.dim.DIM_SCHEMA``) with the ``s3://`` prefix rewritten. The corpus is
   built to expose the two ways an example query has silently returned a wrong
   answer here before:

   * **two devices sharing a ``channel_id``.** This house has two Leviton hubs,
     so ``breaker_p10`` and ``ct_1_a`` exist on both panels and are different
     circuits. A query grouped or labelled on ``channel_id`` alone sums them and
     reports one number; the tell is a doubled ``sample_count``.
   * **the November fall-back Sunday.** A bucket key built from the naive
     ``ts_local`` merges the two 01:00–01:59 local hours. The corpus draws
     different watts in each of them so the merge is visible, and sentinel tests
     assert the *broken* forms really do produce the wrong answer — otherwise the
     assertions on the fixed forms would be vacuous.

Nothing here checks prose. Sentences are free to change.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.main import get_command

from energy_capture import cli, model, timeutil
from energy_capture.sources import bryant
from energy_capture.stages import dim, rollup

from tests.conftest import LOCAL_TZ

README = Path(__file__).resolve().parent.parent / "README.md"


@pytest.fixture(scope="module")
def readme() -> str:
    return README.read_text(encoding="utf-8")


# ------------------------------------------------------------- enum decodes


def test_the_readme_enum_table_matches_bryant_integer_for_integer(readme: str) -> None:
    """``| `mode` | `system` | `0` = off, `1` = heat, … |`` — parsed, not eyeballed.

    A renumbered or dropped entry here would tell a reader that ``2`` means
    something it does not, and the rows are already archived.
    """
    documented: dict[str, dict[int, str]] = {}
    for line in readme.splitlines():
        # Generated from bryant.ENUM_TABLES: a new enum metric must appear in
        # the README table, and this regex must not be the thing that hides it.
        roster = "|".join(re.escape(m) for m in bryant.ENUM_TABLES)
        match = re.match(rf"^\|\s*`({roster})`\s*\|", line)
        if not match:
            continue
        pairs = re.findall(r"`(\d+)`\s*=\s*([a-z]+)", line)
        assert pairs, f"no decode pairs parsed out of: {line}"
        documented[match.group(1)] = {int(code): name for code, name in pairs}

    assert set(documented) == set(bryant.ENUM_TABLES), "a metric's row is missing"
    for metric, table in bryant.ENUM_TABLES.items():
        expected = {code: name for name, code in table.items()}
        assert documented[metric] == expected, metric


def test_the_readme_sql_decodes_match_bryant_integer_for_integer(readme: str) -> None:
    """The ``LEFT JOIN (VALUES (0,'off'),…)`` blocks a reader copies and runs.

    These are the decode that actually reaches a query result, so they matter
    more than the prose table above.
    """
    blocks = re.findall(r"\(VALUES\s+((?:\(\d+,'[a-z]+'\),?\s*)+)\)", readme)
    assert blocks, "no VALUES decode blocks found in the README"

    tables_by_size = {
        frozenset((code, name) for name, code in table.items()): metric
        for metric, table in bryant.ENUM_TABLES.items()
    }
    seen: set[str] = set()
    for block in blocks:
        pairs = frozenset(
            (int(code), name) for code, name in re.findall(r"\((\d+),'([a-z]+)'\)", block)
        )
        metric = tables_by_size.get(pairs)
        assert metric is not None, (
            "a VALUES decode in the README matches no table in sources/bryant.py: "
            f"{sorted(pairs)}"
        )
        seen.add(metric)
    # `mode` and `stage` are the two the queries decode; `fan` is per-zone and is
    # only documented in the prose table. Whichever appear must be exact.
    assert {"mode", "stage"} <= seen


def test_the_glue_comment_and_the_readme_quote_the_same_decode(readme: str) -> None:
    """One decode string, two documents (PLAN.md §12, CLAUDE.md "Conventions")."""
    from energy_capture.aws import glue

    # The decode moved to dim_channel's description when six enum metrics
    # overflowed the 255-character column limit (DEVIATIONS.md #174). The seam
    # this test guards is unchanged: ONE decode string, quoted identically by the
    # catalog and the README.
    comment = _spec(glue.TABLE_DIM_CHANNEL).description
    raw_value = next(
        column["Comment"]
        for column in glue.table_input(
            _spec(glue.TABLE_ENERGY_RAW_30S), "example-bucket"
        )["StorageDescriptor"]["Columns"]
        if column["Name"] == "value"
    )
    assert "dim_channel" in raw_value, "raw_30s.value must point at the decode"
    for metric, table in bryant.ENUM_TABLES.items():
        decode = bryant.enum_decode_text(metric).replace(", ", ",")
        assert decode in comment, f"{metric} decode is not in the Glue catalog"
        for name, code in table.items():
            assert f"{code}={name}" in decode


# ----------------------------------------------------------- the CLI surface


def test_every_cli_command_is_documented(readme: str) -> None:
    """A command nobody documented is a command nobody runs."""
    commands = set(get_command(cli.app).commands)  # type: ignore[attr-defined]
    assert {"import-greenbutton", "compare-meter"} <= commands
    for command in commands:
        assert f"`energycap {command}" in readme, f"{command} is undocumented"


def test_the_readme_documents_no_command_that_does_not_exist(readme: str) -> None:
    """The opposite failure: an invented command an operator would try to run."""
    commands = set(get_command(cli.app).commands)  # type: ignore[attr-defined]
    known = commands | {"--help", "--version"}
    for mentioned in set(re.findall(r"`energycap ([a-z][a-z-]*)", readme)):
        assert mentioned in known, f"README documents `energycap {mentioned}`, which does not exist"


# --------------------------------------------------------- the load-bearing math


def test_the_readme_states_the_observed_time_kwh_formula(readme: str) -> None:
    """CLAUDE.md rule 5. Re-deriving this wrong extrapolates across gaps."""
    normalized = re.sub(r"[`\s]", "", readme).lower().replace("×", "*")
    assert "mean_watts*(sample_count*poll_interval_s)/3.6e6" in normalized


def test_the_readme_states_the_canonical_dedupe_key(readme: str) -> None:
    """CLAUDE.md rule 7 — the key every stage dedupes on."""
    normalized = re.sub(r"[`*\s]", "", readme)
    assert "(" + ",".join(model.DEDUPE_KEY) + ")" in normalized


# ------------------------------------------------------- the closed vocabularies


def _vocabulary_cell(readme: str, column: str) -> str:
    """The third cell of the schema table's row for ``column``."""
    for line in readme.splitlines():
        if line.startswith(f"| `{column}` | string |"):
            return line.split("|")[3]
    raise AssertionError(f"no schema-table row for `{column}` in the README")


def test_the_readme_metric_vocabulary_is_the_whole_vocabulary(readme: str) -> None:
    """An omitted metric is a reader's ``WHERE metric IN (...)`` dropping real rows.

    ``blower_rpm`` and ``cfm`` are emitted by ``sources/bryant.py`` today;
    ``kwh_interval`` / ``ccf_interval`` are defined for the designed-but-unbuilt
    ``energy/meter`` dataset. All four belong on the list.
    """
    listed = set(re.findall(r"`([a-z][a-z0-9_]*)`", _vocabulary_cell(readme, "metric")))
    assert listed == set(model.METRICS)


def test_the_readme_unit_vocabulary_is_the_whole_vocabulary(readme: str) -> None:
    """Same failure, one column over: ``rpm``, ``CFM`` and ``CCF`` are real units."""
    listed = set(re.findall(r"`([A-Za-z]+)`", _vocabulary_cell(readme, "unit")))
    assert listed == set(model.UNITS)


# ------------------------------------------------ the README <-> Glue seam
#
# The README and the Glue comments are the same claims made twice, to two
# different readers, and they have drifted apart before. Everything below pins
# the *seam*: not "the README is right" and not "the catalog is right", but that
# the two say the same thing and that both still say what the code does. A
# reader who consults one and acts on the other must not get a different answer.


def _spec(name: str) -> Any:
    from energy_capture.aws import glue

    return next(spec for spec in glue.table_specs() if spec.name == name)


def _rendered_column_comment(table: str, column: str) -> str:
    """The comment as it reaches the catalog — length-checked and collapsed."""
    from energy_capture.aws import glue

    rendered = glue.table_input(_spec(table), "example-bucket")
    return next(
        entry["Comment"]
        for entry in rendered["StorageDescriptor"]["Columns"]
        if entry["Name"] == column
    )


def _tables_with_column(column: str) -> list[str]:
    from energy_capture.aws import glue

    return [spec.name for spec in glue.table_specs() if column in spec.schema.names]


def _named_in_glue(column: str, vocabulary: set[str]) -> set[str]:
    """Every member of ``vocabulary`` any table's ``column`` comment names."""
    named: set[str] = set()
    for table in _tables_with_column(column):
        comment = _rendered_column_comment(table, column)
        named |= {word for word in re.findall(r"[A-Za-z][A-Za-z0-9_]*", comment) if word in vocabulary}
    return named


def _metrics_no_table_can_hold() -> set[str]:
    """Metrics the model defines that no Glue table can actually carry.

    ``kwh_interval``/``ccf_interval`` today: PLAN.md §13 designs ``energy_meter``
    and deliberately does not build it. Derived from ``glue``'s own per-table
    reachability so that building that table makes this set shrink by itself.
    """
    from energy_capture.aws import glue

    reachable = {
        metric
        for spec in glue.table_specs()
        for metric in glue._metrics_for_table(spec.name)
    }
    return set(model.METRICS) - reachable


def test_the_readme_and_the_glue_comments_name_the_same_metrics(readme: str) -> None:
    """Both documents present ``metric`` as a CLOSED list, so they must match.

    A reader writes ``WHERE metric IN (...)`` out of whichever one they happened
    to read. The only permitted difference is a metric no table can hold yet, and
    the README has to say so in the same breath as it lists it.
    """
    readme_metrics = set(
        re.findall(r"`([a-z][a-z0-9_]*)`", _vocabulary_cell(readme, "metric"))
    )
    glue_metrics = _named_in_glue("metric", set(model.METRICS))

    assert glue_metrics <= readme_metrics, (
        "the Glue metric comments name metrics the README omits: "
        f"{sorted(glue_metrics - readme_metrics)}"
    )
    # The Glue comment stopped being a closed list at 28 metrics — the raw_30s
    # names alone are 251 of the 255 characters allowed (DEVIATIONS.md #174). The
    # README still is one, and it has to stay the superset. What the catalog owes
    # a reader instead is the traps plus a way to enumerate.
    uncollected = _metrics_no_table_can_hold()
    assert not (uncollected & glue_metrics), (
        "the catalog names a metric no table can hold: "
        f"{sorted(uncollected & glue_metrics)}"
    )
    assert uncollected <= readme_metrics
    traps = {"watts", bryant.STAGE_METRIC, bryant.STAGE_PCT_METRIC} | set(
        model.DAY_GRAIN_METRICS
    )
    assert traps <= glue_metrics, (
        "the catalog stopped naming a metric a reader gets wrong: "
        f"{sorted(traps - glue_metrics)}"
    )

    _, _, tail = _vocabulary_cell(readme, "metric").partition("designed but not yet collected")
    assert set(re.findall(r"`([a-z][a-z0-9_]*)`", tail)) == uncollected, (
        "the README lists a metric no table can hold without flagging it as "
        "designed-but-not-collected (or flags one that is now collected)"
    )


def test_the_readme_and_the_glue_comments_name_the_same_units(readme: str) -> None:
    """Same seam, one column over — and the same failure mode, ``WHERE unit =``."""
    readme_units = set(re.findall(r"`([A-Za-z]+)`", _vocabulary_cell(readme, "unit")))
    glue_units = _named_in_glue("unit", set(model.UNITS))

    assert glue_units <= readme_units, (
        f"the Glue unit comments name units the README omits: {sorted(glue_units - readme_units)}"
    )
    uncollected = _metrics_no_table_can_hold()
    only_uncollected = {model.unit_for_metric(metric) for metric in uncollected} - {
        model.unit_for_metric(metric) for metric in set(model.METRICS) - uncollected
    }
    assert readme_units - glue_units == only_uncollected


def test_the_enum_decode_reaches_the_hourly_table_comment_too(readme: str) -> None:
    """``energy_hourly`` has no ``value`` column, and it aggregates enum rows.

    So the decode cannot live on a column there; it lives in the table comment,
    and the README says that is where it lives. Same source of truth as the
    ``energy_raw_30s`` quotation above — ``sources/bryant.py``, integer for integer.
    """
    from energy_capture.aws import glue

    description = _spec(glue.TABLE_ENERGY_HOURLY).description
    assert "value" not in _spec(glue.TABLE_ENERGY_HOURLY).schema.names
    # The decode outgrew a 255-character column comment at six enum metrics and
    # now lives once, in dim_channel's description (DEVIATIONS.md #174). What
    # energy_hourly owes a reader is the pointer — and the pointer has to resolve
    # to every code, integer for integer, or moving it lost something.
    assert "dim_channel" in description
    published = _spec(glue.TABLE_DIM_CHANNEL).description
    for metric, table in bryant.ENUM_TABLES.items():
        decode = bryant.enum_decode_text(metric).replace(", ", ",")
        assert decode in published, f"{metric} decode is missing from the catalog"
        for name, code in table.items():
            assert f"{code}={name}" in decode
    assert "energy_hourly" in readme and "table** comment" in readme


def test_the_readme_and_the_hourly_comment_give_the_same_enum_warning(readme: str) -> None:
    """The rollup carries enum rows, so ``mean``/``p95`` there are arithmetic on labels.

    Whichever document a reader meets first has to stop them writing that AVG().
    """
    from energy_capture.aws import glue

    description = _spec(glue.TABLE_ENERGY_HOURLY).description
    assert glue.ENUM_ROLLUP_WARNING in description

    section = readme.split("### The hourly rollup", 1)[1].split("\n### ", 1)[0]
    lowered = section.lower()
    assert "meaningless" in lowered
    for column in ("mean", "p95"):
        assert f"`{column}`" in section
    # ... and that min/max/sample_count are the ones that DO survive the rollup,
    # which is the other half of the catalog's claim.
    for column in ("min", "max", "sample_count"):
        assert f"`{column}`" in section
    assert "enum" in lowered


# ------------------------- stage vs stage_pct: one field, two metrics, one house
#
# `odu.opstat` renders as EITHER `stage` (enum code) or `stage_pct` (0-100
# capacity percentage) — never both, and the hardware decides. This house is
# variable-capacity, so `WHERE metric = 'stage'` matches nothing, for all time.
# Both metrics are in the vocabulary lists, so the lists cannot express that;
# only prose can, and only if the prose says the same thing in both documents and
# stays tied to what the live capture actually showed.

_VARCAP_FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "bryant" / "status_varcap.json"
)


def _observed_odu() -> dict[str, str]:
    """The outdoor unit of the committed live capture — the evidence for the docs."""
    import json

    return json.loads(_VARCAP_FIXTURE.read_text())["data"]["infinityStatus"]["odu"]


def _readme_section(readme: str, heading: str) -> str:
    """One ``##`` section of the README, heading included."""
    assert heading in readme, f"the README no longer has a {heading!r} section"
    body = readme.split(heading, 1)[1]
    return heading + body.split("\n## ", 1)[0]


def test_the_readme_documents_the_rendering_the_live_capture_showed(readme: str) -> None:
    """Which metric THIS system emits is a fact about one response, so check it.

    The README states an outcome (``stage_pct``, never ``stage``) that is only
    true because ``odu.type``/``odu.opstat`` came back the way they did. Pinning
    the prose to the capture means a replaced outdoor unit — new fixture, new
    rendering — makes the documentation fail rather than quietly mislead.
    """
    odu = _observed_odu()
    emitted = bryant.stage_metric_for(odu["opstat"])
    assert emitted == bryant.STAGE_PCT_METRIC
    absent = bryant.STAGE_METRIC

    section = _readme_section(readme, "## Compressor stage")
    assert f"`{emitted}`" in section and f"`{absent}`" in section
    assert odu["type"] in section, "the README does not name the odu.type observed"
    assert odu["opstat"] in section, "the README does not quote the observed opstat"
    assert "variable-capacity" in section.lower()
    assert "never" in section.lower(), (
        "the README names both renderings without saying the other one never "
        "appears here — which is the whole trap"
    )


def test_the_readme_and_the_glue_comments_tell_the_same_stage_story(readme: str) -> None:
    """Same claim, two documents; a reader consults one and acts on the other."""
    from energy_capture.aws import glue

    for table in (glue.TABLE_ENERGY_RAW_30S, glue.TABLE_ENERGY_HOURLY):
        assert glue.STAGE_REPRESENTATION_NOTE in _spec(table).description

    lowered = readme.lower()
    # Was "mutually exclusive" until DEVIATIONS.md #179: the renderings are not
    # exclusive per system, only per reading, and the README now says so. Both
    # documents must carry the per-reading mechanism, not the old claim.
    assert "per reading" in lowered
    assert glue.ODU_TYPE_OBSERVED in readme
    assert bryant.STAGE_PCT_METRIC in readme and bryant.STAGE_METRIC in readme
    # Both documents have to land the cardinal rule, not just the mechanism.
    assert "absence" in lowered and "not zero" in lowered


def test_no_example_query_asks_for_one_stage_rendering_alone(readme: str) -> None:
    """An example that selects only ``stage`` returns an empty column here.

    Whichever rendering a reader's system uses, an example that names one and
    not the other silently answers "the compressor never ran" on half the
    hardware in the world — and on this house specifically.
    """
    offenders = [
        lineno
        for lineno, section, sql in _sql_blocks(readme)
        if section in ("duckdb", "athena")
        and f"'{bryant.STAGE_METRIC}'" in sql
        and f"'{bryant.STAGE_PCT_METRIC}'" not in sql
    ]
    assert not offenders, (
        f"README:{offenders} filter on '{bryant.STAGE_METRIC}' without also "
        f"selecting '{bryant.STAGE_PCT_METRIC}'"
    )


def test_the_correlation_query_returns_the_rendering_this_system_emits(
    readme: str, corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """Query 4, run against a corpus shaped like the real house.

    The corpus emits ``stage_pct`` and no ``stage`` row, exactly as the live
    system does. So the query's ``stage`` column comes back NULL in every row —
    that is the trap, reproduced — while ``stage_pct`` carries the compressor
    signal. If somebody ever "simplifies" the query back to ``stage`` only, this
    test reports a column of nothing instead of shrugging.
    """
    rows = _rows(con, _localise(_query_containing(readme, "hvac_instants"), corpus))
    assert rows
    assert all(row["stage"] is None for row in rows), (
        "a corpus with no 'stage' rows produced a non-null stage column"
    )
    assert {row["stage_pct"] for row in rows} == {VARCAP_STAGE_PCT}


def test_the_odu_opstat_question_is_no_longer_listed_as_unproven(readme: str) -> None:
    """It was the highest-risk open item; a live run answered it.

    Leaving it on the unproven list would send the next reader looking for an
    answer that is already in the repo — and the answer changes which metric
    they query.
    """
    settled = _readme_section(readme, "## Settled by the first live run")
    odu = _observed_odu()
    assert "odu.opstat" in settled
    assert odu["type"] in settled and bryant.STAGE_PCT_METRIC in settled

    unproven = _readme_section(readme, "## Known-unproven")
    still_open = re.search(
        r"odu\.opstat[^.]*\b(?:if|whether|unknown|capacity percentage)\b",
        unproven,
        re.IGNORECASE,
    )
    assert not still_open, (
        "the Known-unproven list still presents odu.opstat as an open question: "
        f"{still_open.group(0) if still_open else ''}"
    )


def test_the_readme_and_the_raw_table_comment_agree_about_a_day_being_compacted(
    readme: str,
) -> None:
    """DEVIATIONS #35's window, told the same way in both documents.

    Both used to state an absolute — "always exactly one authoritative copy",
    "no query ever needs DISTINCT" — and a reader who believed either would
    double-count a day mid-compaction. Both now qualify the dedupe claim the same
    way, name the same window, the same tell and the same remedy.
    """
    from energy_capture.aws import glue

    description = _spec(glue.TABLE_ENERGY_RAW_30S).description
    for document, what in ((readme, "README"), (description, "energy_raw_30s comment")):
        assert "settled partition" in document, f"{what} still makes DISTINCT an absolute"
        assert "in flight" in document, f"{what} omits the in-flight window"
        assert "count twice" in document, f"{what} omits the consequence"
        assert "energycap compact-daily --start D --end D" in document, (
            f"{what} omits the remedy"
        )
        assert "dedupe that day" in document, f"{what} omits the interim workaround"
    assert "no query ever needs" not in readme


# ------------------------------------------------------ the SQL blocks, statically

#: Section headings that bound the two query dialects.
_DUCKDB_HEADING = "## Querying with DuckDB"
_ATHENA_HEADING = "## The same queries in Athena"
_AFTER_QUERIES_HEADING = "## Enum decodes"


def _sql_blocks(readme: str) -> list[tuple[int, str, str]]:
    """``(line_number, section, sql)`` for every fenced ```sql block."""
    lines = readme.splitlines()
    bounds = {
        heading: next(i for i, line in enumerate(lines) if line.startswith(heading))
        for heading in (_DUCKDB_HEADING, _ATHENA_HEADING, _AFTER_QUERIES_HEADING)
    }
    out: list[tuple[int, str, str]] = []
    index = 0
    while index < len(lines):
        opening = re.match(r"^\s*```(\w*)\s*$", lines[index])
        if not opening:
            index += 1
            continue
        start = index + 1
        end = start
        while end < len(lines) and not lines[end].lstrip().startswith("```"):
            end += 1
        if opening.group(1) == "sql":
            if bounds[_DUCKDB_HEADING] < start < bounds[_ATHENA_HEADING]:
                section = "duckdb"
            elif bounds[_ATHENA_HEADING] < start < bounds[_AFTER_QUERIES_HEADING]:
                section = "athena"
            else:
                section = "other"
            out.append((start + 1, section, "\n".join(lines[start:end])))
        index = end + 1
    return out


def test_the_readme_has_query_examples_in_both_dialects(readme: str) -> None:
    """Guards the extractor itself: the tests below are only worth anything if it
    actually finds the blocks (PLAN.md §12 asks for 4–6 real examples, twice)."""
    sections = [section for _, section, _ in _sql_blocks(readme)]
    assert sections.count("duckdb") >= 6
    assert sections.count("athena") >= 6


def test_no_example_query_buckets_on_the_naive_wall_clock(readme: str) -> None:
    """CLAUDE.md rule 3: ``ts_utc`` is canonical for every bucket key.

    A five-minute bucket cut from ``ts_local`` merges the two 01:00–01:59 local
    hours of the November fall-back Sunday: watts get averaged across two
    physically different hours and the sample counts double. Truncating or
    extracting from ``ts_local`` is how that mistake is spelled, in either
    dialect. (Grouping the *label* column ``local_hour_start`` to a DATE is fine
    — a day is a day in both offsets — so only ``ts_local`` is policed here.)
    """
    forbidden = re.compile(
        r"""(?:
              time_bucket\s*\([^)]*ts_local
            | date_trunc\s*\(\s*'(?:hour|minute|second)'\s*,\s*[\w.]*ts_local
            | \b(?:minute|hour|second)\s*\(\s*[\w.]*ts_local\s*\)
            | to_unixtime\s*\(\s*[\w.]*ts_local
            | epoch\s*\(\s*[\w.]*ts_local
        )""",
        re.VERBOSE,
    )
    offenders = [
        (lineno, match.group(0))
        for lineno, _section, sql in _sql_blocks(readme)
        for match in forbidden.finditer(sql)
    ]
    assert not offenders, f"README buckets on the naive wall clock at {offenders}"


def test_no_example_query_hardcodes_the_length_of_a_local_day(readme: str) -> None:
    """A local day is 23 hours in March and 25 in November.

    ``samples_expected`` is the entire point of the coverage queries, so it must
    be derived from real elapsed time. Per-*hour* expectations (``3600 / 30``)
    are exempt: an hourly rollup bucket is keyed on ``hour_start_utc`` and is
    always exactly one real hour.
    """
    offenders = [
        (lineno, match.group(0))
        for lineno, _section, sql in _sql_blocks(readme)
        for match in re.finditer(r"\(\s*\d{1,2}\s*\*\s*3600\s*\)|\b2880\b", sql)
    ]
    assert not offenders, f"README hardcodes a day length at {offenders}"


def test_the_readme_names_the_setting_behind_the_literal_30(readme: str) -> None:
    """The ``30`` in every ``samples_expected`` and kWh literal is a *setting*."""
    assert "POLL_INTERVAL_S" in readme
    assert readme.count("POLL_INTERVAL_S") >= 4


# ------------------------------------------------- the SQL blocks, actually run
#
# Everything below builds a small S3-shaped tree of Parquet on local disk, using
# this package's own writers, and runs the README's DuckDB queries against it
# with the bucket URI rewritten. Offline by construction: no httpfs, no AWS.

_BUCKET_URI = "s3://my-energy-bucket/"

#: Two hubs. Everything about defect-class #1 depends on them sharing channel ids.
HUB_A = "4C45565275C6"
HUB_B = "4C4556527AB1"
SERIAL = "4022W200213"

#: Local days the corpus covers. 2026-03-08 is 23 hours long, 2026-11-01 is 25.
NORMAL_DAYS = (date(2026, 8, 14), date(2026, 8, 15))
SPRING_FORWARD = date(2026, 3, 8)
FALL_BACK = date(2026, 11, 1)

#: The two distinct 01:00–01:59 local hours of the fall-back Sunday, in UTC.
_FIRST_0100_UTC = datetime(2026, 11, 1, 5, tzinfo=timezone.utc)
_SECOND_0100_UTC = datetime(2026, 11, 1, 6, tzinfo=timezone.utc)
_AFTER_0100_UTC = datetime(2026, 11, 1, 7, tzinfo=timezone.utc)

#: Watts each hub's ``ct_1_a`` draws, and what they become in the repeated hour.
WATTS = {HUB_A: 1030.0, HUB_B: 520.0}
FALL_BACK_WATTS = {
    "first": {HUB_A: 1000.0, HUB_B: 500.0},
    "second": {HUB_A: 2000.0, HUB_B: 1500.0},
}

#: One hour of hub A's ``breaker_p10`` is missing outright, and the next hour is
#: only a third observed — so the two gap queries have something honest to find.
_MISSING_HOUR_UTC = datetime(2026, 8, 14, 7, tzinfo=timezone.utc)
_SHORT_HOUR_UTC = datetime(2026, 8, 14, 8, tzinfo=timezone.utc)


def _obs(ts: datetime, source: str, device: str, channel: str, metric: str, value: float):
    return model.make_observation(
        ts_utc=ts,
        source=source,
        device_id=device,
        channel_id=channel,
        metric=metric,
        value=value,
    )


def _leviton_day(local_day: date) -> list[model.Observation]:
    start, end = timeutil.local_day_bounds_utc(local_day, tz=LOCAL_TZ)
    rows: list[model.Observation] = []
    ts = start
    while ts < end:
        watts = WATTS
        if _FIRST_0100_UTC <= ts < _SECOND_0100_UTC:
            watts = FALL_BACK_WATTS["first"]
        elif _SECOND_0100_UTC <= ts < _AFTER_0100_UTC:
            watts = FALL_BACK_WATTS["second"]
        for hub in (HUB_A, HUB_B):
            rows.append(_obs(ts, "leviton", hub, "ct_1_a", "watts", watts[hub]))
        rows.append(_obs(ts, "leviton", HUB_B, "breaker_p10", "watts", 100.0))
        rows.append(_obs(ts, "leviton", HUB_A, "panel_leg_a", "volts", 121.0))
        missing = _MISSING_HOUR_UTC <= ts < _MISSING_HOUR_UTC + timedelta(hours=1)
        short = _SHORT_HOUR_UTC <= ts < _SHORT_HOUR_UTC + timedelta(hours=1)
        if not missing and not (short and ts.minute >= 20):
            rows.append(_obs(ts, "leviton", HUB_A, "breaker_p10", "watts", 300.0))
        ts += timedelta(seconds=30)
    return rows


#: The compressor capacity this corpus reports, matching the live capture's
#: ``odu.opstat = "35"``. See ``_bryant_status``.
VARCAP_STAGE_PCT = 35.0


def _bryant_status(local_day: date, first_hour: int, last_hour: int) -> list[model.Observation]:
    """System + zone status for part of a local day (query 4's window).

    The system channel emits ``stage_pct`` and **no ``stage`` row**, because that
    is what this house's outdoor unit does: it is variable-capacity
    (``odu.type = gs3ngiphp``), so ``odu.opstat`` is a capacity percentage and
    the enum rendering never occurs (DEVIATIONS.md #59/#75.1). Building the
    corpus the other way round would let an example query that only asks for
    ``stage`` look correct here while returning an empty column against the real
    bucket — the exact trap the README now warns about.
    """
    start, _ = timeutil.local_day_bounds_utc(local_day, tz=LOCAL_TZ)
    rows: list[model.Observation] = []
    for tick in range(first_hour * 120, last_hour * 120):
        ts = start + timedelta(seconds=30 * tick)
        rows.append(_obs(ts, "bryant", SERIAL, "system", "mode", 2.0))
        rows.append(_obs(ts, "bryant", SERIAL, "system", "stage_pct", VARCAP_STAGE_PCT))
        rows.append(_obs(ts, "bryant", SERIAL, "system", "outdoor_temp_f", 88.0 + tick % 5))
        rows.append(_obs(ts, "bryant", SERIAL, "system", "blower_rpm", 900.0))
        rows.append(_obs(ts, "bryant", SERIAL, "system", "cfm", 1150.0))
        rows.append(_obs(ts, "bryant", SERIAL, "zone_1", "indoor_temp_f", 72.0))
        rows.append(_obs(ts, "bryant", SERIAL, "zone_1", "setpoint_cool_f", 71.0))
    return rows


def _daily_rows() -> list[model.Observation]:
    rows: list[model.Observation] = []
    for day in (date(2026, 8, 14), date(2026, 8, 15), date(2026, 8, 16)):
        midnight_utc, _ = timeutil.local_day_bounds_utc(day, tz=LOCAL_TZ)
        for offset, component in enumerate(("cooling", "hpheat", "fan", "eheat")):
            rows.append(
                _obs(midnight_utc, "bryant", SERIAL, component, "kwh_day", 5.0 + offset)
            )
            rows.append(
                _obs(midnight_utc, "bryant", SERIAL, component, "cost_day_usd", 0.6 + offset)
            )
    return rows


def _dim_table() -> pa.Table:
    """A ``dim_channel`` in ``DIM_SCHEMA``, mirroring the real coverage problem.

    Both hubs' ``ct_1_a`` are mapped — and a human gave them the *same*
    ``short_label``, which is realistic and is why a label is not an identity.
    ``breaker_p10`` is unmapped on both hubs, which is the state every Leviton
    channel is actually in today (README "Known-unproven" #5).
    """
    updated = datetime(2026, 8, 16, 12, tzinfo=timezone.utc)
    records = [
        ("leviton", HUB_A, "ct_1_a", "HVAC subpanel feeder (leg A)", "HVAC feeder",
         "A", "6,8", "hvac", None, "critical", 3500.0, "A-6-8"),
        ("leviton", HUB_B, "ct_1_a", "HVAC subpanel feeder (leg A)", "HVAC feeder",
         "B", "6,8", "hvac", None, "critical", 3500.0, "B-6-8"),
        ("bryant", SERIAL, "cooling", "Heat pump — cooling", "Cooling",
         None, None, "hvac", None, None, None, None),
        ("bryant", SERIAL, "hpheat", "Heat pump — heating", "Heat pump",
         None, None, "hvac", None, None, None, None),
        ("bryant", SERIAL, "fan", "Air handler fan", "Fan",
         None, None, "hvac", None, None, None, None),
        ("bryant", SERIAL, "eheat", "Electric strips", "Strips",
         None, None, "hvac", None, None, None, None),
    ]
    # The tuples above cover every column except the two trailing ones set
    # below: `primary` (#178) and `updated_at`. None of these fixtures is the
    # primary meter -- they are breakers and Bryant components.
    columns = dict(zip(dim.DIM_COLUMNS[:-2], zip(*records), strict=True))
    data = {name: list(values) for name, values in columns.items()}
    data["is_primary"] = [False] * len(records)
    data["updated_at"] = [updated] * len(records)
    return pa.table(data, schema=dim.DIM_SCHEMA)


def _write(table: pa.Table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
    return path


def _build_corpus(root: Path) -> Path:
    raw_paths: dict[date, Path] = {}
    for day in (*NORMAL_DAYS, SPRING_FORWARD, FALL_BACK):
        rows = _leviton_day(day)
        if day == date(2026, 8, 15):
            rows += _bryant_status(day, first_hour=12, last_hour=19)
        table = model.observations_to_table(rows, dataset=model.Dataset.RAW_30S)
        raw_paths[day] = _write(
            table,
            root / f"energy/raw_30s/year={day.year}/month={day.month:02d}"
                   f"/day={day.day:02d}/day-{day:%Y%m%d}.parquet",
        )
    for day, source_path in raw_paths.items():
        hourly = rollup.rollup_day(
            day, [str(source_path)], poll_interval_s=30, tz=LOCAL_TZ
        )
        _write(
            hourly,
            root / f"energy/hourly/year={day.year}/month={day.month:02d}"
                   f"/rollup-{day:%Y%m%d}.parquet",
        )
    _write(
        model.observations_to_table(_daily_rows(), dataset=model.Dataset.DAILY),
        root / "energy/daily/year=2026/bryant-202608.parquet",
    )
    _write(_dim_table(), root / "energy/dim_channel/dim_channel.parquet")
    return root


_CORPUS: Path | None = None


@pytest.fixture
def corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The local Parquet tree, built once for the module (it is a few seconds)."""
    global _CORPUS
    if _CORPUS is None:
        _CORPUS = _build_corpus(tmp_path_factory.mktemp("readme-bucket"))
    return _CORPUS


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """A DuckDB session configured exactly as the README's setup block says."""
    connection = duckdb.connect(config={"threads": 1})
    connection.execute("SET TimeZone = 'UTC'")
    try:
        yield connection
    finally:
        connection.close()


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    """Run ``sql`` and return rows as dicts.

    Materialised through Arrow rather than ``fetchall()`` because DuckDB's
    conversion of ``TIMESTAMPTZ`` to Python wants ``pytz``, which this project
    does not depend on.
    """
    result = con.execute(sql)
    fetch = getattr(result, "to_arrow_table", None) or result.fetch_arrow_table
    return fetch().to_pylist()


def _localise(sql: str, root: Path) -> str:
    return sql.replace(_BUCKET_URI, f"{root}/")


def _duckdb_queries(readme: str) -> list[tuple[int, str]]:
    """Every runnable DuckDB example (the httpfs session setup is not one)."""
    return [
        (lineno, sql)
        for lineno, section, sql in _sql_blocks(readme)
        if section == "duckdb" and "INSTALL httpfs" not in sql
    ]


def _query_containing(readme: str, needle: str) -> str:
    matches = [sql for _, sql in _duckdb_queries(readme) if needle in sql]
    assert len(matches) == 1, f"expected exactly one DuckDB query containing {needle!r}"
    return matches[0]


def test_every_duckdb_example_query_actually_runs(
    readme: str, corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """PLAN.md §12 wants *real* queries. A query that does not parse is not real."""
    queries = _duckdb_queries(readme)
    assert len(queries) >= 6
    for lineno, sql in queries:
        try:
            rows = _rows(con, _localise(sql, corpus))
        except duckdb.Error as exc:  # pragma: no cover - the failure message is the point
            pytest.fail(f"README:{lineno} does not run: {exc}")
        assert rows, f"README:{lineno} returned no rows against the corpus"


def test_the_afternoon_summary_reports_each_physical_channel_separately(
    readme: str, corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """Query 1's summary: two hubs, one shared ``channel_id``, one shared label.

    Grouped on the label alone this collapses to a single row of 1440 samples
    against 720 expected and a mean of 775 W — the mean of two circuits, which is
    the wattage of neither.
    """
    rows = _rows(con, _localise(_query_containing(readme, "kwh_observed DESC"), corpus))
    by_device = {row["device_id"]: row for row in rows}
    assert set(by_device) == {HUB_A, HUB_B}
    for hub, watts in WATTS.items():
        row = by_device[hub]
        assert row["channel_id"] == "ct_1_a"
        assert row["samples"] == 720
        assert row["samples_expected"] == 720
        assert row["mean_watts"] == pytest.approx(watts)
        # 6 observed hours at a constant load: watts * 21600 s / 3.6e6.
        assert row["kwh_observed"] == pytest.approx(watts * 6 / 1000, abs=0.005)


def test_the_daily_coverage_query_separates_two_hubs_sharing_a_channel_id(
    readme: str, corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """Query 2's daily roll-up — the one an auditor caught reporting 37.2 kWh.

    24.72 + 12.48 = 37.20, and 2880 + 2880 = 5760 samples in a day that can hold
    2880. A doubled sample count is always the tell.
    """
    rows = [
        row
        for row in _rows(con, _localise(_query_containing(readme, "hours_present"), corpus))
        if row["local_day"] == date(2026, 8, 15) and row["channel_id"] == "ct_1_a"
    ]
    assert {row["device_id"] for row in rows} == {HUB_A, HUB_B}
    for row in rows:
        assert int(row["samples"]) == 2880
        assert int(row["samples_expected"]) == 2880
        assert row["hours_present"] == 24
    assert {round(row["kwh"], 2) for row in rows} == {24.72, 12.48}


@pytest.mark.parametrize(
    ("local_day", "month", "hours", "samples"),
    [
        (SPRING_FORWARD, "03", 23, 2760),
        (date(2026, 8, 15), "08", 24, 2880),
        (FALL_BACK, "11", 25, 3000),
    ],
)
def test_the_daily_coverage_query_expects_a_real_number_of_samples_across_dst(
    readme: str,
    corpus: Path,
    con: duckdb.DuckDBPyConnection,
    local_day: date,
    month: str,
    hours: int,
    samples: int,
) -> None:
    """PLAN.md §15.3, expressed as a query: 23-, 24- and 25-hour local days.

    The README's query is retargeted by substituting its partition glob and its
    two window literals — the arithmetic under test is untouched. A hardcoded
    ``(24 * 3600) / 30`` would report 2880 on all three days, which is wrong on
    exactly the two days this project most needs to be right about.
    """
    sql = (
        _query_containing(readme, "hours_present")
        .replace("month=08", f"month={month}")
        .replace("2026-08-10 00:00:00", f"{local_day:%Y-%m-%d} 00:00:00")
        .replace("2026-08-17 00:00:00", f"{local_day + timedelta(days=1):%Y-%m-%d} 00:00:00")
    )
    rows = [
        row
        for row in _rows(con, _localise(sql, corpus))
        if row["device_id"] == HUB_B and row["channel_id"] == "ct_1_a"
    ]
    assert len(rows) == 1
    assert rows[0]["hours_present"] == hours
    assert int(rows[0]["samples_expected"]) == samples
    assert int(rows[0]["samples"]) == samples


def test_the_correlation_query_buckets_the_fall_back_hours_apart(
    readme: str, corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """Query 4's bucket key, lifted out and pointed at the fall-back Sunday.

    Keyed on ``ts_utc`` the two 01:00 local hours stay 12 + 12 buckets of 10
    samples each, drawing 1000 W and 2000 W. Keyed on ``ts_local`` they would be
    12 buckets of 20 samples drawing the average of the two, 1500 W.
    """
    bucket_expr = re.search(
        r"time_bucket\(INTERVAL 5 MINUTE, (\w+\.)?ts_utc\)",
        _query_containing(readme, "hvac_instants"),
    )
    assert bucket_expr, "query 4 no longer buckets on ts_utc"
    raw = f"{corpus}/energy/raw_30s/year=2026/month=11/day=01/*.parquet"
    rows = _rows(
        con,
        f"""
        SELECT time_bucket(INTERVAL 5 MINUTE, ts_utc) AS bucket,
               avg(value) AS watts, count(*) AS samples
        FROM read_parquet('{raw}')
        WHERE metric = 'watts' AND device_id = '{HUB_A}' AND channel_id = 'ct_1_a'
          AND ts_local >= TIMESTAMP '2026-11-01 01:00:00'
          AND ts_local <  TIMESTAMP '2026-11-01 02:00:00'
        GROUP BY 1 ORDER BY 1
        """,
    )
    assert len(rows) == 24, "the repeated local hour collapsed"
    assert {row["samples"] for row in rows} == {10}
    assert [row["watts"] for row in rows[:12]] == [1000.0] * 12
    assert [row["watts"] for row in rows[12:]] == [2000.0] * 12


# ------------------------------------------------------------------ sentinels
#
# The assertions above only mean something if the corpus can actually express the
# two failures. These pin that: run the *broken* forms and require the known-wrong
# answer. If one of them ever starts agreeing with the fixed form, the fixture has
# drifted and the tests above have quietly stopped testing anything.


def test_the_corpus_would_expose_a_group_by_on_channel_id_alone(
    corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """The defect, reproduced: 37.2 kWh and 5760 samples as one channel."""
    hourly = f"{corpus}/energy/hourly/year=2026/month=08/rollup-*.parquet"
    dim_path = f"{corpus}/energy/dim_channel/dim_channel.parquet"
    rows = _rows(
        con,
        f"""
        SELECT coalesce(d.short_label, h.channel_id) AS channel,
               round(sum(h.kwh), 2) AS kwh,
               sum(h.sample_count)  AS samples,
               count(*)             AS hours_present
        FROM read_parquet('{hourly}') AS h
        LEFT JOIN read_parquet('{dim_path}') AS d
               ON d.source = h.source AND d.device_id = h.device_id
              AND d.channel_id = h.channel_id
        WHERE h.metric = 'watts'
          AND CAST(h.local_hour_start AS DATE) = DATE '2026-08-15'
        GROUP BY 1
        """,
    )
    merged = next(row for row in rows if row["channel"] == "HVAC feeder")
    assert merged["kwh"] == pytest.approx(37.2)
    assert int(merged["samples"]) == 5760      # a day holds 2880
    assert merged["hours_present"] == 48       # a day holds 24


def test_the_corpus_would_expose_a_bucket_key_cut_from_ts_local(
    corpus: Path, con: duckdb.DuckDBPyConnection
) -> None:
    """The DST defect, reproduced: two real hours averaged into one set of buckets.

    ``date_trunc('hour', ts_local) + (minute(ts_local) // 5) * INTERVAL 5 MINUTE``
    is the DuckDB spelling of the Athena translation this README used to publish
    (Trino's ``/`` on integers is integer division, hence ``//`` here).
    """
    raw = f"{corpus}/energy/raw_30s/year=2026/month=11/day=01/*.parquet"
    rows = _rows(
        con,
        f"""
        SELECT date_trunc('hour', ts_local)
                 + ((minute(ts_local) // 5) * INTERVAL 5 MINUTE) AS bucket,
               avg(value) AS watts, count(*) AS samples
        FROM read_parquet('{raw}')
        WHERE metric = 'watts' AND device_id = '{HUB_A}' AND channel_id = 'ct_1_a'
          AND ts_local >= TIMESTAMP '2026-11-01 01:00:00'
          AND ts_local <  TIMESTAMP '2026-11-01 02:00:00'
        GROUP BY 1 ORDER BY 1
        """,
    )
    assert len(rows) == 12, "the two 01:00 hours did not merge — fixture drift"
    assert {row["samples"] for row in rows} == {20}
    assert {row["watts"] for row in rows} == {1500.0}   # the mean of 1000 and 2000
