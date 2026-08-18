"""``energycap`` — the command line surface (PLAN.md §5, §10, §16).

Two rules shape this module.

**1. Every stage is a standalone command over an arbitrary local date range.**
PLAN.md §5: the long-running ``energycap run`` process schedules the same code
the operator can invoke by hand. So every stage command takes ``--start`` and
``--end`` as **local** dates (America/Kentucky/Louisville — partitioning is on
the local date, CLAUDE.md rule 4), defaulting to that stage's scheduled window,
and every one of them is idempotent: deterministic output filenames mean a
re-run overwrites rather than duplicates (CLAUDE.md rule 7). "Re-run the rollup
over the range" is the documented fix for a collector bug, so it has to be
boring and safe.

**2. The CLI must load before the stages exist.** Stage modules are imported
*lazily, inside the command body*, and a command whose module has not landed
prints a clear "not implemented yet" line and exits non-zero instead of blowing
up with an ImportError. That keeps ``energycap --help`` honest during the build
order in PLAN.md §16 and keeps one broken stage from taking down the others.

The stage entry points and their call signatures are in
:data:`STAGE_ENTRYPOINTS` and :data:`STAGE_SIGNATURES` — that table is the
contract between this module and ``energy_capture.stages``.

Exit codes:

===  =========================================================================
0    success
1    the stage ran and failed (see the JSON log line; ``--traceback`` for more)
2    bad usage — unparseable date, inverted range, unknown option (click)
3    the stage is not implemented yet, or is a documented future feature (§13)
130  interrupted
===  =========================================================================
"""

import asyncio
import importlib
import inspect
import logging
import time
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any

import typer

from energy_capture import __version__
from energy_capture.config import Settings, get_settings
from energy_capture.logging import configure_logging, get_logger, scrub_text
from energy_capture.timeutil import local_date_of, now_utc, parse_local_date

__all__ = ["STAGE_ENTRYPOINTS", "app", "main"]

# --------------------------------------------------------------- exit codes

EXIT_STAGE_FAILED = 1
EXIT_USAGE = 2
#: The stage module/attribute is absent, or the feature is explicitly future
#: work (``import-greenbutton``, PLAN.md §13).
EXIT_NOT_IMPLEMENTED = 3
EXIT_INTERRUPTED = 130


# ------------------------------------------------------- the stage contract

#: ``command name -> (module, attribute)``. The CLI imports these lazily; a
#: missing module or attribute becomes a readable message, not a traceback.
STAGE_ENTRYPOINTS: dict[str, tuple[str, str]] = {
    # `run` is the process host, not a pipeline stage: it *drives* the stages
    # below (poll loops, scheduler, health server), so it lives at the package
    # root rather than under `stages/`.
    "run": ("energy_capture.runtime", "run"),
    "poll": ("energy_capture.stages.poller", "run"),
    "upload": ("energy_capture.stages.uploader", "run"),
    "compact-daily": ("energy_capture.stages.compactor", "run"),
    "rollup": ("energy_capture.stages.rollup", "run"),
    "fetch-daily": ("energy_capture.stages.daily", "run"),
    "backfill": ("energy_capture.stages.backfill", "run"),
    "discover": ("energy_capture.stages.discover", "run"),
    "build-dim": ("energy_capture.stages.dim", "build"),
    "create-glue-tables": ("energy_capture.aws.glue", "create_or_update_tables"),
    "import-greenbutton": ("energy_capture.stages.greenbutton", "run"),
    "fetch-greenbutton": ("energy_capture.stages.greenbutton_fetch", "run"),
    "greenbutton-authorize": ("energy_capture.stages.greenbutton_auth", "run"),
    "compare-meter": ("energy_capture.stages.compare", "run"),
}

#: The exact signature each entry point must accept, for whoever implements the
#: stages. All arguments are passed by keyword, so a stage may accept them in
#: any order and may add further keyword parameters *with defaults*. The return
#: value may be ``None``, an ``int`` row count, or a mapping of fields to fold
#: into the completion log line (e.g. ``{"rows": 4212, "files": 3}``). Sync or
#: ``async def`` are both fine — a coroutine is run with ``asyncio.run``.
STAGE_SIGNATURES: dict[str, str] = {
    "run": "async run() -> dict                             # blocks until SIGTERM",
    "poll": "async run(*, once: bool, sources: tuple[str, ...] | None)",
    "upload": "run(*, start: date, end: date)",
    "compact-daily": "run(*, start: date, end: date)",
    "rollup": "run(*, start: date, end: date)",
    "fetch-daily": "run(*, start: date, end: date)",
    "backfill": "run(*, start: date, end: date)",
    "discover": (
        "run(*, sources: tuple[str, ...] | None, map_path: Path, json_only: bool, "
        "dump_path: Path | None, raw: bool, out_path: Path | None, "
        "write_live_channels: bool)"
    ),
    "build-dim": (
        "build(*, map_path: Path, inventory_path: Path | None, "
        "live_channels_path: Path | None, dry_run: bool)"
    ),
    "create-glue-tables": "create_or_update_tables(*, database: str, dry_run: bool)",
    "import-greenbutton": (
        "run(*, path: Path, source: str, channel_id: str, out_dir: Path | None, "
        "assume_uom: str | None, interval_s: int | None, bucket: str | None, "
        "dry_run: bool)"
    ),
    "fetch-greenbutton": (
        "run(*, start: date, end: date, source: str, channel_id: str, "
        "out_dir: Path | None, bucket: str | None, dry_run: bool)"
    ),
    "greenbutton-authorize": "run(*, code: str | None, state: str | None)",
    "compare-meter": (
        "run(*, start: date, end: date, meter_dir: Path | None, "
        "channels: tuple[str, ...] | None, source: str, meter: str | None, "
        "min_coverage: float)"
    ),
}

#: Hand-maintained semantic layer, committed to the repo (PLAN.md §9).
DEFAULT_CHANNEL_MAP = Path("config/channel_map.json")


# ------------------------------------------------------------ shared options

StartOpt = Annotated[
    str | None,
    typer.Option(
        "--start",
        "-s",
        metavar="YYYY-MM-DD",
        help=(
            "First LOCAL date to process, inclusive. "
            "Defaults to this stage's scheduled window (see the description above)."
        ),
    ),
]
EndOpt = Annotated[
    str | None,
    typer.Option(
        "--end",
        "-e",
        metavar="YYYY-MM-DD",
        help=(
            "Last LOCAL date to process, inclusive. "
            "Defaults to --start when only --start was given."
        ),
    ),
]
SourceOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--source",
        help="Limit to one source (leviton, bryant). Repeatable; default is all.",
    ),
]
DryRunOpt = Annotated[
    bool,
    typer.Option("--dry-run", help="Compute and log the result without writing it."),
]


# ------------------------------------------------------------------- helpers


class CliState:
    """Per-invocation state: what the callback resolved for the commands."""

    __slots__ = ("settings", "log_level", "traceback")

    def __init__(self, settings: Settings, log_level: str, traceback: bool) -> None:
        self.settings = settings
        self.log_level = log_level
        self.traceback = traceback


#: Set by :func:`main` on every invocation. One CLI invocation per process, so a
#: module global is honest here; it is also stored on ``ctx.obj`` for commands
#: that would rather take a ``typer.Context``.
_STATE: CliState | None = None


def _state() -> CliState | None:
    return _STATE


def _today() -> date:
    """Today's LOCAL date — every window in PLAN.md §10 is local."""
    return local_date_of(now_utc())


def _days_ago(days: int) -> date:
    return _today() - timedelta(days=days)


def _parse_date(value: str | None, param: str) -> date | None:
    if value is None:
        return None
    try:
        return parse_local_date(value)
    except (ValueError, TypeError) as exc:
        raise typer.BadParameter(
            f"{value!r} is not a local date; use YYYY-MM-DD (e.g. 2026-08-16). {exc}",
            param_hint=param,
        ) from None


def _resolve_range(
    start: str | None,
    end: str | None,
    *,
    default_start: date,
    default_end: date,
) -> tuple[date, date]:
    """Parse ``--start/--end`` into an inclusive local date range.

    Both omitted -> the stage's scheduled window. One omitted -> it mirrors the
    other, so ``--start 2026-08-15`` means exactly that one local day.
    """
    parsed_start = _parse_date(start, "--start")
    parsed_end = _parse_date(end, "--end")
    if parsed_start is None and parsed_end is None:
        parsed_start, parsed_end = default_start, default_end
    elif parsed_start is None:
        parsed_start = parsed_end
    elif parsed_end is None:
        parsed_end = parsed_start
    assert parsed_start is not None and parsed_end is not None  # narrowing
    if parsed_end < parsed_start:
        raise typer.BadParameter(
            f"--end {parsed_end.isoformat()} is before --start {parsed_start.isoformat()}",
            param_hint="--end",
        )
    return parsed_start, parsed_end


def _echo_error(message: str) -> None:
    """Write a human-readable error to stderr (scrubbed, like the logs)."""
    typer.secho(scrub_text(message), err=True, fg=typer.colors.RED)


def _not_implemented(command: str, detail: str) -> typer.Exit:
    module, attr = STAGE_ENTRYPOINTS[command]
    _echo_error(
        f"energycap {command}: not implemented yet — {detail}\n"
        f"  expected: {module}.{attr}\n"
        f"  signature: {STAGE_SIGNATURES[command]}\n"
        "Stages land one at a time (PLAN.md §16); the CLI is usable before they do."
    )
    return typer.Exit(EXIT_NOT_IMPLEMENTED)


def _resolve_entrypoint(command: str) -> Any:
    """Import a stage entry point lazily.

    A genuinely missing stage module (or attribute) raises :class:`typer.Exit`
    with a readable message. A ``ModuleNotFoundError`` for some *other* module —
    a stage that exists but has a broken import — propagates, because that is a
    real bug the operator needs to see.
    """
    module_name, attr = STAGE_ENTRYPOINTS[command]
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        missing = exc.name or ""
        if missing == module_name or module_name.startswith(f"{missing}."):
            raise _not_implemented(command, f"no module {module_name}") from None
        raise
    entry = getattr(module, attr, None)
    if entry is None or not callable(entry):
        raise _not_implemented(command, f"{module_name} has no callable {attr}()")
    return entry


def _loggable(fields: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in fields.items():
        if isinstance(value, (list, tuple)):
            out[key] = list(value)
        elif isinstance(value, Path):
            out[key] = str(value)
        else:
            out[key] = value
    return out


def _run_stage(command: str, **kwargs: Any) -> Any:
    """Resolve, invoke and log one stage run.

    Emits ``stage_start`` / ``stage_ok`` / ``stage_failed`` JSON lines with the
    row counts PLAN.md §10 requires ("row counts logged per stage run").
    """
    entry = _resolve_entrypoint(command)
    log = get_logger(command.replace("-", "_"))
    fields = _loggable(kwargs)
    started = time.monotonic()
    log.info("stage_start", command=command, **fields)
    try:
        if inspect.iscoroutinefunction(entry):
            result = asyncio.run(entry(**kwargs))
        else:
            result = entry(**kwargs)
            if inspect.isawaitable(result):
                result = asyncio.run(_await(result))
    except (typer.Exit, typer.Abort, typer.BadParameter):
        raise
    except KeyboardInterrupt:
        log.warning("stage_interrupted", command=command)
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except Exception as exc:
        log.exception("stage_failed", command=command, **fields)
        _echo_error(f"energycap {command} failed: {type(exc).__name__}: {exc}")
        state = _state()
        if state is not None and state.traceback:
            raise
        raise typer.Exit(EXIT_STAGE_FAILED) from None

    duration_s = round(time.monotonic() - started, 3)
    outcome: dict[str, Any] = {"command": command, "duration_s": duration_s}
    if isinstance(result, int) and not isinstance(result, bool):
        outcome["rows"] = result
    elif isinstance(result, Mapping):
        outcome.update(_loggable(result))
    log.info("stage_ok", **{**fields, **outcome})
    return result


async def _await(awaitable: Any) -> Any:
    return await awaitable


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"energycap {__version__}")
        raise typer.Exit()


# ----------------------------------------------------------------- the app

app = typer.Typer(
    name="energycap",
    help=(
        "Household energy + HVAC capture: Leviton and Bryant clouds -> SQLite spool "
        "-> partitioned Parquet in S3.\n\n"
        "Every stage below is idempotent over an arbitrary range of LOCAL dates "
        "(--start/--end, YYYY-MM-DD): output filenames are deterministic, so a "
        "re-run overwrites instead of duplicating. Partitioning is on the LOCAL "
        "date; ts_utc is canonical for sorting, bucketing and dedupe."
    ),
    no_args_is_help=True,
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback()
def main(
    ctx: typer.Context,
    log_level: Annotated[
        str | None,
        typer.Option(
            "--log-level",
            metavar="LEVEL",
            help="DEBUG, INFO, WARNING, ERROR or CRITICAL. Overrides LOG_LEVEL.",
        ),
    ] = None,
    traceback: Annotated[
        bool,
        typer.Option(
            "--traceback",
            help="Re-raise stage exceptions instead of printing a one-line error.",
        ),
    ] = False,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            is_eager=True,
            callback=_version_callback,
            help="Show the version and exit.",
        ),
    ] = False,
) -> None:
    """Load configuration and start structured JSON logging on stdout."""
    try:
        settings = get_settings()
    except Exception as exc:  # pydantic validation of the environment
        _echo_error(f"configuration error: {exc}")
        raise typer.Exit(EXIT_USAGE) from None

    level = settings.log_level
    if log_level is not None:
        candidate = log_level.strip().upper()
        if candidate not in logging.getLevelNamesMapping():
            raise typer.BadParameter(
                f"unknown log level {log_level!r}; "
                "use DEBUG, INFO, WARNING, ERROR or CRITICAL",
                param_hint="--log-level",
            )
        level = candidate
    global _STATE
    configure_logging(level)
    _STATE = CliState(settings=settings, log_level=level, traceback=traceback)
    ctx.obj = _STATE


# ------------------------------------------------------------------ commands


@app.command("run")
def run_cmd() -> None:
    """Run the long-lived collector: poll loops, scheduler and health server.

    This is what the container runs (PLAN.md §5). It hosts the 30s Leviton and
    Bryant status polls writing to the SQLite spool, the Leviton bandwidth
    keepalive (§6.4), the in-process scheduler (hourly upload, ~01:30 daily
    compaction, hourly rollup, ~08:30 Bryant daily energy) and the /healthz
    server (§11). Blocks until interrupted; restart-safe — the spool on /data
    survives, so nothing is lost.
    """
    _run_stage("run")


@app.command("poll")
def poll_cmd(
    once: Annotated[
        bool,
        typer.Option("--once", help="Run a single poll cycle and exit."),
    ] = False,
    source: SourceOpt = None,
) -> None:
    """Poll the cloud sources and append observations to the SQLite spool.

    One timestamp per source per cycle, taken when the response set is complete
    (PLAN.md §6.5). A failed cycle writes zero rows — it never repeats the last
    value and never zero-fills (CLAUDE.md rule 1). Idempotent in the sense that
    matters: re-polling only adds new observations, and the standard dedupe key
    collapses any overlap downstream.
    """
    _run_stage("poll", once=once, sources=tuple(source) if source else None)


@app.command("upload")
def upload_cmd(start: StartOpt = None, end: EndOpt = None) -> None:
    """Upload closed local hours from the spool to S3 as hourly parts.

    Writes part-YYYYMMDDTHH.parquet into the LOCAL-date partition, verifies the
    S3 Parquet row count matches what was written, and only then marks the spool
    rows uploaded (PLAN.md §10). Handles multi-hour catch-up after downtime in
    one invocation. Idempotent: the filename is deterministic, so a re-run
    overwrites the part rather than adding a second copy.

    Default window: yesterday and today (local), which covers the normal hourly
    run and an overnight outage.
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(1), default_end=_today()
    )
    _run_stage("upload", start=start_date, end=end_date)


@app.command("compact-daily")
def compact_daily_cmd(start: StartOpt = None, end: EndOpt = None) -> None:
    """Compact each local day's hourly parts into one day-YYYYMMDD.parquet.

    Reads every part for the day plus any existing day file, dedupes, sorts,
    writes the day file, verifies the row count, and only then moves the parts
    to the non-tabled archive prefix so the raw_30s prefix always holds exactly
    one authoritative copy of the day (PLAN.md §10). Parts are never archived or
    deleted unless the day file verifies. Idempotent and convergent after a
    partial failure.

    Default window: yesterday (the ~01:30 scheduled run for D-1).
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(1), default_end=_days_ago(1)
    )
    _run_stage("compact-daily", start=start_date, end=end_date)


@app.command("rollup")
def rollup_cmd(start: StartOpt = None, end: EndOpt = None) -> None:
    """Rebuild the hourly rollup for each local day: rollup-YYYYMMDD.parquet.

    Regenerates the whole local day every time — cheap, and it avoids intra-day
    merge logic. Buckets on the local hour keyed by ts_utc, so the DST fall-back
    day yields 25 buckets and the spring-forward day 23. Emits mean/min/max/p95,
    sample_count, first/last ts_utc, and kwh for watts only, computed over
    OBSERVED time (mean_watts * sample_count * poll_interval_s / 3.6e6) — never
    extrapolated across a gap. Day-grain metrics are excluded from the input.

    This is the heal for late data: if you fix a collector bug, re-run rollup
    over the affected range. Fully idempotent and disposable — the hourly
    dataset can always be regenerated from raw.

    Default window: yesterday and today (local).
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(1), default_end=_today()
    )
    _run_stage("rollup", start=start_date, end=end_date)


@app.command("fetch-daily")
def fetch_daily_cmd(start: StartOpt = None, end: EndOpt = None) -> None:
    """Fetch Bryant daily energy from the Carrier cloud into energy/daily.

    Day-grain rows (kwh_day, cost_day_usd per component) stamped at LOCAL
    midnight of the measured day (PLAN.md §7.2). Components whose energyConfig
    says enabled=false are structurally absent and are skipped, not written as
    zero. These rows never enter raw_30s — they would poison the hourly rollup
    (CLAUDE.md rule 6). Idempotent: the monthly file is regenerated and the
    standard dedupe key collapses the day1/day2 revision overlap.

    Default window: the scheduled ~08:30 pair — the day before yesterday (day2,
    the revision) through yesterday (day1).
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(2), default_end=_days_ago(1)
    )
    _run_stage("fetch-daily", start=start_date, end=end_date)


@app.command("backfill")
def backfill_cmd(start: StartOpt = None, end: EndOpt = None) -> None:
    """Backfill historical Bryant daily energy into energy/daily.

    Reads the legacy DynamoDB table (read-only Scan) and the old collector's
    JSON files, maps both to identical rows, and prefers the DynamoDB copy where
    they overlap because it carries provenance (PLAN.md §8). Idempotent over an
    arbitrary range of local dates: it regenerates every affected monthly file
    completely, so a double run is byte-identical.

    Default window: the day before yesterday through yesterday. Pass an explicit
    range to import history.
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(2), default_end=_days_ago(1)
    )
    _run_stage("backfill", start=start_date, end=end_date)


@app.command("discover")
def discover_cmd(
    source: SourceOpt = None,
    map_path: Annotated[
        Path,
        typer.Option(
            "--map-path",
            help="channel_map.json to compare the live hierarchy against.",
        ),
    ] = DEFAULT_CHANNEL_MAP,
    json_only: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Print only the channel_map skeleton JSON (no table).",
        ),
    ] = False,
    dump_path: Annotated[
        Path | None,
        typer.Option(
            "--dump",
            metavar="FILE",
            help=(
                "Write every raw Leviton and Carrier response to FILE (mode 0600) "
                "so the first live run captures the evidence the fixtures need."
            ),
        ),
    ] = None,
    raw: Annotated[
        bool,
        typer.Option(
            "--raw",
            help="Capture raw responses to a timestamped file (see --dump).",
        ),
    ] = False,
    out_path: Annotated[
        Path | None,
        typer.Option(
            "--out",
            metavar="FILE",
            help=(
                "Where to write the machine-readable live-channel sidecar that "
                "build-dim reads. Default: live_channels.json beside --map-path."
            ),
        ),
    ] = None,
    write_live_channels: Annotated[
        bool,
        typer.Option(
            "--write-live-channels/--no-write-live-channels",
            help="Write the live-channel sidecar at all (a strictly read-only run).",
        ),
    ] = True,
) -> None:
    """Enumerate the live Leviton and Bryant hierarchy and print a mapping stub.

    Lists hubs, breakers (position, name, branchType, model), CTs (channel,
    usageType) and Bryant zones, then prints ready-to-paste channel_map.json
    entries for everything not already mapped (PLAN.md §9). This is how a newly
    installed smart breaker gets a label in five minutes.

    It touches no S3 object, no spool row and no Parquet file. It does write one
    LOCAL file: live_channels.json beside the map, which build-dim reads to WARN
    about channels nobody has labelled yet (--no-write-live-channels disables
    it). --dump additionally records every raw upstream response, which is the
    only cheap chance to capture evidence on the first live run.
    """
    _run_stage(
        "discover",
        sources=tuple(source) if source else None,
        map_path=map_path,
        json_only=json_only,
        dump_path=dump_path,
        raw=raw,
        out_path=out_path,
        write_live_channels=write_live_channels,
    )


@app.command("build-dim")
def build_dim_cmd(
    map_path: Annotated[
        Path,
        typer.Option("--map-path", help="Hand-maintained channel_map.json (PLAN.md §9)."),
    ] = DEFAULT_CHANNEL_MAP,
    inventory_path: Annotated[
        Path | None,
        typer.Option(
            "--inventory-path",
            help="blackstart montfort.json; defaults to BLACKSTART_INVENTORY_PATH.",
        ),
    ] = None,
    live_channels_path: Annotated[
        Path | None,
        typer.Option(
            "--live-channels",
            metavar="FILE",
            help=(
                "Live channel list from `energycap discover`. Defaults to "
                "live_channels.json beside --map-path when that file exists."
            ),
        ),
    ] = None,
    dry_run: DryRunOpt = False,
) -> None:
    """Build dim_channel.parquet from channel_map.json + the blackstart inventory.

    The semantic layer every useful query joins through: label, panel, slots,
    category, room, priority, estimated_watts per (source, device_id,
    channel_id). Blackstart stays the source of truth for labels; explicit
    fields in channel_map override it. Live channels that are still unmapped are
    WARNed, never silently dropped — the list comes from the live_channels.json
    that `energycap discover` writes beside the map, so run discover first.
    Written atomically to a single overwritten object, so this is idempotent by
    construction — no date range applies.
    """
    _run_stage(
        "build-dim",
        map_path=map_path,
        inventory_path=inventory_path,
        live_channels_path=live_channels_path,
        dry_run=dry_run,
    )


@app.command("create-glue-tables")
def create_glue_tables_cmd(
    database: Annotated[
        str | None,
        typer.Option("--database", help="Glue database; defaults to GLUE_DATABASE."),
    ] = None,
    dry_run: DryRunOpt = False,
) -> None:
    """Create or update the Athena/Glue tables and their comments.

    Idempotent create-or-update (no crawler, no CloudFormation) for
    energy_raw_30s, energy_hourly, energy_daily and dim_channel, with partition
    projection on year/month/day (PLAN.md §12). The table and column comments
    are a first-class deliverable: they state the grain, the LOCAL-date
    partitioning, the dedupe key, the enum decodes, the observed-time kWh
    formula, and the blunt warning that a low sample_count or an absent row
    means the collector had a gap — not that the load was off.
    """
    resolved = database or get_settings().glue_database
    _run_stage("create-glue-tables", database=resolved, dry_run=dry_run)


@app.command("import-greenbutton")
def import_greenbutton_cmd(
    path: Annotated[
        Path,
        typer.Argument(
            metavar="FILE",
            help="Green Button export (ESPI XML or CSV) from the LG&E portal.",
        ),
    ],
    source: Annotated[
        str,
        typer.Option("--source", help="Source name to record on the rows."),
    ] = "lge",
    channel: Annotated[
        str,
        typer.Option("--channel", help="channel_id for the meter's readings."),
    ] = "electric_main",
    out_dir: Annotated[
        Path | None,
        typer.Option("--out-dir", help="Where to write. Default: SPOOL_DIR/meter."),
    ] = None,
    assume_uom: Annotated[
        str | None,
        typer.Option(
            "--assume-uom",
            help="Force units ('Wh' or 'kWh') when the export has no ReadingType.",
        ),
    ] = None,
    interval_s: Annotated[
        int | None,
        typer.Option("--interval-s", help="Interval length for CSV exports."),
    ] = None,
    bucket: Annotated[
        str | None,
        typer.Option("--bucket", help="Also mirror the month files to this S3 bucket."),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Parse and report; write nothing."),
    ] = False,
) -> None:
    """Import an LG&E Green Button meter export into energy/meter.

    Reads a Download My Data export — Green Button ESPI XML (preferred, because
    it states its own units and interval length) or MyMeter's Usage.csv — and
    lands it as interval rows: ts_utc is the interval START and interval_s is
    how long it covers.

    It will not guess. If the XML has no ReadingType, the import fails rather
    than assuming watt-hours, because a silent factor of 1000 is exactly the
    error a meter comparison exists to catch; --assume-uom is the deliberate
    override. A CSV's interval length comes from an end-time column, or
    --interval-s, or is inferred from the spacing and logged as inferred.

    Writes one Parquet file per calendar month touched, named as s3io.meter_key
    names it. Local only unless you pass --bucket: an import is a manual act on
    a file you just downloaded, so it does not fan out to S3 by surprise.
    Re-importing an overlapping range merges on the canonical dedupe key with
    the freshly read row winning, so MyMeter's revisions converge.

    Then compare it against the panels with `energycap compare-meter`.
    """
    _run_stage(
        "import-greenbutton",
        path=path,
        source=source,
        channel_id=channel,
        out_dir=out_dir,
        assume_uom=assume_uom,
        interval_s=interval_s,
        bucket=bucket,
        dry_run=dry_run,
    )


@app.command("greenbutton-authorize")
def greenbutton_authorize_cmd(
    code: Annotated[
        str | None,
        typer.Option("--code", help="Authorization code from the callback page."),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option("--state", help="The state that came back with the code."),
    ] = None,
) -> None:
    """Authorize this application against LG&E Green Button Connect.

    With no --code, prints the URL to open in a browser. Sign in to MyMeter with
    your LOCAL account — the one whose email differs from your My Account login,
    created with the registration code LG&E emails on request.

    With --code, exchanges the code for tokens and caches them at
    SPOOL_DIR/tokens/lge.json, mode 600. The refresh token is the asset: losing
    it costs another trip through the browser, so it is written before the access
    token is used, and a rotation is never dropped.

    The callback page prints this exact command with the code filled in. That
    page is registered with the utility as this application's redirect URI, so
    the command name and options are a published contract, not an internal
    detail. Codes expire in minutes — finish in one sitting.
    """
    _run_stage("greenbutton-authorize", code=code, state=state)


@app.command("fetch-greenbutton")
def fetch_greenbutton_cmd(
    start: StartOpt = None,
    end: EndOpt = None,
    source: Annotated[
        str, typer.Option("--source", help="Source name to record on the rows.")
    ] = "lge",
    channel: Annotated[
        str, typer.Option("--channel", help="channel_id for the meter's readings.")
    ] = "electric_main",
    out_dir: Annotated[
        Path | None,
        typer.Option("--out-dir", help="Where to write. Default: SPOOL_DIR/meter."),
    ] = None,
    bucket: Annotated[
        str | None,
        typer.Option("--bucket", help="Also mirror the month files to this S3 bucket."),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Fetch and report; write nothing.")
    ] = False,
) -> None:
    """Fetch meter intervals from Green Button Connect into energy/meter.

    The automated twin of import-greenbutton: same ESPI, same parser, same
    writer — only the transport differs. Requires greenbutton-authorize to have
    run once; the access token is refreshed automatically after that.

    Idempotent, and deliberately overlapping: the month file is rebuilt from
    existing + fetched on the canonical dedupe key with the freshly fetched row
    winning, because MyMeter publishes recent intervals and then revises them.
    Re-running over a range you already have corrects it rather than duplicating.

    Default window: the last four local days, wide enough to re-read and correct
    revised intervals the way fetch-daily re-reads day2.
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(3), default_end=_today()
    )
    _run_stage(
        "fetch-greenbutton",
        start=start_date,
        end=end_date,
        source=source,
        channel_id=channel,
        out_dir=out_dir,
        bucket=bucket,
        dry_run=dry_run,
    )


@app.command("compare-meter")
def compare_meter_cmd(
    start: StartOpt = None,
    end: EndOpt = None,
    meter_dir: Annotated[
        Path | None,
        typer.Option("--meter-dir", help="Where the imported meter Parquet lives."),
    ] = None,
    channel: Annotated[
        list[str] | None,
        typer.Option(
            "--channel",
            help="Panel-side channel_id to sum. Repeatable. Default: the feed CTs.",
        ),
    ] = None,
    min_coverage: Annotated[
        float,
        typer.Option(
            "--min-coverage",
            help="Exclude hours below this fraction of expected samples from totals.",
        ),
    ] = 0.9,
    source: Annotated[
        str, typer.Option("--source", help="Meter source to read.")
    ] = "lge",
    meter: Annotated[
        str | None,
        typer.Option("--meter", help="Which meter's device_id to compare against."),
    ] = None,
) -> None:
    """Compare the utility meter against the summed panel feeds, hour by hour.

    The whole point of the sub-metering: the two service-feed CT pairs summed
    (ct_1_a + ct_1_b on each hub) should equal what the meter recorded. This
    prints both, their difference in kWh and percent, and the sample coverage
    of each hour.

    The panel side goes through the same rollup_day() and rollup.sql the
    warehouse uses, so this cannot disagree with energy/hourly about what an
    hour of watts is worth — including that kWh is observed-time-only.

    Hours below --min-coverage are shown but kept out of the totals, and the
    count of excluded hours is printed: a partly observed hour understates the
    panels because the collector was down, not because the CTs are wrong.

    Reads the SQLite spool, so it must run where the spool is — inside the
    container while the collector holds it:

        container exec energycap energycap compare-meter --start ... --end ...

    Default window: yesterday and today (local).
    """
    start_date, end_date = _resolve_range(
        start, end, default_start=_days_ago(1), default_end=_today()
    )
    _run_stage(
        "compare-meter",
        start=start_date,
        end=end_date,
        meter_dir=meter_dir,
        channels=tuple(channel) if channel else None,
        source=source,
        meter=meter,
        min_coverage=min_coverage,
    )


if __name__ == "__main__":  # pragma: no cover
    app()
