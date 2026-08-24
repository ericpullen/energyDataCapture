"""CLI contract tests (PLAN.md §5, §10, §16).

What these pin down:

* every stage in PLAN.md §5/§16 is reachable as a command and is *documented*
  as idempotent over a local date range;
* ``--start/--end`` parse as local dates and reject garbage with a usage error,
  never a traceback;
* a stage module that has not landed yet degrades to a readable "not
  implemented yet" message and a non-zero exit, because the CLI must be usable
  before the stages exist;
* stages receive real ``datetime.date`` objects and the documented default
  windows.

No network, no AWS, no stage modules required.
"""

from __future__ import annotations

import importlib
import io
import sys
import types
from datetime import date, timedelta

import pytest
from typer.testing import CliRunner

from energy_capture import cli
from energy_capture.logging import configure_logging
from energy_capture.timeutil import local_date_of, now_utc

runner = CliRunner()

#: Every command PLAN.md §5/§16 and CLAUDE.md "Commands" require.
DOCUMENTED_COMMANDS = (
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
    "import-greenbutton",
    "fetch-greenbutton",
    "greenbutton-authorize",
    "compare-meter",
    "verify-bill",
    "watch-health",
    "digest",
    "check-channels",
)

#: The commands that take a local date range (PLAN.md §10).
DATE_RANGE_COMMANDS = (
    "upload",
    "compact-daily",
    "rollup",
    "fetch-daily",
    "backfill",
)

_BOX_CHARS = "─│╭╮╰╯━┃┏┓┗┛"


@pytest.fixture(autouse=True)
def _isolated_logging() -> io.StringIO:
    """Point the JSON log handler at a buffer, not the runner's captured stdout.

    ``configure_logging`` binds a handler to ``sys.stdout`` on first use; under
    ``CliRunner`` that stream is closed when the invocation ends, so the next
    test would log into a dead file. Forcing a fresh buffer per test also keeps
    log output out of the assertions.
    """
    buffer = io.StringIO()
    configure_logging("INFO", stream=buffer, force=True)
    return buffer


def _flat(text: str) -> str:
    """Rich help is wrapped and boxed; flatten it so substrings survive."""
    stripped = "".join(" " if ch in _BOX_CHARS else ch for ch in text)
    return " ".join(stripped.split())


def _fake_stage(monkeypatch: pytest.MonkeyPatch, command: str, func) -> dict:
    """Install a stand-in stage module for ``command`` and capture its kwargs."""
    module_name, attr = cli.STAGE_ENTRYPOINTS[command]
    captured: dict = {}

    def entry(**kwargs):
        captured.update(kwargs)
        return func(**kwargs)

    module = types.ModuleType(module_name)
    setattr(module, attr, entry)
    monkeypatch.setitem(sys.modules, module_name, module)
    return captured


def _today() -> date:
    return local_date_of(now_utc())


# ------------------------------------------------------------------- help


def test_help_lists_every_documented_command() -> None:
    result = runner.invoke(app_ := cli.app, ["--help"])
    assert result.exit_code == 0, result.output
    flat = _flat(result.output)
    for command in DOCUMENTED_COMMANDS:
        assert command in flat, f"{command} missing from `energycap --help`"
    assert app_ is cli.app


def test_every_documented_command_has_an_entrypoint() -> None:
    """Every command now resolves to a real stage — including the last two.

    ``import-greenbutton`` was PLAN.md §13's deliberately-unbuilt command and is
    now built (Download My Data lands meter intervals without waiting on the
    Connect registration), and ``compare-meter`` is what reads them back against
    the panels. Nothing is left exiting 3.
    """
    wired = set(cli.STAGE_ENTRYPOINTS)
    assert wired == set(DOCUMENTED_COMMANDS)
    assert set(cli.STAGE_SIGNATURES) == wired


def test_top_level_help_states_the_idempotency_contract() -> None:
    flat = _flat(runner.invoke(cli.app, ["--help"]).output).lower()
    assert "idempotent" in flat
    assert "local" in flat


@pytest.mark.parametrize("command", DATE_RANGE_COMMANDS)
def test_stage_help_documents_local_date_range_and_idempotence(command: str) -> None:
    result = runner.invoke(cli.app, [command, "--help"])
    assert result.exit_code == 0, result.output
    flat = _flat(result.output)
    assert "--start" in flat and "--end" in flat
    assert "YYYY-MM-DD" in flat
    assert "idempotent" in flat.lower()
    assert "local" in flat.lower()


def test_version_option() -> None:
    result = runner.invoke(cli.app, ["--version"])
    assert result.exit_code == 0
    assert "energycap" in result.output


def test_no_args_shows_help_without_traceback() -> None:
    result = runner.invoke(cli.app, [])
    assert result.exit_code != 0
    assert "Usage" in result.output
    assert "Traceback" not in result.output


# --------------------------------------------------------------- date parsing


@pytest.mark.parametrize("command", DATE_RANGE_COMMANDS)
@pytest.mark.parametrize(
    "garbage", ["yesterday", "2026-13-01", "2026-02-30", "8/16/2026", "not-a-date", ""]
)
def test_garbage_dates_are_a_usage_error_not_a_traceback(
    command: str, garbage: str
) -> None:
    result = runner.invoke(cli.app, [command, "--start", garbage])
    assert result.exit_code == cli.EXIT_USAGE
    flat = _flat(result.output)
    assert "YYYY-MM-DD" in flat
    assert "--start" in flat
    assert "Traceback" not in result.output
    # click turns a BadParameter into a clean SystemExit; nothing escapes raw.
    assert result.exception is None or isinstance(result.exception, SystemExit)


@pytest.mark.parametrize("command", DATE_RANGE_COMMANDS)
def test_inverted_range_is_rejected(command: str) -> None:
    result = runner.invoke(
        cli.app, [command, "--start", "2026-08-16", "--end", "2026-08-15"]
    )
    assert result.exit_code == cli.EXIT_USAGE
    flat = _flat(result.output)
    assert "2026-08-15" in flat and "2026-08-16" in flat
    assert "Traceback" not in result.output


def test_dates_reach_the_stage_as_date_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _fake_stage(monkeypatch, "rollup", lambda **kw: {"rows": 7})
    result = runner.invoke(
        cli.app, ["rollup", "--start", "2026-03-07", "--end", "2026-03-09"]
    )
    assert result.exit_code == 0, result.output
    assert captured["start"] == date(2026, 3, 7)
    assert captured["end"] == date(2026, 3, 9)
    assert isinstance(captured["start"], date)
    # The interval guard's knobs default to "unset" and "refuse".
    assert captured["poll_interval_s"] is None
    assert captured["allow_interval_mismatch"] is False


def test_lone_start_means_a_single_local_day(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _fake_stage(monkeypatch, "upload", lambda **kw: None)
    result = runner.invoke(cli.app, ["upload", "--start", "2026-11-01"])
    assert result.exit_code == 0, result.output
    assert captured["start"] == captured["end"] == date(2026, 11, 1)


def test_default_windows_follow_the_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    today = _today()
    compact = _fake_stage(monkeypatch, "compact-daily", lambda **kw: None)
    assert runner.invoke(cli.app, ["compact-daily"]).exit_code == 0
    # The 01:30 job compacts D-1.
    assert compact["start"] == compact["end"] == today - timedelta(days=1)

    upload = _fake_stage(monkeypatch, "upload", lambda **kw: None)
    assert runner.invoke(cli.app, ["upload"]).exit_code == 0
    # Yesterday + today, so an overnight outage is caught up in one run.
    assert upload["start"] == today - timedelta(days=1)
    assert upload["end"] == today

    daily = _fake_stage(monkeypatch, "fetch-daily", lambda **kw: None)
    assert runner.invoke(cli.app, ["fetch-daily"]).exit_code == 0
    # day2 (revision) through day1, per PLAN.md §7.2.
    assert daily["start"] == today - timedelta(days=2)
    assert daily["end"] == today - timedelta(days=1)


# ------------------------------------------------------- missing stage modules


#: Commands with a required argument, so the parametrised checks below reach the
#: stage dispatch rather than stopping at a usage error.
REQUIRED_ARGS: dict[str, list[str]] = {"import-greenbutton": ["usage.xml"]}


@pytest.mark.parametrize("command", sorted(cli.STAGE_ENTRYPOINTS))
def test_missing_stage_module_exits_nonzero_with_a_readable_message(
    command: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        cli.STAGE_ENTRYPOINTS,
        command,
        ("energy_capture.stages._not_landed_yet", "run"),
    )
    result = runner.invoke(cli.app, [command, *REQUIRED_ARGS.get(command, [])])
    assert result.exit_code == cli.EXIT_NOT_IMPLEMENTED
    flat = _flat(result.output)
    assert "not implemented yet" in flat
    assert "energy_capture.stages._not_landed_yet" in flat
    assert "Traceback" not in result.output
    assert not isinstance(result.exception, ImportError)


def test_stage_module_without_the_entrypoint_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = types.ModuleType("energy_capture.stages.rollup")  # no run()
    monkeypatch.setitem(sys.modules, "energy_capture.stages.rollup", module)
    result = runner.invoke(cli.app, ["rollup"])
    assert result.exit_code == cli.EXIT_NOT_IMPLEMENTED
    assert "not implemented yet" in _flat(result.output)


def test_a_broken_import_inside_a_stage_is_not_disguised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stage that exists but imports a missing third-party module is a bug.

    It must not be reported as "not implemented yet" — that would hide a real
    deployment problem behind a friendly message.
    """

    def explode(name: str, package: str | None = None):
        raise ModuleNotFoundError("No module named 'some_third_party'", name="some_third_party")

    monkeypatch.setattr(importlib, "import_module", explode)
    result = runner.invoke(cli.app, ["rollup"])
    assert isinstance(result.exception, ModuleNotFoundError)
    assert "not implemented yet" not in _flat(result.output)


def test_import_greenbutton_reports_a_missing_file_without_a_traceback() -> None:
    """It is built now; a bad path is still an operator error, not a crash."""
    result = runner.invoke(cli.app, ["import-greenbutton", "no-such-export.xml"])
    assert result.exit_code != 0
    assert "Traceback" not in result.output


# ------------------------------------------------------------ failure handling


def test_stage_failure_is_a_one_line_error_not_a_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(**kwargs):
        raise RuntimeError("S3_BUCKET is not configured")

    _fake_stage(monkeypatch, "rollup", boom)
    result = runner.invoke(cli.app, ["rollup", "--start", "2026-08-16"])
    assert result.exit_code == cli.EXIT_STAGE_FAILED
    assert "S3_BUCKET is not configured" in _flat(result.output)
    assert "Traceback" not in result.output
    assert isinstance(result.exception, SystemExit)


def test_traceback_flag_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(**kwargs):
        raise RuntimeError("kaboom")

    _fake_stage(monkeypatch, "rollup", boom)
    result = runner.invoke(
        cli.app, ["--traceback", "rollup", "--start", "2026-08-16"]
    )
    assert isinstance(result.exception, RuntimeError)


def test_async_stage_entrypoints_are_awaited(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name, attr = cli.STAGE_ENTRYPOINTS["poll"]
    seen: dict = {}

    async def entry(**kwargs):
        seen.update(kwargs)
        return 3

    module = types.ModuleType(module_name)
    setattr(module, attr, entry)
    monkeypatch.setitem(sys.modules, module_name, module)

    result = runner.invoke(cli.app, ["poll", "--once", "--source", "leviton"])
    assert result.exit_code == 0, result.output
    assert seen == {"once": True, "sources": ("leviton",)}


# ------------------------------------------------------------------ options


def test_unknown_log_level_is_a_usage_error() -> None:
    result = runner.invoke(cli.app, ["--log-level", "LOUD", "rollup"])
    assert result.exit_code == cli.EXIT_USAGE
    assert "LOUD" in _flat(result.output)
    assert "Traceback" not in result.output


def test_log_level_option_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_stage(monkeypatch, "rollup", lambda **kw: None)
    result = runner.invoke(
        cli.app, ["--log-level", "debug", "rollup", "--start", "2026-08-16"]
    )
    assert result.exit_code == 0, result.output
