"""The deployment assets are deliverables, so they are pinned like the README.

Nothing here can prove that ``container build`` works — there is no ``container``
CLI and no Docker daemon on this machine, and the image has never been built by
either runtime (DEVIATIONS.md #163). What these tests *can* do is stop the two
mistakes that would be silent and expensive:

1. **``--detach`` in the LaunchAgent.** launchd supervises a *process*. A detached
   ``container run`` returns immediately, the wrapper exits 0, ``KeepAlive``
   restarts it, and you get a throttled restart loop with several collectors
   racing over one SQLite spool instead of a supervisor. It is written down in
   three prose places; this is the one that fails a build.
2. **A shutdown budget that does not add up.** The wrapper turns launchd's
   SIGTERM into ``container stop --time 30`` (compose's ``stop_grace_period``).
   If the plist's ``ExitTimeOut`` is not comfortably larger, launchd SIGKILLs the
   wrapper mid-shutdown and the poller never closes its spool transaction.

``plistlib`` rather than ``plutil`` on purpose: stdlib, no subprocess, and it
works off macOS, so this file adds no skips.
"""

from __future__ import annotations

import plistlib
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "energycap-container.sh"
PLIST = REPO / "deploy" / "com.duckbillhq.energycap.plist"
DEPLOY_README = REPO / "deploy" / "README.md"

#: The wrapper's own default, and the value the plist has to outlive.
STOP_TIMEOUT_S = 30


@pytest.fixture(scope="module")
def script() -> str:
    return SCRIPT.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def plist() -> dict:
    with PLIST.open("rb") as handle:
        return plistlib.load(handle)


# ------------------------------------------------------------------ the wrapper


def test_the_wrapper_is_executable() -> None:
    """``ProgramArguments`` execs it directly; launchd does not go via a shell."""
    assert SCRIPT.stat().st_mode & 0o111, f"{SCRIPT} is not executable (chmod +x)"


def test_the_wrapper_parses_as_bash() -> None:
    """``bash -n``. The one check that runs here and means something."""
    bash = shutil.which("bash")
    assert bash, "bash is required to check the deployment wrapper"
    done = subprocess.run(  # noqa: S603
        [bash, "-n", str(SCRIPT)], capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr


def test_the_wrapper_passes_the_mount_the_env_file_and_the_port(script: str) -> None:
    """The compose service, argument for argument."""
    for fragment in ("--env-file", "-e SPOOL_DIR=/data", "-e TZ=UTC", "--name", "--rm"):
        assert fragment in script, f"the run arguments no longer contain {fragment!r}"
    assert '-v "${DATA_DIR}:/data"' in script, "the /data bind mount is gone"
    assert '-p "${port}:${port}"' in script, "the health port is no longer published"


def test_the_wrapper_stops_the_container_gracefully(script: str) -> None:
    """``stop_grace_period: 30s`` has no equivalent flag; this is the equivalent."""
    assert f'STOP_TIMEOUT=${{ENERGYCAP_STOP_TIMEOUT_S:-{STOP_TIMEOUT_S}}}' in script
    assert 'container stop --time "${STOP_TIMEOUT}"' in script


def test_the_wrapper_reads_only_a_numeric_port_out_of_the_env_file(script: str) -> None:
    """`.env` holds live credentials. The only thing extracted from it is digits."""
    assert r"HEALTH_PORT[[:space:]]*=[[:space:]]*\([0-9]\{1,5\}\)" in script


# ----------------------------------------------------------------- the plist


def test_the_launchagent_supervises_the_wrapper_in_the_foreground(plist: dict) -> None:
    """The one that matters: no ``--detach`` anywhere in ProgramArguments."""
    argv = plist["ProgramArguments"]
    assert argv[0].endswith("scripts/energycap-container.sh"), argv
    assert argv[1] == "run", argv
    assert not {"-d", "--detach"} & set(argv), (
        "--detach in ProgramArguments turns KeepAlive into a restart loop with "
        "several collectors racing over one SQLite spool — see deploy/README.md"
    )


def test_the_launchagent_replaces_restart_unless_stopped(plist: dict) -> None:
    """Plain ``true``, not a dict: restart on ANY exit, exactly as compose did."""
    assert plist["KeepAlive"] is True
    assert plist["RunAtLoad"] is True
    assert plist["ThrottleInterval"] >= 10, "below launchd's own floor is meaningless"


def test_the_launchagent_outlives_the_wrappers_own_stop_timeout(plist: dict) -> None:
    """launchd's default ExitTimeOut (20s) would SIGKILL mid-``container stop``."""
    assert plist["ExitTimeOut"] > STOP_TIMEOUT_S, (
        f"ExitTimeOut must exceed the wrapper's {STOP_TIMEOUT_S}s graceful stop"
    )


def test_the_launchagent_can_actually_find_the_container_cli(plist: dict) -> None:
    """launchd's default PATH is /usr/bin:/bin:/usr/sbin:/sbin and would not."""
    path = plist["EnvironmentVariables"]["PATH"].split(":")
    assert "/usr/local/bin" in path and "/opt/homebrew/bin" in path, path


def test_the_launchagent_is_a_template_with_exactly_two_placeholders(plist: dict) -> None:
    """Installing it unsubstituted is the obvious first mistake.

    Scans the parsed *values*, not the raw text: ``plutil -p`` drops the XML
    comments, so this is the same view ``deploy/README.md`` tells the operator to
    grep for ``__`` after substituting.
    """

    def strings(node: object) -> list[str]:
        if isinstance(node, str):
            return [node]
        if isinstance(node, dict):
            return [s for value in node.values() for s in strings(value)]
        if isinstance(node, list):
            return [s for item in node for s in strings(item)]
        return []

    found = {p for value in strings(plist) for p in re.findall(r"__[A-Z_]+__", value)}
    assert found == {"__REPO_ROOT__", "__DATA_DIR__"}, found
    for key in ("StandardOutPath", "StandardErrorPath"):
        assert plist[key].startswith("__DATA_DIR__/logs/"), plist[key]


# ------------------------------------------------------------- the documents


def test_the_readme_presents_both_runtimes() -> None:
    """A reader on an Intel Mac must not be sent down the arm64-only path."""
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    assert "scripts/energycap-container.sh" in readme
    assert "docker compose up -d" in readme
    assert "deploy/README.md" in readme


def test_the_deployment_documents_do_not_overstate_what_was_proven() -> None:
    """CLAUDE.md rule: never overstate.

    The image WAS built and run on Apple ``container`` 1.2.2 on 2026-08-17, so the
    original blanket "never been run" caveat is now itself the overstatement. What
    must not rot is the boundary: the supervision story that replaces compose's
    ``restart:`` — the LaunchAgent, KeepAlive, reboot survival — is still entirely
    unexercised, and Docker's build is now the untested one. If a future edit
    claims either has been proven, this fails.
    """
    deploy = " ".join(DEPLOY_README.read_text(encoding="utf-8").split())

    # The LaunchAgent is the load-bearing unproven piece; it must stay flagged.
    assert "LaunchAgent has never been loaded" in deploy, (
        "deploy/README.md no longer admits the LaunchAgent is unexercised"
    )
    # Docker is now the untested path, and saying so is the honest inversion.
    assert "docker build" in deploy.lower() and "never" in deploy, (
        "deploy/README.md no longer records that the Docker build is unproven"
    )
    # And it must not have swung the other way into claiming a full success.
    for overclaim in (
        "fully tested in production",
        "proven in production",
        "verified end to end in production",
    ):
        assert overclaim not in deploy.lower(), f"deploy/README.md overclaims: {overclaim!r}"
