# Deploying energycap on the Mac Mini with Apple `container`

This is the **preferred** way to run the collector on the Apple-silicon Mac Mini:
Apple's [`container`](https://github.com/apple/container) (built and run here on v1.2.2) for the
runtime, **launchd** for the supervision that `docker-compose` used to provide.

Docker is not removed and not deprecated. `docker-compose.yml` still works, is still
correct, and is still the only option on anything that is not an Apple-silicon Mac. See
[The Docker fallback](#the-docker-fallback).

---

## Status: built and run on `container` 1.2.2 (2026-08-17)

The image **has been built and the container has been run** on Apple `container`
**1.2.2** (macOS 27.0, arm64), using this wrapper. What was exercised, end to end:

- `container system start --enable-kernel-install` → apiserver running, kernel installed.
- `./scripts/energycap-container.sh build` → image built. The Dockerfile's own in-image
  assertions passed: `tz ok, energy_capture 0.1.0` (America/Kentucky/Louisville resolves
  inside the container) and `duckdb httpfs ok 1.5.5`.
- `.dockerignore` **is** honoured: `/app/.env` does not exist in the image.
- `./scripts/energycap-container.sh run` → 12 poll cycles across both sources, **zero
  errors or warnings**, the WebSocket ingester connected from inside the container, and
  204 rows landed in the **host's** `data/spool.db` through the bind mount.
- `-p 8080` reachable from the host: `curl localhost:8080/healthz` → 200.
- `./scripts/energycap-container.sh stop` → graceful SIGTERM drain
  (`spool_final`, `runtime_stopped`), container record removed.

What that settles, and what it does not:

| Was unverified | Outcome |
|---|---|
| The image builds at all | **Confirmed.** `uv sync --frozen`, the timezone assertion and the baked-in DuckDB `httpfs` extension all succeeded. |
| `.dockerignore` is honoured | **Confirmed.** No `.env` in the image. Still worth [re-checking](#verify-the-build) after any Dockerfile change. |
| Bind-mount writability (the uid trap) | **Confirmed not a problem on 1.2.2.** virtiofs presents the mount as root-owned and does not enforce guest ownership; uid 10001 writes it with no chown. See [The uid trap](#the-uid-trap). |
| `-p` reachability | **Confirmed.** 200 from the host on the published port. |
| Flag precedence | **Confirmed.** `-e SPOOL_DIR=/data` beats the same key in `--env-file`; a host-shaped `SPOOL_DIR=./data` in `.env` is overridden and the container logs `"spool": "/data/spool.db"`. |
| `container inspect` accepting a name | **Confirmed.** `--name energycap` is usable as the id for `inspect`, `stop` and `logs`. |
| `--rm` actually removing the record | **Confirmed on a clean stop.** An unclean kill is still the risk the wrapper guards against — see [The name-collision wedge](#the-name-collision-wedge). |
| `container`'s env-file parser | **Still unverified.** Documented as "key=value, ignores `#` comments and blank lines" and nothing more; our `.env` happens to be lint-clean, so nothing exercised quoting. Keep values **unquoted**. See [What `--env-file` does not promise](#what---env-file-does-not-promise). |

Still **not** executed, and still the real risks:

- **The LaunchAgent has never been loaded.** KeepAlive, `ThrottleInterval`, restart-on-crash
  and survival across a reboot are all unproven. That is the whole supervision story.
- **No stage past the SQLite spool has touched AWS**, in a container or out of one — see
  [Known-unproven](../README.md#known-unproven).
- **`docker build` has still never run** (no Docker daemon here). The Docker fallback is
  the untested path now, not this one.

---

## Prerequisites

- Apple-silicon Mac (this is arm64-only; the Mac Mini qualifies).
- macOS 26 or newer.
- `container` installed: download the signed installer from
  [github.com/apple/container/releases](https://github.com/apple/container/releases) and
  run it. Then start the subsystem, once per user:

  ```bash
  container system start
  container system status
  ```

  `container system start` is not a one-time thing that survives everything — the
  subsystem is a per-user launchd service under `com.apple.container.*`, so it comes up
  with **your login session**, not with the machine. That is the whole of
  [The boot question](#the-boot-question).

- A configured checkout:

  ```bash
  git clone <this repo> && cd energyDataCapture
  cp .env.example .env && $EDITOR .env     # credentials; .env is gitignored
  mkdir -p data data/logs
  ```

---

## 1. Build

```bash
./scripts/energycap-container.sh build
```

which is:

```bash
container build -t energycap:latest -f Dockerfile .
```

No `--arch`. The `Dockerfile` is deliberately architecture-neutral (its own header says
so: multi-arch base image, multi-arch `uv`, multi-arch wheels), and `container` builds
native `arm64` here. Forcing an architecture would buy emulation and nothing else.

### Verify the build

Before this image goes anywhere, confirm `.dockerignore` was honoured and no credentials
were layered in. The runtime stage does `COPY . /app`, so if the ignore file were skipped,
`.env` would be inside the image:

```bash
./scripts/energycap-container.sh shell
# then, inside:
ls -la /app/.env      # MUST be "No such file or directory"
ls /app/data          # MUST NOT exist either
exit
```

If `/app/.env` exists, stop. Delete the image, and do not push it anywhere.

`shell` needs either a `container exec` subcommand or `container run --entrypoint`; the
wrapper probes for both and tells you if neither exists, because the image's `ENTRYPOINT`
is the `energycap` CLI and nothing else will get you a prompt. If neither is available,
build a throwaway image whose last line is `USER root` + `CMD ["ls","-la","/app"]`, or
inspect the layers with any OCI tool that can — do not skip the check.

---

## 2. First run, in the foreground

Run it by hand once, before launchd is anywhere near it. You want to see the JSON log
lines with your own eyes:

```bash
./scripts/energycap-container.sh run
```

In another terminal:

```bash
./scripts/energycap-container.sh status
curl -s localhost:8080/healthz | jq
```

`status` prints the subsystem state, `container list --all`, the container's IP from
`container inspect`, a `curl` of `/healthz` (on `127.0.0.1` first, then that IP), and the
ownership of the data directory.

`Ctrl-C` to stop; the wrapper turns that into `container stop --time 30` so the poll loop
closes its spool transaction instead of being shot.

The port comes from `HEALTH_PORT` in `.env` (default `8080`) and is published
`host:container` on the same number. The wrapper reads *only* that one numeric value out
of `.env` — nothing else in that file is ever printed, copied or logged.

### If it will not start

| Message | Fix |
|---|---|
| `the \`container\` CLI is not installed or not on PATH` | Install it; under launchd, fix `PATH` in the plist. |
| `the container subsystem is not running` | `container system start` |
| `env file not found` | `cp .env.example .env && $EDITOR .env` |
| `data directory not found` | `mkdir -p data data/logs`, or set `ENERGYCAP_DATA_DIR`. |
| `a container named 'energycap' already exists and could not be cleared` | [The name-collision wedge](#the-name-collision-wedge). |
| `permission denied` / `unable to open database file` on `/data/...` | [The uid trap](#the-uid-trap). |
| a warning that `.env` "uses syntax that Docker tolerates" | [What `--env-file` does not promise](#what---env-file-does-not-promise). |
| the LaunchAgent runs but there is nothing in `data/logs/` | The log **directory** must exist before the first `bootstrap`: `mkdir -p "$DATA/logs"`. See [The log directory](#the-log-directory-launchd-does-not-create-it). |

---

## The name-collision wedge

The wrapper passes `--name energycap --rm`. `--rm` is supposed to drop the container
record when it exits — but it will not have, if the wrapper was `SIGKILL`ed (launchd's
`ExitTimeOut` expiring), if the Mac lost power mid-run, or if the runtime itself died.
The name then survives, and the next `container run --name energycap` fails with *already
exists*.

**Under compose this could not happen**: `restart: unless-stopped` restarts the *same*
container. Under `KeepAlive` every restart is a fresh `container run`, so one ungraceful
shutdown would otherwise mean **every** subsequent start fails, once per
`ThrottleInterval`, forever, until a human logs in. A permanent data outage from nothing
worse than a power cut.

So `run` now tries to clear it first: `container stop --time 30` (documented), then — only
if the name is still taken — whichever of `delete` / `rm` / `remove` **`container --help`
actually advertises**. That subcommand is *not* in the reference this was built against,
so it is probed and never assumed; if the help does not list one, the wrapper dies with
the message above and you clear it by hand. Nothing invented is ever executed.

If you *do* see the fatal version, `container list --all` will show the stale record.

## What `--env-file` does not promise

Apple documents exactly one thing about it: *"key=value format, ignores `#` comments and
blank lines."* Docker's parser does more — it strips surrounding quotes, strips trailing
whitespace, and (under `docker compose`) interpolates `${VAR}`. **Assume none of that
here.** A `.env` written to Docker's habits can land values like:

| in `.env` | what may actually reach the process |
|---|---|
| `S3_BUCKET="my-bucket"` | `"my-bucket"` — with the quote characters |
| `HEALTH_PORT=8080·····` | `8080     ` — trailing space, fails `int()` |
| `TZ_LOCAL=America/... # zone` | the comment as part of the zone name |

The third is the dangerous one: LOCAL-date partitioning is derived from `TZ_LOCAL`
(CLAUDE.md cardinal rule 4), so a corrupted zone silently mis-assigns every partition.

The committed `.env.example` is already clean — every comment is on its own line, nothing
is quoted, nothing has trailing whitespace — so this only bites a hand-edited `.env`. The
wrapper lints for it on every `run` and warns, reporting the **key name and line number
only, never the value**.

## The log directory (launchd does not create it)

launchd opens `StandardOutPath` / `StandardErrorPath` *before* it execs the job, and it
creates the **files** but not their **directory**. If `__DATA_DIR__/logs` does not exist
at `bootstrap` time, the job's output goes nowhere — and the symptom is the worst kind:
a service that appears to be loaded with no log to explain itself.

```bash
mkdir -p "$DATA/logs"                                    # do this BEFORE bootstrap
launchctl print gui/$(id -u)/com.duckbillhq.energycap    # last exit status, spawn errors
```

The wrapper also does `mkdir -p "$DATA_DIR/logs"`, but that runs *after* launchd has
already opened the files, so it only helps the next start.

## KeepAlive when the container cannot start at all

Every fatal preflight (`container` not on `PATH`, missing `.env`, missing data dir, the
wedge above) and every fast `container run` failure (image not built, host port already
bound) exits the wrapper in well under a second. `KeepAlive` restarts it, `ThrottleInterval
30` holds that to one attempt every 30 seconds. That is a slow retry loop, not a hot loop,
and it self-heals the moment you fix the cause — but it will keep appending to an
**unrotated** log file the whole time, so fix it, and see the `newsyslog` stanza in
[What you lost](#what-you-lost-relative-to-docker-compose-and-what-replaces-it).

The one failure that does *not* self-heal is a `container system status` that is down:
a LaunchAgent can be running while the per-user `container` subsystem is not. The wrapper
treats a non-zero exit from `container system status` as fatal, but only **warns** if the
subsystem merely *prints* something that looks stopped — we have never seen that output,
and a wrong guess about its wording would turn a healthy machine into a permanent outage.
The real `container` command a moment later gets to be the authority.

---

## The uid trap

**Measured on `container` 1.2.2, 2026-08-17: this does not bite.** It is documented
anyway, because the reasoning is non-obvious and the day it changes you will want it.

The image runs as a non-root user, uid **10001** (`Dockerfile`: `useradd --uid 10001
energycap`). It is `/data` — the bind mount — that everything is written to: `spool.db`
(+ `-wal`/`-shm`), `tokens/*.json` (mode 600, deliberately), `status.json`.

On a real filesystem that would fail: the host directory is owned by *you* (uid 501,
mode 755), so uid 10001 has no write bit. What actually happens is that virtiofs does
**not** enforce guest ownership. Inside the container the mount presents as:

```
drwxr-xr-x 8 0 0 256 Aug 17 17:17 /data
```

— owned by `0 0`, not by the host uid — and a write as uid 10001 succeeds regardless. A
full run then created `spool.db`, `status.json` and `tokens/` through the mount with no
`chown` of any kind, and the rows were readable from the host afterwards.

Docker Desktop is likewise permissive here, and `docker-compose.yml` sidesteps the
question entirely by using a **named volume**, which Docker seeds with the image's
ownership.

So the wrapper prints an informational **note**, not a warning:

```
energycap: note: /Users/.../data is not owned by the container's uid 10001 (...).
  That is expected and fine: virtiofs presents the mount as root-owned inside the
  container and does not enforce guest ownership... Nothing to do.
```

**When to stop believing this.** The measurement is specific to `container` 1.2.2 with a
plain bind mount of a local APFS path. Re-check if any of these change: a `container` or
macOS upgrade; a `--user`/`--uid`/`--gid` override on `run` (1.2.2 has all three); a data
directory on NFS, SMB or an external volume; or a switch to `--mount type=volume`.

**Symptom, if it ever does bite.** The container starts and then dies, or logs errors,
with `permission denied` or SQLite's `unable to open database file`, naming
`/data/spool.db`, `/data/status.json` or `/data/tokens/*.json`.

A nastier variant: everything *looks* fine because the spool opens read-only or the token
cache silently fails to persist, and you get a re-login to Leviton every restart. Check
`./scripts/energycap-container.sh logs` for `permission` at the first sign of weirdness.

The case that would bite hardest is a directory you have already used from the host: after
`uv run energycap discover`, `data/status.json` and `data/tokens/` are mode 600 owned by
*you*, and on a filesystem that *did* enforce ownership, uid 10001 could not touch them no
matter how permissive the directory itself was. That is the exact situation measured above
— and it worked.

**Remedies, in order of preference:**

1. **Hand the directory to the container's uid.** Cleanest, matches what a named volume
   did under Docker:

   ```bash
   sudo chown -R 10001:10001 /path/to/data
   ```

   Cost: you now need `sudo` to read `spool.db` from the host (e.g. with DuckDB). If you
   query the spool from the Mac regularly, that is a real cost — measure it against
   option 2 rather than assuming.

2. **Open the permissions instead.** Blunter, keeps host access:

   ```bash
   chmod -R a+rwX /path/to/data
   ```

   Note that this widens the token caches, which CLAUDE.md's cardinal rule 8 wants at
   mode 600. The process re-creates its own files 600 as it writes them; it is the
   *directory* being group/other-writable that matters. Acceptable on a single-user Mac
   Mini, not somewhere with other logins.

3. **Match the uid at run time.** If your build of the CLI has a `--user`/`-u` flag
   (`container run --help`), running as your own uid removes the mismatch entirely. It is
   not in the documented flag set, so treat this as "check, don't assume".

4. **Start clean.** If there is nothing worth keeping in `data/`, `rm -rf data && mkdir -p
   data data/logs` and let the container create everything itself — then whatever uid the
   mapping produces is the owner, and it is self-consistent from the start.

---

## 3. Install the LaunchAgent

launchd is what replaces `restart: unless-stopped`. `deploy/com.duckbillhq.energycap.plist`
is a **template** with `__REPO_ROOT__` and `__DATA_DIR__` placeholders — it will not work
until you substitute them.

```bash
REPO=/Users/$USER/code/energyDataCapture       # your actual checkout
DATA=$REPO/data                                # or an external disk

mkdir -p "$DATA/logs"                          # launchd creates the log FILES, not the dir
mkdir -p ~/Library/LaunchAgents

sed -e "s|__REPO_ROOT__|$REPO|g" -e "s|__DATA_DIR__|$DATA|g" \
    "$REPO/deploy/com.duckbillhq.energycap.plist" \
    > ~/Library/LaunchAgents/com.duckbillhq.energycap.plist

plutil -lint ~/Library/LaunchAgents/com.duckbillhq.energycap.plist
# Must print "OK". `plutil -p` drops the XML comments, so this only sees real values;
# the un-substituted template prints 5 lines here and a substituted one prints none.
plutil -p ~/Library/LaunchAgents/com.duckbillhq.energycap.plist | grep '__'
```

> `plutil -lint` is **not** a strict XML check: it accepted this file while it still had
> `--` inside an XML comment, which the XML spec forbids and which any strict parser
> (Python's `plistlib`, `xmllint`) rejects outright. The template no longer contains one,
> and `tests/test_deploy.py` parses it with `plistlib` so it cannot come back. If you edit
> the comments, spell flags out in words rather than writing a double hyphen.

Check that `container` is on the plist's `PATH` — launchd does **not** use your shell's:

```bash
command -v container      # must live in one of the PATH entries in the plist
```

Load it with the modern subcommands (`launchctl load`/`unload` are deprecated and lie
about failures):

```bash
UID_=$(id -u)
launchctl enable    gui/$UID_/com.duckbillhq.energycap
launchctl bootstrap gui/$UID_ ~/Library/LaunchAgents/com.duckbillhq.energycap.plist
launchctl print     gui/$UID_/com.duckbillhq.energycap | head -40
```

`RunAtLoad` means it starts immediately. Watch it come up:

```bash
tail -f "$DATA/logs/energycap.out.log"
curl -s localhost:8080/healthz | jq
```

### Everyday launchctl

```bash
UID_=$(id -u)
launchctl kickstart -k gui/$UID_/com.duckbillhq.energycap   # restart (the -k kills first)
launchctl print       gui/$UID_/com.duckbillhq.energycap    # state, last exit status, PID
launchctl bootout     gui/$UID_/com.duckbillhq.energycap    # stop AND unload
launchctl disable     gui/$UID_/com.duckbillhq.energycap    # keep it down across logins
```

**`./scripts/energycap-container.sh stop` is not how you stop it under launchd.** KeepAlive
will just start it again — that is the point. Use `bootout` (and `disable` if it must stay
down). After a code change: rebuild, then `kickstart -k`.

### Why foreground, and why you must not "fix" it

The plist runs `energycap-container.sh run`, which runs `container run` in the
**foreground** and stays alive for exactly as long as the container does. launchd
supervises a *process*; that is the only thing that makes KeepAlive equivalent to
`restart: unless-stopped`.

Put `--detach` in `ProgramArguments` and `container run` returns instantly, the script
exits 0, launchd sees the job finish, KeepAlive restarts it — forever, throttled only by
`ThrottleInterval`. You get a restart loop instead of a collector, with several instances
potentially racing over one SQLite spool. The comment is in the script and in the plist;
this is the third place it is written down.

`--detach` is still there for interactive use (`./scripts/energycap-container.sh run -d`).
Just never in the plist.

---

## The boot question

**A LaunchAgent only runs once its user has logged in.** Apple's `container` subsystem is
itself a per-user launchd service (`com.apple.container.*`) — so this is not a limitation
we chose, it is the shape of the runtime. A LaunchDaemon running as root at boot **would
not work**: root's session has no `container` subsystem, and `container system start` as
root would start a *different* one that knows nothing about your image.

So for an unattended Mac Mini that reboots after a power cut, you must arrange for the
user session to exist:

1. **Automatic login.** System Settings → Users & Groups → *Automatically log in as* →
   the collector's user.
   **FileVault blocks this**: with FileVault on, the Mac stops at the login window after a
   reboot until someone types a password, and nothing starts until then. If this machine
   must come back unattended, FileVault has to be off — a deliberate trade of at-rest disk
   encryption for unattended restart. Decide it consciously; `.env` and the token caches
   live on that disk.
2. **Start up automatically after a power failure.** System Settings → Energy, or:

   ```bash
   sudo pmset -a autorestart 1
   ```

3. **Do not let it sleep.** A sleeping Mac Mini is a gap in the data, and gaps stay gaps
   (CLAUDE.md cardinal rule 1) — you will see it as a `sample_count` dip, not as a zero:

   ```bash
   sudo pmset -a sleep 0 disksleep 0
   pmset -g                       # verify
   ```

4. **Confirm the whole chain after a real reboot.** Reboot, do not touch the keyboard, and
   check that the login happened, `container system status` is running, and `/healthz`
   answers 200. That is the only test of this that means anything.

---

## What you lost relative to docker-compose, and what replaces it

Apple's `container` has no compose, no restart policy, no healthcheck and no
`depends_on`. Line by line, against `docker-compose.yml`:

| docker-compose.yml | Under `container` | Replacement |
|---|---|---|
| `restart: unless-stopped` | Does not exist | **launchd `KeepAlive`** in the LaunchAgent, with `ThrottleInterval 30` so an instantly-failing container cannot hot-loop. Note the semantics differ slightly: compose's `unless-stopped` remembers a manual stop across daemon restarts; KeepAlive does not, so `container stop` alone will not keep it down — `launchctl bootout`/`disable` is the manual stop. And compose restarted the *same* container, where KeepAlive gives us a fresh `container run` every time — see [The name-collision wedge](#the-name-collision-wedge). |
| `healthcheck:` | Does not exist | **Nothing automatic.** `/healthz` is still served and still returns 503 when a poller's last success is older than 3× its interval (PLAN.md §11) — but nothing polls it and nothing acts on it. It is on the operator, `./scripts/energycap-container.sh status`, or a separate watchdog (a `cron`/`launchd` `StartInterval` job that curls `/healthz` and `launchctl kickstart -k`s on a non-200, or an external uptime monitor). Until you write that, a wedged-but-alive collector goes unnoticed: KeepAlive only sees process death. |
| `docker compose up -d` / `logs` / `restart` | No compose at all | **`scripts/energycap-container.sh`** (`build`/`run`/`stop`/`restart`/`logs`/`status`/`shell`). |
| `depends_on` | Does not exist | Not used — there is one service by design (PLAN.md §5: no database, no queue, no sidecar). |
| `init: true` | No `--init` flag | **Nothing needed.** `energycap run` installs its own `SIGTERM`/`SIGINT` handlers (`runtime.py`), so it stops cleanly as PID 1, and it spawns no children to reap. Signals reach it via the wrapper's `container stop --time 30`. |
| `stop_grace_period: 30s` | Not a run flag | **`container stop --time 30`** in the wrapper, plus **`ExitTimeOut 45`** in the plist so launchd does not SIGKILL the wrapper mid-shutdown (its default is 20s). |
| `logging: json-file, max-size 10m, max-file 5` | No log driver | **Not replaced — this is a real regression.** launchd appends to `StandardOutPath` forever. Add rotation, e.g. `/etc/newsyslog.d/energycap.conf` (tab-separated, `sudo` to install): `/Users/YOU/code/energyDataCapture/data/logs/energycap.out.log 644 5 10240 * J` — 5 generations, 10 MB each, bzip2. Do the same for `.err.log`. Then `sudo newsyslog -nv` to check the parse. Without this, one long-lived container fills the Mac Mini's disk, which is the exact failure compose's caps existed to prevent. |
| named volume `energycap-data` | No volume management | **A host bind mount** (`-v <host>/data:/data`), which is why [The uid trap](#the-uid-trap) exists at all. The upside: `spool.db` is directly readable from the Mac. |
| `ports: "${HEALTH_PORT}:${HEALTH_PORT}"` | Supported | `-p` works; the container also has its own IP (`container inspect ... .networks[0].ipv4Address`) if publishing misbehaves. |
| `env_file: .env` | Supported | `--env-file`. Keep values unquoted. |
| `HEALTHCHECK` in the Dockerfile | Ignored | Same as above: informational only. The `Dockerfile` is unchanged and still correct for Docker. |

---

## The Docker fallback

Unchanged, still supported, and the only path on an Intel Mac or a Linux box:

```bash
docker compose up -d
docker compose logs -f
curl -s localhost:8080/healthz | jq
docker compose down
```

`docker-compose.yml` keeps its named volume, its restart policy and its healthcheck. The
two paths do **not** share state: compose stores the spool in the `energycap-data` named
volume, while the `container` path bind-mounts `./data`. Running both at once means two
collectors writing two different spools and both polling the same clouds — don't.

To move from one to the other, stop the first, then copy the spool across (from the named
volume: `docker run --rm -v energycap-data:/from -v "$PWD/data":/to alpine cp -a /from/. /to/`)
and fix the ownership per [The uid trap](#the-uid-trap).

---

## Files

| Path | What |
|---|---|
| `scripts/energycap-container.sh` | The wrapper. Preflight checks, foreground `run`, `status`. |
| `deploy/com.duckbillhq.energycap.plist` | LaunchAgent **template** — substitute the placeholders. |
| `deploy/README.md` | This file. |
| `tests/test_deploy.py` | The only automated check on any of this. `bash -n` on the wrapper, a strict `plistlib` parse of the template, and the two mistakes that would be silent: a detach flag in `ProgramArguments`, and an `ExitTimeOut` that does not outlive the wrapper's 30-second graceful stop. |
| `Dockerfile` | Unchanged, shared by both runtimes. |
| `docker-compose.yml` | Unchanged, the fallback path. |
