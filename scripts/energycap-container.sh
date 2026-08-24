#!/usr/bin/env bash
#
# energycap under Apple's `container` (v1.0.0+) on an Apple-silicon Mac.
#
# This is the compose-replacement. Apple's container has no compose, no
# `restart:` policy, no healthcheck and no depends_on, so:
#
#   docker-compose.yml `restart: unless-stopped`  ->  launchd KeepAlive
#                      `healthcheck:`             ->  nothing automatic; poll /healthz
#                      `docker compose ...`       ->  this script
#
# See deploy/README.md. docker-compose.yml stays valid and is the fallback path
# (and the only path on non-Apple-silicon hosts).
#
# STATUS: this file HAS been executed. It was written before either runtime was
# available anywhere and carried a "nothing here has ever run" banner for that
# reason; the banner outlived its truth once the collector moved to Lightsail and
# the measurement log further down this file was recorded. What is still true is
# narrower and worth keeping: the *development Mac* has no `container` CLI and no
# Docker daemon, so nothing here can be exercised from there — check on the host
# that actually runs the collector, not on the machine you are editing from.
#
# Usage:
#   scripts/energycap-container.sh build
#   scripts/energycap-container.sh run [--detach]
#   scripts/energycap-container.sh stop
#   scripts/energycap-container.sh restart [--detach]
#   scripts/energycap-container.sh logs [-f] [-n N]
#   scripts/energycap-container.sh status
#   scripts/energycap-container.sh shell
#
# Environment overrides:
#   ENERGYCAP_DATA_DIR        host dir bind-mounted at /data   (default: <repo>/data)
#   ENERGYCAP_ENV_FILE        env file passed to --env-file    (default: <repo>/.env)
#   ENERGYCAP_IMAGE           image tag                        (default: energycap:latest)
#   ENERGYCAP_CONTAINER_NAME  container name                   (default: energycap)
#   ENERGYCAP_STOP_TIMEOUT_S  SIGTERM grace period             (default: 30)
#
# This script never prints, copies or logs the contents of the env file. The
# only thing it ever reads out of it is a numeric HEALTH_PORT.

set -euo pipefail

# ---------------------------------------------------------------- constants

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

IMAGE=${ENERGYCAP_IMAGE:-energycap:latest}
NAME=${ENERGYCAP_CONTAINER_NAME:-energycap}
DATA_DIR=${ENERGYCAP_DATA_DIR:-${REPO_ROOT}/data}
ENV_FILE=${ENERGYCAP_ENV_FILE:-${REPO_ROOT}/.env}
STOP_TIMEOUT=${ENERGYCAP_STOP_TIMEOUT_S:-30}

# Must match the Dockerfile's `useradd --uid 10001 energycap`. The image runs
# non-root; every file the process writes under /data is created as this uid.
CONTAINER_UID=10001
CONTAINER_GID=10001

INSTALL_HINT='Install it from https://github.com/apple/container/releases (Apple silicon, macOS 26+), then run: container system start'

# ------------------------------------------------------------------ output

info() { printf '%s\n' "energycap: $*"; }
warn() { printf '%s\n' "energycap: WARNING: $*" >&2; }
die()  { printf '%s\n' "energycap: error: $*" >&2; exit 1; }

# ------------------------------------------------------------------ checks

require_cli() {
  command -v container >/dev/null 2>&1 \
    || die "the \`container\` CLI is not installed or not on PATH.
  ${INSTALL_HINT}
  (Under launchd, PATH is set by the plist — see deploy/README.md.)"
}

# The container subsystem is itself a per-user launchd service (com.apple.container.*).
# It is NOT running just because the CLI is installed, and it is not running at
# all until the owning user has logged in. See deploy/README.md, "The boot question".
#
# TWO checks with deliberately DIFFERENT severities, because they are not equally
# trustworthy:
#
#   exit status  — a contract every CLI keeps. Fatal.
#   output text  — a guess. We have never seen what `container system status`
#                  actually prints. If it exits 0 but says something we happen to
#                  match ("... stopped 3 containers", "inactive since ..."), a
#                  fatal here would be a PERMANENT outage under launchd: KeepAlive
#                  would re-run us every ThrottleInterval and we would refuse to
#                  start a subsystem that was fine all along. So it WARNS, and the
#                  real `container` command a moment later gets to be the
#                  authority. A genuinely stopped subsystem still fails loudly —
#                  just with the CLI's own error, which is the honest one.
require_system_running() {
  local out
  if ! out=$(container system status 2>&1); then
    die "the container subsystem is not running.
  Start it with:  container system start
  (\`container system status\` said: ${out})"
  fi
  case $(printf '%s' "${out}" | tr '[:upper:]' '[:lower:]') in
    *"not running"*|*stopped*|*inactive*)
      warn "\`container system status\` exited 0 but its output looks like the subsystem
  is not running. If the next command fails, start it with:  container system start
  (it said: ${out})" ;;
  esac
}

require_env_file() {
  [ -f "${ENV_FILE}" ] || die "env file not found: ${ENV_FILE}
  Create it from the template:  cp ${REPO_ROOT}/.env.example ${ENV_FILE} && \$EDITOR ${ENV_FILE}
  (Set ENERGYCAP_ENV_FILE to point somewhere else.)"
}

require_data_dir() {
  [ -d "${DATA_DIR}" ] || die "data directory not found: ${DATA_DIR}
  Create it:  mkdir -p ${DATA_DIR}
  (Set ENERGYCAP_DATA_DIR to keep the spool somewhere else — e.g. an external disk.)"
  # launchd opens the plist's log files before exec'ing this script, so this
  # mkdir only helps the *next* boot. deploy/README.md tells you to do it once by hand.
  mkdir -p "${DATA_DIR}/logs" 2>/dev/null || true
}

# THE UID TRAP — MEASURED 2026-08-17 on container 1.2.2, and it does NOT bite.
#
# The image runs as uid 10001, and the host data dir is owned by the host user
# (uid 501, mode 755), so on a real filesystem uid 10001 could not write it.
# Apple's container maps the directory in over virtiofs, which does NOT enforce
# guest ownership: inside the container the mount presents as `drwxr-xr-x 8 0 0`
# — owned by root, not by the host uid — and a `touch /data/...` as uid 10001
# succeeds anyway. A full run then wrote spool.db, status.json and tokens/
# through the mount with no chown of any kind.
#
# So this is an informational NOTE, not a warning. It is kept because the
# host-side facts it prints are the first thing you would want if the mapping
# ever changes (a container release, a macOS release, an NFS or FileVault-backed
# path, or a `--user`/`--uid` override), and because the symptom is otherwise a
# confusing SQLite error at 3am: "permission denied" or "unable to open database
# file" on /data/spool.db, /data/status.json or /data/tokens/*.json.

# `stat` on a path that vanished mid-check must not take the whole script down
# under `set -e` (and an empty mode would make $((8#)) a fatal arithmetic error).
_stat_uid()  { stat -f '%u'  "$1" 2>/dev/null || printf '%s' "-1"; }
_stat_mode() { stat -f '%Lp' "$1" 2>/dev/null || printf '%s' "0"; }

check_data_writable() {
  local owner mode perm_bits complaint=""

  owner=$(_stat_uid "${DATA_DIR}")
  mode=$(_stat_mode "${DATA_DIR}")
  perm_bits=$((8#${mode:-0}))

  if [ "${owner}" != "${CONTAINER_UID}" ] && [ $((perm_bits & 2)) -eq 0 ]; then
    complaint="the directory itself is uid ${owner}, mode ${mode}"
  fi

  # Even a world-writable directory does not help if the files already in it are
  # mode 600 owned by someone else — which is exactly what happens after you have
  # run `uv run energycap ...` on the host, because token caches and status.json
  # are deliberately created 600 (CLAUDE.md cardinal rule 8).
  local f
  for f in "${DATA_DIR}/spool.db" "${DATA_DIR}/status.json" "${DATA_DIR}/tokens"; do
    [ -e "${f}" ] || continue
    owner=$(_stat_uid "${f}")
    mode=$(_stat_mode "${f}")
    perm_bits=$((8#${mode:-0}))
    if [ "${owner}" != "${CONTAINER_UID}" ] && [ $((perm_bits & 2)) -eq 0 ]; then
      complaint="${complaint}${complaint:+; }$(basename "${f}") is uid ${owner}, mode ${mode}"
    fi
  done

  [ -n "${complaint}" ] || return 0

  info "note: ${DATA_DIR} is not owned by the container's uid ${CONTAINER_UID} (${complaint}).
  That is expected and fine: virtiofs presents the mount as root-owned inside the
  container and does not enforce guest ownership, so uid ${CONTAINER_UID} writes it anyway
  (measured on container 1.2.2). Nothing to do.
  Only if the container ever DOES die with 'permission denied' or 'unable to open
  database file' on /data/spool.db, /data/status.json or /data/tokens/*.json:
      sudo chown -R ${CONTAINER_UID}:${CONTAINER_GID} ${DATA_DIR}
  (you would then need sudo to read the spool from the host), or, more bluntly:
      chmod -R a+rwX ${DATA_DIR}
  See deploy/README.md, 'The uid trap'."
}

# .env is read for exactly one thing: the port to publish. The sed only ever
# emits digits, so nothing else in that file can escape through here.
health_port() {
  local port=""
  if [ -f "${ENV_FILE}" ]; then
    port=$(sed -n 's/^[[:space:]]*HEALTH_PORT[[:space:]]*=[[:space:]]*\([0-9]\{1,5\}\).*$/\1/p' \
             "${ENV_FILE}" | tail -n 1)
  fi
  printf '%s' "${port:-8080}"
}

# SPOOL_DIR must be /data inside the container (it is the mount point), which is
# what docker-compose.yml pins with its `environment:` block.
#
# MEASURED 2026-08-17 on container 1.2.2: `-e SPOOL_DIR=/data` DOES take
# precedence over the same key in --env-file. A .env carrying the host-side
# `SPOOL_DIR=./data` (which is what you want for `uv run energycap run`) was
# overridden, and the container logged "spool": "/data/spool.db". So a
# host-shaped SPOOL_DIR in .env is normal and correct, and warning about it on
# every start was noise about a non-problem.
#
# The check is kept for the one case that IS a problem: a value that is neither
# /data nor an obviously host-side path would mean somebody intended something
# we are silently ignoring. Prints the key and the fact, never the value.
warn_on_spool_dir() {
  local raw
  raw=$(sed -n 's/^[[:space:]]*SPOOL_DIR[[:space:]]*=[[:space:]]*\(.*\)$/\1/p' \
          "${ENV_FILE}" 2>/dev/null | tail -n 1)
  [ -n "${raw}" ] || return 0

  # Trim trailing whitespace/CR only — never print or export the value.
  raw=${raw%%[[:space:]]}
  case "${raw}" in
    /data|/data/) return 0 ;;               # already container-shaped
    .|./*|~/*|"${HOME}"/*) return 0 ;;      # host-shaped: expected, -e overrides it
    *)
      warn "${ENV_FILE} sets SPOOL_DIR to an absolute path that is neither /data nor a
  host path. The wrapper passes -e SPOOL_DIR=/data, which wins, so that setting is
  being ignored inside the container. Remove it, or set it to /data, so the file
  says what actually happens."
      ;;
  esac
}

# WHAT APPLE'S --env-file PROMISES, AND WHAT IT DOES NOT.
#
# The documented contract is exactly: "key=value format, ignores # comments and
# blank lines". That is all. Docker's parser does MORE than that — it strips
# surrounding quotes, strips trailing whitespace, and (for `docker compose`)
# interpolates ${VAR}. Nothing says Apple's does any of it, and we have never run
# it, so a .env written to Docker's rules can silently land values like
#     S3_BUCKET="my-bucket"      -> literally  "my-bucket"  including the quotes
#     HEALTH_PORT=8080           -> "8080 "  with a trailing space, failing int()
#     TZ_LOCAL=America/... # zone -> "America/... # zone"
# The first two are a container that crash-loops; the third is worse, because
# LOCAL-date partitioning is derived from TZ_LOCAL (CLAUDE.md rule 4).
#
# The committed .env.example is already clean — every comment is on its own line,
# no value is quoted, none has trailing whitespace — so this fires only on a
# hand-edited .env. It reports the KEY and the LINE NUMBER and never the value:
# key names are all public (they are in .env.example); values are credentials.
warn_on_env_file_syntax() {
  local suspects
  suspects=$(awk '
    BEGIN { sq = sprintf("%c", 39); dq = sprintf("%c", 34) }
    /^[[:space:]]*#/  { next }
    /^[[:space:]]*$/  { next }
    {
      eq = index($0, "=")
      if (eq == 0) { printf "  line %d: no \"=\" on the line\n", NR; next }
      key = substr($0, 1, eq - 1)
      sub(/^[[:space:]]+/, "", key)
      value = substr($0, eq + 1)
      while (substr(value, 1, 1) == " " || substr(value, 1, 1) == "\t") value = substr(value, 2)
      first = substr(value, 1, 1)
      last  = substr(value, length(value), 1)
      if (key ~ /[[:space:]]/)
        printf "  line %d: key %s has whitespace in or after it\n", NR, key
      else if (first == dq || first == sq)
        printf "  line %d: %s has a quoted value\n", NR, key
      else if (value ~ /[[:space:]]#/)
        printf "  line %d: %s has an inline # comment\n", NR, key
      else if (last == " " || last == "\t")
        printf "  line %d: %s has trailing whitespace\n", NR, key
      else if (value ~ /\$[({]?[A-Za-z_]/)
        printf "  line %d: %s looks like it expects ${VAR} interpolation\n", NR, key
    }' "${ENV_FILE}" 2>/dev/null || true)

  [ -n "${suspects}" ] || return 0

  warn "${ENV_FILE} uses syntax that Docker tolerates but Apple's \`container\`
  does not promise to. Its --env-file contract is only \"key=value, ignores #
  comments and blank lines\" — no quote stripping, no trailing-space trimming,
  no \${VAR} interpolation. These lines would be passed through verbatim:
${suspects}
  Fix by unquoting the value, moving the comment to its own line, and deleting
  trailing whitespace. (Values are never printed here — only key names.)"
}

preflight() {
  require_cli
  require_system_running
  require_env_file
  require_data_dir
  warn_on_spool_dir
  warn_on_env_file_syntax
  check_data_writable
}

# ------------------------------------------------------------------ helpers

# NOTE: `container inspect` is documented as taking an <id>. Whether it also
# accepts the --name we set is NOT verified. If it does not, this is always false
# — which makes the already-exists guard below inert and `status` show no IP, but
# breaks nothing else. `container list --all` is the cross-check `status` prints.
container_exists() { container inspect "${NAME}" >/dev/null 2>&1; }

# Is <name> a subcommand this build of the CLI actually advertises? Used only for
# subcommands that are NOT in the reference we built against, so that we never
# invoke an invented command — we run what `container --help` says exists, or
# nothing. Echoes the first one that matches.
container_subcommand() {
  local help want
  help=$(container --help 2>&1 || true)
  for want in "$@"; do
    if printf '%s\n' "${help}" | grep -qE "^[[:space:]]*${want}([[:space:]]|$)"; then
      printf '%s' "${want}"
      return 0
    fi
  done
  return 1
}

# THE NAME-COLLISION WEDGE — the failure mode launchd turns from an annoyance
# into an outage.
#
# `--rm` is supposed to drop the container record when it exits. It will not have
# done so if the wrapper was SIGKILLed (launchd's ExitTimeOut expiring), if the
# machine lost power, or if the runtime itself died. The name then survives, and
# the next `container run --name energycap` fails with "already exists".
#
# Under compose that could not happen: `restart: unless-stopped` REUSES the
# container. Under KeepAlive we get a fresh `container run` every time, so a
# stale record means every single restart attempt fails, every ThrottleInterval,
# forever, until a human logs in — a permanent silent data outage caused by
# nothing worse than an ungraceful shutdown.
#
# So try to clear it, using `container stop` (documented) and then whichever of
# delete/rm/remove this CLI advertises (NOT documented in our reference — probed,
# never assumed). Returns non-zero only if the name is still taken afterwards.
clear_stale_container() {
  container_exists || return 0

  warn "a container named '${NAME}' is still registered (an unclean shutdown leaves
  one behind even with --rm). Trying to clear it before starting."
  container stop --time "${STOP_TIMEOUT}" "${NAME}" >/dev/null 2>&1 || true

  # --rm removal can lag the stop.
  local i
  for i in 1 2 3 4 5; do
    container_exists || return 0
    sleep 1
  done

  local rm_cmd
  if rm_cmd=$(container_subcommand delete rm remove); then
    info "removing the stopped container:  container ${rm_cmd} ${NAME}"
    container "${rm_cmd}" "${NAME}" >/dev/null 2>&1 || true
    for i in 1 2 3; do
      container_exists || return 0
      sleep 1
    done
  fi

  return 1
}

# `container inspect` is documented to emit JSON with
# .[0].networks[0].ipv4Address. Prefer jq; fall back to sed so this works on a
# stock macOS with no Homebrew. The address may carry a /prefix — strip it.
container_ip() {
  local json ip=""
  json=$(container inspect "${NAME}" 2>/dev/null) || return 1
  if command -v jq >/dev/null 2>&1; then
    ip=$(printf '%s' "${json}" | jq -r '.[0].networks[0].ipv4Address // empty' 2>/dev/null || true)
  fi
  if [ -z "${ip}" ]; then
    ip=$(printf '%s' "${json}" \
         | sed -n 's/.*"ipv4Address"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  fi
  ip=${ip%%/*}
  [ -n "${ip}" ] || return 1
  printf '%s' "${ip}"
}

# --------------------------------------------------------------- subcommands

cmd_build() {
  require_cli
  require_system_running
  # No --arch: the Dockerfile is deliberately architecture-neutral (its own
  # header says so) and `container` builds native arm64 on this host, which is
  # what we want. Forcing --arch amd64 would drag in emulation for no reason.
  info "building ${IMAGE} from ${REPO_ROOT}/Dockerfile"
  container build -t "${IMAGE}" -f "${REPO_ROOT}/Dockerfile" "${REPO_ROOT}"
  info "built ${IMAGE}"
  info "verify no secrets were baked in (see deploy/README.md, 'Verify the build'):"
  info "  the image must NOT contain /app/.env"
}

cmd_run() {
  local detach=0
  while [ $# -gt 0 ]; do
    case $1 in
      -d|--detach) detach=1; shift ;;
      -h|--help)   usage; exit 0 ;;
      *)           die "run: unknown option '$1' (only --detach is supported)" ;;
    esac
  done

  preflight

  if ! clear_stale_container; then
    die "a container named '${NAME}' already exists and could not be cleared.
  If it is still running, stop it:  $0 stop
  If it is stopped but still listed by \`container list --all\`, remove the record
  with this CLI's delete subcommand (see \`container --help\`).
  Under launchd this blocks EVERY restart until it is cleared — see deploy/README.md,
  'The name-collision wedge'."
  fi

  local port; port=$(health_port)

  # Argument order mirrors docker-compose.yml: env_file first, then the
  # `environment:` overrides that must be true inside the container regardless
  # of what the host env file says.
  local args=(
    run
    --name "${NAME}"
    --rm
    --env-file "${ENV_FILE}"
    -e SPOOL_DIR=/data
    -e TZ=UTC
    -e "HEALTH_PORT=${port}"
    -v "${DATA_DIR}:/data"
    -p "${port}:${port}"
  )

  # The blackstart inventory is the source of truth for circuit LABELS (PLAN.md
  # §9), and it lives outside this repo — so inside the container the path in
  # .env does not exist and every blackstart-labelled channel degrades to its
  # raw channel_id ("breaker_p19" instead of "Water heater"). Measured, not
  # theorised: the dashboard reported `blackstart inventory not joined
  # (DimBuildError)` on the first containerised run.
  #
  # Mount it read-only at a fixed in-container path and point the setting at it.
  # Read-only because this process has no business writing the inventory, and
  # because a bind mount of someone else's repo should not be writable by us.
  local inventory=""
  inventory=$(sed -n 's/^[[:space:]]*BLACKSTART_INVENTORY_PATH[[:space:]]*=[[:space:]]*\(.*\)$/\1/p' \
                "${ENV_FILE}" 2>/dev/null | tail -n 1)
  inventory=${inventory%%[[:space:]]}
  case "${inventory}" in "~/"*) inventory="${HOME}/${inventory#\~/}" ;; esac

  # NOTE: container 1.2.2 cannot bind-mount a single FILE — `--mount` on a file
  # fails with "path '...' is not a directory" (Docker allows it; this does not).
  # So mount the containing DIRECTORY read-only and point at the file inside it.
  if [ -n "${inventory}" ] && [ -f "${inventory}" ]; then
    local inv_dir inv_file
    inv_dir=$(cd "$(dirname "${inventory}")" && pwd)
    inv_file=$(basename "${inventory}")
    args+=( --mount "type=bind,source=${inv_dir},target=/inventory,readonly" )
    args+=( -e "BLACKSTART_INVENTORY_PATH=/inventory/${inv_file}" )
  elif [ -n "${inventory}" ]; then
    warn "BLACKSTART_INVENTORY_PATH in ${ENV_FILE} does not point at a file on this host,
  so it cannot be mounted. Channels that take their label from the blackstart
  inventory will fall back to their raw channel_id (e.g. 'breaker_p19' rather
  than 'Water heater'). Everything else is unaffected."
  fi

  if [ "${detach}" -eq 1 ]; then
    # Interactive convenience ONLY. Never put --detach in the LaunchAgent: see
    # the comment on the foreground path below.
    info "starting ${NAME} detached (image ${IMAGE}, /data <- ${DATA_DIR}, port ${port})"
    container "${args[@]}" --detach "${IMAGE}" run
    info "follow it with:  $0 logs -f"
    return 0
  fi

  # ------------------------------------------------------------------------
  # FOREGROUND IS THE DEFAULT AND THAT IS LOAD-BEARING. DO NOT "FIX" THIS.
  #
  # launchd supervises a PROCESS, not a container. This script is the process
  # in the LaunchAgent's ProgramArguments, so it must stay alive for exactly as
  # long as the container does — that is what makes KeepAlive the replacement
  # for compose's `restart: unless-stopped`.
  #
  # If you add --detach here, `container run` returns immediately, this script
  # exits 0, launchd sees its job finish, and KeepAlive restarts it — forever,
  # several times a second but for ThrottleInterval. That is a hot loop, not a
  # supervisor, and the collector ends up racing itself over one SQLite spool.
  # ------------------------------------------------------------------------
  info "starting ${NAME} in the foreground (image ${IMAGE}, /data <- ${DATA_DIR}, port ${port})"

  # Run as a background job of *this* shell (not `exec`) purely so the trap can
  # fire: launchd's `bootout`/`kickstart -k` sends SIGTERM to this script, and
  # we have to turn that into a graceful `container stop` so the poll loop can
  # close the spool transaction it is holding (compose did this with
  # stop_grace_period: 30s; the plist's ExitTimeOut is the equivalent).
  # The trap is installed BEFORE the child is started, and CHILD_PID is a global
  # rather than a local. Both matter: launchd can send SIGTERM at any instant
  # (`bootout` during a rebuild, a logout mid-start), and a SIGTERM landing in the
  # window between `&` and `trap` would kill this shell outright, leaving the
  # container running and its name taken — which is exactly the wedge above.
  CHILD_PID=""

  # shellcheck disable=SC2317  # invoked via trap
  _on_signal() {
    info "signal received; stopping ${NAME} (grace ${STOP_TIMEOUT}s)"
    container stop --time "${STOP_TIMEOUT}" "${NAME}" >/dev/null 2>&1 || true
    if [ -n "${CHILD_PID:-}" ]; then
      wait "${CHILD_PID}" 2>/dev/null || true
    fi
    exit 0
  }
  trap _on_signal TERM INT

  container "${args[@]}" "${IMAGE}" run &
  CHILD_PID=$!

  local status=0
  wait "${CHILD_PID}" || status=$?
  info "container exited with status ${status}"
  # Exit non-zero on a crash so it is visible in the launchd log; KeepAlive
  # restarts either way.
  return "${status}"
}

cmd_stop() {
  require_cli
  require_system_running
  if ! container_exists; then
    info "no container named '${NAME}'"
    return 0
  fi
  info "stopping ${NAME} (grace ${STOP_TIMEOUT}s)"
  container stop --time "${STOP_TIMEOUT}" "${NAME}"
}

cmd_restart() {
  # Note: under launchd the supervisor owns the lifecycle, so the honest restart
  # there is  launchctl kickstart -k gui/$(id -u)/com.duckbillhq.energycap
  # This path is for a hand-run container.
  cmd_stop
  cmd_run "$@"
}

cmd_logs() {
  require_cli
  require_system_running
  container_exists || die "no container named '${NAME}' — is it running? (\`$0 status\`)"
  # Passthrough: --follow/-f, -n <n>, --boot.
  container logs "$@" "${NAME}"
}

cmd_status() {
  require_cli
  local ip=""

  printf '== container subsystem ==\n'
  container system status 2>&1 || true

  printf '\n== containers ==\n'
  container list --all 2>&1 || true

  printf '\n== %s ==\n' "${NAME}"
  if ! container_exists; then
    printf 'not present\n'
  else
    ip=$(container_ip || true)
    printf 'ip: %s\n' "${ip:-<none>}"
  fi

  local port; port=$(health_port)
  printf '\n== /healthz (port %s) ==\n' "${port}"
  if command -v curl >/dev/null 2>&1; then
    local url shown=0
    for url in "http://127.0.0.1:${port}/healthz" "${ip:+http://${ip}:${port}/healthz}"; do
      [ -n "${url}" ] || continue
      printf -- '-- %s\n' "${url}"
      # -sS: quiet but show real errors. Prints the HTTP status on its own line;
      # /healthz returns 503 when a poller's last success is older than 3x its
      # interval (PLAN.md §11), so a 200 here is the liveness signal that
      # compose's healthcheck used to give us automatically.
      if curl -sS -m 5 -w '\nHTTP %{http_code}\n' "${url}" 2>&1; then
        shown=1
        break
      fi
    done
    [ "${shown}" -eq 1 ] || printf 'unreachable\n'
  else
    printf 'curl not installed; try: curl -s localhost:%s/healthz\n' "${port}"
  fi

  printf '\n== data dir ==\n'
  printf 'path: %s\n' "${DATA_DIR}"
  if [ -d "${DATA_DIR}" ]; then
    ls -ld "${DATA_DIR}"
    ls -l "${DATA_DIR}/spool.db" "${DATA_DIR}/status.json" 2>/dev/null || true
  else
    printf 'missing\n'
  fi
}

# `shell` needs to replace the image's ENTRYPOINT (["energycap"]), and we have
# not verified that this build of the CLI has --entrypoint or an `exec`
# subcommand. So probe for them and say plainly what is missing rather than
# emitting a baffling "energycap: unknown command /bin/sh".
cmd_shell() {
  require_cli
  require_system_running

  # Probe the top-level help for an `exec` subcommand rather than trusting the
  # exit status of `container exec --help`, which may well print general help
  # and exit 0 for a subcommand that does not exist.
  if container_exists && container --help 2>&1 | grep -qE '^[[:space:]]*exec([[:space:]]|$)'; then
    info "attaching to the running ${NAME}"
    # -i/-t are Docker spellings and are NOT in the documented flag set; if this
    # errors, check `container exec --help` for this build's equivalents.
    container exec -it "${NAME}" /bin/sh
    return $?
  fi

  if container run --help 2>&1 | grep -q -- '--entrypoint'; then
    require_env_file
    require_data_dir
    info "starting a throwaway shell in ${IMAGE}"
    container run --rm -it --entrypoint /bin/sh \
      --env-file "${ENV_FILE}" -e SPOOL_DIR=/data -v "${DATA_DIR}:/data" "${IMAGE}"
    return $?
  fi

  die "this build of the \`container\` CLI exposes neither \`container exec\` nor
  \`container run --entrypoint\`, and the image's ENTRYPOINT is the energycap CLI,
  so there is no way to get a shell from here. Check \`container --help\`.
  Meanwhile every energycap subcommand is reachable directly, e.g.:
      container run --rm --env-file ${ENV_FILE} -e SPOOL_DIR=/data \\
          -v ${DATA_DIR}:/data ${IMAGE} rollup --start 2026-08-01 --end 2026-08-16"
}

usage() {
  cat <<EOF
energycap under Apple's \`container\` — the compose replacement on this Mac.

  $0 build              build ${IMAGE} from ./Dockerfile (native arm64)
  $0 run [--detach]     run the collector; FOREGROUND by default (launchd needs that)
  $0 stop               graceful stop (${STOP_TIMEOUT}s)
  $0 restart [--detach] stop, then run
  $0 logs [-f] [-n N]   container logs
  $0 status             subsystem + container state + IP + /healthz + data dir
  $0 shell              a shell in the image (needs exec or --entrypoint support)

data dir: ${DATA_DIR}   (ENERGYCAP_DATA_DIR)
env file: ${ENV_FILE}   (ENERGYCAP_ENV_FILE)
image:    ${IMAGE}      (ENERGYCAP_IMAGE)

Supervision is launchd, not this script: see deploy/README.md.
Docker remains supported and unchanged:  docker compose up -d
EOF
}

# --------------------------------------------------------------------- main

main() {
  local sub=${1:-}
  [ $# -gt 0 ] && shift || true
  case ${sub} in
    build)          cmd_build "$@" ;;
    run)            cmd_run "$@" ;;
    stop)           cmd_stop "$@" ;;
    restart)        cmd_restart "$@" ;;
    logs)           cmd_logs "$@" ;;
    status)         cmd_status "$@" ;;
    shell)          cmd_shell "$@" ;;
    -h|--help|help) usage ;;
    "")             usage; exit 2 ;;
    *)              printf 'energycap: error: unknown subcommand %s\n\n' "${sub}" >&2
                    usage >&2; exit 2 ;;
  esac
}

main "$@"
