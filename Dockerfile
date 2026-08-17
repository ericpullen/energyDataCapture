# energycap — one container, one long-running process (PLAN.md §5).
#
# Build/run target is an Apple-silicon Mac Mini, so nothing here pins an
# architecture: the base image, the uv binary and every wheel we install are
# multi-arch and resolve to linux/arm64 on that host (and to linux/amd64 if this
# is ever built on an Intel box or in CI). Do NOT add `--platform=linux/amd64`.
#
#   docker compose build && docker compose up -d
#
# Layer strategy: pyproject.toml + uv.lock are copied before the source tree so
# the dependency layer is reused across ordinary code edits.

# ---------------------------------------------------------------- build stage
FROM python:3.12-slim AS builder

# uv ships as a static binary in its own multi-arch image; copying it beats
# curl|sh here (python:3.12-slim has no curl, and this keeps the layer cacheable).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.12

WORKDIR /app

# 1) Dependencies only. Cached until pyproject.toml or uv.lock actually change.
#    --frozen: never re-resolve inside the image; uv.lock is committed and is the
#    single source of truth. If someone edits pyproject.toml without re-locking,
#    this line fails the build loudly instead of silently drifting.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-editable

# 2) The project itself. README.md/LICENSE are referenced by pyproject metadata,
#    so the wheel build needs them present.
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

# -------------------------------------------------------------- runtime stage
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="energycap" \
      org.opencontainers.image.description="Household energy + HVAC time-series capture (Leviton + Bryant -> SQLite spool -> Parquet in S3)" \
      org.opencontainers.image.licenses="MIT"

# Container clock stays UTC on purpose: every UTC<->local conversion goes through
# energy_capture.timeutil using TZ_LOCAL, never through the process timezone.
# TZ_LOCAL (America/Kentucky/Louisville) resolves via the `tzdata` PyPI wheel,
# which is a hard runtime dependency in pyproject.toml precisely so this image
# does not depend on the base image shipping /usr/share/zoneinfo. The RUN check
# below fails the build if that ever stops being true.
# HOME is set explicitly rather than left to Docker's discretion: DuckDB caches
# its httpfs extension under $HOME/.duckdb (baked in below) and boto3 looks for
# ~/.aws there, which is the path docker-compose.yml's optional mount assumes.
ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    HOME=/home/energycap \
    SPOOL_DIR=/data \
    HEALTH_PORT=8080

# Non-root. A fixed uid keeps bind-mount ownership predictable on the host.
# /data is created and chowned in the image so a *fresh named volume* inherits
# the right ownership when Docker seeds it. (Bind mounts keep host ownership —
# see docker-compose.yml.)
RUN set -eux; \
    groupadd --system --gid 10001 energycap; \
    useradd --system --uid 10001 --gid 10001 \
            --create-home --home-dir /home/energycap --shell /usr/sbin/nologin energycap; \
    mkdir -p /data /app; \
    chown -R energycap:energycap /data /app

WORKDIR /app

# Source tree (src/, config/channel_map.json, rollup SQL, README) then the venv.
# .dockerignore keeps this to the few files that matter.
COPY --chown=energycap:energycap . /app
COPY --from=builder --chown=energycap:energycap /app/.venv /app/.venv

# Build-time proof that the CLI imports and that the local timezone resolves.
# A missing tz database here would silently mis-assign every LOCAL-date
# partition (CLAUDE.md cardinal rule 4), so it is worth failing the build over.
RUN python -c "\
from zoneinfo import ZoneInfo; \
from datetime import datetime, timezone; \
tz = ZoneInfo('America/Kentucky/Louisville'); \
assert datetime(2026, 8, 16, 18, tzinfo=timezone.utc).astimezone(tz).hour == 14, 'tz database is wrong'; \
import energy_capture; print('tz ok, energy_capture', energy_capture.__version__)"

USER energycap

# Pre-install DuckDB's httpfs extension into the image.
#
# stages/rollup.py runs `INSTALL httpfs` before it reads S3, and INSTALL fetches
# the extension over the network on first use. Discovering that at 01:30 — from
# a container that may be behind a proxy, or on a night the extension repository
# is having a bad minute — is not a thing to leave to chance, so it is baked in
# and the build fails loudly if it cannot be. It must run as `energycap`: DuckDB
# caches extensions under $HOME/.duckdb, and one installed as root would be
# invisible to the runtime user.
RUN python -c "\
import duckdb; \
con = duckdb.connect(); \
con.execute('INSTALL httpfs'); \
con.execute('LOAD httpfs'); \
print('duckdb httpfs ok', duckdb.__version__)"

EXPOSE 8080

# Healthcheck also declared in docker-compose.yml (compose is the source of truth
# for the deployed unit); this one covers plain `docker run`. python:3.12-slim has
# no curl/wget, so probe with the interpreter that is guaranteed to be here.
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python -c "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('HEALTH_PORT','8080')+'/healthz',timeout=5)"

# ENTRYPOINT is the CLI, CMD is the default subcommand, so the long-running
# process is `energycap run` while every other stage stays reachable:
#   docker compose run --rm energycap rollup --start 2026-08-01 --end 2026-08-16
STOPSIGNAL SIGTERM
ENTRYPOINT ["energycap"]
CMD ["run"]
