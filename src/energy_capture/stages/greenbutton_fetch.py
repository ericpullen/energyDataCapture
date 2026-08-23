"""``energycap fetch-greenbutton`` — the meter series, without the manual click.

The automated half of PLAN.md §13. ``import-greenbutton`` reads a file a human
downloaded; this fetches the same ESPI over the authorised Connect API and lands
it through the *same parser and the same writer*, so the two paths cannot drift
apart in how a reading becomes a row.

That sharing is the whole design. Everything specific to Connect lives here and
in :mod:`energy_capture.sources.lge_auth`; everything about what an ESPI
document *means* — units, scale, flow direction, which UsagePoint, interval
start — stays in :mod:`energy_capture.stages.greenbutton`, already tested
against a real LG&E export.

Idempotency and late data
-------------------------
The month file is regenerated from ``existing + fetched`` on the canonical
dedupe key with the freshly fetched row winning, so re-running over an
overlapping range converges. That is not a nicety: MyMeter publishes recent
intervals and revises them, and the default window is deliberately wide enough
(``--start`` D-3) to re-read and correct them, the same way the Bryant daily
energy stage re-reads day2.

What it will not do
-------------------
Write a partial day and call it complete. If the custodian returns nothing for a
range, that is zero rows — not zeros — and the stage says so.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from energy_capture import model, timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger
from energy_capture.sources.lge_auth import (
    DEFAULT_TIMEOUT,
    LgeAuth,
    LgeAuthError,
    LgeTransientError,
)
from energy_capture.stages import greenbutton

STAGE = "fetch_greenbutton"
log = get_logger(STAGE)

__all__ = ["fetch_espi", "run"]


def _espi_instant(moment: datetime) -> str:
    """``2026-08-16T04:00:00Z`` — what LG&E's filter actually accepts.

    The ESPI spec says ``published-min``/``published-max`` are **epoch seconds**.
    LG&E's implementation is not: measured 2026-08-18 against the live endpoint,

    ==========================================  ==========================
    ``published-min=1755230400`` (spec)         **400** Bad Request
    ``published-min=2026-08-15`` (date only)    **400**
    ``published-min=2026-08-15T00:00:00`` (no Z) **400**
    ``publishedMin=…`` (camelCase)              200, **filter ignored**
    ``published-min=2026-08-15T00:00:00Z``      200, **filtered**
    ==========================================  ==========================

    The camelCase row is the dangerous one: it answers 200 with the *entire*
    authorised history (49 MB against 415 KB for four days) and looks like it
    worked. Getting this wrong is not a failed request, it is a daily job quietly
    downloading fifty megabytes and a fetch window that means nothing.

    No microseconds, and an explicit ``Z``.
    """
    return timeutil.ensure_utc(moment).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_espi(
    *,
    start: date,
    end: date,
    auth: LgeAuth | None = None,
    client: httpx.Client | None = None,
) -> str:
    """GET the authorised subscription's ESPI feed for a LOCAL date range.

    The window is sent as ESPI's ``published-min``/``published-max`` (epoch
    seconds), derived from the local dates through :mod:`~energy_capture.timeutil`
    so the range means local midnight to local midnight — the same boundary every
    partition in this project uses, including across a 23- or 25-hour DST day.
    """
    resolved = auth or LgeAuth()
    token = resolved.access_token()
    base = resolved.resource_base(token)

    window_start, _ = timeutil.local_day_bounds_utc(start)
    _, window_end = timeutil.local_day_bounds_utc(end)
    params = {
        "published-min": _espi_instant(window_start),
        "published-max": _espi_instant(window_end),
    }

    owned = client is None
    http = client or httpx.Client(timeout=DEFAULT_TIMEOUT)
    try:
        response = http.get(
            base,
            params=params,
            headers={
                "Authorization": token.authorization,
                "Accept": "application/atom+xml",
            },
        )
    except httpx.HTTPError as exc:
        raise LgeTransientError(f"LG&E resource endpoint unreachable: {exc}") from exc
    finally:
        if owned:
            http.close()

    if response.status_code in (401, 403):
        raise LgeAuthError(
            f"LG&E rejected the access token ({response.status_code}) for {base} — "
            "re-authorise with `energycap greenbutton-authorize`"
        )
    if response.status_code >= 500:
        raise LgeTransientError(
            f"LG&E resource endpoint returned {response.status_code}"
        )
    if response.status_code != 200:
        raise LgeTransientError(
            f"unexpected {response.status_code} from {base}"
        )
    return response.text


def run(
    *,
    start: date,
    end: date,
    source: str = model.SOURCE_LGE,
    channel_id: str = "electric_main",
    out_dir: Path | None = None,
    bucket: str | None = None,
    dry_run: bool = False,
    auth: LgeAuth | None = None,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """``energycap fetch-greenbutton --start … --end …``."""
    settings = get_settings()
    document = fetch_espi(start=start, end=end, auth=auth, client=client)

    parsed = greenbutton.parse_espi_xml(
        document, source=source, channel_id=channel_id
    )
    log.info(
        "greenbutton_fetched",
        start=start.isoformat(),
        end=end.isoformat(),
        bytes=len(document),
        **parsed.to_dict(),
    )
    for note in parsed.notes:
        log.warning("greenbutton_note", note=note)

    destination = Path(out_dir) if out_dir else settings.spool_dir / "meter"
    written = greenbutton.write_months(
        parsed, destination, source=source, dry_run=dry_run
    )

    # A SCHEDULED fetch mirrors automatically when a bucket exists — the same
    # rule `fetch-daily` follows (stages/daily.py, and stages/dailystore's
    # docstring). Without this the nightly `greenbutton_daily` job passed no
    # bucket and the meter dataset never reached S3 at all even with S3_BUCKET
    # set, which is exactly the failure DEVIATIONS.md #173 describes for Bryant.
    # `import-greenbutton` deliberately does NOT do this: an import is a manual
    # act on a file a human just downloaded, and it must not fan out by surprise.
    from energy_capture.aws import s3io  # local: keeps boto3 off the import path

    target_bucket = bucket if bucket is not None else s3io.configured_bucket()

    summary: dict[str, Any] = {
        "fetched_at": timeutil.format_utc(datetime.now(UTC)),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "files": written,
        "dry_run": dry_run,
        **parsed.to_dict(),
    }
    if target_bucket and not dry_run:
        summary["s3"] = greenbutton._upload_months(
            destination, written, target_bucket, source
        )
    elif dry_run:
        summary["s3"] = "dry run"
    else:
        summary["s3"] = "no bucket configured (local only)"
    return summary
