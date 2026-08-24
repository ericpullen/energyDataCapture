#!/usr/bin/env -S uv run python
"""DEVIATIONS #180 — is Panel B's latched zero a stale store, or a real reading?

The question, stated so the answer is falsifiable
-------------------------------------------------
Panel B's feed CTs report exact ``0.0`` for minutes at a time while that panel's
own breakers report real load. Two explanations survive:

**A. The WebSocket store is stale.** The hub stops publishing the CT field, and
``LEVITON_INGEST=hybrid`` gates freshness on the *connection* rather than on how
old a field is, so the store keeps handing out the last value it saw — a ``0``.
Then an independent REST read at the same moment shows a NON-ZERO current.

**B. The hub really is reporting zero.** A clamp or firmware under-range with
hysteresis. Then REST shows ``0`` too, and the fix is not in our store at all.

So the probe pairs, every cycle:

* ``GET /healthz`` on the production collector — the WS store's per-field
  ``age_s``. Values are not published there, only ages, and the age is the point:
  a field the hub has stopped mentioning has a climbing age.
* an **independent REST read** of the same hubs from this machine — the ground
  truth the store is supposed to be tracking.

Disagreement on a channel whose store-age is large is explanation A. Agreement at
zero is explanation B.

Why this is safe to run beside production
------------------------------------------
It opens **no WebSocket** and sends **no bandwidth keepalive** — the keepalive is
the one Leviton call that changes hub behaviour, and this never issues it. It
logs in once and reuses the cached token from its own ``--spool-dir``, so it adds
one session, not one per cycle (CLAUDE.md: never log in more than once per 10s).
It writes no rows to any spool and touches no production state; the only side
effect is REST reads, at the same cadence production already uses.

When to run it
--------------
It must span **22:00–02:00 local**. A latch cannot be observed while the panel is
busy: Panel B's only loads are the heat pump, the cooktop and the double oven,
and in the evening the feed carries several amps and both sources agree to three
decimals. That is exactly why the fault survived six days of clean-looking data.

    scripts/latch-probe.py --hours 8 --out data/latch-probe.jsonl

Then:

    scripts/latch-probe.py --analyse data/latch-probe.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

#: The hub whose feed CTs latch. Panel A (``1000_0046_1D52``) is sampled too, as
#: the control: its feed never drops below 2.455 A, so it should never latch.
PANEL_B = "1000_0046_1D48"


def _now() -> str:
    return datetime.now(UTC).isoformat()


async def rest_snapshot(adapter: Any, residence_ids: tuple[int, ...]) -> dict[str, Any]:
    """One REST read, flattened to ``{device: {channel: {metric: value}}}``."""
    snapshots = await adapter.fetch_snapshot(residence_ids)
    out: dict[str, Any] = {}
    for snap in snapshots:
        hub_id = snap.hub.device_id
        channels: dict[str, Any] = {}
        for ct in snap.cts:
            # channel/position naming mirrors sources/leviton.py's mapper; the
            # probe only needs a stable key, not the production channel_id.
            key = f"ct_{getattr(ct, 'channel', '?')}"
            channels[f"{key}_a"] = {
                "amps": getattr(ct, "rms_current", None),
                "watts": getattr(ct, "active_power", None),
            }
            channels[f"{key}_b"] = {
                "amps": getattr(ct, "rms_current_2", None),
                "watts": getattr(ct, "active_power_2", None),
            }
        breaker_w = 0.0
        for br in snap.breakers:
            for field in ("power", "power_2"):
                value = getattr(br, field, None)
                if isinstance(value, (int, float)):
                    breaker_w += float(value)
        channels["_breaker_total_w"] = breaker_w
        out[hub_id] = channels
    return out


def healthz_ages(body: dict[str, Any]) -> dict[str, Any]:
    """Per-field ``age_s`` for every CT object the WS store tracks."""
    ws = body.get("leviton_ws") or {}
    objects = ws.get("objects") or {}
    ages: dict[str, Any] = {}
    for name, obj in objects.items():
        if not name.startswith("IotCt"):
            continue
        fields = obj.get("fields") or {}
        ages[name] = {
            "object_age_s": obj.get("age_s"),
            **{
                field: round(info.get("age_s", 0.0), 1)
                for field, info in fields.items()
                if "Current" in field or "Power" in field
            },
        }
    return {
        "ct_field_ages": ages,
        "drift": (body.get("leviton_ingest") or {}).get("last_reconcile_drift"),
        "hub_silence_s": ws.get("hub_silence_s"),
        "value_source": (body.get("leviton_ingest") or {}).get("value_source"),
        "connected": ws.get("connected"),
    }


async def sample(healthz_url: str, adapter: Any, residence_ids: tuple[int, ...]) -> dict:
    row: dict[str, Any] = {"ts_utc": _now()}
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(healthz_url)
        row["healthz"] = healthz_ages(response.json())
    except Exception as exc:  # noqa: BLE001 - a probe must not die
        row["healthz_error"] = f"{type(exc).__name__}: {exc}"
    try:
        row["rest"] = await rest_snapshot(adapter, residence_ids)
    except Exception as exc:  # noqa: BLE001
        row["rest_error"] = f"{type(exc).__name__}: {exc}"
    return row


async def collect(args: argparse.Namespace) -> None:
    from energy_capture.config import get_settings
    from energy_capture.sources.leviton import LevitonAdapter

    settings = get_settings()
    # Its OWN token cache, deliberately: sharing production's would mean two
    # processes racing the same file, and this must not be able to disturb the
    # collector's session.
    token_path = Path(args.token_cache)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    adapter = LevitonAdapter(
        username=settings.require("leviton_username"),
        password=settings.leviton_password.get_secret_value(),
        token_path=token_path,
    )
    try:
        # `start()` is the login/token-cache entry point; without it every call
        # fails with "Not authenticated".
        await adapter.start()
        residence_ids = await adapter.fetch_residence_ids()
        print(f"# residences: {residence_ids}", file=sys.stderr)

        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        deadline = asyncio.get_running_loop().time() + args.hours * 3600
        n = 0
        with out.open("a") as handle:
            while asyncio.get_running_loop().time() < deadline:
                row = await sample(args.healthz, adapter, residence_ids)
                handle.write(json.dumps(row) + "\n")
                handle.flush()
                n += 1
                if n % 20 == 0:
                    print(f"# {n} samples, {_now()}", file=sys.stderr)
                await asyncio.sleep(args.interval)
        print(f"# done: {n} samples -> {out}", file=sys.stderr)
    finally:
        await adapter.close()


def analyse(path: Path) -> None:
    """Answer the question the probe was run to answer."""
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        print("no samples")
        return

    print(f"{len(rows)} samples, {rows[0]['ts_utc']} .. {rows[-1]['ts_utc']}\n")

    zero_rest = 0
    zero_with_load = 0
    stale_ages: list[float] = []
    verdicts: dict[str, int] = defaultdict(int)

    for row in rows:
        rest = (row.get("rest") or {}).get(PANEL_B)
        if not rest:
            continue
        feeds = {
            name: metrics
            for name, metrics in rest.items()
            if name.startswith("ct_") and isinstance(metrics, dict)
        }
        amps = [
            metrics.get("amps")
            for metrics in feeds.values()
            if isinstance(metrics.get("amps"), (int, float))
        ]
        if not amps:
            continue
        breaker_w = rest.get("_breaker_total_w") or 0.0
        rest_is_zero = all(a == 0.0 for a in amps)

        ages = (row.get("healthz") or {}).get("ct_field_ages") or {}
        worst_age = max(
            (
                v
                for obj in ages.values()
                for k, v in obj.items()
                if k != "object_age_s" and isinstance(v, (int, float))
            ),
            default=0.0,
        )
        stale_ages.append(worst_age)

        if rest_is_zero:
            zero_rest += 1
            if breaker_w > 300:
                zero_with_load += 1
                verdicts["B: REST also zero WHILE breakers carry >300W"] += 1
            else:
                verdicts["ambiguous: everything quiet, both zero"] += 1
        elif worst_age > 120:
            verdicts["A: REST non-zero while the store's field is stale >120s"] += 1
        else:
            verdicts["clean: REST non-zero, store fresh"] += 1

    print("Panel B feed CTs, REST ground truth vs the WS store's field age:\n")
    for verdict, count in sorted(verdicts.items(), key=lambda kv: -kv[1]):
        print(f"  {count:>6}  {verdict}")
    print(f"\n  REST read exactly 0 A on all feeds: {zero_rest} samples")
    print(f"  ...of those, with >300 W of breaker load: {zero_with_load}")
    if stale_ages:
        stale_ages.sort()
        print(
            f"  WS CT field age: median {stale_ages[len(stale_ages) // 2]:.0f}s, "
            f"max {stale_ages[-1]:.0f}s"
        )
    print(
        "\nRead it this way:\n"
        "  many 'A' rows  -> the store is serving values nobody refreshes; the\n"
        "                    staleness-timeout fix is the right layer.\n"
        "  many 'B' rows  -> the hub itself reports zero under load; our store is\n"
        "                    innocent and the fix is query-time or hardware.\n"
        "  only 'clean'/'ambiguous' -> the window did not contain a latch. Re-run\n"
        "                    across 22:00-02:00 local; a busy panel cannot latch."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--healthz", default="http://13.219.164.226:8080/healthz")
    parser.add_argument("--out", default="data/latch-probe.jsonl")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--hours", type=float, default=8.0)
    parser.add_argument(
        "--token-cache",
        default="data/latch-probe-tokens/leviton.json",
        help="Probe's own Leviton token cache; never production's.",
    )
    parser.add_argument("--analyse", metavar="FILE")
    args = parser.parse_args()

    if args.analyse:
        analyse(Path(args.analyse))
        return
    asyncio.run(collect(args))


if __name__ == "__main__":
    main()
