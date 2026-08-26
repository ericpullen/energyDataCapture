#!/usr/bin/env python3
"""Compare the hub's identity NOW against the pre-reset snapshot.

A factory reset can change three things that this archive keys on, and every one
of them fails SILENTLY -- the collector keeps writing rows, the dashboard keeps
rendering, and the numbers quietly mean something else:

* ``device_id`` — the hub serial. Every historical Panel B row hangs off it, and
  ``config/channel_map.json`` maps it by hand. A new id orphans the history.
* **breaker positions** — ``channel_id`` is ``breaker_p{position}``, keyed on the
  physical slot precisely because fw >= 2.2.0 mutates the API's breaker ids. Same
  positions means history stays continuous; shifted positions means one
  ``channel_id`` silently covers two different circuits.
* **CT channel and usage type** — ``historyview`` calls ``ct_1_*`` the panel feed
  and every other ``ct_*`` a subfeed. Swap channel 1 and 2, or move
  GRID_POWER/SUB_PANEL, and the nesting hierarchy misclassifies: every Panel B
  total, and the meter comparison built on it, is then wrong.

Run inside the container so it uses the same settings and token cache:

    docker compose run --rm -T \
      -v $PWD/data/panelb-pre-reset.json:/snapshot.json:ro \
      -v $PWD/scripts/panelb-reset-check.py:/check.py:ro \
      --entrypoint python energycap /check.py /snapshot.json
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path

from energy_capture.config import get_settings
from energy_capture.sources.leviton import LevitonAdapter

HUB = "1000_0046_1D48"


def positions(breakers) -> list[int]:
    out = set()
    for b in breakers:
        m = re.search(r"position=(\d+)", str(b))
        if m:
            out.add(int(m.group(1)))
    return sorted(out)


def ct_layout(cts) -> list[tuple]:
    return sorted((c["channel"], c["usage_type"]) for c in cts)


async def live() -> dict:
    s = get_settings()
    a = LevitonAdapter(
        username=s.require("leviton_username"),
        password=s.leviton_password.get_secret_value(),
        token_path=Path("/data/tokens/leviton.json"),
    )
    await a.start()
    try:
        for snap in await a.fetch_snapshot(await a.fetch_residence_ids()):
            if snap.hub.device_id != HUB:
                continue
            return {
                "device_id": snap.hub.device_id,
                "cts": [
                    {"channel": c.channel, "usage_type": c.usage_type, "api_id": c.api_id,
                     "watts_a": c.active_power, "watts_b": c.active_power_2}
                    for c in snap.cts
                ],
                "breakers": [str(b) for b in snap.breakers],
            }
    finally:
        await a.close()
    return {}


def main() -> int:
    snapshot = json.loads(Path(sys.argv[1] if len(sys.argv) > 1 else "/snapshot.json").read_text())
    before = next(
        (h for h in snapshot["live_devices"] if h["device_id"] == HUB), None
    )
    if before is None:
        print(f"FAIL: {HUB} is not in the snapshot")
        return 2

    after = asyncio.run(live())
    if not after:
        print(f"FAIL: {HUB} did not appear in a live read at all.")
        print("      The hub has not rejoined, or it came back under a NEW device_id.")
        print("      Run `energycap discover` and compare by hand before trusting anything.")
        return 2

    problems = 0

    def check(name: str, old, new, why: str) -> None:
        nonlocal problems
        if old == new:
            print(f"  OK        {name}: {new}")
        else:
            problems += 1
            print(f"  CHANGED   {name}")
            print(f"              before: {old}")
            print(f"              after : {new}")
            print(f"              -> {why}")

    print(f"Panel B hub {HUB}, before vs after\n")
    check("device_id", before["device_id"], after["device_id"],
          "history is orphaned; add the new id to config/channel_map.json "
          "and expect a discontinuity in every Panel B series")
    check("CT channel/usage layout", ct_layout(before["cts"]), ct_layout(after["cts"]),
          "historyview's feed/subfeed classification is now wrong; fix the "
          "wiring or the map BEFORE trusting a panel total")
    check("breaker positions", positions(before["breakers"]), positions(after["breakers"]),
          "breaker_p{position} channel_ids no longer mean the same circuits; "
          "history is silently corrupted from here on")

    print("\n  (CT api_ids are expected to change and are not checked: this "
          "project keys on channel and position, never on API ids.)")
    print("\nlive CT readings right now:")
    for c in sorted(after["cts"], key=lambda c: c["channel"]):
        print(f"  channel {c['channel']:<3} {str(c['usage_type']):<12} "
              f"A={c['watts_a']}  B={c['watts_b']}")

    print()
    if problems:
        print(f"{problems} thing(s) changed. Do NOT trust new Panel B numbers until resolved.")
        return 1
    print("Identity and layout are unchanged. The archive stays continuous.")
    print("Now prove the CTs actually track load -- switch a known subpanel load")
    print("on and off and watch the readings above move.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
