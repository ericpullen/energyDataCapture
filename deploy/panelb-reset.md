# Factory-resetting the Panel B hub — before and after

Hub `1000_0046_1D48`. Written 2026-08-26, before the reset, while the fault was
live and reproducible.

## Why we are doing this

Since 2026-08-18 the two CT pairs on this hub have been latching: reporting one
value, unchanging, for hours, with a full `sample_count` and a plausible number.
184 such hours are recorded. Over the 106 hours from 08-22:

| pattern | hours |
|---|---|
| both CT channels latched | 25 |
| **feed latched alone** | **0** |
| subpanel latched alone | 27 |
| clean | 54 |

The feed *never* latches without the subpanel also latching. That containment,
plus stuck values that repeat across days (feed pins at ~489 W on two separate
days), points at hub-level retained state rather than a bad clamp — a single
faulty CT cannot take channel 1 down with it. The other hub, `1000_0046_1D52`,
is healthy across the same window and is the control.

A live REST read returns the same latched values the WebSocket store holds, so
this is **not** our collector caching anything. The stuck value is at the hub or
in Leviton's cloud. Nothing in this codebase can fix it, which is why the next
step is a reset and then, if that fails, Leviton support.

## Before

**1. Refresh the snapshot.** One exists at `data/panelb-pre-reset.json`, but take
a fresh one if hours have passed — the reset destroys this state, and it is what
a support case needs.

```bash
ssh ubuntu@<instance>
cd ~/energyDataCapture
docker compose run --rm -T \
  -v $PWD/scripts/panelb-capture.py:/capture.py:ro \
  --entrypoint python energycap /capture.py /data/panelb-pre-reset.json
```

**2. Write down the firmware version** from the Leviton app. fw 2.1.0 and
fw >= 2.2.0 behave differently in ways this project depends on (breaker id
mutation, keepalive tolerance), and a reset may move you between them.

**3. Note the current stuck values** so you can tell a genuine change from a
coincidence: feed ~489/533 W, subpanel **418.61 / 446.55 W** — pinned since
05:00 local on 08-26.

## During

Reset and re-add the hub in the Leviton app as normal.

The data gap is expected and honest: the collector runs `LEVITON_INGEST=ws`, so
an unreachable hub emits **no rows** rather than a cached value. Nothing to do
about it, and nothing to clean up afterwards.

Two things to get right in the re-setup, because both fail silently downstream:

- **CT channel assignment.** Channel 1 must be the panel feed (`GRID_POWER`) and
  channel 2 the subpanel (`SUB_PANEL`). `historyview` classifies `ct_1_*` as the
  feed level and every other `ct_*` as a subfeed; swapping them misclassifies the
  nesting hierarchy and every Panel B total is wrong, including the meter
  comparison.
- **Breaker positions.** Put each breaker back in the slot it came from:
  `1, 2, 6, 9, 10, 11, 13, 14, 17, 26, 28`. `channel_id` is
  `breaker_p{position}`, keyed on the physical slot precisely because fw >= 2.2.0
  mutates the API's breaker ids. Shifted positions mean one `channel_id` covers
  two different circuits, with no error anywhere.

## After

**4. Force rediscovery.** Discovery runs hourly, so do not wait for it.

```bash
docker compose restart
```

**5. Check the hub came back as the same device.**

```bash
docker compose run --rm -T \
  -v $PWD/scripts/panelb-reset-check.py:/check.py:ro \
  -v $PWD/data/panelb-pre-reset.json:/snapshot.json:ro \
  --entrypoint python energycap /check.py /snapshot.json
```

It compares `device_id`, the CT channel/usage layout, and the breaker positions
against the snapshot, and exits nonzero if any changed. CT `api_id`s are expected
to change and are deliberately not checked.

**If anything reports CHANGED, stop and fix it before trusting a Panel B number.**
A new `device_id` needs a `config/channel_map.json` entry and means the history
is discontinuous; shifted positions or a swapped CT layout mean the archive is
being silently corrupted from that moment on.

**6. Prove the CTs track load.** This is the direct test and takes a minute —
switch a known subpanel load on and off and watch the readings the checker prints
move. A latched channel will not.

**7. Watch by the hour.** The default rule needs two consecutive pinned hours;
this one fires on a single hour, which for a CT is already conclusive (a healthy
clamp produced 82 distinct values in the hour its faulty twin produced one).

```bash
docker compose run --rm energycap check-channels \
  --start $(date +%F) --end $(date +%F) --frozen-min-hours 1 --no-notify
```

**8. Do not call it fixed for 24–48 hours.** The fault ran clean for 54 of 106
hours before, so a quiet evening proves nothing. The 10:00 digest gives the first
full-day verdict, and it pushes to Pushover on its own — no need to keep checking
by hand.

## If it comes back

The snapshot plus the new latch hours are the support case. Give Leviton:

- hub `1000_0046_1D48`, `IotCt/14942` (channel 1, GRID_POWER) and `IotCt/14943`
  (channel 2, SUB_PANEL);
- that a live REST read returns the same stuck value as the push feed, so it is
  not a client-side cache;
- that the second hub on the same account, same firmware, same app, is healthy
  throughout;
- the latched-hour list from the snapshot.
