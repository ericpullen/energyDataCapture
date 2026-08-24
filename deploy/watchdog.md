# The watchdog — `energycap watch-health`

Everything else in this project detects its own failures well and delivers none
of them. `/healthz` and `status.json` are written carefully and read by nothing:
the only automated consumer is a Docker healthcheck that flips a label. Both
recent real incidents — a three-day LG&E authorisation lapse and six days of
latched CT zeros — ran under a **green** healthcheck and were found by a human
looking at a chart.

This is the delivery half. One command, one launchd job, Pushover.

---

## What it checks

Seven rules, run against the collector's status document.

| Check | Alarms when | Severity |
|---|---|---|
| `reachable` | the endpoint cannot be read at all | CRITICAL |
| `health.ok` | the collector reports itself unhealthy, or has no `health` block | CRITICAL |
| `pollers` | any poller is stale or has never succeeded; or there are no poller checks | CRITICAL |
| `uploader` | no successful upload in `UPLOADER_STALE_AFTER_S` (default 2h), or no timestamp at all | CRITICAL |
| `spool` | `pending_rows` over `SPOOL_PENDING_CEILING` (default 45,000) | CRITICAL |
| `stage_failures` | any stage's own `consecutive_failures` ≥ `FAILURE_STREAK_ALARM` (default 2) | CRITICAL |
| `meter` | the LG&E meter data is stale — **or its freshness is unknown** | WARNING |

Two of these exist because `/healthz` deliberately will not cover them.
`uploader` and `spool` are the answer to rotated S3 credentials: `/healthz`
judges *pollers only*, so the collector stays green indefinitely while the
archive quietly stops growing. Neither rule consults `health.ok`.

### Absence is a failure, not a pass

This is the rule the whole command is built around, and it is the one to
preserve if anything here is ever rewritten.

The most dangerous thing a watchdog can do is conclude "fine" because the field
it wanted was missing. `/healthz` on the live instance has **no `greenbutton`
section at all** until the daily fetch has run once, so `health.meter.stale` is
*absent* — and the obvious `jq '.health.meter.stale == true'` reads that as
healthy. That is the precise disguise the #177 lapse wore.

So every rule either finds its evidence or raises an alarm saying it could not.
An empty `{}` document raises five alarms, not zero. An unreachable host is the
loudest alarm there is, because that is exactly what a dead collector looks
like. There is a test for each of these.

### What it deliberately does not check

`scheduler.consecutive_failures` — the shared aggregate across all jobs. It
counted only failures and never successes, so it read **203** on the live
instance while every job was in fact succeeding. That is fixed
(DEVIATIONS #187), but the per-stage sections are the precise signal and the
aggregate is still not consulted: a counter that cannot go down produces an
alarm that never clears, which gets muted, which is how the next real event
gets missed.

---

## When it pushes

Not on every failing run. At a 15-minute cadence that would be ~96 identical
notifications a day for one persistent fault; you would mute the channel, and a
muted channel is the silent-failure hole rebuilt one level up.

- the set of failing checks **changed** → push
- everything clear and last time it was not → push the **all-clear**, so a
  resolved page is visibly resolved rather than just going quiet
- still failing, unchanged, and 6 hours have passed → push again, so a fault
  reported once at 03:00 cannot be forgotten
- otherwise → stay quiet

State lives in `{SPOOL_DIR}/watch-state.json`. A missing or corrupt state file
means "no history", which **sends** — failing open is the only safe direction.

---

## Setup

### 1. Pushover

`PUSHOVER_TOKEN` is an **application/API token** — create one at
<https://pushover.net/apps/build>. `PUSHOVER_USER` is your 30-character **user
key**, on the dashboard. Both go in `.env`, both are `SecretStr`, and both are
in `SECRET_SETTING_FIELDS`, so the log scrubber redacts them from every line.

```bash
HEALTHZ_URL=http://<collector-host>:8080/healthz
PUSHOVER_TOKEN=...
PUSHOVER_USER=...
```

Prove the channel before trusting it:

```bash
uv run energycap watch-health --always-notify
```

`--always-notify` pushes even on a clean run. Without it you cannot tell "no
alarms" from "no delivery".

### 2. The launchd job

Runs on the **Mac**, not the instance. Requires the Mac to be inside the house
IP allowed by the Lightsail firewall rule.

```bash
REPO=$HOME/code/energyDataCapture
UV=$(command -v uv)
mkdir -p "$REPO/data/logs"

sed -e "s|__REPO_ROOT__|$REPO|g" \
    -e "s|__DATA_DIR__|$REPO/data|g" \
    -e "s|__UV_DIR__|$(dirname "$UV")|g" \
    -e "s|__UV__|$UV|g" \
    "$REPO/deploy/com.duckbillhq.energycap-watch.plist" \
    > ~/Library/LaunchAgents/com.duckbillhq.energycap-watch.plist

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.duckbillhq.energycap-watch.plist
launchctl kickstart -p gui/$(id -u)/com.duckbillhq.energycap-watch
```

Check it:

```bash
launchctl print gui/$(id -u)/com.duckbillhq.energycap-watch | head -20
tail -f "$REPO/data/logs/energycap-watch.out.log"
```

To remove it:

```bash
launchctl bootout gui/$(id -u)/com.duckbillhq.energycap-watch
```

`WorkingDirectory` must be the repo root: `Settings` finds `.env` by walking up
from the working directory, so started anywhere else the job runs with no
credentials and cannot deliver.

---

## What this still cannot tell you

**That the watcher itself stopped.** A launchd job on a sleeping laptop is
silent, and silence is indistinguishable from health. launchd *skips* missed
`StartInterval` firings during sleep rather than catching up, so a closed lid is
a blind spot with no trace. This is the same class of bug as the one the command
fixes, one level up, and it is honest to say the fix is incomplete without it.

Closing it needs a **dead-man's switch**: an external service the watcher pings
on every successful run, which alerts when the pings *stop*. `watch-health`
exits 0 on a clean run specifically so one can be bolted on:

```bash
uv run energycap watch-health && curl -fsS -m 10 https://hc-ping.com/<uuid>
```

That is a free healthchecks.io check and about five minutes of work. Not done
here because it needs an account this project does not have — but it is the
single highest-value thing left in the alerting story, and it is the difference
between "I get told when the collector breaks" and "I get told when anything in
the chain breaks, including the telling".

**A slow drift.** Everything here is a threshold on a status field. A CT reading
20% low with full sample coverage passes every check in this file — that is what
the meter comparison and the (unbuilt) nightly digest are for.
