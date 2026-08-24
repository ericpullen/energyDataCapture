# The watchdog — `energycap watch-health`

Everything else in this project detects its own failures well and delivers none
of them. `/healthz` and `status.json` are written carefully and read by nothing:
the only automated consumer is a Docker healthcheck that flips a label. Both
recent real incidents — a three-day LG&E authorisation lapse and six days of
latched CT zeros — ran under a **green** healthcheck and were found by a human
looking at a chart.

This is the delivery half. One command, one systemd timer, Pushover — plus a
dead-man's switch, without which none of it can report its own death.

---

## What it checks

Eight rules, run against the collector's status document.

| Check | Alarms when | Severity |
|---|---|---|
| `reachable` | the endpoint cannot be read at all | CRITICAL |
| `health.ok` | the collector reports itself unhealthy, or has no `health` block | CRITICAL |
| `pollers` | any poller is stale or has never succeeded; or there are no poller checks | CRITICAL |
| `uploader` | no successful upload in `UPLOADER_STALE_AFTER_S` (default 2h), or no timestamp at all | CRITICAL |
| `spool` | `pending_rows` over `SPOOL_PENDING_CEILING` (default 45,000) | CRITICAL |
| `stage_failures` | any stage's own `consecutive_failures` ≥ `FAILURE_STREAK_ALARM` (default 2) | CRITICAL |
| `meter` | the LG&E meter data is stale — **or its freshness is unknown** | WARNING |
| `digest` | the daily anomaly review has not succeeded in 26h — **or has never been seen** | WARNING |

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

### 2. The dead-man's switch

Do this BEFORE the timer, because it is what makes the timer trustworthy.

Create a check at <https://healthchecks.io> (free; Cronitor and Better Uptime
work the same way) with a period of 15 minutes and a grace of 15, and put the
ping URL in `.env`:

```bash
HEALTHCHECKS_PING_URL=https://hc-ping.com/<uuid>
```

`watch-health` pings it on every run it **completes**, whatever the verdict.
Not on a clean verdict — that distinction matters. The obvious shell chain,
`watch-health && curl hc-ping…`, only pings when nothing is wrong, so a real,
persistent fault silences the heartbeat too and the external service pages for
the same fault a second time. After a few of those the heartbeat means nothing.

Pushover carries what is wrong with the **collector**. This carries the fact
that the **watcher** is still there to say so.

Treat the URL as a secret: anyone holding it can fake the heartbeat and suppress
the alert that the watcher died. It is in `SECRET_SETTING_FIELDS`, so the log
scrubber redacts it.

### 3. The systemd timer

Runs on the **collector's host**, outside the container, against the running
container.

This is a change from the original design, which put it on the Mac. The
reasoning there was sound — *a box that has died cannot report that it died* —
but the conclusion did not follow. The second machine had its own availability,
its own way of being asleep (launchd **skips** missed `StartInterval` firings
during sleep, leaving no trace), and a dependency on the house IP staying inside
the Lightsail firewall rule for TCP 8080. Nothing was watching it. The
dead-man's switch covers that entire failure class from genuinely outside — and
covers this command's own death too, which no placement can cover for itself.

`docker compose run --rm` starts a **fresh** container from the same image
rather than executing inside the live one. A collector wedged but not dead would
swallow a check running inside it; this one still starts, still fails to read
`/healthz`, and still pages. It also needs no Python on the host, which the
Lightsail box does not have.

```bash
cd ~/energyDataCapture
sudo cp deploy/energycap-watch.service deploy/energycap-watch.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now energycap-watch.timer
```

Check it:

```bash
systemctl list-timers energycap-watch.timer
sudo systemctl start energycap-watch.service   # fire one now
journalctl -u energycap-watch.service -n 50 --no-pager
```

`HEALTHZ_URL` must be the **compose service name**, not localhost — the probe
runs in its own container on the compose network:

```bash
HEALTHZ_URL=http://energycap:8080/healthz
```

`127.0.0.1` there would resolve to the probe container itself, which serves
nothing: the check would fail every time and page forever. (It would at least
fail loudly rather than pass, which is the right direction for this command to
be wrong in.)

### The old launchd job, if you still want a second opinion

Runs on the **Mac**. Requires the Mac to be inside the house IP allowed by the
Lightsail firewall rule, and it stops seeing anything the moment that IP
changes — a false `reachable` CRITICAL every six hours until someone edits the
rule. Optional now, and no longer carrying the design.

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

## What a watcher cannot do for itself

**Report its own death.** No placement fixes this — that is why
`HEALTHCHECKS_PING_URL` is not optional decoration. If the timer is masked, the
host is stopped, Docker dies, or the unit starts failing, this command sends
nothing, and *nothing* is exactly what a healthy quiet night looks like. Only an
external service noticing the *absence* of a beat can tell those apart.

Concretely, with the switch configured, here is what reports what:

| what breaks | what tells you |
|---|---|
| a poller, the uploader, the spool, a stage streak | `watch-health` → Pushover |
| the collector container | `watch-health` → Pushover (`reachable` CRITICAL) |
| Docker, the host, the network, the timer, this command | healthchecks.io, when the beat stops |
| Pushover itself | healthchecks.io — the run still completes and still pings |

That last row is why the ping is not chained to delivery.

**A slow drift.** Everything here is a threshold on a status field. A CT reading
20% low with full sample coverage passes every check in this file — that is what
the meter comparison and the (unbuilt) nightly digest are for.
