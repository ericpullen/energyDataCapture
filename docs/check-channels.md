# `check-channels` — instrument integrity, and pushing it to a phone

Spec, 2026-08-24, and **implemented the same day** — the design below is what shipped, with
four corrections forced by running it against real data (§10). `PLAN.md` remains the spec of
record; this covers something PLAN.md does not, in the same shape as `docs/s3-storage.md`.

Motivating fault: `DEVIATIONS.md` #180 — a Leviton hub that stops updating its CT channels,
sometimes stuck at `0`, sometimes stuck at a plausible non-zero value, for hours.

---

## 1. The problem this exists to solve

Every existing check in this project asks *"did we observe it?"* — `sample_count`,
`observed_seconds`, staleness, failure streaks. All of them pass while a CT channel returns
the same wrong number 600 times in a row. The row is present, the value is non-null, the
coverage is complete, and the number is plausible. #180 went **six days** undetected and was
found only because someone eyed a total.

So the gap is not observation, it is **trust in the instrument**. That is a different
question and it needs its own checks.

Second, quieter problem: `digest` builds a 21-day median band per circuit and judges each day
against it. A frozen channel poisons that band. Integrity therefore has to be established
*before* consumption anomalies are judged, or the digest silently launders bad data into a
baseline.

## 2. Scope decision: one new module, two entry points, no new notifier

Everything needed to deliver this already exists and **must be reused, not duplicated**:

| need | already exists |
|---|---|
| push to a phone | `PUSHOVER_TOKEN`/`PUSHOVER_USER`, used by `watch-health` and `digest` |
| a finding with a headline, detail, and optional cost | `digest.Finding` |
| a report that formats, totals, and names what it skipped | `digest.DigestReport` |
| a daily scheduled slot with a push | `digest_daily`, `DailyAt(*DIGEST_DAILY_AT)`, 06:00 local |
| meter-vs-panel comparison, coverage-gated | `compare.compare_range`, `compare.HourComparison`, `DEFAULT_MIN_COVERAGE = 0.9` |
| the data all four checks need | `energy_hourly` — `min`, `max`, `mean`, `sample_count`, `observed_seconds` |

**No schema change is required.** An earlier proposal to add a `distinct_values` column to
the rollup is withdrawn: `min = max` over a complete hour is the freeze signature, and it
separates the faulty hub from the healthy one cleanly (§3.1).

So:

- **`stages/integrity.py`** — new module, the four checks, pure functions over rollup rows.
- **`energycap check-channels --start --end [--notify]`** — ad-hoc verification over any local
  date range. This is the "I just re-seated the clamps, is it fixed?" command.
- **`digest` calls `integrity` first**, adds its findings to the same report, and **gates its
  own band rules** on the result.

One new module, one new command, one new job name. Nothing else moves.

## 3. The four checks

All four read `energy_hourly` only. All four are coverage-gated: an hour with
`sample_count < 100` is **skipped and named**, never treated as passing — the
`skipped_incomplete` discipline `digest` already enforces.

### 3.1 Frozen channel — `min = max` across consecutive hours

A channel whose hourly `min` equals its `max` reported one value for the whole hour.

**Threshold: ≥ 2 consecutive frozen hours on one channel.** Calibrated against eight days,
both hubs, feed CT pairs, `sample_count >= 100`:

| | 1-hour runs | 2-hour | 3-hour | 5-hour |
|---|---|---|---|---|
| healthy hub `…1D52` | 2 | 0 | 0 | 0 |
| faulty hub `…1D48` | 14 | 5 | 3 | 4 |

A single frozen hour is normal — the healthy hub's two are both legs at 04:00 on a quiet
night, which is a real steady load. **Two in a row is not**, and the threshold gives 12
detections on the faulty hub and **zero false positives** on the healthy one.

Finding: `frozen_channel`. Headline names the channel's `dim_channel` label, the pinned
value, and the run length.

Would have first fired **2026-08-18 13:00**, day two of collection, six days early.

### 3.2 A panel's children exceed its feed — physically impossible

Everything on a panel passes through its feed clamp, so
`sum(children.mean) <= feed.mean` must hold for every hour, within instrument tolerance.
Children = that `device_id`'s `breaker_*` channels plus any `ct_2*` subpanel pair.

**Threshold: excess > `max(5% of feed, 100 W)`.** Calibrated the same way, 164 panel-hours
each:

| | any excess | > 5% or 100 W | > 10% or 150 W |
|---|---|---|---|
| healthy hub | 18 | **0** | 0 |
| faulty hub | 34 | **9** | 6 |

The tolerance is load-bearing: bare "any excess" fires 18 times on the healthy hub from
ordinary CT tolerance, which is exactly how an alert teaches you to ignore it. 5%/100 W
separates perfectly, and the measured clamp-vs-meter agreement is ~3.4%, so 5% is one
tolerance-width of headroom.

This is the strongest check because it is **mode-independent** — it fires on a frozen
channel, a stuck-zero channel, and a clamp reading low, without knowing which.

Finding: `feed_below_children`. Worth noting it also caught an hour I had assumed was
healthy: 08-23 17:00, feed 2,240 W against children 3,485 W.

Only meaningful on a panel whose circuits are substantially metered. Panel B qualifies since
2026-08-22; a panel with few smart breakers should report `not enough coverage to judge`
rather than a pass.

### 3.3 Disagreement with the utility meter — the explicit ask

The one failure mode neither check above catches is **a clamp that reads live but scaled
wrong** — a partially-closed jaw, the wrong conductor, a clamp not fully latched. It
produces a fresh, varying, plausible series that is simply low. `min < max` every hour, and
if it stays above the children nothing else notices.

The only independent instrument is the LG&E meter. `compare.compare_range` already does
this, coverage-gated, and already knows which meter is the house via `dim_channel.is_primary`.

**Threshold: alarm when the daily total disagrees by more than 10%, on days where
`compare_range` reports ≥ 90% hour coverage.** Grounded in measurement, not taste:

- clean hours measured **+4.6%, −3.6%, −2.8%**; the established whole-day figures are
  **−3.1%** and **~3.4%**
- fault hours measured **−22.2%, −12.5%, −18.7%, −13.0%**

10% sits above every clean observation and below every faulty one. Report the signed
percentage, both totals, and the hour count, so a slow drift is visible before it trips.

Finding: `meter_disagreement`. **This is the check that must run after any clamp is
re-seated** — it is the only one that can see a live-but-wrong clamp.

Two guards: skip when the meter has no overlapping data (Green Button publishes on the
utility's lag, not ours — `METER_STALE_AFTER_DAYS` exists for this), and **never** sum two
`interval_s` series (#169).

### 3.4 Negative values — free, and currently untested

**No Leviton value has ever been negative** in eight days, so a reversed CT has never been
observed and the detection is unproven. `min < 0` on any `leviton` watts/amps row is the
check, it costs nothing, and a backwards clamp is a realistic outcome of re-seating work.

Finding: `negative_reading`.

## 4. Integrity gates the digest

`digest` must not judge a channel it cannot trust, and must not fold an untrustworthy day
into a 21-day baseline.

- A channel with a `frozen_channel` or `negative_reading` finding is added to
  `DigestReport.skipped_incomplete` (or a new `skipped_untrusted`) and **excluded from band
  comparison for that day**.
- A day with a `meter_disagreement` finding still reports its consumption findings, but the
  report carries a note that the panel data disagreed with the meter by N% — the numbers may
  all be proportionally wrong.
- Integrity findings sort **first** in the pushed body. "Your CT is lying" outranks "the
  dryer ran long", because every consumption number below it depends on it.

## 5. Notification and scheduling

Reuse `digest`'s path exactly: `digest_daily` at 06:00 local already builds a report and
pushes it. Integrity findings ride along, so **the default answer to "notify me" needs no new
delivery mechanism and no new schedule.**

`check-channels` takes `--notify/--no-notify` and `--always-notify` with the same meanings
`watch-health` and `digest` already give them, and exits non-zero when any check fires, so a
launchd/cron wrapper sees the failure even when the push cannot be delivered — the
`watch-health` rule, for the same reason.

**Where it runs matters, and differs from `watch-health`.** `watch-health` must live on
another machine because a dead box cannot report that it died. `check-channels` reads S3, not
the collector, so it can run anywhere with `energycap-batch` credentials — including the Mac,
alongside `digest`. It does **not** need to be on the instance, and the instance's scoped key
cannot read `energy/hourly` for writing anyway (#181 widened that for the collector, but the
batch key is the right one here).

`/healthz` gains an `integrity` section for the last complete local day: the four counts, the
worst finding, and the day it covers. Missing must read as failure, not pass —
`watch-health`'s rule, and the bug it exists to stop repeating.

## 6. Configuration

New env vars, `PLAN.md` §14 style, defaults being the calibrated values above:

```
INTEGRITY_FROZEN_MIN_HOURS=2          # consecutive frozen hours before it is a finding
INTEGRITY_FEED_EXCESS_PCT=5.0         # children-over-feed tolerance, percent of feed
INTEGRITY_FEED_EXCESS_MIN_W=100.0     # ...and its absolute floor, so small panels are sane
INTEGRITY_METER_DISAGREE_PCT=10.0     # daily panel-vs-meter disagreement that alarms
INTEGRITY_MIN_SAMPLES=100             # hourly sample_count below this is skipped, not passed
```

`.env.example` must gain all five. **It also has a real bug to fix while there:** the
`SCHEDULED_JOBS` comment lists five valid names and omits `digest_daily`, which
`runtime.default_jobs` does register. Anyone setting `SCHEDULED_JOBS` from that comment
silently loses the digest.

## 7. Tests (`PLAN.md` §15 discipline — fixtures, no network)

1. **Freeze:** 2 consecutive frozen hours fires; 1 does not; 2 non-consecutive does not;
   an hour with `sample_count = 40` is skipped and named, not passed.
2. **Freeze regression pin:** the real 08-18 13:00 → 15:00 shape fires; the healthy hub's
   isolated 08-24 04:00 hour does not. These two are the whole calibration and a change to
   the threshold must break a test.
3. **Feed vs children:** excess of exactly the tolerance does not fire, one watt more does;
   a panel with no smart breakers reports "not enough coverage" rather than passing; the
   `ct_2` subpanel pair counts as a child, not a sibling feed.
4. **Meter:** −3.4% is quiet, −12.5% fires; a day with 40% meter coverage is skipped; a
   meter with two `interval_s` series is never summed (#169); no meter data at all is
   skipped, never a pass.
5. **Negative:** a single negative watts row fires.
6. **Gating:** a frozen channel is excluded from `digest`'s band comparison and appears in
   the skipped list; integrity findings sort before consumption findings.
7. **Notification:** empty Pushover credentials still run every check and still exit
   non-zero — the `watch-health` invariant, tested there and worth repeating here.
8. **Scrubbing:** no finding body can carry a token; extend the existing AST walk over
   `log.*()` calls.
9. **Idempotence:** `check-channels` over the same range twice is identical and writes
   nothing (it is read-only — the first stage in this project that is).

## 8. Deliberately out of scope

- **Fixing the Leviton fault.** It is upstream of every line of our code (#180). This
  detects and reports; it never repairs, interpolates, or filters a value out of the
  archive. Cardinal rules 1 and 2 are unchanged.
- **A channel alternating between two stuck values** would evade `min = max`. Not observed;
  if it ever is, that is when `distinct_values` earns its column.
- **Per-leg imbalance** on a 240 V pair. Plausible detector, no evidence yet that it fires
  on anything real.
- **Backfilling findings over history.** `check-channels --start/--end` can be pointed at
  any past range by hand; nothing is stored.

## 9. Open questions for the implementer

1. Should `check-channels` findings be **persisted** (an `energy/integrity/` dataset) so
   "when did this start" is a query rather than a re-scan? Leaning no — it is derivable from
   `energy_hourly`, and a derived dataset that can go stale is a liability. But "when did
   this start" is the first question asked every time.
2. `digest` prices findings from `config/tariff.json`. An integrity finding has no
   meaningful cost — but a 12% meter disagreement over a month does. Worth pricing, or is a
   dollar figure on an instrument fault actively misleading?
3. Does the `integrity` `/healthz` section belong in `watch-health`'s rule set too? It would
   mean the watchdog alarms on instrument faults as well as liveness, which is arguably its
   job and arguably scope creep.

---

## 10. What running it against real data changed

The spec above was written from queries; the implementation was then pointed at the live
archive (`--start 2026-08-17 --end 2026-08-24`) before any test existed. **Four things were
wrong**, and each is now a named regression test in `tests/test_integrity.py`.

### 10.1 Pinned at exactly zero is *off*, not frozen

First run, first output: `breaker_p19 reported exactly 0.00 W for 2 consecutive hours`.
`breaker_p19` is the water heater. Idle for two hours it reports `0.0` unchanging, and that
is simply true — the **healthy** hub has 340 such channel-hours. The freeze check now
requires a **non-zero** pinned value.

Nothing is lost by that: the stuck-at-zero mode is real, but it belongs to
`feed_below_children`, which catches it without having to guess whether a zero is honest.

### 10.2 The freeze check is CT-only, because breakers report integers

Excluding zeros was not enough. The healthy hub still produced frozen non-zero runs on
`breaker_p23`, `p17`, `p18` and `p6` — a mini-split idling, a porch light at 21 W. **Breakers
report integer watts and CTs report floats**: a steady 21 W load pins trivially, while a CT
repeating `494.76` exactly for hours is a much stronger claim. The healthy hub's feed CTs
average 55 distinct values an hour and produced **zero** runs of two.

So `frozen_channel` applies to `ct_*` channels only. A breaker that genuinely sticks still
surfaces through `feed_below_children`, which needs no per-channel threshold.

### 10.3 The panel coverage gate could never fire

`observed_seconds` is summed across **every feed leg on every hub**, so a fully watched day
reports roughly `series × seconds_in_the_day` — four times a day's length on this house. The
gate compared that against one day, so it never tripped, and the partial first day of
collection (2026-08-17, watched from 15:22 local) was reported as a **−60.4% instrument
fault**. It was a coverage gap. `PANEL_DAILY_SQL` now returns a `series` count and the
coverage fraction divides by it.

### 10.4 The meter needed the same gate, for the opposite reason

With the panel side fixed, 2026-08-23 reported the panels **+59.3% above** the meter. That was
a half-published *meter* day beside a complete panel day: **Green Button publishes on the
utility's lag, not ours**, so the newest day or two is habitually partial. `METER_DAILY_SQL`
now returns an interval count and a day below 90% of its expected intervals is skipped and
named.

Both gates matter more than any threshold in this document. An alert that reports the
collector's own gaps as instrument faults gets muted, and a muted alert is how #180 survived
six days in the first place.

## 11. What it reports on the real archive

`--start 2026-08-17 --end 2026-08-24`, after all four corrections:

| | |
|---|---|
| `frozen_channel` | 24 — **all on the faulty hub** |
| `feed_below_children` | 9 — **all on the faulty hub** |
| `meter_disagreement` | 0 |
| `negative_reading` | 0 |
| skipped and named | 2026-08-17 (36% watched), 2026-08-23 (meter published 16/24 intervals) |

**Zero findings on the healthy hub.** The meter check reads 0 because the five days with
complete data on both sides agree to −3.2%, −4.9%, −4.8%, −3.1% and −7.4%; dropping the
threshold to 1% fires on all five, which is how the check was confirmed live rather than
inert.

## 12. Answers to §9's open questions

1. **Persist findings?** No, and the reason is stronger than "derivable": the checks read
   `energy_hourly`, which `rollup` rebuilds from `raw_30s` at will, so a stored finding could
   outlive the data that justified it. `check-channels --start/--end` over any past range
   answers "when did this start" in one command.
2. **Price integrity findings?** No. A dollar figure on a stuck clamp is meaningless, and
   `meter_disagreement` already reports the kWh gap, which is the honest number. `Finding`
   carries `cost_usd=None` for all four rules.
3. **Add `integrity` to `watch-health`?** Not yet — it is published to `/healthz` (with
   `ok: None` for "nobody has checked", never `True`), so the watchdog *can* read it, but
   wiring it in would mean the liveness alarm also fires on instrument faults that the digest
   already pushes. Left as a deliberate decision rather than an oversight.

## 13. What shipped

- `src/energy_capture/stages/integrity.py` — the four checks, pure over rollup rows.
- `energycap check-channels --start --end [--notify] [--always-notify] [--bucket]`, exiting
  non-zero on any finding.
- `digest` runs it **first**, prepends its findings, and gates its own rules on the result: an
  untrusted channel is neither judged against its band **nor admitted to the baseline that
  will judge it tomorrow**, and an untrusted service feed also silences the overnight-floor
  rule, which reads the feed CTs directly.
- `/healthz` gains an `integrity` section; `ok` starts `None`, never `True`.
- Five `INTEGRITY_*` settings, all documented in `.env.example`.
- 29 tests in `tests/test_integrity.py`, including two calibration pins and four
  false-positive regressions.
- **Bug fixed in passing:** `.env.example`'s `SCHEDULED_JOBS` comment omitted `digest_daily`,
  which `runtime.default_jobs` registers, so anyone configuring from that comment silently
  lost the digest. A test now pins the list against the real schedule by set equality — and
  the first version of that test passed while the list was broken, because the prose
  explaining the bug contained the missing name. It parses only the name list now.
