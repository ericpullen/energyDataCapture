# Leviton support report — hub stops updating its CT channels while its own breakers keep reporting

Draft for a Leviton support ticket. Evidence collected 2026-08-17 → 2026-08-24 by a 30 s
polling collector; see `DEVIATIONS.md` #180 for the full investigation.

---

## Summary

On one of two LWHEM-2 load centres, **all four CT channels stop updating for long periods while
the same hub, in the same API response, continues to report changing current through smart
breakers on that panel.** The stuck reading takes one of two forms:

1. **Stuck at zero** — `0.000 A` / `0.0 W` while up to 1,960 W is measurably flowing.
2. **Stuck at a non-zero value** — the identical figure returned for hours. On 2026-08-24 the
   service-feed CT returned `489.3 W` for **600 consecutive 30 s samples (05:00–10:41 local)**
   while the heat-pump breaker on that same panel cycled between 0 W and 165 W.

Both forms appear over **both the REST API and the WebSocket push**, so this is not client-side
caching. The second hub in the same house, on identical firmware 2.1.2, has never done either.

**A power cycle does not fix it, and reveals the mechanism.** The hub was reset at 10:40 local on
2026-08-24. At 10:41:51 the feed CT updated exactly once, `489.3 W → 494.76 W`, and then froze
again at that new value for the next 90 minutes and counting. It appears to take a single reading
at startup and then stop refreshing the channel.

## Hardware

| | |
|---|---|
| Affected hub | `1000_0046_1D48` — firmware **2.1.2** |
| Unaffected hub | `1000_0046_1D52` — firmware **2.1.2** (identical) |
| Affected channels | channel 1 legs A and B, `usageType: GRID_POWER` (whole-panel service feed) |
| Also affected, but see below | channel 2 legs A and B, `usageType: SUB_PANEL` |
| Smart breakers on the affected hub | LB250-0ST (pos 1), LB240-0ST (pos 10), LB240-0ST (pos 14), LB120-0ST (pos 26) |
| Sampling | REST `GET` and WebSocket, every 30 s, continuously |
| Observation window | 2026-08-17 15:22 → 2026-08-24 09:59 local, 19,505 complete cycles |

## The defect, in one observation

Every number below comes from a **single REST response** — one hub, one moment, no WebSocket
involved:

```
time (UTC)   ct_1_a      ct_1_a     breaker_p10   breaker_p10
             watts       amps       watts         amps
04:57:00     0.0         0.000      1457.0        6.085
04:57:41     0.0         0.000      1457.0        6.085
04:58:21     0.0         0.000      1436.0        5.595
04:59:02     0.0         0.000       704.0        3.160
05:52:26     0.0         0.000      1754.0        7.330
```

`breaker_p10` is a 40 A LB240-0ST feeding a 5-ton variable-speed heat pump. The service-feed CT
that this current must physically pass through reports zero amps.

## How often, and with how much current flowing

Since the panel was fully instrumented with smart breakers (2026-08-22 onward), every cycle can
be checked against the sum of that hub's own breaker channels:

| feed CT state | cycles | mean breaker sum | max breaker sum | cycles with ≥300 W |
|---|---|---|---|---|
| reporting | 4,232 | 758 W | 8,734 W | 2,240 |
| **at exactly 0** | **750** | **254 W** | **1,960 W** | **172** |

So **172 of 750 zero-readings (23%) had 300 W or more of measured current** on that panel, and the
worst carried 1,960 W — roughly 8 A per leg, far above any plausible measurement floor.

## The frozen non-zero mode, which is the more damning of the two

2026-08-24, hub `1000_0046_1D48`, 30 s sampling, distinct values reported per hour:

| local hour | feed CT (ch 1 leg A) | subpanel CT (ch 2 leg A) | heat-pump breaker | breaker range |
|---|---|---|---|---|
| 05:00 | **1** | **1** | 5 | 83–165 W |
| 06:00 | **1** | **1** | 9 | 0–158 W |
| 07:00 | **1** | **1** | 10 | 0–157 W |
| 08:00 | **1** | **1** | 13 | 0–164 W |
| 09:00 | **1** | **1** | 9 | 0–163 W |

120 samples an hour, one value. The breaker channel on the same hub, reporting through the same
API in the same responses, tracked the compressor cycling on and off throughout.

The feed CT is the airtight case, because it **must** include that varying heat-pump load — every
amp the breaker measured passed through the feed clamp, so a constant feed reading cannot be
correct. (We are deliberately not resting the argument on channel 2 here: the air handler it
feeds was turning at a near-constant 1,223–1,242 rpm across the same five hours, so a steady
reading there is plausible on its own.)

**The frozen feed reading is physically impossible**, and the hub's own data proves it: summing
that panel's metered children (channel-2 CT pair plus all four smart breakers) **exceeds the
frozen service-feed reading in 459 of those 600 cycles**, by up to 116 W. Every one of those
loads passes through the feed clamp.

Measured across eight days, distinct values reported per channel-hour on the feed CT pairs:

| | channel-hours | pinned to 1 value | ≤3 values | mean distinct | median |
|---|---|---|---|---|---|
| unaffected hub `…1D52` | 326 | 2 | 7 | 55.0 | 54.5 |
| **affected hub `…1D48`** | 326 | **53** | **129** | **8.2** | **5.0** |

The same contrast appears in a REST-only reader run independently from a second machine: 900
samples of the affected hub's feed CT yielded **42 distinct non-zero values with a longest
identical run of 141 samples**, against 131 distinct values and a longest run of 36 on the
unaffected hub over the same window.

## Duration — it is not a single-sample glitch

Consecutive-zero runs on `ct_1_a`, at 30 s sampling:

| | |
|---|---|
| Runs observed | 143 |
| Median run | 7 minutes |
| Mean run | 10 minutes |
| Longest run | **63.5 minutes** |

Recovery is always clean and complete — the channel returns to plausible values with no
intervention.

## What we have ruled out, and how

1. **Not a client caching or staleness problem.** A controlled A/B ran 900 paired cycles over
   10 hours: a REST-only reader and the WebSocket push reader, sampling the same channels at the
   same moments. Of 3,556 comparable observations, **1,273 had both transports at exactly 0**,
   while disagreements were symmetric and negligible (17 one way, 23 the other, nearly all
   transition samples at the edge of a run). Both transports report the same zero.
2. **Not the watts computation or a power-factor issue.** `amps` reads exactly `0.0` in
   **29,325 of 29,325** samples where `watts` is `0.0`. There is not one instance of `watts = 0`
   with `amps > 0`. The current measurement itself is zero.
3. **Not the hub losing connectivity.** In every zero cycle checked (2,881 of them), the same hub
   reported `volts = 121.0` and `hz = 60.0` correctly at the same timestamp, and its breaker
   channels answered normally.
4. **Not the breaker channels.** They report continuously through every zero run, down to
   `0.228 A` and `0.170 A` — well below the current the CT pair is failing to see.
5. **Not a simple low-current cutoff, though it starts as one.** No CT sample on either hub has
   ever landed between `0` and `0.562 A`, so there is clearly a floor. But a floor alone cannot
   explain a reading of `0.000 A` while 8 A per leg flows. The dropout appears to be entered at
   low current and then **not exited when current returns**.
6. **`connected: false` is not the signal.** All six CT channels across both hubs report
   `connected: false`, including the pair on the hub that has never failed. It does not
   distinguish the healthy CTs from the failing ones.

## On channel 2 (`SUB_PANEL`) — mostly a red herring, and why

Channel 2 clamps the feeder to an HVAC subpanel whose only significant load is an air-handler
blower. That blower's speed is independently known from the HVAC system's own telemetry, and it
explains channel 2's zeros almost entirely:

| blower state | minutes | channel 2 at zero | mean W when reporting |
|---|---|---|---|
| stopped | 1,331 | 98.1% | 603 |
| low (<600 rpm) | 5,021 | 95.1% | 172 |
| running (≥600 rpm) | 3,402 | **6.8%** | 694 |

On the frozen mode, channel 2 is genuinely ambiguous and we are not claiming it: the blower ran
at a near-constant 1,223–1,242 rpm during the five frozen hours above, so a constant wattage is
believable. We note only that a real measurement normally jitters in its last decimal — the
unaffected hub's feed CT produced 55 distinct values an hour — and channel 2 produced exactly
one. Treat that as a hint, not evidence.

Channel 2 reads zero when there is genuinely nothing to read, and reports correctly once the
blower spins up. It shares the same low-current floor as channel 1 and spends most of its life
below it, legitimately.

The same comparison on channel 1 is what isolates the fault:

| blower state | channel 1 (`GRID_POWER`) at zero |
|---|---|
| stopped | **62.1%** |
| low | 15.5% |
| running | 1.9% |

Channel 1 clamps the whole-panel service feed. It is never legitimately at zero — the panel
always carries at least a heat-pump standby load — and 23% of its zero readings coincide with
measured current on its own breakers.

**Channel 1 also never drops out unless channel 2 is already at zero (3,165 cycles both, 0
cycles channel 1 alone).** We believe that is simply because channel 2 always carries less
current and therefore always crosses the threshold first, not evidence of a shared failure — but
it may be diagnostic on your side.

## Cross-check against the utility meter

Summed feed CTs against the electric utility's own 15-minute interval data for 2026-08-23:

| local hour | utility meter | summed feed CTs | difference |
|---|---|---|---|
| 02:00 | 1.944 kWh | 2.033 kWh | +4.6% |
| 03:00 | 1.967 | 1.530 | **−22.2%** |
| 04:00 | 2.208 | 1.933 | **−12.5%** |
| 05:00 | 1.965 | 1.597 | **−18.7%** |
| 06:00 | 2.330 | 2.028 | **−13.0%** |
| 07:00 | 3.173 | 3.060 | −3.6% |
| 08:00 | 1.571 | 1.527 | −2.8% |

Hours containing zero-readings under-report by 12–22%. Hours without them agree to within ±4%,
which is the normal combined tolerance of the clamps and the meter. The energy is real and the
CT pair is missing it.

## What we are asking

1. Is a stuck CT reading — either `0.000 A` or a frozen non-zero value — concurrent with
   changing breaker readings on the same hub, a known firmware behaviour in 2.1.2?
2. Why would a power cycle produce exactly one CT update and then stop? That is the single most
   specific symptom we have.
2. Is there a documented minimum sensing current for these CT clamps, and a documented recovery
   behaviour once current rises back above it?
3. Is there any diagnostic we can read to distinguish "clamp reports zero" from "hub discarded
   the reading" — anything beyond `connected`, which does not discriminate here?
4. Should the channel-1 clamps on this hub be re-seated or replaced, or is this a firmware matter?

## What we have already tried

**A power cycle of the affected hub, 2026-08-24 10:40 local. It did not fix it** — see the
Summary. One CT update at 10:41:51, then frozen again for 90 minutes and counting, while the
unaffected hub's feed CT moved from 2,755 W to 920 W in 57 seconds over the same interval.

Next on our side is re-seating the channel-1 clamps, unless you would rather we leave the
installation untouched for diagnosis. Both hubs run identical firmware and only one misbehaves,
which is why we eliminated a stuck firmware state first.

## Notes on our setup, for completeness

- The mandatory high-bandwidth keepalive (`PUT /IotWhems/{id}` with `{"bandwidth": 1}`) runs every
  50 s per connected hub. We never send `bandwidth: 0`.
- Values are never interpolated, averaged or zero-filled anywhere in our pipeline. Every number
  in this report is a value your API returned verbatim, which is why the zeros were detectable at
  all.
- Raw data for any window above can be supplied as Parquet or CSV on request.
