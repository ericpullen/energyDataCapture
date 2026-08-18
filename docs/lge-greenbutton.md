# LG&E Green Button Connect — registration research & application draft

Research date **2026-08-18**. This answers the open questions `STATE.md` left for the LG&E
work and drafts the third-party registration form. `PLAN.md` §13 is still the design; this
file is what the utility actually offers and what we have to hand them to get in.

Sources: the [LG&E/KU Green Button page](https://lge-ku.com/mymeter-greenbutton), their
[3rd Party Vendor Registration Process PDF][pdf], the registration form's own reference
guide (pasted from the MyMeter site), and the ESPI
[authorization scope spec](https://github.com/green-button/green-button.github.io/blob/master/espi/authorization_scope.md).

[pdf]: https://lge-ku.com/sites/default/files/media/files/downloads/LGE-KU-Green-Button-Connect-Third-Party%20Vendor-Registration-Process.pdf

---

## 1. Findings — the questions STATE.md asked

**Q1. Does LG&E offer Green Button _Connect_, or only Download?** → **Connect exists**, and
it is a real OAuth2/ESPI implementation, not a rebranded export. Download My Data exists
alongside it. `PLAN.md` §13's stated preference order therefore resolves to **Connect**, and
its "assume manual import first" hedge can be retired.

**Q2. What is the registration path?** → **Self-submitted form, human-approved.** Anyone can
submit; a person at LG&E decides. From the vendor PDF, the sequence is:

1. Third-party vendor (3PV) submits the registration form on the MyMeter site.
2. LG&E/KU review it "on a regular basis" and notify you of the outcome.
3. **On approval, LG&E generates a 3PV token and sends it to the third party.**
4. The *customer* creates a MyMeter **local** account.
5. The customer authorizes the 3PV; the 3PV receives a customer token and fetches data.

Two consequences worth internalising before filling anything in:

- **There is no developer portal and no published API base URL.** The OAuth authorize/token
  endpoints, the resource base URI, and the 3PV client credentials all arrive *after*
  approval, with the token. So the client cannot be written to a spec — the approval email is
  the spec. Nothing past the registration form should be built until it arrives.
- **One form per vendor, forever.** "Each 3PV will only send one registration form for all
  customers... if the 3PV is deactivated for any reason, it will be deactivated for all
  customers using that 3PV." There is no per-customer application, and no sandbox is
  mentioned. Get it right once.

**Q3. Granularity and history?** → The form's reference guide fixes the supported values:
**IntervalDuration 900 (15-minute) or 3600 (hourly)**; **BlockDuration Daily, Weekly,
Monthly, Quarterly or Yearly**. History depth is *not* stated anywhere public — it is a
question for the approval correspondence. Step 5 of the PDF says "all available usage and
billing data (**VEE and raw readings**) will be available for download", which implies both
validated and raw registers are exposed.

**Q4. Staleness?** → Still unanswered publicly. But sharing can be requested as a specific
period, **bulk, or a subscription (daily/monthly)**, so a daily subscription is available and
that is what we want. MyMeter is generally a day or more behind, so plan for the same
day1/day2 re-fetch treatment the Bryant daily energy stage already uses.

### The finding that changes the design

**Connect is electric-only.** The vendor PDF says "Green Button Connect (GBC) allows
customers to share **electric** usage data", and the required function block set tops out at
FB 5 = *Interval Electricity Metering*. There is no gas function block in the required list.

`PLAN.md` §13 assumed gas would come along ("gas likely daily therms/CCF", `channel_id`
`gas_main`, `ccf_interval`/`CCF`). It will not, via Connect. Gas stays a **Download My Data**
problem, which means `import-greenbutton` keeps its reason to exist even after the Connect
client is built — it becomes the gas path and the historical-backfill path, not dead code.
Recorded in `DEVIATIONS.md`.

### Customer-side gotcha

Step 4 is easy to trip over: the MyMeter **local** account needs a registration code
requested by email from `MyMeter@lge-ku.com`, and **its email address cannot be the same as
the primary email on the My Account login.** So this needs a second address before the
authorization step — worth starting that email now, since it is the long-latency item and it
is independent of the vendor approval.

---

## 2. The one decision that gates the form

Six of the eighteen fields are URIs that a human reviewer will click. That forces a posture
choice, and it is the only thing here that is genuinely yours to pick:

**Option A — Desktop application.** `Third-Party Application Type: Desktop`, redirect URI
`http://localhost:8080/greenbutton/callback`. The health server already listens on 8080, so
it can host the callback with no new infrastructure and no inbound firewall hole. Honest
(this genuinely is a single-machine desktop collector) and cheapest. **Risk:** the Notify URI
still has to be a public HTTPS endpoint, and a reviewer may bounce a `localhost` redirect on
sight.

**Option B — Web application.** Static one-pager for client/policy/logo/portal URIs, plus a
Lambda Function URL or Cloudflare Worker for the redirect and notify callbacks. More credible
to a reviewer and gives a real notify endpoint; costs an afternoon and a public footprint.

**Recommendation: A, with B's static page.** Put up a genuine one-page site (GitHub Pages is
enough — free, HTTPS, stable) carrying the description, privacy policy and logo so the URI
fields point at something real, declare Desktop, and use the localhost redirect. If the
review bounces on the redirect or the notify URI, add the Function URL and resubmit — but
don't build the serverless half speculatively for a form that may not need it.

Notify is a nicety regardless: a daily subscription can simply be **polled**, so nothing in
the pipeline should depend on inbound push.

---

## 3. Draft application

Substitute the four bracketed values. Everything else is ready to paste.

| Field | Value |
|---|---|
| Software Version | `1.0.0` |
| Client Name | `energycap` |
| Third-Party Name | `energycap` |
| Contact | `[a monitored email address]` |
| Policy URI | `https://[host]/energycap/privacy` |
| Third-Party Application Description | *see below* |
| Redirect URI | `http://localhost:8080/greenbutton/callback` |
| Third-Party Application Status | `Production` |
| Client URI | `https://[host]/energycap` |
| Token Endpoint Authentication Method | `client_secret_basic` |
| Third-Party Application Type | `Desktop` |
| Scope | `FB=1_3_4_5;IntervalDuration=900_3600;BlockDuration=Daily;HistoryLength=63072000;SubscriptionFrequency=Daily` |
| Third-Party Application Use | `Energy management` |
| Grant Types | `authorization_code refresh_token client_credentials` |
| Third-Party Phone | `1-[XXX-XXX-XXXX]` |
| Response Types | `code` |
| Third-Party User Portal Screen URI | `https://[host]/energycap` |
| Third-Party Notify URI | `https://[host]/greenbutton/notify` |
| Logo URI | `https://[host]/energycap/logo.png` (≤ 180×150) |
| Software ID | `597b1e33-dae8-4262-8fa1-8ae1ea0a68ec` |

**Description** (truthful — this is a single-household personal deployment, and describing it
as a commercial service to a human reviewer would be both dishonest and easy to catch):

> energycap is a personal, single-household energy monitoring application. It collects
> whole-home circuit-level electrical measurements and HVAC telemetry from the homeowner's own
> equipment and stores them as a private time-series archive for the homeowner's own analysis.
> Green Button Connect access is requested so the household's utility-metered interval usage
> can be compared against those on-premises measurements. Data is retrieved only for the
> operator's own account, is stored privately, and is never resold, shared or aggregated with
> other customers' data.

### Notes on the non-obvious fields

- **Software ID** is meant to be stable across every copy of the software and is *asserted by*
  the software, so it is a fixed UUID, generated once, above. **Software Version** is compared
  by string equality and SHOULD change on any update — so bump it, don't reuse it.
- **Scope** follows the ESPI syntax (`FB=` underscore-joined; `;`-delimited terms; no commas or
  spaces, deliberately, because some OAuth libraries split scope on both). The function blocks
  are exactly the four LG&E lists as required — **1** Common, **3** Connect My Data, **4**
  Interval Metering, **5** Interval Electric Metering. Do not pad the list with FBs they
  didn't ask for; unsupported blocks are a way to fail validation for nothing.
- **`IntervalDuration=900_3600`** requests both and lets them serve the finer one they have.
  **`BlockDuration=Daily`** with **`SubscriptionFrequency=Daily`** is the freshest combination
  they support, and daily blocks are the natural fetch unit for a local-date-partitioned
  pipeline.
- **`HistoryLength=63072000`** is 730 days, and it is **a guess** — no public document states
  the cap. If the review comes back with a limit, that number is the thing to change.
- **Grant types** include `client_credentials` because ESPI uses it for the 3PV's own
  metadata/bulk token (the one LG&E mails on approval), separately from the
  `authorization_code` flow that yields the per-customer token.

---

## 4. What happens after approval

Nothing in `src/` should change before the approval email lands, because it carries the
endpoints. When it does, the build is genuinely an import rather than a redesign — §13's
groundwork is already in place (`source='lge'`, `model.METER_SCHEMA` with `interval_s`,
`s3io.meter_key`, the `dim_channel` placeholder, the `import-greenbutton` stub). Expected
shape:

1. `sources/lge_greenbutton.py` — OAuth2 client (authorization code → refresh token, token
   cache on `/data` at mode 600 like the others), plus ESPI XML parsing.
2. A scheduled daily stage fetching the subscription, with day1/day2 re-fetch for revisions.
3. `import-greenbutton` retargeted at gas and at bulk historical XML.

Two things to carry over from the existing sources: the **token cache must never be logged**
(the scrubber is tested, §15.8), and **a missing interval stays missing** — ESPI omits
intervals it has no reading for, and those must not become zeros.

---

## 5. Still unknown — ask these in the approval correspondence

1. The OAuth authorize/token endpoints, resource base URI, and whether a sandbox exists.
2. Maximum accepted `HistoryLength`, and how far back real data actually goes.
3. Access-token and refresh-token lifetimes, and whether the customer authorization expires
   on a schedule that will need re-consent.
4. Publication lag, and whether readings are revised after first publication (this decides
   whether the day1/day2 re-fetch is necessary or merely cheap insurance).
5. Whether raw and VEE readings arrive as distinct `MeterReading`s — if so they need distinct
   `metric` values rather than silently overwriting each other on the dedupe key.
