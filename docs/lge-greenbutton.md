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
`gas_main`, `ccf_interval`/`CCF`). It will not, via Connect.

**In practice this costs us nothing: the property has no gas service** (confirmed by the owner,
2026-08-18), so there is no gas meter to export and the `gas_main` half of §13 is moot rather
than blocked. The design stays fuel-agnostic — `METER_SCHEMA`, `meter_key` and `dim_channel`
never knew about fuels — but nothing should be built for gas, and the `channel_map.json` note
suggesting a future `gas_main` entry has been removed so it does not read as pending work.
Recorded in `DEVIATIONS.md` #166.

What *does* survive is `import-greenbutton`'s reason to exist: Download My Data remains the
route for **bulk history** beyond whatever `HistoryLength` Connect grants, and the fallback if
the vendor registration is not approved.

### Customer-side gotcha

Step 4 is easy to trip over: the MyMeter **local** account needs a registration code
requested by email from `MyMeter@lge-ku.com`, and **its email address cannot be the same as
the primary email on the My Account login.** So this needs a second address before the
authorization step — worth starting that email now, since it is the long-latency item and it
is independent of the vendor approval.

---

## 2. Posture: a public site *and* the localhost hand-off

Six of the eighteen fields are URIs a human reviewer will click, so the app needs a real public
presence — but the collector runs on a machine inside the house with no public address, which is
exactly the situation OAuth redirect URIs are worst at. The resolution is to do both, and it is
now built (`site/`, published to **`https://energycap.ericpullen.com/`** via GitHub Pages):

- **The public HTTPS redirect URI is the one registered**, so it survives review and works
  from any browser. It is a *static* page.
- **That page hands the authorization to `localhost`.** It reads `code`/`state` out of the query
  string in the visitor's own browser, then offers a button to
  `http://localhost:<port>/greenbutton/callback?…` plus a copy-and-paste CLI fallback. Nothing
  is transmitted to the site or to anyone else — there is no server-side code to transmit it
  *with* — and the code is stripped from the address bar with `history.replaceState` as soon as
  it is read, so it does not linger in browser history.

So `localhost` is not registered as the redirect URI; it is the hand-off *target*, which sidesteps
the question of whether the form accepts a list at all. If the form does accept multiple redirect
URIs, adding `http://localhost:8080/greenbutton/callback` as a second one lets the collector be
driven directly, and is worth asking for in the approval correspondence — but nothing depends on
it.

**Application type is therefore `Web`**, not `Desktop`: the registered redirect is a web page on
a public origin, and describing it as anything else would be inaccurate.

### Trailing slashes are load-bearing

`redirect_uri` is compared by **exact string match** in OAuth, and GitHub Pages serves these
pages as directories. Measured against the live site on 2026-08-18:

```
/privacy               301 -> https://energycap.ericpullen.com/privacy/
/greenbutton/callback  301 -> https://energycap.ericpullen.com/greenbutton/callback/
/greenbutton/callback?code=TEST123&state=abc
                       301 -> .../greenbutton/callback/?code=TEST123&state=abc
```

The query string *does* survive the redirect, so the failure mode is not a lost authorization
code — it is the exact-match comparison the custodian makes at token exchange, against a
registration that is approved by hand, once, forever. **Every URI in the form below is therefore
the canonical trailing-slash form**, which is what the site serves with no redirect at all.

### The one field GitHub Pages cannot honestly satisfy

**Notify URI.** A static host answers `GET` but not `POST`, so a push notification to it would
fail. This is survivable because a daily subscription can simply be **polled** — and it should
be regardless, since a collector with no inbound access must never depend on push. The
registered notify page says exactly that in plain language, so nobody reading it is misled.

If LG&E requires a working `POST` endpoint, the fix is a Lambda Function URL (or a Cloudflare
Worker) that returns 200 and drops a marker in S3 — roughly half an hour. Deliberately not built
yet: it is speculative work for a requirement that may not exist.

---

## 3. The application, ready to submit

Values settled with the owner 2026-08-18. **The phone number is deliberately not recorded in
this file** — the repository is public — so it is the one field to fill in by hand; it is in the
conversation where it was given.

| Field | Value |
|---|---|
| Software Version | `1.0.0` |
| Client Name | `energycap` |
| Third-Party Name | `energycap` |
| Contact | `eric@ericpullen.com` |
| Policy URI | `https://energycap.ericpullen.com/privacy/` |
| Third-Party Application Description | *see below* |
| Redirect URI | `https://energycap.ericpullen.com/greenbutton/callback/` |
| Third-Party Application Status | `Production` |
| Client URI | `https://energycap.ericpullen.com/` |
| Token Endpoint Authentication Method | `client_secret_basic` |
| Third-Party Application Type | `Web` |
| Scope | `FB=1_3_4_5;IntervalDuration=900_3600;BlockDuration=Daily;HistoryLength=63072000;SubscriptionFrequency=Daily` |
| Third-Party Application Use | `Energy management` |
| Grant Types | `authorization_code refresh_token client_credentials` |
| Third-Party Phone | *(fill in by hand — not committed to a public repo)* |
| Response Types | `code` |
| Third-Party User Portal Screen URI | `https://energycap.ericpullen.com/greenbutton/` |
| Third-Party Notify URI | `https://energycap.ericpullen.com/greenbutton/notify/` |
| Logo URI | `https://energycap.ericpullen.com/logo.png` (180×150 exactly) |
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

## 3a. The site is live

**Done and verified 2026-08-18** — all six registered URIs return a bare `200` over HTTPS on
`https://energycap.ericpullen.com/`, with the certificate approved and HTTPS enforced. Re-run
this before submitting the form, and any time `site/` changes:

```bash
for p in / /privacy/ /greenbutton/ /greenbutton/callback/ /greenbutton/notify/ /logo.png; do
  printf '%-28s %s\n' "$p" "$(curl -s -o /dev/null -w '%{http_code}' "https://energycap.ericpullen.com$p")"
done
```

All six must be `200`, not `301` — see the trailing-slash note in §2.

How it is wired, for whoever has to move it:

- `.github/workflows/pages.yml` publishes **only `site/`** on any push to `main` that touches
  it. Deliberately only `site/`: this research file and the rest of the repo are read on GitHub
  and have no business being served from the application's own origin.
- **Pages had to be enabled out of band.** `actions/configure-pages`'s `enablement: true` does
  not work — the default `GITHUB_TOKEN` cannot create a Pages site and fails with *"Resource not
  accessible by integration"*. It is a one-time admin action:
  `gh api repos/OWNER/REPO/pages -X POST -f build_type=workflow`, or Settings → Pages → Source:
  GitHub Actions.
- **The custom domain is a repository setting, not `site/CNAME`.** With the Actions build type
  the `CNAME` file in the artifact is ignored; the domain was set with
  `gh api … /pages -X PUT -f cname=energycap.ericpullen.com`, then `-F https_enforced=true` once
  the certificate was approved. `site/CNAME` is kept anyway so a branch-based deploy or a
  fork does the right thing, and so the hostname is visible in the tree.
- DNS is a **CNAME** `energycap` → `ericpullen.github.io` in the `ericpullen.com` Route 53
  hosted zone. The apex is untouched.

One consequence worth knowing: the pages reference `/style.css` and `/logo.png` **absolutely**,
which is correct for the custom domain at a root — but means they do not resolve on the
fallback `https://ericpullen.github.io/energyDataCapture/` project URL. The custom domain is the
registered one, so that is the right trade; just don't judge the site by the github.io URL.

Moving to AWS later (CloudFront + S3) is a DNS change and a different publish step; nothing in
the registration changes, which is the point of registering a hostname we control rather than a
`github.io` URL.

---

## 3b. Download My Data already works — `import-greenbutton` is built

Waiting for Connect approval turned out to be unnecessary for *data*: **Download My Data
needs no OAuth**, and it is the same ESPI. `energycap import-greenbutton` is built and has
been run against a real export (2026-08-18, 10 days), so the meter comparison is available
now — see `DEVIATIONS.md` #167–#168 and the README's "Meter vs. panels".

What the real file taught us, which no amount of reading the spec would have:

| | |
|---|---|
| Granularity | **15-minute** (`intervalLength` 900), `uom` 72 (Wh), `powerOfTenMultiplier` 0 |
| Flow | every UsagePoint pairs a forward and a **reverse** MeterReading; reverse is skipped |
| Link direction | the **ReadingType** points down at its MeterReading, not the reverse — the obvious implementation finds nothing |
| Meters | **three UsagePoints carrying an identical series** (`1308468`, `944401`, `944006`) — the same service through meter changes. Summing them trebles the reading |
| `device_id` | the UsagePoint's `name` (the number on the bill), so XML and CSV agree |
| Freshness | the export ended ~8 hours behind real time |

This does not make Connect redundant. Download is a manual click; Connect is the daily
subscription that keeps the meter series current without one, which is the whole point of
having registered.

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
3. `import-greenbutton` retargeted at bulk historical XML (and at Download My Data if the
   registration is not approved).
4. **`energycap greenbutton-authorize --code … [--state …]`** — the CLI that exchanges an
   authorization code for tokens. The published callback page already tells the operator to run
   exactly this, and prints the command with the real code filled in, so **the name is now a
   contract**: if it changes, `site/greenbutton/callback/index.html` changes with it.
5. Optionally `GET /greenbutton/callback` on the health server, so the callback page's hand-off
   button works without a copy-and-paste. Same contract: the path is published.

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
