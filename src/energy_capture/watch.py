"""``energycap watch-health`` — the part that makes every other signal matter.

This project detects its failures well and delivers none of them. `/healthz` and
``status.json`` are written carefully and read by nothing: the only automated
consumer is a Docker healthcheck that flips a label. Both recent real incidents —
a three-day LG&E authorisation lapse and six days of latched CT zeros — ran under
a green healthcheck and were found by a human looking at a chart.

So this module is deliberately small and deliberately paranoid. It fetches the
status document from the **watched host**, applies a handful of rules, and pushes
what it finds to Pushover.

Two design rules, both learned from the incidents
-------------------------------------------------
**Absence is a failure, not a pass.** The single most dangerous shape here is a
check that silently evaluates to "fine" because the field it wanted was missing.
`/healthz` on the live instance right now has no ``greenbutton`` section at all,
so ``health.meter.stale`` is *absent* — and a naive ``jq '.health.meter.stale ==
true'`` reads that as healthy. Every rule below either finds what it needs or
raises an alarm saying it could not; none of them can pass by default. Same for
the document itself: an unreachable host is the loudest alarm there is, because
that is what a dead collector looks like.

**Never trust an aggregate that cannot go down.** The shared ``scheduler``
section counted only failures and never successes, so it read
``consecutive_failures: 203`` while every job was in fact succeeding. Anything
watching it would have alarmed forever and then been ignored. That is fixed in
``runtime`` (DEVIATIONS #187) and the rules here key on each STAGE's own section,
which resets correctly, rather than on the aggregate.

What it does not do
-------------------
Run on the machine it watches. A box that has died cannot report that it died, so
``HEALTHZ_URL`` has no default — the command has to be told where to look, rather
than quietly checking localhost and passing.

It also cannot tell you that *it* stopped running. A watcher on a sleeping laptop
is silent, and silence is indistinguishable from health — the same hole one level
up. Closing that needs a dead-man's switch (an external service that alerts when
pings STOP). ``deploy/watchdog.md`` says so plainly; this command exits 0 on a
clean run precisely so such a service can be wired to it later.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum
from pathlib import Path
from typing import Any

import httpx

from energy_capture import timeutil
from energy_capture.config import get_settings
from energy_capture.logging import get_logger

STAGE = "watch"
log = get_logger(STAGE)

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"
DEFAULT_TIMEOUT = 15.0

#: How long a still-failing state waits before it pushes again. Six hours is
#: "tell me once per working stretch", not "tell me every quarter hour".
DEFAULT_REPEAT_AFTER_S = 6 * 3600

#: How stale the last successful digest may be before the watchdog says so.
#: The digest is daily, so 26 hours allows one missed firing plus the drift of a
#: restart without alarming, and catches the second miss.
DIGEST_STALE_AFTER_S = 26 * 3600

__all__ = [
    "DIGEST_STALE_AFTER_S",
    "Alarm",
    "ping",
    "Severity",
    "WatchReport",
    "evaluate",
    "fetch_status",
    "push",
    "push_message",
    "run",
    "should_notify",
]


class Severity(IntEnum):
    """Pushover priority levels, named for what they mean here."""

    #: Something is degraded but data is still landing.
    WARNING = 0
    #: Data is being lost, or the collector is gone.
    CRITICAL = 1


@dataclass(frozen=True)
class Alarm:
    check: str
    severity: Severity
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "severity": self.severity.name,
            "detail": self.detail,
        }


@dataclass
class WatchReport:
    url: str
    reachable: bool
    alarms: list[Alarm] = field(default_factory=list)
    checked: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.alarms

    @property
    def worst(self) -> Severity:
        return max((a.severity for a in self.alarms), default=Severity.WARNING)

    def title(self) -> str:
        if self.ok:
            return "energycap OK"
        worst = "CRITICAL" if self.worst is Severity.CRITICAL else "warning"
        return f"energycap {worst}: {len(self.alarms)} check(s) failing"

    def body(self) -> str:
        if self.ok:
            return f"All {len(self.checked)} checks passed."
        return "\n".join(f"[{a.severity.name}] {a.check}: {a.detail}" for a in self.alarms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "reachable": self.reachable,
            "ok": self.ok,
            "alarms": [a.to_dict() for a in self.alarms],
            "checks_run": self.checked,
        }


# ------------------------------------------------------------------- fetching


def fetch_status(url: str, *, client: httpx.Client | None = None) -> dict[str, Any]:
    """GET the status document. Raises for anything that is not a usable body.

    A 503 is NOT an error here: ``/healthz`` answers 503 precisely when it has
    decided it is unhealthy, and that body is the most important one this
    command ever reads. Treating it as a transport failure would replace a
    specific alarm with a generic one.
    """
    owned = client is None
    http = client or httpx.Client(timeout=DEFAULT_TIMEOUT)
    try:
        response = http.get(url, headers={"Accept": "application/json"})
    finally:
        if owned:
            http.close()
    if response.status_code not in (200, 503):
        raise RuntimeError(f"{url} returned HTTP {response.status_code}")
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"{url} did not return JSON: {exc}") from None
    if not isinstance(body, dict):
        raise RuntimeError(f"{url} returned {type(body).__name__}, not an object")
    return body


# ------------------------------------------------------------------ the rules


def _age_s(doc: Mapping[str, Any], section: str, key: str, now: datetime) -> float | None:
    body = doc.get(section)
    if not isinstance(body, Mapping):
        return None
    stamp = body.get(key)
    if not isinstance(stamp, str) or not stamp.strip():
        return None
    try:
        parsed = timeutil.ensure_utc(datetime.fromisoformat(stamp.replace("Z", "+00:00")))
    except ValueError:
        return None
    return (now - parsed).total_seconds()


def _hms(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def evaluate(
    doc: Mapping[str, Any],
    *,
    now: datetime | None = None,
    uploader_stale_after_s: int = 7200,
    spool_pending_ceiling: int = 45000,
    failure_streak_alarm: int = 2,
    stage_sections: Sequence[str] = (
        "uploader",
        "rollup",
        "compactor",
        "bryant_daily",
        "greenbutton",
        "digest",
        "integrity",
    ),
    digest_stale_after_s: int = DIGEST_STALE_AFTER_S,
) -> WatchReport:
    """Apply every rule to a status document.

    Pure: no clock, no network, no settings — everything comes in as arguments,
    so the whole rule set is testable against a recorded body.
    """
    moment = timeutil.ensure_utc(now) if now else datetime.now(UTC)
    report = WatchReport(url="", reachable=True)

    def alarm(check: str, severity: Severity, detail: str) -> None:
        report.alarms.append(Alarm(check, severity, detail))

    # -- the collector's own verdict ------------------------------------
    report.checked.append("health.ok")
    health = doc.get("health")
    if not isinstance(health, Mapping):
        alarm(
            "health.ok",
            Severity.CRITICAL,
            "the status document has no `health` block at all — this is not a "
            "/healthz body, or the collector is writing a shape nothing expects",
        )
    elif health.get("ok") is not True:
        stale = [
            c.get("section")
            for c in health.get("checks", [])
            if isinstance(c, Mapping) and c.get("stale")
        ]
        alarm(
            "health.ok",
            Severity.CRITICAL,
            f"the collector reports itself UNHEALTHY; stale pollers: "
            f"{stale or 'none named'}",
        )

    # -- pollers: a poller that never succeeded is not a fresh poller ----
    report.checked.append("pollers")
    checks = health.get("checks") if isinstance(health, Mapping) else None
    if not isinstance(checks, list) or not checks:
        # NOT nested under "if health exists". A rule that skips itself when its
        # input is missing is the bug this whole command exists to stop, and an
        # earlier draft of this function had exactly that shape.
        alarm(
            "pollers",
            Severity.CRITICAL,
            "no poller checks present — nothing is collecting, or the health "
            "block was built without them",
        )
    else:
        for check in checks:
            if not isinstance(check, Mapping):
                continue
            name = check.get("section", "?")
            if check.get("never_succeeded"):
                alarm("pollers", Severity.CRITICAL, f"{name} has NEVER succeeded")
            elif check.get("stale"):
                age = check.get("age_s")
                alarm(
                    "pollers",
                    Severity.CRITICAL,
                    f"{name} is stale — last success {_hms(float(age))} ago"
                    if isinstance(age, (int, float))
                    else f"{name} is stale",
                )

    # -- the archive is still growing ------------------------------------
    report.checked.append("uploader")
    upload_age = _age_s(doc, "uploader", "last_success_utc", moment)
    if upload_age is None:
        alarm(
            "uploader",
            Severity.CRITICAL,
            "no uploader success timestamp — the archive may have stopped growing "
            "and /healthz would not say so (it judges pollers only)",
        )
    elif upload_age > uploader_stale_after_s:
        alarm(
            "uploader",
            Severity.CRITICAL,
            f"last upload {_hms(upload_age)} ago (limit {_hms(uploader_stale_after_s)}) "
            "— rows are accumulating in the spool instead of landing in S3",
        )

    # -- the spool is not backing up --------------------------------------
    report.checked.append("spool")
    spool = doc.get("spool")
    pending = spool.get("pending_rows") if isinstance(spool, Mapping) else None
    if not isinstance(pending, int):
        alarm("spool", Severity.WARNING, "no spool.pending_rows gauge in the document")
    elif pending > spool_pending_ceiling:
        alarm(
            "spool",
            Severity.CRITICAL,
            f"{pending:,} rows pending upload (ceiling {spool_pending_ceiling:,})",
        )

    # -- per-stage failure streaks ----------------------------------------
    # Each stage's own section, never the shared `scheduler` aggregate: that one
    # only ever counted up (DEVIATIONS #187) and an alarm that cannot clear is
    # an alarm nobody reads.
    report.checked.append("stage_failures")
    for name in stage_sections:
        body = doc.get(name)
        if not isinstance(body, Mapping):
            continue  # a stage this deployment does not run
        streak = body.get("consecutive_failures")
        if isinstance(streak, int) and streak >= failure_streak_alarm:
            detail = str(body.get("last_error", "")).strip()
            alarm(
                "stage_failures",
                Severity.CRITICAL,
                f"{name} has failed {streak}x in a row"
                + (f": {detail[:200]}" if detail else ""),
            )

    # -- the meter dataset ------------------------------------------------
    # `health.meter` is ABSENT until Green Button has been fetched at least
    # once, and absence must not read as healthy — that is the exact shape of
    # the lapse this block was built to expose (#177).
    report.checked.append("meter")
    meter = health.get("meter") if isinstance(health, Mapping) else None
    if isinstance(meter, Mapping):
        if meter.get("stale"):
            age = meter.get("age_days")
            alarm(
                "meter",
                Severity.WARNING,
                f"newest LG&E meter interval is {age} days old — publication "
                "lag, or an authorisation that has quietly stopped working",
            )
    elif isinstance(doc.get("greenbutton"), Mapping):
        alarm(
            "meter",
            Severity.WARNING,
            "greenbutton has run but reports no newest-interval timestamp — "
            "the freshness signal is missing, so meter staleness is UNKNOWN",
        )
    else:
        alarm(
            "meter",
            Severity.WARNING,
            "no greenbutton section — the meter fetch has not run since this "
            "process started, so meter staleness is UNKNOWN, not fine",
        )

    # -- the digest is alive ----------------------------------------------
    # A quiet night and a dead digest produce the same thing on a phone:
    # nothing. So the watchdog asks the question the digest cannot ask about
    # itself — did it run at all? — and treats a missing section as unknown
    # rather than fine, like every other rule here.
    report.checked.append("digest")
    digest_age = _age_s(doc, "digest", "last_success_utc", moment)
    if digest_age is None:
        alarm(
            "digest",
            Severity.WARNING,
            "no digest section with a last_success_utc — the daily anomaly "
            "review has not completed since this process started, so whether "
            "yesterday was normal is UNKNOWN, not fine",
        )
    elif digest_age > digest_stale_after_s:
        alarm(
            "digest",
            Severity.WARNING,
            f"the last successful digest was {_hms(digest_age)} ago (limit "
            f"{_hms(digest_stale_after_s)}) — a daily job that has not run in "
            "over a day is not reviewing anything",
        )

    return report


# ------------------------------------------------------------- notify policy


def should_notify(
    report: WatchReport,
    previous: Mapping[str, Any] | None,
    *,
    now: datetime,
    repeat_after_s: int = DEFAULT_REPEAT_AFTER_S,
) -> tuple[bool, str]:
    """Decide whether this run is worth a push. Returns ``(send, reason)``.

    At a 15-minute cadence, pushing on every failing run means a persistent
    fault sends ~96 identical notifications a day. The owner mutes it, and then
    the next real event arrives in a muted channel — which is precisely the
    silent-failure hole this command was built to close, rebuilt one layer up.

    So the rule is state-change first:

    * the set of failing checks CHANGED -> send (something new broke, or part
      of it cleared)
    * everything is clear and last time it was not -> send the all-clear, so a
      resolved page is visibly resolved rather than just going quiet
    * still failing, unchanged, and ``repeat_after_s`` has passed -> send, so a
      fault cannot be forgotten because it was reported once at 03:00
    * otherwise -> stay quiet
    """
    firing = sorted({a.check for a in report.alarms})
    was_firing = sorted(previous.get("firing", [])) if previous else []

    if firing != was_firing:
        if not firing:
            return True, "recovered"
        return True, "changed"
    if not firing:
        return False, "still-clear"

    last = previous.get("notified_utc") if previous else None
    if isinstance(last, str):
        try:
            when = timeutil.ensure_utc(datetime.fromisoformat(last.replace("Z", "+00:00")))
        except ValueError:
            return True, "unparseable-state"
        if (now - when).total_seconds() >= repeat_after_s:
            return True, "repeat"
        return False, "unchanged"
    return True, "no-prior-notification"


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        body = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        # A missing or corrupt state file must not suppress an alarm; it means
        # "no history", which sends.
        return None
    return body if isinstance(body, dict) else None


def _write_state(
    path: Path,
    report: WatchReport,
    *,
    now: datetime,
    notified: bool,
    undelivered: bool = False,
) -> None:
    """Persist what was reported, and — when a push failed — what was NOT.

    ``firing`` is the memory the change-detector compares against, so advancing
    it is the act of saying "this has been reported". It used to advance
    unconditionally, including on runs where the push was due and Pushover was
    unreachable: the next run then compared the new alarm set against itself,
    found no change, and stayed quiet under the six-hour repeat timer. A
    brand-new CRITICAL raised during a Pushover outage was therefore silent for
    up to six hours — the notifier's own outage suppressing the notification, in
    a command whose entire purpose is delivery.

    So an undelivered push leaves ``firing`` exactly as it was. The very next
    run sees the same state change again and tries again. Failing open, like the
    corrupt-state path above.
    """
    previous = _read_state(path) or {}
    firing = sorted({a.check for a in report.alarms})
    state = {
        "firing": previous.get("firing", []) if undelivered else firing,
        "checked_utc": timeutil.format_utc(now),
        "notified_utc": timeutil.format_utc(now)
        if notified
        else previous.get("notified_utc"),
    }
    if undelivered:
        state["undelivered"] = firing
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2) + "\n")
    except OSError as exc:  # pragma: no cover - defensive
        log.warning("watch_state_unwritable", path=str(path), error=str(exc))


# ---------------------------------------------------------------- delivery


def push_message(
    *,
    title: str,
    message: str,
    token: str,
    user: str,
    priority: int = int(Severity.WARNING),
    client: httpx.Client | None = None,
) -> bool:
    """Send one Pushover notification. Returns whether it was accepted.

    The transport, shared by the watchdog and the nightly digest so there is one
    place that knows how to reach the phone — and one place to fix when it
    breaks. Failing to notify is itself worth a loud log line: a watchdog that
    cannot reach its channel is not watching anything.
    """
    owned = client is None
    http = client or httpx.Client(timeout=DEFAULT_TIMEOUT)
    try:
        response = http.post(
            PUSHOVER_URL,
            data={
                "token": token,
                "user": user,
                "title": title,
                # Pushover truncates at 1024; better to cut it ourselves and
                # say so than to have the last finding vanish silently.
                "message": message
                if len(message) <= 1000
                else message[:990] + "\n… (truncated)",
                "priority": priority,
            },
        )
    except httpx.HTTPError as exc:
        log.error("pushover_unreachable", error=f"{type(exc).__name__}: {exc}")
        return False
    finally:
        if owned:
            http.close()

    if response.status_code == 200:
        log.info("pushover_sent", title=title, chars=len(message))
        return True
    # Pushover puts the reason in the body; it never contains our credentials,
    # and the scrubber covers them regardless.
    log.error(
        "pushover_rejected",
        status=response.status_code,
        body=response.text[:300],
    )
    return False


def push(
    report: WatchReport,
    *,
    token: str,
    user: str,
    client: httpx.Client | None = None,
) -> bool:
    """Send a watchdog report to Pushover."""
    return push_message(
        title=report.title(),
        message=report.body(),
        token=token,
        user=user,
        priority=int(report.worst) if not report.ok else int(Severity.WARNING),
        client=client,
    )


def ping(url: str, *, client: httpx.Client | None = None) -> bool | None:
    """Tell a dead-man's switch this watcher is still alive. Never raises.

    Pinged on every run that COMPLETES, whatever the verdict — including a run
    whose verdict is "the collector is unreachable". That distinction is the
    whole design: Pushover carries what is wrong with the *collector*, and this
    carries the fact that the *watcher* is still there to say so. Tying the ping
    to a clean verdict instead would make a real, persistent alarm silence the
    heartbeat as well, and page twice for one fault.

    Returns True/False, or None when no URL is configured.
    """
    if not url:
        return None
    http = client or httpx.Client(timeout=DEFAULT_TIMEOUT)
    owned = client is None
    try:
        response = http.get(url)
    except httpx.HTTPError as exc:
        # A failed ping is not a failed check. The external service will notice
        # the missing beat on its own schedule, which is exactly its job.
        log.warning("deadman_ping_failed", error=f"{type(exc).__name__}: {exc}")
        return False
    finally:
        if owned:
            http.close()
    if response.status_code < 400:
        return True
    log.warning("deadman_ping_rejected", status=response.status_code)
    return False


def run(
    *,
    url: str | None = None,
    notify: bool = True,
    always_notify: bool = False,
    state_path: Path | str | None = None,
    repeat_after_s: int | None = None,
    ping_url: str | None = None,
) -> dict[str, Any]:
    """``energycap watch-health``. Returns the report; exit code is the caller's.

    Pushes on state CHANGE, plus a reminder every ``repeat_after_s`` while a
    fault persists — see :func:`should_notify` for why pushing every run would
    be worse than not pushing at all. ``always_notify`` overrides that and
    pushes unconditionally, for proving the channel works.
    """
    settings = get_settings()
    target = (url or settings.healthz_url).strip()
    if not target:
        raise ValueError(
            "no health URL: pass --url or set HEALTHZ_URL. There is deliberately "
            "no default — this must point at the WATCHED host, and defaulting to "
            "localhost would make a watcher that always passes."
        )

    try:
        doc = fetch_status(target)
    except Exception as exc:
        # The loudest case. A collector that is gone cannot report that it is
        # gone, so an unreachable endpoint is the alarm, not an error to retry
        # quietly and forget.
        report = WatchReport(url=target, reachable=False)
        report.alarms.append(
            Alarm(
                "reachable",
                Severity.CRITICAL,
                f"cannot read {target}: {type(exc).__name__}: {exc}",
            )
        )
        report.checked.append("reachable")
    else:
        report = evaluate(
            doc,
            uploader_stale_after_s=settings.uploader_stale_after_s,
            spool_pending_ceiling=settings.spool_pending_ceiling,
            failure_streak_alarm=settings.failure_streak_alarm,
        )
        report.url = target
        report.checked.insert(0, "reachable")

    for entry in report.alarms:
        log.warning("watch_alarm", **entry.to_dict())
    log.info(
        "watch_done",
        url=target,
        ok=report.ok,
        alarms=len(report.alarms),
        checks=len(report.checked),
    )

    now = datetime.now(UTC)
    path = Path(state_path) if state_path else settings.spool_dir / "watch-state.json"
    previous = _read_state(path)
    send, reason = should_notify(
        report,
        previous,
        now=now,
        repeat_after_s=(
            repeat_after_s if repeat_after_s is not None else DEFAULT_REPEAT_AFTER_S
        ),
    )
    if always_notify:
        send, reason = True, "always-notify"

    token = settings.pushover_token.get_secret_value()
    user = settings.pushover_user.get_secret_value()
    delivered: bool | None = None
    if notify and send:
        if token and user:
            delivered = push(report, token=token, user=user)
        else:
            delivered = False
            # Not a warning. The command's entire purpose is delivery, so being
            # unable to deliver is the loudest thing it can say about itself.
            log.error(
                "pushover_not_configured",
                detail=(
                    "a notification was due but PUSHOVER_TOKEN/PUSHOVER_USER are "
                    "unset, so nothing was delivered"
                ),
            )
    log.info("watch_notify", send=send, reason=reason, delivered=delivered)
    # A push that was DUE and did not land must not be recorded as reported.
    undelivered = bool(send) and delivered is not True
    if undelivered:
        log.warning(
            "watch_state_not_advanced",
            reason=reason,
            detail=(
                "a notification was due but not delivered; the previous alarm "
                "set is kept so the next run re-detects the change and retries"
            ),
        )
    _write_state(
        path,
        report,
        now=now,
        notified=bool(delivered),
        undelivered=undelivered,
    )

    # Last, and unconditionally: the run completed, so the switch is fed. It is
    # deliberately after delivery, so a ping means "the whole cycle ran", not
    # merely "the process started".
    target_ping = (
        ping_url
        if ping_url is not None
        else settings.healthchecks_ping_url.get_secret_value()
    )
    pinged = ping(target_ping)

    result = report.to_dict()
    result["notified"] = delivered
    result["notify_reason"] = reason
    result["state_path"] = str(path)
    result["deadman_pinged"] = pinged
    return result
