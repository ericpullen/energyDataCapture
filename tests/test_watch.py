"""The watchdog's only job is to not be quietly wrong.

Every incident this command exists to catch had the same shape: a signal was
produced correctly and consumed by nothing, so the system looked healthy. The
way to rebuild that hole here is a rule that passes because the field it wanted
was missing. So the bulk of this file is absence cases — no section, no
timestamp, no checks list, no document at all — each asserting an ALARM rather
than silence.

``evaluate`` is pure, so the whole rule set is exercised against dict literals
with no clock and no network.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from energy_capture.watch import (
    Alarm,
    Severity,
    WatchReport,
    evaluate,
    fetch_status,
    push,
    should_notify,
)

NOW = datetime(2026, 8, 23, 23, 30, tzinfo=UTC)


def healthy(**overrides) -> dict:
    """A status document with every check passing."""
    doc = {
        "uploader": {"last_success_utc": "2026-08-23T23:05:01Z", "consecutive_failures": 0},
        "rollup": {"last_success_utc": "2026-08-23T23:20:03Z", "consecutive_failures": 0},
        "compactor": {"last_success_utc": "2026-08-23T05:30:00Z", "consecutive_failures": 0},
        "greenbutton": {
            "last_success_utc": "2026-08-23T13:15:00Z",
            "consecutive_failures": 0,
            "newest_interval_utc": "2026-08-23T03:45:00Z",
        },
        "spool": {"pending_rows": 3612},
        "health": {
            "ok": True,
            "meter": {"stale": False, "age_days": 0.8},
            "checks": [
                {"section": "leviton", "stale": False, "never_succeeded": False, "age_s": 11.9},
                {"section": "bryant_status", "stale": False, "never_succeeded": False, "age_s": 7.2},
            ],
        },
    }
    doc.update(overrides)
    return doc


def checks_that_fired(report: WatchReport) -> set[str]:
    return {alarm.check for alarm in report.alarms}


def test_a_fully_healthy_document_raises_nothing() -> None:
    report = evaluate(healthy(), now=NOW)
    assert report.ok, report.body()
    assert report.alarms == []
    # If this list shrinks, a rule was silently dropped.
    assert set(report.checked) == {
        "health.ok", "pollers", "uploader", "spool", "stage_failures", "meter",
    }


# ------------------------------------------------------ absence is not a pass


def test_an_empty_document_alarms_on_everything_it_cannot_check() -> None:
    """The nightmare case: a body that answers 200 and says nothing.

    Every rule must notice it has no evidence rather than concluding health.
    """
    report = evaluate({}, now=NOW)
    assert not report.ok
    assert checks_that_fired(report) == {
        "health.ok", "pollers", "uploader", "spool", "meter",
    }
    assert report.worst is Severity.CRITICAL


def test_a_missing_meter_block_is_unknown_not_healthy() -> None:
    """Measured on the live instance 2026-08-23: no ``greenbutton`` section at
    all, so ``health.meter`` is absent. ``jq '.health.meter.stale == true'``
    reads that as fine — which is exactly the #177 lapse's disguise."""
    doc = healthy()
    del doc["greenbutton"]
    del doc["health"]["meter"]

    report = evaluate(doc, now=NOW)
    assert checks_that_fired(report) == {"meter"}
    assert "UNKNOWN, not fine" in report.alarms[0].detail


def test_greenbutton_running_but_reporting_no_freshness_also_alarms() -> None:
    """The D1 shape: the fetch succeeds, ``newest_interval_utc`` is gone, and
    the freshness field deletes itself. Distinct message from 'never ran'."""
    doc = healthy()
    del doc["health"]["meter"]
    doc["greenbutton"].pop("newest_interval_utc")

    report = evaluate(doc, now=NOW)
    assert checks_that_fired(report) == {"meter"}
    assert "reports no newest-interval timestamp" in report.alarms[0].detail


def test_an_uploader_with_no_timestamp_alarms_rather_than_scoring_zero_age() -> None:
    doc = healthy()
    doc["uploader"] = {"consecutive_failures": 0}
    report = evaluate(doc, now=NOW)
    assert "uploader" in checks_that_fired(report)
    assert report.worst is Severity.CRITICAL


def test_an_empty_poller_checks_list_alarms() -> None:
    """`health.ok: true` with nothing actually being checked is not health."""
    doc = healthy()
    doc["health"]["checks"] = []
    report = evaluate(doc, now=NOW)
    assert "pollers" in checks_that_fired(report)


def test_a_document_with_no_health_block_is_not_a_healthz_body() -> None:
    doc = healthy()
    del doc["health"]
    report = evaluate(doc, now=NOW)
    assert "health.ok" in checks_that_fired(report)
    assert "no `health` block" in next(
        a.detail for a in report.alarms if a.check == "health.ok"
    )


# ------------------------------------------------------------- real failures


def test_the_collector_declaring_itself_unhealthy_names_the_stale_poller() -> None:
    doc = healthy()
    doc["health"]["ok"] = False
    doc["health"]["checks"][0]["stale"] = True

    report = evaluate(doc, now=NOW)
    assert "health.ok" in checks_that_fired(report)
    assert "leviton" in next(a.detail for a in report.alarms if a.check == "health.ok")


def test_a_poller_that_never_succeeded_is_critical() -> None:
    doc = healthy()
    doc["health"]["checks"][1]["never_succeeded"] = True
    report = evaluate(doc, now=NOW)
    assert "pollers" in checks_that_fired(report)
    assert "NEVER succeeded" in report.alarms[0].detail


def test_a_stalled_uploader_is_caught_even_while_healthz_stays_green() -> None:
    """A3: /healthz judges pollers only. Rotated S3 keys leave it green forever
    while the archive quietly stops growing — so this rule does not consult it."""
    doc = healthy()
    doc["uploader"]["last_success_utc"] = "2026-08-23T15:05:01Z"  # 8h+ stale

    report = evaluate(doc, now=NOW, uploader_stale_after_s=7200)
    assert doc["health"]["ok"] is True
    assert "uploader" in checks_that_fired(report)
    assert report.worst is Severity.CRITICAL


def test_a_backed_up_spool_is_caught() -> None:
    doc = healthy()
    doc["spool"]["pending_rows"] = 120_000
    report = evaluate(doc, now=NOW, spool_pending_ceiling=45_000)
    assert "spool" in checks_that_fired(report)


def test_a_stage_failure_streak_alarms_and_carries_the_error() -> None:
    doc = healthy()
    doc["rollup"] = {"consecutive_failures": 6, "last_error": "AccessDenied: s3:PutObject"}
    report = evaluate(doc, now=NOW, failure_streak_alarm=2)
    assert "stage_failures" in checks_that_fired(report)
    assert "AccessDenied" in report.alarms[0].detail


def test_one_transient_miss_stays_quiet() -> None:
    """An alarm that fires on every single hourly blip gets muted by its owner,
    and then the real one is missed too."""
    doc = healthy()
    doc["rollup"]["consecutive_failures"] = 1
    assert evaluate(doc, now=NOW, failure_streak_alarm=2).ok


def test_the_shared_scheduler_aggregate_is_deliberately_not_a_rule() -> None:
    """It counted only failures and never successes, reaching 203 on the live
    instance while every job was succeeding (DEVIATIONS #187). Even with that
    fixed, the per-stage sections are the precise signal and this one is not
    consulted — a watcher keyed to it would have alarmed forever."""
    doc = healthy()
    doc["scheduler"] = {
        "consecutive_failures": 203,
        "last_error": "RollupError: 1 of 1 day(s) failed",
        "job": "rollup_hourly",
    }
    assert evaluate(doc, now=NOW).ok


def test_a_stage_this_deployment_does_not_run_is_not_an_alarm() -> None:
    """After the poller/batch split (#186) the collector has no rollup section.
    Absent-because-not-configured differs from absent-because-broken, and only
    the sections that report at all are streak-checked."""
    doc = healthy()
    del doc["rollup"]
    del doc["compactor"]
    assert evaluate(doc, now=NOW).ok


def test_alarms_accumulate_rather_than_short_circuiting() -> None:
    """One page should say everything that is wrong, not just the first thing."""
    doc = healthy()
    doc["health"]["ok"] = False
    doc["uploader"]["last_success_utc"] = "2026-08-20T00:00:00Z"
    doc["spool"]["pending_rows"] = 200_000
    doc["rollup"]["consecutive_failures"] = 9

    report = evaluate(doc, now=NOW)
    assert checks_that_fired(report) == {
        "health.ok", "uploader", "spool", "stage_failures",
    }
    assert report.body().count("\n") == 3


# --------------------------------------------------------------- transport


def test_a_503_body_is_read_not_discarded() -> None:
    """/healthz answers 503 exactly when it has decided it is unhealthy. That
    body is the most important one this command ever reads."""
    transport = httpx.MockTransport(
        lambda request: httpx.Response(503, json={"health": {"ok": False, "checks": []}})
    )
    with httpx.Client(transport=transport) as client:
        doc = fetch_status("http://host:8080/healthz", client=client)
    assert doc["health"]["ok"] is False


@pytest.mark.parametrize("status", [401, 404, 500])
def test_any_other_status_is_an_error(status: int) -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(status, text="nope"))
    with httpx.Client(transport=transport) as client:
        with pytest.raises(RuntimeError, match=str(status)):
            fetch_status("http://host:8080/healthz", client=client)


def test_a_non_json_body_is_an_error_not_an_empty_document() -> None:
    """An empty document would evaluate to 'everything unknown', which is the
    right answer — but saying 'the endpoint served HTML' is a better one."""
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<html>gateway</html>")
    )
    with httpx.Client(transport=transport) as client:
        with pytest.raises(RuntimeError, match="did not return JSON"):
            fetch_status("http://host:8080/healthz", client=client)


# ---------------------------------------------------------------- delivery


def test_pushover_gets_the_alarms_and_a_priority() -> None:
    sent: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent["body"] = request.content.decode()
        return httpx.Response(200, json={"status": 1})

    report = WatchReport(url="u", reachable=True)
    report.alarms.append(Alarm("uploader", Severity.CRITICAL, "8h stale"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert push(report, token="tok", user="usr", client=client)

    assert "uploader" in sent["body"]
    assert "priority=1" in sent["body"]


def test_a_rejected_push_reports_failure_rather_than_pretending() -> None:
    """A watchdog that cannot reach its channel is not watching anything."""
    transport = httpx.MockTransport(
        lambda request: httpx.Response(400, json={"errors": ["application token is invalid"]})
    )
    report = WatchReport(url="u", reachable=True)
    report.alarms.append(Alarm("x", Severity.CRITICAL, "y"))

    with httpx.Client(transport=transport) as client:
        assert not push(report, token="bad", user="usr", client=client)


def test_an_unreachable_pushover_is_survived_and_reported() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route")

    report = WatchReport(url="u", reachable=True)
    report.alarms.append(Alarm("x", Severity.CRITICAL, "y"))

    with httpx.Client(transport=httpx.MockTransport(boom)) as client:
        assert not push(report, token="t", user="u", client=client)


def test_the_title_says_how_bad_it_is_at_a_glance() -> None:
    report = WatchReport(url="u", reachable=True)
    assert report.title() == "energycap OK"

    report.alarms.append(Alarm("meter", Severity.WARNING, "lagging"))
    assert "warning" in report.title()

    report.alarms.append(Alarm("uploader", Severity.CRITICAL, "stale"))
    assert "CRITICAL" in report.title()
    assert "2 check(s)" in report.title()


# ------------------------------------------------------------ notify policy
#
# An alarm that fires 96 times a day gets muted, and a muted channel is the
# silent-failure hole this command exists to close, rebuilt one layer up.


def _report(*checks: str) -> WatchReport:
    report = WatchReport(url="u", reachable=True)
    for name in checks:
        report.alarms.append(Alarm(name, Severity.CRITICAL, "detail"))
    return report


def test_a_new_fault_notifies() -> None:
    """No history at all: the firing set went from nothing to something."""
    send, reason = should_notify(_report("uploader"), None, now=NOW)
    assert send and reason == "changed"


def test_a_state_file_that_records_no_push_notifies() -> None:
    """Same firing set, but nothing was ever actually delivered — e.g. Pushover
    was unconfigured or rejected the last attempt. Retry rather than assume."""
    state = {"firing": ["uploader"], "notified_utc": None}
    send, reason = should_notify(_report("uploader"), state, now=NOW)
    assert send and reason == "no-prior-notification"


def test_the_same_fault_unchanged_stays_quiet() -> None:
    state = {"firing": ["uploader"], "notified_utc": "2026-08-23T23:00:00Z"}
    send, reason = should_notify(_report("uploader"), state, now=NOW)
    assert not send and reason == "unchanged"


def test_a_second_fault_appearing_notifies_again() -> None:
    state = {"firing": ["uploader"], "notified_utc": "2026-08-23T23:00:00Z"}
    send, reason = should_notify(_report("uploader", "spool"), state, now=NOW)
    assert send and reason == "changed"


def test_recovery_sends_an_all_clear() -> None:
    """A resolved page should be visibly resolved, not merely quiet — otherwise
    'no notification' means both 'fixed' and 'the watcher died'."""
    state = {"firing": ["uploader"], "notified_utc": "2026-08-23T23:00:00Z"}
    send, reason = should_notify(_report(), state, now=NOW)
    assert send and reason == "recovered"


def test_a_persistent_fault_is_repeated_after_the_window() -> None:
    """So a fault reported once at 03:00 cannot be forgotten."""
    state = {"firing": ["uploader"], "notified_utc": "2026-08-23T10:00:00Z"}
    send, reason = should_notify(_report("uploader"), state, now=NOW, repeat_after_s=6 * 3600)
    assert send and reason == "repeat"


def test_steady_health_never_notifies() -> None:
    state = {"firing": [], "notified_utc": "2026-08-01T00:00:00Z"}
    send, reason = should_notify(_report(), state, now=NOW)
    assert not send and reason == "still-clear"


def test_a_corrupt_state_file_notifies_rather_than_suppressing() -> None:
    """Failing open is the only safe direction: a lost state file must never be
    able to silence a live alarm."""
    send, _ = should_notify(_report("uploader"), {"firing": ["uploader"], "notified_utc": "garbage"}, now=NOW)
    assert send
