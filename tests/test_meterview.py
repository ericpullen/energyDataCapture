"""The ``/ui`` utility-meter card (:mod:`energy_capture.meterview`).

Four ways this card could lie, and a test for each:

1. **Summing two resolutions.** LG&E publishes the same energy at 900s and
   3600s; totalling both roughly doubles every number on the card.
2. **Showing one service as three meters.** The Download export republishes the
   house under two retired ids. Three tiles for one service invites adding them
   up — which trebles it.
3. **Comparing against the wrong meter.** The account has a house and a barn.
   Only the house has panel CTs, so the comparison must follow the ``primary``
   flag in the map rather than a heuristic.
4. **Presenting stale data as current.** The utility publishes hours behind and
   the fetch is daily, so "today so far" is never "now". The card has to say how
   old it is.

Everything reads a tmp_path spool and tmp_path Parquet; nothing touches a live
API or the real ``SPOOL_DIR``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from energy_capture import meterview, model, timeutil
from energy_capture.config import Settings
from energy_capture.spool.sqlite import open_spool

HOUSE = "1308468"
BARN = "1326254"
RETIRED = "944006"
#: Noon local on an ordinary EDT day.
HOUR_UTC = datetime(2026, 8, 16, 16, 0, tzinfo=UTC)
NOW = datetime(2026, 8, 17, 18, 0, tzinfo=UTC)  # 14:00 local the next day


@pytest.fixture(autouse=True)
def _clean_cache():
    meterview.reset_cache()
    yield
    meterview.reset_cache()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, spool_dir=tmp_path)  # type: ignore[call-arg]


def write_meter(settings: Settings, rows: list[tuple[str, datetime, float, int]]) -> None:
    """Land meter rows exactly as the importer does."""
    import pyarrow.parquet as pq

    table = model.observations_to_table(
        [
            model.make_observation(
                ts_utc=ts,
                source=model.SOURCE_LGE,
                device_id=device,
                channel_id="electric_main",
                metric="kwh_interval",
                value=value,
                interval_s=interval,
            )
            for device, ts, value, interval in rows
        ],
        dataset=model.Dataset.METER,
    )
    out = settings.spool_dir / "meter"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / "lge-202608.parquet")


def quarter_hours(device: str, day_start: datetime, kwh_each: float, count: int = 96):
    return [(device, day_start + timedelta(minutes=15 * n), kwh_each, 900) for n in range(count)]


def labels_for(primary: str | None = HOUSE) -> dict:
    out = {}
    for device, label in ((HOUSE, "LG&E house meter"), (BARN, "LG&E barn meter")):
        out[(model.SOURCE_LGE, device, "electric_main")] = {
            "label": label,
            "short_label": label,
            "primary": device == primary,
        }
    return out


# ------------------------------------------------------------- no data yet


def test_no_meter_data_says_how_to_get_some(settings: Settings) -> None:
    block = meterview.meter_block(now=NOW, settings=settings)
    assert block["available"] is False
    assert "greenbutton-authorize" in block["hint"]


# --------------------------------------------------------------- the tiles


def test_yesterday_and_today_are_split_on_the_local_date(settings: Settings) -> None:
    yesterday_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    today_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW))
    write_meter(
        settings,
        quarter_hours(HOUSE, yesterday_start, 1.0) + quarter_hours(HOUSE, today_start, 0.5, 8),
    )
    block = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())

    (house,) = block["meters"]
    assert house["yesterday_kwh"] == pytest.approx(96.0)
    assert house["today_kwh"] == pytest.approx(4.0)


def test_only_the_finest_interval_series_is_totalled(settings: Settings) -> None:
    """Both series cover the same energy; adding them doubles the tile."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    rows = quarter_hours(HOUSE, day_start, 1.0, 4)
    rows.append((HOUSE, day_start, 4.0, 3600))  # the hourly rollup of those four
    write_meter(settings, rows)

    (house,) = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())["meters"]
    assert house["yesterday_kwh"] == pytest.approx(4.0)
    assert house["interval_s"] == 900


def test_retired_meter_ids_collapse_into_one_tile(settings: Settings) -> None:
    """One service, one tile — three would invite someone to add them up."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    rows = quarter_hours(HOUSE, day_start, 1.0, 8) + quarter_hours(RETIRED, day_start, 1.0, 8)
    write_meter(settings, rows)

    meters = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())["meters"]
    assert [m["device_id"] for m in meters] == [HOUSE]
    assert meters[0]["aliases"] == [RETIRED]


def test_a_genuinely_different_meter_keeps_its_own_tile(settings: Settings) -> None:
    """The barn is a real second service, not an alias — never collapsed."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    write_meter(
        settings,
        quarter_hours(HOUSE, day_start, 1.0, 8) + quarter_hours(BARN, day_start, 0.25, 8),
    )
    meters = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())["meters"]

    assert [m["device_id"] for m in meters] == [HOUSE, BARN], "biggest first"
    assert all(m["aliases"] == [] for m in meters)
    assert meters[1]["label"] == "LG&E barn meter"


def test_the_label_falls_back_to_the_meter_id(settings: Settings) -> None:
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    write_meter(settings, quarter_hours(HOUSE, day_start, 1.0, 4))
    (house,) = meterview.meter_block(now=NOW, settings=settings)["meters"]
    assert house["label"] == HOUSE


# ------------------------------------------------------------- freshness


def test_the_card_reports_how_old_the_data_is(settings: Settings) -> None:
    """"Today so far" is never "now" — the utility publishes hours behind."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW))
    write_meter(settings, quarter_hours(HOUSE, day_start, 1.0, 4))
    block = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())

    last = day_start + timedelta(minutes=45)
    assert block["age_s"] == pytest.approx((NOW - last).total_seconds(), abs=1)
    assert block["stale"] is False
    assert block["last_reading_local"].startswith("2026-08-17")


def test_data_older_than_a_day_and_a_half_is_flagged_stale(settings: Settings) -> None:
    old = NOW - timedelta(days=3)
    write_meter(settings, [(HOUSE, old, 1.0, 900)])
    block = meterview.meter_block(now=NOW, settings=settings, labels=labels_for())
    assert block["stale"] is True


# ------------------------------------------------------------ comparison


def _spool_with_feed(settings: Settings, day_start: datetime, hours: int, watts: float):
    spool = open_spool(settings.spool_dir / "spool.db")
    rows = [
        model.make_observation(
            ts_utc=day_start + timedelta(seconds=30 * tick),
            source=model.SOURCE_LEVITON,
            device_id="hub-a",
            channel_id=channel,
            metric="watts",
            value=watts,
        )
        for tick in range(120 * hours)
        for channel in ("ct_1_a", "ct_1_b")
    ]
    spool.append(rows)
    spool.close()


def _feed_map(tmp_path, hubs=("hub-a",)):
    """A channel map matching the fixture's feeds.

    Without this the module falls back to the repo's real config/channel_map.json
    (four series, two hubs) and every fixture — which writes one hub — is
    correctly judged incomplete. Tests must state their own expectation.
    """
    import json

    path = tmp_path / "channel_map.json"
    path.write_text(json.dumps({"mappings": [
        {"source": "leviton", "device_id": hub, "channel_id": ch}
        for hub in hubs
        for ch in ("ct_1_a", "ct_1_b")
    ]}))
    return path


def test_the_comparison_follows_the_primary_flag_not_a_heuristic(
    settings: Settings, tmp_path
) -> None:
    """The barn could out-consume the house on a heavy charging day.

    Picking "the bigger one" would then compare the panels against the barn and
    report a wild discrepancy, so which meter is the house comes from the map.
    """
    day = timeutil.local_date_of(NOW) - timedelta(days=1)
    day_start, _ = timeutil.local_day_bounds_utc(day)
    # Two feed legs at 1000 W for two hours == 4 kWh.
    _spool_with_feed(settings, day_start, hours=2, watts=1000.0)
    write_meter(
        settings,
        [(HOUSE, day_start, 2.0, 3600), (HOUSE, day_start + timedelta(hours=1), 2.0, 3600)]
        # The barn deliberately reads much higher, to catch a "biggest wins" rule.
        + [(BARN, day_start, 50.0, 3600)],
    )

    block = meterview.meter_block(
        now=NOW,
        settings=settings,
        spool_path=settings.spool_dir / "spool.db",
        labels=labels_for(primary=HOUSE),
        channel_map=_feed_map(tmp_path),
    )
    comparison = block["comparison"]
    assert comparison["available"] is True
    assert comparison["meter"] == HOUSE
    assert comparison["meter_kwh"] == pytest.approx(4.0)
    assert comparison["panel_kwh"] == pytest.approx(4.0)
    assert comparison["difference_pct"] == pytest.approx(0.0)


def test_without_a_primary_flag_the_card_refuses_rather_than_guessing(
    settings: Settings,
) -> None:
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    write_meter(settings, [(HOUSE, day_start, 2.0, 3600), (BARN, day_start, 50.0, 3600)])
    block = meterview.meter_block(
        now=NOW, settings=settings, labels=labels_for(primary=None)
    )
    assert block["comparison"]["available"] is False
    assert "primary" in block["comparison"]["reason"]


def test_the_comparison_is_memoised_on_its_inputs(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The page polls every 5s; a DuckDB rollup must not run at that rate."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    _spool_with_feed(settings, day_start, hours=1, watts=1000.0)
    write_meter(settings, [(HOUSE, day_start, 2.0, 3600)])

    calls = {"n": 0}
    real = meterview._compute_comparison

    def counted(**kwargs):
        calls["n"] += 1
        return real(**kwargs)

    monkeypatch.setattr(meterview, "_compute_comparison", counted)
    for _ in range(4):
        meterview.meter_block(
            now=NOW,
            settings=settings,
            spool_path=settings.spool_dir / "spool.db",
            labels=labels_for(),
        )
    assert calls["n"] == 1, "the comparison recomputed on an unchanged input"


def test_an_unreadable_spool_degrades_to_a_reason_not_an_exception(
    settings: Settings,
) -> None:
    """A dashboard that 500s tells the owner nothing."""
    day_start, _ = timeutil.local_day_bounds_utc(timeutil.local_date_of(NOW) - timedelta(days=1))
    write_meter(settings, [(HOUSE, day_start, 2.0, 3600)])
    broken = settings.spool_dir / "not-a-database.db"
    broken.write_text("this is not sqlite")

    block = meterview.meter_block(
        now=NOW, settings=settings, spool_path=broken, labels=labels_for()
    )
    assert block["available"] is True, "the meter tiles must survive a bad spool"
    assert block["comparison"]["available"] is False


def test_the_card_withholds_the_comparison_when_a_whole_hub_is_missing(
    settings: Settings, tmp_path
) -> None:
    """The /ui meter card had the same blindness `compare-meter` did.

    The fixture writes one hub's two feed legs; the map says two hubs should
    report. Every channel that reported reported fully, so coverage is 100% and
    the old gate passed — while the panel total is short by a whole panel and
    the card would have published "panels read ~50% below the meter" as fact.
    """
    day = timeutil.local_date_of(NOW) - timedelta(days=1)
    day_start, _ = timeutil.local_day_bounds_utc(day)
    _spool_with_feed(settings, day_start, hours=2, watts=1000.0)
    write_meter(
        settings,
        [(HOUSE, day_start, 2.0, 3600), (HOUSE, day_start + timedelta(hours=1), 2.0, 3600)],
    )

    block = meterview.meter_block(
        now=NOW,
        settings=settings,
        spool_path=settings.spool_dir / "spool.db",
        labels=labels_for(primary=HOUSE),
        # Two hubs expected; the spool only has hub-a.
        channel_map=_feed_map(tmp_path, hubs=("hub-a", "hub-b")),
    )
    comparison = block["comparison"]
    assert comparison["available"] is False
    assert comparison["hours_missing_a_feed"] == 2
    assert comparison["series_expected"] == 4
    assert "every feed series" in comparison["reason"]
