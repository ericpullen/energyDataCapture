"""``energycap fetch-greenbutton`` — the same ESPI, over the Connect API.

The design claim this file has to defend is that the automated path and the
manual one differ **only in transport**. If they can disagree about what a
reading means, then a month file built from a download and one built from a
fetch are not the same dataset, and the meter comparison quietly depends on
which route the data took.

The other thing pinned here is the window. ESPI's ``published-min``/
``published-max`` are epoch seconds, and this project's ranges are *local* dates
— so the boundary has to be local midnight, including on a 23- or 25-hour DST
day, or a fetch silently clips or overruns a day.

``httpx.MockTransport`` throughout; nothing reaches the utility.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from energy_capture import timeutil
from energy_capture.config import Settings
from energy_capture.sources.lge_auth import (
    LgeAuth,
    LgeAuthError,
    LgeToken,
    LgeTokenCache,
    LgeTransientError,
)
from energy_capture.stages import greenbutton_fetch

RESOURCE = "https://services.example.com/espi/1_1/resource/Subscription/5"


def espi(readings: list[tuple[int, int, int]]) -> str:
    base = "https://services.example.com/espi/1_1/resource"
    point = f"{base}/UsagePoint/00121847"
    mr = f"{point}/MeterReading/1"
    body = "".join(
        f"""<espi:IntervalReading><espi:timePeriod>
          <espi:duration>{d}</espi:duration><espi:start>{s}</espi:start>
        </espi:timePeriod><espi:value>{v}</espi:value></espi:IntervalReading>"""
        for s, d, v in readings
    )
    return f"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:espi="http://naesb.org/espi">
  <entry><link rel="self" href="{point}"/>
    <content><espi:UsagePoint><espi:name>1308468</espi:name>
    </espi:UsagePoint></content></entry>
  <entry><link rel="self" href="{mr}"/><link rel="related" href="{point}"/>
    <content><espi:MeterReading/></content></entry>
  <entry><link rel="self" href="{point}/ReadingType/1"/>
    <link rel="related" href="{mr}"/>
    <content><espi:ReadingType><espi:flowDirection>1</espi:flowDirection>
      <espi:intervalLength>900</espi:intervalLength>
      <espi:powerOfTenMultiplier>0</espi:powerOfTenMultiplier>
      <espi:uom>72</espi:uom></espi:ReadingType></content></entry>
  <entry><link rel="self" href="{mr}/IntervalBlock/1"/><content>
    <espi:IntervalBlock><espi:interval><espi:duration>86400</espi:duration>
      <espi:start>{readings[0][0] if readings else 0}</espi:start></espi:interval>
      {body}</espi:IntervalBlock></content></entry>
</feed>
"""


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        lge_client_id="gbc_test",
        lge_client_secret="s3cret-value",
        spool_dir=tmp_path,
    )


@pytest.fixture
def authorized(settings: Settings) -> LgeAuth:
    """An LgeAuth with a fresh cached token, so no refresh is attempted."""
    LgeTokenCache(settings.spool_dir / "tokens" / "lge.json").save(
        LgeToken(
            access_token="access-token-xxxxxxxxxxxx",
            refresh_token="refresh-token-yyyyyyyyyyyy",
            expires_at=datetime.now(UTC) + timedelta(hours=2),
            resource_uri=RESOURCE,
            client_id="gbc_test",
        )
    )
    return LgeAuth(settings=settings)


def transport(handler) -> tuple[httpx.Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    return httpx.Client(transport=httpx.MockTransport(record)), seen


# --------------------------------------------------------------- the window


def test_the_window_is_local_midnight_to_local_midnight(
    settings: Settings, authorized: LgeAuth
) -> None:
    client, seen = transport(lambda r: httpx.Response(200, text=espi([])))
    day = date(2026, 8, 16)
    greenbutton_fetch.fetch_espi(start=day, end=day, auth=authorized, client=client)

    params = dict(seen[0].url.params)
    start_utc, end_utc = timeutil.local_day_bounds_utc(day)
    assert int(params["published-min"]) == int(start_utc.timestamp())
    assert int(params["published-max"]) == int(end_utc.timestamp())


def test_a_twenty_five_hour_day_is_fetched_whole(
    settings: Settings, authorized: LgeAuth
) -> None:
    """DST fall-back. Assuming 86400 seconds would clip the extra hour."""
    client, seen = transport(lambda r: httpx.Response(200, text=espi([])))
    day = date(2026, 11, 1)  # America/Kentucky/Louisville falls back
    greenbutton_fetch.fetch_espi(start=day, end=day, auth=authorized, client=client)

    params = dict(seen[0].url.params)
    span = int(params["published-max"]) - int(params["published-min"])
    assert span == 25 * 3600, span


def test_the_authorised_subscription_is_preferred_over_the_configured_base(
    settings: Settings, authorized: LgeAuth
) -> None:
    """Only the custodian knows which Subscription this authorisation covers."""
    client, seen = transport(lambda r: httpx.Response(200, text=espi([])))
    greenbutton_fetch.fetch_espi(
        start=date(2026, 8, 16), end=date(2026, 8, 16), auth=authorized, client=client
    )
    assert str(seen[0].url).startswith(RESOURCE)


def test_the_access_token_is_sent_as_a_bearer(
    settings: Settings, authorized: LgeAuth
) -> None:
    client, seen = transport(lambda r: httpx.Response(200, text=espi([])))
    greenbutton_fetch.fetch_espi(
        start=date(2026, 8, 16), end=date(2026, 8, 16), auth=authorized, client=client
    )
    assert seen[0].headers["authorization"] == "Bearer access-token-xxxxxxxxxxxx"


# --------------------------------------------------------------- the errors


@pytest.mark.parametrize("status", [401, 403])
def test_a_rejected_token_says_to_reauthorise(
    settings: Settings, authorized: LgeAuth, status: int
) -> None:
    client, _ = transport(lambda r: httpx.Response(status))
    with pytest.raises(LgeAuthError, match="greenbutton-authorize"):
        greenbutton_fetch.fetch_espi(
            start=date(2026, 8, 16), end=date(2026, 8, 16), auth=authorized, client=client
        )


def test_a_server_error_is_transient(settings: Settings, authorized: LgeAuth) -> None:
    """A 500 must not send a human to a browser; the next run will retry."""
    client, _ = transport(lambda r: httpx.Response(500))
    with pytest.raises(LgeTransientError):
        greenbutton_fetch.fetch_espi(
            start=date(2026, 8, 16), end=date(2026, 8, 16), auth=authorized, client=client
        )


# ------------------------------------------------------- same rows as import


def test_a_fetch_lands_the_same_rows_a_download_would(
    settings: Settings, authorized: LgeAuth, tmp_path: Path
) -> None:
    """The design claim: only the transport differs.

    The same ESPI document is imported from a file and fetched over HTTP, into
    two separate directories, and the resulting Parquet must be identical.
    """
    from energy_capture.stages import greenbutton

    start = int(datetime(2026, 8, 16, 16, tzinfo=UTC).timestamp())
    document = espi([(start, 900, 412), (start + 900, 900, 398)])

    downloaded = tmp_path / "gb.xml"
    downloaded.write_text(document)
    greenbutton.run(path=downloaded, out_dir=tmp_path / "via_file")

    client, _ = transport(lambda r: httpx.Response(200, text=document))
    greenbutton_fetch.run(
        start=date(2026, 8, 16),
        end=date(2026, 8, 16),
        out_dir=tmp_path / "via_api",
        auth=authorized,
        client=client,
    )

    by_file = pq.read_table(tmp_path / "via_file" / "lge-202608.parquet")
    by_api = pq.read_table(tmp_path / "via_api" / "lge-202608.parquet")
    assert by_file.equals(by_api)


def test_refetching_an_overlapping_range_corrects_rather_than_duplicates(
    settings: Settings, authorized: LgeAuth, tmp_path: Path
) -> None:
    """MyMeter revises recent intervals; the second read has to win."""
    start = int(datetime(2026, 8, 16, 16, tzinfo=UTC).timestamp())
    out = tmp_path / "meter"

    client, _ = transport(lambda r: httpx.Response(200, text=espi([(start, 900, 400)])))
    greenbutton_fetch.run(
        start=date(2026, 8, 16), end=date(2026, 8, 16),
        out_dir=out, auth=authorized, client=client,
    )
    client, _ = transport(lambda r: httpx.Response(200, text=espi([(start, 900, 550)])))
    greenbutton_fetch.run(
        start=date(2026, 8, 16), end=date(2026, 8, 16),
        out_dir=out, auth=authorized, client=client,
    )

    table = pq.read_table(out / "lge-202608.parquet")
    assert table.num_rows == 1, "the revised interval was duplicated"
    assert table.column("value").to_pylist() == [pytest.approx(0.550)]


def test_an_empty_range_writes_nothing_rather_than_an_empty_file(
    settings: Settings, authorized: LgeAuth, tmp_path: Path
) -> None:
    """No data is no rows. It is never a file of zeros."""
    client, _ = transport(lambda r: httpx.Response(200, text=espi([])))
    summary = greenbutton_fetch.run(
        start=date(2026, 8, 16), end=date(2026, 8, 16),
        out_dir=tmp_path / "meter", auth=authorized, client=client,
    )
    assert summary["rows"] == 0
    assert summary["files"] == []
    assert not (tmp_path / "meter" / "lge-202608.parquet").exists()


def test_dry_run_fetches_but_writes_nothing(
    settings: Settings, authorized: LgeAuth, tmp_path: Path
) -> None:
    start = int(datetime(2026, 8, 16, 16, tzinfo=UTC).timestamp())
    client, seen = transport(
        lambda r: httpx.Response(200, text=espi([(start, 900, 400)]))
    )
    summary = greenbutton_fetch.run(
        start=date(2026, 8, 16), end=date(2026, 8, 16),
        out_dir=tmp_path / "meter", auth=authorized, client=client, dry_run=True,
    )
    assert seen, "a dry run should still prove the fetch works"
    assert summary["rows"] == 1
    assert summary["files"] == []
    assert not (tmp_path / "meter").exists()
