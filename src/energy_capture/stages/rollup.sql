-- =====================================================================
-- energy/hourly — THE hourly rollup.  This file is the documentation of
-- the math (PLAN.md §10, CLAUDE.md "the rollup SQL is the documentation
-- of the kWh math").  It is executed verbatim: never assembled from
-- string fragments, never string-formatted.  Everything variable is a
-- bound parameter or a registered relation.
--
-- Bound parameters (energy_capture.stages.rollup passes all four):
--   $input_files      LIST(VARCHAR) — raw_30s Parquet paths/URIs for ONE
--                     local day's partition (hourly parts and/or the
--                     compacted day file; both is fine, see `deduped`).
--   $day_start_utc    TIMESTAMPTZ   — local midnight of the day, in UTC.
--   $day_end_utc      TIMESTAMPTZ   — local midnight of the NEXT day, in
--                     UTC.  [start, end) is 23h, 24h or 25h wide.
--   $poll_interval_s  BIGINT        — POLL_INTERVAL_S, the observed
--                     seconds each sample stands for.
--
-- Registered relations (built in Python, so no timezone logic lives in
-- SQL — CLAUDE.md: timeutil is the only place UTC<->local happens):
--   rollup_hours              (hour_start_utc, hour_end_utc, local_hour_start)
--                             one row per physical hour of the local day:
--                             23 on spring-forward, 25 on fall-back.
--   rollup_excluded_metrics   (metric) — model.DAY_GRAIN_METRICS.
--
-- Cardinal rules this query obeys:
--
--  * GROUP BY hour_start_utc, NOT the naive local hour (DEVIATIONS.md #1).
--    On the DST fall-back day the two 01:00 local hours are two distinct
--    UTC hours; grouping on the wall-clock label would merge them and
--    silently lose an hour.  `local_hour_start` rides along as the human
--    readable (and, that day, deliberately ambiguous) label.
--
--  * NO gap filling of any kind.  There is no generate_series over the
--    hours, no LEFT JOIN from rollup_hours, no COALESCE, no zero-fill and
--    no interpolation.  An hour with no samples produces NO ROW; a partly
--    observed hour produces one row with a smaller `sample_count`.
--    `sample_count` is how a reader tells "the load was off" (samples
--    present, low watts) from "the collector was down" (few samples, or
--    no row at all).
--
--  * kwh is OBSERVED-TIME-ONLY (PLAN.md §2.5):
--        kwh = mean_watts * (sample_count * poll_interval_s) / 3.6e6
--    Energy is charged only for time we actually watched, so a
--    half-populated hour yields exactly half the kwh of a full hour at
--    the same wattage.  It is never extrapolated across a gap.
--    kwh is NULL — never 0 — for every metric other than 'watts'
--    (DEVIATIONS.md #2): a 0 would read as "no energy used".
--
-- Output columns and their order are exactly model.HOURLY_SCHEMA; the
-- ORDER BY is exactly model.HOURLY_DEDUPE_KEY.
-- =====================================================================

WITH
-- 1. Assign every raw sample of this local day to its UTC hour bucket.
--    The join to rollup_hours is what both buckets AND scopes the query:
--    an INNER join means only samples inside the day's [start, end) can
--    survive, and the bucket label comes from timeutil, not from SQL
--    timezone arithmetic.
bucketed AS (
    SELECT
        h.hour_start_utc,
        h.local_hour_start,
        r.ts_utc,
        r.source,
        r.device_id,
        r.channel_id,
        r.metric,
        r.unit,
        r.value
    FROM read_parquet($input_files) AS r
    JOIN rollup_hours AS h
      ON r.ts_utc >= h.hour_start_utc
     AND r.ts_utc <  h.hour_end_utc
    WHERE
        -- Redundant with the join (rollup_hours tiles exactly this span);
        -- stated separately so DuckDB can prune Parquet row groups.
        r.ts_utc >= $day_start_utc
    AND r.ts_utc <  $day_end_utc
        -- Day-grain rows (kwh_day, cost_day_usd) live in energy/daily and
        -- would poison an hourly mean (CLAUDE.md rule 6, PLAN.md §4).
        -- They should never be in raw_30s at all; excluded here as well so
        -- that a stray one cannot corrupt the rollup.
    AND r.metric NOT IN (SELECT metric FROM rollup_excluded_metrics)
        -- A NULL value is a gap, not a sample: it must not be counted.
        -- (model.make_observation forbids these upstream; belt and braces.)
    AND r.value IS NOT NULL
),

-- 2. Collapse duplicates on the canonical dedupe key (CLAUDE.md rule 7:
--    ts_utc, source, device_id, channel_id, metric).  A local day's
--    partition normally holds exactly one authoritative copy of the day —
--    either the hourly parts or the compacted day file — but a rollup run
--    that catches the compactor mid-flight could see both, and counting a
--    sample twice would inflate sample_count and therefore kwh.
--    Duplicates are byte-identical in practice; ties are broken on
--    (value, unit) purely so the choice is deterministic rather than
--    dependent on file scan order, which is what makes a re-run
--    byte-identical.
deduped AS (
    SELECT DISTINCT ON (ts_utc, source, device_id, channel_id, metric)
        hour_start_utc,
        local_hour_start,
        ts_utc,
        source,
        device_id,
        channel_id,
        metric,
        unit,
        value
    FROM bucketed
    ORDER BY ts_utc, source, device_id, channel_id, metric, value, unit
),

-- 3. The aggregate.  One row per (hour_start_utc, source, device_id,
--    channel_id, metric) — the hourly grain of PLAN.md §10.
--    local_hour_start is functionally determined by hour_start_utc, so
--    grouping on it too cannot split a bucket; it is listed for clarity.
--    unit is min() rather than a grouping column so that a (pathological)
--    mid-hour unit change cannot split one metric into two rows and break
--    the hourly dedupe key.
aggregated AS (
    SELECT
        hour_start_utc,
        local_hour_start,
        source,
        device_id,
        channel_id,
        metric,
        min(unit)                    AS unit,
        avg(value)                   AS mean,
        min(value)                   AS min,
        max(value)                   AS max,
        quantile_cont(value, 0.95)   AS p95,   -- PLAN.md §10: interpolated
        count(*)                     AS sample_count,
        min(ts_utc)                  AS first_ts_utc,
        max(ts_utc)                  AS last_ts_utc
    FROM deduped
    GROUP BY
        hour_start_utc,
        local_hour_start,
        source,
        device_id,
        channel_id,
        metric
)

-- 4. Energy, for watts only, over observed time only.
--    mean_watts * observed_seconds / 3_600_000, where observed_seconds is
--    sample_count * poll_interval_s — the time we actually watched, not
--    the 3600 seconds the hour contains.  Half the samples => half the
--    kwh.  Every other metric gets NULL (a temperature has no kWh, and 0
--    would be a lie).
SELECT
    hour_start_utc,
    local_hour_start,
    source,
    device_id,
    channel_id,
    metric,
    unit,
    mean,
    min,
    max,
    p95,
    sample_count,
    first_ts_utc,
    last_ts_utc,
    CASE
        WHEN metric = 'watts'
        THEN mean * (sample_count * $poll_interval_s) / 3.6e6
        ELSE NULL
    END AS kwh
FROM aggregated
ORDER BY hour_start_utc, source, device_id, channel_id, metric
