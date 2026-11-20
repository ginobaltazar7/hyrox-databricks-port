{{ config(materialized='table', schema='gold_marketing') }}

-- Q2 per-result: first-km deviation vs later-station degradation.
-- pacing_date is the sentinel for MetricFlow, not an event date.
-- using 8.0 to force float division in Spark SQL, since run times are integers and 8.0 makes intent clearer and safer.
SELECT
    result_id,
    division,
    (run1_time + run2_time + run3_time + run4_time + run5_time + run6_time + run7_time + run8_time) / 8.0 AS mean_run_time,
    run1_time - (run1_time + run2_time + run3_time + run4_time + run5_time + run6_time + run7_time + run8_time) / 8.0 AS first_km_deviation,
    wallballs_time - skierg_time AS station_degradation,
    DATE('2020-01-01') AS pacing_date
FROM {{ ref('pacing_obt') }}
-- Complete results only: a row missing any run or station time cannot
-- produce a deviation or a degradation. Mirrors the Snowflake model;
-- the filter was dropped when this model was ported.
WHERE run1_time IS NOT NULL
  AND run2_time IS NOT NULL
  AND run3_time IS NOT NULL
  AND run4_time IS NOT NULL
  AND run5_time IS NOT NULL
  AND run6_time IS NOT NULL
  AND run7_time IS NOT NULL
  AND run8_time IS NOT NULL
  AND skierg_time IS NOT NULL
  AND wallballs_time IS NOT NULL
