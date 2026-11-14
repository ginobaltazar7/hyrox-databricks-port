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
