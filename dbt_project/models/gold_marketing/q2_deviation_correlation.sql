{{ config(materialized='table', schema='gold_marketing') }}

-- Q2 correlation: per-division, plus pooled ALL row.
-- Correlation lives here only, not as a MetricFlow metric.
-- Snowflake corr() -> Spark corr() in Spark dialect. See https://docs.snowflake.com/en/sql-reference/functions/corr.html and https://spark.apache.org/docs/latest/api/sql/index.html#corr
SELECT
    division,
    COUNT(*) AS n_results,
    AVG(first_km_deviation) AS avg_first_km_deviation,
    AVG(station_degradation) AS avg_station_degradation,
    corr(first_km_deviation, station_degradation) AS correlation
FROM {{ ref('q2_station_degradation') }}
GROUP BY division

UNION ALL

SELECT
    'ALL' AS division,
    COUNT(*) AS n_results,
    AVG(first_km_deviation) AS avg_first_km_deviation,
    AVG(station_degradation) AS avg_station_degradation,
    corr(first_km_deviation, station_degradation) AS correlation
FROM {{ ref('q2_station_degradation') }}
