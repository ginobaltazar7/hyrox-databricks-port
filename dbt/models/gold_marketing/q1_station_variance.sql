{{ config(materialized='table', schema='gold_marketing') }}

-- Q1 variance: sample variance per station, precomputed, one row per station.
-- Snowflake VAR_SAMP -> Spark var_samp() in Spark dialect. See https://docs.snowflake.com/en/sql-reference/functions/var_samp.html and https://spark.apache.org/docs/latest/api/sql/index.html#var_samp
SELECT
    station,
    DATE('2020-01-01') AS variance_date,
    var_samp(transition_time) AS transition_time_variance,
    COUNT(*) AS n_results,
    AVG(transition_time) AS avg_transition_time
FROM {{ ref('q1_station_long') }}
GROUP BY station, DATE('2020-01-01')
