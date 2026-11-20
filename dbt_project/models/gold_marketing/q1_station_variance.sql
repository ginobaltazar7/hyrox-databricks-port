{{ config(materialized='table', schema='gold_marketing') }}

-- Q1 variance: sample variance per station, precomputed, one row per station.
-- Snowflake VARIANCE (= VAR_POP, population variance) -> Spark var_pop() in Spark dialect. See https://docs.snowflake.com/en/sql-reference/functions/var_pop.html and https://spark.apache.org/docs/latest/api/sql/index.html#var_pop
SELECT
    station,
    DATE('2020-01-01') AS variance_date,
    var_pop(transition_time) AS transition_time_variance,
    COUNT(*) AS n_results,
    AVG(transition_time) AS avg_transition_time
FROM {{ ref('q1_station_long') }}
GROUP BY station, DATE('2020-01-01')
