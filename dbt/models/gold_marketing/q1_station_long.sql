{{ config(materialized='table', schema='gold_marketing') }}

-- Q1 long: one row per result per station, elite-pro union only, roxzone excluded.
-- Snowflake UNPIVOT -> Spark stack() in Spark dialect. See https://docs.snowflake.com/en/sql-reference/sql/unpivot.html and https://spark.apache.org/docs/latest/api/sql/index.html#stack
SELECT
    result_id,
    division,
    station,
    transition_time
FROM {{ ref('pacing_obt') }}
LATERAL VIEW stack(
    8,
    'SKIERG', skierg_time,
    'SLEDPUSH', sledpush_time,
    'SLEDPULL', sledpull_time,
    'BURPEEBROADJUMP', burpeebroadjump_time,
    'ROWERG', rowerg_time,
    'FARMERSCARRY', farmerscarry_time,
    'SANDBAGLUNGES', sandbaglunges_time,
    'WALLBALLS', wallballs_time
) t AS station, transition_time
WHERE division IN ('pro', 'pro_doubles', 'elite', 'elite_doubles')
