{{ config(
    materialized='incremental',
    unique_key='result_id',
    incremental_strategy='merge'
) }}

-- Silver: cleaned race results, one row per result_id.
-- Ported from dbt_project/models/silver/stg_hyrox_races.sql (Snowflake repo).
-- Plausibility guard: no single run lap or station takes over an hour in a
-- real HYROX race (the slowest legit split is well under 30 minutes). The
-- source data holds a handful of impossible splits (e.g. a 558-minute
-- SkiErg), and one such row wrecked three Q1 variances in the Nov 20
-- refresh. COALESCE keeps rows with a missing split: nulls pass through
-- and the downstream models handle them (Q2 drops incomplete rows).
SELECT
  result_id, source_result_id, event_id, athlete_id,
  name, gender, nationality, age_group,
  division, season, year, event_name, location,
  total_time AS total_time_minutes,
  run1_time, run2_time, run3_time, run4_time,
  run5_time, run6_time, run7_time, run8_time,
  skierg_time, sledpush_time, sledpull_time,
  burpeebroadjump_time, rowerg_time, farmerscarry_time,
  sandbaglunges_time, wallballs_time,
  roxzone_time, run_time, work_time, best_run_lap,
  updated_at
FROM {{ source('bronze', 'hyrox_raw_season_8') }}
WHERE
  COALESCE(run1_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run2_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run3_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run4_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run5_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run6_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run7_time, 0) BETWEEN 0 AND 60
  AND COALESCE(run8_time, 0) BETWEEN 0 AND 60
  AND COALESCE(skierg_time, 0) BETWEEN 0 AND 60
  AND COALESCE(sledpush_time, 0) BETWEEN 0 AND 60
  AND COALESCE(sledpull_time, 0) BETWEEN 0 AND 60
  AND COALESCE(burpeebroadjump_time, 0) BETWEEN 0 AND 60
  AND COALESCE(rowerg_time, 0) BETWEEN 0 AND 60
  AND COALESCE(farmerscarry_time, 0) BETWEEN 0 AND 60
  AND COALESCE(sandbaglunges_time, 0) BETWEEN 0 AND 60
  AND COALESCE(wallballs_time, 0) BETWEEN 0 AND 60
  {% if is_incremental() %}
  AND updated_at > (SELECT MAX(updated_at) FROM {{ this }})
  {% endif %}
