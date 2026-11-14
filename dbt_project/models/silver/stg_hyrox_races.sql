{{ config(
    materialized='incremental',
    unique_key='result_id',
    incremental_strategy='merge'
) }}

-- Silver: cleaned race results, one row per result_id.
-- Ported from dbt_project/models/silver/stg_hyrox_races.sql (Snowflake repo).
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
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
