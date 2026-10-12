{{ config(materialized='table', schema='gold_marketing') }}

-- Gold: pacing one-big-table for the analyst questions (Q1–Q5).
-- Ported from dbt_project/models/gold_marketing/pacing_obt.sql (Snowflake repo).
-- Ownership note: exactly one writer owns gold_marketing.pacing_obt. If the
-- Snowflake repo settles on WAP publishing gold instead of dbt, mirror that
-- decision here — do not let both write it.
SELECT * FROM {{ ref('stg_hyrox_races') }}
