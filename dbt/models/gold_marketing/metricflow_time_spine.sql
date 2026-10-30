{{ config(materialized='table', schema='gold_marketing') }}

-- MetricFlow time spine, Spark translation.
-- Snowflake used dbt.date_spine() + to_date(); Spark uses sequence() + explode() more documentation: https://spark.apache.org/docs/latest/api/sql/index.html#explode
-- Same logic applies: generate a date range and explode it into individual rows.
-- Same range as Snowflake: 2020-01-01 to 2030-01-01, daily.
SELECT explode(sequence(DATE('2020-01-01'), DATE('2030-01-01'), INTERVAL 1 DAY)) AS date_day
