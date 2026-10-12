-- Delta DDL for the Hyrox Databricks port.
-- Translated from sql/01_setup_infrastructure.sql in hyrox-tieredsla-pipeline.
-- Snowflake → Delta type map: VARCHAR→STRING, INT→INT, FLOAT→DOUBLE,
-- TIMESTAMP_NTZ→TIMESTAMP. Run on the Serverless Starter Warehouse.
-- Free Edition: the metastore has no storage root, so create catalog/schemas
-- via SQL here (CLI/API catalog creation fails).

-- If CREATE CATALOG is not permitted on your workspace, skip it and create the
-- schemas inside the default workspace catalog instead.
CREATE CATALOG IF NOT EXISTS hyrox COMMENT 'Hyrox pipeline port from Snowflake';

USE CATALOG hyrox;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold_marketing;

-- Bronze: raw ingested season 8 results. Grain = result_id.
-- Mirrors SPORTS_ANALYTICS_DB.RAW_BRONZE.HYROX_RAW_SEASON_8 (35 columns).
CREATE TABLE IF NOT EXISTS bronze.hyrox_raw_season_8 (
  result_id STRING NOT NULL,          -- coalesce(source_result_id, synthetic), the raw grain
  source_result_id STRING,            -- raw value, nullable (698K nulls observed)
  event_id STRING NOT NULL,           -- zero nulls, anchors the synthetic hash
  athlete_id STRING NOT NULL,         -- derived sha256(name|gender|nationality|age_group), rule v1
  name STRING,
  gender STRING,
  nationality STRING,
  age_group STRING,
  division STRING,                    -- doubles/relay encoded here (elite_doubles, doubles, pro_doubles, relay)
  season INT,                         -- NULLABLE: do not default missing to 8
  year INT,
  event_name STRING,
  location STRING,
  total_time_minutes DOUBLE,
  run1_time DOUBLE,
  run2_time DOUBLE,
  run3_time DOUBLE,
  run4_time DOUBLE,
  run5_time DOUBLE,
  run6_time DOUBLE,
  run7_time DOUBLE,
  run8_time DOUBLE,
  skierg_time DOUBLE,
  sledpush_time DOUBLE,
  sledpull_time DOUBLE,
  burpeebroadjump_time DOUBLE,
  rowerg_time DOUBLE,
  farmerscarry_time DOUBLE,
  sandbaglunges_time DOUBLE,
  wallballs_time DOUBLE,
  roxzone_time DOUBLE,
  run_time DOUBLE,
  work_time DOUBLE,
  best_run_lap DOUBLE,
  updated_at TIMESTAMP               -- pipeline load time
)
USING DELTA
COMMENT 'Bronze: raw Hyrox season 8 results, ported from Snowflake RAW_BRONZE';

-- Silver and gold are built by dbt (see dbt/models/). Tables are created
-- by the models themselves; the schemas above are all the DDL they need.
