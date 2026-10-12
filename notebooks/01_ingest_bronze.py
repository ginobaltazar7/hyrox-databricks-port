# Databricks notebook source
# Ingest the Hyrox season 8 parquet into Delta bronze.
# Adapted from scripts/raw_ingest.py in hyrox-tieredsla-pipeline.
# The pyrox-client path is intentionally left out: the default path reads the
# pre-fetched parquet, exactly like the Snowflake pipeline.

# COMMAND ----------

# Config — set these before running.
PARQUET_PATH = "/Volumes/hyrox/bronze/landing/hyrox_season_8.parquet"
# Alternative: download once from the Snowflake repo and upload to the Volume:
#   https://github.com/ginobaltazar7/hyrox-tieredsla-pipeline/raw/main/data/hyrox_season_8.parquet
BRONZE_TABLE = "hyrox.bronze.hyrox_raw_season_8"

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.parquet(PARQUET_PATH)

# Identity rules, ported from the Snowflake pipeline (rule v1):
#   athlete_id  = sha256(name|gender|nationality|age_group)
#   result_id   = coalesce(source_result_id, synthetic hash anchored on event_id)
df = df.withColumn(
    "athlete_id",
    F.sha2(F.concat_ws("|", F.col("name"), F.col("gender"), F.col("nationality"), F.col("age_group")), 256),
).withColumn(
    "result_id",
    F.coalesce(
        F.col("source_result_id"),
        F.sha2(F.concat_ws("|", F.col("event_id"), F.col("athlete_id")), 256),
    ),
).withColumn("updated_at", F.current_timestamp())

# COMMAND ----------

# Validate grain before writing: result_id must be unique and non-null.
violations = df.filter(F.col("result_id").isNull()).count()
assert violations == 0, f"null result_id rows: {violations}"
dupes = df.groupBy("result_id").count().filter("count > 1").count()
assert dupes == 0, f"duplicate result_id values: {dupes}"

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)
print(f"wrote {df.count()} rows to {BRONZE_TABLE}")
