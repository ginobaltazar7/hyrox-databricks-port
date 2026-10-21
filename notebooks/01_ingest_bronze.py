# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Ingest Bronze

# COMMAND ----------

# Ingest the Hyrox season 8 parquet into Delta bronze.
# Adapted from scripts/raw_ingest.py in hyrox-tieredsla-pipeline.
# The pyrox-client path is intentionally left out: the default path reads the
# pre-fetched parquet, exactly like the Snowflake pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional Full season pull via Pyrox client

# COMMAND ----------

# OPTIONAL — full-season pull via the pyrox client (~715K rows). Commented out
# by default: the live path below reads the pre-fetched 5,000-row sample parquet,
# exactly like the Snowflake pipeline. Mirrors the commented pyrox block in
# scripts/raw_ingest.py (hyrox-tieredsla-pipeline).
#
# To use: uncomment this cell AND comment out the
# df = spark.read.parquet(PARQUET_PATH)
# line in the cell below, then re-run.
#
# Caveats:
# - Needs the pyrox package first: run `%pip install pyrox` (serverless allows it).
# - 715K rows land on the driver as pandas, then convert to Spark, ok at this scale, but do monitor driver memory if the dataset ever grows 10x.
# - Columns are normalized (lowercase, underscores) to match the sample parquet.

# %pip install pyrox

# import pyrox
#
# _client = pyrox.PyroxClient()
# _pdf = _client.get_season(season=8)
# _pdf.columns = [c.lower().replace(" ", "_") for c in _pdf.columns]
# print(f"pyrox returned {_pdf.shape[0]:,} rows x {_pdf.shape[1]} cols")
#
# df = spark.createDataFrame(_pdf)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Parquet file pull

# COMMAND ----------

# Config — set these before running.
PARQUET_PATH = "/Volumes/hyrox/bronze/landing/hyrox_season_8.parquet"
# Alternative: download once from the Snowflake repo and upload to the Volume:
#   https://github.com/ginobaltazar7/hyrox-tieredsla-pipeline/raw/main/data/hyrox_season_8.parquet
BRONZE_TABLE = "hyrox.bronze.hyrox_raw_season_8"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-Flight Checks

# COMMAND ----------

# COMMAND ----------

# Pre-flight: source schema peek. The split columns' types decide how the
# split parsing below has to work: numeric minutes pass through, text
# durations ('0:03:53') need converting, any missing must stay NULL — never 0.

# comment this next spark.read.parquet statement out if using the pyrox client above
df = spark.read.parquet(PARQUET_PATH)

# commence pre-flight check
for f in df.schema.fields:
    if f.name.endswith("_time") or f.name == "best_run_lap":
        print(f.name, f.dataType)
df.select("run1_time", "skierg_time", "best_run_lap").show(5, truncate=False)
print("rows:", df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse Split Minutes

# COMMAND ----------

# COMMAND ----------

# Split parsing — port of parse_split_minutes (in Snowflake repo "utils/sanitizers.py").
# Pre-flight showed 19 of 20 split fields already arrive as DoubleType minutes:
# pass-through, NULLs included. Only best_run_lap arrives as 'H:MM:SS' text.
# Missing / blank / unparseable -> NULL, never 0.

from pyspark.sql import functions as F

lap = F.trim(F.col("best_run_lap"))
parts = F.split(lap, ":")
seconds = (
    F.when(F.size(parts) == 3,
           parts.getItem(0).cast("double") * 3600
           + parts.getItem(1).cast("double") * 60
           + parts.getItem(2).cast("double"))
     .when(F.size(parts) == 2,
           parts.getItem(0).cast("double") * 60
           + parts.getItem(1).cast("double"))
)
df = df.withColumn(
    "best_run_lap",
    F.when(lap.isNull() | (lap == "") | ~lap.rlike(r"^\d+:\d{1,2}(:\d{1,2})?$"), None)
     .otherwise(seconds / 60.0),
)
df.select("run1_time", "skierg_time", "best_run_lap").show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Identity Rules

# COMMAND ----------

from pyspark.sql import functions as F

# Identity rules, ported exactly from scripts/raw_ingest.py (Snowflake):
#   athlete_id = "ath_" + sha256(name|gender|nationality|age_group)[:16]
#   result_id  = source_result_id when present and non-blank, else
#                "res_synth_" + sha256(event_id|name|division|total_time)[:16]
df = df.withColumn(
    "athlete_id",
    F.concat(
        F.lit("ath_"),
        F.substring(
            F.sha2(F.concat_ws("|", F.col("name"), F.col("gender"),
                               F.col("nationality"), F.col("age_group")), 256),
            1, 16,
        ),
    ),
).withColumn(
    "result_id",
    F.when(
        F.col("source_result_id").isNotNull() & (F.trim(F.col("source_result_id")) != ""),
        F.trim(F.col("source_result_id")),
    ).otherwise(
        F.concat(
            F.lit("res_synth_"),
            F.substring(
                F.sha2(F.concat_ws("|", F.col("event_id"), F.col("name"),
                                   F.col("division"), F.col("total_time")), 256),
                1, 16,
            ),
        )
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