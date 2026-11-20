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
# full-season parquet from the Volume and draws a fixed-seed 5,000-row sample
# in Spark, so bronze stays the 5,000-row sample the models and the app are
# built on. The full pull runs on GitHub Actions (pull-season.yml) and lands
# in the Volume; Colab plus the land workflow is the manual fallback.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional Full season pull via Pyrox client

# COMMAND ----------

# OPTIONAL — full-season pull via the pyrox client (~715K rows). Commented out
# by default: the full pull runs on GitHub Actions (pull-season.yml) and lands
# in the Volume, and the live path below samples 5,000 rows from it in Spark.
# Mirrors the commented pyrox block in scripts/raw_ingest.py
# (hyrox-tieredsla-pipeline).
#
# To use: uncomment this cell AND comment out the
# df = spark.read.parquet(PARQUET_PATH)
# line in the cell below, then re-run.
#
# Caveats:
# - Needs the pyrox-client package first: run `%pip install pyrox-client`
#   (NOT `pyrox` — that is an unrelated HTTP proxy library on PyPI).
#   NOTE: this block cannot run on Free Edition serverless — outbound internet
#   is restricted to trusted domains (PyPI is allowed, the HYROX API is not),
#   so the API call fails. The pull runs on GitHub Actions (pull-season.yml)
#   instead; Colab plus the land workflow is the manual fallback.
# - 715K rows land on the driver as pandas, then convert to Spark, ok at this scale, but do monitor driver memory if the dataset ever grows 10x.
# - Columns are normalized (lowercase, underscores) to match the sample parquet.

# %pip install pyrox-client

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
# Sample — the parquet at PARQUET_PATH is now the full-season pull (~715K
# rows). Bronze stays the 5,000-row sample the models and the app are built
# on. The sample is drawn by a hash of each row's identity fields (see
# below), so the same source file always yields the same sample regardless
# of how Spark partitions the read, and a refresh only changes the CSVs
# when the source file changes.
SAMPLE_ROWS = 5000

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
print("rows in source file:", df.count())

# Sample before anything else: exact 5,000 rows, deterministic.
# The sample key is a hash of the row's stable identity fields; taking the
# 5,000 smallest hashes gives an exact count (df.sample(fraction) only
# approximates it) and the same source file always yields the same sample,
# regardless of how Spark partitions the read — unlike F.rand(seed), which
# seeds per partition. Sampling here (before split parsing and the identity
# rules) also keeps the SHA work to 5,000 rows instead of ~715K. The extra
# orderBy columns are tiebreakers, covering every field the identity rules
# use, so the order is total even when two rows share a sample key.
from pyspark.sql import functions as F
df = df.withColumn(
    "_sample_key",
    F.sha2(
        F.concat_ws(
            "|",
            F.col("source_result_id"),
            F.col("event_id"),
            F.col("name"),
            F.col("total_time"),
        ),
        256,
    ),
)
df = (
    df.orderBy(
        F.col("_sample_key"),
        F.col("source_result_id"),
        F.col("event_id"),
        F.col("division"),
        F.col("name"),
        F.col("total_time"),
    )
    .limit(SAMPLE_ROWS)
    .drop("_sample_key")
)
print("rows after sample:", df.count())

# commence pre-flight check
for f in df.schema.fields:
    if f.name.endswith("_time") or f.name == "best_run_lap":
        print(f.name, f.dataType)
df.select("run1_time", "skierg_time", "best_run_lap").show(5, truncate=False)


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