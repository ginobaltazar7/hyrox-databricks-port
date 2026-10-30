"""
    WAP Transform & Anomaly Detection Gate Script — Databricks port.
    Mirrors Snowflake scripts/wap_transforms.py: Write-Audit-Publish,
    Isolation Forest on total_time_minutes only, halt on >5% anomalies.
    Never writes pacing_obt — dbt owns that. Publishes verified staging set only.
"""

import logging
import sys
from sklearn.ensemble import IsolationForest
from pyspark.sql import SparkSession

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s ====>> [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("wap-transform-runner")

def main():
    spark = SparkSession.builder.getOrCreate()

    logger.info("Write Stage: Materializing Silver staging table...")
    # Port bronze column is total_time; alias to total_time_minutes
    # to keep the exact 7-column Snowflake shape.
    spark.sql("""
        CREATE OR REPLACE TABLE hyrox.silver.stg_race_performance_wap AS
        SELECT athlete_id, name, season, location,
               total_time AS total_time_minutes, division, updated_at
        FROM hyrox.bronze.hyrox_raw_season_8
    """)

    logger.info("Audit Stage: Fetching lightweight feature vector for Isolation Forest...")
    df_ml = spark.sql("""
        SELECT athlete_id, total_time_minutes
        FROM hyrox.silver.stg_race_performance_wap
    """).toPandas()

    halt = False
    if len(df_ml) > 10:
        model = IsolationForest(contamination=0.01, random_state=42)
        scores = model.fit_predict(df_ml[['total_time_minutes']])
        anomaly_rate = (scores == -1).mean()
        logger.info(f"Isolation Forest evaluated {len(df_ml)} rows. Anomalies rate: {anomaly_rate * 100:.2f}%")
        if anomaly_rate > 0.05:
            halt = True
    if halt:
        logger.error(f"WAP GATE HALTED: {anomaly_rate * 100:.2f}% anomaly rate exceeds threshold.")
        raise ValueError(f"WAP GATE HALTED: {anomaly_rate * 100:.2f}% anomaly rate exceeds threshold.")

    logger.info("Publish Stage: Promoting verified records to Gold tiers...")
    # GOLD OWNERSHIP: dbt owns hyrox.gold_marketing.pacing_obt.
    # WAP must NEVER write pacing_obt — publish verified set under distinct names only.
    spark.sql("""
        CREATE OR REPLACE TABLE hyrox.gold_marketing.pacing_obt_wap_verified AS
        SELECT * FROM hyrox.silver.stg_race_performance_wap
    """)
    spark.sql("""
        CREATE OR REPLACE TABLE hyrox.gold_finance.fact_race_performance_audit AS
        SELECT * FROM hyrox.silver.stg_race_performance_wap
    """)

    logger.info("WAP Audit Passed: Verified and published cleanly to Gold.")

if __name__ == "__main__":
    main()
