# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Route Reliability Scores
# MAGIC
# MAGIC Aggregates Silver stop events into the `route_reliability_scores` table —
# MAGIC one row per (route_id, day_type, hour_bucket) combination.
# MAGIC
# MAGIC **Schema:**
# MAGIC ```
# MAGIC route_id, day_type, hour_bucket,
# MAGIC score_1_to_5,          -- headline score, 1 (worst) to 5 (best)
# MAGIC pct_on_time,           -- % of observed trips with delay <= ON_TIME_THRESHOLD_SEC
# MAGIC avg_delay_minutes,
# MAGIC delay_stddev_minutes,  -- consistency: low stddev = predictable even if often late
# MAGIC cancellation_rate,     -- not yet available from GTFS-R alone; placeholder 0.0
# MAGIC sample_size,           -- number of stop events this score is based on
# MAGIC last_updated           -- when this Gold run executed
# MAGIC ```
# MAGIC
# MAGIC Buckets with fewer than MIN_SAMPLE_SIZE observations get `score_1_to_5 = NULL`
# MAGIC rather than a misleading number. The API surfaces this as "insufficient data."

# COMMAND ----------

SILVER_PATH = "dbfs:/FileStore/dublin_transit/silver/stop_events"
GOLD_PATH = "dbfs:/FileStore/dublin_transit/gold/route_reliability_scores"

# A trip is "on time" if it arrives no more than this many seconds late.
ON_TIME_THRESHOLD_SEC = 300   # 5 minutes — standard transit industry threshold

# Minimum observed stop events before we'll report a confident score.
MIN_SAMPLE_SIZE = 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver and aggregate per (route, day_type, hour_bucket)

# COMMAND ----------

from pyspark.sql.functions import (
    col, avg, stddev, count, sum as spark_sum,
    when, lit, round as spark_round, current_timestamp
)
from pyspark.sql.types import DoubleType

silver = spark.read.format("delta").load(SILVER_PATH)

aggregated = (
    silver
    .groupBy("route_id", "day_type", "hour_bucket")
    .agg(
        count("*").alias("sample_size"),
        avg(col("delay_seconds") / 60).alias("avg_delay_minutes"),
        stddev(col("delay_seconds") / 60).alias("delay_stddev_minutes"),
        # pct_on_time: fraction of events where delay was within threshold
        (
            spark_sum(
                when(col("delay_seconds") <= ON_TIME_THRESHOLD_SEC, 1).otherwise(0)
            ).cast(DoubleType()) / count("*")
        ).alias("pct_on_time"),
    )
)

print(f"Aggregated buckets: {aggregated.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute the 1-5 reliability score
# MAGIC
# MAGIC The score combines pct_on_time (main signal) with consistency (stddev).
# MAGIC Formula is intentionally simple and explainable — no black-box weighting.
# MAGIC
# MAGIC **Components:**
# MAGIC - `on_time_score` (0–4 points): based on % on time
# MAGIC   - ≥90% → 4, ≥75% → 3, ≥60% → 2, ≥40% → 1, else → 0
# MAGIC - `consistency_bonus` (0–1 point): if delay stddev < 3 min, add 0.5;
# MAGIC   if < 5 min, add 0.25; else 0.
# MAGIC   This rewards predictability — a route that's always exactly 2 min late is
# MAGIC   better than one that's sometimes on time and sometimes 15 min late.
# MAGIC - Total clamped to [1, 5] and rounded to nearest 0.5.
# MAGIC
# MAGIC Buckets below MIN_SAMPLE_SIZE get score NULL ("insufficient data").

# COMMAND ----------

on_time_score = (
    when(col("pct_on_time") >= 0.90, 4)
    .when(col("pct_on_time") >= 0.75, 3)
    .when(col("pct_on_time") >= 0.60, 2)
    .when(col("pct_on_time") >= 0.40, 1)
    .otherwise(0)
)

consistency_bonus = (
    when(col("delay_stddev_minutes") < 3.0, 0.5)
    .when(col("delay_stddev_minutes") < 5.0, 0.25)
    .otherwise(0.0)
)

raw_score = on_time_score + consistency_bonus

# Clamp to [1, 5], round to nearest 0.5
clamped_score = (
    when(raw_score >= 5, 5.0)
    .when(raw_score <= 1, 1.0)
    .otherwise(spark_round(raw_score * 2) / 2)   # round to nearest 0.5
)

gold_df = (
    aggregated
    .withColumn(
        "score_1_to_5",
        when(col("sample_size") >= MIN_SAMPLE_SIZE, clamped_score)
        .otherwise(lit(None).cast(DoubleType())),
    )
    .withColumn("pct_on_time", spark_round(col("pct_on_time") * 100, 1))   # express as percentage
    .withColumn("avg_delay_minutes", spark_round(col("avg_delay_minutes"), 2))
    .withColumn("delay_stddev_minutes", spark_round(col("delay_stddev_minutes"), 2))
    .withColumn("cancellation_rate", lit(0.0))   # not yet derivable from GTFS-R alone
    .withColumn("last_updated", current_timestamp())
    .select(
        "route_id",
        "day_type",
        "hour_bucket",
        "score_1_to_5",
        "pct_on_time",
        "avg_delay_minutes",
        "delay_stddev_minutes",
        "cancellation_rate",
        "sample_size",
        "last_updated",
    )
)

print(f"Gold rows: {gold_df.count()}")
gold_df.printSchema()

# COMMAND ----------

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)
print(f"Written to {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sanity checks — top and bottom routes

# COMMAND ----------

from pyspark.sql.functions import desc, asc

print("=== Top 10 most reliable route/time-bucket combinations ===")
(
    gold_df
    .filter(col("score_1_to_5").isNotNull())
    .orderBy(desc("score_1_to_5"), desc("pct_on_time"))
    .limit(10)
    .show(truncate=False)
)

print("=== Bottom 10 least reliable ===")
(
    gold_df
    .filter(col("score_1_to_5").isNotNull())
    .orderBy(asc("score_1_to_5"), asc("pct_on_time"))
    .limit(10)
    .show(truncate=False)
)

print("=== Score distribution ===")
(
    gold_df
    .filter(col("score_1_to_5").isNotNull())
    .groupBy("score_1_to_5")
    .count()
    .orderBy("score_1_to_5")
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Export Gold to CSV for the Node.js API
# MAGIC
# MAGIC Databricks Community Edition doesn't have a REST SQL endpoint the Express app
# MAGIC can query directly. Instead, we export the Gold table as a single CSV that
# MAGIC the API reads from disk (or a lightweight SQLite DB — see the API layer).
# MAGIC
# MAGIC This is a deliberate pragmatic choice for CE. In a paid tier you'd use
# MAGIC Databricks SQL Warehouse + the REST API instead.

# COMMAND ----------

GOLD_CSV_PATH = "dbfs:/FileStore/dublin_transit/gold_export/route_reliability_scores.csv"

(
    gold_df
    .coalesce(1)   # single file so the API can read it simply
    .write
    .option("header", True)
    .mode("overwrite")
    .csv(GOLD_CSV_PATH)
)
print(f"Exported CSV to {GOLD_CSV_PATH}")
print("Download via: https://community.cloud.databricks.com/files/dublin_transit/gold_export/route_reliability_scores.csv/<part-file>")
