"""
Gold: Route reliability scores.

Aggregates Silver stop events into route_reliability_scores —
one row per (route_id, day_type, hour_bucket) combination.

Scoring formula (intentionally simple and explainable):
  on_time_score (0-4):  based on % of trips arriving within 5 min of schedule
  consistency_bonus (0-0.5): rewards low delay variance (predictability matters)
  total = clamp(on_time_score + consistency_bonus, 1, 5), rounded to nearest 0.5

Buckets with fewer than MIN_SAMPLE_SIZE events get score=NULL ("insufficient data")
rather than a misleading number based on too few observations.

Also exports a flat CSV that the Node.js API reads directly — no database needed.

Adapted from databricks/notebooks/03_gold_reliability_score.py.
"""

from __future__ import annotations

import logging

from pyspark.sql.functions import (
    col, avg, stddev, count, sum as spark_sum,
    when, lit, round as spark_round, current_timestamp,
)
from pyspark.sql.types import DoubleType

from spark_session import get_spark
from config import SILVER_PATH, GOLD_PATH, GOLD_CSV_PATH, ON_TIME_THRESHOLD_SEC, MIN_SAMPLE_SIZE

log = logging.getLogger(__name__)


def run(spark=None) -> None:
    spark = spark or get_spark()

    silver = spark.read.format("delta").load(SILVER_PATH)

    # ── Aggregate per (route, day_type, hour_bucket) ──────────────────────────
    aggregated = (
        silver
        .groupBy("route_id", "day_type", "hour_bucket")
        .agg(
            count("*").alias("sample_size"),
            avg(col("delay_seconds") / 60).alias("avg_delay_minutes"),
            stddev(col("delay_seconds") / 60).alias("delay_stddev_minutes"),
            (
                spark_sum(
                    when(col("delay_seconds") <= ON_TIME_THRESHOLD_SEC, 1).otherwise(0)
                ).cast(DoubleType()) / count("*")
            ).alias("pct_on_time"),
        )
    )

    log.info(f"Aggregated route/time buckets: {aggregated.count()}")

    # ── Compute 1-5 score ─────────────────────────────────────────────────────
    on_time_score = (
        when(col("pct_on_time") >= 0.90, 4)
        .when(col("pct_on_time") >= 0.75, 3)
        .when(col("pct_on_time") >= 0.60, 2)
        .when(col("pct_on_time") >= 0.40, 1)
        .otherwise(0)
    )

    # Consistency bonus: a route that's reliably 2 min late is better than one
    # that's unpredictably 0-15 min late. Stddev < 3 min = very consistent.
    consistency_bonus = (
        when(col("delay_stddev_minutes") < 3.0, 0.5)
        .when(col("delay_stddev_minutes") < 5.0, 0.25)
        .otherwise(0.0)
    )

    raw_score = on_time_score + consistency_bonus
    clamped_score = (
        when(raw_score >= 5, 5.0)
        .when(raw_score <= 1, 1.0)
        .otherwise(spark_round(raw_score * 2) / 2)
    )

    gold_df = (
        aggregated
        .withColumn(
            "score_1_to_5",
            when(col("sample_size") >= MIN_SAMPLE_SIZE, clamped_score)
            .otherwise(lit(None).cast(DoubleType())),
        )
        .withColumn("pct_on_time", spark_round(col("pct_on_time") * 100, 1))
        .withColumn("avg_delay_minutes", spark_round(col("avg_delay_minutes"), 2))
        .withColumn("delay_stddev_minutes", spark_round(col("delay_stddev_minutes"), 2))
        # Cancellation rate requires a separate cancelled-trips feed not yet available.
        .withColumn("cancellation_rate", lit(0.0))
        .withColumn("last_updated", current_timestamp())
        .select(
            "route_id", "day_type", "hour_bucket",
            "score_1_to_5", "pct_on_time",
            "avg_delay_minutes", "delay_stddev_minutes",
            "cancellation_rate", "sample_size", "last_updated",
        )
    )

    log.info(f"Gold rows: {gold_df.count()}")

    # ── Write Delta table ─────────────────────────────────────────────────────
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )
    log.info(f"Written Delta to {GOLD_PATH}")

    # ── Export CSV for the Node.js API ────────────────────────────────────────
    # coalesce(1) writes a single part file — fine for this data volume and
    # makes the API read simpler (no glob needed). In a larger system you'd
    # expose a SQL endpoint instead.
    import os
    from pathlib import Path

    Path(GOLD_CSV_PATH).parent.mkdir(parents=True, exist_ok=True)

    # Write via pandas to get a clean single file without Spark's part-file naming.
    gold_pdf = gold_df.toPandas()
    gold_pdf.to_csv(GOLD_CSV_PATH, index=False)

    log.info(f"Exported CSV to {GOLD_CSV_PATH}")
    log.info(f"Rows with confident scores: {gold_pdf[gold_pdf['score_1_to_5'].notna()].shape[0]}")

    # Quick sanity output
    if not gold_pdf.empty:
        scored = gold_pdf[gold_pdf["score_1_to_5"].notna()].sort_values("score_1_to_5", ascending=False)
        log.info("Top 5 most reliable route/time buckets:")
        log.info(scored[["route_id", "day_type", "hour_bucket", "score_1_to_5", "pct_on_time", "sample_size"]].head().to_string())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
