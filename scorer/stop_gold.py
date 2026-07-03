"""
Gold: Stop-level average delay.

Same shape and scoring formula as gold.py, but aggregated per
(stop_id, day_type, hour_bucket) instead of per route — this is what
powers "how late does this stop usually run" in the stop-schedule feature.

Joined against the static feed's stops.txt purely for stop_name/lat/lon so
the API can serve search results without a second lookup file.

Adapted from gold.py — kept as a separate script rather than folded into
gold.py's run() so either can be re-run independently.
"""

from __future__ import annotations

import logging

from pyspark.sql.functions import (
    col, avg, stddev, count, sum as spark_sum,
    when, lit, round as spark_round, current_timestamp,
)
from pyspark.sql.types import DoubleType

from spark_session import get_spark
from silver import latest_static_dir
from config import (
    SILVER_PATH, STOP_GOLD_PATH, STOP_GOLD_CSV_PATH,
    ON_TIME_THRESHOLD_SEC, MIN_SAMPLE_SIZE,
)

log = logging.getLogger(__name__)


def run(spark=None) -> None:
    spark = spark or get_spark()

    silver = spark.read.format("delta").load(SILVER_PATH)

    # ── Aggregate per (stop, day_type, hour_bucket) ───────────────────────────
    aggregated = (
        silver
        .groupBy("stop_id", "day_type", "hour_bucket")
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

    log.info(f"Aggregated stop/time buckets: {aggregated.count()}")

    # ── Same 1-5 scoring formula as gold.py, for consistency ──────────────────
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
    clamped_score = (
        when(raw_score >= 5, 5.0)
        .when(raw_score <= 1, 1.0)
        .otherwise(spark_round(raw_score * 2) / 2)
    )

    scored = (
        aggregated
        .withColumn(
            "score_1_to_5",
            when(col("sample_size") >= MIN_SAMPLE_SIZE, clamped_score)
            .otherwise(lit(None).cast(DoubleType())),
        )
        .withColumn("pct_on_time", spark_round(col("pct_on_time") * 100, 1))
        .withColumn("avg_delay_minutes", spark_round(col("avg_delay_minutes"), 2))
        .withColumn("delay_stddev_minutes", spark_round(col("delay_stddev_minutes"), 2))
        .withColumn("last_updated", current_timestamp())
    )

    # ── Join static stops.txt for name/lat/lon ────────────────────────────────
    static_dir = latest_static_dir()
    log.info(f"Using static GTFS snapshot: {static_dir}")

    stops = (
        spark.read
        .option("header", True)
        .csv(f"{static_dir}/stops.txt")
        .select(
            col("stop_id"),
            col("stop_name"),
            col("stop_lat").cast(DoubleType()),
            col("stop_lon").cast(DoubleType()),
        )
    )

    gold_df = (
        scored
        .join(stops, on="stop_id", how="left")
        .select(
            "stop_id", "stop_name", "stop_lat", "stop_lon",
            "day_type", "hour_bucket",
            "score_1_to_5", "pct_on_time",
            "avg_delay_minutes", "delay_stddev_minutes",
            "sample_size", "last_updated",
        )
    )

    log.info(f"Gold stop rows: {gold_df.count()}")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(STOP_GOLD_PATH)
    )
    log.info(f"Written Delta to {STOP_GOLD_PATH}")

    from pathlib import Path
    Path(STOP_GOLD_CSV_PATH).parent.mkdir(parents=True, exist_ok=True)

    gold_pdf = gold_df.toPandas()
    gold_pdf.to_csv(STOP_GOLD_CSV_PATH, index=False)

    log.info(f"Exported CSV to {STOP_GOLD_CSV_PATH}")
    log.info(f"Rows with confident scores: {gold_pdf[gold_pdf['score_1_to_5'].notna()].shape[0]}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
