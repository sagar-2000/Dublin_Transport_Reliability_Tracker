"""
Silver: Clean, join & compute delays per stop event.

Reads Bronze TripUpdates and joins against the static GTFS schedule to
produce one clean row per observed (trip, stop) event:

    route_id, trip_id, stop_id, stop_sequence,
    scheduled_arrival_unix, observed_arrival_unix, delay_seconds,
    day_type, hour_bucket, feed_timestamp

Why join against the static schedule?
TripUpdates report delay as an offset in seconds from the scheduled time.
To bucket events by hour of day (needed for scoring), we need the absolute
observed arrival time = scheduled_time + delay. The scheduled time lives
in stop_times.txt from the static feed, so we have to join to get it.

Adapted from databricks/notebooks/02_silver_transform.py.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql.functions import (
    col, explode, coalesce, from_unixtime, when,
    unix_timestamp, to_date, hour as spark_hour, udf,
)
from pyspark.sql.types import IntegerType, LongType

from spark_session import get_spark
from config import BRONZE_BASE, STATIC_BASE, SILVER_PATH

log = logging.getLogger(__name__)


@udf(returnType=LongType())
def gtfs_time_to_seconds(time_str):
    """Convert GTFS HH:MM:SS (may be >24h) to seconds since midnight."""
    if time_str is None:
        return None
    parts = time_str.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return None


def latest_static_dir() -> str:
    """Return the most recently downloaded static GTFS snapshot directory."""
    dirs = sorted(Path(STATIC_BASE).iterdir(), reverse=True)
    if not dirs:
        raise FileNotFoundError(f"No static snapshots found under {STATIC_BASE}")
    return str(dirs[0])


def run(spark=None) -> None:
    spark = spark or get_spark()

    bronze_trip_updates_path = f"{BRONZE_BASE}/trip_updates"
    static_dir = latest_static_dir()
    log.info(f"Using static GTFS snapshot: {static_dir}")

    # ── Load Bronze ───────────────────────────────────────────────────────────
    bronze_tu = spark.read.format("delta").load(bronze_trip_updates_path)

    # Explode stop_time_update array: one row per (trip, stop) observation.
    stop_events_raw = (
        bronze_tu
        .select(
            col("feed_timestamp").cast(LongType()),
            col("trip_update.trip.route_id").alias("route_id"),
            col("trip_update.trip.trip_id").alias("trip_id"),
            col("trip_update.trip.start_date").alias("service_date"),
            explode(col("trip_update.stop_time_update")).alias("stu"),
        )
        .select(
            col("feed_timestamp"),
            col("route_id"),
            col("trip_id"),
            col("service_date"),
            col("stu.stop_id").alias("stop_id"),
            col("stu.stop_sequence").alias("stop_sequence"),
            # Prefer arrival delay; fall back to departure delay if absent.
            coalesce(
                col("stu.arrival.delay"),
                col("stu.departure.delay"),
            ).cast(IntegerType()).alias("delay_seconds"),
            coalesce(
                col("stu.arrival.time"),
                col("stu.departure.time"),
            ).cast(LongType()).alias("observed_time_unix"),
        )
        .filter(col("route_id").isNotNull())
        .filter(col("trip_id").isNotNull())
    )

    log.info(f"Raw stop events: {stop_events_raw.count()}")

    # ── Load static reference tables ─────────────────────────────────────────
    # stop_times.txt is large (~330MB) — Spark reads it in parallel which is
    # much faster than pandas would be for this size.
    stop_times = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{static_dir}/stop_times.txt")
        .select(
            col("trip_id"),
            col("stop_sequence").cast(IntegerType()),
            col("arrival_time").alias("scheduled_arrival_str"),
        )
    )

    trips = (
        spark.read
        .option("header", True)
        .csv(f"{static_dir}/trips.txt")
        .select("trip_id", "service_id")
    )

    calendar = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{static_dir}/calendar.txt")
        .select("service_id", "saturday", "sunday")
    )

    log.info(f"stop_times rows: {stop_times.count()}")

    # ── Join realtime events against static schedule ──────────────────────────
    joined = (
        stop_events_raw
        .join(stop_times.select("trip_id", "stop_sequence", "scheduled_arrival_str"),
              on=["trip_id", "stop_sequence"], how="left")
        .join(trips.select("trip_id", "service_id"), on="trip_id", how="left")
        .join(calendar, on="service_id", how="left")
    )

    # ── Compute observed arrival timestamp ────────────────────────────────────
    # GTFS service_date is YYYYMMDD, scheduled time is HH:MM:SS (can exceed 24h)
    enriched = (
        joined
        .withColumn("service_date_epoch",
                    unix_timestamp(to_date(col("service_date"), "yyyyMMdd")))
        .withColumn("scheduled_secs",
                    gtfs_time_to_seconds(col("scheduled_arrival_str")))
        .withColumn("scheduled_arrival_unix",
                    (col("service_date_epoch") + col("scheduled_secs")).cast(LongType()))
        .withColumn("observed_arrival_unix",
                    coalesce(
                        col("observed_time_unix"),
                        (col("scheduled_arrival_unix") + col("delay_seconds")).cast(LongType()),
                    ))
        .withColumn("observed_arrival_ts", from_unixtime(col("observed_arrival_unix")))
    )

    # ── Classify day_type and hour_bucket ─────────────────────────────────────
    observed_hour = spark_hour(col("observed_arrival_ts"))

    silver_df = (
        enriched
        .withColumn("day_type",
                    when(col("saturday") == 1, "saturday")
                    .when(col("sunday") == 1, "sunday")
                    .otherwise("weekday"))
        .withColumn("hour_bucket",
                    when((observed_hour >= 7) & (observed_hour < 10), "am_peak")
                    .when((observed_hour >= 10) & (observed_hour < 16), "midday")
                    .when((observed_hour >= 16) & (observed_hour < 19), "pm_peak")
                    .when((observed_hour >= 19) & (observed_hour < 23), "evening")
                    .otherwise("night"))
        .select(
            "route_id", "trip_id", "stop_id", "stop_sequence",
            "service_date", "scheduled_arrival_unix", "observed_arrival_unix",
            "delay_seconds", "day_type", "hour_bucket", "feed_timestamp",
        )
        .filter(col("delay_seconds").isNotNull())
        # Filter extreme outliers — >3h late or >1h early are almost certainly
        # cancelled trips or data-quality issues, not real delays.
        .filter(col("delay_seconds") < 10800)
        .filter(col("delay_seconds") > -3600)
    )

    log.info(f"Silver stop events: {silver_df.count()}")

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("service_date")
        .save(SILVER_PATH)
    )
    log.info(f"Written to {SILVER_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
