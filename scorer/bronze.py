"""
Bronze: Read raw JSON snapshots → Delta tables.

Reads the JSON files landed by ingest_dag and writes them into Delta
tables — one for TripUpdates, one for VehiclePositions. No cleaning or
transformation happens here. Bronze = raw source data in queryable,
versioned format. If Silver or Gold has a bug, we replay from Bronze.

Adapted from databricks/notebooks/01_bronze_ingest.py.
"""

from __future__ import annotations

import logging
from pyspark.sql.functions import col, explode, input_file_name

from spark_session import get_spark
from config import RAW_BASE, BRONZE_BASE

log = logging.getLogger(__name__)


def run(spark=None) -> None:
    spark = spark or get_spark()

    trip_updates_raw_path      = f"{RAW_BASE}/trip_updates"
    vehicle_positions_raw_path = f"{RAW_BASE}/vehicle_positions"
    bronze_trip_updates_path   = f"{BRONZE_BASE}/trip_updates"
    bronze_vehicle_path        = f"{BRONZE_BASE}/vehicle_positions"

    # ── TripUpdates ──────────────────────────────────────────────────────────
    log.info("Reading TripUpdates raw snapshots...")
    raw_tu = (
        spark.read.json(f"{trip_updates_raw_path}/*.json")
        .withColumn("source_file", input_file_name())
    )

    # Each JSON file has a header.timestamp and an entity[] array.
    # Flatten to one row per entity (one trip update).
    bronze_tu = (
        raw_tu
        .select(
            col("header.timestamp").alias("feed_timestamp"),
            explode(col("entity")).alias("entity"),
            col("source_file"),
        )
        .select(
            col("feed_timestamp"),
            col("entity.id").alias("entity_id"),
            col("entity.trip_update").alias("trip_update"),
            col("source_file"),
        )
    )

    log.info(f"TripUpdate rows: {bronze_tu.count()}")
    (
        bronze_tu.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_trip_updates_path)
    )
    log.info(f"Written to {bronze_trip_updates_path}")

    # ── VehiclePositions ─────────────────────────────────────────────────────
    log.info("Reading VehiclePositions raw snapshots...")
    raw_vp = (
        spark.read.json(f"{vehicle_positions_raw_path}/*.json")
        .withColumn("source_file", input_file_name())
    )

    bronze_vp = (
        raw_vp
        .select(
            col("header.timestamp").alias("feed_timestamp"),
            explode(col("entity")).alias("entity"),
            col("source_file"),
        )
        .select(
            col("feed_timestamp"),
            col("entity.id").alias("entity_id"),
            col("entity.vehicle").alias("vehicle"),
            col("source_file"),
        )
    )

    log.info(f"VehiclePosition rows: {bronze_vp.count()}")
    (
        bronze_vp.write
        .format("delta")
        .mode("overwrite")
        .save(bronze_vehicle_path)
    )
    log.info(f"Written to {bronze_vehicle_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
