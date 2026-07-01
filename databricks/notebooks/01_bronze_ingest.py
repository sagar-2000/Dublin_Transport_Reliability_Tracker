# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Raw Snapshot Ingest
# MAGIC
# MAGIC Reads the raw JSON snapshots landed by Airflow's `ingest_dag` from DBFS
# MAGIC and writes them into Delta tables — one for TripUpdates, one for VehiclePositions.
# MAGIC
# MAGIC **This notebook does not clean or transform anything.** Its job is purely to
# MAGIC get the raw source data into Delta format so it's queryable, versioned, and
# MAGIC re-processable. If we ever find a bug in Silver or Gold, we replay from here.
# MAGIC
# MAGIC **Run cadence:** triggered by Airflow's `scoring_trigger_dag` after each ingest
# MAGIC batch, or manually from the Databricks UI for backfills.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — adjust these paths if you change the DBFS landing location

# COMMAND ----------

# Where Airflow lands the raw JSON files (must match GTFS_RAW_DATA_DIR in docker-compose).
# On DBFS these live under /FileStore/ because CE doesn't have a mounted external store.
RAW_BASE_PATH = "dbfs:/FileStore/dublin_transit/raw"
BRONZE_BASE_PATH = "dbfs:/FileStore/dublin_transit/bronze"

TRIP_UPDATES_RAW_PATH = f"{RAW_BASE_PATH}/trip_updates"
VEHICLE_POSITIONS_RAW_PATH = f"{RAW_BASE_PATH}/vehicle_positions"

BRONZE_TRIP_UPDATES_PATH = f"{BRONZE_BASE_PATH}/trip_updates"
BRONZE_VEHICLE_POSITIONS_PATH = f"{BRONZE_BASE_PATH}/vehicle_positions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: how many new snapshots are there?

# COMMAND ----------

import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, input_file_name, lit, from_unixtime, to_timestamp

def count_files(path: str) -> int:
    try:
        return len(dbutils.fs.ls(path))
    except Exception:
        return 0

print(f"Trip update snapshots available:   {count_files(TRIP_UPDATES_RAW_PATH)}")
print(f"Vehicle position snapshots available: {count_files(VEHICLE_POSITIONS_RAW_PATH)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest TripUpdates → Bronze Delta table
# MAGIC
# MAGIC Each raw snapshot file is a JSON object with:
# MAGIC - `header.timestamp` — feed generation time (Unix seconds)
# MAGIC - `entity[]` — array of trip update entities
# MAGIC
# MAGIC We flatten `entity[]` one level so each row = one entity (one trip update),
# MAGIC and we attach the source file name so we can trace any row back to its origin.
# MAGIC
# MAGIC **Spark concept:** `spark.read.json()` infers the schema from the JSON and
# MAGIC returns a DataFrame — a distributed table. `.write.format("delta").save(path)`
# MAGIC persists it as a Delta table (Parquet + transaction log) on DBFS.

# COMMAND ----------

def ingest_trip_updates() -> DataFrame:
    raw_df = (
        spark.read.json(TRIP_UPDATES_RAW_PATH + "/*.json")
        .withColumn("source_file", input_file_name())
    )

    # header.timestamp is the feed-level timestamp (when NTA generated this snapshot).
    # We pull it out as a top-level column so Silver can filter/partition by snapshot time.
    entities_df = (
        raw_df
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
    return entities_df

trip_updates_bronze = ingest_trip_updates()
print(f"Trip update rows: {trip_updates_bronze.count()}")
trip_updates_bronze.printSchema()

# COMMAND ----------

# Write to Bronze Delta table. mode="overwrite" for simplicity on CE —
# in a production setup you'd use merge/upsert to avoid reprocessing.
# For this portfolio project, overwrite keeps the logic transparent.
(
    trip_updates_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_TRIP_UPDATES_PATH)
)
print(f"Written to {BRONZE_TRIP_UPDATES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest VehiclePositions → Bronze Delta table

# COMMAND ----------

def ingest_vehicle_positions() -> DataFrame:
    raw_df = (
        spark.read.json(VEHICLE_POSITIONS_RAW_PATH + "/*.json")
        .withColumn("source_file", input_file_name())
    )

    entities_df = (
        raw_df
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
    return entities_df

vehicle_positions_bronze = ingest_vehicle_positions()
print(f"Vehicle position rows: {vehicle_positions_bronze.count()}")
vehicle_positions_bronze.printSchema()

# COMMAND ----------

(
    vehicle_positions_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_VEHICLE_POSITIONS_PATH)
)
print(f"Written to {BRONZE_VEHICLE_POSITIONS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quick sanity checks

# COMMAND ----------

print("=== Sample TripUpdate entity ===")
spark.read.format("delta").load(BRONZE_TRIP_UPDATES_PATH).limit(3).show(truncate=False)

print("=== Sample VehiclePosition entity ===")
spark.read.format("delta").load(BRONZE_VEHICLE_POSITIONS_PATH).limit(3).show(truncate=False)
