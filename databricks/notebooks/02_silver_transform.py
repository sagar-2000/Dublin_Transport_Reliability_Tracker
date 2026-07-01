# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Clean, Join & Compute Delays
# MAGIC
# MAGIC Takes the raw Bronze trip update entities and produces one clean row per
# MAGIC observed stop event:
# MAGIC
# MAGIC ```
# MAGIC (route_id, trip_id, stop_id, stop_sequence,
# MAGIC  scheduled_arrival_ts, observed_arrival_ts, delay_seconds,
# MAGIC  feed_timestamp, day_type, hour_bucket)
# MAGIC ```
# MAGIC
# MAGIC This is the join layer — we combine the realtime TripUpdates (which give us
# MAGIC actual delay in seconds per stop) with the static GTFS schedule (which gives
# MAGIC us the scheduled time so we can reconstruct the absolute observed arrival time).
# MAGIC
# MAGIC **Why not just use the delay field from TripUpdates directly?**
# MAGIC TripUpdates report delay as an offset in seconds from the scheduled time.
# MAGIC To get an absolute observed arrival time (needed for bucketing by hour), we
# MAGIC need: `observed = scheduled + delay`. The scheduled time lives in `stop_times.txt`
# MAGIC from the static feed, not in the realtime feed. Hence the join.

# COMMAND ----------

BRONZE_BASE_PATH = "dbfs:/FileStore/dublin_transit/bronze"
STATIC_BASE_PATH = "dbfs:/FileStore/dublin_transit/static"
SILVER_PATH = "dbfs:/FileStore/dublin_transit/silver/stop_events"

BRONZE_TRIP_UPDATES_PATH = f"{BRONZE_BASE_PATH}/trip_updates"
STATIC_TRIPS_PATH = f"{STATIC_BASE_PATH}/trips.txt"
STATIC_STOP_TIMES_PATH = f"{STATIC_BASE_PATH}/stop_times.txt"
STATIC_ROUTES_PATH = f"{STATIC_BASE_PATH}/routes.txt"
STATIC_CALENDAR_PATH = f"{STATIC_BASE_PATH}/calendar.txt"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze TripUpdates and explode stop_time_updates
# MAGIC
# MAGIC Each Bronze row has a `trip_update` struct that contains an array of
# MAGIC `stop_time_update` entries — one per upcoming stop. We explode these so
# MAGIC each row = one (trip, stop) observation.

# COMMAND ----------

from pyspark.sql.functions import (
    col, explode, to_timestamp, from_unixtime, hour,
    when, lit, coalesce, expr, dayofweek, date_format
)
from pyspark.sql.types import IntegerType, LongType

bronze_tu = spark.read.format("delta").load(BRONZE_TRIP_UPDATES_PATH)

# Explode stop_time_update array: one row per (trip, stop) observation
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
        # Prefer arrival delay; fall back to departure delay if arrival absent.
        coalesce(
            col("stu.arrival.delay"),
            col("stu.departure.delay"),
        ).cast(IntegerType()).alias("delay_seconds"),
        # Absolute observed timestamp (seconds since epoch), if provided.
        coalesce(
            col("stu.arrival.time"),
            col("stu.departure.time"),
        ).cast(LongType()).alias("observed_time_unix"),
    )
    .filter(col("route_id").isNotNull())
    .filter(col("trip_id").isNotNull())
)

print(f"Raw stop events: {stop_events_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load static schedule reference tables
# MAGIC
# MAGIC We need:
# MAGIC - `stop_times.txt` → scheduled arrival time per (trip_id, stop_sequence)
# MAGIC - `trips.txt` → service_id per trip_id (needed to look up day type)
# MAGIC - `calendar.txt` → which days of week a service_id runs

# COMMAND ----------

# Load the most recent static snapshot (latest timestamp = most recent weekly download)
def latest_static_path(filename: str) -> str:
    """Return the path to the most recent static GTFS file."""
    dirs = sorted(
        [f.path for f in dbutils.fs.ls(STATIC_BASE_PATH)],
        reverse=True,
    )
    if not dirs:
        raise FileNotFoundError(f"No static snapshots found under {STATIC_BASE_PATH}")
    return dirs[0] + filename


stop_times = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(latest_static_path("stop_times.txt"))
    .select(
        col("trip_id"),
        col("stop_sequence").cast(IntegerType()),
        col("stop_id"),
        col("arrival_time").alias("scheduled_arrival_str"),
    )
)

trips = (
    spark.read
    .option("header", True)
    .csv(latest_static_path("trips.txt"))
    .select("trip_id", "service_id", "route_id")
)

calendar = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(latest_static_path("calendar.txt"))
    .select("service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
)

print(f"stop_times rows: {stop_times.count()}")
print(f"trips rows:      {trips.count()}")
print(f"calendar rows:   {calendar.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Join realtime events against static schedule

# COMMAND ----------

# Join stop events → stop_times to get scheduled arrival string
joined = (
    stop_events_raw
    .join(
        stop_times.select("trip_id", "stop_sequence", "scheduled_arrival_str"),
        on=["trip_id", "stop_sequence"],
        how="left",
    )
    .join(
        trips.select("trip_id", "service_id"),
        on="trip_id",
        how="left",
    )
    .join(
        calendar,
        on="service_id",
        how="left",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse scheduled arrival time and compute observed arrival
# MAGIC
# MAGIC GTFS stores scheduled times as "HH:MM:SS" strings — but times can exceed 24:00
# MAGIC for trips running past midnight (e.g. "25:30:00" = 1:30am next day). Standard
# MAGIC timestamp parsing won't handle this, so we convert to seconds-since-midnight
# MAGIC manually, then add to the service date to get an absolute epoch timestamp.

# COMMAND ----------

from pyspark.sql.functions import (
    split, substring, to_date, unix_timestamp, udf
)
from pyspark.sql.types import LongType as LongT

@udf(returnType=LongT())
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

# service_date in GTFS is YYYYMMDD string
enriched = (
    joined
    .withColumn("service_date_ts", to_date(col("service_date"), "yyyyMMdd"))
    .withColumn("service_date_epoch", unix_timestamp(col("service_date_ts")))
    .withColumn("scheduled_seconds_since_midnight", gtfs_time_to_seconds(col("scheduled_arrival_str")))
    .withColumn(
        "scheduled_arrival_unix",
        (col("service_date_epoch") + col("scheduled_seconds_since_midnight")).cast(LongType()),
    )
    # Observed arrival: use explicit timestamp from feed if present, else derive from delay
    .withColumn(
        "observed_arrival_unix",
        coalesce(
            col("observed_time_unix"),
            (col("scheduled_arrival_unix") + col("delay_seconds")).cast(LongType()),
        ),
    )
    .withColumn("observed_arrival_ts", from_unixtime(col("observed_arrival_unix")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Classify day_type and hour_bucket
# MAGIC
# MAGIC These are the two dimensions the Gold reliability scoring groups by.
# MAGIC day_type comes from the static calendar (what days does this service run?).
# MAGIC hour_bucket comes from the observed arrival hour.

# COMMAND ----------

from pyspark.sql.functions import hour as spark_hour

day_type_expr = (
    when(col("saturday") == 1, "saturday")
    .when(col("sunday") == 1, "sunday")
    .otherwise("weekday")
)

observed_hour = spark_hour(col("observed_arrival_ts"))

hour_bucket_expr = (
    when((observed_hour >= 7) & (observed_hour < 10), "am_peak")
    .when((observed_hour >= 10) & (observed_hour < 16), "midday")
    .when((observed_hour >= 16) & (observed_hour < 19), "pm_peak")
    .when((observed_hour >= 19) & (observed_hour < 23), "evening")
    .otherwise("night")
)

silver_df = (
    enriched
    .withColumn("day_type", day_type_expr)
    .withColumn("hour_bucket", hour_bucket_expr)
    .select(
        "route_id",
        "trip_id",
        "stop_id",
        "stop_sequence",
        "service_date",
        "scheduled_arrival_unix",
        "observed_arrival_unix",
        "delay_seconds",
        "day_type",
        "hour_bucket",
        "feed_timestamp",
    )
    .filter(col("delay_seconds").isNotNull())
    # Filter out extreme outliers — delays over 3 hours are almost certainly
    # cancelled or data-quality issues, not real delays. Flag for review later.
    .filter(col("delay_seconds") < 10800)
    .filter(col("delay_seconds") > -3600)
)

print(f"Silver stop events: {silver_df.count()}")
silver_df.printSchema()

# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("service_date")   # Partition by date so Gold aggregations scan less data
    .save(SILVER_PATH)
)
print(f"Written to {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quick sanity check — delay distribution

# COMMAND ----------

from pyspark.sql.functions import percentile_approx, avg, stddev, count

print("=== Delay distribution (seconds) ===")
silver_df.select(
    count("*").alias("n"),
    avg("delay_seconds").alias("mean_delay"),
    stddev("delay_seconds").alias("stddev_delay"),
    percentile_approx("delay_seconds", 0.5).alias("p50"),
    percentile_approx("delay_seconds", 0.9).alias("p90"),
    percentile_approx("delay_seconds", 0.95).alias("p95"),
).show()

print("=== Row count by day_type + hour_bucket ===")
(
    silver_df
    .groupBy("day_type", "hour_bucket")
    .count()
    .orderBy("day_type", "hour_bucket")
    .show()
)
