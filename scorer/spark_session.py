"""
Builds a local PySpark + Delta Lake SparkSession.

Delta Lake is configured here rather than in each script so every stage
uses identical Spark settings. The key difference from Databricks is that
we have to register the Delta extension explicitly — on Databricks this
is automatic.
"""

from __future__ import annotations
from pyspark.sql import SparkSession


def get_spark(app_name: str = "dublin-transit-scorer") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Run locally using all available cores.
        .master("local[*]")
        # Wire in the Delta Lake extension.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Limit executor memory to leave headroom for the OS and Airflow on the VM.
        # 8GB gives PySpark enough room for stop_times.txt (large) without OOM.
        .config("spark.driver.memory", "8g")
        # Suppress noisy INFO logs — WARNING and above only.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
