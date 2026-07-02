"""
Builds a local PySpark + Delta Lake SparkSession.

Delta Lake is configured here rather than in each script so every stage
uses identical Spark settings. The key difference from Databricks is that
we have to register the Delta extension explicitly — on Databricks this
is automatic.
"""

from __future__ import annotations
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark(app_name: str = "dublin-transit-scorer") -> SparkSession:
    # configure_spark_with_delta_pip wires the Delta JARs automatically when
    # delta-spark is installed via pip — this is the recommended approach and
    # avoids the ClassNotFoundException you get when setting catalog config manually.
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
