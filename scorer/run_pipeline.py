"""
Runs the full Bronze → Silver → Gold scoring pipeline in sequence.

Called by Airflow's scoring_trigger_dag (nightly), or directly:
    python scorer/run_pipeline.py

Each stage reuses the same SparkSession for efficiency — starting Spark
has a fixed overhead (~10-15s), so sharing it across stages saves time.
"""

from __future__ import annotations

import logging
import sys

import bronze
import silver
import gold
from spark_session import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(module)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def main() -> int:
    log.info("=== Dublin Transit Reliability Scorer — starting pipeline ===")

    spark = get_spark()

    log.info("--- Stage 1/3: Bronze ---")
    bronze.run(spark)

    log.info("--- Stage 2/3: Silver ---")
    silver.run(spark)

    log.info("--- Stage 3/3: Gold ---")
    gold.run(spark)

    spark.stop()
    log.info("=== Pipeline complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
