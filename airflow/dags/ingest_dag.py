"""
ingest_dag: polls the NTA GTFS-Realtime feeds and lands raw snapshots.

Runs every 3 minutes (within the 2-5 min fair-use window). Pulls both
TripUpdates and VehiclePositions on each run and writes each as a JSON
file under data/raw/<feed_type>/<unix_timestamp>.json.

This DAG intentionally does no parsing/cleaning/joining — that's Silver's
job, done in Databricks. Ingestion's only responsibility is "get the data
out of the API and onto disk reliably, on schedule."
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from gtfs_ingest_lib import fetch_and_land

DEFAULT_ARGS = {
    "owner": "dublin-transit-tracker",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="ingest_dag",
    description="Poll NTA GTFS-Realtime feeds and land raw snapshots for Bronze ingestion",
    schedule=timedelta(minutes=3),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "gtfs"],
)
def ingest_dag():
    @task
    def fetch_trip_updates():
        api_key = os.environ.get("NTA_API_KEY")
        if not api_key:
            raise AirflowSkipException("NTA_API_KEY is not set — skipping this run.")
        path = fetch_and_land("trip_updates", api_key)
        return str(path)

    @task
    def fetch_vehicle_positions():
        api_key = os.environ.get("NTA_API_KEY")
        if not api_key:
            raise AirflowSkipException("NTA_API_KEY is not set — skipping this run.")
        path = fetch_and_land("vehicle_positions", api_key)
        return str(path)

    # Independent feeds, no ordering dependency — run them in parallel.
    fetch_trip_updates()
    fetch_vehicle_positions()


ingest_dag()
