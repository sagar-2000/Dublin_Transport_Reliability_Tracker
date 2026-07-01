"""
static_schedule_dag: refreshes the static GTFS schedule weekly.

Downloads TFI's combined GTFS zip, extracts the files Silver needs
(routes/stops/trips/stop_times/calendar/calendar_dates/agency/feed_info),
and lands them under data/static/<unix_timestamp>/. Runs weekly since the
static schedule changes far less often than realtime positions/delays.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from static_schedule_lib import download_and_extract_static_feed

DEFAULT_ARGS = {
    "owner": "dublin-transit-tracker",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="static_schedule_dag",
    description="Download and land the weekly static GTFS schedule feed",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "gtfs", "static"],
)
def static_schedule_dag():
    @task
    def fetch_static_schedule():
        out_dir = download_and_extract_static_feed()
        return str(out_dir)

    fetch_static_schedule()


static_schedule_dag()
