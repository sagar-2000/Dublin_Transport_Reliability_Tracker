"""
Shared logic for pulling and landing raw GTFS-Realtime snapshots.

Used by ingest_dag.py. Kept separate from scripts/poc_fetch_gtfs.py
deliberately — the POC script is meant to stay a zero-dependency,
standalone sanity check, while this module is the "real" ingestion path
that Airflow tasks call into.

Design choice: land raw feed snapshots as JSON on local disk (mounted
into the Airflow container as a volume) rather than pushing straight to
Databricks from here. This keeps the ingest task simple and decoupled —
if the Databricks side is down or rate-limited, ingestion still succeeds
and nothing is lost. A later step (or the Bronze notebook itself) is
responsible for picking up these JSON snapshots and loading them into
Delta tables.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Literal

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

FEED_URLS = {
    "trip_updates": "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates",
    "vehicle_positions": "https://api.nationaltransport.ie/gtfsr/v2/Vehicles",
}

# Default landing zone. Overridden by GTFS_RAW_DATA_DIR env var so the
# Docker container can point this at a mounted volume.
DEFAULT_RAW_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

FeedType = Literal["trip_updates", "vehicle_positions"]


def get_raw_data_dir() -> Path:
    return Path(os.environ.get("GTFS_RAW_DATA_DIR", DEFAULT_RAW_DATA_DIR))


def fetch_feed(feed_type: FeedType, api_key: str) -> dict:
    """Call the given NTA GTFS-R feed and return it as a plain dict."""
    response = requests.get(
        FEED_URLS[feed_type],
        headers={"x-api-key": api_key},
        timeout=15,
    )
    if response.status_code == 429:
        # Raise a specific message so the Airflow log makes the cause obvious.
        # The DAG's retry_delay (1 min) gives the rate limit time to clear.
        raise requests.exceptions.HTTPError(
            f"429 Rate limited by NTA API — will retry after backoff delay",
            response=response,
        )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    # Convert protobuf -> dict so we can land it as plain JSON. Bronze
    # tables are meant to be a near-raw copy of the source, so we keep
    # this conversion as close to lossless as the protobuf allows.
    return MessageToDict(feed, preserving_proto_field_name=True)


def land_snapshot(feed_type: FeedType, feed_dict: dict, run_timestamp: int | None = None) -> Path:
    """Write a fetched feed to disk as JSON, partitioned by feed type and run time."""
    run_timestamp = run_timestamp or int(time.time())

    out_dir = get_raw_data_dir() / feed_type
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{run_timestamp}.json"
    out_path.write_text(json.dumps(feed_dict))

    return out_path


def fetch_and_land(feed_type: FeedType, api_key: str) -> Path:
    feed_dict = fetch_feed(feed_type, api_key)
    return land_snapshot(feed_type, feed_dict)
