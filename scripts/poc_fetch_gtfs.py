"""
Proof-of-concept: validate access to the NTA GTFS-Realtime TripUpdates feed.

This is a standalone script with no project dependencies (no Airflow, no
Databricks) — its only job is to prove the data source works end-to-end:
read an API key, call the feed, parse the protobuf, print something human
readable. Everything else in this repo builds on top of this once it works.

Usage:
    export NTA_API_KEY=your_key_here   # or put it in a .env file
    python scripts/poc_fetch_gtfs.py
"""

import os
import sys

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
MAX_UPDATES_TO_PRINT = 10


def fetch_trip_updates(api_key: str) -> gtfs_realtime_pb2.FeedMessage:
    """Call the NTA GTFS-R TripUpdates endpoint and parse the protobuf response."""
    response = requests.get(
        TRIP_UPDATES_URL,
        headers={"x-api-key": api_key},
        timeout=15,
    )
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def summarize_trip_update(entity) -> str:
    """Build a one-line human-readable summary for a single TripUpdate entity."""
    trip_update = entity.trip_update
    route_id = trip_update.trip.route_id or "UNKNOWN_ROUTE"
    trip_id = trip_update.trip.trip_id or "UNKNOWN_TRIP"

    if not trip_update.stop_time_update:
        return f"route={route_id} trip={trip_id}: no stop-level updates"

    # Just look at the next stop update for this trip, since that's the
    # most relevant one for "is this trip currently delayed".
    next_stop = trip_update.stop_time_update[0]
    delay_seconds = None
    if next_stop.HasField("arrival") and next_stop.arrival.HasField("delay"):
        delay_seconds = next_stop.arrival.delay
    elif next_stop.HasField("departure") and next_stop.departure.HasField("delay"):
        delay_seconds = next_stop.departure.delay

    if delay_seconds is None:
        delay_str = "n/a"
    else:
        sign = "-" if delay_seconds < 0 else ""
        abs_delay = abs(delay_seconds)
        delay_str = f"{sign}{abs_delay // 60}m{abs_delay % 60}s"
    stop_id = next_stop.stop_id or "UNKNOWN_STOP"

    return f"route={route_id} trip={trip_id} next_stop={stop_id} delay={delay_str}"


def main() -> int:
    load_dotenv()
    api_key = os.environ.get("NTA_API_KEY")

    if not api_key:
        print("ERROR: NTA_API_KEY is not set. Add it to a .env file or export it.", file=sys.stderr)
        print("Sign up for a free key at https://developer.nationaltransport.ie", file=sys.stderr)
        return 1

    try:
        feed = fetch_trip_updates(api_key)
    except requests.exceptions.HTTPError as exc:
        print(f"ERROR: API request failed: {exc}", file=sys.stderr)
        return 1
    except requests.exceptions.RequestException as exc:
        print(f"ERROR: Could not reach the NTA API: {exc}", file=sys.stderr)
        return 1

    trip_update_entities = [e for e in feed.entity if e.HasField("trip_update")]

    print(f"Feed timestamp: {feed.header.timestamp}")
    print(f"Total trip update entities in feed: {len(trip_update_entities)}")
    print(f"Showing first {min(MAX_UPDATES_TO_PRINT, len(trip_update_entities))}:\n")

    for entity in trip_update_entities[:MAX_UPDATES_TO_PRINT]:
        print(summarize_trip_update(entity))

    return 0


if __name__ == "__main__":
    sys.exit(main())
