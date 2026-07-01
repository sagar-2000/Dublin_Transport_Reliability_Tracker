"""
Shared logic for downloading and landing the static GTFS schedule.

The static feed is the national "all operators" combined GTFS zip from
TFI — no API key required, unlike the realtime feeds. It changes
infrequently (NTA updates it roughly weekly as service patterns change),
so this only needs to run on a weekly schedule, not every few minutes.

Silver needs this to know what a trip was *scheduled* to do (stop
sequence, scheduled arrival time) so it can compute "how late was this
trip" by comparing against the realtime TripUpdates landed by ingest_dag.
"""

from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path

import requests

STATIC_GTFS_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_All.zip"

# Only these files are needed downstream (routes/stops/trips/stop_times/
# calendar for schedule joins). shapes.txt and translations.txt are
# dropped — shapes.txt alone is ~280MB and isn't used by the reliability
# scoring logic, so there's no reason to carry it through the pipeline.
FILES_TO_KEEP = {
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
}

DEFAULT_STATIC_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "static"


def get_static_data_dir() -> Path:
    return Path(os.environ.get("GTFS_STATIC_DATA_DIR", DEFAULT_STATIC_DATA_DIR))


def download_and_extract_static_feed(run_timestamp: int | None = None) -> Path:
    """Download the static GTFS zip and extract the needed files to a dated folder."""
    run_timestamp = run_timestamp or int(time.time())

    response = requests.get(STATIC_GTFS_URL, timeout=300)
    response.raise_for_status()

    out_dir = get_static_data_dir() / str(run_timestamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    zip_path = out_dir / "GTFS_All.zip"
    zip_path.write_bytes(response.content)

    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name in FILES_TO_KEEP]
        zf.extractall(out_dir, members=members)

    zip_path.unlink()

    return out_dir
