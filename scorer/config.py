"""
Shared path config for the local PySpark scoring pipeline.

All paths are local filesystem paths, set via environment variables so
the same scripts run identically on a dev laptop or the Oracle Cloud VM.
The defaults assume you're running from the repo root.
"""

from __future__ import annotations
import os
from pathlib import Path

_BASE = Path(os.environ.get("SCORING_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))

RAW_BASE       = str(_BASE / "raw")
STATIC_BASE    = str(_BASE / "static")
BRONZE_BASE    = str(_BASE / "bronze")
SILVER_PATH    = str(_BASE / "silver" / "stop_events")
GOLD_PATH      = str(_BASE / "gold" / "route_reliability_scores")
GOLD_CSV_PATH  = str(_BASE / "gold" / "route_reliability_scores.csv")
STOP_GOLD_PATH     = str(_BASE / "gold" / "stop_reliability_scores")
STOP_GOLD_CSV_PATH = str(_BASE / "gold" / "stop_reliability_scores.csv")

# A trip is "on time" if it arrives no more than this many seconds late.
ON_TIME_THRESHOLD_SEC = 300   # 5 minutes — standard transit industry threshold

# Minimum observed stop events before we'll report a confident score.
MIN_SAMPLE_SIZE = 30
