"""
Uploads local Airflow-landed data to DBFS so Databricks notebooks can read it.

Databricks Community Edition doesn't support mounting external storage (S3/GCS),
so we push files up via the DBFS REST API. This script is meant to be run manually
(or by Airflow's scoring_trigger_dag before kicking off the notebooks).

Usage:
    export DATABRICKS_HOST=https://community.cloud.databricks.com
    export DATABRICKS_TOKEN=your_personal_access_token
    python scripts/upload_to_dbfs.py
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Chunk size for DBFS block API — max is 1MB per block per API docs.
CHUNK_SIZE = 1024 * 1024  # 1MB


def get_config() -> tuple[str, str]:
    load_dotenv()
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        print("ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN in your .env file.", file=sys.stderr)
        print("Get a token: Databricks UI → top-right menu → User Settings → Access tokens", file=sys.stderr)
        sys.exit(1)
    return host, token


def dbfs_upload(local_path: Path, dbfs_path: str, host: str, token: str) -> None:
    """Upload a single file to DBFS using the chunked block API."""
    headers = {"Authorization": f"Bearer {token}"}

    # Open a write handle
    r = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": True},
        timeout=30,
    )
    r.raise_for_status()
    handle = r.json()["handle"]

    # Upload in chunks
    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            requests.post(
                f"{host}/api/2.0/dbfs/add-block",
                headers=headers,
                json={"handle": handle, "data": base64.b64encode(chunk).decode()},
                timeout=60,
            ).raise_for_status()

    # Close the handle to finalise the file
    requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=headers,
        json={"handle": handle},
        timeout=30,
    ).raise_for_status()

    print(f"  uploaded → {dbfs_path}")


def upload_directory(local_dir: Path, dbfs_dir: str, host: str, token: str) -> int:
    """Recursively upload all files in local_dir to dbfs_dir."""
    if not local_dir.exists():
        print(f"  [skip] {local_dir} does not exist yet")
        return 0
    count = 0
    for local_file in sorted(local_dir.rglob("*")):
        if local_file.is_file():
            relative = local_file.relative_to(local_dir)
            dbfs_path = f"{dbfs_dir}/{relative}".replace("\\", "/")
            dbfs_upload(local_file, dbfs_path, host, token)
            count += 1
    return count


def main() -> None:
    host, token = get_config()

    repo_root = Path(__file__).resolve().parents[1]
    data_root = repo_root / "data"

    print("Uploading raw snapshots (trip_updates + vehicle_positions)...")
    n = upload_directory(
        data_root / "raw",
        "dbfs:/FileStore/dublin_transit/raw",
        host, token,
    )
    print(f"  {n} files uploaded.\n")

    print("Uploading static schedule (latest snapshot only)...")
    static_dir = data_root / "static"
    if static_dir.exists():
        snapshots = sorted(static_dir.iterdir(), reverse=True)
        if snapshots:
            latest = snapshots[0]
            n = upload_directory(
                latest,
                "dbfs:/FileStore/dublin_transit/static",
                host, token,
            )
            print(f"  {n} files uploaded from {latest.name}.\n")
        else:
            print("  [skip] no static snapshots found.\n")
    else:
        print(f"  [skip] {static_dir} does not exist.\n")

    print("Done. You can now run the Databricks notebooks in order:")
    print("  1. 01_bronze_ingest.py")
    print("  2. 02_silver_transform.py")
    print("  3. 03_gold_reliability_score.py")


if __name__ == "__main__":
    main()
