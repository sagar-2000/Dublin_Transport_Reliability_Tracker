"""
scoring_trigger_dag: runs the PySpark Bronze→Silver→Gold pipeline nightly.

Triggers run_pipeline.py in a Docker container (using the scorer image)
after ingest_dag has had a full day to accumulate data. Runs at 2am UTC
to avoid overlap with peak ingestion and to score the previous day's data.

Why a separate Docker container rather than running PySpark inside the
Airflow container? PySpark + Delta Lake requires a specific JVM and package
set that would bloat the Airflow image. Keeping them separate means each
image stays lean and purpose-built.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "dublin-transit-tracker",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Path to the repo on the VM host — must match where the repo is cloned.
REPO_ROOT = "/home/ubuntu/Dublin_Transport_Reliability_Tracker"


@dag(
    dag_id="scoring_trigger_dag",
    description="Run PySpark Bronze→Silver→Gold reliability scoring pipeline",
    schedule="0 2 * * *",   # 2am UTC nightly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["scoring", "pyspark"],
)
def scoring_trigger_dag():
    # Run the scorer container. Docker-in-Docker works here because the
    # Airflow container has access to the host Docker socket (not mounted
    # by default — see note below if this task fails with "docker not found").
    run_scorer = BashOperator(
        task_id="run_pyspark_pipeline",
        bash_command=f"""
            docker run --rm \
                -v {REPO_ROOT}/data:/app/data \
                -e SCORING_DATA_DIR=/app/data \
                dublin-transit-scorer \
                python /app/scorer/run_pipeline.py
        """,
    )

    run_scorer


scoring_trigger_dag()
