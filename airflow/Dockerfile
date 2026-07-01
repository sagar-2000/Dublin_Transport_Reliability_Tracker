# Extends the official Airflow image with the extra Python packages our
# DAGs need (GTFS-Realtime protobuf parsing). Kept minimal on purpose —
# everything else uses what ships with the base image.
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
