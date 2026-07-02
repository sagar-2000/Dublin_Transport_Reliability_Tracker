# PySpark + Delta Lake image for the scoring pipeline.
# Separate from the Airflow image to keep each container purpose-built.
FROM python:3.11-slim

# Java is required by PySpark.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY scorer/requirements.txt /app/scorer/requirements.txt
RUN pip install --no-cache-dir -r /app/scorer/requirements.txt

COPY scorer/ /app/scorer/

# Default entrypoint runs the full pipeline.
CMD ["python", "/app/scorer/run_pipeline.py"]
