FROM python:3.11-slim

# Install sqlite3 CLI (optional, but useful for debugging)
RUN apt-get update && apt-get install -y sqlite3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY pipeline_ingest.py /app/pipeline_ingest.py
COPY analytics_queries.sql /app/analytics_queries.sql

# Default env vars (can be overridden in docker-compose)
ENV EVENTS_DIR=/data/events
ENV DB_PATH=/state/analytics.sqlite
ENV CHECKPOINT_PATH=/state/.checkpoint_ingested_files.txt

# Run pipeline by default
CMD ["python", "pipeline_ingest.py"]