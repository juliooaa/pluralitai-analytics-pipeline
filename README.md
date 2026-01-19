## Overview

This project simulates ingestion from a streaming source by reading raw JSON log files from a folder (`data/events/`).  
It loads data incrementally into a local SQLite database (`analytics.sqlite`) and produces normalized tables for downstream analytics and reporting.

## Requirements

- Python 3.9+
- SQLite (`sqlite3` CLI recommended for running queries)
- Docker

## How to Run

This project can be executed in two ways:

1) **Locally with Python** (recommended for development)  
2) **With Docker Compose** (recommended for consistency and easy setup)

### ✅ Option 1 — Run Locally (Python)
   - run pipeline_ingest: python3 pipeline_ingest.py
   - run analytics: sqlite3 storage/analytics.sqlite -cmd ".headers on" -cmd ".mode column" < analytics_queries.sql

### ✅ Option 2 — Run with Docker Compose
   - run pipeline_ingest: docker compose up --build
   - run analytics: docker compose run --rm pipeline sh -lc \
"sqlite3 /state/analytics.sqlite -cmd '.headers on' -cmd '.mode column' < /app/analytics_queries.sql"

### Event examples
- `user_login`
- `document_edit`
- `comment_added`
- `document_shared`

## Features

- **Incremental ingestion** from local JSON logs (`data/events/`)
- **Checkpointing** to ingest only new files (`.checkpoint_ingested_files.txt`)
- **Bronze → Normalized tables**
  - `raw_events` (bronze/audit)
  - `users` (deduplicated users)
  - `documents` (deduplicated documents + metadata)
  - `events` (flattened fact table + derived fields)
- **Analytics-ready SQL** (`analytics_queries.sql`)
  - DAU (last 30 days)
  - Avg session duration by user (via `session_id`)
  - Top edited documents
  - Shared docs per user
  - Anomaly checks

## Project Structure
.
├── pipeline_ingest.py              # Ingestion + transformations (SQLite)
├── analytics_queries.sql           # Reporting queries / metrics
├── dags/
│   └── analytics_pipeline_steps.py  # Airflow DAG (no Docker)
├── data/
│   └── events/                     # Input JSON events (local)
└── storage/
├── analytics.sqlite            # SQLite DB (generated)
└── .checkpoint_ingested_files.txt  # Checkpoint file (generated)


## Database Schema

### raw_events (Bronze / Audit)

Stores raw ingested events for debugging and auditability.

Key columns:
	•	event_id (PRIMARY KEY)
	•	event_type
	•	event_ts
	•	user_id
	•	document_id
	•	session_id
	•	source_file
	•	raw_json

### users

Deduplicated users with first/last observed timestamps.

### documents

Deduplicated documents. Attempts to extract metadata from raw_json using JSON paths such as:
	•	$.document.title or $.title
	•	$.document.owner_user_id / $.owner_user_id / $.ownerUserId

### events

Flattened fact table with derived fields (e.g., day_of_week) and a few event-specific extracted fields:
	•	comment_text
	•	shared_with_user_id
	•	edit_chars_delta

