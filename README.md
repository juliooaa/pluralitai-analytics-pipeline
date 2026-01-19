## Overview

This project simulates ingestion from a streaming source by reading raw JSON log files from a folder (`data/events/`).  
It loads data incrementally into a local SQLite database (`analytics.sqlite`) and produces normalized tables for downstream analytics and reporting.

### Event examples
- `user_login`
- `document_edit`
- `comment_added`
- `document_shared`

---

## Project Structure
.
├── pipeline_ingest.py          # Ingestion + normalization pipeline (Python)
├── analytics_queries.sql       # Analytics queries (SQL) for metrics
├── data/
   └── events/                 # Input JSON event files (not committed)
