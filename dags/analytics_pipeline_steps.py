from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


from pipeline_ingest import (
    connect_db,
    init_schema,
    find_new_files,
    ingest_raw,
    run_transformations,
    save_checkpoint,
)

EVENTS_DIR = os.getenv("EVENTS_DIR", "/opt/airflow/project/data/events")
DB_PATH = os.getenv("DB_PATH", "/opt/airflow/project/storage/analytics.sqlite")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/airflow/project/storage/.checkpoint_ingested_files.txt")

default_args = {
    "owner": "data-team",
    "retries": 1,
}

with DAG(
    dag_id="analytics_pipeline_steps",
    description="Ingest JSON events into SQLite and transform into normalized tables (no Docker)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",  # every day at 06:00
    catchup=False,
    tags=["analytics", "sqlite", "pipeline"],
) as dag:

    def task_find_new_files(**context):
        """
        Finds new files and pushes them to XCom.
        """
        # override global config inside pipeline module
        import pipeline_ingest
        pipeline_ingest.EVENTS_DIR = pipeline_ingest.Path(EVENTS_DIR)
        pipeline_ingest.DB_PATH = pipeline_ingest.Path(DB_PATH)
        pipeline_ingest.CHECKPOINT_PATH = pipeline_ingest.Path(CHECKPOINT_PATH)

        new_files, _already = find_new_files()
        new_files_str = [str(p) for p in new_files]

        print(f"New files detected: {len(new_files_str)}")
        context["ti"].xcom_push(key="new_files", value=new_files_str)

    def task_ingest_raw(**context):
        """
        Ingests only raw_events for new files.
        Saves processed file list into XCom (but DOES NOT checkpoint yet).
        """
        import pipeline_ingest
        pipeline_ingest.EVENTS_DIR = pipeline_ingest.Path(EVENTS_DIR)
        pipeline_ingest.DB_PATH = pipeline_ingest.Path(DB_PATH)
        pipeline_ingest.CHECKPOINT_PATH = pipeline_ingest.Path(CHECKPOINT_PATH)

        new_files_str = context["ti"].xcom_pull(key="new_files", task_ids="find_new_files") or []
        if not new_files_str:
            print("No new files. Skipping ingestion.")
            context["ti"].xcom_push(key="processed_paths", value=[])
            return

        con = connect_db()
        try:
            init_schema(con)

            # start transaction for ingestion step
            con.execute("BEGIN;")

            from pathlib import Path
            processed_paths = ingest_raw(con, [Path(p) for p in new_files_str])

            con.execute("COMMIT;")
            context["ti"].xcom_push(key="processed_paths", value=processed_paths)

            print(f"Raw ingestion completed. Processed files: {len(processed_paths)}")

        except Exception:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            con.close()

    def task_transform(**context):
        """
        Runs transformations (users/documents/events).
        """
        import pipeline_ingest
        pipeline_ingest.EVENTS_DIR = pipeline_ingest.Path(EVENTS_DIR)
        pipeline_ingest.DB_PATH = pipeline_ingest.Path(DB_PATH)
        pipeline_ingest.CHECKPOINT_PATH = pipeline_ingest.Path(CHECKPOINT_PATH)

        con = connect_db()
        try:
            init_schema(con)

            con.execute("BEGIN;")
            run_transformations(con)
            con.execute("COMMIT;")

            print("Transformations completed successfully.")

        except Exception:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            raise
        finally:
            con.close()

    def task_checkpoint(**context):
        """
        Saves checkpoint only after ALL previous steps succeeded.
        """
        import pipeline_ingest
        pipeline_ingest.EVENTS_DIR = pipeline_ingest.Path(EVENTS_DIR)
        pipeline_ingest.DB_PATH = pipeline_ingest.Path(DB_PATH)
        pipeline_ingest.CHECKPOINT_PATH = pipeline_ingest.Path(CHECKPOINT_PATH)

        processed_paths = context["ti"].xcom_pull(key="processed_paths", task_ids="ingest_raw") or []
        if not processed_paths:
            print("No processed paths to checkpoint.")
            return

        save_checkpoint(processed_paths)
        print(f"Checkpoint updated with {len(processed_paths)} files.")

    find_new_files_task = PythonOperator(
        task_id="find_new_files",
        python_callable=task_find_new_files,
    )

    ingest_raw_task = PythonOperator(
        task_id="ingest_raw",
        python_callable=task_ingest_raw,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    checkpoint_task = PythonOperator(
        task_id="save_checkpoint",
        python_callable=task_checkpoint,
    )

    find_new_files_task >> ingest_raw_task >> transform_task >> checkpoint_task