from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# ============================
# CONFIG
# ============================

EVENTS_DIR = Path(os.getenv("EVENTS_DIR", "data/events"))
DB_PATH = Path(os.getenv("DB_PATH", "analytics.sqlite"))
CHECKPOINT_PATH = Path(os.getenv("CHECKPOINT_PATH", ".checkpoint_ingested_files.txt"))


# ============================
# CHECKPOINT (safe)
# ============================

def load_checkpoint() -> Set[str]:
    """Load the list of already ingested file paths from the checkpoint file."""
    if not CHECKPOINT_PATH.exists():
        print("Checkpoint does not exist yet. Starting from scratch.")
        return set()
    return set(CHECKPOINT_PATH.read_text(encoding="utf-8").splitlines())


def save_checkpoint(processed_paths: List[str]) -> None:
    """Append-only checkpoint. Should be called ONLY after a successful DB commit."""
    with open(CHECKPOINT_PATH, "a", encoding="utf-8") as f:
        for p in processed_paths:
            f.write(p + "\n")


# ============================
# FILE READING (JSON only)
# ============================

def read_event_file(path: Path) -> List[Dict[str, Any]]:
    """
    Reads a single JSON file.
    Supported payloads:
      - A single JSON object (dict)
      - A list of JSON objects (list[dict])

    Notes:
      - JSONL is intentionally NOT supported (one JSON per line).
      - Malformed JSON returns an empty list (pipeline keeps running).
    """
    try:
        txt = path.read_text(encoding="utf-8", errors="replace").strip()
        if not txt:
            return []

        obj = json.loads(txt)

        if isinstance(obj, dict):
            return [obj]

        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]

        print(f"⚠️ Skipping file (JSON is not a dict or list of dicts): {path}")
        return []

    except json.JSONDecodeError as e:
        print(f"⚠️ Invalid JSON file skipped: {path} | Error: {e}")
        return []


# ============================
# NORMALIZATION HELPERS
# ============================

def to_str(v: Any) -> Optional[str]:
    """Convert a value to a trimmed string or return None if empty."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def parse_timestamp_candidates(event: Dict[str, Any]) -> Optional[str]:
    """Pick the first available timestamp candidate and return it as a string."""
    for k in ["timestamp", "event_timestamp", "ts", "time"]:
        if event.get(k):
            return str(event[k])
    return None


# ============================
# DB SCHEMA (SQLite + JSON1)
# ============================

def connect_db() -> sqlite3.Connection:
    """Open a SQLite connection with FK enforcement enabled."""
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA foreign_keys = ON;")     # Enforce foreign keys
    con.execute("PRAGMA journal_mode = WAL;")    # Better write performance for incremental loads
    return con


def init_schema(con: sqlite3.Connection) -> None:
    """Create tables if they do not exist."""
    # Bronze layer
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT,
            event_ts TEXT,
            user_id TEXT,
            document_id TEXT,
            source_file TEXT,
            raw_json TEXT NOT NULL
        );
    """)

    # Normalized tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            first_seen_ts TEXT,
            last_seen_ts TEXT
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY,
            title TEXT,
            owner_user_id TEXT,
            created_ts TEXT,
            last_seen_ts TEXT,
            FOREIGN KEY (owner_user_id) REFERENCES users(user_id)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_pk INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            event_type TEXT NOT NULL,
            event_ts TEXT,
            user_id TEXT,
            document_id TEXT,

            -- derived
            day_of_week TEXT,

            -- example event-specific fields (extend as needed)
            comment_text TEXT,
            shared_with_user_id TEXT,
            edit_chars_delta INTEGER,

            raw_json TEXT NOT NULL,

            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (document_id) REFERENCES documents(document_id)
        );
    """)


# ============================
# INGESTION (incremental + safe)
# ============================

def find_new_files() -> Tuple[List[Path], Set[str]]:
    """Find new JSON files that are not present in the checkpoint."""
    already = load_checkpoint()

    if not EVENTS_DIR.exists():
        print(f"⚠️ EVENTS_DIR does not exist: {EVENTS_DIR.resolve()}")
        return [], already

    all_files = sorted([p for p in EVENTS_DIR.rglob("*.json") if p.is_file()])
    new_files = [p for p in all_files if str(p.resolve()) not in already]

    print(f"Total files found: {len(all_files)}")
    print(f"New files to ingest: {len(new_files)}")
    return new_files, already


def ingest_raw(con: sqlite3.Connection, new_files: List[Path]) -> List[str]:
    """
    Insert raw events into raw_events.
    Idempotency is guaranteed by PRIMARY KEY(event_id).

    IMPORTANT:
    - If an event does NOT have event_id, it will be skipped.
    - If an event does NOT have event_type, it will be skipped.
    """
    processed: List[str] = []

    for file_path in new_files:
        source_file = str(file_path.resolve())
        events = read_event_file(file_path)

        inserted_rows = 0
        skipped_missing_id = 0
        skipped_missing_type = 0

        for ev in events:
            event_id = to_str(ev.get("event_id"))
            if not event_id:
                skipped_missing_id += 1
                continue

            event_type = to_str(ev.get("event_type"))
            if not event_type:
                skipped_missing_type += 1
                continue

            event_type = event_type.strip().lower()

            event_ts = parse_timestamp_candidates(ev)

            user_id = to_str(ev.get("user_id") or ev.get("userId") or ev.get("uid"))
            document_id = to_str(ev.get("document_id") or ev.get("documentId") or ev.get("doc_id"))

            raw_json = json.dumps(ev, ensure_ascii=False)

            con.execute(
                """
                INSERT OR IGNORE INTO raw_events
                (event_id, event_type, event_ts, user_id, document_id, source_file, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (event_id, event_type, event_ts, user_id, document_id, source_file, raw_json),
            )
            inserted_rows += 1

        processed.append(source_file)
        print(
            f"✅ Ingest raw: {file_path.name} | events={len(events)} | inserted={inserted_rows} "
            f"| skipped_missing_event_id={skipped_missing_id} | skipped_missing_event_type={skipped_missing_type}"
        )

    return processed


# ============================
# TRANSFORMATIONS (normalized tables)
# ============================

def transform_users(con: sqlite3.Connection) -> None:
    """Build a deduplicated users table."""
    con.execute("""
        INSERT OR IGNORE INTO users (user_id, first_seen_ts, last_seen_ts)
        SELECT
            user_id,
            MIN(event_ts) AS first_seen_ts,
            MAX(event_ts) AS last_seen_ts
        FROM raw_events
        WHERE user_id IS NOT NULL
        GROUP BY user_id;
    """)

    con.execute("""
        UPDATE users
        SET
            first_seen_ts = (
                SELECT MIN(r.event_ts) FROM raw_events r
                WHERE r.user_id = users.user_id AND r.event_ts IS NOT NULL
            ),
            last_seen_ts = (
                SELECT MAX(r.event_ts) FROM raw_events r
                WHERE r.user_id = users.user_id AND r.event_ts IS NOT NULL
            )
        WHERE user_id IN (SELECT DISTINCT user_id FROM raw_events WHERE user_id IS NOT NULL);
    """)


def transform_documents(con: sqlite3.Connection) -> None:
    """
    Build a deduplicated documents table.
    Extract document metadata from raw_json using SQLite JSON1 functions.
    """
    con.execute("""
        INSERT OR IGNORE INTO documents (document_id, title, owner_user_id, created_ts, last_seen_ts)
        SELECT
            document_id,
            COALESCE(
                json_extract(raw_json, '$.document.title'),
                json_extract(raw_json, '$.title')
            ) AS title,
            COALESCE(
                json_extract(raw_json, '$.document.owner_user_id'),
                json_extract(raw_json, '$.owner_user_id'),
                json_extract(raw_json, '$.ownerUserId')
            ) AS owner_user_id,
            MIN(event_ts) AS created_ts,
            MAX(event_ts) AS last_seen_ts
        FROM raw_events
        WHERE document_id IS NOT NULL
        GROUP BY document_id;
    """)

    con.execute("""
        UPDATE documents
        SET
            last_seen_ts = (
                SELECT MAX(r.event_ts) FROM raw_events r
                WHERE r.document_id = documents.document_id AND r.event_ts IS NOT NULL
            )
        WHERE document_id IN (SELECT DISTINCT document_id FROM raw_events WHERE document_id IS NOT NULL);
    """)


def day_of_week_expr() -> str:
    """SQLite: strftime('%w', ts) => 0 Sunday ... 6 Saturday."""
    return """
        CASE strftime('%w', event_ts)
            WHEN '0' THEN 'Sunday'
            WHEN '1' THEN 'Monday'
            WHEN '2' THEN 'Tuesday'
            WHEN '3' THEN 'Wednesday'
            WHEN '4' THEN 'Thursday'
            WHEN '5' THEN 'Friday'
            WHEN '6' THEN 'Saturday'
        END
    """


def transform_events(con: sqlite3.Connection) -> None:
    """
    Build a flattened events table with derived fields.
    Inserts are idempotent due to UNIQUE(event_id).
    """
    con.execute(f"""
        INSERT OR IGNORE INTO events (
            event_id, event_type, event_ts, user_id, document_id,
            day_of_week,
            comment_text, shared_with_user_id, edit_chars_delta,
            raw_json
        )
        SELECT
            r.event_id,
            r.event_type,
            r.event_ts,
            r.user_id,
            r.document_id,

            {day_of_week_expr()} AS day_of_week,

            COALESCE(
                json_extract(r.raw_json, '$.comment.text'),
                json_extract(r.raw_json, '$.comment_text')
            ) AS comment_text,

            COALESCE(
                json_extract(r.raw_json, '$.shared_with_user_id'),
                json_extract(r.raw_json, '$.sharedWithUserId')
            ) AS shared_with_user_id,

            COALESCE(
                json_extract(r.raw_json, '$.edit.chars_delta'),
                json_extract(r.raw_json, '$.edit_chars_delta')
            ) AS edit_chars_delta,

            r.raw_json
        FROM raw_events r
        WHERE r.event_type IS NOT NULL;
    """)


def run_transformations(con: sqlite3.Connection) -> None:
    """Run all transformation steps."""
    transform_users(con)
    transform_documents(con)
    transform_events(con)


# ============================
# MAIN
# ============================

def main() -> None:
    new_files, _already = find_new_files()
    if not new_files:
        print("No new files to ingest.")
        return

    con = connect_db()
    try:
        init_schema(con)

        # Use an explicit transaction
        con.execute("BEGIN;")

        processed_paths = ingest_raw(con, new_files)
        run_transformations(con)

        con.execute("COMMIT;")

        # Save checkpoint only after a successful commit
        save_checkpoint(processed_paths)

        # Basic metrics
        raw_cnt = con.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
        users_cnt = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        docs_cnt = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        events_cnt = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

        print("\n📌 Pipeline OK")
        print(f"raw_events: {raw_cnt}")
        print(f"users: {users_cnt}")
        print(f"documents: {docs_cnt}")
        print(f"events: {events_cnt}")

    except Exception:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()