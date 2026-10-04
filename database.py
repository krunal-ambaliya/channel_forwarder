import sqlite3
import json
import os
from typing import List, Dict, Any, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forwarder.db")

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    # Forwarding Tasks table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        source_id TEXT NOT NULL,
        source_title TEXT,
        destination_id TEXT NOT NULL,
        destination_title TEXT,
        status TEXT DEFAULT 'idle',  -- idle, running, paused, completed, error
        mode TEXT DEFAULT 'history',  -- history, live, both
        reupload_mode INTEGER DEFAULT 0, -- 0: smart clean copy, 1: true download & re-upload
        media_filter TEXT DEFAULT 'all', -- all, media_only, text_only, photos, videos, documents
        delay_seconds REAL DEFAULT 1.5,
        total_messages INTEGER DEFAULT 0,
        processed_messages INTEGER DEFAULT 0,
        forwarded_messages INTEGER DEFAULT 0,
        failed_messages INTEGER DEFAULT 0,
        last_source_msg_id INTEGER DEFAULT 0,
        min_msg_id INTEGER DEFAULT 0,
        max_msg_id INTEGER DEFAULT 0,
        reverse_order INTEGER DEFAULT 1, -- 1: oldest to newest (chronological)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Forwarded messages record (deduplication)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forwarded_records (
        task_id INTEGER,
        source_id TEXT,
        source_msg_id INTEGER,
        destination_id TEXT,
        dest_msg_id INTEGER,
        forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (task_id, source_id, source_msg_id)
    )
    """)

    # App settings / config store
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    conn.commit()
    conn.close()

def is_message_forwarded(task_id: int, source_id: str, source_msg_id: int) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM forwarded_records WHERE task_id = ? AND source_id = ? AND source_msg_id = ?",
        (task_id, str(source_id), source_msg_id)
    )
    res = cursor.fetchone()
    conn.close()
    return res is not None

def record_forwarded_message(task_id: int, source_id: str, source_msg_id: int, dest_id: str, dest_msg_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO forwarded_records (task_id, source_id, source_msg_id, destination_id, dest_msg_id)
        VALUES (?, ?, ?, ?, ?)
    """, (task_id, str(source_id), source_msg_id, str(dest_id), dest_msg_id))
    conn.commit()
    conn.close()

def get_setting(key: str, default: Any = None) -> Any:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row["value"])
        except Exception:
            return row["value"]
    return default

def set_setting(key: str, value: Any):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, json.dumps(value)))
    conn.commit()
    conn.close()
