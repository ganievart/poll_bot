#!/usr/bin/env python3
"""
Meeting Storage Module for Simple Poll Bot

Provides database helpers for persisting finalized meetings
into the dedicated `meetings` table (see schema/meetings_schema.sql).

Conventions:
- meeting_datetime is stored in UTC as a naive DATETIME (no tzinfo),
  aligning with how scheduled_tasks stores times.
- Callers should pass timezone-aware datetimes if possible; this
  module will convert to UTC and strip tzinfo before insert.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("❌ mysql-connector-python not installed!")
    print("📝 Install it with: pip install mysql-connector-python")
    raise

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'simple_poll_bot'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True,
    'raise_on_warnings': True,
}


def get_db_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    if conn.is_connected():
        return conn
    raise RuntimeError("DB connection failed")


def _to_utc_naive(dt: datetime) -> datetime:
    """Convert datetime to UTC and drop tzinfo for MySQL DATETIME storage.
    If dt has no tzinfo, assume it is already UTC.
    """
    if dt is None:
        raise ValueError("meeting_datetime must not be None")
    try:
        # Prefer zoneinfo if available
        try:
            from zoneinfo import ZoneInfo
            utc_tz = ZoneInfo("UTC")
        except Exception:
            import pytz
            utc_tz = pytz.UTC
        if dt.tzinfo is None:
            # Assume already UTC
            return dt.replace(tzinfo=None)
        return dt.astimezone(utc_tz).replace(tzinfo=None)
    except Exception:
        # Fallback to naive
        return dt.replace(tzinfo=None)


def insert_or_update_meeting(chat_id: int,
                             poll_id: Optional[str],
                             meeting_datetime: datetime,
                             selected_option_text: str,
                             confirmation_message_id: Optional[int] = None,
                             pinned_message_id: Optional[int] = None) -> int:
    """Insert a meeting row. If poll_id is provided and exists (unique), update fields.

    Returns: meeting id (lastrowid for insert, or existing id for update if retrievable)
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        dt_utc = _to_utc_naive(meeting_datetime)
        # Clamp text to column length (255)
        text = (selected_option_text or "")[:255]

        # Use INSERT ... ON DUPLICATE KEY UPDATE on unique poll_id
        cur.execute(
            """
            INSERT INTO meetings (
                chat_id, poll_id, meeting_datetime, selected_option_text,
                confirmation_message_id, pinned_message_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                chat_id = VALUES(chat_id),
                meeting_datetime = VALUES(meeting_datetime),
                selected_option_text = VALUES(selected_option_text),
                confirmation_message_id = VALUES(confirmation_message_id),
                pinned_message_id = VALUES(pinned_message_id)
            """,
            (chat_id, poll_id, dt_utc, text, confirmation_message_id, pinned_message_id)
        )
        meeting_id = cur.lastrowid or 0
        return int(meeting_id)
    finally:
        cur.close(); conn.close()


def get_last_meeting_for_chat(chat_id: int) -> Optional[Dict[str, Any]]:
    """Return the most recent meeting for chat with meeting_datetime <= NOW() (UTC)."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT * FROM meetings 
            WHERE chat_id = %s 
              AND meeting_datetime <= UTC_TIMESTAMP()
            ORDER BY meeting_datetime DESC
            LIMIT 1
            """,
            (chat_id,)
        )
        row = cur.fetchone()
        return row
    finally:
        cur.close(); conn.close()


def list_meetings_for_chat(chat_id: int, limit: int = 50, past_only: bool = True) -> List[Dict[str, Any]]:
    """List meetings for a chat, newest first. If past_only, only <= NOW()."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if past_only:
            cur.execute(
                """
                SELECT * FROM meetings 
                WHERE chat_id = %s AND meeting_datetime <= UTC_TIMESTAMP()
                ORDER BY meeting_datetime DESC
                LIMIT %s
                """,
                (chat_id, limit)
            )
        else:
            cur.execute(
                """
                SELECT * FROM meetings 
                WHERE chat_id = %s
                ORDER BY meeting_datetime DESC
                LIMIT %s
                """,
                (chat_id, limit)
            )
        rows = cur.fetchall() or []
        return rows
    finally:
        cur.close(); conn.close()


def delete_future_meetings_for_chat(chat_id: int) -> int:
    """Delete meetings for this chat scheduled in the future (>= now UTC)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM meetings
            WHERE chat_id = %s AND meeting_datetime >= UTC_TIMESTAMP()
            """,
            (chat_id,)
        )
        return cur.rowcount or 0
    finally:
        cur.close(); conn.close()


def cleanup_old_meetings(days_old: int = 365) -> int:
    """Optionally remove meetings older than N days (housekeeping)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM meetings
            WHERE meeting_datetime < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
            """,
            (days_old,)
        )
        return cur.rowcount or 0
    finally:
        cur.close(); conn.close()
