#!/usr/bin/env python3
"""
Persistent storage helpers for polls and votes.
Uses mysql-connector-python and same env configuration as task_storage.
"""
import os
import logging
import json
from typing import Optional, Dict, List, Set, Any, Iterable
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("❌ mysql-connector-python not installed!")
    print("📝 Install it with: pip install mysql-connector-python")
    raise

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME', 'simple_poll_bot'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'autocommit': True,
    'raise_on_warnings': True
}


def get_db_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    if conn.is_connected():
        return conn
    raise RuntimeError("DB connection failed")


# Polls

def upsert_poll(poll_id: str, chat_id: int, question: str, options: List[str], creator_id: int,
                poll_message_id: Optional[int] = None, target_member_count: Optional[int] = None,
                pinned_message_id: Optional[int] = None,
                is_closed: bool = False) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO polls (poll_id, chat_id, question, options_json, creator_id, poll_message_id, target_member_count, pinned_message_id, is_closed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              chat_id = VALUES(chat_id),
              question = VALUES(question),
              options_json = VALUES(options_json),
              creator_id = VALUES(creator_id),
              poll_message_id = VALUES(poll_message_id),
              target_member_count = VALUES(target_member_count),
              pinned_message_id = VALUES(pinned_message_id),
              is_closed = VALUES(is_closed)
            """,
            (poll_id, chat_id, question, json.dumps(options, ensure_ascii=False), creator_id,
             poll_message_id, target_member_count, pinned_message_id, is_closed)
        )
    finally:
        cur.close(); conn.close()


def set_poll_closed(poll_id: str, closed: bool = True) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE polls SET is_closed=%s WHERE poll_id=%s", (closed, poll_id))
    finally:
        cur.close(); conn.close()


def get_poll(poll_id: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM polls WHERE poll_id=%s", (poll_id,))
        row = cur.fetchone()
        if not row:
            return None
        # decode JSON
        row['options'] = json.loads(row['options_json']) if row.get('options_json') else []
        return row
    finally:
        cur.close(); conn.close()


def get_open_polls() -> List[Dict[str, Any]]:
    """Return all polls where is_closed = false"""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM polls WHERE is_closed = FALSE")
        rows = cur.fetchall() or []
        for row in rows:
            row['options'] = json.loads(row['options_json']) if row.get('options_json') else []
        return rows
    finally:
        cur.close(); conn.close()


# Votes


def get_polls_with_pins_by_chat(chat_id: int) -> List[Dict[str, Any]]:
    """Return polls in given chat that have a pinned message id set."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT poll_id, pinned_message_id FROM polls WHERE chat_id=%s AND pinned_message_id IS NOT NULL",
            (chat_id,)
        )
        rows = cur.fetchall() or []
        return rows
    finally:
        cur.close(); conn.close()


def clear_pinned_message_id(poll_id: str) -> None:
    """Clear pinned_message_id for a poll after unpinning in chat."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE polls SET pinned_message_id=NULL WHERE poll_id=%s", (poll_id,))
    finally:
        cur.close(); conn.close()


def upsert_vote(poll_id: str, user_id: int, option_ids: List[int]) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO poll_votes (poll_id, user_id, option_ids_json)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE option_ids_json = VALUES(option_ids_json)
            """,
            (poll_id, user_id, json.dumps(option_ids))
        )
    finally:
        cur.close(); conn.close()


def get_votes(poll_id: str) -> Dict[str, Set[int]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT user_id, option_ids_json FROM poll_votes WHERE poll_id=%s", (poll_id,))
        rows = cur.fetchall() or []
        result: Dict[str, Set[int]] = {}
        for r in rows:
            try:
                opts = json.loads(r['option_ids_json'])
            except Exception:
                opts = []
            result[str(r['user_id'])] = set(int(i) for i in opts)
        return result
    finally:
        cur.close(); conn.close()


# Immediate confirmations (removed)

# Immediate confirmations removed from storage layer

# def get_immediate_confirmation(chat_id: int, message_id: int) -> Optional[Dict[str, Any]]:
#     return None
