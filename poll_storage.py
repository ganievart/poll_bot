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
                is_closed: bool = False,
                revote_notified: Optional[bool] = None,
                in_revote: Optional[bool] = None,
                last_tie_signature: Optional[str] = None,
                last_tie_message_at: Optional[datetime] = None,
                tie_message_count: Optional[int] = None,
                revote_message_id: Optional[int] = None) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Coerce None values to column defaults to avoid NOT NULL constraint errors
        rn = bool(revote_notified) if revote_notified is not None else False
        ir = bool(in_revote) if in_revote is not None else False
        lts = last_tie_signature  # nullable
        ltm = last_tie_message_at  # nullable
        tmc = int(tie_message_count) if tie_message_count is not None else 0
        rmid = revote_message_id  # nullable

        cur.execute(
            """
            INSERT INTO polls (
              poll_id, chat_id, question, options_json, creator_id,
              poll_message_id, target_member_count, pinned_message_id, is_closed,
              revote_notified, in_revote, last_tie_signature, last_tie_message_at, tie_message_count, revote_message_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            AS new
            ON DUPLICATE KEY UPDATE
              chat_id = new.chat_id,
              question = new.question,
              options_json = new.options_json,
              creator_id = new.creator_id,
              poll_message_id = new.poll_message_id,
              target_member_count = new.target_member_count,
              pinned_message_id = new.pinned_message_id,
              is_closed = new.is_closed,
              revote_notified = COALESCE(new.revote_notified, polls.revote_notified),
              in_revote = COALESCE(new.in_revote, polls.in_revote),
              last_tie_signature = COALESCE(new.last_tie_signature, polls.last_tie_signature),
              last_tie_message_at = COALESCE(new.last_tie_message_at, polls.last_tie_message_at),
              tie_message_count = COALESCE(new.tie_message_count, polls.tie_message_count),
              revote_message_id = COALESCE(new.revote_message_id, polls.revote_message_id)
            """,
            (poll_id, chat_id, question, json.dumps(options, ensure_ascii=False), creator_id,
             poll_message_id, target_member_count, pinned_message_id, is_closed,
             rn, ir, lts, ltm, tmc, rmid)
        )
    finally:
        cur.close(); conn.close()


def set_poll_closed(poll_id: str, closed: bool = True) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Log connection details
        cur.execute("UPDATE polls SET is_closed=%s WHERE poll_id=%s", (closed, poll_id))
        rows_affected = cur.rowcount

        # Verify in same connection
        cur.execute("SELECT is_closed FROM polls WHERE poll_id=%s", (poll_id,))
        result = cur.fetchone()
        logger.info(f"is_closed value after update: {result[0] if result else 'NOT FOUND'}")

        if rows_affected > 0:
            logger.info(f"Successfully updated poll {poll_id} closed status to {closed}")
        else:
            logger.warning(f"No rows updated for poll {poll_id}")
    except Exception as e:
        logger.error(f"Error updating poll {poll_id}: {e}")
        raise
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


def get_expired_open_polls(days: int = 2) -> List[Dict[str, Any]]:
    """Return polls still open whose created_at is older than NOW() - INTERVAL days."""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT poll_id, chat_id, poll_message_id, question, created_at, is_closed FROM polls WHERE is_closed = FALSE AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
            (days,)
        )
        rows = cur.fetchall() or []
        logger.info(f"Found {len(rows)} expired open polls (days={days})")
        if rows:
            logger.warning(f"Expired open polls details: {[(r['poll_id'], r['is_closed'], r['created_at']) for r in rows]}")

        # Return only the originally requested columns
        return [{k: v for k, v in row.items() if k != 'is_closed'} for row in rows]
    finally:
        cur.close(); conn.close()


# Votes

def update_tie_state(poll_id: str,
                     revote_notified: Optional[bool] = None,
                     in_revote: Optional[bool] = None,
                     last_tie_signature: Optional[str] = None,
                     last_tie_message_at: Optional[datetime] = None,
                     tie_message_count: Optional[int] = None,
                     revote_message_id: Optional[int] = None) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Build dynamic SET clause only for provided fields
        fields = []
        params = []
        if revote_notified is not None:
            fields.append("revote_notified=%s"); params.append(revote_notified)
        if in_revote is not None:
            fields.append("in_revote=%s"); params.append(in_revote)
        if last_tie_signature is not None:
            fields.append("last_tie_signature=%s"); params.append(last_tie_signature)
        if last_tie_message_at is not None:
            fields.append("last_tie_message_at=%s"); params.append(last_tie_message_at)
        if tie_message_count is not None:
            fields.append("tie_message_count=%s"); params.append(tie_message_count)
        if revote_message_id is not None:
            fields.append("revote_message_id=%s"); params.append(revote_message_id)
        if not fields:
            return
        set_clause = ", ".join(fields)
        query = f"UPDATE polls SET {set_clause} WHERE poll_id=%s"
        params.append(poll_id)
        cur.execute(query, tuple(params))
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
            AS new
            ON DUPLICATE KEY UPDATE option_ids_json = new.option_ids_json
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
