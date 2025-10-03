#!/usr/bin/env python3
"""
Immediate Confirmation Storage Module
Handles persistence of immediate confirmation messages and user responses
"""

import json
import logging
from typing import Optional, Set, Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Database connection - reuse the same pattern as other storage modules
try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    mysql = None
    logger.warning("mysql-connector-python not available; immediate confirmation storage disabled")

# Global connection variable
_db_connection = None

def get_db_connection():
    """Get database connection, creating it if needed"""
    global _db_connection
    
    if _db_connection is None or not _db_connection.is_connected():
        try:
            import os
            _db_connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME', 'simple_poll_bot'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=True
            )
            logger.info("Connected to MySQL database for immediate confirmations")
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            _db_connection = None
    
    return _db_connection

def upsert_immediate_confirmation(
    chat_id: int, 
    message_id: int, 
    poll_result: str, 
    poll_id: Optional[str] = None,
    all_voters: Optional[Set[int]] = None, 
    confirmed_users: Optional[Set[int]] = None, 
    declined_users: Optional[Set[int]] = None
) -> bool:
    """
    Store or update immediate confirmation state
    
    Args:
        chat_id: Telegram chat ID
        message_id: Telegram message ID of confirmation message
        poll_result: Selected meeting option text
        poll_id: Optional reference to originating poll
        all_voters: Set of all user IDs who participated in original poll
        confirmed_users: Set of user IDs who clicked "yes"
        declined_users: Set of user IDs who clicked "no"
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not mysql:
        logger.warning("MySQL not available, cannot store immediate confirmation")
        return False
    
    connection = get_db_connection()
    if not connection:
        return False
    
    # Convert sets to lists for JSON serialization
    all_voters = list(all_voters) if all_voters else []
    confirmed_users = list(confirmed_users) if confirmed_users else []
    declined_users = list(declined_users) if declined_users else []
    
    try:
        cursor = connection.cursor()
        
        query = """
            INSERT INTO immediate_confirmations 
            (chat_id, poll_id, message_id, poll_result, all_voters, confirmed_users, declined_users)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            confirmed_users = VALUES(confirmed_users),
            declined_users = VALUES(declined_users),
            updated_at = CURRENT_TIMESTAMP
        """
        
        values = (
            chat_id, 
            poll_id, 
            message_id, 
            poll_result,
            json.dumps(all_voters),
            json.dumps(confirmed_users),
            json.dumps(declined_users)
        )
        
        cursor.execute(query, values)
        logger.info(f"Stored immediate confirmation for chat {chat_id}, message {message_id}")
        return True
        
    except Error as e:
        logger.error(f"Error storing immediate confirmation: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def get_immediate_confirmation(chat_id: int, message_id: int) -> Optional[Dict[str, Any]]:
    """
    Get immediate confirmation state for recovery
    
    Args:
        chat_id: Telegram chat ID
        message_id: Telegram message ID
    
    Returns:
        Dict with confirmation data or None if not found
    """
    if not mysql:
        return None
    
    connection = get_db_connection()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT chat_id, poll_id, message_id, poll_result, all_voters, 
                   confirmed_users, declined_users, status, completion_message_id,
                   created_at, updated_at
            FROM immediate_confirmations 
            WHERE chat_id = %s AND message_id = %s AND status = 'pending'
        """
        
        cursor.execute(query, (chat_id, message_id))
        row = cursor.fetchone()
        
        if row:
            return {
                'chat_id': row['chat_id'],
                'poll_id': row['poll_id'],
                'message_id': row['message_id'],
                'poll_result': row['poll_result'],
                'all_voters': set(json.loads(row['all_voters'])),
                'confirmed_users': set(json.loads(row['confirmed_users'])),
                'declined_users': set(json.loads(row['declined_users'])),
                'status': row['status'],
                'completion_message_id': row['completion_message_id'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            }
        
        return None
        
    except Error as e:
        logger.error(f"Error getting immediate confirmation: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

def get_all_pending_confirmations() -> List[Dict[str, Any]]:
    """
    Get all pending immediate confirmations for recovery on bot startup
    
    Returns:
        List of confirmation dictionaries
    """
    if not mysql:
        return []
    
    connection = get_db_connection()
    if not connection:
        return []
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT chat_id, poll_id, message_id, poll_result, all_voters, 
                   confirmed_users, declined_users, status, completion_message_id,
                   created_at, updated_at
            FROM immediate_confirmations 
            WHERE status = 'pending'
            ORDER BY created_at DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        confirmations = []
        for row in rows:
            confirmations.append({
                'chat_id': row['chat_id'],
                'poll_id': row['poll_id'],
                'message_id': row['message_id'],
                'poll_result': row['poll_result'],
                'all_voters': set(json.loads(row['all_voters'])),
                'confirmed_users': set(json.loads(row['confirmed_users'])),
                'declined_users': set(json.loads(row['declined_users'])),
                'status': row['status'],
                'completion_message_id': row['completion_message_id'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            })
        
        logger.info(f"Retrieved {len(confirmations)} pending confirmations")
        return confirmations
        
    except Error as e:
        logger.error(f"Error getting pending confirmations: {e}")
        return []
    finally:
        if cursor:
            cursor.close()

def update_confirmation_response(chat_id: int, message_id: int, user_id: int, response: str) -> bool:
    """
    Update user response (yes/no) for immediate confirmation
    Prevents duplicate responses and rejects expired confirmations
    
    Args:
        chat_id: Telegram chat ID
        message_id: Telegram message ID
        user_id: User ID who responded
        response: 'yes' or 'no'
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not mysql:
        return False
    
    # Get current state (only pending confirmations)
    conf = get_immediate_confirmation(chat_id, message_id)
    if not conf:
        logger.warning(f"No confirmation found for chat {chat_id}, message {message_id}")
        return False
    
    # Reject expired confirmations - user can't respond after 24h
    if conf['status'] == 'expired':
        logger.info(f"Ignoring response from user {user_id} - confirmation expired for chat {chat_id}, message {message_id}")
        return False
    
    # Reject if confirmation is not pending
    if conf['status'] != 'pending':
        logger.info(f"Ignoring response from user {user_id} - confirmation status is '{conf['status']}' for chat {chat_id}, message {message_id}")
        return False
    
    # Check if user already responded with the same answer (prevent duplicate responses)
    confirmed_users = conf['confirmed_users']
    declined_users = conf['declined_users']
    
    if response == 'yes' and user_id in confirmed_users:
        logger.info(f"User {user_id} already confirmed attendance for chat {chat_id} - ignoring duplicate response")
        return False
    
    if response == 'no' and user_id in declined_users:
        logger.info(f"User {user_id} already declined attendance for chat {chat_id} - ignoring duplicate response")
        return False
    
    # Update user lists
    confirmed_users = list(conf['confirmed_users'])
    declined_users = list(conf['declined_users'])
    
    # Remove from both lists first
    if user_id in confirmed_users:
        confirmed_users.remove(user_id)
    if user_id in declined_users:
        declined_users.remove(user_id)
    
    # Add to appropriate list
    if response == 'yes':
        confirmed_users.append(user_id)
        logger.info(f"User {user_id} confirmed attendance for chat {chat_id}")
    elif response == 'no':
        declined_users.append(user_id)
        logger.info(f"User {user_id} declined attendance for chat {chat_id}")
    else:
        logger.warning(f"Invalid response '{response}' from user {user_id}")
        return False
    
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        query = """
            UPDATE immediate_confirmations 
            SET confirmed_users = %s, declined_users = %s, updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = %s AND message_id = %s AND status = 'pending'
        """
        
        values = (
            json.dumps(confirmed_users),
            json.dumps(declined_users),
            chat_id,
            message_id
        )
        
        cursor.execute(query, values)
        
        if cursor.rowcount > 0:
            logger.info(f"Updated confirmation response for user {user_id} in chat {chat_id}")
            return True
        else:
            logger.warning(f"No pending confirmation found to update for chat {chat_id}, message {message_id}")
            return False
        
    except Error as e:
        logger.error(f"Error updating confirmation response: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def complete_immediate_confirmation(chat_id: int, message_id: int, completion_message_id: Optional[int] = None) -> bool:
    """
    Mark immediate confirmation as completed
    
    Args:
        chat_id: Telegram chat ID
        message_id: Telegram message ID of original confirmation
        completion_message_id: Message ID of "🎉 Отлично! Встреча состоится!" message
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not mysql:
        return False
    
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        query = """
            UPDATE immediate_confirmations 
            SET status = 'completed', 
                completed_at = CURRENT_TIMESTAMP, 
                completion_message_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = %s AND message_id = %s
        """
        
        cursor.execute(query, (completion_message_id, chat_id, message_id))
        
        if cursor.rowcount > 0:
            logger.info(f"Marked confirmation as completed for chat {chat_id}, message {message_id}")
            return True
        else:
            logger.warning(f"No confirmation found to complete for chat {chat_id}, message {message_id}")
            return False
        
    except Error as e:
        logger.error(f"Error completing immediate confirmation: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def cancel_immediate_confirmation(chat_id: int, message_id: int) -> bool:
    """
    Mark immediate confirmation as cancelled
    
    Args:
        chat_id: Telegram chat ID
        message_id: Telegram message ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not mysql:
        return False
    
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        query = """
            UPDATE immediate_confirmations 
            SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = %s AND message_id = %s
        """
        
        cursor.execute(query, (chat_id, message_id))
        
        if cursor.rowcount > 0:
            logger.info(f"Cancelled confirmation for chat {chat_id}, message {message_id}")
            return True
        else:
            logger.warning(f"No confirmation found to cancel for chat {chat_id}, message {message_id}")
            return False
        
    except Error as e:
        logger.error(f"Error cancelling immediate confirmation: {e}")
        return False
    finally:
        if cursor:
            cursor.close()

def cleanup_expired_confirmations() -> int:
    """
    Clean up expired confirmations (older than 24 hours)
    
    Returns:
        int: Number of confirmations cleaned up
    """
    if not mysql:
        return 0
    
    connection = get_db_connection()
    if not connection:
        return 0
    
    try:
        cursor = connection.cursor()
        
        # Mark old pending confirmations as expired
        expire_query = """
            UPDATE immediate_confirmations 
            SET status = 'expired', updated_at = CURRENT_TIMESTAMP
            WHERE status = 'pending' 
            AND created_at < DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """
        
        cursor.execute(expire_query)
        expired_count = cursor.rowcount
        
        # Delete very old completed/expired records (older than 7 days)
        delete_query = """
            DELETE FROM immediate_confirmations 
            WHERE status IN ('completed', 'cancelled', 'expired') 
            AND (completed_at < DATE_SUB(NOW(), INTERVAL 7 DAY) 
                 OR updated_at < DATE_SUB(NOW(), INTERVAL 7 DAY))
        """
        
        cursor.execute(delete_query)
        deleted_count = cursor.rowcount
        
        total_cleaned = expired_count + deleted_count
        if total_cleaned > 0:
            logger.info(f"Cleaned up {expired_count} expired and {deleted_count} old confirmations")
        
        return total_cleaned
        
    except Error as e:
        logger.error(f"Error cleaning up confirmations: {e}")
        return 0
    finally:
        if cursor:
            cursor.close()

# Helper function to check if all users have confirmed
def check_all_confirmed(confirmation_data: Dict[str, Any]) -> bool:
    """
    Check if all users have confirmed attendance
    
    Args:
        confirmation_data: Dictionary returned by get_immediate_confirmation
    
    Returns:
        bool: True if all voters have confirmed (voted yes)
    """
    if not confirmation_data:
        return False
    
    all_voters = confirmation_data['all_voters']
    confirmed_users = confirmation_data['confirmed_users']
    
    return len(confirmed_users) == len(all_voters)

def get_confirmation_stats(confirmation_data: Dict[str, Any]) -> Dict[str, int]:
    """
    Get statistics about confirmation responses
    
    Args:
        confirmation_data: Dictionary returned by get_immediate_confirmation
    
    Returns:
        Dict with response counts
    """
    if not confirmation_data:
        return {'total': 0, 'confirmed': 0, 'declined': 0, 'pending': 0}
    
    total = len(confirmation_data['all_voters'])
    confirmed = len(confirmation_data['confirmed_users'])
    declined = len(confirmation_data['declined_users'])
    pending = total - confirmed - declined
    
    return {
        'total': total,
        'confirmed': confirmed,
        'declined': declined,
        'pending': pending
    }