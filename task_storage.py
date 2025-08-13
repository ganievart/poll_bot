#!/usr/bin/env python3
"""
Task Storage Module for Telegram Bot
Replaces asyncio.sleep() calls with MySQL-backed scheduling
"""

import os
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("❌ mysql-connector-python not installed!")
    print("📝 Install it with: pip install mysql-connector-python")
    exit(1)

# Configure logging
logger = logging.getLogger(__name__)

# Database configuration from environment variables
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
    """
    Get a MySQL database connection

    Returns:
        mysql.connector.connection: Database connection object

    Raises:
        mysql.connector.Error: If connection fails
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logger.debug("Successfully connected to MySQL database")
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise
    

def get_due_tasks() -> List[Dict[str, Any]]:
    """
    Get all tasks that are due for execution
    
    Returns:
        List[Dict]: List of task dictionaries with keys:
                   - id, chat_id, poll_id, task_type, scheduled_time, task_data
                   
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT id, chat_id, poll_id, task_type, scheduled_time, task_data, created_at
        FROM scheduled_tasks 
        WHERE is_executed = FALSE 
          AND scheduled_time <= NOW() 
        ORDER BY scheduled_time ASC
        """
        
        cursor.execute(query)
        tasks = cursor.fetchall()
        
        logger.info(f"Found {len(tasks)} due tasks")
        return tasks
        
    except Error as e:
        logger.error(f"Error fetching due tasks: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def mark_task_executed(task_id: int) -> bool:
    """
    Mark a task as executed
    
    Args:
        task_id (int): The ID of the task to mark as executed
        
    Returns:
        bool: True if task was marked as executed, False otherwise
        
    Raises:
        mysql.connector.Error: If database update fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        UPDATE scheduled_tasks 
        SET is_executed = TRUE, executed_at = NOW() 
        WHERE id = %s
        """
        
        cursor.execute(query, (task_id,))
        rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"Task {task_id} marked as executed")
            return True
        else:
            logger.warning(f"Task {task_id} not found or already executed")
            return False
            
    except Error as e:
        logger.error(f"Error marking task {task_id} as executed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def add_scheduled_task(chat_id: int, poll_id: str, task_type: str, 
                      scheduled_time: datetime, task_data: str = None) -> int:
    """
    Add a new scheduled task
    
    Args:
        chat_id (int): Telegram chat ID
        poll_id (str): Poll identifier
        task_type (str): Type of task (confirmation, followup, unpin_message, etc.)
        scheduled_time (datetime): When to execute the task
        task_data (str, optional): Additional data for the task
        
    Returns:
        int: ID of the created task
        
    Raises:
        mysql.connector.Error: If database insert fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        INSERT INTO scheduled_tasks (chat_id, poll_id, task_type, scheduled_time, task_data) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(query, (chat_id, poll_id, task_type, scheduled_time, task_data))
        task_id = cursor.lastrowid
        
        logger.info(f"Added scheduled task {task_id}: {task_type} for chat {chat_id} at {scheduled_time}")
        return task_id
        
    except Error as e:
        logger.error(f"Error adding scheduled task: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()



def cancel_chat_tasks(chat_id: int, task_type: Optional[str] = None) -> int:
    """
    Cancel all pending tasks for a specific chat (for /cancel_bot command or poll closure).

    Args:
        chat_id (int): Telegram chat ID
        task_type (Optional[str]): If provided, only cancel tasks of this type.

    Returns:
        int: Number of tasks cancelled
        
    Raises:
        mysql.connector.Error: If database update fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        if task_type:
            query = (
                "UPDATE scheduled_tasks "
                "SET is_executed = TRUE, executed_at = NOW() "
                "WHERE chat_id = %s AND task_type = %s AND is_executed = FALSE"
            )
            params = (chat_id, task_type)
        else:
            query = (
                "UPDATE scheduled_tasks "
                "SET is_executed = TRUE, executed_at = NOW() "
                "WHERE chat_id = %s AND is_executed = FALSE"
            )
            params = (chat_id,)
        
        cursor.execute(query, params)
        cancelled_count = cursor.rowcount
        
        if task_type:
            logger.info(f"Cancelled {cancelled_count} tasks of type '{task_type}' for chat {chat_id}")
        else:
            logger.info(f"Cancelled {cancelled_count} tasks for chat {chat_id}")
        return cancelled_count
        
    except Error as e:
        logger.error(f"Error cancelling tasks for chat {chat_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def get_chat_pending_tasks(chat_id: int) -> List[Dict[str, Any]]:
    """
    Get all pending tasks for a specific chat
    
    Args:
        chat_id (int): Telegram chat ID
        
    Returns:
        List[Dict]: List of pending task dictionaries
        
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT id, poll_id, task_type, scheduled_time, task_data, created_at
        FROM scheduled_tasks 
        WHERE chat_id = %s AND is_executed = FALSE 
        ORDER BY scheduled_time ASC
        """
        
        cursor.execute(query, (chat_id,))
        tasks = cursor.fetchall()
        
        logger.debug(f"Found {len(tasks)} pending tasks for chat {chat_id}")
        return tasks
        
    except Error as e:
        logger.error(f"Error fetching pending tasks for chat {chat_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def cleanup_old_tasks(days_old: int = 30) -> int:
    """
    Clean up old executed tasks
    
    Args:
        days_old (int): Remove tasks executed more than this many days ago
        
    Returns:
        int: Number of tasks deleted
        
    Raises:
        mysql.connector.Error: If database delete fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        DELETE FROM scheduled_tasks 
        WHERE is_executed = TRUE 
          AND executed_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        
        cursor.execute(query, (days_old,))
        deleted_count = cursor.rowcount
        
        logger.info(f"Cleaned up {deleted_count} old tasks (older than {days_old} days)")
        return deleted_count
        
    except Error as e:
        logger.error(f"Error cleaning up old tasks: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def test_connection() -> bool:
    """
    Test database connection
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        connection = get_db_connection()
        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            logger.info(f"Successfully connected to MySQL Server version {db_info}")
            connection.close()
            return True
    except Error as e:
        logger.error(f"Database connection test failed: {e}")
        return False


# Example usage and testing
if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🔍 Testing database connection...")
    if test_connection():
        print("✅ Database connection successful!")
        
        # Test adding a task
        try:
            from datetime import datetime, timedelta
            
            # Add a test task
            future_time = datetime.now() + timedelta(minutes=1)
            task_id = add_scheduled_task(
                chat_id=-1001234567890,
                poll_id="test_poll_123",
                task_type="confirmation",
                scheduled_time=future_time,
                task_data="Test confirmation message"
            )
            print(f"✅ Added test task with ID: {task_id}")
            
            # Get pending tasks
            pending = get_due_tasks()
            print(f"📋 Found {len(pending)} due tasks")
            
            # Get chat tasks
            chat_tasks = get_chat_pending_tasks(-1001234567890)
            print(f"💬 Found {len(chat_tasks)} tasks for test chat")
            
        except Exception as e:
            print(f"❌ Error during testing: {e}")
    else:
        print("❌ Database connection failed!")
        print("💡 Check your database configuration in environment variables:")
        print("   - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")