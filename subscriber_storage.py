#!/usr/bin/env python3
"""
Subscriber Storage Module for Telegram Bot
Database-backed subscriber management with MySQL
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
        logger.debug("Database connection established")
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise


def add_subscriber(user_id: int) -> bool:
    """
    Add a new subscriber to the database
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        bool: True if added successfully, False if already exists or error
        
    Raises:
        mysql.connector.Error: If database operation fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Check if user already exists
        check_query = "SELECT user_id FROM subscribers WHERE user_id = %s"
        cursor.execute(check_query, (user_id,))
        
        if cursor.fetchone():
            logger.info(f"User {user_id} already subscribed")
            return False
        
        # Insert new subscriber
        insert_query = """
        INSERT INTO subscribers (user_id, subscribed_at) 
        VALUES (%s, NOW())
        """
        
        cursor.execute(insert_query, (user_id,))
        
        logger.info(f"Added subscriber: user_id={user_id}")
        return True
        
    except Error as e:
        logger.error(f"Error adding subscriber {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def remove_subscriber(user_id: int) -> bool:
    """
    Remove a subscriber from the database
    
    Args:
        user_id (int): Telegram user ID to remove
        
    Returns:
        bool: True if removed successfully, False if not found
        
    Raises:
        mysql.connector.Error: If database operation fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = "DELETE FROM subscribers WHERE user_id = %s"
        cursor.execute(query, (user_id,))
        
        rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"Removed subscriber: user_id={user_id}")
            return True
        else:
            logger.info(f"User {user_id} not found in subscribers")
            return False
            
    except Error as e:
        logger.error(f"Error removing subscriber {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def is_subscribed(user_id: int) -> bool:
    """
    Check if a user is subscribed
    
    Args:
        user_id (int): Telegram user ID to check
        
    Returns:
        bool: True if subscribed, False otherwise
        
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = "SELECT 1 FROM subscribers WHERE user_id = %s AND is_active = TRUE"
        cursor.execute(query, (user_id,))
        
        result = cursor.fetchone()
        return result is not None
        
    except Error as e:
        logger.error(f"Error checking subscription for user {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def get_all_subscribers() -> List[Dict[str, Any]]:
    """
    Get all active subscribers
    
    Returns:
        List[Dict]: List of subscriber dictionaries with keys:
                   - user_id, subscribed_at
                   
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT user_id, subscribed_at
        FROM subscribers 
        WHERE is_active = TRUE 
        ORDER BY subscribed_at ASC
        """
        
        cursor.execute(query)
        subscribers = cursor.fetchall()
        
        logger.info(f"Retrieved {len(subscribers)} active subscribers")
        return subscribers
        
    except Error as e:
        logger.error(f"Error fetching subscribers: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def get_subscriber_count() -> int:
    """
    Get total count of active subscribers
    
    Returns:
        int: Number of active subscribers
        
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = "SELECT COUNT(*) FROM subscribers WHERE is_active = TRUE"
        cursor.execute(query)
        
        count = cursor.fetchone()[0]
        logger.debug(f"Subscriber count: {count}")
        return count
        
    except Error as e:
        logger.error(f"Error getting subscriber count: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def get_subscriber_ids() -> List[int]:
    """
    Get list of all active subscriber user IDs
    
    Returns:
        List[int]: List of user IDs
        
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = "SELECT user_id FROM subscribers WHERE is_active = TRUE"
        cursor.execute(query)
        
        user_ids = [row[0] for row in cursor.fetchall()]
        logger.debug(f"Retrieved {len(user_ids)} subscriber IDs")
        return user_ids
        
    except Error as e:
        logger.error(f"Error fetching subscriber IDs: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


# Removed update_last_message function - not needed for simplified storage


def deactivate_subscriber(user_id: int) -> bool:
    """
    Deactivate a subscriber (soft delete) instead of removing completely
    
    Args:
        user_id (int): Telegram user ID to deactivate
        
    Returns:
        bool: True if deactivated successfully, False if not found
        
    Raises:
        mysql.connector.Error: If database update fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        UPDATE subscribers 
        SET is_active = FALSE, unsubscribed_at = NOW() 
        WHERE user_id = %s AND is_active = TRUE
        """
        
        cursor.execute(query, (user_id,))
        rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"Deactivated subscriber: user_id={user_id}")
            return True
        else:
            logger.info(f"User {user_id} not found or already inactive")
            return False
            
    except Error as e:
        logger.error(f"Error deactivating subscriber {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def reactivate_subscriber(user_id: int) -> bool:
    """
    Reactivate a previously deactivated subscriber
    
    Args:
        user_id (int): Telegram user ID to reactivate
        
    Returns:
        bool: True if reactivated successfully, False if not found
        
    Raises:
        mysql.connector.Error: If database update fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        UPDATE subscribers 
        SET is_active = TRUE, unsubscribed_at = NULL, subscribed_at = NOW() 
        WHERE user_id = %s AND is_active = FALSE
        """
        
        cursor.execute(query, (user_id,))
        rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            logger.info(f"Reactivated subscriber: user_id={user_id}")
            return True
        else:
            logger.info(f"User {user_id} not found or already active")
            return False
            
    except Error as e:
        logger.error(f"Error reactivating subscriber {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def get_subscriber_info(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Get basic information about a specific subscriber
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        Dict or None: Subscriber information or None if not found
        
    Raises:
        mysql.connector.Error: If database query fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT user_id, subscribed_at, unsubscribed_at, is_active
        FROM subscribers 
        WHERE user_id = %s
        """
        
        cursor.execute(query, (user_id,))
        subscriber = cursor.fetchone()
        
        if subscriber:
            logger.debug(f"Retrieved info for subscriber {user_id}")
        else:
            logger.debug(f"Subscriber {user_id} not found")
            
        return subscriber
        
    except Error as e:
        logger.error(f"Error fetching subscriber info for {user_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def cleanup_inactive_subscribers(days: int = 30) -> int:
    """
    Remove subscribers who haven't received messages in specified days
    
    Args:
        days (int): Number of days of inactivity before cleanup
        
    Returns:
        int: Number of subscribers cleaned up
        
    Raises:
        mysql.connector.Error: If database operation fails
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        query = """
        DELETE FROM subscribers 
        WHERE is_active = FALSE 
        AND (unsubscribed_at IS NULL OR unsubscribed_at < DATE_SUB(NOW(), INTERVAL %s DAY))
        """
        
        cursor.execute(query, (days,))
        rows_affected = cursor.rowcount
        
        logger.info(f"Cleaned up {rows_affected} inactive subscribers (older than {days} days)")
        return rows_affected
        
    except Error as e:
        logger.error(f"Error cleaning up inactive subscribers: {e}")
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
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        connection.close()
        logger.info("Database connection test successful")
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
    
    print("🔍 Testing subscriber storage...")
    if test_connection():
        print("✅ Database connection successful!")
        
        # Test adding a subscriber
        try:
            test_user_id = 123456789
            success = add_subscriber(test_user_id)
            print(f"✅ Add subscriber test: {'Success' if success else 'Already exists'}")
            
            # Test checking subscription
            subscribed = is_subscribed(test_user_id)
            print(f"✅ Subscription check: {subscribed}")
            
            # Test getting count
            count = get_subscriber_count()
            print(f"✅ Subscriber count: {count}")
            
            # Test getting all subscribers
            subscribers = get_all_subscribers()
            print(f"✅ Retrieved {len(subscribers)} subscribers")
            
        except Exception as e:
            print(f"❌ Error during testing: {e}")
    else:
        print("❌ Database connection failed!")
        print("💡 Check your database configuration in environment variables:")
        print("   - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")