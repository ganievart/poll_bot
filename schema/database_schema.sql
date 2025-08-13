-- Simple MySQL Schema for Telegram Bot Scheduled Tasks
-- Single table to replace asyncio.sleep() calls with database-backed scheduling

-- Create database (optional - uncomment if needed)
-- CREATE DATABASE simple_poll_bot;
-- USE simple_poll_bot;

-- Single table for all scheduled tasks
CREATE TABLE scheduled_tasks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    poll_id VARCHAR(255) NULL,
    task_type ENUM(
        'confirmation',
        'followup', 
        'unpin_message',
        'poll_voting_timeout',
        'session_cleanup'
    ) NOT NULL,
    scheduled_time DATETIME NOT NULL,
    is_executed BOOLEAN DEFAULT FALSE,
    task_data TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP NULL,
    
    -- Essential indexes only
    INDEX idx_pending (is_executed, scheduled_time),
    INDEX idx_chat_id (chat_id)
);