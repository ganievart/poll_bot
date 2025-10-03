-- Schema for immediate confirmation tracking
-- This table stores state for "План в силе? 💪" confirmation messages
-- Tracks individual user responses to determine when everyone has confirmed

CREATE TABLE IF NOT EXISTS immediate_confirmations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    poll_id VARCHAR(255) NULL,                -- Optional reference to originating poll
    message_id BIGINT NOT NULL,               -- Telegram message ID of confirmation message
    poll_result VARCHAR(500) NOT NULL,       -- Selected meeting option text (e.g., "Понедельник (25.11) в 15:00")
    
    -- User tracking (JSON arrays for flexibility and easy serialization)
    all_voters JSON NOT NULL,                 -- All users who participated in original poll: [123456, 789012, 456789]
    confirmed_users JSON NOT NULL,            -- Users who clicked "👍 Да, продолжаем!": [123456, 789012]
    declined_users JSON NOT NULL,             -- Users who clicked "❌ Нет, отменить": [456789]
    
    -- Status and completion tracking
    status ENUM('pending', 'completed', 'cancelled', 'expired') DEFAULT 'pending',
    completion_message_id BIGINT NULL,        -- Message ID of "🎉 Отлично! Встреча состоится!" message
    
    -- Timestamps for lifecycle management
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    
    -- Essential indexes for performance
    UNIQUE KEY uniq_chat_message (chat_id, message_id),    -- One confirmation per message
    INDEX idx_chat_status (chat_id, status),               -- Find pending confirmations per chat
    INDEX idx_status_created (status, created_at),         -- Cleanup expired confirmations
    INDEX idx_poll_id (poll_id)                           -- Optional poll reference lookup
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Optional: Add cleanup procedure for expired confirmations (older than 24 hours)
-- This helps prevennomt table bloat from abandoned confirmation sessions
DELIMITER //
CREATE EVENT IF NOT EXISTS cleanup_expired_confirmations
ON SCHEDULE EVERY 6 HOUR
DO
BEGIN
    -- Mark old pending confirmations as expired
    UPDATE immediate_confirmations 
    SET status = 'expired', updated_at = CURRENT_TIMESTAMP
    WHERE status = 'pending' 
    AND created_at < DATE_SUB(NOW(), INTERVAL 24 HOUR);
    
    -- Delete very old completed/expired records (older than 7 days)
    DELETE FROM immediate_confirmations 
    WHERE status IN ('completed', 'cancelled', 'expired') 
    AND (completed_at < DATE_SUB(NOW(), INTERVAL 7 DAY) OR updated_at < DATE_SUB(NOW(), INTERVAL 7 DAY));
END //
DELIMITER ;

-- Enable event scheduler if not already enabled
-- SET GLOBAL event_scheduler = ON;