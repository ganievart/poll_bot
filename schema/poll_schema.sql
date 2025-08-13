-- Schema for persisting polls and votes

-- Polls table stores basic poll metadata and options
CREATE TABLE IF NOT EXISTS polls (
    poll_id VARCHAR(255) PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    question VARCHAR(255) NOT NULL,
    options_json JSON NOT NULL,
    creator_id BIGINT NOT NULL,
    poll_message_id BIGINT NULL,
    target_member_count INT NULL,
    pinned_message_id BIGINT NULL,
    is_closed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_chat_id (chat_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Votes table stores per-user selections as array of option indices
CREATE TABLE IF NOT EXISTS poll_votes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    poll_id VARCHAR(255) NOT NULL,
    user_id BIGINT NOT NULL,
    option_ids_json JSON NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_poll_user (poll_id, user_id),
    INDEX idx_poll (poll_id),
    CONSTRAINT fk_votes_poll FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Immediate confirmations table removed
