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

ALTER TABLE polls ADD COLUMN revote_notified BOOLEAN NOT NULL DEFAULT FALSE AFTER is_closed, ADD COLUMN in_revote
BOOLEAN NOT NULL DEFAULT FALSE AFTER revote_notified, ADD COLUMN last_tie_signature TEXT NULL AFTER in_revote, ADD
COLUMN last_tie_message_at DATETIME NULL AFTER last_tie_signature, ADD COLUMN tie_message_count INT NOT NULL DEFAULT
0 AFTER last_tie_message_at, ADD COLUMN revote_message_id BIGINT NULL AFTER tie_message_count;


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

