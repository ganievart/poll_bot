-- Schema for storing finalized meetings per chat
-- This table is the single source of truth for past (and future) meetings.
-- Convention: store meeting_datetime in UTC (application converts from/to local time e.g. Europe/Warsaw).

CREATE TABLE IF NOT EXISTS meetings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    poll_id VARCHAR(255) NULL,
    meeting_datetime DATETIME NOT NULL,
    selected_option_text VARCHAR(255) NOT NULL,
    confirmation_message_id BIGINT NULL,
    pinned_message_id BIGINT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_poll (poll_id),
    INDEX idx_chat_meeting_dt (chat_id, meeting_datetime),
    INDEX idx_chat_id (chat_id),
    INDEX idx_meeting_dt (meeting_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
