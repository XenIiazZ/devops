CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(32) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chats (
    id UUID PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chat_members (
    chat_id UUID REFERENCES chats(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    chat_id UUID REFERENCES chats(id) ON DELETE CASCADE,
    sender_id UUID REFERENCES users(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_chat_created ON messages (chat_id, created_at, id);

CREATE TABLE idempotency_keys (
    idem_key VARCHAR(128),
    user_id UUID REFERENCES users(id),
    message_id UUID REFERENCES messages(id),
    body_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, idem_key)
);

-- Таблицы для Лабы 4
CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    chat_id UUID REFERENCES chats(id) ON DELETE CASCADE,
    owner_id UUID REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    s3_key VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(20) DEFAULT 'ready',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE message_attachments (
    message_id UUID REFERENCES messages(id) ON DELETE CASCADE,
    attachment_id UUID REFERENCES attachments(id) ON DELETE CASCADE,
    PRIMARY KEY (message_id, attachment_id)
);