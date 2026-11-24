-- Добавляем поля для асинхронной обработки
ALTER TABLE attachments ADD COLUMN IF NOT EXISTS staging_key VARCHAR(255);
ALTER TABLE attachments ADD COLUMN IF NOT EXISTS error_code VARCHAR(50);
ALTER TABLE attachments ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Таблица вариантов
CREATE TABLE IF NOT EXISTS variants (
    attachment_id UUID REFERENCES attachments(id) ON DELETE CASCADE,
    pipeline_version INTEGER NOT NULL,
    name VARCHAR(20) NOT NULL,
    s3_key VARCHAR(255) NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    size_bytes BIGINT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (attachment_id, pipeline_version, name)
);

-- Outbox
CREATE TABLE IF NOT EXISTS outbox (
    id SERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);