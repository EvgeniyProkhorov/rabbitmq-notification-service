CREATE TABLE IF NOT EXISTS outbox_messages (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    exchange_name TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT outbox_messages_status_check
        CHECK (status IN ('pending', 'processing', 'published', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_status_created_at
ON outbox_messages(status, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_event_id
ON outbox_messages(event_id);