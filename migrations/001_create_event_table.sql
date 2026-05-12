CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY ,
    user_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT events_event_type_check CHECK (event_type IN ('created', 'paid', 'shipped')),

    CONSTRAINT events_status_check CHECK (status IN ('pending', 'sent', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_events_user_id 
ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_status_created_at
ON events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_events_user_filters 
ON events(user_id, event_type, status);