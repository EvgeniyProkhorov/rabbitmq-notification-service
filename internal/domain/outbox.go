package domain

import "time"

// OutboxStatus описывает статус публикации сообщения во внешний брокер.
type OutboxStatus string

const (
	OutboxStatusPending    OutboxStatus = "pending"
	OutboxStatusProcessing OutboxStatus = "processing"
	OutboxStatusPublished  OutboxStatus = "published"
	OutboxStatusFailed     OutboxStatus = "failed"
)

// OutboxMessage описывает сообщение, ожидающее публикации во внешний брокер.
type OutboxMessage struct {
	ID           string
	EventID      string
	Exchange     string
	RoutingKey   string
	Payload      []byte
	Status       OutboxStatus
	Attempts     int
	ErrorMessage *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
