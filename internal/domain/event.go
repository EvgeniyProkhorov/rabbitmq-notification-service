package domain

import (
	"encoding/json"
	"time"
)

type EventStatus string
type EventType string

const (
	EventTypeCreated EventType = "created"
	EventTypePaid    EventType = "paid"
	EventTypeShipped EventType = "shipped"
)

const (
	EventStatusPending EventStatus = "pending"
	EventStatusSent    EventStatus = "sent"
	EventStatusFailed  EventStatus = "failed"
)

type Event struct {
	EventID      string
	UserID       string
	OrderID      string
	EventType    EventType
	Payload      json.RawMessage
	Status       EventStatus
	Attempts     int
	ErrorMessage *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
