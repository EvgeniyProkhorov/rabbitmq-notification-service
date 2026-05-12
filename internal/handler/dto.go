package handler

import (
	"encoding/json"
)

type CreateEventRequest struct {
	UserID    string          `json:"user_id" validate:"required"`
	OrderID   string          `json:"order_id" validate:"required"`
	EventType string          `json:"event_type" validate:"required,oneof=created paid shipped"`
	Payload   json.RawMessage `json:"payload"`
}

type CreateEventResponse struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}

type ListEventsRequest struct {
	UserID    string `query:"user_id"  validate:"required"`
	EventType string `query:"event_type"  validate:"omitempty,oneof=created paid shipped"`
	Status    string `query:"status"  validate:"omitempty,oneof=pending sent failed"`
	Page      int    `query:"page"  validate:"min=1"`
	Limit     int    `query:"limit"  validate:"min=1,max=100"`
}

type EventListItemResponse struct {
	EventID   string `json:"event_id"`
	OrderID   string `json:"order_id"`
	EventType string `json:"event_type"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type EventDetailsRequest struct {
	EventID string `path:"event_id" validate:"required,uuid"`
	UserID  string `query:"user_id" validate:"required"`
}

type EventDetailsResponse struct {
	EventID      string          `json:"event_id"`
	UserID       string          `json:"user_id"`
	OrderID      string          `json:"order_id"`
	EventType    string          `json:"event_type"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Attempts     int             `json:"attempts"`
	ErrorMessage *string         `json:"error_message"`
	CreatedAt    string          `json:"created_at"`
	UpdatedAt    string          `json:"updated_at"`
}

type RetryPendingResponse struct {
	Status string `json:"status"`
}
