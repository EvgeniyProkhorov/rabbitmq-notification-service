package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"rabbitmq-notification-service/internal/broker"
	"rabbitmq-notification-service/internal/correlation"
	"rabbitmq-notification-service/internal/domain"
	"rabbitmq-notification-service/internal/repository"
	"time"

	"github.com/google/uuid"
)

const (
	pendingBatchLimit = 10
	maxAttempts       = 3
)

type CreateEventInput struct {
	UserID    string
	OrderID   string
	EventType string
	Payload   json.RawMessage
}

type CreateEventOutput struct {
	EventID string
	Status  string
}

type ListEventsInput struct {
	UserID    string
	EventType string
	Status    string
	Page      int
	Limit     int
}

type EventListItemOutput struct {
	EventID   string
	OrderID   string
	EventType string
	Status    string
	CreatedAt time.Time
}
type ListEventsOutput struct {
	Items []EventListItemOutput
}

type GetEventDetailsInput struct {
	EventID string
	UserID  string
}

type GetEventDetailsOutput struct {
	EventID      string
	UserID       string
	OrderID      string
	EventType    string
	Payload      json.RawMessage
	Status       string
	Attempts     int
	ErrorMessage *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// EventService описывает бизнес-сценарии работы с событиями заказов.
type EventService interface {
	CreateEvent(ctx context.Context, input CreateEventInput) (CreateEventOutput, error)
	ListEvents(ctx context.Context, input ListEventsInput) (ListEventsOutput, error)
	GetEventDetails(ctx context.Context, input GetEventDetailsInput) (GetEventDetailsOutput, error)
	ProcessPendingEvents(ctx context.Context) error
	HandleNotification(ctx context.Context, eventID string) error
}

type eventService struct {
	repo      repository.EventRepository
	publisher broker.Publisher
}

// NewEventService создаёт service для работы с событиями заказов.
func NewEventService(
	repo repository.EventRepository,
	publisher broker.Publisher,
) EventService {
	return &eventService{
		repo:      repo,
		publisher: publisher,
	}
}

func (s *eventService) CreateEvent(ctx context.Context, input CreateEventInput) (CreateEventOutput, error) {
	eventID := uuid.NewString()

	event := domain.Event{
		EventID:      eventID,
		UserID:       input.UserID,
		OrderID:      input.OrderID,
		EventType:    domain.EventType(input.EventType),
		Payload:      input.Payload,
		Status:       domain.EventStatusPending,
		Attempts:     0,
		ErrorMessage: nil,
	}

	if err := s.repo.Create(ctx, event); err != nil {
		return CreateEventOutput{}, fmt.Errorf("save event: %w", err)
	}

	return CreateEventOutput{EventID: eventID, Status: "queued"}, nil
}

func (s *eventService) ListEvents(ctx context.Context, input ListEventsInput) (ListEventsOutput, error) {
	offset := (input.Page - 1) * input.Limit

	filter := repository.EventFilter{
		UserID:    input.UserID,
		EventType: input.EventType,
		Status:    input.Status,
		Limit:     input.Limit,
		Offset:    offset,
	}

	events, err := s.repo.List(ctx, filter)
	if err != nil {
		return ListEventsOutput{}, fmt.Errorf("list events: %w", err)
	}

	items := make([]EventListItemOutput, 0, len(events))

	for _, event := range events {
		items = append(items, EventListItemOutput{
			EventID:   event.EventID,
			OrderID:   event.OrderID,
			EventType: string(event.EventType),
			Status:    string(event.Status),
			CreatedAt: event.CreatedAt,
		})
	}

	return ListEventsOutput{Items: items}, nil

}

func (s *eventService) GetEventDetails(ctx context.Context, input GetEventDetailsInput) (GetEventDetailsOutput, error) {
	event, err := s.repo.GetByID(ctx, input.EventID, input.UserID)
	if err != nil {
		return GetEventDetailsOutput{}, fmt.Errorf("get event details: %w", err)
	}

	return GetEventDetailsOutput{
		EventID:      event.EventID,
		UserID:       event.UserID,
		OrderID:      event.OrderID,
		EventType:    string(event.EventType),
		Payload:      event.Payload,
		Status:       string(event.Status),
		Attempts:     event.Attempts,
		ErrorMessage: event.ErrorMessage,
		CreatedAt:    event.CreatedAt,
		UpdatedAt:    event.UpdatedAt,
	}, nil
}

func (s *eventService) ProcessPendingEvents(ctx context.Context) error {
	events, err := s.repo.GetPending(ctx, pendingBatchLimit)
	if err != nil {
		return fmt.Errorf("load pending events: %w", err)
	}

	for _, event := range events {
		if err := s.publisher.PublishEvent(ctx, event); err != nil {
			return fmt.Errorf("publish pending event %s: %w", event.EventID, err)
		}
	}

	return nil
}

func (s *eventService) HandleNotification(ctx context.Context, eventID string) error {
	correlationID := correlation.FromContext(ctx)

	log.Printf(
		"correlation_id=%s handle notification started event_id=%s",
		correlationID,
		eventID,
	)

	time.Sleep(100 * time.Millisecond)

	if rand.Intn(100) < 10 {
		errMessage := "simulated notification failure"

		attempts, err := s.repo.IncrementAttempts(ctx, eventID)
		if err != nil {
			return fmt.Errorf("increment attempts for event %s: %w", eventID, err)
		}

		log.Printf(
			"correlation_id=%s notification failed event_id=%s attempts=%d",
			correlationID,
			eventID,
			attempts,
		)

		if attempts >= maxAttempts {
			if err := s.repo.MarkAsFailed(ctx, eventID, errMessage); err != nil {
				return fmt.Errorf("mark event %s as failed: %w", eventID, err)
			}

			log.Printf(
				"correlation_id=%s event marked as failed event_id=%s attempts=%d",
				correlationID,
				eventID,
				attempts,
			)

		}
		return fmt.Errorf("send notification for event %s: %s", eventID, errMessage)
	}

	if err := s.repo.MarkAsSent(ctx, eventID); err != nil {
		return fmt.Errorf("mark event %s as sent: %w", eventID, err)
	}

	log.Printf(
		"correlation_id=%s notification handled successfully event_id=%s",
		correlationID,
		eventID,
	)

	return nil

}
