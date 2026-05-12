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
	pendingBatchLimit    = 10
	maxAttempts          = 3
	maxOutboxAttempts    = 3
	outboxPublishTimeout = 5 * time.Second
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
	HandleNotification(ctx context.Context, eventID string) (domain.NotificationResult, error)
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

// CreateEvent регистрирует событие заказа и создаёт outbox-сообщение для публикации.
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

	outboxMessage, err := s.publisher.BuildOutboxMessage(event)
	if err != nil {
		return CreateEventOutput{}, fmt.Errorf("build outbox message: %w", err)
	}

	if err := s.repo.CreateWithOutbox(ctx, event, outboxMessage); err != nil {
		return CreateEventOutput{}, fmt.Errorf("save event: %w", err)
	}

	return CreateEventOutput{EventID: eventID, Status: string(domain.EventStatusPending)}, nil
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

// ProcessPendingEvents публикует pending outbox-сообщения в RabbitMQ.
func (s *eventService) ProcessPendingEvents(ctx context.Context) error {
	messages, err := s.repo.ClaimPendingOutbox(ctx, pendingBatchLimit)
	if err != nil {
		return fmt.Errorf("claim pending outbox messages: %w", err)
	}

	for _, message := range messages {
		publishCtx, cancel := context.WithTimeout(ctx, outboxPublishTimeout)
		err := s.publisher.PublishOutboxMessage(publishCtx, message)
		cancel()

		if err != nil {
			if message.Attempts >= maxOutboxAttempts {
				if markErr := s.repo.MarkOutboxFailed(ctx, message.ID, err.Error()); markErr != nil {
					return fmt.Errorf("publish outbox message %s: %v; mark as failed: %w", message.ID, err, markErr)
				}

				return fmt.Errorf("publish outbox message %s: %w", message.ID, err)
			}

			if releaseErr := s.repo.ReleaseOutboxForRetry(ctx, message.ID, err.Error()); releaseErr != nil {
				return fmt.Errorf("publish outbox message %s: %v; release for retry: %w", message.ID, err, releaseErr)
			}

			return fmt.Errorf("publish outbox message %s: %w", message.ID, err)
		}

		if err := s.repo.MarkOutboxPublished(ctx, message.ID); err != nil {
			return fmt.Errorf("mark outbox message %s as published: %w", message.ID, err)
		}
	}

	return nil
}

func (s *eventService) HandleNotification(ctx context.Context, eventID string) (domain.NotificationResult, error) {
	correlationID := correlation.FromContext(ctx)

	log.Printf(
		"correlation_id=%s handle notification started event_id=%s",
		correlationID,
		eventID,
	)

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return domain.NotificationResult{}, fmt.Errorf("notification canceled: %w", ctx.Err())
	case <-timer.C:
	}

	if rand.Intn(100) < 10 {
		errMessage := "simulated notification failure"

		attempts, err := s.repo.IncrementAttempts(ctx, eventID)
		if err != nil {
			return domain.NotificationResult{}, fmt.Errorf("increment attempts for event %s: %w", eventID, err)
		}

		log.Printf(
			"correlation_id=%s notification failed event_id=%s attempts=%d",
			correlationID,
			eventID,
			attempts,
		)

		if attempts >= maxAttempts {
			if err := s.repo.MarkAsFailed(ctx, eventID, errMessage); err != nil {
				return domain.NotificationResult{}, fmt.Errorf("mark event %s as failed: %w", eventID, err)
			}

			log.Printf(
				"correlation_id=%s event marked as failed event_id=%s attempts=%d",
				correlationID,
				eventID,
				attempts,
			)
			return domain.NotificationResult{
				Retryable: false,
			}, fmt.Errorf("send notification for event %s: %s", eventID, errMessage)

		}
		return domain.NotificationResult{
			Retryable: true,
		}, fmt.Errorf("send notification for event %s: %s", eventID, errMessage)
	}

	if err := s.repo.MarkAsSent(ctx, eventID); err != nil {
		return domain.NotificationResult{}, fmt.Errorf("mark event %s as sent: %w", eventID, err)
	}

	log.Printf(
		"correlation_id=%s notification handled successfully event_id=%s",
		correlationID,
		eventID,
	)

	return domain.NotificationResult{}, nil

}
