package repository

import (
	"context"
	"errors"
	"rabbitmq-notification-service/internal/domain"
)

var ErrEventNotFound = errors.New("event not found")

type EventFilter struct {
	UserID    string
	EventType string
	Status    string
	Limit     int
	Offset    int
}

type EventRepository interface {
	// List получаем список событий пользователя
	List(ctx context.Context, filter EventFilter) ([]domain.Event, error)

	// GetByID получить детальную инфу по событию
	GetByID(ctx context.Context, eventID string, userID string) (domain.Event, error)

	// MarkAsSent обновляет статус события в БД на sent и отправляет Ack.
	MarkAsSent(ctx context.Context, eventID string) error

	// MarkAsFailed если событие не удалось обработать 3 раза — пометить в БД как failed и логировать ошибку.
	MarkAsFailed(ctx context.Context, eventID string, errorMessage string) error

	// IncrementAttempts увеличиваем счетчик попыток
	IncrementAttempts(ctx context.Context, eventID string) (int, error)

	// CreateWithOutbox сохраняет событие в свою и outbox таблицу
	CreateWithOutbox(ctx context.Context, event domain.Event, message domain.OutboxMessage) error

	// ClaimPendingOutbox забирает pending outbox-сообщения в обработку.
	ClaimPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxMessage, error)

	// MarkOutboxPublished помечает outbox-сообщение как успешно опубликованное.
	MarkOutboxPublished(ctx context.Context, id string) error

	// MarkOutboxFailed помечает outbox-сообщение как не опубликованное и сохраняет текст ошибки.
	MarkOutboxFailed(ctx context.Context, id string, errorMessage string) error

	// ReleaseOutboxForRetry возвращает outbox-сообщение в pending для повторной публикации.
	ReleaseOutboxForRetry(ctx context.Context, id string, errorMessage string) error
}
