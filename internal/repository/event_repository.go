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
	// Create сохраяняем событие в таблицу
	Create(ctx context.Context, event domain.Event) error

	// List получаем список событий пользователя
	List(ctx context.Context, filter EventFilter) ([]domain.Event, error)

	// GetByID получить детальную инфу по событию
	GetByID(ctx context.Context, eventID string, userID string) (domain.Event, error)

	// GetPending Раз в 30 секунд забирать из БД события со статусом pending
	GetPending(ctx context.Context, limit int) ([]domain.Event, error)

	// MarkAsSent обновляет статус события в БД на sent и отправляет Ack.
	MarkAsSent(ctx context.Context, eventID string) error

	// MarkAsFailed если событие не удалось обработать 3 раза — пометить в БД как failed и логировать ошибку.
	MarkAsFailed(ctx context.Context, eventID string, errorMessage string) error

	// IncrementAttempts увеличиваем счетчик попыток
	IncrementAttempts(ctx context.Context, eventID string) (int, error)
}
