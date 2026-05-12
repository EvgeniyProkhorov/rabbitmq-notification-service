package broker

import (
	"context"
	"rabbitmq-notification-service/internal/domain"
)

// Publisher описывает контракт публикации событий во внешний брокер сообщений.
type Publisher interface {
	BuildOutboxMessage(event domain.Event) (domain.OutboxMessage, error)
	PublishOutboxMessage(ctx context.Context, message domain.OutboxMessage) error
}
