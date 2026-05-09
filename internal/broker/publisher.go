package broker

import (
	"context"
	"rabbitmq-notification-service/internal/domain"
)

// Publisher описывает контракт публикации событий во внешний брокер сообщений.
type Publisher interface {
	PublishEvent(ctx context.Context, event domain.Event) error
}
